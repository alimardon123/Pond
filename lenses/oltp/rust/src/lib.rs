// OLTPLens — fast key-value with in-memory memtable + batch flush.
//
// Solves the KV competitive gap: direct shard writes are ~0.5-3ms per write
// (S3 RTT). With a memtable, writes are sub-microsecond (in-memory) and
// flush to object storage in batches — amortizing S3 latency across N writes.
//
// DESIGN FOR COLD CONCURRENT MULTI-APP:
//   Each app process has its own OLTPLens instance with its own memtable.
//   Writes go to the in-memory memtable (sub-µs). When the memtable is full
//   (or flush() is called explicitly), it flushes to storage as a commit.
//
//   Multiple apps flush independently — no coordination, no CAS, no locks.
//   CRDT merge (read_with_shards) handles conflicts deterministically.
//
// This is the SAME pattern as RocksDB's LSM-tree, but:
//   - SST files → commits (concurrent-safe, no CAS)
//   - Compaction → compact_shards (already built in UnifiedStorage)
//   - Multi-process → each flushes independently (CRDT handles conflicts)
//
// API parity with Python OLTPLens:
//   - put(key, value)         — fast write (in-memory)
//   - delete(key)             — tombstone (in-memory)
//   - get(key)                — read (memtable first, then storage)
//   - exists(key)             — check existence
//   - keys()                  — list keys
//   - count()                 — count rows
//   - flush()                 — flush memtable to storage
//   - pending_count()         — count unflushed entries
//
// See lenses/oltp/python/oltp_lens.py for the full Python implementation.

use std::collections::HashMap;
use std::sync::Mutex;

use pond_storage::UnifiedStorage;
use pond_storage::{write as storage_write, read as storage_read};
use serde_json::{Value, json};

/// Default flush threshold: 1000 entries.
pub const DEFAULT_FLUSH_THRESHOLD: usize = 1000;

/// OLTPLens — fast KV with in-memory memtable + batch flush.
///
/// # Example
/// ```ignore
/// use pond_oltp_lens::OLTPLens;
/// use pond_storage::UnifiedStorage;
///
/// let storage = UnifiedStorage::new_local("/var/lib/pond").unwrap();
/// let mut oltp = OLTPLens::new(storage, "kv", 1000);
///
/// // Fast writes (in-memory, sub-µs)
/// oltp.put("user:1", &json!({"name": "alice", "age": 30}));
/// oltp.put("user:2", &json!({"name": "bob", "age": 25}));
/// oltp.delete("user:2");
///
/// // Reads check memtable first (0 GETs), then storage
/// let user = oltp.get("user:1"); // → Some({"name": "alice", "age": 30})
///
/// // Flush to storage (1 PUT, amortized across all writes)
/// oltp.flush();
/// ```
pub struct OLTPLens {
    storage: UnifiedStorage,
    collection: String,
    flush_threshold: usize,
    // In-memory memtable: key → Optional<value>
    // None means "deleted" (tombstone)
    memtable: Mutex<HashMap<String, Option<Value>>>,
}

impl OLTPLens {
    /// Create a new OLTPLens.
    ///
    /// Args:
    ///   - `storage`: The UnifiedStorage handle
    ///   - `collection`: The collection name to bind to
    ///   - `flush_threshold`: Auto-flush when memtable reaches this size
    pub fn new(storage: UnifiedStorage, collection: &str, flush_threshold: usize) -> Self {
        Self {
            storage,
            collection: collection.to_string(),
            flush_threshold,
            memtable: Mutex::new(HashMap::new()),
        }
    }

    /// Put a key-value pair into the memtable (fast, in-memory).
    ///
    /// Auto-flushes if the memtable exceeds `flush_threshold`.
    pub fn put(&self, key: &str, value: &Value) {
        let should_flush = {
            let mut memtable = self.memtable.lock().unwrap();
            memtable.insert(key.to_string(), Some(value.clone()));
            memtable.len() >= self.flush_threshold
        };

        if should_flush {
            let _ = self.flush();
        }
    }

    /// Delete a key from the memtable (tombstone).
    ///
    /// Auto-flushes if the memtable exceeds `flush_threshold`.
    pub fn delete(&self, key: &str) {
        let should_flush = {
            let mut memtable = self.memtable.lock().unwrap();
            memtable.insert(key.to_string(), None);
            memtable.len() >= self.flush_threshold
        };

        if should_flush {
            let _ = self.flush();
        }
    }

    /// Get a value by key.
    ///
    /// Checks the memtable first (0 GETs). If not in memtable, reads from storage.
    pub fn get(&self, key: &str) -> Option<Value> {
        // Check memtable first
        {
            let memtable = self.memtable.lock().unwrap();
            if let Some(entry) = memtable.get(key) {
                return entry.clone(); // Some(value) or None (deleted)
            }
        }

        // Not in memtable — read from storage
        let active = self.storage.get_active_branch(&self.collection);
        let data = match storage_read::read(self.storage.kernel(), &self.collection, &active) {
            Ok(d) => d,
            Err(_) => return None,
        };

        if data.is_empty() {
            return None;
        }

        let rows: Vec<Value> = match serde_json::from_slice(&data) {
            Ok(r) => r,
            Err(_) => return None,
        };

        for row in &rows {
            if let Some(obj) = row.as_object() {
                if let Some(k) = obj.get("_key").and_then(|k| k.as_str()) {
                    if k == key {
                        let mut value = obj.clone();
                        value.remove("_key");
                        return Some(Value::Object(value));
                    }
                }
            }
        }
        None
    }

    /// Check if a key exists (memtable or storage).
    pub fn exists(&self, key: &str) -> bool {
        self.get(key).is_some()
    }

    /// List all keys (memtable + storage).
    pub fn keys(&self) -> Vec<String> {
        let mut all_keys: Vec<String> = Vec::new();

        // Add memtable keys
        {
            let memtable = self.memtable.lock().unwrap();
            for (k, v) in memtable.iter() {
                if v.is_some() {
                    all_keys.push(k.clone());
                }
            }
        }

        // Add storage keys
        let active = self.storage.get_active_branch(&self.collection);
        if let Ok(data) = storage_read::read(self.storage.kernel(), &self.collection, &active) {
            if !data.is_empty() {
                if let Ok(rows) = serde_json::from_slice::<Vec<Value>>(&data) {
                    for row in &rows {
                        if let Some(obj) = row.as_object() {
                            if let Some(k) = obj.get("_key").and_then(|k| k.as_str()) {
                                if !all_keys.contains(&k.to_string()) {
                                    all_keys.push(k.to_string());
                                }
                            }
                        }
                    }
                }
            }
        }

        all_keys.sort();
        all_keys
    }

    /// Count all entries (memtable + storage).
    pub fn count(&self) -> usize {
        self.keys().len()
    }

    /// Flush the memtable to storage.
    ///
    /// Reads current storage state, applies memtable changes, and writes
    /// the merged result as a new commit.
    pub fn flush(&self) -> Result<String, String> {
        let memtable = {
            let mut memtable = self.memtable.lock().unwrap();
            std::mem::take(&mut *memtable)
        };

        if memtable.is_empty() {
            return Err("memtable is empty".to_string());
        }

        // Read current storage state
        let active = self.storage.get_active_branch(&self.collection);
        let mut existing: HashMap<String, Value> = HashMap::new();

        if let Ok(data) = storage_read::read(self.storage.kernel(), &self.collection, &active) {
            if !data.is_empty() {
                if let Ok(rows) = serde_json::from_slice::<Vec<Value>>(&data) {
                    for row in rows {
                        if let Some(obj) = row.as_object() {
                            if let Some(k) = obj.get("_key").and_then(|k| k.as_str()) {
                                let mut v = obj.clone();
                                v.remove("_key");
                                existing.insert(k.to_string(), Value::Object(v));
                            }
                        }
                    }
                }
            }
        }

        // Apply memtable changes
        for (key, value_opt) in &memtable {
            match value_opt {
                Some(v) => { existing.insert(key.clone(), v.clone()); }
                None => { existing.remove(key); }
            }
        }

        // Serialize to JSON array
        let rows: Vec<Value> = existing.iter()
            .map(|(k, v)| {
                if let Some(obj) = v.as_object() {
                    let mut obj = obj.clone();
                    obj.insert("_key".to_string(), json!(k));
                    Value::Object(obj)
                } else {
                    json!({"_key": k, "value": v})
                }
            })
            .collect();

        let data = serde_json::to_vec(&rows).map_err(|e| e.to_string())?;

        storage_write::write(
            self.storage.kernel(),
            &self.collection,
            &active,
            &data,
            "OLTP flush",
        ).map_err(|e| e.to_string())
    }

    /// Count unflushed entries in the memtable.
    pub fn pending_count(&self) -> usize {
        self.memtable.lock().unwrap().len()
    }

    /// Get a reference to the underlying UnifiedStorage.
    pub fn storage(&self) -> &UnifiedStorage {
        &self.storage
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_oltp() -> OLTPLens {
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        std::mem::forget(dir);
        OLTPLens::new(storage, "kv", 1000)
    }

    #[test]
    fn test_put_and_get() {
        let oltp = make_test_oltp();
        oltp.put("user:1", &json!({"name": "alice", "age": 30}));

        let val = oltp.get("user:1").unwrap();
        assert_eq!(val["name"], "alice");
        assert_eq!(val["age"], 30);
    }

    #[test]
    fn test_get_nonexistent() {
        let oltp = make_test_oltp();
        assert!(oltp.get("user:999").is_none());
    }

    #[test]
    fn test_delete_in_memtable() {
        let oltp = make_test_oltp();
        oltp.put("user:1", &json!({"name": "alice"}));
        assert!(oltp.exists("user:1"));

        oltp.delete("user:1");
        assert!(!oltp.exists("user:1"));
    }

    #[test]
    fn test_flush_and_cold_read() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let storage = UnifiedStorage::new_local(&path).unwrap();
        let oltp = OLTPLens::new(storage, "kv", 1000);
        oltp.put("user:1", &json!({"name": "alice"}));
        oltp.put("user:2", &json!({"name": "bob"}));
        oltp.flush().unwrap();

        // Create a new OLTPLens pointing to the same storage
        let storage2 = UnifiedStorage::new_local(&path).unwrap();
        let oltp2 = OLTPLens::new(storage2, "kv", 1000);

        // Cold read (empty memtable) — should see flushed data
        let val = oltp2.get("user:1").unwrap();
        assert_eq!(val["name"], "alice");
    }

    #[test]
    fn test_keys_and_count() {
        let oltp = make_test_oltp();
        oltp.put("user:1", &json!({"name": "alice"}));
        oltp.put("user:2", &json!({"name": "bob"}));
        oltp.put("user:3", &json!({"name": "carol"}));

        let mut keys = oltp.keys();
        keys.sort();
        assert_eq!(keys, vec!["user:1", "user:2", "user:3"]);
        assert_eq!(oltp.count(), 3);
    }

    #[test]
    fn test_pending_count() {
        let oltp = make_test_oltp();
        oltp.put("user:1", &json!({"name": "alice"}));
        oltp.put("user:2", &json!({"name": "bob"}));
        assert_eq!(oltp.pending_count(), 2);

        oltp.flush().unwrap();
        assert_eq!(oltp.pending_count(), 0);
    }

    #[test]
    fn test_overwrite_key() {
        let oltp = make_test_oltp();
        oltp.put("user:1", &json!({"name": "alice"}));
        oltp.put("user:1", &json!({"name": "alice v2"}));

        let val = oltp.get("user:1").unwrap();
        assert_eq!(val["name"], "alice v2");
    }

    #[test]
    fn test_auto_flush_at_threshold() {
        let oltp = make_test_oltp();
        // flush_threshold = 1000, so we need 1000 entries to trigger auto-flush
        for i in 0..1001 {
            oltp.put(&format!("key:{}", i), &json!({"i": i}));
        }
        // After 1000 entries, auto-flush should have been triggered
        assert_eq!(oltp.pending_count(), 1); // only the 1001st entry remains
    }
}
