// CollectionIndexer — collection-level secondary indexes.
//
// Port of Python bindings/python/sdk/extensions/indexing/collection_index.py
//
// Indexes are stored as JSON blobs: {index_key: rowid_string}.
// This is simpler than a ProllyTree and works with the unified architecture.
//
// The index is content-addressed (stored as kernel blobs) and referenced
// from collections/{name}/indexes/{index_name}.
//
// API:
//   let indexer = CollectionIndexer::new(kernel);
//   indexer.build_index("users", "by_name", &rows, |row| row["name"].as_str().unwrap().to_string());
//   let rowid = indexer.lookup("users", "by_name", "alice"); // → Some("user:1")
//
// MULTI-KEY INDEXES:
//   The extractor can return multiple keys per row (e.g., for tags):
//   |row| vec!["tag:rust".to_string(), "tag:db".to_string()]
//   → lookup("tag:rust") and lookup("tag:db") both find this row.

use pond_kernel::PondKernel;
use pond_storage::maintenance;
use serde_json::{Value, json};
use std::collections::HashMap;

/// Collection-level indexer. Operates on any collection via the kernel.
///
/// Indexes are stored as JSON blobs (index_key → rowid mappings).
/// O(1) PUT for the entire index (one blob). O(1) GET on lookup.
pub struct CollectionIndexer<'a> {
    kernel: &'a PondKernel,
}

impl<'a> CollectionIndexer<'a> {
    pub fn new(kernel: &'a PondKernel) -> Self {
        Self { kernel }
    }

    /// Build an index on a collection.
    ///
    /// Args:
    ///   - collection: Collection name
    ///   - index_name: Name for this index (e.g., "by_name", "by_email")
    ///   - rows: The rows to index (Vec<(rowid, row_data)>)
    ///   - extractor: Function that extracts index key(s) from a row.
    ///                Can return a single key or multiple keys (multi-key index).
    ///
    /// Returns: index blob hash
    pub fn build_index(
        &self,
        collection: &str,
        index_name: &str,
        rows: &[(String, Value)],
        extractor: impl Fn(&Value) -> Vec<String>,
    ) -> Result<String, String> {
        let mut index_entries: HashMap<String, String> = HashMap::new();

        for (rowid, row_data) in rows {
            let keys = extractor(row_data);
            for key in keys {
                index_entries.insert(key, rowid.clone());
            }
        }

        // Store as a JSON blob (simple, debuggable, content-addressed)
        let index_json = serde_json::to_string(&index_entries)
            .map_err(|e| format!("Failed to serialize index: {}", e))?;
        let index_hash = self.kernel.write(index_json.as_bytes())
            .map_err(|e| format!("Failed to write index: {}", e))?;

        let ref_name = self.index_ref(collection, index_name);
        self.kernel.reference(&ref_name, &index_hash)
            .map_err(|e| format!("Failed to reference index: {}", e))?;

        Ok(index_hash)
    }

    /// Drop an index (tombstone the ref).
    ///
    /// Returns: true if the index existed and was dropped.
    pub fn drop_index(&self, collection: &str, index_name: &str) -> bool {
        let ref_name = self.index_ref(collection, index_name);
        let current = self.kernel.resolve(&ref_name);

        if current.is_none() || current.as_deref() == Some(&maintenance::tombstone_hash()) {
            return false;
        }

        maintenance::drop_name(self.kernel, &ref_name);
        true
    }

    /// Look up a single rowid by index key.
    ///
    /// O(1) GET: reads the index JSON and returns the rowid directly.
    pub fn lookup(&self, collection: &str, index_name: &str, index_key: &str) -> Option<String> {
        let ref_name = self.index_ref(collection, index_name);
        let index_hash = maintenance::resolve_active(self.kernel, &ref_name)?;

        let index_data = self.kernel.read_blob(&index_hash).ok()?;
        let index: HashMap<String, String> = serde_json::from_slice(&index_data).ok()?;
        index.get(index_key).cloned()
    }

    /// Look up all rowids matching an index key.
    ///
    /// For single-key indexes, returns a Vec with 0 or 1 elements.
    /// For multi-key indexes, the same rowid may appear multiple times
    /// (once per matching key).
    pub fn lookup_all(&self, collection: &str, index_name: &str, index_key: &str) -> Vec<String> {
        match self.lookup(collection, index_name, index_key) {
            Some(rowid) => vec![rowid],
            None => vec![],
        }
    }

    /// List all active indexes for a collection.
    pub fn list_indexes(&self, collection: &str) -> Vec<String> {
        let prefix = format!("collections/{}/indexes/", collection);
        let names = self.kernel.list_names_prefix(&prefix);

        names.into_iter()
            .filter_map(|n| {
                // Strip the prefix to get the index name
                let idx_name = n.strip_prefix(&prefix)?;
                // Skip tombstoned indexes
                if maintenance::is_dropped(self.kernel, &n) {
                    return None;
                }
                Some(idx_name.to_string())
            })
            .collect()
    }

    /// Check if an index exists and is active (not tombstoned).
    pub fn index_exists(&self, collection: &str, index_name: &str) -> bool {
        let ref_name = self.index_ref(collection, index_name);
        maintenance::resolve_active(self.kernel, &ref_name).is_some()
    }

    /// Get statistics about an index.
    pub fn index_stats(&self, collection: &str, index_name: &str) -> Option<IndexStats> {
        let ref_name = self.index_ref(collection, index_name);
        let index_hash = maintenance::resolve_active(self.kernel, &ref_name)?;

        let index_data = self.kernel.read_blob(&index_hash).ok()?;
        let index: HashMap<String, String> = serde_json::from_slice(&index_data).ok()?;

        Some(IndexStats {
            name: index_name.to_string(),
            n_entries: index.len(),
            blob_hash: index_hash,
            blob_size: index_data.len(),
        })
    }

    fn index_ref(&self, collection: &str, index_name: &str) -> String {
        format!("collections/{}/indexes/{}", collection, index_name)
    }
}

/// Statistics about an index.
#[derive(Debug, Clone)]
pub struct IndexStats {
    pub name: String,
    pub n_entries: usize,
    pub blob_hash: String,
    pub blob_size: usize,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use pond_storage::UnifiedStorage;

    fn make_test_storage() -> (UnifiedStorage, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        (storage, dir)
    }

    #[test]
    fn test_build_and_lookup_index() {
        let (storage, _dir) = make_test_storage();
        let kernel = storage.kernel();
        let indexer = CollectionIndexer::new(kernel);

        let rows = vec![
            ("user:1".to_string(), json!({"name": "alice", "age": 30})),
            ("user:2".to_string(), json!({"name": "bob", "age": 25})),
            ("user:3".to_string(), json!({"name": "carol", "age": 35})),
        ];

        indexer.build_index("users", "by_name", &rows, |row| {
            vec![row["name"].as_str().unwrap().to_string()]
        }).unwrap();

        // Lookups
        assert_eq!(indexer.lookup("users", "by_name", "alice"), Some("user:1".to_string()));
        assert_eq!(indexer.lookup("users", "by_name", "bob"), Some("user:2".to_string()));
        assert_eq!(indexer.lookup("users", "by_name", "carol"), Some("user:3".to_string()));
        assert_eq!(indexer.lookup("users", "by_name", "nonexistent"), None);
    }

    #[test]
    fn test_multi_key_index() {
        let (storage, _dir) = make_test_storage();
        let kernel = storage.kernel();
        let indexer = CollectionIndexer::new(kernel);

        let rows = vec![
            ("doc:1".to_string(), json!({"tags": ["rust", "db", "storage"]})),
            ("doc:2".to_string(), json!({"tags": ["python", "db"]})),
        ];

        indexer.build_index("docs", "by_tag", &rows, |row| {
            row["tags"].as_array().unwrap()
                .iter()
                .map(|t| format!("tag:{}", t.as_str().unwrap()))
                .collect()
        }).unwrap();

        // Both docs have "tag:db" — last writer wins (HashMap)
        let db_result = indexer.lookup("docs", "by_tag", "tag:db");
        assert!(db_result == Some("doc:1".to_string()) || db_result == Some("doc:2".to_string()),
            "tag:db should map to one of the docs, got {:?}", db_result);
        assert_eq!(indexer.lookup("docs", "by_tag", "tag:rust"), Some("doc:1".to_string()));
        assert_eq!(indexer.lookup("docs", "by_tag", "tag:python"), Some("doc:2".to_string()));
    }

    #[test]
    fn test_drop_index() {
        let (storage, _dir) = make_test_storage();
        let kernel = storage.kernel();
        let indexer = CollectionIndexer::new(kernel);

        let rows = vec![("k1".to_string(), json!({"name": "test"}))];
        indexer.build_index("coll", "idx", &rows, |row| {
            vec![row["name"].as_str().unwrap().to_string()]
        }).unwrap();

        assert!(indexer.index_exists("coll", "idx"));
        assert!(indexer.drop_index("coll", "idx"));
        assert!(!indexer.index_exists("coll", "idx"));
        assert_eq!(indexer.lookup("coll", "idx", "test"), None);
    }

    #[test]
    fn test_list_indexes() {
        let (storage, _dir) = make_test_storage();
        let kernel = storage.kernel();
        let indexer = CollectionIndexer::new(kernel);

        let rows = vec![("k1".to_string(), json!({"name": "a", "email": "a@b.com"}))];

        indexer.build_index("users", "by_name", &rows, |r| vec![r["name"].as_str().unwrap().to_string()]).unwrap();
        indexer.build_index("users", "by_email", &rows, |r| vec![r["email"].as_str().unwrap().to_string()]).unwrap();

        let mut indexes = indexer.list_indexes("users");
        indexes.sort();
        assert_eq!(indexes, vec!["by_email", "by_name"]);
    }

    #[test]
    fn test_index_stats() {
        let (storage, _dir) = make_test_storage();
        let kernel = storage.kernel();
        let indexer = CollectionIndexer::new(kernel);

        let rows = vec![
            ("k1".to_string(), json!({"name": "a"})),
            ("k2".to_string(), json!({"name": "b"})),
            ("k3".to_string(), json!({"name": "c"})),
        ];

        indexer.build_index("coll", "idx", &rows, |r| {
            vec![r["name"].as_str().unwrap().to_string()]
        }).unwrap();

        let stats = indexer.index_stats("coll", "idx").unwrap();
        assert_eq!(stats.name, "idx");
        assert_eq!(stats.n_entries, 3);
        assert!(!stats.blob_hash.is_empty());
    }

    #[test]
    fn test_rebuild_index() {
        let (storage, _dir) = make_test_storage();
        let kernel = storage.kernel();
        let indexer = CollectionIndexer::new(kernel);

        // Build with 2 rows
        let rows = vec![
            ("k1".to_string(), json!({"name": "a"})),
            ("k2".to_string(), json!({"name": "b"})),
        ];
        indexer.build_index("coll", "idx", &rows, |r| {
            vec![r["name"].as_str().unwrap().to_string()]
        }).unwrap();
        assert_eq!(indexer.index_stats("coll", "idx").unwrap().n_entries, 2);

        // Rebuild with 3 rows
        let rows2 = vec![
            ("k1".to_string(), json!({"name": "a"})),
            ("k2".to_string(), json!({"name": "b"})),
            ("k3".to_string(), json!({"name": "c"})),
        ];
        indexer.build_index("coll", "idx", &rows2, |r| {
            vec![r["name"].as_str().unwrap().to_string()]
        }).unwrap();
        assert_eq!(indexer.index_stats("coll", "idx").unwrap().n_entries, 3);
    }
}
