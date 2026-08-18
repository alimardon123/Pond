// store.rs — content-addressed node storage.
//
// The index only needs two operations, and they are deliberately the same two
// every backend can provide: write immutable bytes addressed by their hash,
// and read them back. No conditional writes (object-store only), no append
// (object-store only), no rename (filesystem only). That is what lets the same
// index work unchanged on local disk, S3, R2, GCS, or memory.
//
// `MemStore` additionally counts reads and writes, because the metric that
// decides whether this design works is *how many round trips an operation
// costs*. Tests assert on those counters directly — see the constant-depth
// test in `tree.rs`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use sha2::{Digest, Sha256};

/// Content hash of a node, as lowercase hex.
pub type Hash = String;

/// Compute the content address of a node.
pub fn hash_bytes(data: &[u8]) -> Hash {
    let mut hasher = Sha256::new();
    hasher.update(data);
    let out = hasher.finalize();
    let mut hex = String::with_capacity(64);
    for b in out.iter() {
        use std::fmt::Write;
        let _ = write!(hex, "{:02x}", b);
    }
    hex
}

/// Where index nodes live.
///
/// Implementations must be content-addressed: `put` returns the hash of the
/// bytes, and writing the same bytes twice is a no-op. That property is what
/// makes caching safe without any invalidation logic, and what makes two
/// writers who compute the same node share it for free.
pub trait NodeStore {
    fn put(&self, bytes: Vec<u8>) -> Hash;
    fn get(&self, hash: &str) -> Option<Vec<u8>>;

    /// Write many nodes, returning their hashes in the same order.
    ///
    /// Building a tree produces every node of a level before any of the next
    /// level is known, so a level can be written all at once. On object
    /// storage that is the difference between one PUT round trip per node and
    /// one per level: at ~100 ms per PUT, a 2000-node index goes from minutes
    /// to seconds.
    ///
    /// The default is the obvious sequential loop, so every store is correct
    /// without implementing this; backends that can issue requests in
    /// parallel override it.
    fn put_batch(&self, items: Vec<Vec<u8>>) -> Vec<Hash> {
        items.into_iter().map(|b| self.put(b)).collect()
    }
}

/// In-memory store with I/O counters, for tests and measurement.
#[derive(Default)]
pub struct MemStore {
    nodes: Mutex<HashMap<Hash, Vec<u8>>>,
    reads: AtomicU64,
    writes: AtomicU64,
    /// Writes that were deduplicated because the node already existed. This is
    /// the direct measure of how much two versions of a tree share.
    deduped_writes: AtomicU64,
    bytes_written: AtomicU64,
}

impl MemStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of `get` calls — the round-trip count an object store would see.
    pub fn reads(&self) -> u64 {
        self.reads.load(Ordering::Relaxed)
    }

    /// Number of *new* nodes written (excludes dedup hits).
    pub fn writes(&self) -> u64 {
        self.writes.load(Ordering::Relaxed)
    }

    pub fn deduped_writes(&self) -> u64 {
        self.deduped_writes.load(Ordering::Relaxed)
    }

    pub fn bytes_written(&self) -> u64 {
        self.bytes_written.load(Ordering::Relaxed)
    }

    /// Distinct nodes currently stored.
    pub fn len(&self) -> usize {
        self.nodes.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn reset_counters(&self) {
        self.reads.store(0, Ordering::Relaxed);
        self.writes.store(0, Ordering::Relaxed);
        self.deduped_writes.store(0, Ordering::Relaxed);
        self.bytes_written.store(0, Ordering::Relaxed);
    }
}

impl NodeStore for MemStore {
    fn put(&self, bytes: Vec<u8>) -> Hash {
        let h = hash_bytes(&bytes);
        let mut nodes = self.nodes.lock().unwrap();
        if nodes.contains_key(&h) {
            self.deduped_writes.fetch_add(1, Ordering::Relaxed);
        } else {
            self.writes.fetch_add(1, Ordering::Relaxed);
            self.bytes_written
                .fetch_add(bytes.len() as u64, Ordering::Relaxed);
            nodes.insert(h.clone(), bytes);
        }
        h
    }

    fn get(&self, hash: &str) -> Option<Vec<u8>> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.nodes.lock().unwrap().get(hash).cloned()
    }
}

/// A store that serves a bounded set of hashes from memory without counting a
/// read, modelling the cache tier: the upper levels of the index are tiny,
/// immutable, and content-addressed, so they stay resident and never need
/// invalidation. Reads that miss the cache fall through and are counted.
pub struct CachingStore<'a, S: NodeStore> {
    inner: &'a S,
    cache: Mutex<HashMap<Hash, Vec<u8>>>,
    hits: AtomicU64,
}

impl<'a, S: NodeStore> CachingStore<'a, S> {
    pub fn new(inner: &'a S) -> Self {
        Self {
            inner,
            cache: Mutex::new(HashMap::new()),
            hits: AtomicU64::new(0),
        }
    }

    /// Pull a node into the cache (used to model a warm upper index).
    pub fn warm(&self, hash: &str) {
        if let Some(bytes) = self.inner.get(hash) {
            self.cache.lock().unwrap().insert(hash.to_string(), bytes);
        }
    }

    pub fn hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    pub fn cached_nodes(&self) -> usize {
        self.cache.lock().unwrap().len()
    }
}

impl<S: NodeStore> NodeStore for CachingStore<'_, S> {
    fn put(&self, bytes: Vec<u8>) -> Hash {
        self.inner.put(bytes)
    }

    fn get(&self, hash: &str) -> Option<Vec<u8>> {
        if let Some(bytes) = self.cache.lock().unwrap().get(hash) {
            self.hits.fetch_add(1, Ordering::Relaxed);
            return Some(bytes.clone());
        }
        self.inner.get(hash)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_content_addressed_dedup() {
        let s = MemStore::new();
        let h1 = s.put(b"same".to_vec());
        let h2 = s.put(b"same".to_vec());
        assert_eq!(h1, h2);
        assert_eq!(s.writes(), 1, "identical bytes must be stored once");
        assert_eq!(s.deduped_writes(), 1);
        assert_eq!(s.len(), 1);
    }

    #[test]
    fn test_roundtrip_and_read_counter() {
        let s = MemStore::new();
        let h = s.put(b"payload".to_vec());
        assert_eq!(s.reads(), 0);
        assert_eq!(s.get(&h).unwrap(), b"payload");
        assert_eq!(s.reads(), 1);
        assert!(s.get("missing").is_none());
        assert_eq!(s.reads(), 2, "misses count as round trips too");
    }

    #[test]
    fn test_cache_absorbs_reads() {
        let s = MemStore::new();
        let h = s.put(b"hot node".to_vec());
        let cached = CachingStore::new(&s);
        cached.warm(&h);
        s.reset_counters();

        for _ in 0..10 {
            assert_eq!(cached.get(&h).unwrap(), b"hot node");
        }
        assert_eq!(s.reads(), 0, "cached reads must not hit the store");
        assert_eq!(cached.hits(), 10);
    }
}
