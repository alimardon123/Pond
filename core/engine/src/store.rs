// store.rs — bridges an ObjectStore to the index's NodeStore.
//
// The index needs two operations, `put(bytes) -> hash` and `get(hash)`, which
// is a strict subset of what every backend provides. This adapter is where
// those meet, and it is deliberately thin: no policy, no caching of its own
// (the BlobCache underneath handles that), no error swallowing beyond what the
// NodeStore signature forces.

use std::sync::atomic::{AtomicU64, Ordering};

use pond_index::{Hash, NodeStore};
use pond_kernel::ObjectStore;

/// Adapts any [`ObjectStore`] to the index's [`NodeStore`], counting requests.
///
/// The counters are not instrumentation for its own sake: round trips are the
/// unit this whole design is optimised for, so tests assert on them directly
/// rather than on timings, which vary with the network.
pub struct EngineStore<S: ObjectStore> {
    inner: S,
    gets: AtomicU64,
    puts: AtomicU64,
}

impl<S: ObjectStore> EngineStore<S> {
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            gets: AtomicU64::new(0),
            puts: AtomicU64::new(0),
        }
    }

    pub fn inner(&self) -> &S {
        &self.inner
    }

    /// Node reads issued. A cache hit underneath still counts here — this
    /// measures index traversal, not backend traffic.
    pub fn gets(&self) -> u64 {
        self.gets.load(Ordering::Relaxed)
    }

    pub fn puts(&self) -> u64 {
        self.puts.load(Ordering::Relaxed)
    }

    pub fn reset_counters(&self) {
        self.gets.store(0, Ordering::Relaxed);
        self.puts.store(0, Ordering::Relaxed);
    }
}

impl<S: ObjectStore> NodeStore for EngineStore<S> {
    fn put(&self, bytes: Vec<u8>) -> String {
        self.puts.fetch_add(1, Ordering::Relaxed);
        // A failed node write must not be silently swallowed into a wrong
        // hash: the index would then reference a node that does not exist and
        // the tree would be unreadable. Panicking here is deliberate — this is
        // an unrecoverable storage failure, and the alternative is corruption.
        self.inner
            .put_blob(&bytes)
            .expect("node write failed — storage is unavailable or full")
    }

    fn get(&self, hash: &str) -> Option<Vec<u8>> {
        self.gets.fetch_add(1, Ordering::Relaxed);
        self.inner.get_blob(hash).ok()
    }

    /// Routes to the backend's batch API, which S3 implements with parallel
    /// requests. This is what keeps bulk load from being bounded by round-trip
    /// latency — the single biggest cost in building a large index.
    fn put_batch(&self, items: Vec<Vec<u8>>) -> Vec<Hash> {
        if items.is_empty() {
            return Vec::new();
        }
        self.puts.fetch_add(items.len() as u64, Ordering::Relaxed);
        self.inner
            .put_blob_batch(&items)
            .expect("batch node write failed — storage is unavailable or full")
    }
}
