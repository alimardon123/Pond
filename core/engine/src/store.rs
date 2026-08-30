// store.rs — bridges an ObjectStore to the index's NodeStore.
//
// The index needs two operations, `put(bytes) -> hash` and `get(hash)`, which
// is a strict subset of what every backend provides. This adapter is where
// those meet, and it is deliberately thin: no policy, no caching of its own
// (the BlobCache underneath handles that).
//
// # The one thing the signature cannot express
//
// `NodeStore::get` returns `Option<Vec<u8>>`, which cannot tell "this node is
// not there" apart from "asking for it failed". A transient 500, a dropped
// connection, an expired credential — every one of them arrives here as
// `None`, and a traversal that treats `None` as "empty subtree" then returns
// *fewer rows* and calls it success. One failed GET on a 20,000-row collection
// returned `Ok` with 18,293 rows. A read error had become a wrong answer, and
// nothing above could tell.
//
// That is the worst failure mode a storage system has. The write path already
// refuses it — `put` panics rather than let a failed write become a hash that
// references nothing — and the read path was quietly doing the opposite.
//
// So failures are counted here. The traversal still gets its `Option`, because
// changing the index's trait to `Result` would push error handling through
// every descent; instead a reader samples the counter before and after an
// operation and refuses to return a result that a failed read passed through.
// Missing is still missing; failed is now failed.

use std::io;
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
    /// Reads that failed at the backend, as opposed to finding nothing.
    read_failures: AtomicU64,
}

impl<S: ObjectStore> EngineStore<S> {
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            gets: AtomicU64::new(0),
            puts: AtomicU64::new(0),
            read_failures: AtomicU64::new(0),
        }
    }

    /// How many node reads have failed at the backend since this store was
    /// created. Monotonic, so a caller samples it either side of an operation
    /// rather than resetting it — resetting would race with any other thread
    /// sharing the store.
    pub fn read_failures(&self) -> u64 {
        self.read_failures.load(Ordering::Relaxed)
    }

    /// Turn a failure that happened during an operation into an error.
    ///
    /// `before` is the value [`read_failures`](Self::read_failures) had when
    /// the operation started. If it has moved, some node the traversal walked
    /// past could not be read, so whatever the traversal produced is missing
    /// rows and must not be returned as a success.
    pub fn failure_since(&self, before: u64) -> Option<io::Error> {
        let now = self.read_failures();
        if now > before {
            Some(io::Error::other(format!(
                "{} node read(s) failed during this operation; the result would \
                 have been silently short",
                now - before
            )))
        } else {
            None
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
        match self.inner.get_blob(hash) {
            Ok(bytes) => Some(bytes),
            Err(_) => {
                // Recorded rather than swallowed. See the module comment: the
                // caller checks this counter and refuses a short result.
                self.read_failures.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
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
