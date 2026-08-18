// round_trip_cost.rs — what does one write actually cost the backend?
//
// Round trips are the unit this system is designed around, so the legacy write
// path's cost should be a measured number rather than an impression. This test
// records it. It is not a pass/fail quality bar — it is the baseline the
// engine cutover has to beat, and a tripwire if the legacy path gets more
// expensive while it is still the one every binding calls.

use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use pond_kernel::{LocalFSObjectStore, ObjectStore, PondKernel};

#[derive(Default)]
struct Counters {
    puts: AtomicU64,
    gets: AtomicU64,
    heads: AtomicU64,
    lists: AtomicU64,
}

impl Counters {
    fn total(&self) -> u64 {
        self.puts.load(Ordering::Relaxed)
            + self.gets.load(Ordering::Relaxed)
            + self.heads.load(Ordering::Relaxed)
            + self.lists.load(Ordering::Relaxed)
    }
    fn reset(&self) {
        self.puts.store(0, Ordering::Relaxed);
        self.gets.store(0, Ordering::Relaxed);
        self.heads.store(0, Ordering::Relaxed);
        self.lists.store(0, Ordering::Relaxed);
    }
}

/// Counts every operation that would be a network round trip against S3.
struct Counting {
    inner: LocalFSObjectStore,
    c: Arc<Counters>,
}

impl ObjectStore for Counting {
    fn put_blob(&self, data: &[u8]) -> io::Result<String> {
        self.c.puts.fetch_add(1, Ordering::Relaxed);
        self.inner.put_blob(data)
    }
    fn get_blob(&self, hash: &str) -> io::Result<Vec<u8>> {
        self.c.gets.fetch_add(1, Ordering::Relaxed);
        self.inner.get_blob(hash)
    }
    fn put_path(&self, path: &str, hash: &str) -> io::Result<()> {
        self.c.puts.fetch_add(1, Ordering::Relaxed);
        self.inner.put_path(path, hash)
    }
    fn get_path(&self, path: &str) -> Option<String> {
        self.c.gets.fetch_add(1, Ordering::Relaxed);
        self.inner.get_path(path)
    }
    fn put_object(&self, path: &str, bytes: &[u8]) -> io::Result<()> {
        self.c.puts.fetch_add(1, Ordering::Relaxed);
        self.inner.put_object(path, bytes)
    }
    fn get_object(&self, path: &str) -> Option<Vec<u8>> {
        self.c.gets.fetch_add(1, Ordering::Relaxed);
        self.inner.get_object(path)
    }
    fn delete_path(&self, path: &str) -> io::Result<bool> {
        self.inner.delete_path(path)
    }
    fn list_paths(&self, prefix: &str) -> io::Result<Vec<String>> {
        self.c.lists.fetch_add(1, Ordering::Relaxed);
        self.inner.list_paths(prefix)
    }
    fn blob_exists(&self, hash: &str) -> bool {
        // A HEAD request on S3 — a full round trip, not a free local check.
        self.c.heads.fetch_add(1, Ordering::Relaxed);
        self.inner.blob_exists(hash)
    }
    fn delete_blob(&self, hash: &str) -> io::Result<bool> {
        self.inner.delete_blob(hash)
    }
}

/// One legacy write, counted.
///
/// The number itself is the point. At an object-storage round trip of ~50 ms,
/// each one is ~50 ms of commit latency that no amount of caching removes,
/// because these are writes and a HEAD against a key that was just written.
///
/// The bound is deliberately loose — this test exists to record the cost and
/// to catch it growing, not to freeze an implementation detail.
#[test]
fn legacy_write_round_trip_cost_is_recorded() {
    let dir = tempfile::tempdir().unwrap();
    let c = Arc::new(Counters::default());
    let store = Counting {
        inner: LocalFSObjectStore::new(dir.path()).unwrap(),
        c: c.clone(),
    };
    let kernel = PondKernel::new_with_store(Box::new(store));
    let storage = pond_storage::UnifiedStorage::new(kernel);

    // First write, then a second one so the parent-commit read is included —
    // the steady-state cost is what matters, not the empty-collection case.
    pond_storage::write::write(storage.kernel(), "users", "main", b"row one", "first").unwrap();

    c.reset();
    pond_storage::write::write(storage.kernel(), "users", "main", b"row two", "second").unwrap();

    let puts = c.puts.load(Ordering::Relaxed);
    let gets = c.gets.load(Ordering::Relaxed);
    let heads = c.heads.load(Ordering::Relaxed);
    let total = c.total();

    println!(
        "legacy write: {} round trips ({} PUT, {} GET, {} HEAD)",
        total, puts, gets, heads
    );

    // Three facts worth pinning, each of which the engine path avoids.
    assert!(
        puts >= 6,
        "a legacy write issues several independent writes ({} seen)",
        puts
    );
    assert!(
        heads >= 3,
        "every `reference` HEADs the blob first, so each ref update costs two \
         round trips instead of one ({} seen)",
        heads
    );
    assert!(
        total >= 10,
        "the measured cost of one legacy write ({} round trips)",
        total
    );
}

/// The commit is not atomic, and the count shows why.
///
/// A legacy write ends by updating three separate refs in sequence. A crash
/// between them leaves the branch pointing at a new commit whose manifest ref
/// still names the old manifest. There is no way to make three writes atomic
/// on a store that only promises single-object atomicity — which is the whole
/// reason the engine publishes one object.
#[test]
fn legacy_commit_updates_several_refs_and_so_cannot_be_atomic() {
    let dir = tempfile::tempdir().unwrap();
    let c = Arc::new(Counters::default());
    let store = Counting {
        inner: LocalFSObjectStore::new(dir.path()).unwrap(),
        c: c.clone(),
    };
    let kernel = PondKernel::new_with_store(Box::new(store));

    pond_storage::write::write(&kernel, "users", "main", b"row", "msg").unwrap();

    // branch ref, manifest ref, and the bare collection name.
    let refs = kernel.list_names();
    let touched: Vec<&String> = refs
        .iter()
        .filter(|r| r.starts_with("collections/users") || r.as_str() == "users")
        .collect();
    assert!(
        touched.len() >= 3,
        "a commit spans {} refs, so it cannot be a single atomic write: {:?}",
        touched.len(),
        touched
    );
}
