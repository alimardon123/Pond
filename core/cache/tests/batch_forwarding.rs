// Does the cache preserve the backend's batch operations, or silently
// serialise them?
//
// `ObjectStore`'s batch methods have sequential default implementations so
// that every backend is correct immediately. That default is a trap for any
// decorator: if it does not forward the batch method, the default runs against
// the *decorator*, calling the singular method N times, and the backend's
// parallel implementation never executes. Request counts are unchanged, so
// nothing in the suite notices — only wall clock moves, by a factor of the
// batch width.

use std::sync::atomic::{AtomicU64, Ordering};

use pond_cache::{BlobCache, CacheConfig};
use pond_kernel::{LocalFSObjectStore, ObjectStore};

/// A backend whose batch methods are distinguishable from its singular ones.
struct BatchAware {
    inner: LocalFSObjectStore,
    batch_calls: AtomicU64,
    single_calls: AtomicU64,
}

impl BatchAware {
    fn new(dir: &std::path::Path) -> Self {
        Self {
            inner: LocalFSObjectStore::new(dir).unwrap(),
            batch_calls: AtomicU64::new(0),
            single_calls: AtomicU64::new(0),
        }
    }
}

impl ObjectStore for BatchAware {
    fn put_blob(&self, d: &[u8]) -> std::io::Result<String> {
        self.single_calls.fetch_add(1, Ordering::Relaxed);
        self.inner.put_blob(d)
    }
    fn get_blob(&self, h: &str) -> std::io::Result<Vec<u8>> {
        self.single_calls.fetch_add(1, Ordering::Relaxed);
        self.inner.get_blob(h)
    }
    fn put_blob_batch(&self, items: &[Vec<u8>]) -> std::io::Result<Vec<String>> {
        self.batch_calls.fetch_add(1, Ordering::Relaxed);
        self.inner.put_blob_batch(items)
    }
    fn get_blob_batch(&self, hashes: &[String]) -> std::io::Result<Vec<Vec<u8>>> {
        self.batch_calls.fetch_add(1, Ordering::Relaxed);
        self.inner.get_blob_batch(hashes)
    }
    fn delete_blob_batch(&self, hashes: &[String]) -> std::io::Result<usize> {
        self.batch_calls.fetch_add(1, Ordering::Relaxed);
        self.inner.delete_blob_batch(hashes)
    }
    fn put_path(&self, p: &str, h: &str) -> std::io::Result<()> {
        self.inner.put_path(p, h)
    }
    fn get_path(&self, p: &str) -> Option<String> {
        self.inner.get_path(p)
    }
    fn put_object(&self, p: &str, b: &[u8]) -> std::io::Result<()> {
        self.inner.put_object(p, b)
    }
    fn get_object(&self, p: &str) -> Option<Vec<u8>> {
        self.inner.get_object(p)
    }
    fn delete_path(&self, p: &str) -> std::io::Result<bool> {
        self.inner.delete_path(p)
    }
    fn list_paths(&self, p: &str) -> std::io::Result<Vec<String>> {
        self.inner.list_paths(p)
    }
    fn blob_exists(&self, h: &str) -> bool {
        self.inner.blob_exists(h)
    }
    fn delete_blob(&self, h: &str) -> std::io::Result<bool> {
        self.single_calls.fetch_add(1, Ordering::Relaxed);
        self.inner.delete_blob(h)
    }
}

fn cache(dir: &std::path::Path) -> BlobCache<BatchAware> {
    BlobCache::new(BatchAware::new(dir), CacheConfig::memory_only(0)).unwrap()
}

#[test]
fn put_blob_batch_reaches_the_backend_as_one_batch() {
    let d = tempfile::tempdir().unwrap();
    let c = cache(d.path());
    let items: Vec<Vec<u8>> = (0..32u8).map(|i| vec![i; 64]).collect();

    c.put_blob_batch(&items).unwrap();

    assert_eq!(
        c.inner().batch_calls.load(Ordering::Relaxed),
        1,
        "one batch of 32 must reach the backend as one call"
    );
    assert_eq!(
        c.inner().single_calls.load(Ordering::Relaxed),
        0,
        "it must not be unrolled into 32 singular puts"
    );
}

#[test]
fn get_blob_batch_reaches_the_backend_as_one_batch() {
    let d = tempfile::tempdir().unwrap();
    let c = cache(d.path());
    let items: Vec<Vec<u8>> = (0..32u8).map(|i| vec![i; 64]).collect();
    let hashes = c.put_blob_batch(&items).unwrap();

    let before_single = c.inner().single_calls.load(Ordering::Relaxed);
    let before_batch = c.inner().batch_calls.load(Ordering::Relaxed);
    c.get_blob_batch(&hashes).unwrap();

    assert_eq!(
        c.inner().batch_calls.load(Ordering::Relaxed) - before_batch,
        1,
        "a 32-wide read must reach the backend as one batch"
    );
    assert_eq!(
        c.inner().single_calls.load(Ordering::Relaxed) - before_single,
        0,
        "it must not be unrolled into 32 singular gets"
    );
}

#[test]
fn delete_blob_batch_reaches_the_backend_as_one_batch() {
    let d = tempfile::tempdir().unwrap();
    let c = cache(d.path());
    let items: Vec<Vec<u8>> = (0..32u8).map(|i| vec![i; 64]).collect();
    let hashes = c.put_blob_batch(&items).unwrap();

    let before = c.inner().single_calls.load(Ordering::Relaxed);
    c.delete_blob_batch(&hashes).unwrap();

    assert_eq!(
        c.inner().single_calls.load(Ordering::Relaxed) - before,
        0,
        "bulk delete must not become 32 individual DELETEs"
    );
}

/// The property that makes forwarding worth more than a one-line delegation:
/// a batch where some entries are already warm must fetch only the cold ones.
///
/// Delegating straight to the inner store would re-fetch everything; the
/// trait default would fetch the cold ones one at a time. Both are wrong in
/// the case that matters most, because a long-lived reader's batches are
/// mostly warm.
#[test]
fn a_half_warm_batch_fetches_only_the_cold_half() {
    let d = tempfile::tempdir().unwrap();
    // A real memory budget, so the first reads actually warm the cache.
    let c = BlobCache::new(
        BatchAware::new(d.path()),
        CacheConfig::memory_only(1 << 20),
    )
    .unwrap();

    let items: Vec<Vec<u8>> = (0..32u8).map(|i| vec![i; 64]).collect();
    // Written through the inner store, so the write does not warm the cache
    // and "cold" means cold. (`put_blob_batch` on the cache admits what it
    // writes, which is right in production and useless for this test.)
    let hashes = c.inner().put_blob_batch(&items).unwrap();

    // Warm the first half only.
    let warm: Vec<String> = hashes[..16].to_vec();
    c.get_blob_batch(&warm).unwrap();

    let before_batch = c.inner().batch_calls.load(Ordering::Relaxed);
    let before_single = c.inner().single_calls.load(Ordering::Relaxed);
    let all = c.get_blob_batch(&hashes).unwrap();

    assert_eq!(all, items, "a partially warm batch must still be correct");
    assert_eq!(
        c.inner().single_calls.load(Ordering::Relaxed) - before_single,
        0,
        "the cold half must not be fetched one at a time"
    );
    assert_eq!(
        c.inner().batch_calls.load(Ordering::Relaxed) - before_batch,
        1,
        "exactly one backend batch, carrying only the cold half"
    );
}

/// A fully warm batch costs no backend call at all.
#[test]
fn a_fully_warm_batch_touches_the_backend_zero_times() {
    let d = tempfile::tempdir().unwrap();
    let c = BlobCache::new(
        BatchAware::new(d.path()),
        CacheConfig::memory_only(1 << 20),
    )
    .unwrap();

    let items: Vec<Vec<u8>> = (0..32u8).map(|i| vec![i; 64]).collect();
    // Here the write *should* warm the cache — that is the property.
    let hashes = c.put_blob_batch(&items).unwrap();

    let before_batch = c.inner().batch_calls.load(Ordering::Relaxed);
    let before_single = c.inner().single_calls.load(Ordering::Relaxed);
    let all = c.get_blob_batch(&hashes).unwrap();

    assert_eq!(all, items);
    assert_eq!(
        c.inner().batch_calls.load(Ordering::Relaxed) - before_batch,
        0,
        "a warm batch must issue no backend request"
    );
    assert_eq!(
        c.inner().single_calls.load(Ordering::Relaxed) - before_single,
        0
    );
}

/// Exercises the disk tier inside a batch read.
///
/// This branch promotes a disk hit into memory, so it takes the memory lock
/// while the surrounding lookup may still hold it. Every other test here is
/// memory-only, which means none of them reach this path — the deadlock it
/// once contained was found by clippy, not by the suite, and this test is what
/// makes the suite able to find it next time.
#[test]
fn a_batch_read_served_from_disk_completes_and_is_correct() {
    let backing = tempfile::tempdir().unwrap();
    let disk = tempfile::tempdir().unwrap();
    let items: Vec<Vec<u8>> = (0..32u8).map(|i| vec![i; 64]).collect();

    // Write and warm the disk tier, then drop the cache so memory is empty.
    let hashes = {
        let c = BlobCache::new(
            BatchAware::new(backing.path()),
            CacheConfig::memory_only(1 << 20).with_disk(disk.path(), 1 << 20),
        )
        .unwrap();
        c.put_blob_batch(&items).unwrap()
    };

    // A fresh cache over the same disk directory: memory cold, disk warm.
    let c = BlobCache::new(
        BatchAware::new(backing.path()),
        CacheConfig::memory_only(1 << 20).with_disk(disk.path(), 1 << 20),
    )
    .unwrap();

    let all = c.get_blob_batch(&hashes).unwrap();

    assert_eq!(all, items, "a disk-served batch must be correct");
    assert_eq!(
        c.stats().disk_hits,
        32,
        "all 32 should have come from the disk tier"
    );
    assert_eq!(
        c.inner().batch_calls.load(Ordering::Relaxed)
            + c.inner().single_calls.load(Ordering::Relaxed),
        0,
        "a disk-warm batch must not touch the backend"
    );
}
