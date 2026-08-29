// metered.rs — what a run actually cost the object store, and a check that
// decorators do not quietly destroy batching.
//
// # Why this belongs in the kernel
//
// Round trips are the objective function of this whole design. Every cost
// claim in the tree is stated in them, and every one of those claims was
// measured by a counting `ObjectStore` wrapper hand-written inside the test
// file that needed it — eight of them at last count, each about fifty lines of
// delegation, each free to drift from the trait it wraps. That is a lot of
// duplicated boilerplate to get at the one number the system is designed
// around, and it is not available at all outside tests: an operator running
// Pond has no way to see what their workload is costing them.
//
// [`Metered`] is that wrapper, once, in the place both tests and production
// can reach.
//
// # Requests and round trips are not the same number
//
// A batch of 32 GETs is thirty-two billable requests and one round trip,
// because the backend issues them in parallel. S3 charges for the former;
// latency is the latter. Conflating them is easy — and every hand-rolled
// wrapper did, because none of them forwarded the batch methods, so a batch
// unrolled into 32 sequential singular calls and the two numbers coincided.
//
// So both are counted. `gets`/`puts`/`deletes`/`lists` are what the bill is
// computed from; `round_trips` is what the wall clock is computed from. When
// those two diverge, batching is working; when they track each other, it is
// not.
//
// # The forwarding trap
//
// `ObjectStore`'s batch methods have sequential default implementations, so a
// new backend is correct as soon as it implements the four required
// operations. For a *decorator* that default is a trap: it calls the singular
// method on `self`, so a decorator that forgets to forward a batch method
// unrolls it into N calls against itself and the backend's parallel
// implementation never runs. Nothing observable changes except wall clock,
// which no request-count assertion can see.
//
// [`assert_forwards_batches`] checks this directly, by asking the backend
// which method it received. It is exported rather than kept private because
// the property belongs to every decorator over `ObjectStore`, not just this
// one.

use std::io;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::object_store::ObjectStore;

/// What a workload cost the backing store.
///
/// All counters are monotonic since the last [`Metered::reset`].
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct StoreStats {
    /// Objects read. Billable requests, not round trips.
    pub gets: u64,
    /// Objects written.
    pub puts: u64,
    /// Objects deleted.
    pub deletes: u64,
    /// List operations. Priced as a PUT on most object stores, not as a GET.
    pub lists: u64,
    /// Existence checks — a HEAD on S3.
    ///
    /// Counted apart from `gets` even though it is billed as one, because
    /// "this costs zero HEADs" is a property the design asserts in several
    /// places: a kernel that remembers what it just wrote should never ask the
    /// store whether that write landed.
    pub heads: u64,
    /// Dependent latency steps. A batch of any width counts once, because its
    /// members are issued in parallel and do not wait on each other.
    ///
    /// This is the number to optimise. Requests are what you are billed for;
    /// round trips are what you wait for, and they cannot be parallelised away
    /// once one depends on the result of the last.
    pub round_trips: u64,
    /// Bytes sent to the store.
    pub bytes_written: u64,
    /// Bytes received from the store.
    pub bytes_read: u64,
}

impl StoreStats {
    /// Total billable requests.
    pub fn requests(&self) -> u64 {
        self.gets + self.puts + self.deletes + self.lists + self.heads
    }

    /// Mean requests per round trip — how wide the batching is actually
    /// running. 1.0 means every request waited for the one before it.
    pub fn batch_width(&self) -> f64 {
        if self.round_trips == 0 {
            0.0
        } else {
            self.requests() as f64 / self.round_trips as f64
        }
    }

    /// Modelled wall clock, given a per-round-trip latency and a throughput.
    ///
    /// Deliberately takes both terms. Counting requests alone prices a leaf
    /// rewritten whole as one cheap PUT however large it is, which is
    /// backwards once the bytes have to cross a network.
    pub fn modelled_millis(&self, latency_ms: f64, ms_per_mib: f64) -> f64 {
        let mib = (self.bytes_written + self.bytes_read) as f64 / (1024.0 * 1024.0);
        self.round_trips as f64 * latency_ms + mib * ms_per_mib
    }
}

/// Counts what passes through to the store beneath it, changing nothing else.
///
/// Every method is forwarded explicitly, including the batch methods that have
/// sequential defaults — see the module comment for why that matters.
pub struct Metered<S: ObjectStore> {
    inner: S,
    gets: AtomicU64,
    puts: AtomicU64,
    deletes: AtomicU64,
    lists: AtomicU64,
    heads: AtomicU64,
    round_trips: AtomicU64,
    bytes_written: AtomicU64,
    bytes_read: AtomicU64,
}

impl<S: ObjectStore> Metered<S> {
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            gets: AtomicU64::new(0),
            puts: AtomicU64::new(0),
            deletes: AtomicU64::new(0),
            lists: AtomicU64::new(0),
            heads: AtomicU64::new(0),
            round_trips: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            bytes_read: AtomicU64::new(0),
        }
    }

    pub fn stats(&self) -> StoreStats {
        StoreStats {
            gets: self.gets.load(Ordering::Relaxed),
            puts: self.puts.load(Ordering::Relaxed),
            deletes: self.deletes.load(Ordering::Relaxed),
            lists: self.lists.load(Ordering::Relaxed),
            heads: self.heads.load(Ordering::Relaxed),
            round_trips: self.round_trips.load(Ordering::Relaxed),
            bytes_written: self.bytes_written.load(Ordering::Relaxed),
            bytes_read: self.bytes_read.load(Ordering::Relaxed),
        }
    }

    /// Zero every counter. Useful to exclude setup from a measurement.
    pub fn reset(&self) {
        for c in [
            &self.gets,
            &self.puts,
            &self.deletes,
            &self.lists,
            &self.heads,
            &self.round_trips,
            &self.bytes_written,
            &self.bytes_read,
        ] {
            c.store(0, Ordering::Relaxed);
        }
    }

    pub fn inner(&self) -> &S {
        &self.inner
    }

    fn trip(&self) {
        self.round_trips.fetch_add(1, Ordering::Relaxed);
    }

    /// A round trip for a batch, which an empty batch is not.
    ///
    /// A batch method called with nothing in it issues no request and waits
    /// for no response — the backend's implementations return immediately.
    /// Counting it anyway put `round_trips` above `requests`, which is not a
    /// state the world can be in, and inflated every measured figure in the
    /// tree by however many empty batches the caller happened to make. The
    /// round-trip profile showed `open` costing three waits for two requests
    /// before this was fixed.
    fn trip_batch(&self, width: usize) {
        if width > 0 {
            self.trip();
        }
    }
}

impl<S: ObjectStore> ObjectStore for Metered<S> {
    fn put_blob(&self, data: &[u8]) -> io::Result<String> {
        self.puts.fetch_add(1, Ordering::Relaxed);
        self.bytes_written
            .fetch_add(data.len() as u64, Ordering::Relaxed);
        self.trip();
        self.inner.put_blob(data)
    }

    fn get_blob(&self, hash: &str) -> io::Result<Vec<u8>> {
        self.gets.fetch_add(1, Ordering::Relaxed);
        self.trip();
        let data = self.inner.get_blob(hash)?;
        self.bytes_read
            .fetch_add(data.len() as u64, Ordering::Relaxed);
        Ok(data)
    }

    fn get_blob_range(&self, hash: &str, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        self.gets.fetch_add(1, Ordering::Relaxed);
        self.trip();
        let data = self.inner.get_blob_range(hash, offset, len)?;
        self.bytes_read
            .fetch_add(data.len() as u64, Ordering::Relaxed);
        Ok(data)
    }

    fn put_blob_batch(&self, items: &[Vec<u8>]) -> io::Result<Vec<String>> {
        self.puts.fetch_add(items.len() as u64, Ordering::Relaxed);
        self.bytes_written.fetch_add(
            items.iter().map(|i| i.len() as u64).sum::<u64>(),
            Ordering::Relaxed,
        );
        self.trip_batch(items.len());
        self.inner.put_blob_batch(items)
    }

    fn get_blob_batch(&self, hashes: &[String]) -> io::Result<Vec<Vec<u8>>> {
        self.gets.fetch_add(hashes.len() as u64, Ordering::Relaxed);
        self.trip_batch(hashes.len());
        let out = self.inner.get_blob_batch(hashes)?;
        self.bytes_read.fetch_add(
            out.iter().map(|b| b.len() as u64).sum::<u64>(),
            Ordering::Relaxed,
        );
        Ok(out)
    }

    fn put_path(&self, path: &str, hash: &str) -> io::Result<()> {
        self.puts.fetch_add(1, Ordering::Relaxed);
        self.trip();
        self.inner.put_path(path, hash)
    }

    fn get_path(&self, path: &str) -> Option<String> {
        self.gets.fetch_add(1, Ordering::Relaxed);
        self.trip();
        self.inner.get_path(path)
    }

    fn put_object(&self, path: &str, bytes: &[u8]) -> io::Result<()> {
        self.puts.fetch_add(1, Ordering::Relaxed);
        self.bytes_written
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        self.trip();
        self.inner.put_object(path, bytes)
    }

    fn get_object(&self, path: &str) -> Option<Vec<u8>> {
        self.gets.fetch_add(1, Ordering::Relaxed);
        self.trip();
        let out = self.inner.get_object(path);
        if let Some(b) = &out {
            self.bytes_read.fetch_add(b.len() as u64, Ordering::Relaxed);
        }
        out
    }

    fn get_object_batch(&self, paths: &[String]) -> Vec<Option<Vec<u8>>> {
        self.gets.fetch_add(paths.len() as u64, Ordering::Relaxed);
        self.trip_batch(paths.len());
        let out = self.inner.get_object_batch(paths);
        self.bytes_read.fetch_add(
            out.iter().flatten().map(|b| b.len() as u64).sum::<u64>(),
            Ordering::Relaxed,
        );
        out
    }

    fn delete_path(&self, path: &str) -> io::Result<bool> {
        self.deletes.fetch_add(1, Ordering::Relaxed);
        self.trip();
        self.inner.delete_path(path)
    }

    fn delete_path_batch(&self, paths: &[String]) -> io::Result<usize> {
        self.deletes
            .fetch_add(paths.len() as u64, Ordering::Relaxed);
        self.trip_batch(paths.len());
        self.inner.delete_path_batch(paths)
    }

    fn list_paths(&self, prefix: &str) -> io::Result<Vec<String>> {
        self.lists.fetch_add(1, Ordering::Relaxed);
        self.trip();
        self.inner.list_paths(prefix)
    }

    fn blob_exists(&self, hash: &str) -> bool {
        self.heads.fetch_add(1, Ordering::Relaxed);
        self.trip();
        self.inner.blob_exists(hash)
    }

    fn delete_blob(&self, hash: &str) -> io::Result<bool> {
        self.deletes.fetch_add(1, Ordering::Relaxed);
        self.trip();
        self.inner.delete_blob(hash)
    }

    fn delete_blob_batch(&self, hashes: &[String]) -> io::Result<usize> {
        self.deletes
            .fetch_add(hashes.len() as u64, Ordering::Relaxed);
        self.trip_batch(hashes.len());
        self.inner.delete_blob_batch(hashes)
    }
}

/// Call counts from a [`BatchProbe`], shared so they stay readable after the
/// probe is moved inside the decorator under test.
#[derive(Debug, Default)]
pub struct ProbeCounts {
    batched: AtomicU64,
    unbatched: AtomicU64,
}

impl ProbeCounts {
    /// Batch methods entered.
    pub fn batched(&self) -> u64 {
        self.batched.load(Ordering::Relaxed)
    }

    /// Singular methods entered — the ones a lost batch turns into.
    pub fn unbatched(&self) -> u64 {
        self.unbatched.load(Ordering::Relaxed)
    }

    pub fn reset(&self) {
        self.batched.store(0, Ordering::Relaxed);
        self.unbatched.store(0, Ordering::Relaxed);
    }
}

/// A backend that reports which of its methods a decorator actually called.
///
/// Exists so [`assert_forwards_batches`] can distinguish "one batch arrived"
/// from "the batch was unrolled into N singular calls" — a difference no
/// request count can see, because both produce the same count.
pub struct BatchProbe<S: ObjectStore> {
    inner: S,
    counts: std::sync::Arc<ProbeCounts>,
}

impl<S: ObjectStore> BatchProbe<S> {
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            counts: std::sync::Arc::new(ProbeCounts::default()),
        }
    }

    /// A handle to the counters that outlives moving the probe into a
    /// decorator — which is the whole point, since a decorator gives no way
    /// to reach back into what it wrapped.
    pub fn counts(&self) -> std::sync::Arc<ProbeCounts> {
        std::sync::Arc::clone(&self.counts)
    }
}

impl<S: ObjectStore> ObjectStore for BatchProbe<S> {
    fn put_blob(&self, d: &[u8]) -> io::Result<String> {
        self.counts.unbatched.fetch_add(1, Ordering::Relaxed);
        self.inner.put_blob(d)
    }
    fn get_blob(&self, h: &str) -> io::Result<Vec<u8>> {
        self.counts.unbatched.fetch_add(1, Ordering::Relaxed);
        self.inner.get_blob(h)
    }
    fn delete_blob(&self, h: &str) -> io::Result<bool> {
        self.counts.unbatched.fetch_add(1, Ordering::Relaxed);
        self.inner.delete_blob(h)
    }
    fn get_object(&self, p: &str) -> Option<Vec<u8>> {
        self.counts.unbatched.fetch_add(1, Ordering::Relaxed);
        self.inner.get_object(p)
    }
    fn delete_path(&self, p: &str) -> io::Result<bool> {
        self.counts.unbatched.fetch_add(1, Ordering::Relaxed);
        self.inner.delete_path(p)
    }

    fn put_blob_batch(&self, items: &[Vec<u8>]) -> io::Result<Vec<String>> {
        self.counts.batched.fetch_add(1, Ordering::Relaxed);
        self.inner.put_blob_batch(items)
    }
    fn get_blob_batch(&self, hashes: &[String]) -> io::Result<Vec<Vec<u8>>> {
        self.counts.batched.fetch_add(1, Ordering::Relaxed);
        self.inner.get_blob_batch(hashes)
    }
    fn get_object_batch(&self, paths: &[String]) -> Vec<Option<Vec<u8>>> {
        self.counts.batched.fetch_add(1, Ordering::Relaxed);
        self.inner.get_object_batch(paths)
    }
    fn delete_path_batch(&self, paths: &[String]) -> io::Result<usize> {
        self.counts.batched.fetch_add(1, Ordering::Relaxed);
        self.inner.delete_path_batch(paths)
    }
    fn delete_blob_batch(&self, hashes: &[String]) -> io::Result<usize> {
        self.counts.batched.fetch_add(1, Ordering::Relaxed);
        self.inner.delete_blob_batch(hashes)
    }

    fn put_path(&self, p: &str, h: &str) -> io::Result<()> {
        self.inner.put_path(p, h)
    }
    fn get_path(&self, p: &str) -> Option<String> {
        self.inner.get_path(p)
    }
    fn put_object(&self, p: &str, b: &[u8]) -> io::Result<()> {
        self.inner.put_object(p, b)
    }
    fn list_paths(&self, p: &str) -> io::Result<Vec<String>> {
        self.inner.list_paths(p)
    }
    fn blob_exists(&self, h: &str) -> bool {
        self.inner.blob_exists(h)
    }
}

/// Assert that a decorator passes every batch operation through as a batch.
///
/// Call it with a closure that wraps the given [`BatchProbe`] in the decorator
/// under test:
///
/// ```
/// # use pond_kernel::{assert_forwards_batches, BatchProbe, LocalFSObjectStore};
/// # let dir = tempfile::tempdir().unwrap();
/// # struct Passthrough<S>(S);
/// # impl<S: pond_kernel::ObjectStore> pond_kernel::ObjectStore for Passthrough<S> {
/// #   fn put_blob(&self, d: &[u8]) -> std::io::Result<String> { self.0.put_blob(d) }
/// #   fn get_blob(&self, h: &str) -> std::io::Result<Vec<u8>> { self.0.get_blob(h) }
/// #   fn put_blob_batch(&self, i: &[Vec<u8>]) -> std::io::Result<Vec<String>> { self.0.put_blob_batch(i) }
/// #   fn get_blob_batch(&self, h: &[String]) -> std::io::Result<Vec<Vec<u8>>> { self.0.get_blob_batch(h) }
/// #   fn delete_blob_batch(&self, h: &[String]) -> std::io::Result<usize> { self.0.delete_blob_batch(h) }
/// #   fn get_object_batch(&self, p: &[String]) -> Vec<Option<Vec<u8>>> { self.0.get_object_batch(p) }
/// #   fn delete_path_batch(&self, p: &[String]) -> std::io::Result<usize> { self.0.delete_path_batch(p) }
/// #   fn put_path(&self, p: &str, h: &str) -> std::io::Result<()> { self.0.put_path(p, h) }
/// #   fn get_path(&self, p: &str) -> Option<String> { self.0.get_path(p) }
/// #   fn put_object(&self, p: &str, b: &[u8]) -> std::io::Result<()> { self.0.put_object(p, b) }
/// #   fn get_object(&self, p: &str) -> Option<Vec<u8>> { self.0.get_object(p) }
/// #   fn delete_path(&self, p: &str) -> std::io::Result<bool> { self.0.delete_path(p) }
/// #   fn list_paths(&self, p: &str) -> std::io::Result<Vec<String>> { self.0.list_paths(p) }
/// #   fn blob_exists(&self, h: &str) -> bool { self.0.blob_exists(h) }
/// #   fn delete_blob(&self, h: &str) -> std::io::Result<bool> { self.0.delete_blob(h) }
/// # }
/// let store = LocalFSObjectStore::new(dir.path()).unwrap();
/// assert_forwards_batches(|probe| Passthrough(probe), store);
/// ```
///
/// # Panics
///
/// With the name of the first batch operation the decorator failed to forward.
pub fn assert_forwards_batches<S, D, F>(wrap: F, backend: S)
where
    S: ObjectStore,
    D: ObjectStore,
    F: FnOnce(BatchProbe<S>) -> D,
{
    let probe = BatchProbe::new(backend);
    let counts = probe.counts();
    let decorated = wrap(probe);

    /// Runs one batch operation and insists the backend saw a batch and no
    /// singular calls. Named per operation, so a failure says which one.
    macro_rules! check {
        ($name:literal, $body:expr) => {{
            counts.reset();
            let out = $body;
            assert_eq!(
                counts.unbatched(),
                0,
                "{} was unrolled into {} singular backend calls — the \
                 decorator does not forward it, so the backend's parallel \
                 implementation never runs",
                $name,
                counts.unbatched()
            );
            assert_eq!(
                counts.batched(),
                1,
                "{} reached the backend {} times; it must arrive exactly once, \
                 as one batch",
                $name,
                counts.batched()
            );
            out
        }};
    }

    let items: Vec<Vec<u8>> = (0..8u8).map(|i| vec![i; 32]).collect();
    let hashes = check!("put_blob_batch", {
        decorated
            .put_blob_batch(&items)
            .expect("put_blob_batch failed")
    });
    assert_eq!(hashes.len(), items.len(), "put_blob_batch lost entries");

    let read = check!("get_blob_batch", {
        decorated
            .get_blob_batch(&hashes)
            .expect("get_blob_batch failed")
    });
    assert_eq!(read, items, "get_blob_batch returned the wrong bytes");

    let paths: Vec<String> = (0..8).map(|i| format!("probe/p{}", i)).collect();
    for (p, h) in paths.iter().zip(&hashes) {
        decorated.put_path(p, h).expect("put_path failed");
    }

    let fetched = check!("get_object_batch", decorated.get_object_batch(&paths));
    assert_eq!(fetched.len(), paths.len(), "get_object_batch lost entries");

    check!("delete_path_batch", {
        decorated
            .delete_path_batch(&paths)
            .expect("delete_path_batch failed")
    });
    check!("delete_blob_batch", {
        decorated
            .delete_blob_batch(&hashes)
            .expect("delete_blob_batch failed")
    });
}

/// Assert that a backend's `list_paths` matches a *prefix*, not a directory.
///
/// The trait specifies a prefix listing and S3 implements one. The local
/// backend used to list a directory, so `list_paths("heads/writer-00")`
/// returned matches on one backend and nothing on the other — a semantic that
/// changes with the backend, which is precisely what this system refuses
/// elsewhere (it is why there is no conditional write in the trait).
///
/// The divergence stayed invisible for as long as every caller happened to
/// pass a prefix ending in `/`. This makes the contract checkable instead of
/// conventional, for any backend, including one reached over the network.
///
/// # Panics
///
/// Describing which case the store got wrong.
pub fn assert_list_paths_is_a_prefix_listing<S: ObjectStore>(store: &S, scope: &str) {
    let scope = scope.trim_end_matches('/');
    let hash = store.put_blob(b"conformance").expect("put_blob");

    let keys = [
        format!("{}/alpha-one", scope),
        format!("{}/alpha-two", scope),
        format!("{}/beta-one", scope),
        format!("{}/nested/alpha-three", scope),
    ];
    for k in &keys {
        store.put_path(k, &hash).expect("put_path");
    }

    let listed = |prefix: &str| -> Vec<String> {
        let mut v = store.list_paths(prefix).expect("list_paths");
        v.retain(|p| p.starts_with(scope));
        v.sort();
        v
    };

    // A directory prefix returns everything beneath it, recursively.
    assert_eq!(
        listed(&format!("{}/", scope)).len(),
        4,
        "a directory prefix must return every key beneath it, nested included"
    );

    // A partial name is a prefix, not a directory. This is the case that
    // diverged.
    let partial = listed(&format!("{}/alpha-", scope));
    assert_eq!(
        partial,
        vec![keys[0].clone(), keys[1].clone()],
        "a partial name must match as a string prefix; got {:?}",
        partial
    );

    // Including a partial name that is also a directory's prefix.
    let nest = listed(&format!("{}/nest", scope));
    assert_eq!(
        nest,
        vec![keys[3].clone()],
        "a prefix of a directory name must match the keys under it; got {:?}",
        nest
    );

    // A prefix matching nothing returns nothing rather than erroring.
    assert!(listed(&format!("{}/zzz", scope)).is_empty());

    // And a prefix must not match a key it is merely a substring of.
    let beta = listed(&format!("{}/beta", scope));
    assert_eq!(beta, vec![keys[2].clone()]);

    for k in &keys {
        let _ = store.delete_path(k);
    }
    let _ = store.delete_blob(&hash);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::LocalFSObjectStore;

    /// A decorator that forwards everything. The check must accept it.
    struct Good<S: ObjectStore>(S);

    /// A decorator that forgets the batch methods — the mistake this exists to
    /// catch, and the one `BlobCache` actually made.
    struct Forgetful<S: ObjectStore>(S);

    impl<S: ObjectStore> ObjectStore for Good<S> {
        fn put_blob(&self, d: &[u8]) -> io::Result<String> {
            self.0.put_blob(d)
        }
        fn get_blob(&self, h: &str) -> io::Result<Vec<u8>> {
            self.0.get_blob(h)
        }
        fn put_path(&self, p: &str, h: &str) -> io::Result<()> {
            self.0.put_path(p, h)
        }
        fn get_path(&self, p: &str) -> Option<String> {
            self.0.get_path(p)
        }
        fn put_object(&self, p: &str, b: &[u8]) -> io::Result<()> {
            self.0.put_object(p, b)
        }
        fn get_object(&self, p: &str) -> Option<Vec<u8>> {
            self.0.get_object(p)
        }
        fn delete_path(&self, p: &str) -> io::Result<bool> {
            self.0.delete_path(p)
        }
        fn list_paths(&self, p: &str) -> io::Result<Vec<String>> {
            self.0.list_paths(p)
        }
        fn blob_exists(&self, h: &str) -> bool {
            self.0.blob_exists(h)
        }
        fn delete_blob(&self, h: &str) -> io::Result<bool> {
            self.0.delete_blob(h)
        }
        fn put_blob_batch(&self, i: &[Vec<u8>]) -> io::Result<Vec<String>> {
            self.0.put_blob_batch(i)
        }
        fn get_blob_batch(&self, h: &[String]) -> io::Result<Vec<Vec<u8>>> {
            self.0.get_blob_batch(h)
        }
        fn get_object_batch(&self, p: &[String]) -> Vec<Option<Vec<u8>>> {
            self.0.get_object_batch(p)
        }
        fn delete_path_batch(&self, p: &[String]) -> io::Result<usize> {
            self.0.delete_path_batch(p)
        }
        fn delete_blob_batch(&self, h: &[String]) -> io::Result<usize> {
            self.0.delete_blob_batch(h)
        }
    }

    impl<S: ObjectStore> ObjectStore for Forgetful<S> {
        fn put_blob(&self, d: &[u8]) -> io::Result<String> {
            self.0.put_blob(d)
        }
        fn get_blob(&self, h: &str) -> io::Result<Vec<u8>> {
            self.0.get_blob(h)
        }
        fn put_path(&self, p: &str, h: &str) -> io::Result<()> {
            self.0.put_path(p, h)
        }
        fn get_path(&self, p: &str) -> Option<String> {
            self.0.get_path(p)
        }
        fn put_object(&self, p: &str, b: &[u8]) -> io::Result<()> {
            self.0.put_object(p, b)
        }
        fn get_object(&self, p: &str) -> Option<Vec<u8>> {
            self.0.get_object(p)
        }
        fn delete_path(&self, p: &str) -> io::Result<bool> {
            self.0.delete_path(p)
        }
        fn list_paths(&self, p: &str) -> io::Result<Vec<String>> {
            self.0.list_paths(p)
        }
        fn blob_exists(&self, h: &str) -> bool {
            self.0.blob_exists(h)
        }
        fn delete_blob(&self, h: &str) -> io::Result<bool> {
            self.0.delete_blob(h)
        }
    }

    fn backend() -> (tempfile::TempDir, LocalFSObjectStore) {
        let d = tempfile::tempdir().unwrap();
        let s = LocalFSObjectStore::new(d.path()).unwrap();
        (d, s)
    }

    #[test]
    fn the_check_accepts_a_decorator_that_forwards() {
        let (_d, s) = backend();
        assert_forwards_batches(Good, s);
    }

    /// The check has to fail on the real mistake, or it is decoration.
    #[test]
    #[should_panic(expected = "put_blob_batch was unrolled")]
    fn the_check_rejects_a_decorator_that_forgets() {
        let (_d, s) = backend();
        assert_forwards_batches(Forgetful, s);
    }

    #[test]
    fn metered_forwards_every_batch() {
        let (_d, s) = backend();
        assert_forwards_batches(Metered::new, s);
    }

    /// Requests and round trips are different numbers, and a batch is where
    /// they separate. If they ever track each other, batching is not running.
    #[test]
    fn a_batch_is_many_requests_and_one_round_trip() {
        let (_d, s) = backend();
        let m = Metered::new(s);
        let items: Vec<Vec<u8>> = (0..32u8).map(|i| vec![i; 64]).collect();

        let hashes = m.put_blob_batch(&items).unwrap();
        m.get_blob_batch(&hashes).unwrap();

        let st = m.stats();
        assert_eq!(st.puts, 32, "32 objects written is 32 billable PUTs");
        assert_eq!(st.gets, 32);
        assert_eq!(
            st.round_trips, 2,
            "but only two waits: one batched write, one batched read"
        );
        assert_eq!(st.batch_width(), 32.0);
        assert_eq!(st.bytes_written, 32 * 64);
        assert_eq!(st.bytes_read, 32 * 64);
    }

    #[test]
    fn singular_calls_are_one_request_each_and_one_round_trip_each() {
        let (_d, s) = backend();
        let m = Metered::new(s);
        for i in 0..8u8 {
            m.put_blob(&[i; 16]).unwrap();
        }
        let st = m.stats();
        assert_eq!(st.puts, 8);
        assert_eq!(
            st.round_trips, 8,
            "unbatched work pays a round trip per request"
        );
        assert_eq!(st.batch_width(), 1.0, "width 1.0 means nothing batched");
    }

    #[test]
    fn the_local_backend_lists_by_prefix_not_by_directory() {
        let (_d, s) = backend();
        assert_list_paths_is_a_prefix_listing(&s, "conformance");
    }

    /// The check has to fail on a directory-only listing, or it proves nothing.
    #[test]
    #[should_panic(expected = "must match as a string prefix")]
    fn the_check_rejects_a_directory_only_listing() {
        struct DirectoryOnly<S: ObjectStore>(S);
        impl<S: ObjectStore> ObjectStore for DirectoryOnly<S> {
            fn list_paths(&self, prefix: &str) -> io::Result<Vec<String>> {
                // What the local backend used to do: nothing unless the prefix
                // names a directory.
                if prefix.is_empty() || prefix.ends_with('/') {
                    self.0.list_paths(prefix)
                } else {
                    Ok(Vec::new())
                }
            }
            fn put_blob(&self, d: &[u8]) -> io::Result<String> {
                self.0.put_blob(d)
            }
            fn get_blob(&self, h: &str) -> io::Result<Vec<u8>> {
                self.0.get_blob(h)
            }
            fn put_path(&self, p: &str, h: &str) -> io::Result<()> {
                self.0.put_path(p, h)
            }
            fn get_path(&self, p: &str) -> Option<String> {
                self.0.get_path(p)
            }
            fn put_object(&self, p: &str, b: &[u8]) -> io::Result<()> {
                self.0.put_object(p, b)
            }
            fn get_object(&self, p: &str) -> Option<Vec<u8>> {
                self.0.get_object(p)
            }
            fn delete_path(&self, p: &str) -> io::Result<bool> {
                self.0.delete_path(p)
            }
            fn blob_exists(&self, h: &str) -> bool {
                self.0.blob_exists(h)
            }
            fn delete_blob(&self, h: &str) -> io::Result<bool> {
                self.0.delete_blob(h)
            }
        }

        let (_d, s) = backend();
        assert_list_paths_is_a_prefix_listing(&DirectoryOnly(s), "conformance");
    }

    #[test]
    fn reset_clears_setup_from_a_measurement() {
        let (_d, s) = backend();
        let m = Metered::new(s);
        m.put_blob(b"setup").unwrap();
        m.reset();
        assert_eq!(m.stats(), StoreStats::default());
    }

    /// A batch with nothing in it costs nothing.
    ///
    /// It issues no request and waits for no response, so counting it as a
    /// round trip put `round_trips` above `requests` — a state that cannot
    /// occur, since every wait is a wait for at least one request. The
    /// round-trip profile reported `open` as three waits for two requests
    /// until this was fixed, and every figure derived from a run that made an
    /// empty batch call was inflated by one wait per call.
    #[test]
    fn an_empty_batch_is_not_a_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let m = Metered::new(LocalFSObjectStore::new(dir.path()).unwrap());

        m.put_blob_batch(&[]).unwrap();
        m.get_blob_batch(&[]).unwrap();
        m.get_object_batch(&[]);
        m.delete_path_batch(&[]).unwrap();
        m.delete_blob_batch(&[]).unwrap();

        assert_eq!(m.stats(), StoreStats::default(), "empty batches cost nothing");
    }

    /// You cannot wait more times than you ask.
    ///
    /// The general form of the bug above: `round_trips` counts dependent
    /// waits and `requests()` counts the operations those waits are for, so
    /// the first can never exceed the second. Asserted over a mixture of
    /// singular and batch calls, empty batches included, because it was
    /// exactly the empty ones that broke it.
    #[test]
    fn waits_never_exceed_requests() {
        let dir = tempfile::tempdir().unwrap();
        let m = Metered::new(LocalFSObjectStore::new(dir.path()).unwrap());

        let h = m.put_blob(b"one").unwrap();
        m.get_blob(&h).unwrap();
        m.put_blob_batch(&[b"a".to_vec(), b"b".to_vec()]).unwrap();
        m.get_blob_batch(std::slice::from_ref(&h)).unwrap();
        m.get_blob_batch(&[]).unwrap();
        m.put_path("p", &h).unwrap();
        m.get_path("p");
        m.get_object_batch(&[]);
        m.list_paths("").unwrap();
        m.blob_exists(&h);
        m.delete_path_batch(&[]).unwrap();

        let s = m.stats();
        assert!(
            s.round_trips <= s.requests(),
            "{} waits for {} requests: a wait with no request in it",
            s.round_trips,
            s.requests()
        );
        assert!(s.batch_width() >= 1.0, "batch width below 1 is not a number a run can have");
    }
}
