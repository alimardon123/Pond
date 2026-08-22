// pond_cache — a read-through cache for content-addressed blobs.
//
// # Why this is simple, and why that is the point
//
// Caching is normally the hardest thing in a storage system, because the hard
// part is not storing data — it is knowing when what you stored became wrong.
// Every cache over mutable objects needs invalidation, versioning, TTLs, or a
// coherence protocol, and those are where the bugs live.
//
// Pond's blobs are content-addressed: the key *is* the hash of the bytes. A
// given hash can only ever name one sequence of bytes, so a cached entry can
// never become stale. That removes invalidation entirely — not "makes it
// easier", removes it. Consequences:
//
//   - No TTL on data. An entry is valid until evicted for space.
//   - No coherence protocol. Two processes, two machines, two tenants can
//     share one disk cache safely, because agreement is guaranteed by the
//     addressing scheme rather than negotiated.
//   - Cross-version sharing is free. Two branches, or two commits, that share
//     a subtree share its cache entries with no bookkeeping.
//   - Entries are self-verifying: re-hashing a cached blob detects bit rot or
//     a truncated write, which is why `verify_on_read` exists.
//
// Only *refs* are mutable, and refs are deliberately not cached here — they
// belong to a separate layer with a short TTL, so the mutable and immutable
// halves never get confused with one another.
//
// # Tiers
//
//     memory  (bytes, hot)          ~µs      bounded by max_memory_bytes
//       ↓ miss
//     local disk (NVMe)             ~100µs   bounded by max_disk_bytes
//       ↓ miss
//     backing store (S3 / FS)       ~20-60ms
//
// A hit in either tier issues zero backend requests, which is the metric that
// matters: against object storage, cost and latency are both dominated by
// round trips, not bytes.

use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use pond_kernel::{hash_bytes, ObjectStore};

/// Cache hit/miss counters, so callers can see round trips actually avoided.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CacheStats {
    pub memory_hits: u64,
    pub disk_hits: u64,
    pub misses: u64,
    /// Entries dropped because a tier was at capacity.
    pub evictions: u64,
    /// Cached bytes that failed re-hashing and were discarded.
    pub corrupt_entries: u64,
}

impl CacheStats {
    pub fn hits(&self) -> u64 {
        self.memory_hits + self.disk_hits
    }

    /// Fraction of reads served without touching the backing store.
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits() + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits() as f64 / total as f64
        }
    }
}

/// Cache sizing and behaviour.
#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// In-process byte budget.
    pub max_memory_bytes: u64,
    /// On-disk byte budget. Zero disables the disk tier.
    pub max_disk_bytes: u64,
    /// Directory for the disk tier. None disables it.
    pub disk_dir: Option<PathBuf>,
    /// Re-hash cached bytes on read and discard them if they do not match.
    ///
    /// Content addressing makes this check nearly free to implement and it is
    /// the only way to notice a corrupted cache file, so it defaults on for
    /// the disk tier. The memory tier never needs it.
    pub verify_on_read: bool,
    /// Skip the memory tier for blobs above this size, so one large object
    /// cannot evict the whole working set.
    pub max_memory_entry_bytes: u64,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            max_memory_bytes: 256 * 1024 * 1024,
            max_disk_bytes: 8 * 1024 * 1024 * 1024,
            disk_dir: None,
            verify_on_read: true,
            max_memory_entry_bytes: 8 * 1024 * 1024,
        }
    }
}

impl CacheConfig {
    pub fn memory_only(max_memory_bytes: u64) -> Self {
        Self {
            max_memory_bytes,
            max_disk_bytes: 0,
            disk_dir: None,
            ..Default::default()
        }
    }

    pub fn with_disk(mut self, dir: impl AsRef<Path>, max_disk_bytes: u64) -> Self {
        self.disk_dir = Some(dir.as_ref().to_path_buf());
        self.max_disk_bytes = max_disk_bytes;
        self
    }
}

/// Memory tier. Eviction is CLOCK-like: a cheap approximation of LRU that
/// needs no ordering structure, which keeps the hot path to one lock and one
/// hash lookup.
struct MemoryTier {
    entries: HashMap<String, Vec<u8>>,
    /// Insertion order, used as the eviction queue.
    order: Vec<String>,
    bytes: u64,
    budget: u64,
}

impl MemoryTier {
    fn new(budget: u64) -> Self {
        Self {
            entries: HashMap::new(),
            order: Vec::new(),
            bytes: 0,
            budget,
        }
    }

    fn get(&self, hash: &str) -> Option<Vec<u8>> {
        self.entries.get(hash).cloned()
    }

    /// Insert, evicting oldest-first until the new entry fits. Returns the
    /// number of entries evicted.
    fn insert(&mut self, hash: &str, data: &[u8]) -> u64 {
        if self.budget == 0 || data.len() as u64 > self.budget {
            return 0;
        }
        if self.entries.contains_key(hash) {
            return 0;
        }
        let mut evicted = 0;
        while self.bytes + data.len() as u64 > self.budget && !self.order.is_empty() {
            let oldest = self.order.remove(0);
            if let Some(gone) = self.entries.remove(&oldest) {
                self.bytes -= gone.len() as u64;
                evicted += 1;
            }
        }
        self.entries.insert(hash.to_string(), data.to_vec());
        self.order.push(hash.to_string());
        self.bytes += data.len() as u64;
        evicted
    }
}

/// A read-through cache in front of any [`ObjectStore`].
///
/// Implements `ObjectStore` itself, so it drops in wherever a store is
/// expected — including under `PondKernel` — without any caller changes.
pub struct BlobCache<S: ObjectStore> {
    inner: S,
    memory: Mutex<MemoryTier>,
    config: CacheConfig,
    memory_hits: AtomicU64,
    disk_hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
    corrupt_entries: AtomicU64,
    disk_bytes: AtomicU64,
}

impl<S: ObjectStore> BlobCache<S> {
    pub fn new(inner: S, config: CacheConfig) -> io::Result<Self> {
        if let Some(dir) = &config.disk_dir {
            std::fs::create_dir_all(dir)?;
        }
        Ok(Self {
            memory: Mutex::new(MemoryTier::new(config.max_memory_bytes)),
            inner,
            config,
            memory_hits: AtomicU64::new(0),
            disk_hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            corrupt_entries: AtomicU64::new(0),
            disk_bytes: AtomicU64::new(0),
        })
    }

    pub fn stats(&self) -> CacheStats {
        CacheStats {
            memory_hits: self.memory_hits.load(Ordering::Relaxed),
            disk_hits: self.disk_hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            corrupt_entries: self.corrupt_entries.load(Ordering::Relaxed),
        }
    }

    /// The store this cache fronts.
    pub fn inner(&self) -> &S {
        &self.inner
    }

    /// Pull a blob into the cache without returning it.
    ///
    /// Useful for warming the upper levels of an index on startup or after a
    /// branch checkout: they are small, immutable, and read on every lookup,
    /// so keeping them resident is what turns a 3-round-trip cold lookup into
    /// a 1-round-trip warm one.
    pub fn warm(&self, hash: &str) -> io::Result<()> {
        self.get_blob(hash).map(|_| ())
    }

    fn disk_path(&self, hash: &str) -> Option<PathBuf> {
        let dir = self.config.disk_dir.as_ref()?;
        if hash.len() < 2 || !hash.chars().all(|c| c.is_ascii_hexdigit()) {
            // Only content hashes are cacheable — never let a caller-supplied
            // string become a path component.
            return None;
        }
        Some(dir.join(&hash[..2]).join(hash))
    }

    fn read_disk(&self, hash: &str) -> Option<Vec<u8>> {
        let path = self.disk_path(hash)?;
        let data = std::fs::read(&path).ok()?;
        if self.config.verify_on_read && hash_bytes(&data) != hash {
            // A truncated write or bit rot. Content addressing makes this
            // detectable for free; drop the entry and fall through to the
            // backing store rather than serving wrong bytes.
            self.corrupt_entries.fetch_add(1, Ordering::Relaxed);
            let _ = std::fs::remove_file(&path);
            return None;
        }
        Some(data)
    }

    fn write_disk(&self, hash: &str, data: &[u8]) {
        if self.config.max_disk_bytes == 0 {
            return;
        }
        let Some(path) = self.disk_path(hash) else {
            return;
        };
        if path.exists() {
            return; // immutable: already correct
        }
        if self.disk_bytes.load(Ordering::Relaxed) + data.len() as u64
            > self.config.max_disk_bytes
        {
            // Budget reached. Eviction is intentionally not implemented here:
            // a cache is allowed to simply stop admitting entries, and a wrong
            // eviction policy is worse than none. Reclaiming is a maintenance
            // task, not a hot-path decision.
            self.evictions.fetch_add(1, Ordering::Relaxed);
            return;
        }
        if let Some(parent) = path.parent() {
            if std::fs::create_dir_all(parent).is_err() {
                return;
            }
        }
        // Temp + rename so a concurrent reader never sees a partial file.
        // Cache writes are best-effort: a failure here must never fail a read.
        let tmp = path.with_extension(format!("tmp.{}", std::process::id()));
        if std::fs::write(&tmp, data).is_ok() && std::fs::rename(&tmp, &path).is_ok() {
            self.disk_bytes
                .fetch_add(data.len() as u64, Ordering::Relaxed);
        } else {
            let _ = std::fs::remove_file(&tmp);
        }
    }

    fn admit(&self, hash: &str, data: &[u8]) {
        if data.len() as u64 <= self.config.max_memory_entry_bytes {
            let evicted = self.memory.lock().unwrap().insert(hash, data);
            if evicted > 0 {
                self.evictions.fetch_add(evicted, Ordering::Relaxed);
            }
        }
        self.write_disk(hash, data);
    }
}

impl<S: ObjectStore> ObjectStore for BlobCache<S> {
    /// Writes go straight through and are admitted to the cache, since the
    /// bytes just written are the bytes that will be read back — a write is
    /// the cheapest possible warm.
    fn put_blob(&self, data: &[u8]) -> io::Result<String> {
        let hash = self.inner.put_blob(data)?;
        self.admit(&hash, data);
        Ok(hash)
    }

    /// Forwarded, not left to the trait's sequential default.
    ///
    /// That default calls `self.put_blob` N times — on `self`, the cache —
    /// so the backend's parallel batch never runs and a 32-wide level write
    /// becomes 32 dependent round trips. Request counts are identical either
    /// way, which is why no cost assertion in the suite catches it; only wall
    /// clock moves, by the width of the batch. Every decorator over
    /// `ObjectStore` has to forward these three explicitly.
    fn put_blob_batch(&self, items: &[Vec<u8>]) -> io::Result<Vec<String>> {
        let hashes = self.inner.put_blob_batch(items)?;
        for (hash, data) in hashes.iter().zip(items) {
            self.admit(hash, data);
        }
        Ok(hashes)
    }

    /// Serves what is cached and fetches only the misses, in one batch.
    ///
    /// The partial-hit case is the reason this cannot simply forward: a batch
    /// where half the entries are warm should cost one request for the other
    /// half, not one for all of them and not N for the misses.
    fn get_blob_batch(&self, hashes: &[String]) -> io::Result<Vec<Vec<u8>>> {
        let mut out: Vec<Option<Vec<u8>>> = Vec::with_capacity(hashes.len());
        let mut missing = Vec::new();
        let mut missing_at = Vec::new();

        for (i, hash) in hashes.iter().enumerate() {
            // Scoped, not chained into the `if let`: an `if let` holds its
            // guard for the whole block, and the disk branch below locks
            // `memory` again to promote what it found — which on a
            // non-reentrant mutex is a deadlock. It is reachable only when a
            // disk tier is configured and the entry is on it, so no
            // memory-only test would ever hit it.
            let cached = self.memory.lock().unwrap().get(hash);
            if let Some(data) = cached {
                self.memory_hits.fetch_add(1, Ordering::Relaxed);
                out.push(Some(data));
            } else if let Some(data) = self.read_disk(hash) {
                self.disk_hits.fetch_add(1, Ordering::Relaxed);
                if data.len() as u64 <= self.config.max_memory_entry_bytes {
                    self.memory.lock().unwrap().insert(hash, &data);
                }
                out.push(Some(data));
            } else {
                self.misses.fetch_add(1, Ordering::Relaxed);
                out.push(None);
                missing_at.push(i);
                missing.push(hash.clone());
            }
        }

        if !missing.is_empty() {
            let fetched = self.inner.get_blob_batch(&missing)?;
            // A short result would leave slots unfilled, and filling them with
            // empty bytes would hand the caller a silently wrong answer for a
            // blob that exists. Refuse instead.
            if fetched.len() != missing.len() {
                return Err(io::Error::other(format!(
                    "backend returned {} blobs for a batch of {}",
                    fetched.len(),
                    missing.len()
                )));
            }
            for (slot, data) in missing_at.into_iter().zip(fetched) {
                self.admit(&hashes[slot], &data);
                out[slot] = Some(data);
            }
        }

        out.into_iter()
            .map(|d| d.ok_or_else(|| io::Error::other("batch read left a hole")))
            .collect()
    }

    /// Purges every tier, then deletes in one backend call.
    ///
    /// On S3 that call is a single `DeleteObjects` carrying up to 1000 keys;
    /// unrolled it would be 1000 separate DELETEs, which is what GC would
    /// otherwise pay on every sweep.
    fn delete_blob_batch(&self, hashes: &[String]) -> io::Result<usize> {
        for hash in hashes {
            {
                let mut mem = self.memory.lock().unwrap();
                if let Some(gone) = mem.entries.remove(hash) {
                    mem.bytes -= gone.len() as u64;
                    mem.order.retain(|h| h != hash);
                }
            }
            if let Some(path) = self.disk_path(hash) {
                let _ = std::fs::remove_file(path);
            }
        }
        self.inner.delete_blob_batch(hashes)
    }

    fn get_blob(&self, hash: &str) -> io::Result<Vec<u8>> {
        if let Some(data) = self.memory.lock().unwrap().get(hash) {
            self.memory_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(data);
        }
        if let Some(data) = self.read_disk(hash) {
            self.disk_hits.fetch_add(1, Ordering::Relaxed);
            // Promote to memory so a repeat read costs nothing.
            if data.len() as u64 <= self.config.max_memory_entry_bytes {
                self.memory.lock().unwrap().insert(hash, &data);
            }
            return Ok(data);
        }
        self.misses.fetch_add(1, Ordering::Relaxed);
        let data = self.inner.get_blob(hash)?;
        self.admit(hash, &data);
        Ok(data)
    }

    /// Ranged reads are served from a cached whole blob when one is present,
    /// which is the common case once a segment is hot. On a miss the range
    /// goes to the backing store *as a range* — fetching the whole object to
    /// satisfy a small range would defeat the purpose of having ranged reads.
    fn get_blob_range(&self, hash: &str, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        if let Some(data) = self.memory.lock().unwrap().get(hash) {
            self.memory_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(pond_kernel::object_store::slice_range(&data, offset, len));
        }
        if let Some(data) = self.read_disk(hash) {
            self.disk_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(pond_kernel::object_store::slice_range(&data, offset, len));
        }
        self.misses.fetch_add(1, Ordering::Relaxed);
        self.inner.get_blob_range(hash, offset, len)
    }

    fn put_path(&self, path: &str, hash: &str) -> io::Result<()> {
        // Refs are mutable — deliberately never cached here.
        self.inner.put_path(path, hash)
    }

    fn get_path(&self, path: &str) -> Option<String> {
        self.inner.get_path(path)
    }

    /// Named bytes are mutable, so they pass straight through — the same
    /// reason refs are never cached. Caching here would need invalidation,
    /// which is precisely what content addressing exists to avoid.
    fn put_object(&self, path: &str, bytes: &[u8]) -> io::Result<()> {
        self.inner.put_object(path, bytes)
    }

    fn get_object(&self, path: &str) -> Option<Vec<u8>> {
        self.inner.get_object(path)
    }

    fn get_object_batch(&self, paths: &[String]) -> Vec<Option<Vec<u8>>> {
        self.inner.get_object_batch(paths)
    }

    fn delete_path(&self, path: &str) -> io::Result<bool> {
        self.inner.delete_path(path)
    }

    fn delete_path_batch(&self, paths: &[String]) -> io::Result<usize> {
        self.inner.delete_path_batch(paths)
    }

    fn list_paths(&self, prefix: &str) -> io::Result<Vec<String>> {
        self.inner.list_paths(prefix)
    }

    fn blob_exists(&self, hash: &str) -> bool {
        if self.memory.lock().unwrap().get(hash).is_some() {
            return true;
        }
        self.inner.blob_exists(hash)
    }

    /// Deleting a blob drops it from every tier. This is the one case where a
    /// content-addressed entry can become invalid — not because the bytes
    /// changed, but because they are meant to be gone.
    fn delete_blob(&self, hash: &str) -> io::Result<bool> {
        {
            let mut mem = self.memory.lock().unwrap();
            if let Some(gone) = mem.entries.remove(hash) {
                mem.bytes -= gone.len() as u64;
                mem.order.retain(|h| h != hash);
            }
        }
        if let Some(path) = self.disk_path(hash) {
            let _ = std::fs::remove_file(path);
        }
        self.inner.delete_blob(hash)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pond_kernel::LocalFSObjectStore;
    use std::sync::atomic::AtomicU64;

    /// A store that counts backend requests, so tests assert on round trips
    /// avoided rather than on timings.
    struct CountingStore {
        inner: LocalFSObjectStore,
        gets: AtomicU64,
        range_gets: AtomicU64,
    }

    impl CountingStore {
        fn new(dir: &Path) -> Self {
            Self {
                inner: LocalFSObjectStore::new(dir).unwrap(),
                gets: AtomicU64::new(0),
                range_gets: AtomicU64::new(0),
            }
        }
        fn gets(&self) -> u64 {
            self.gets.load(Ordering::Relaxed)
        }
        fn range_gets(&self) -> u64 {
            self.range_gets.load(Ordering::Relaxed)
        }
    }

    impl ObjectStore for CountingStore {
        // Forwarded explicitly. The trait's batch defaults call the singular
        // method on `self`, so a decorator that omits them unrolls every batch
        // against itself and the backend's parallel implementation never runs.
        fn put_blob_batch(&self, i: &[Vec<u8>]) -> io::Result<Vec<String>> {
            self.inner.put_blob_batch(i)
        }
        fn get_blob_batch(&self, h: &[String]) -> io::Result<Vec<Vec<u8>>> {
            self.inner.get_blob_batch(h)
        }
        fn get_object_batch(&self, p: &[String]) -> Vec<Option<Vec<u8>>> {
            self.inner.get_object_batch(p)
        }
        fn delete_path_batch(&self, p: &[String]) -> io::Result<usize> {
            self.inner.delete_path_batch(p)
        }
        fn delete_blob_batch(&self, h: &[String]) -> io::Result<usize> {
            self.inner.delete_blob_batch(h)
        }

        fn put_blob(&self, data: &[u8]) -> io::Result<String> {
            self.inner.put_blob(data)
        }
        fn get_blob(&self, hash: &str) -> io::Result<Vec<u8>> {
            self.gets.fetch_add(1, Ordering::Relaxed);
            self.inner.get_blob(hash)
        }
        fn get_blob_range(&self, hash: &str, offset: u64, len: usize) -> io::Result<Vec<u8>> {
            self.range_gets.fetch_add(1, Ordering::Relaxed);
            self.inner.get_blob_range(hash, offset, len)
        }
        fn put_path(&self, p: &str, h: &str) -> io::Result<()> {
            self.inner.put_path(p, h)
        }
        fn get_path(&self, p: &str) -> Option<String> {
            self.inner.get_path(p)
        }
        fn put_object(&self, p: &str, bytes: &[u8]) -> io::Result<()> {
            self.inner.put_object(p, bytes)
        }
        fn get_object(&self, p: &str) -> Option<Vec<u8>> {
            self.inner.get_object(p)
        }
        fn delete_path(&self, p: &str) -> io::Result<bool> {
            self.inner.delete_path(p)
        }
        fn list_paths(&self, p: &str) -> io::Result<Vec<String>> {
            self.inner.list_paths(p)
        }
        fn blob_exists(&self, h: &str) -> bool {
            self.inner.blob_exists(h)
        }
        fn delete_blob(&self, h: &str) -> io::Result<bool> {
            self.inner.delete_blob(h)
        }
    }

    fn setup() -> (tempfile::TempDir, tempfile::TempDir) {
        (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap())
    }

    /// The headline property: a warm read issues zero backend requests.
    #[test]
    fn test_warm_reads_issue_no_backend_requests() {
        let (store_dir, _) = setup();
        let backend = CountingStore::new(store_dir.path());
        let cache = BlobCache::new(backend, CacheConfig::memory_only(1 << 20)).unwrap();

        let h = cache.put_blob(b"hot data").unwrap();
        let before = cache.inner().gets();

        for _ in 0..100 {
            assert_eq!(cache.get_blob(&h).unwrap(), b"hot data");
        }

        assert_eq!(
            cache.inner().gets(),
            before,
            "100 warm reads must not reach the backing store"
        );
        assert_eq!(cache.stats().memory_hits, 100);
        assert_eq!(cache.stats().hit_rate(), 1.0);
    }

    /// A cold read costs exactly one backend request, and warms the entry.
    #[test]
    fn test_cold_read_costs_one_request_then_warms() {
        let (store_dir, _) = setup();
        let backend = CountingStore::new(store_dir.path());
        // Write through a bare store so the cache starts empty.
        let h = LocalFSObjectStore::new(store_dir.path())
            .unwrap()
            .put_blob(b"cold data")
            .unwrap();

        let cache = BlobCache::new(backend, CacheConfig::memory_only(1 << 20)).unwrap();
        assert_eq!(cache.get_blob(&h).unwrap(), b"cold data");
        assert_eq!(cache.inner().gets(), 1);
        assert_eq!(cache.stats().misses, 1);

        assert_eq!(cache.get_blob(&h).unwrap(), b"cold data");
        assert_eq!(cache.inner().gets(), 1, "second read must be served warm");
    }

    /// The disk tier survives losing the memory tier — which is what makes it
    /// worth having across process restarts.
    #[test]
    fn test_disk_tier_survives_new_cache_instance() {
        let (store_dir, disk_dir) = setup();
        let h = {
            let backend = CountingStore::new(store_dir.path());
            let cache = BlobCache::new(
                backend,
                CacheConfig::default().with_disk(disk_dir.path(), 1 << 30),
            )
            .unwrap();
            cache.put_blob(b"persistent").unwrap()
        };

        // A brand-new cache with an empty memory tier, same disk directory.
        let backend = CountingStore::new(store_dir.path());
        let cache = BlobCache::new(
            backend,
            CacheConfig::default().with_disk(disk_dir.path(), 1 << 30),
        )
        .unwrap();

        assert_eq!(cache.get_blob(&h).unwrap(), b"persistent");
        assert_eq!(cache.stats().disk_hits, 1);
        assert_eq!(
            cache.inner().gets(),
            0,
            "disk tier must serve without reaching the backing store"
        );
    }

    /// Corrupted cache files are detected by re-hashing and never served.
    ///
    /// This is only possible because entries are content-addressed: the key
    /// carries the expected hash, so verification needs no extra metadata.
    #[test]
    fn test_corrupt_disk_entry_is_detected_and_bypassed() {
        let (store_dir, disk_dir) = setup();
        let backend = CountingStore::new(store_dir.path());
        let cache = BlobCache::new(
            backend,
            CacheConfig::default().with_disk(disk_dir.path(), 1 << 30),
        )
        .unwrap();
        let h = cache.put_blob(b"authentic bytes").unwrap();

        // Corrupt the cached copy behind the cache's back.
        let cached = disk_dir.path().join(&h[..2]).join(&h);
        std::fs::write(&cached, b"tampered bytes!").unwrap();

        // Drop the memory tier so the read must consult disk.
        let backend2 = CountingStore::new(store_dir.path());
        let cache2 = BlobCache::new(
            backend2,
            CacheConfig::default().with_disk(disk_dir.path(), 1 << 30),
        )
        .unwrap();

        assert_eq!(
            cache2.get_blob(&h).unwrap(),
            b"authentic bytes",
            "must serve the real bytes, not the tampered cache entry"
        );
        assert_eq!(cache2.stats().corrupt_entries, 1);

        // The cache self-heals: the bad entry is dropped, the truth is fetched
        // from the backing store, and the correct bytes are re-admitted. So
        // the file exists again — but now it verifies.
        assert_eq!(
            std::fs::read(&cached).unwrap(),
            b"authentic bytes",
            "cache should have healed itself with the authentic bytes"
        );
        assert_eq!(
            cache2.get_blob(&h).unwrap(),
            b"authentic bytes",
            "the healed entry must serve correctly on the next read"
        );
        assert_eq!(
            cache2.stats().corrupt_entries,
            1,
            "healing must not re-report the same corruption"
        );
    }

    /// Ranged reads hit the cache when the blob is resident, and go to the
    /// backend *as a range* when it is not.
    #[test]
    fn test_ranged_read_uses_cache_and_stays_ranged_on_miss() {
        let (store_dir, _) = setup();
        let data: Vec<u8> = (0..=255u8).cycle().take(4096).collect();
        let h = LocalFSObjectStore::new(store_dir.path())
            .unwrap()
            .put_blob(&data)
            .unwrap();

        let backend = CountingStore::new(store_dir.path());
        let cache = BlobCache::new(backend, CacheConfig::memory_only(0)).unwrap();

        // Memory budget 0 → always a miss → must issue a *ranged* backend read.
        assert_eq!(cache.get_blob_range(&h, 100, 50).unwrap(), &data[100..150]);
        assert_eq!(cache.inner().range_gets(), 1);
        assert_eq!(cache.inner().gets(), 0, "must not fetch the whole object");

        // With a budget, the whole blob caches and ranges are served locally.
        let backend = CountingStore::new(store_dir.path());
        let cache = BlobCache::new(backend, CacheConfig::memory_only(1 << 20)).unwrap();
        cache.get_blob(&h).unwrap();
        let before = cache.inner().range_gets();
        assert_eq!(cache.get_blob_range(&h, 10, 20).unwrap(), &data[10..30]);
        assert_eq!(cache.inner().range_gets(), before);
    }

    /// Eviction keeps the memory tier inside its budget.
    #[test]
    fn test_memory_tier_respects_budget() {
        let (store_dir, _) = setup();
        let backend = CountingStore::new(store_dir.path());
        // Room for ~4 x 1 KiB entries.
        let cache = BlobCache::new(backend, CacheConfig::memory_only(4096)).unwrap();

        let mut hashes = Vec::new();
        for i in 0..20u8 {
            hashes.push(cache.put_blob(&vec![i; 1024]).unwrap());
        }
        assert!(cache.stats().evictions > 0, "budget must force eviction");
        assert!(
            cache.memory.lock().unwrap().bytes <= 4096,
            "memory tier exceeded its budget"
        );
        // Everything is still readable — eviction costs a round trip, never
        // correctness.
        for (i, h) in hashes.iter().enumerate() {
            assert_eq!(cache.get_blob(h).unwrap(), vec![i as u8; 1024]);
        }
    }

    /// Deleting a blob must purge every tier — the one case where a
    /// content-addressed entry stops being valid.
    #[test]
    fn test_delete_purges_all_tiers() {
        let (store_dir, disk_dir) = setup();
        let backend = CountingStore::new(store_dir.path());
        let cache = BlobCache::new(
            backend,
            CacheConfig::default().with_disk(disk_dir.path(), 1 << 30),
        )
        .unwrap();

        let h = cache.put_blob(b"doomed").unwrap();
        assert!(cache.delete_blob(&h).unwrap());
        assert!(!disk_dir.path().join(&h[..2]).join(&h).exists());
        assert_eq!(
            cache.get_blob(&h).unwrap_err().kind(),
            io::ErrorKind::NotFound
        );
    }

    /// A cache key must never be able to escape the cache directory, even
    /// though hashes are the only thing that should reach it.
    #[test]
    fn test_cache_path_rejects_non_hash_keys() {
        let (store_dir, disk_dir) = setup();
        let backend = CountingStore::new(store_dir.path());
        let cache = BlobCache::new(
            backend,
            CacheConfig::default().with_disk(disk_dir.path(), 1 << 30),
        )
        .unwrap();
        assert!(cache.disk_path("../escape").is_none());
        assert!(cache.disk_path("not-hex!!").is_none());
        assert!(cache.disk_path("a").is_none());
        assert!(cache.disk_path(&"a".repeat(64)).is_some());
    }

    /// Two independent caches over the same disk directory share entries
    /// safely — no coherence protocol, because the addressing guarantees it.
    #[test]
    fn test_two_caches_share_one_disk_directory() {
        let (store_dir, disk_dir) = setup();
        let cfg = || CacheConfig::default().with_disk(disk_dir.path(), 1 << 30);

        let a = BlobCache::new(CountingStore::new(store_dir.path()), cfg()).unwrap();
        let h = a.put_blob(b"shared between processes").unwrap();

        let b = BlobCache::new(CountingStore::new(store_dir.path()), cfg()).unwrap();
        assert_eq!(b.get_blob(&h).unwrap(), b"shared between processes");
        assert_eq!(b.stats().disk_hits, 1);
        assert_eq!(b.inner().gets(), 0);
    }
}
