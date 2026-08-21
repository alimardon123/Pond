// Pond Storage Kernel — the 3 primitives (Write, Read, Ref) in pure Rust
//
// ARCHITECTURE:
//   ObjectStore trait (put_blob, get_blob, put_path, get_path, ...)
//       ↓ implemented by
//   LocalFSObjectStore  ←→  S3ObjectStore (future)  ←→  GCSObjectStore (future)
//       ↓ used by
//   PondKernel (Write, Read, Ref — the 3 primitives)
//
// PATH LAYOUT (same on ALL backends — local FS, S3, GCS):
//   blobs/{hash[:2]}/{hash}                          — content-addressed blobs
//   collections/{name}/_branches/{branch}/commit     — branch commit refs
//   collections/{name}/_branches/{branch}/shards/...  — CRDT shards
//   collections/{name}/_active_branch                 — active branch name
//   collections/{name}/definition                     — collection schema
//   transactions/{tx_id}                              — transaction markers
//
// This layout works identically on local FS and S3 — migrating is a
// straight `aws s3 sync` or `rsync`. No backend-specific path logic.

pub mod crdt;
pub mod object_store;
pub mod c_abi;

pub use object_store::{
    is_blob_key, prefix_targets_blobs, LocalFSObjectStore, ObjectStore, StoreStats,
};
#[cfg(feature = "async")]
pub use object_store::AsyncObjectStore;

use std::io;
use std::sync::{Arc, Mutex};

use sha2::{Digest, Sha256};

// ---------------------------------------------------------------------------
// Hashing
// ---------------------------------------------------------------------------

/// Compute the SHA-256 hash of a byte slice, returned as a lowercase
/// hex string. This is the canonical content-address for Pond blobs.
pub fn hash_bytes(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    let result = hasher.finalize();
    let mut hex = String::with_capacity(64);
    for byte in result.iter() {
        hex.push_str(&format!("{:02x}", byte));
    }
    hex
}

// ---------------------------------------------------------------------------
// PondKernel — the 3 primitives (uses ObjectStore trait)
// ---------------------------------------------------------------------------

/// The storage kernel. Owns an ObjectStore (local FS now, S3 later).
///
/// The kernel is the ONLY stateful component. Everything above it
/// (lenses, UnifiedStorage, manifests, commits) is a pattern over
/// these 3 primitives:
///   Write(bytes) → hash     — content-addressed immutable blob
///   Read(hash_or_name) → bytes — read by hash or by name
///   Ref(name, hash)         — mutable name → hash mapping
pub struct PondKernel {
    // `Arc` (not `Box`) so async methods can clone the store into a
    // `spawn_blocking` closure without owning the kernel. The public API
    // (`new_with_store`) still accepts `Box<dyn ObjectStore>` for back-compat.
    store: Arc<dyn ObjectStore>,
    stats: Mutex<KernelStats>,
    /// Hashes this kernel has written itself.
    ///
    /// `reference` refuses to bind a name to a blob that does not exist, which
    /// is worth keeping — a dangling ref is unrecoverable corruption. But the
    /// check is a HEAD request, and on object storage that is a full round
    /// trip. In the overwhelmingly common case the caller wrote the blob
    /// moments ago and holds the hash `write` just returned, so the round trip
    /// asks the network to confirm something already known locally.
    ///
    /// Remembering what we wrote turns that case into a set lookup and leaves
    /// the check intact for the case it actually guards: a hash that came from
    /// somewhere else. One legacy write does three `reference` calls, so this
    /// is three round trips off every commit.
    ///
    /// Bounded, because a writer that runs for weeks would otherwise
    /// accumulate every hash it ever wrote. Forgetting is always safe — it
    /// costs one HEAD, which is exactly what happened before this existed.
    written: Mutex<std::collections::HashSet<String>>,
    /// Where subject keys live, when that is not the data store.
    ///
    /// Erasure works by destroying a key, so it is exactly as complete as the
    /// destruction of the last copy of that key. Keeping keys in the same
    /// store as the data means every backup, snapshot and replica of the data
    /// is also a backup of the keys — and restoring one undoes every erasure
    /// performed since it was taken.
    ///
    /// The keystore is a few bytes per subject precisely so that it can live
    /// somewhere with real deletion and a retention policy of its own. This
    /// field is how a deployment says where.
    ///
    /// `None` means the data store, which is the right default for a
    /// single-machine pond and the wrong one for anything with a backup
    /// schedule.
    keystore: Option<Arc<dyn ObjectStore>>,
}

/// How many recently written hashes to remember.
///
/// The access pattern this serves is "write a blob, bind a name to it moments
/// later", so a small window catches essentially all of it. At 64 hex chars
/// plus overhead this caps the set at a few megabytes.
const RECENT_WRITES_CAPACITY: usize = 65_536;

#[derive(Debug, Default, Clone)]
pub struct KernelStats {
    pub writes: u64,
    pub reads: u64,
    pub references: u64,
}

impl PondKernel {
    /// Create a kernel with a local FS backend.
    pub fn new_local(base_dir: impl AsRef<std::path::Path>) -> io::Result<Self> {
        let store = LocalFSObjectStore::new(base_dir)?;
        Ok(Self {
            store: Arc::new(store),
            stats: Mutex::new(KernelStats::default()),
            written: Mutex::new(std::collections::HashSet::new()),
            keystore: None,
        })
    }

    /// Create a kernel with a custom ObjectStore (for S3, GCS, etc.).
    pub fn new_with_store(store: Box<dyn ObjectStore>) -> Self {
        Self {
            store: Arc::from(store),
            stats: Mutex::new(KernelStats::default()),
            written: Mutex::new(std::collections::HashSet::new()),
            keystore: None,
        }
    }

    // ------------------------------------------------------------------
    // Primitive 1: Write
    // ------------------------------------------------------------------

    pub fn write(&self, data: &[u8]) -> io::Result<String> {
        let h = self.store.put_blob(data)?;
        self.stats.lock().unwrap().writes += 1;
        self.remember_written(std::iter::once(h.clone()));
        Ok(h)
    }

    pub fn write_batch(&self, items: &[Vec<u8>]) -> io::Result<Vec<String>> {
        let hashes = self.store.put_blob_batch(items)?;
        self.stats.lock().unwrap().writes += hashes.len() as u64;
        self.remember_written(hashes.iter().cloned());
        Ok(hashes)
    }

    /// Record hashes this kernel wrote, discarding the whole set if it grows
    /// past its cap.
    ///
    /// Dropping everything rather than evicting one entry keeps this to a
    /// single branch on the hot path. The cost of being wrong is one HEAD
    /// request on a later `reference`, so an occasional full reset is a better
    /// trade than maintaining eviction order on every write.
    fn remember_written(&self, hashes: impl Iterator<Item = String>) {
        let mut written = self.written.lock().unwrap();
        if written.len() >= RECENT_WRITES_CAPACITY {
            written.clear();
        }
        written.extend(hashes);
    }

    // ------------------------------------------------------------------
    // Primitive 2: Read
    // ------------------------------------------------------------------

    pub fn read(&self, hash_or_name: &str) -> io::Result<Vec<u8>> {
        if is_hash(hash_or_name) {
            return self.read_blob(hash_or_name);
        }
        let h = self.resolve(hash_or_name)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound,
                format!("Name '{}' not found", hash_or_name)))?;
        self.read_blob(&h)
    }

    pub fn read_blob(&self, h: &str) -> io::Result<Vec<u8>> {
        let data = self.store.get_blob(h)?;
        self.stats.lock().unwrap().reads += 1;
        Ok(data)
    }

    /// Read `len` bytes of a blob starting at `offset`.
    ///
    /// Ranged reads are what let a reader fetch exactly the bytes it needs —
    /// one index chunk, one column chunk inside a segment — instead of pulling
    /// whole objects. Available identically on every backend.
    pub fn read_blob_range(&self, h: &str, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        let data = self.store.get_blob_range(h, offset, len)?;
        self.stats.lock().unwrap().reads += 1;
        Ok(data)
    }

    pub fn read_blob_batch(&self, hashes: &[String]) -> io::Result<Vec<Vec<u8>>> {
        let results = self.store.get_blob_batch(hashes)?;
        self.stats.lock().unwrap().reads += results.len() as u64;
        Ok(results)
    }

    // ------------------------------------------------------------------
    // Primitive 3: Ref (mutable name → hash mapping)
    // ------------------------------------------------------------------

    pub fn reference(&self, name: &str, h: &str) -> io::Result<()> {
        validate_ref_path(name)?;
        // Skip the existence probe for blobs this kernel wrote: the write
        // already proved they exist, and re-proving it costs a round trip on
        // the commit path. Anything else is still checked.
        let known = self.written.lock().unwrap().contains(h);
        if !known && !self.store.blob_exists(h) {
            return Err(io::Error::new(io::ErrorKind::NotFound,
                format!("Hash '{}' does not refer to an existing blob", h)));
        }
        self.store.put_path(name, h)?;
        self.stats.lock().unwrap().references += 1;
        Ok(())
    }

    pub fn resolve(&self, name: &str) -> Option<String> {
        validate_ref_path(name).ok()?;
        self.store.get_path(name)
    }

    /// A handle to the underlying store.
    ///
    /// Exposed so a layer above can hand the same store to another component —
    /// the engine, for instance — rather than opening a second connection to
    /// it. Sharing the handle means sharing the connection pool and the cache;
    /// two independent stores over one bucket would double both.
    pub fn store_handle(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.store)
    }

    /// Put subject keys somewhere other than the data store.
    ///
    /// See [`keystore`](Self::keystore) for why that matters: a keystore
    /// backed up alongside the data is a keystore whose restore undoes every
    /// erasure since the backup.
    pub fn with_keystore(mut self, store: Arc<dyn ObjectStore>) -> Self {
        self.keystore = Some(store);
        self
    }

    /// Is the keystore held separately from the data?
    ///
    /// A deployment that must be able to prove erasure should check this and
    /// refuse to start if it is false, rather than discover after a restore
    /// that the keys came back with the data.
    pub fn keystore_is_separate(&self) -> bool {
        self.keystore.is_some()
    }

    /// The store subject keys live in.
    pub fn keystore_handle(&self) -> Arc<dyn ObjectStore> {
        match &self.keystore {
            Some(s) => Arc::clone(s),
            None => Arc::clone(&self.store),
        }
    }

    /// Write bytes under a name, replacing whatever was there.
    ///
    /// The companion to `reference`, and the more primitive of the two:
    /// `reference` binds a name to a *hash*, which costs a blob write plus a
    /// name write and a read back through the indirection. For state that is
    /// small, mutable, and owned by one writer — a head, a collection
    /// definition — that indirection buys nothing and costs a round trip in
    /// each direction.
    ///
    /// Content addressing remains the default for everything large or shared.
    pub fn write_named(&self, name: &str, bytes: &[u8]) -> io::Result<()> {
        validate_ref_path(name)?;
        self.store.put_object(name, bytes)?;
        self.stats.lock().unwrap().references += 1;
        Ok(())
    }

    /// Read bytes written by [`write_named`](Self::write_named).
    pub fn read_named(&self, name: &str) -> Option<Vec<u8>> {
        validate_ref_path(name).ok()?;
        let bytes = self.store.get_object(name)?;
        self.stats.lock().unwrap().reads += 1;
        Some(bytes)
    }

    pub fn list_names(&self) -> Vec<String> {
        self.store.list_paths("").unwrap_or_default()
    }

    pub fn list_names_prefix(&self, prefix: &str) -> Vec<String> {
        if validate_ref_path(prefix).is_err() {
            return Vec::new();
        }
        self.store.list_paths(prefix).unwrap_or_default()
    }

    pub fn delete_ref(&self, name: &str) -> io::Result<bool> {
        validate_ref_path(name)?;
        self.store.delete_path(name)
    }

    /// Physically delete a blob (maintenance operation, NOT a kernel primitive).
    pub fn delete_blob(&self, hash: &str) -> io::Result<bool> {
        self.store.delete_blob(hash)
    }

    // ------------------------------------------------------------------
    // Stats
    // ------------------------------------------------------------------

    pub fn stats(&self) -> KernelStats {
        self.stats.lock().unwrap().clone()
    }

    /// Get the objects directory path (for prefix-matching in cat).
    pub fn objects_dir(&self) -> &std::path::Path {
        // This is only used by the CLI for prefix matching.
        // The ObjectStore trait doesn't expose this, so we return empty.
        // The CLI uses list_blobs_prefix instead.
        std::path::Path::new("")
    }

    /// List all blob hashes with a given prefix (for `cat` prefix matching).
    pub fn list_blobs_prefix(&self, prefix: &str) -> Vec<String> {
        if prefix.len() < 2 {
            return Vec::new();
        }
        let shard = &prefix[..2];
        self.store.list_paths(&format!("blobs/{}/", shard))
            .unwrap_or_default()
            .into_iter()
            .filter_map(|p| {
                let parts: Vec<&str> = p.split('/').collect();
                parts.get(2).map(|s| s.to_string())
            })
            .filter(|h| h.starts_with(prefix))
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Async API — behind `feature = "async"`.
//
// Strategy: `PondKernel.store` is `Arc<dyn ObjectStore>`, so async methods
// clone the Arc into a `spawn_blocking` closure and await the join handle.
// This gives async callers a non-blocking API without duplicating the sync
// backend logic. Backends with native async I/O (LocalFS via tokio::fs,
// S3 via reqwest) can be used directly via the `AsyncObjectStore` trait.
// ---------------------------------------------------------------------------

#[cfg(feature = "async")]
impl PondKernel {
    /// Async variant of [`write`](Self::write). Writes bytes via the sync
    /// `ObjectStore::put_blob` on a blocking thread.
    ///
    /// The `??` unwraps both `JoinError` (panic in the blocking task) and
    /// `io::Error` (from the store).
    pub async fn write_async(&self, data: Vec<u8>) -> io::Result<String> {
        let store = self.store.clone();
        let h = tokio::task::spawn_blocking(move || store.put_blob(&data)).await??;
        self.stats.lock().unwrap().writes += 1;
        Ok(h)
    }

    /// Async variant of [`read_blob`](Self::read_blob).
    pub async fn read_blob_async(&self, hash: &str) -> io::Result<Vec<u8>> {
        let store = self.store.clone();
        let hash = hash.to_string();
        let data = tokio::task::spawn_blocking(move || store.get_blob(&hash)).await??;
        self.stats.lock().unwrap().reads += 1;
        Ok(data)
    }

    /// Async variant of [`read`](Self::read) — resolves a name to a hash
    /// (sync, fast — just a ref lookup) and then reads the blob async.
    pub async fn read_async(&self, hash_or_name: &str) -> io::Result<Vec<u8>> {
        if is_hash(hash_or_name) {
            return self.read_blob_async(hash_or_name).await;
        }
        let h = self.resolve(hash_or_name)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound,
                format!("Name '{}' not found", hash_or_name)))?;
        self.read_blob_async(&h).await
    }

    /// Async variant of [`reference`](Self::reference).
    pub async fn reference_async(&self, name: &str, h: &str) -> io::Result<()> {
        let store = self.store.clone();
        let name = name.to_string();
        let h = h.to_string();
        tokio::task::spawn_blocking(move || {
            if !store.blob_exists(&h) {
                return Err(io::Error::new(io::ErrorKind::NotFound,
                    format!("Hash '{}' does not refer to an existing blob", h)));
            }
            store.put_path(&name, &h)
        }).await??;
        self.stats.lock().unwrap().references += 1;
        Ok(())
    }

    /// Async variant of [`delete_blob`](Self::delete_blob).
    pub async fn delete_blob_async(&self, hash: &str) -> io::Result<bool> {
        let store = self.store.clone();
        let hash = hash.to_string();
        // `await?` flattens `Result<Result<bool, io::Error>, JoinError>` →
        // `Result<bool, io::Error>` (JoinError auto-converts to io::Error).
        tokio::task::spawn_blocking(move || store.delete_blob(&hash)).await?
    }

    /// Async variant of [`list_blobs_prefix`](Self::list_blobs_prefix).
    pub async fn list_blobs_prefix_async(&self, prefix: &str) -> Vec<String> {
        let store = self.store.clone();
        let prefix = prefix.to_string();
        match tokio::task::spawn_blocking(move || {
            let shard = match prefix.get(..2) {
                Some(s) => s.to_string(),
                None => return Vec::new(),
            };
            let list_key = format!("blobs/{}/", shard);
            store.list_paths(&list_key)
                .unwrap_or_default()
                .into_iter()
                .filter_map(|p| {
                    let parts: Vec<&str> = p.split('/').collect();
                    parts.get(2).map(|s| s.to_string())
                })
                .filter(|h| h.starts_with(&prefix))
                .collect::<Vec<_>>()
        }).await {
            Ok(v) => v,
            Err(_) => Vec::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn is_hash(s: &str) -> bool {
    s.len() == 64 && s.chars().all(|c| c.is_ascii_hexdigit())
}

/// Reject ref paths that could escape the store root or confuse a backend.
///
/// Ref paths are the ONLY place user-controlled strings become storage keys:
/// a collection named `../../x` turns into `collections/../../x/_branches/...`,
/// which on a local FS resolves outside the pond root. That input is reachable
/// from the MCP server (where an agent picks the collection name), the CLI, the
/// C ABI, and the Python binding, so it is validated here — the one point every
/// backend goes through — rather than at each call site.
///
/// The rules are deliberately strict and backend-neutral, so a path that is
/// valid on a local FS is valid as an S3/GCS key and vice versa:
///   - no empty path, no leading `/` (absolute), no trailing `/`
///   - no `.` or `..` components (traversal)
///   - no empty components (`a//b`)
///   - no backslashes (Windows separators, and `\` is legal in an S3 key)
///   - no control characters or NUL
///
/// A trailing `/` is permitted only for listing prefixes; callers that need it
/// use [`PondKernel::list_names_prefix`], which passes the prefix through the
/// same rules minus the trailing-slash check.
pub fn validate_ref_path(path: &str) -> io::Result<()> {
    let invalid = |why: &str| {
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid ref path '{}': {}", path, why),
        ))
    };

    if path.is_empty() {
        return Ok(()); // the empty prefix means "list everything"
    }
    if path.starts_with('/') {
        return invalid("absolute paths are not allowed");
    }
    if path.contains('\\') {
        return invalid("backslashes are not allowed");
    }
    if path.chars().any(|c| c.is_control()) {
        return invalid("control characters are not allowed");
    }

    // A single trailing slash is allowed (listing prefix); strip it before
    // splitting so it doesn't read as an empty final component.
    let body = path.strip_suffix('/').unwrap_or(path);
    for component in body.split('/') {
        match component {
            "" => return invalid("empty path component"),
            "." | ".." => return invalid("'.' and '..' components are not allowed"),
            _ => {}
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_write_read_roundtrip() {
        let dir = tempdir().unwrap();
        let kernel = PondKernel::new_local(dir.path()).unwrap();
        let h = kernel.write(b"hello, pond!").unwrap();
        assert_eq!(kernel.read_blob(&h).unwrap(), b"hello, pond!");
    }

    #[test]
    fn test_dedup() {
        let dir = tempdir().unwrap();
        let kernel = PondKernel::new_local(dir.path()).unwrap();
        let h1 = kernel.write(b"same").unwrap();
        let h2 = kernel.write(b"same").unwrap();
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_reference_resolve() {
        let dir = tempdir().unwrap();
        let kernel = PondKernel::new_local(dir.path()).unwrap();
        let h = kernel.write(b"data").unwrap();
        kernel.reference("my_coll", &h).unwrap();
        assert_eq!(kernel.resolve("my_coll"), Some(h.clone()));
        assert_eq!(kernel.resolve("nope"), None);
    }

    #[test]
    fn test_read_by_name() {
        let dir = tempdir().unwrap();
        let kernel = PondKernel::new_local(dir.path()).unwrap();
        let h = kernel.write(b"by name").unwrap();
        kernel.reference("coll", &h).unwrap();
        assert_eq!(kernel.read("coll").unwrap(), b"by name");
    }

    #[test]
    fn test_hierarchical_refs() {
        let dir = tempdir().unwrap();
        let kernel = PondKernel::new_local(dir.path()).unwrap();
        let h = kernel.write(b"branch").unwrap();
        kernel.reference("collections/users/_branches/main/commit", &h).unwrap();
        kernel.reference("collections/users/_branches/exp/commit", &h).unwrap();
        assert_eq!(
            kernel.resolve("collections/users/_branches/main/commit"),
            Some(h.clone())
        );
        let branches = kernel.list_names_prefix("collections/users/_branches");
        assert_eq!(branches.len(), 2);
    }

    #[test]
    fn test_persistence() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();
        {
            let kernel = PondKernel::new_local(&path).unwrap();
            let h = kernel.write(b"persistent").unwrap();
            kernel.reference("my_coll", &h).unwrap();
        }
        {
            let kernel = PondKernel::new_local(&path).unwrap();
            assert!(kernel.resolve("my_coll").is_some());
            assert_eq!(kernel.read("my_coll").unwrap(), b"persistent");
        }
    }

    #[test]
    fn test_delete_ref() {
        let dir = tempdir().unwrap();
        let kernel = PondKernel::new_local(dir.path()).unwrap();
        let h = kernel.write(b"data").unwrap();
        kernel.reference("temp", &h).unwrap();
        assert!(kernel.resolve("temp").is_some());
        kernel.delete_ref("temp").unwrap();
        assert!(kernel.resolve("temp").is_none());
        assert_eq!(kernel.read_blob(&h).unwrap(), b"data");
    }

    #[test]
    fn test_write_batch() {
        let dir = tempdir().unwrap();
        let kernel = PondKernel::new_local(dir.path()).unwrap();
        let items = vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()];
        let hashes = kernel.write_batch(&items).unwrap();
        for (i, h) in hashes.iter().enumerate() {
            assert_eq!(kernel.read_blob(h).unwrap(), items[i]);
        }
    }

    #[test]
    fn test_blob_path_layout() {
        let dir = tempdir().unwrap();
        let kernel = PondKernel::new_local(dir.path()).unwrap();
        let h = kernel.write(b"check layout").unwrap();
        let expected = dir.path().join("blobs").join(&h[..2]).join(&h);
        assert!(expected.exists(), "blob should be at blobs/{}/{}, got: {}",
                &h[..2], &h, expected.display());
    }

    #[test]
    fn test_ref_path_layout() {
        let dir = tempdir().unwrap();
        let kernel = PondKernel::new_local(dir.path()).unwrap();
        let h = kernel.write(b"ref layout").unwrap();
        kernel.reference("collections/users/_branches/main/commit", &h).unwrap();
        let expected = dir.path().join("collections/users/_branches/main/commit");
        assert!(expected.exists(), "ref should be at {}", expected.display());
    }

    #[test]
    fn test_validate_ref_path_accepts_normal_refs() {
        for ok in [
            "",
            "collections/users/_branches/main/commit",
            "transactions/abc123",
            "blobs/ab/",
            "a_b-c.d",
        ] {
            assert!(validate_ref_path(ok).is_ok(), "should accept {:?}", ok);
        }
    }

    #[test]
    fn test_validate_ref_path_rejects_traversal() {
        for bad in [
            "../escaped",
            "collections/../../escaped",
            "collections/users/../../../etc/passwd",
            "/absolute/path",
            "a//b",
            "./relative",
            "back\\slash",
            "nul\0byte",
            "trailing/..",
        ] {
            assert!(
                validate_ref_path(bad).is_err(),
                "should reject {:?}",
                bad
            );
        }
    }

    /// Regression: a collection name containing `..` must not create files
    /// outside the store root. This was reachable end-to-end from the MCP
    /// server, where the collection name is supplied by the calling agent.
    #[test]
    fn test_reference_cannot_escape_store_root() {
        let outer = tempdir().unwrap();
        let root = outer.path().join("pond");
        let kernel = PondKernel::new_local(&root).unwrap();
        let h = kernel.write(b"payload").unwrap();

        let err = kernel
            .reference("collections/../../pwned/_branches/main/commit", &h)
            .expect_err("traversal must be rejected");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);

        // Nothing was created outside the pond root.
        assert!(!outer.path().join("pwned").exists());
        assert!(!root.join("../pwned").exists());
    }

    /// The object store itself refuses too, even if a caller bypasses the
    /// kernel and holds a `LocalFSObjectStore` directly.
    #[test]
    fn test_object_store_refuses_traversal_directly() {
        let outer = tempdir().unwrap();
        let root = outer.path().join("pond");
        let store = LocalFSObjectStore::new(&root).unwrap();

        assert!(store.put_path("../pwned", "deadbeef").is_err());
        assert_eq!(store.get_path("../pwned"), None);
        assert!(!store.delete_path("../pwned").unwrap());
        assert!(!outer.path().join("pwned").exists());
    }

    /// `list_paths` enumerates refs, not blobs — and `list_blobs_prefix`
    /// reaches the blob tree through the same call.
    ///
    /// Both halves matter. The first keeps `pond ls` from printing thousands
    /// of content hashes; the second is what makes prefix lookup and any form
    /// of garbage collection possible at all. The S3 backend used to get this
    /// wrong in a way that depended on whether a store prefix was configured,
    /// so the two backends disagreed about what a listing contains.
    #[test]
    fn test_list_paths_separates_refs_from_blobs() {
        let dir = tempdir().unwrap();
        let store = LocalFSObjectStore::new(dir.path()).unwrap();

        let hash = store.put_blob(b"payload").unwrap();
        store.put_path("collections/users", "cafebabe").unwrap();

        let refs = store.list_paths("").unwrap();
        assert!(
            refs.contains(&"collections/users".to_string()),
            "refs must be listed: {:?}",
            refs
        );
        assert!(
            refs.iter().all(|p| !is_blob_key(p)),
            "blob keys must not appear in a ref listing: {:?}",
            refs
        );

        // Asking for the blob tree explicitly returns it.
        let shard = &hash[..2];
        let blobs = store.list_paths(&format!("blobs/{}/", shard)).unwrap();
        assert!(
            blobs.iter().any(|p| p.ends_with(&hash)),
            "listing blobs/{}/ must return the blob: {:?}",
            shard,
            blobs
        );

        // And the kernel's prefix lookup, which is built on it, finds it.
        let kernel = PondKernel::new_local(dir.path()).unwrap();
        assert!(kernel.list_blobs_prefix(&hash[..4]).contains(&hash));
    }

    /// Binding a name to a blob is free of extra round trips when this kernel
    /// wrote the blob — and still refuses a hash that names nothing.
    ///
    /// The optimisation is only sound because it narrows *when* the check
    /// runs, never what it guarantees: a dangling ref is unrecoverable
    /// corruption, so the check has to survive. What it must not do is ask the
    /// network to confirm a fact the process already established.
    #[test]
    fn test_reference_skips_probe_for_own_writes_but_still_validates() {
        let dir = tempdir().unwrap();
        let kernel = PondKernel::new_local(dir.path()).unwrap();

        let h = kernel.write(b"payload").unwrap();
        kernel.reference("collections/users", &h).unwrap();
        assert_eq!(kernel.resolve("collections/users"), Some(h));

        // A hash this kernel never wrote, and which does not exist, is still
        // rejected — the guarantee is unchanged.
        let err = kernel
            .reference("collections/bad", &"0".repeat(64))
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
        assert_eq!(kernel.resolve("collections/bad"), None);

        // And a hash written by a *different* kernel over the same store is
        // accepted, because the fallback probe finds it.
        let other = PondKernel::new_local(dir.path()).unwrap();
        let foreign = other.write(b"written elsewhere").unwrap();
        kernel.reference("collections/foreign", &foreign).unwrap();
        assert_eq!(kernel.resolve("collections/foreign"), Some(foreign));
    }

    /// The memory of recent writes is bounded, and forgetting is safe.
    ///
    /// A writer that runs for weeks must not accumulate every hash it ever
    /// wrote. When the cap is reached the set is dropped, and the only
    /// consequence is that the next `reference` pays the HEAD it would have
    /// paid anyway — correctness never depended on remembering.
    #[test]
    fn test_recent_write_memory_is_bounded_and_forgetting_is_safe() {
        let dir = tempdir().unwrap();
        let kernel = PondKernel::new_local(dir.path()).unwrap();

        let first = kernel.write(b"the first blob").unwrap();
        assert!(kernel.written.lock().unwrap().contains(&first));

        // Drive the set past its cap directly. Writing that many real blobs
        // would test the filesystem, not the bound.
        let filler: Vec<String> = (0..super::RECENT_WRITES_CAPACITY)
            .map(|i| format!("{:064x}", i))
            .collect();
        kernel.remember_written(filler.into_iter());
        kernel.remember_written(std::iter::once("deadbeef".repeat(8)));

        let remembered = kernel.written.lock().unwrap().len();
        assert!(
            remembered < super::RECENT_WRITES_CAPACITY,
            "the set must be dropped once it reaches the cap, held {}",
            remembered
        );

        // `first` has been forgotten — and referencing it still works, via the
        // fallback probe against the store.
        assert!(!kernel.written.lock().unwrap().contains(&first));
        kernel.reference("collections/first", &first).unwrap();
        assert_eq!(kernel.resolve("collections/first"), Some(first));
    }

    /// Named bytes round-trip, replace in place, and refuse to escape the
    /// store root — the same three properties `put_path` has, because they are
    /// the same operation with one less indirection.
    #[test]
    fn test_put_object_round_trip_and_containment() {
        let dir = tempdir().unwrap();
        let store = LocalFSObjectStore::new(dir.path()).unwrap();

        assert_eq!(store.get_object("heads/writer-1"), None);

        store.put_object("heads/writer-1", b"first").unwrap();
        assert_eq!(store.get_object("heads/writer-1").unwrap(), b"first");

        // Last write wins, in place — no versioning, no CAS.
        store.put_object("heads/writer-1", b"second").unwrap();
        assert_eq!(store.get_object("heads/writer-1").unwrap(), b"second");

        // It appears in a ref listing, because it is one.
        assert!(store
            .list_paths("heads/")
            .unwrap()
            .contains(&"heads/writer-1".to_string()));

        // Batch reads line up with their inputs, misses included.
        let got = store.get_object_batch(&[
            "heads/writer-1".to_string(),
            "heads/absent".to_string(),
        ]);
        assert_eq!(got, vec![Some(b"second".to_vec()), None]);

        // And the containment check applies here too.
        assert!(store.put_object("../escape", b"x").is_err());
        assert!(!dir.path().parent().unwrap().join("escape").exists());
    }

    /// Deleting a batch must remove exactly the named blobs and report how
    /// many existed, whether the backend has a bulk API or falls back to the
    /// sequential default.
    #[test]
    fn test_delete_blob_batch() {
        let dir = tempdir().unwrap();
        let store = LocalFSObjectStore::new(dir.path()).unwrap();

        let hashes: Vec<String> = (0..5)
            .map(|i| store.put_blob(format!("blob-{}", i).as_bytes()).unwrap())
            .collect();
        let keep = store.put_blob(b"survivor").unwrap();

        // A hash that was never written must not be counted as removed.
        let mut targets = hashes.clone();
        targets.push("0".repeat(64));

        let removed = store.delete_blob_batch(&targets).unwrap();
        assert_eq!(removed, hashes.len(), "only existing blobs count as removed");
        for h in &hashes {
            assert!(!store.blob_exists(h), "{} should be gone", h);
        }
        assert!(store.blob_exists(&keep), "untargeted blobs must survive");

        assert_eq!(store.delete_blob_batch(&[]).unwrap(), 0);
    }

    /// The same contract for named paths.
    #[test]
    fn test_delete_path_batch() {
        let dir = tempdir().unwrap();
        let store = LocalFSObjectStore::new(dir.path()).unwrap();

        for i in 0..3 {
            store
                .put_path(&format!("collections/c{}", i), "cafebabe")
                .unwrap();
        }
        store.put_path("collections/keep", "cafebabe").unwrap();

        let targets: Vec<String> = (0..3)
            .map(|i| format!("collections/c{}", i))
            .chain(std::iter::once("collections/never-existed".to_string()))
            .collect();

        assert_eq!(store.delete_path_batch(&targets).unwrap(), 3);
        assert_eq!(store.get_path("collections/c0"), None);
        assert_eq!(
            store.get_path("collections/keep"),
            Some("cafebabe".to_string())
        );
        assert_eq!(store.delete_path_batch(&[]).unwrap(), 0);
    }

    /// Ranged reads must return exactly the requested window, and must agree
    /// with slicing the whole blob — otherwise a reader that ranges into an
    /// index chunk or a column chunk gets different bytes than one that reads
    /// the object whole.
    #[test]
    fn test_read_blob_range_matches_full_read() {
        let dir = tempdir().unwrap();
        let kernel = PondKernel::new_local(dir.path()).unwrap();
        let data: Vec<u8> = (0..=255u8).cycle().take(10_000).collect();
        let h = kernel.write(&data).unwrap();

        for (offset, len) in [
            (0u64, 1usize),
            (0, 10_000),
            (1, 100),
            (4096, 4096),
            (9_999, 1),
            (5_000, 123),
        ] {
            let got = kernel.read_blob_range(&h, offset, len).unwrap();
            let want = &data[offset as usize..(offset as usize + len).min(data.len())];
            assert_eq!(got, want, "range({}, {}) mismatch", offset, len);
        }
    }

    /// Out-of-bounds ranges truncate rather than error, matching HTTP Range
    /// semantics — so the local and S3 backends behave identically.
    #[test]
    fn test_read_blob_range_edges() {
        let dir = tempdir().unwrap();
        let kernel = PondKernel::new_local(dir.path()).unwrap();
        let h = kernel.write(b"0123456789").unwrap();

        // Past the end → empty.
        assert!(kernel.read_blob_range(&h, 10, 5).unwrap().is_empty());
        assert!(kernel.read_blob_range(&h, 999, 5).unwrap().is_empty());
        // Straddling the end → truncated.
        assert_eq!(kernel.read_blob_range(&h, 8, 100).unwrap(), b"89");
        // Zero length → empty.
        assert!(kernel.read_blob_range(&h, 0, 0).unwrap().is_empty());
        // Missing blob → NotFound, same as a full read.
        let missing = "f".repeat(64);
        assert_eq!(
            kernel.read_blob_range(&missing, 0, 1).unwrap_err().kind(),
            io::ErrorKind::NotFound
        );
    }

    /// The trait's default implementation (used by any backend that has not
    /// specialized) must agree with the native LocalFS one.
    #[test]
    fn test_default_range_impl_matches_native() {
        use crate::object_store::slice_range;
        let data: Vec<u8> = (0..200u8).collect();
        for (offset, len) in [(0u64, 5usize), (10, 20), (195, 100), (500, 1), (0, 0)] {
            let dir = tempdir().unwrap();
            let store = LocalFSObjectStore::new(dir.path()).unwrap();
            let h = store.put_blob(&data).unwrap();
            assert_eq!(
                store.get_blob_range(&h, offset, len).unwrap(),
                slice_range(&data, offset, len),
                "native and default range impls disagree at ({}, {})",
                offset,
                len
            );
        }
    }

    /// Concurrent writers to the same ref must never yield a torn read, and
    /// must not leave temp files behind.
    ///
    /// The temp name used to be `{path}.tmp.{pid}`, which is shared by every
    /// thread in a process — two threads writing the same ref wrote to the
    /// same temp path. The name now includes a per-call counter.
    #[test]
    fn test_concurrent_ref_writes_never_tear() {
        let dir = tempdir().unwrap();
        let store = std::sync::Arc::new(LocalFSObjectStore::new(dir.path()).unwrap());
        let kernel = PondKernel::new_local(dir.path()).unwrap();
        let a = kernel.write(b"a").unwrap();
        let b = kernel.write(b"b").unwrap();

        const REF: &str = "collections/x/_branches/main/commit";
        for _ in 0..200 {
            std::thread::scope(|s| {
                for h in [&a, &b] {
                    let store = store.clone();
                    s.spawn(move || {
                        let _ = store.put_path(REF, h);
                    });
                }
            });
            // Every read must return one of the two hashes we wrote — never
            // a partial write, never garbage.
            let got = store.get_path(REF).expect("ref must resolve");
            assert!(got == a || got == b, "torn ref value: {:?}", got);
        }

        // No temp files survived.
        let mut leftovers = Vec::new();
        fn walk(p: &std::path::Path, out: &mut Vec<String>) {
            if let Ok(rd) = std::fs::read_dir(p) {
                for e in rd.flatten() {
                    let q = e.path();
                    if q.is_dir() {
                        walk(&q, out);
                    } else if q.to_string_lossy().contains(".tmp.") {
                        out.push(q.display().to_string());
                    }
                }
            }
        }
        walk(dir.path(), &mut leftovers);
        assert!(leftovers.is_empty(), "temp files left behind: {:?}", leftovers);
    }

    #[test]
    fn test_ref_stores_json_not_raw_hash() {
        let dir = tempdir().unwrap();
        let kernel = PondKernel::new_local(dir.path()).unwrap();
        let h = kernel.write(b"json format").unwrap();
        kernel.reference("my_ref", &h).unwrap();
        let ref_file = dir.path().join("my_ref");
        let content = std::fs::read_to_string(&ref_file).unwrap();
        assert!(content.contains(r#""hash":"#), "ref must store JSON, got: {}", content);
        assert!(content.contains(&h), "ref must contain the hash, got: {}", content);
    }
}

// ---------------------------------------------------------------------------
// Async tests — only compiled when `feature = "async"` is on.
// ---------------------------------------------------------------------------

#[cfg(all(test, feature = "async"))]
mod async_tests {
    use super::*;
    use tempfile::tempdir;

    /// Round-trip: write_async → read_blob_async.
    #[tokio::test]
    async fn test_async_write_read_roundtrip() {
        let dir = tempdir().unwrap();
        let kernel = PondKernel::new_local(dir.path()).unwrap();
        let h = kernel.write_async(b"hello, async pond!".to_vec()).await.unwrap();
        let data = kernel.read_blob_async(&h).await.unwrap();
        assert_eq!(data, b"hello, async pond!");
    }

    /// Async read by name (reference_async + read_async).
    #[tokio::test]
    async fn test_async_reference_and_read_by_name() {
        let dir = tempdir().unwrap();
        let kernel = PondKernel::new_local(dir.path()).unwrap();
        let h = kernel.write_async(b"by name async".to_vec()).await.unwrap();
        kernel.reference_async("my_coll", &h).await.unwrap();
        assert_eq!(kernel.resolve("my_coll"), Some(h.clone()));
        let data = kernel.read_async("my_coll").await.unwrap();
        assert_eq!(data, b"by name async");
    }

    /// Async dedup: same bytes → same hash (matches sync behavior).
    #[tokio::test]
    async fn test_async_dedup() {
        let dir = tempdir().unwrap();
        let kernel = PondKernel::new_local(dir.path()).unwrap();
        let h1 = kernel.write_async(b"same".to_vec()).await.unwrap();
        let h2 = kernel.write_async(b"same".to_vec()).await.unwrap();
        assert_eq!(h1, h2);
    }

    /// Async delete_blob returns true on existing, false after deletion.
    #[tokio::test]
    async fn test_async_delete_blob() {
        let dir = tempdir().unwrap();
        let kernel = PondKernel::new_local(dir.path()).unwrap();
        let h = kernel.write_async(b"to be deleted".to_vec()).await.unwrap();
        assert!(kernel.delete_blob_async(&h).await.unwrap());
        // Second delete returns false (already gone).
        assert!(!kernel.delete_blob_async(&h).await.unwrap());
    }

    /// Async list_blobs_prefix matches sync list_blobs_prefix for the
    /// same set of written blobs.
    #[tokio::test]
    async fn test_async_list_blobs_prefix_matches_sync() {
        let dir = tempdir().unwrap();
        let kernel = PondKernel::new_local(dir.path()).unwrap();
        // Write many blobs so at least 2 land in the same shard (blobs/{xx}/).
        // With 20 blobs, the probability of all 20 having distinct 2-char
        // prefixes is essentially zero (256 shards, birthday paradox).
        let mut all_hashes = Vec::new();
        for i in 0..20u32 {
            let h = kernel
                .write_async(format!("blob-{:04}", i).into_bytes())
                .await
                .unwrap();
            all_hashes.push(h);
        }

        // Pick the first 2 chars of the first hash as the prefix. The sync
        // and async paths should both return every blob whose hash starts
        // with that prefix — and they should agree exactly.
        let prefix = all_hashes[0][..2].to_string();
        let matching: Vec<String> = all_hashes.iter()
            .filter(|h| h.starts_with(&prefix))
            .cloned()
            .collect();
        assert!(!matching.is_empty(), "expected at least 1 blob in shard {}", prefix);

        let sync_list = kernel.list_blobs_prefix(&prefix);
        let async_list = kernel.list_blobs_prefix_async(&prefix).await;

        let mut s = sync_list.clone();
        let mut a = async_list.clone();
        s.sort();
        a.sort();
        assert_eq!(s, a, "sync and async prefix listing must match");

        // And the list must contain every matching hash we wrote.
        for h in &matching {
            assert!(s.contains(h), "list must contain {}", h);
        }
    }

    /// AsyncObjectStore impl on LocalFSObjectStore: direct trait call,
    /// bypassing the kernel. Verifies the trait is usable standalone.
    #[tokio::test]
    async fn test_async_object_store_local_fs() {
        use crate::AsyncObjectStore;
        let dir = tempdir().unwrap();
        let store = LocalFSObjectStore::new(dir.path()).unwrap();
        let h = store.put_blob_async(b"via trait".to_vec()).await.unwrap();
        let data = store.get_blob_async(&h).await.unwrap();
        assert_eq!(data, b"via trait");
        assert!(store.delete_blob_async(&h).await.unwrap());
        // After deletion, get_blob_async returns NotFound.
        let err = store.get_blob_async(&h).await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
    }

    /// Async read of a non-existent hash returns NotFound.
    #[tokio::test]
    async fn test_async_read_blob_not_found() {
        let dir = tempdir().unwrap();
        let kernel = PondKernel::new_local(dir.path()).unwrap();
        let fake = "0".repeat(64);
        let err = kernel.read_blob_async(&fake).await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
    }

    /// Concurrent async writes from many tasks don't deadlock or lose data.
    #[tokio::test]
    async fn test_async_concurrent_writes() {
        let dir = tempdir().unwrap();
        // We can't clone PondKernel itself (it's not Clone by design), so
        // wrap it in Arc and have each task use `&PondKernel` via the Arc.
        let kernel = std::sync::Arc::new(PondKernel::new_local(dir.path()).unwrap());
        let n = 32;
        let mut handles = Vec::with_capacity(n);
        for i in 0..n {
            let k = kernel.clone();
            handles.push(tokio::spawn(async move {
                let payload = format!("payload-{}", i);
                // Borrow through the Arc — async methods take &self.
                k.write_async(payload.into_bytes()).await.unwrap()
            }));
        }
        let mut hashes = Vec::with_capacity(n);
        for h in handles {
            hashes.push(h.await.unwrap());
        }
        // Each hash should read back its own payload.
        for (i, h) in hashes.iter().enumerate() {
            let data = kernel.read_blob_async(h).await.unwrap();
            assert_eq!(data, format!("payload-{}", i).into_bytes());
        }
        // All hashes should be distinct (different content → different hash).
        let mut unique = hashes.clone();
        unique.sort();
        unique.dedup();
        assert_eq!(unique.len(), n, "all {} hashes must be distinct", n);
    }
}
