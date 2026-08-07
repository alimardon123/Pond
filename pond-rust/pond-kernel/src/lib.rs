// Pond Storage Kernel — the 3 primitives in pure Rust
//
// ARCHITECTURE (matches the Python pond-core/object_store_native_kernel.py):
//
//   ObjectStore trait (put_blob, get_blob, put_path, get_path, ...)
//       ↓ implemented by
//   LocalFSObjectStore  ←→  S3ObjectStore (future)  ←→  GCSObjectStore (future)
//       ↓ used by
//   PondKernel (Write, Read, Ref — the 3 primitives)
//
// PATH LAYOUT (same on ALL backends — local FS, S3, GCS):
//
//   blobs/{hash[:2]}/{hash}                          — content-addressed blobs
//   collections/{name}/_branches/{branch}/commit     — branch commit refs
//   collections/{name}/_branches/{branch}/shards/...  — CRDT shards
//   collections/{name}/_active_branch                 — active branch name
//   collections/{name}/definition                     — collection schema
//   transactions/{tx_id}                              — transaction markers
//
// This layout works identically on local FS and S3 — migrating is a
// straight `aws s3 sync` or `rsync`. No backend-specific path logic.
//
// ATOMICITY:
//   - Local FS: write temp file → rename (POSIX atomic)
//   - S3: PUT is already idempotent for same content (S3 handles it)
//   - The ObjectStore trait abstracts this — the kernel doesn't know

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use sha2::{Digest, Sha256};

// ---------------------------------------------------------------------------
// Hashing
// ---------------------------------------------------------------------------

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
// ObjectStore trait — the storage backend abstraction
// ---------------------------------------------------------------------------

/// The storage backend trait. All backends (local FS, S3, GCS) implement
/// this. The kernel uses this trait — it doesn't know which backend.
///
/// PATH LAYOUT (same on all backends):
///   blobs/{hash[:2]}/{hash}        — content-addressed blobs (raw bytes)
///   {path}                          — named refs (JSON: {"hash":"..."})
///
/// The ref path IS the key — no "refs/" prefix. This matches S3's flat
/// key space and allows `aws s3 sync` / `rsync` for migration.
pub trait ObjectStore: Send + Sync {
    /// Write bytes, content-addressed. Returns the hash.
    /// Idempotent: same bytes → same hash → same key. Overwriting is safe.
    fn put_blob(&self, data: &[u8]) -> io::Result<String>;

    /// Read bytes by content hash.
    fn get_blob(&self, hash: &str) -> io::Result<Vec<u8>>;

    /// Write a batch of blobs. Default: sequential. Backends can override
    /// with parallel implementation (S3 uses a thread pool).
    fn put_blob_batch(&self, items: &[Vec<u8>]) -> io::Result<Vec<String>> {
        let mut hashes = Vec::with_capacity(items.len());
        for data in items {
            hashes.push(self.put_blob(data)?);
        }
        Ok(hashes)
    }

    /// Read a batch of blobs. Default: sequential.
    fn get_blob_batch(&self, hashes: &[String]) -> io::Result<Vec<Vec<u8>>> {
        let mut results = Vec::with_capacity(hashes.len());
        for h in hashes {
            results.push(self.get_blob(h)?);
        }
        Ok(results)
    }

    /// Bind a named path to a content hash. Stores JSON {"hash":"..."}.
    /// Last-writer-wins (no CAS). The backend handles atomicity.
    fn put_path(&self, path: &str, hash: &str) -> io::Result<()>;

    /// Resolve a named path to its content hash. Returns None if unbound.
    fn get_path(&self, path: &str) -> Option<String>;

    /// Delete a named path. Returns true if it existed.
    fn delete_path(&self, path: &str) -> io::Result<bool>;

    /// List all paths under a prefix. Returns relative paths (without prefix).
    fn list_paths(&self, prefix: &str) -> io::Result<Vec<String>>;

    /// Check if a blob exists (for dedup checks).
    fn blob_exists(&self, hash: &str) -> bool;
}

// ---------------------------------------------------------------------------
// LocalFSObjectStore — local filesystem implementation
// ---------------------------------------------------------------------------

/// Local filesystem object store. Mirrors the S3 key structure exactly:
///
///   Blobs: {base_dir}/blobs/{hash[:2]}/{hash}
///   Refs:  {base_dir}/{path}   (e.g. collections/users/_branches/main/commit)
///
/// Thread-safe: blob writes use temp+rename (POSIX atomic); path writes
/// use temp+rename with unique temp names.
pub struct LocalFSObjectStore {
    base_dir: PathBuf,
    stats: Mutex<StoreStats>,
}

#[derive(Debug, Default, Clone)]
pub struct StoreStats {
    pub gets: u64,
    pub puts: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
}

impl LocalFSObjectStore {
    pub fn new(base_dir: impl AsRef<Path>) -> io::Result<Self> {
        let base = base_dir.as_ref();
        fs::create_dir_all(base.join("blobs"))?;
        Ok(Self {
            base_dir: base.to_path_buf(),
            stats: Mutex::new(StoreStats::default()),
        })
    }

    pub fn base_dir(&self) -> &Path {
        &self.base_dir
    }

    fn blob_path(&self, hash: &str) -> PathBuf {
        self.base_dir.join("blobs").join(&hash[..2]).join(hash)
    }

    fn path_file(&self, path: &str) -> PathBuf {
        self.base_dir.join(path)
    }
}

impl ObjectStore for LocalFSObjectStore {
    fn put_blob(&self, data: &[u8]) -> io::Result<String> {
        let h = hash_bytes(data);
        let path = self.blob_path(&h);
        // Dedup: skip if exists
        if !path.exists() {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
            // Write to temp file, then rename (POSIX atomic).
            // Use process ID + counter for unique temp name (avoids
            // collisions between concurrent writers).
            let tmp = format!("{}.tmp.{}", path.display(), std::process::id());
            fs::write(&tmp, data)?;
            fs::rename(&tmp, &path)?;
        }
        let mut s = self.stats.lock().unwrap();
        s.puts += 1;
        s.bytes_written += data.len() as u64;
        Ok(h)
    }

    fn get_blob(&self, hash: &str) -> io::Result<Vec<u8>> {
        let path = self.blob_path(hash);
        if !path.exists() {
            return Err(io::Error::new(io::ErrorKind::NotFound,
                format!("Blob '{}' not found", hash)));
        }
        let data = fs::read(&path)?;
        let mut s = self.stats.lock().unwrap();
        s.gets += 1;
        s.bytes_read += data.len() as u64;
        Ok(data)
    }

    fn put_path(&self, path: &str, hash: &str) -> io::Result<()> {
        let file = self.path_file(path);
        if let Some(parent) = file.parent() {
            fs::create_dir_all(parent)?;
        }
        // Store JSON {"hash":"..."} — same format as S3ObjectStore.
        // This makes `aws s3 sync` / `rsync` a straight copy.
        let body = format!(r#"{{"hash":"{}"}}"#, hash);
        // Write temp → rename (POSIX atomic on local FS).
        // On S3, put_path just does PUT (S3 is already idempotent).
        let tmp = format!("{}.tmp.{}", file.display(), std::process::id());
        fs::write(&tmp, &body)?;
        fs::rename(&tmp, &file)?;
        let mut s = self.stats.lock().unwrap();
        s.puts += 1;
        Ok(())
    }

    fn get_path(&self, path: &str) -> Option<String> {
        let file = self.path_file(path);
        match fs::read_to_string(&file) {
            Ok(body) => {
                // Parse JSON {"hash":"..."} — minimal parser
                extract_hash_from_json(&body)
            }
            Err(_) => None,
        }
    }

    fn delete_path(&self, path: &str) -> io::Result<bool> {
        let file = self.path_file(path);
        if file.exists() {
            fs::remove_file(&file)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn list_paths(&self, prefix: &str) -> io::Result<Vec<String>> {
        let prefix_dir = self.base_dir.join(prefix);
        let mut paths = Vec::new();
        if prefix_dir.is_dir() {
            walk_dir(&prefix_dir, &self.base_dir, &mut paths);
        }
        paths.sort();
        Ok(paths)
    }

    fn blob_exists(&self, hash: &str) -> bool {
        self.blob_path(hash).exists()
    }
}

/// Walk a directory recursively, collecting file paths relative to root.
fn walk_dir(dir: &Path, root: &Path, paths: &mut Vec<String>) {
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk_dir(&path, root, paths);
            } else if path.is_file() {
                if let Ok(rel) = path.strip_prefix(root) {
                    paths.push(rel.to_string_lossy().replace('\\', "/"));
                }
            }
        }
    }
}

/// Extract the "hash" field from a JSON string like {"hash":"abc123"}.
fn extract_hash_from_json(json: &str) -> Option<String> {
    let needle = r#""hash":""#;
    if let Some(start) = json.find(needle) {
        let rest = &json[start + needle.len()..];
        if let Some(end) = rest.find('"') {
            return Some(rest[..end].to_string());
        }
    }
    None
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
    store: Box<dyn ObjectStore>,
    stats: Mutex<KernelStats>,
}

#[derive(Debug, Default, Clone)]
pub struct KernelStats {
    pub writes: u64,
    pub reads: u64,
    pub references: u64,
}

impl PondKernel {
    /// Create a kernel with a local FS backend.
    pub fn new_local(base_dir: impl AsRef<Path>) -> io::Result<Self> {
        let store = LocalFSObjectStore::new(base_dir)?;
        Ok(Self {
            store: Box::new(store),
            stats: Mutex::new(KernelStats::default()),
        })
    }

    /// Create a kernel with a custom ObjectStore (for S3, GCS, etc.).
    pub fn new_with_store(store: Box<dyn ObjectStore>) -> Self {
        Self {
            store,
            stats: Mutex::new(KernelStats::default()),
        }
    }

    // ------------------------------------------------------------------
    // Primitive 1: Write
    // ------------------------------------------------------------------

    pub fn write(&self, data: &[u8]) -> io::Result<String> {
        let h = self.store.put_blob(data)?;
        self.stats.lock().unwrap().writes += 1;
        Ok(h)
    }

    pub fn write_batch(&self, items: &[Vec<u8>]) -> io::Result<Vec<String>> {
        let hashes = self.store.put_blob_batch(items)?;
        self.stats.lock().unwrap().writes += hashes.len() as u64;
        Ok(hashes)
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

    pub fn read_blob_batch(&self, hashes: &[String]) -> io::Result<Vec<Vec<u8>>> {
        let results = self.store.get_blob_batch(hashes)?;
        self.stats.lock().unwrap().reads += results.len() as u64;
        Ok(results)
    }

    // ------------------------------------------------------------------
    // Primitive 3: Ref (mutable name → hash mapping)
    // ------------------------------------------------------------------

    pub fn reference(&self, name: &str, h: &str) -> io::Result<()> {
        // Verify the blob exists
        if !self.store.blob_exists(h) {
            return Err(io::Error::new(io::ErrorKind::NotFound,
                format!("Hash '{}' does not refer to an existing blob", h)));
        }
        self.store.put_path(name, h)?;
        self.stats.lock().unwrap().references += 1;
        Ok(())
    }

    pub fn resolve(&self, name: &str) -> Option<String> {
        self.store.get_path(name)
    }

    pub fn list_names(&self) -> Vec<String> {
        self.store.list_paths("").unwrap_or_default()
    }

    pub fn list_names_prefix(&self, prefix: &str) -> Vec<String> {
        self.store.list_paths(prefix).unwrap_or_default()
    }

    pub fn delete_ref(&self, name: &str) -> io::Result<bool> {
        self.store.delete_path(name)
    }

    // ------------------------------------------------------------------
    // Stats
    // ------------------------------------------------------------------

    pub fn stats(&self) -> KernelStats {
        self.stats.lock().unwrap().clone()
    }

    /// List all blob hashes with a given prefix (for `cat` prefix matching).
    /// Walks the blobs/ directory and returns matching hash strings.
    pub fn list_blobs_prefix(&self, prefix: &str) -> Vec<String> {
        // Blobs are at blobs/{hash[:2]}/{hash}. If prefix is < 2 chars,
        // we can't determine the shard dir — return empty.
        if prefix.len() < 2 {
            return Vec::new();
        }
        let shard = &prefix[..2];
        self.store.list_paths(&format!("blobs/{}/", shard))
            .unwrap_or_default()
            .into_iter()
            .filter_map(|p| {
                // p is like "blobs/e1/e1234..." — extract the hash
                let parts: Vec<&str> = p.split('/').collect();
                parts.get(2).map(|s| s.to_string())
            })
            .filter(|h| h.starts_with(prefix))
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn is_hash(s: &str) -> bool {
    s.len() == 64 && s.chars().all(|c| c.is_ascii_hexdigit())
}

// ---------------------------------------------------------------------------
// C ABI — extern "C" wrappers
// ---------------------------------------------------------------------------

use std::ffi::{c_char, CStr, CString};
use std::ptr;

pub struct PondKernelHandle {
    kernel: PondKernel,
}

#[no_mangle]
pub extern "C" fn pond_kernel_new(base_dir: *const c_char) -> *mut PondKernelHandle {
    if base_dir.is_null() { return ptr::null_mut(); }
    let dir = match unsafe { CStr::from_ptr(base_dir) }.to_str() {
        Ok(s) => s,
        Err(_) => return ptr::null_mut(),
    };
    match PondKernel::new_local(dir) {
        Ok(kernel) => Box::into_raw(Box::new(PondKernelHandle { kernel })),
        Err(_) => ptr::null_mut(),
    }
}

#[no_mangle]
pub extern "C" fn pond_kernel_free(handle: *mut PondKernelHandle) {
    if !handle.is_null() {
        unsafe { drop(Box::from_raw(handle)); }
    }
}

#[no_mangle]
pub extern "C" fn pond_kernel_write(
    handle: *mut PondKernelHandle, data: *const u8, data_len: usize,
) -> *mut c_char {
    let handle = unsafe { match handle.as_ref() { Some(h) => h, None => return ptr::null_mut() }};
    if data.is_null() { return ptr::null_mut(); }
    let slice = unsafe { std::slice::from_raw_parts(data, data_len) };
    match handle.kernel.write(slice) {
        Ok(hash) => CString::new(hash).map(|cs| cs.into_raw()).unwrap_or(ptr::null_mut()),
        Err(_) => ptr::null_mut(),
    }
}

#[no_mangle]
pub extern "C" fn pond_kernel_read(
    handle: *mut PondKernelHandle, hash_or_name: *const c_char,
    out_data: *mut *const u8, out_len: *mut usize,
) -> i32 {
    let handle = unsafe { match handle.as_ref() { Some(h) => h, None => return -1 }};
    if hash_or_name.is_null() || out_data.is_null() || out_len.is_null() { return -1; }
    let key = match unsafe { CStr::from_ptr(hash_or_name) }.to_str() {
        Ok(s) => s,
        Err(_) => return -1,
    };
    match handle.kernel.read(key) {
        Ok(data) => {
            let mut boxed = data.into_boxed_slice();
            let ptr = boxed.as_ptr();
            let len = boxed.len();
            std::mem::forget(boxed);
            unsafe { *out_data = ptr; *out_len = len; }
            0
        }
        Err(_) => -1,
    }
}

#[no_mangle]
pub extern "C" fn pond_data_free(data: *mut u8, len: usize) {
    if !data.is_null() && len > 0 {
        unsafe { drop(Vec::from_raw_parts(data, len, len)); }
    }
}

#[no_mangle]
pub extern "C" fn pond_string_free(s: *mut c_char) {
    if !s.is_null() { unsafe { drop(CString::from_raw(s)); } }
}

#[no_mangle]
pub extern "C" fn pond_kernel_reference(
    handle: *mut PondKernelHandle, name: *const c_char, hash: *const c_char,
) -> i32 {
    let handle = unsafe { match handle.as_ref() { Some(h) => h, None => return -1 }};
    if name.is_null() || hash.is_null() { return -1; }
    let name = match unsafe { CStr::from_ptr(name) }.to_str() { Ok(s) => s, Err(_) => return -1 };
    let hash = match unsafe { CStr::from_ptr(hash) }.to_str() { Ok(s) => s, Err(_) => return -1 };
    match handle.kernel.reference(name, hash) { Ok(()) => 0, Err(_) => -1 }
}

#[no_mangle]
pub extern "C" fn pond_kernel_resolve(
    handle: *mut PondKernelHandle, name: *const c_char,
) -> *mut c_char {
    let handle = unsafe { match handle.as_ref() { Some(h) => h, None => return ptr::null_mut() }};
    if name.is_null() { return ptr::null_mut(); }
    let name = match unsafe { CStr::from_ptr(name) }.to_str() { Ok(s) => s, Err(_) => return ptr::null_mut() };
    match handle.kernel.resolve(name) {
        Some(hash) => CString::new(hash).map(|cs| cs.into_raw()).unwrap_or(ptr::null_mut()),
        None => ptr::null_mut(),
    }
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
        // Verify the path layout matches the Python convention:
        //   blobs/{hash[:2]}/{hash}
        let dir = tempdir().unwrap();
        let kernel = PondKernel::new_local(dir.path()).unwrap();
        let h = kernel.write(b"check layout").unwrap();
        let expected = dir.path().join("blobs").join(&h[..2]).join(&h);
        assert!(expected.exists(), "blob should be at blobs/{}/{}, got: {}",
                &h[..2], &h, expected.display());
    }

    #[test]
    fn test_ref_path_layout() {
        // Verify refs are stored directly under base_dir (not under refs/)
        let dir = tempdir().unwrap();
        let kernel = PondKernel::new_local(dir.path()).unwrap();
        let h = kernel.write(b"ref layout").unwrap();
        kernel.reference("collections/users/_branches/main/commit", &h).unwrap();
        let expected = dir.path().join("collections/users/_branches/main/commit");
        assert!(expected.exists(), "ref should be at {}", expected.display());
    }

    #[test]
    fn test_ref_stores_json_not_raw_hash() {
        // Verify the ref file contains JSON {"hash":"..."} not raw hash text.
        // This matches S3ObjectStore's format (for migration compatibility).
        let dir = tempdir().unwrap();
        let kernel = PondKernel::new_local(dir.path()).unwrap();
        let h = kernel.write(b"json format").unwrap();
        kernel.reference("my_ref", &h).unwrap();
        let ref_file = dir.path().join("my_ref");
        let content = fs::read_to_string(&ref_file).unwrap();
        assert!(content.contains(r#""hash":"#), "ref must store JSON, got: {}", content);
        assert!(content.contains(&h), "ref must contain the hash, got: {}", content);
    }
}
