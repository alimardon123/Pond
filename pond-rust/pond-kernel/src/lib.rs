// Pond Storage Kernel — the 3 primitives in pure Rust
//
// This is the Rust port of pond-core/kernel.py (PondMinimal). It provides:
//   Write(bytes) → hash     — content-addressed immutable blob storage
//   Read(hash_or_name) → bytes — read by hash or by name
//   Ref(name, hash)         — mutable name → hash mapping
//
// Plus derived helpers (ReadBlob, Resolve, ListNames, WriteBatch, ReadBlobBatch).
//
// The kernel is the ONLY stateful component in Pond. Everything above it
// (lenses, UnifiedStorage, manifests, commits) is a pattern over these
// 3 primitives.
//
// DESIGN PRINCIPLES (from DESIGN_GOALS.md):
//   - Simple (3.1): 3 primitives + batch helpers. Intellectually small.
//   - Powerful (3.2): rich behavior emerges from composition.
//   - Performant (3.3): content-addressed dedup, parallel I/O for batches.
//   - Scalable (3.4): the kernel doesn't know about collections/lenses.
//   - Efficient (3.5): dedup is free (same bytes → same hash).
//   - Beautiful (3.6): one responsibility (immutable bytes + mutable names).
//   - Storage-Indep (3.8): the backend is swappable (LocalFS now, S3 later).
//
// BACKEND: LocalFS (content-addressed file storage with sharded directories).
// Future: S3ObjectStore, GCSObjectStore — same API, different backend.

use std::collections::HashMap;
use std::fs;
use std::io::{self, Read as IoRead, Write as IoWrite};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};

use sha2::{Digest, Sha256};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// The hash function used for content-addressing. SHA-256, hex-encoded.
/// Same as the Python kernel (hashlib.sha256).

// ---------------------------------------------------------------------------
// Hashing
// ---------------------------------------------------------------------------

/// Compute the SHA-256 hash of a byte slice, returned as a lowercase
/// hex string. This is the canonical content-address for Pond blobs.
pub fn hash_bytes(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    let result = hasher.finalize();
    // Convert to hex string (64 chars)
    let mut hex = String::with_capacity(64);
    for byte in result.iter() {
        hex.push_str(&format!("{:02x}", byte));
    }
    hex
}

// ---------------------------------------------------------------------------
// PondKernel — the 3 primitives
// ---------------------------------------------------------------------------

/// The storage kernel. Owns the local FS backend and the in-memory
/// name→hash map (the only mutable state).
///
/// Thread-safe: all mutations are guarded by a Mutex. Multiple threads
/// can share a `PondKernel` via `Arc<PondKernel>`.
pub struct PondKernel {
    /// Root directory for blob storage (e.g. ".pond/objects")
    objects_dir: PathBuf,
    /// In-memory name→hash map. Persisted to roots.json on every Ref.
    /// (The Python kernel uses SQLite; we use a JSON file for simplicity
    /// and zero external deps. A future version can use SQLite if needed.)
    roots: Mutex<HashMap<String, String>>,
    /// Path to the roots.json file
    roots_path: PathBuf,
    /// Stats counters (for observability)
    stats: Mutex<KernelStats>,
}

#[derive(Debug, Default, Clone)]
pub struct KernelStats {
    pub writes: u64,
    pub reads: u64,
    pub references: u64,
}

impl PondKernel {
    /// Create a new kernel rooted at `base_dir`. Creates `.pond/objects/`
    /// and loads `roots.json` if it exists.
    pub fn new(base_dir: impl AsRef<Path>) -> io::Result<Self> {
        let base = base_dir.as_ref();
        let objects_dir = base.join(".pond").join("objects");
        let roots_path = base.join(".pond").join("roots.json");

        fs::create_dir_all(&objects_dir)?;

        // Load existing roots if roots.json exists
        let roots = if roots_path.exists() {
            let data = fs::read_to_string(&roots_path)?;
            serde_json_deserialize(&data).unwrap_or_default()
        } else {
            HashMap::new()
        };

        Ok(Self {
            objects_dir,
            roots: Mutex::new(roots),
            roots_path,
            stats: Mutex::new(KernelStats::default()),
        })
    }

    // ------------------------------------------------------------------
    // Primitive 1: Write
    // ------------------------------------------------------------------

    /// Write an immutable, content-addressed blob. Returns its hash.
    /// The same bytes always produce the same hash (dedup for free).
    /// If the blob already exists, this is a no-op (returns the hash).
    pub fn write(&self, data: &[u8]) -> io::Result<String> {
        let h = hash_bytes(data);
        let shard_dir = self.objects_dir.join(&h[..2]);
        fs::create_dir_all(&shard_dir)?;
        let path = shard_dir.join(format!("{}.bin", h));
        if !path.exists() {
            // dedup: only write if the file doesn't exist
            let mut file = fs::File::create(&path)?;
            file.write_all(data)?;
        }
        self.stats.lock().unwrap().writes += 1;
        Ok(h)
    }

    /// Write a batch of blobs. Each is written independently (no atomicity
    /// across blobs). Returns the list of hashes in the same order.
    /// This is a same-collection I/O performance primitive, NOT cross-
    /// collection atomicity.
    pub fn write_batch(&self, items: &[Vec<u8>]) -> io::Result<Vec<String>> {
        let mut hashes = Vec::with_capacity(items.len());
        for data in items {
            hashes.push(self.write(data)?);
        }
        Ok(hashes)
    }

    // ------------------------------------------------------------------
    // Primitive 2: Read
    // ------------------------------------------------------------------

    /// Read a blob by hash or by name. If the string looks like a hash
    /// (64 hex chars), read directly. Otherwise, resolve the name first.
    pub fn read(&self, hash_or_name: &str) -> io::Result<Vec<u8>> {
        // Try as hash first
        if is_hash(hash_or_name) {
            return self.read_blob(hash_or_name);
        }
        // Otherwise resolve as name
        let h = self.resolve(hash_or_name)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound,
                format!("Name '{}' not found", hash_or_name)))?;
        self.read_blob(&h)
    }

    /// Read a blob by its hash directly (no name resolution).
    pub fn read_blob(&self, h: &str) -> io::Result<Vec<u8>> {
        let path = self.blob_path(h);
        if !path.exists() {
            return Err(io::Error::new(io::ErrorKind::NotFound,
                format!("Hash '{}' does not refer to an existing blob", h)));
        }
        let data = fs::read(&path)?;
        self.stats.lock().unwrap().reads += 1;
        Ok(data)
    }

    /// Read a batch of blobs by hash. Each is read independently.
    pub fn read_blob_batch(&self, hashes: &[String]) -> io::Result<Vec<Vec<u8>>> {
        let mut results = Vec::with_capacity(hashes.len());
        for h in hashes {
            results.push(self.read_blob(h)?);
        }
        Ok(results)
    }

    // ------------------------------------------------------------------
    // Primitive 3: Ref (mutable name → hash mapping)
    // ------------------------------------------------------------------

    /// Set a mutable name → hash mapping. The hash must refer to an
    /// existing blob (we verify). This is the ONLY mutable operation.
    pub fn reference(&self, name: &str, h: &str) -> io::Result<()> {
        // Verify the blob exists
        let path = self.blob_path(h);
        if !path.exists() {
            return Err(io::Error::new(io::ErrorKind::NotFound,
                format!("Hash '{}' does not refer to an existing blob", h)));
        }
        {
            let mut roots = self.roots.lock().unwrap();
            roots.insert(name.to_string(), h.to_string());
            self.persist_roots(&roots)?;
        }
        self.stats.lock().unwrap().references += 1;
        Ok(())
    }

    /// Resolve a name to its current hash. Returns None if unbound.
    pub fn resolve(&self, name: &str) -> Option<String> {
        let roots = self.roots.lock().unwrap();
        roots.get(name).cloned()
    }

    /// List all names in the namespace.
    pub fn list_names(&self) -> Vec<String> {
        let roots = self.roots.lock().unwrap();
        let mut names: Vec<String> = roots.keys().cloned().collect();
        names.sort();
        names
    }

    // ------------------------------------------------------------------
    // Stats / observability
    // ------------------------------------------------------------------

    pub fn stats(&self) -> KernelStats {
        self.stats.lock().unwrap().clone()
    }

    // ------------------------------------------------------------------
    // Internal helpers
    // ------------------------------------------------------------------

    fn blob_path(&self, h: &str) -> PathBuf {
        self.objects_dir.join(&h[..2]).join(format!("{}.bin", h))
    }

    fn persist_roots(&self, roots: &HashMap<String, String>) -> io::Result<()> {
        let json = serde_json_serialize(roots);
        fs::write(&self.roots_path, json)
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Check if a string looks like a SHA-256 hash (64 hex chars).
fn is_hash(s: &str) -> bool {
    s.len() == 64 && s.chars().all(|c| c.is_ascii_hexdigit())
}

/// Minimal JSON serialization for HashMap<String, String>.
/// Avoids pulling in serde as a dependency for this simple case.
fn serde_json_serialize(map: &HashMap<String, String>) -> String {
    let mut entries: Vec<(&String, &String)> = map.iter().collect();
    entries.sort_by(|a, b| a.0.cmp(b.0));
    let mut out = String::from("{");
    for (i, (k, v)) in entries.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push('"');
        out.push_str(&k.replace('\\', "\\\\").replace('"', "\\\""));
        out.push_str("\":\"");
        out.push_str(&v.replace('\\', "\\\\").replace('"', "\\\""));
        out.push('"');
    }
    out.push('}');
    out
}

/// Minimal JSON deserialization for HashMap<String, String>.
/// Parses the format produced by serde_json_serialize.
fn serde_json_deserialize(s: &str) -> Option<HashMap<String, String>> {
    let s = s.trim();
    if !s.starts_with('{') || !s.ends_with('}') {
        return None;
    }
    let inner = &s[1..s.len()-1];
    if inner.is_empty() {
        return Some(HashMap::new());
    }
    let mut map = HashMap::new();
    // Simple state-machine parser: expects "key":"value","key":"value",...
    let chars: Vec<char> = inner.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        // Skip whitespace and commas
        while i < chars.len() && (chars[i] == ' ' || chars[i] == ',') {
            i += 1;
        }
        if i >= chars.len() {
            break;
        }
        // Expect opening quote for key
        if chars[i] != '"' {
            return None;
        }
        i += 1;
        let mut key = String::new();
        while i < chars.len() && chars[i] != '"' {
            if chars[i] == '\\' && i + 1 < chars.len() {
                i += 1;
                key.push(chars[i]);
            } else {
                key.push(chars[i]);
            }
            i += 1;
        }
        i += 1; // skip closing quote
        // Skip whitespace and colon
        while i < chars.len() && (chars[i] == ' ' || chars[i] == ':') {
            i += 1;
        }
        // Expect opening quote for value
        if i >= chars.len() || chars[i] != '"' {
            return None;
        }
        i += 1;
        let mut val = String::new();
        while i < chars.len() && chars[i] != '"' {
            if chars[i] == '\\' && i + 1 < chars.len() {
                i += 1;
                val.push(chars[i]);
            } else {
                val.push(chars[i]);
            }
            i += 1;
        }
        i += 1; // skip closing quote
        map.insert(key, val);
    }
    Some(map)
}

// ---------------------------------------------------------------------------
// C ABI — extern "C" wrappers for cross-language SDKs
// ---------------------------------------------------------------------------

use std::ffi::{c_char, CStr, CString};
use std::ptr;

/// Opaque handle for the kernel. Callers get one via `pond_kernel_new`
/// and must free it via `pond_kernel_free`.
pub struct PondKernelHandle {
    kernel: PondKernel,
}

/// Create a new kernel rooted at `base_dir` (null-terminated C string).
/// Returns a heap-allocated handle, or NULL on error.
#[no_mangle]
pub extern "C" fn pond_kernel_new(base_dir: *const c_char) -> *mut PondKernelHandle {
    if base_dir.is_null() {
        return ptr::null_mut();
    }
    let dir = match unsafe { CStr::from_ptr(base_dir) }.to_str() {
        Ok(s) => s,
        Err(_) => return ptr::null_mut(),
    };
    match PondKernel::new(dir) {
        Ok(kernel) => Box::into_raw(Box::new(PondKernelHandle { kernel })),
        Err(_) => ptr::null_mut(),
    }
}

/// Free a kernel handle. Safe on NULL.
#[no_mangle]
pub extern "C" fn pond_kernel_free(handle: *mut PondKernelHandle) {
    if !handle.is_null() {
        unsafe { drop(Box::from_raw(handle)); }
    }
}

/// Write a blob. Returns the hash as a heap-allocated null-terminated
/// C string. Caller MUST free it with `pond_string_free`.
/// Returns NULL on error.
#[no_mangle]
pub extern "C" fn pond_kernel_write(
    handle: *mut PondKernelHandle,
    data: *const u8,
    data_len: usize,
) -> *mut c_char {
    let handle = unsafe { match handle.as_ref() {
        Some(h) => h,
        None => return ptr::null_mut(),
    }};
    if data.is_null() {
        return ptr::null_mut();
    }
    let slice = unsafe { std::slice::from_raw_parts(data, data_len) };
    match handle.kernel.write(slice) {
        Ok(hash) => match CString::new(hash) {
            Ok(cs) => cs.into_raw(),
            Err(_) => ptr::null_mut(),
        },
        Err(_) => ptr::null_mut(),
    }
}

/// Read a blob by hash or name. Writes the data pointer + length into
/// the out-params. The data is valid until the next call on this handle.
/// Returns 0 on success, -1 on error.
#[no_mangle]
pub extern "C" fn pond_kernel_read(
    handle: *mut PondKernelHandle,
    hash_or_name: *const c_char,
    out_data: *mut *const u8,
    out_len: *mut usize,
) -> i32 {
    let handle = unsafe { match handle.as_ref() {
        Some(h) => h,
        None => return -1,
    }};
    if hash_or_name.is_null() || out_data.is_null() || out_len.is_null() {
        return -1;
    }
    let key = match unsafe { CStr::from_ptr(hash_or_name) }.to_str() {
        Ok(s) => s,
        Err(_) => return -1,
    };
    // We need to return a pointer that outlives the call. Box the Vec
    // and leak it — the caller must free it with pond_data_free.
    match handle.kernel.read(key) {
        Ok(data) => {
            let mut boxed = data.into_boxed_slice();
            let ptr = boxed.as_ptr();
            let len = boxed.len();
            std::mem::forget(boxed);
            unsafe {
                *out_data = ptr;
                *out_len = len;
            }
            0
        }
        Err(_) => -1,
    }
}

/// Free data returned by pond_kernel_read.
#[no_mangle]
pub extern "C" fn pond_data_free(data: *mut u8, len: usize) {
    if !data.is_null() && len > 0 {
        unsafe { drop(Vec::from_raw_parts(data, len, len)); }
    }
}

/// Free a string returned by pond_kernel_write / pond_kernel_resolve.
#[no_mangle]
pub extern "C" fn pond_string_free(s: *mut c_char) {
    if !s.is_null() {
        unsafe { drop(CString::from_raw(s)); }
    }
}

/// Set a name → hash mapping. Returns 0 on success, -1 on error.
#[no_mangle]
pub extern "C" fn pond_kernel_reference(
    handle: *mut PondKernelHandle,
    name: *const c_char,
    hash: *const c_char,
) -> i32 {
    let handle = unsafe { match handle.as_ref() {
        Some(h) => h,
        None => return -1,
    }};
    if name.is_null() || hash.is_null() {
        return -1;
    }
    let name = match unsafe { CStr::from_ptr(name) }.to_str() {
        Ok(s) => s,
        Err(_) => return -1,
    };
    let hash = match unsafe { CStr::from_ptr(hash) }.to_str() {
        Ok(s) => s,
        Err(_) => return -1,
    };
    match handle.kernel.reference(name, hash) {
        Ok(()) => 0,
        Err(_) => -1,
    }
}

/// Resolve a name to its hash. Returns the hash as a heap-allocated
/// C string, or NULL if the name is unbound. Caller MUST free with
/// pond_string_free.
#[no_mangle]
pub extern "C" fn pond_kernel_resolve(
    handle: *mut PondKernelHandle,
    name: *const c_char,
) -> *mut c_char {
    let handle = unsafe { match handle.as_ref() {
        Some(h) => h,
        None => return ptr::null_mut(),
    }};
    if name.is_null() {
        return ptr::null_mut();
    }
    let name = match unsafe { CStr::from_ptr(name) }.to_str() {
        Ok(s) => s,
        Err(_) => return ptr::null_mut(),
    };
    match handle.kernel.resolve(name) {
        Some(hash) => match CString::new(hash) {
            Ok(cs) => cs.into_raw(),
            Err(_) => ptr::null_mut(),
        },
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
        let kernel = PondKernel::new(dir.path()).unwrap();

        let data = b"hello, pond!";
        let h = kernel.write(data).unwrap();
        assert_eq!(h, hash_bytes(data));
        assert_eq!(kernel.read_blob(&h).unwrap(), data);
    }

    #[test]
    fn test_dedup() {
        let dir = tempdir().unwrap();
        let kernel = PondKernel::new(dir.path()).unwrap();

        let data = b"same bytes";
        let h1 = kernel.write(data).unwrap();
        let h2 = kernel.write(data).unwrap();
        assert_eq!(h1, h2, "same bytes must produce same hash");
    }

    #[test]
    fn test_reference_resolve() {
        let dir = tempdir().unwrap();
        let kernel = PondKernel::new(dir.path()).unwrap();

        let h = kernel.write(b"data for ref").unwrap();
        kernel.reference("my_collection", &h).unwrap();

        assert_eq!(kernel.resolve("my_collection"), Some(h.clone()));
        assert_eq!(kernel.resolve("nonexistent"), None);
    }

    #[test]
    fn test_read_by_name() {
        let dir = tempdir().unwrap();
        let kernel = PondKernel::new(dir.path()).unwrap();

        let data = b"read by name";
        let h = kernel.write(data).unwrap();
        kernel.reference("coll", &h).unwrap();

        // Read by name
        assert_eq!(kernel.read("coll").unwrap(), data);
        // Read by hash
        assert_eq!(kernel.read(&h).unwrap(), data);
    }

    #[test]
    fn test_list_names() {
        let dir = tempdir().unwrap();
        let kernel = PondKernel::new(dir.path()).unwrap();

        let h1 = kernel.write(b"a").unwrap();
        let h2 = kernel.write(b"b").unwrap();
        kernel.reference("coll1", &h1).unwrap();
        kernel.reference("coll2", &h2).unwrap();

        let names = kernel.list_names();
        assert_eq!(names, vec!["coll1", "coll2"]);
    }

    #[test]
    fn test_persistence() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();

        // Write + ref
        {
            let kernel = PondKernel::new(&path).unwrap();
            let h = kernel.write(b"persistent data").unwrap();
            kernel.reference("my_coll", &h).unwrap();
        }

        // Reopen and verify the ref survived
        {
            let kernel = PondKernel::new(&path).unwrap();
            assert!(kernel.resolve("my_coll").is_some());
            let data = kernel.read("my_coll").unwrap();
            assert_eq!(data, b"persistent data");
        }
    }

    #[test]
    fn test_write_batch() {
        let dir = tempdir().unwrap();
        let kernel = PondKernel::new(dir.path()).unwrap();

        let items = vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()];
        let hashes = kernel.write_batch(&items).unwrap();
        assert_eq!(hashes.len(), 3);
        for (i, h) in hashes.iter().enumerate() {
            assert_eq!(kernel.read_blob(h).unwrap(), items[i]);
        }
    }

    #[test]
    fn test_stats() {
        let dir = tempdir().unwrap();
        let kernel = PondKernel::new(dir.path()).unwrap();

        let h = kernel.write(b"data").unwrap();
        kernel.read_blob(&h).unwrap();
        kernel.reference("coll", &h).unwrap();

        let stats = kernel.stats();
        assert_eq!(stats.writes, 1);
        assert_eq!(stats.reads, 1);
        assert_eq!(stats.references, 1);
    }

    #[test]
    fn test_is_hash() {
        assert!(is_hash("a".repeat(64).as_str()));
        assert!(is_hash("0123456789abcdef".repeat(4).as_str()));
        assert!(!is_hash("short"));
        assert!(!is_hash("g".repeat(64).as_str())); // non-hex char
        assert!(!is_hash("collections/my_coll"));
    }

    #[test]
    fn test_json_roundtrip() {
        let mut map = HashMap::new();
        map.insert("collections/users".to_string(), "abc123".to_string());
        map.insert("collections/orders".to_string(), "def456".to_string());
        map.insert("weird/key with spaces".to_string(), "val\"quote".to_string());

        let json = serde_json_serialize(&map);
        let parsed = serde_json_deserialize(&json).unwrap();
        assert_eq!(parsed, map);
    }
}
