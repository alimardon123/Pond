// Pond Storage Kernel — the 3 primitives in pure Rust
//
// This is the Rust port of pond-core/kernel.py (PondMinimal). It provides:
//   Write(bytes) → hash     — content-addressed immutable blob storage
//   Read(hash_or_name) → bytes — read by hash or by name
//   Ref(name, hash)         — mutable name → hash mapping
//
// REFS ARE STORED AS INDIVIDUAL FILES (Git-style), not a central JSON file
// or SQLite database. This avoids contention:
//   - Reads: no Mutex needed, each ref is a separate file
//   - Writes: atomic per-ref (write temp file → rename)
//   - Concurrent writers to DIFFERENT refs don't contend
//
// The ref namespace uses `/` for hierarchy:
//   collections/{name}/_branches/main/commit        → hash
//   collections/{name}/_branches/{branch}/commit    → hash
//   collections/{name}/_active_branch               → branch_name
//   collections/{name}/definition                   → hash
//
// This IS the catalog — no separate catalog.json needed. The kernel's
// namespace is the catalog. Schema metadata lives in the `definition` ref.

use std::fs;
use std::io::{self, Write as IoWrite};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

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
// PondKernel — the 3 primitives
// ---------------------------------------------------------------------------

/// The storage kernel. Owns the local FS backend.
///
/// Refs are stored as individual files under `.pond/refs/` — one file per
/// ref name, containing the hash string. This is the Git approach:
///   - No central database (no SQLite, no JSON file)
///   - No Mutex for reads (each ref file is independent)
///   - Atomic writes per-ref (write temp → rename)
///   - Concurrent writers to different refs don't contend
///
/// Thread-safe: the only Mutex is for stats counters (observability only).
/// All ref operations are lock-free.
pub struct PondKernel {
    /// Root directory for blob storage (e.g. ".pond/objects")
    objects_dir: PathBuf,
    /// Root directory for refs (e.g. ".pond/refs")
    refs_dir: PathBuf,
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
    /// and `.pond/refs/` if they don't exist.
    pub fn new(base_dir: impl AsRef<Path>) -> io::Result<Self> {
        let base = base_dir.as_ref();
        let objects_dir = base.join(".pond").join("objects");
        let refs_dir = base.join(".pond").join("refs");

        fs::create_dir_all(&objects_dir)?;
        fs::create_dir_all(&refs_dir)?;

        Ok(Self {
            objects_dir,
            refs_dir,
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
            let mut file = fs::File::create(&path)?;
            file.write_all(data)?;
        }
        self.stats.lock().unwrap().writes += 1;
        Ok(h)
    }

    /// Write a batch of blobs. Each is written independently (no atomicity
    /// across blobs). Returns the list of hashes in the same order.
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
        if is_hash(hash_or_name) {
            return self.read_blob(hash_or_name);
        }
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

    /// Read a batch of blobs by hash.
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
    ///
    /// The ref is stored as a file at `.pond/refs/{name}` containing the
    /// hash string. The write is atomic (write temp → rename).
    pub fn reference(&self, name: &str, h: &str) -> io::Result<()> {
        // Verify the blob exists
        let path = self.blob_path(h);
        if !path.exists() {
            return Err(io::Error::new(io::ErrorKind::NotFound,
                format!("Hash '{}' does not refer to an existing blob", h)));
        }
        // Write the ref file atomically: write to temp, then rename.
        let ref_path = self.ref_path(name);
        if let Some(parent) = ref_path.parent() {
            fs::create_dir_all(parent)?;
        }
        let temp_path = ref_path.with_extension("tmp");
        fs::write(&temp_path, h)?;
        fs::rename(&temp_path, &ref_path)?;
        self.stats.lock().unwrap().references += 1;
        Ok(())
    }

    /// Resolve a name to its current hash. Returns None if unbound.
    /// Reads the ref file at `.pond/refs/{name}`.
    pub fn resolve(&self, name: &str) -> Option<String> {
        let ref_path = self.ref_path(name);
        match fs::read_to_string(&ref_path) {
            Ok(s) => Some(s.trim().to_string()),
            Err(_) => None,
        }
    }

    /// List all names in the namespace. Walks the refs directory
    /// recursively and returns paths relative to `.pond/refs/`.
    pub fn list_names(&self) -> Vec<String> {
        let mut names = Vec::new();
        Self::walk_refs(&self.refs_dir, &self.refs_dir, &mut names);
        names.sort();
        names
    }

    /// List all names under a given prefix (like listing a directory).
    pub fn list_names_prefix(&self, prefix: &str) -> Vec<String> {
        let prefix_dir = self.refs_dir.join(prefix);
        let mut names = Vec::new();
        if prefix_dir.is_dir() {
            Self::walk_refs(&prefix_dir, &self.refs_dir, &mut names);
        }
        names.sort();
        names
    }

    /// Delete a ref (remove the ref file). Does NOT delete the blob
    /// it points to — that's GC's job.
    pub fn delete_ref(&self, name: &str) -> io::Result<()> {
        let ref_path = self.ref_path(name);
        if ref_path.exists() {
            fs::remove_file(&ref_path)?;
        }
        Ok(())
    }

    // ------------------------------------------------------------------
    // Stats / observability
    // ------------------------------------------------------------------

    pub fn stats(&self) -> KernelStats {
        self.stats.lock().unwrap().clone()
    }

    /// Get the objects directory path (for prefix-matching in cat).
    pub fn objects_dir(&self) -> &Path {
        &self.objects_dir
    }

    // ------------------------------------------------------------------
    // Internal helpers
    // ------------------------------------------------------------------

    fn blob_path(&self, h: &str) -> PathBuf {
        self.objects_dir.join(&h[..2]).join(format!("{}.bin", h))
    }

    fn ref_path(&self, name: &str) -> PathBuf {
        // Refs are stored as files mirroring the name hierarchy.
        // e.g. "collections/users/_branches/main/commit" →
        //      .pond/refs/collections/users/_branches/main/commit
        self.refs_dir.join(name)
    }

    /// Recursively walk a directory and collect all file paths relative
    /// to the refs root.
    fn walk_refs(dir: &Path, refs_root: &Path, names: &mut Vec<String>) {
        if let Ok(entries) = fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    Self::walk_refs(&path, refs_root, names);
                } else if path.is_file() {
                    if let Ok(rel) = path.strip_prefix(refs_root) {
                        names.push(rel.to_string_lossy().replace('\\', "/"));
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Check if a string looks like a SHA-256 hash (64 hex chars).
fn is_hash(s: &str) -> bool {
    s.len() == 64 && s.chars().all(|c| c.is_ascii_hexdigit())
}

// ---------------------------------------------------------------------------
// C ABI — extern "C" wrappers for cross-language SDKs
// ---------------------------------------------------------------------------

use std::ffi::{c_char, CStr, CString};
use std::ptr;

/// Opaque handle for the kernel.
pub struct PondKernelHandle {
    kernel: PondKernel,
}

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

#[no_mangle]
pub extern "C" fn pond_kernel_free(handle: *mut PondKernelHandle) {
    if !handle.is_null() {
        unsafe { drop(Box::from_raw(handle)); }
    }
}

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

#[no_mangle]
pub extern "C" fn pond_data_free(data: *mut u8, len: usize) {
    if !data.is_null() && len > 0 {
        unsafe { drop(Vec::from_raw_parts(data, len, len)); }
    }
}

#[no_mangle]
pub extern "C" fn pond_string_free(s: *mut c_char) {
    if !s.is_null() {
        unsafe { drop(CString::from_raw(s)); }
    }
}

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
        let h1 = kernel.write(b"same bytes").unwrap();
        let h2 = kernel.write(b"same bytes").unwrap();
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_reference_resolve() {
        let dir = tempdir().unwrap();
        let kernel = PondKernel::new(dir.path()).unwrap();
        let h = kernel.write(b"data").unwrap();
        kernel.reference("my_coll", &h).unwrap();
        assert_eq!(kernel.resolve("my_coll"), Some(h.clone()));
        assert_eq!(kernel.resolve("nonexistent"), None);
    }

    #[test]
    fn test_read_by_name() {
        let dir = tempdir().unwrap();
        let kernel = PondKernel::new(dir.path()).unwrap();
        let h = kernel.write(b"read by name").unwrap();
        kernel.reference("coll", &h).unwrap();
        assert_eq!(kernel.read("coll").unwrap(), b"read by name");
        assert_eq!(kernel.read(&h).unwrap(), b"read by name");
    }

    #[test]
    fn test_list_names() {
        let dir = tempdir().unwrap();
        let kernel = PondKernel::new(dir.path()).unwrap();
        let h1 = kernel.write(b"a").unwrap();
        let h2 = kernel.write(b"b").unwrap();
        kernel.reference("coll1", &h1).unwrap();
        kernel.reference("coll2", &h2).unwrap();
        kernel.reference("nested/deep/ref", &h1).unwrap();
        let names = kernel.list_names();
        assert!(names.contains(&"coll1".to_string()));
        assert!(names.contains(&"coll2".to_string()));
        assert!(names.contains(&"nested/deep/ref".to_string()));
    }

    #[test]
    fn test_list_names_prefix() {
        let dir = tempdir().unwrap();
        let kernel = PondKernel::new(dir.path()).unwrap();
        let h = kernel.write(b"data").unwrap();
        kernel.reference("collections/users/main", &h).unwrap();
        kernel.reference("collections/orders/main", &h).unwrap();
        kernel.reference("other/ref", &h).unwrap();
        let names = kernel.list_names_prefix("collections/users");
        assert_eq!(names, vec!["collections/users/main".to_string()]);
    }

    #[test]
    fn test_persistence() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();
        {
            let kernel = PondKernel::new(&path).unwrap();
            let h = kernel.write(b"persistent data").unwrap();
            kernel.reference("my_coll", &h).unwrap();
        }
        {
            let kernel = PondKernel::new(&path).unwrap();
            assert!(kernel.resolve("my_coll").is_some());
            assert_eq!(kernel.read("my_coll").unwrap(), b"persistent data");
        }
    }

    #[test]
    fn test_delete_ref() {
        let dir = tempdir().unwrap();
        let kernel = PondKernel::new(dir.path()).unwrap();
        let h = kernel.write(b"data").unwrap();
        kernel.reference("temp_ref", &h).unwrap();
        assert!(kernel.resolve("temp_ref").is_some());
        kernel.delete_ref("temp_ref").unwrap();
        assert!(kernel.resolve("temp_ref").is_none());
        // The blob still exists
        assert_eq!(kernel.read_blob(&h).unwrap(), b"data");
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
        assert!(is_hash(&"a".repeat(64)));
        assert!(is_hash(&"0123456789abcdef".repeat(4)));
        assert!(!is_hash("short"));
        assert!(!is_hash(&"g".repeat(64)));
        assert!(!is_hash("collections/my_coll"));
    }

    #[test]
    fn test_hierarchical_refs() {
        let dir = tempdir().unwrap();
        let kernel = PondKernel::new(dir.path()).unwrap();
        let h = kernel.write(b"branch data").unwrap();
        // Set up a branch ref hierarchy
        kernel.reference("collections/users/_branches/main/commit", &h).unwrap();
        kernel.reference("collections/users/_branches/experiment/commit", &h).unwrap();
        kernel.reference("collections/users/_active_branch", &h).unwrap();
        // _active_branch stores a branch name, not a hash — but the kernel
        // doesn't care; it just stores whatever string you give it.

        // Resolve by full path
        assert_eq!(
            kernel.resolve("collections/users/_branches/main/commit"),
            Some(h.clone())
        );
        assert_eq!(
            kernel.resolve("collections/users/_branches/experiment/commit"),
            Some(h.clone())
        );

        // List with prefix
        let branches = kernel.list_names_prefix("collections/users/_branches");
        assert_eq!(branches.len(), 2);
        assert!(branches.contains(&"collections/users/_branches/experiment/commit".to_string()));
        assert!(branches.contains(&"collections/users/_branches/main/commit".to_string()));
    }
}
