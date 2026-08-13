// Pond UnifiedStorage — Rust port of the Python unified_storage.py
//
// BROKEN INTO MODULES (following the user's request for smaller files):
//   lib.rs          — UnifiedStorage struct + public API + ref namespace helpers
//   manifest.rs     — CollectionManifest (RowGroupEntry, ColumnStats, encode/decode)
//   commit.rs       — Commit struct + write/read commit blobs + history walking
//   branch.rs       — Branch management (branch, checkout, merge)
//   shard.rs        — CRDT shard management (append, list, clear)
//   read.rs         — Read path (read, read_with_shards, read_at_snapshot)
//   write.rs        — Write path (write, append)
//   transaction.rs  — Atomic publication (begin_tx, commit_tx, abort_tx)
//
// This is a FAITHFUL PORT of the Python implementation — same commit format,
// same ref conventions, same merge logic. The Python code is the reference;
// this Rust code is the production implementation.
//
// DESIGN PRINCIPLES:
//   - Simple: each module has one responsibility
//   - Powerful: composes the kernel's 3 primitives into a full storage layer
//   - Performant: Rust native speed, no Python GIL, no dict intermediate
//   - Scalable: O(conflicting) merge, content-addressed dedup, parallel I/O
//   - Beautiful: clear module boundaries, downward dependencies only

pub mod manifest;
pub mod commit;
pub mod branch;
pub mod shard;
pub mod read;
pub mod write;
pub mod transaction;
pub mod maintenance;
pub mod pond_pack;

use pond_kernel::PondKernel;
use std::sync::Mutex;

// ---------------------------------------------------------------------------
// Ref namespace helpers — match Python UnifiedStorage conventions exactly
// ---------------------------------------------------------------------------

/// Branch commit ref: collections/{name}/_branches/{branch}/commit
pub fn branch_ref(collection: &str, branch: &str) -> String {
    format!("collections/{}/_branches/{}/commit", collection, branch)
}

/// Manifest ref: collections/{name}/_branches/{branch}/manifest
pub fn manifest_ref(collection: &str, branch: &str) -> String {
    format!("collections/{}/_branches/{}/manifest", collection, branch)
}

/// Shard prefix: collections/{name}/_branches/{branch}/shards/
pub fn shards_prefix(collection: &str, branch: &str) -> String {
    format!("collections/{}/_branches/{}/shards/", collection, branch)
}

/// Transaction ref: transactions/{tx_id}
pub fn tx_ref(tx_id: &str) -> String {
    format!("transactions/{}", tx_id)
}

/// Collection definition ref: collections/{name}/definition
pub fn definition_ref(collection: &str) -> String {
    format!("collections/{}/definition", collection)
}

// ---------------------------------------------------------------------------
// UnifiedStorage — the main struct
// ---------------------------------------------------------------------------

/// The unified storage layer. Owns a PondKernel and provides:
///   - Collection management (create, read, list)
///   - Commit history (write commits, walk parent chain, undo, revert)
///   - Branching (branch, checkout, merge)
///   - CRDT shards (append_shard, read_with_shards, compact_shards)
///   - Atomic publication (begin_tx, commit_tx, abort_tx)
///
/// This is the Rust equivalent of Python's UnifiedStorage class.
/// It composes the kernel's 3 primitives (Write, Read, Ref) into a
/// full versioned storage layer with git-like branching.
pub struct UnifiedStorage {
    kernel: PondKernel,
    /// Active branch per collection (in-memory, like Python's _active_branches)
    active_branches: Mutex<std::collections::HashMap<String, String>>,
}

impl UnifiedStorage {
    /// Create a new UnifiedStorage with a local FS kernel.
    pub fn new_local(base_dir: impl AsRef<std::path::Path>) -> std::io::Result<Self> {
        Ok(Self {
            kernel: PondKernel::new_local(base_dir)?,
            active_branches: Mutex::new(std::collections::HashMap::new()),
        })
    }

    /// Create a UnifiedStorage wrapping an existing kernel.
    pub fn new(kernel: PondKernel) -> Self {
        Self {
            kernel,
            active_branches: Mutex::new(std::collections::HashMap::new()),
        }
    }

    /// Get a reference to the kernel.
    pub fn kernel(&self) -> &PondKernel {
        &self.kernel
    }

    /// Get the active branch for a collection (default: "main").
    /// Matches Python's _get_active_branch.
    pub fn get_active_branch(&self, collection: &str) -> String {
        self.active_branches.lock().unwrap()
            .get(collection)
            .cloned()
            .unwrap_or_else(|| "main".to_string())
    }

    /// Set the active branch for a collection (in-memory only, like Python).
    pub fn set_active_branch(&self, collection: &str, branch: &str) {
        self.active_branches.lock().unwrap()
            .insert(collection.to_string(), branch.to_string());
    }

    /// Get the active commit ref for a collection.
    pub fn active_commit_ref(&self, collection: &str) -> String {
        let branch = self.get_active_branch(collection);
        branch_ref(collection, &branch)
    }

    /// Get the active manifest ref for a collection.
    pub fn active_manifest_ref(&self, collection: &str) -> String {
        let branch = self.get_active_branch(collection);
        manifest_ref(collection, &branch)
    }

    // Delegate to submodules
    // The actual implementations are in the module files and take
    // &UnifiedStorage (or &PondKernel) as the first argument.
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ref_namespace() {
        assert_eq!(
            branch_ref("users", "main"),
            "collections/users/_branches/main/commit"
        );
        assert_eq!(
            manifest_ref("users", "main"),
            "collections/users/_branches/main/manifest"
        );
        assert_eq!(
            shards_prefix("users", "main"),
            "collections/users/_branches/main/shards/"
        );
        assert_eq!(tx_ref("abc123"), "transactions/abc123");
        assert_eq!(definition_ref("users"), "collections/users/definition");
    }

    #[test]
    fn test_active_branch_default() {
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        assert_eq!(storage.get_active_branch("users"), "main");
    }

    #[test]
    fn test_set_active_branch() {
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        storage.set_active_branch("users", "experiment");
        assert_eq!(storage.get_active_branch("users"), "experiment");
        assert_eq!(storage.active_commit_ref("users"),
                   "collections/users/_branches/experiment/commit");
    }
}

// ===========================================================================
// C ABI — extern "C" wrappers for cross-language SDKs (Go, Java, Node, C)
// ===========================================================================
//
// These functions expose the full UnifiedStorage API through a C ABI.
// Any language that can call C functions (Go via cgo, Java via JNI, Node
// via N-API, C/C++ directly) gets full Pond storage access.
//
// Memory management:
//   - Strings returned by pond_storage_* functions are heap-allocated.
//     Caller MUST free them with pond_string_free().
//   - Data returned via out-params is heap-allocated.
//     Caller MUST free with pond_data_free().
//   - Handles (PondStorageHandle*) must be freed with pond_storage_free().
//
// Error handling:
//   - Functions that return strings return NULL on error.
//   - Functions that return int return 0 on success, -1 on error.
//   - Functions that return handles return NULL on error.

use std::ffi::{c_char, CStr, CString};
use std::ptr;

/// Opaque handle for UnifiedStorage.
pub struct PondStorageHandle {
    storage: UnifiedStorage,
}

impl PondStorageHandle {
    /// Create a handle from a UnifiedStorage. Used by C ABI constructors
    /// in other crates (e.g., pond_s3's `pond_storage_new_s3`).
    pub fn new(storage: UnifiedStorage) -> Self {
        Self { storage }
    }
}

/// Create a new UnifiedStorage with a local FS backend.
/// Returns NULL on error.
#[no_mangle]
pub extern "C" fn pond_storage_new(base_dir: *const c_char) -> *mut PondStorageHandle {
    if base_dir.is_null() { return ptr::null_mut(); }
    let dir = match unsafe { CStr::from_ptr(base_dir) }.to_str() {
        Ok(s) => s,
        Err(_) => return ptr::null_mut(),
    };
    match UnifiedStorage::new_local(dir) {
        Ok(storage) => Box::into_raw(Box::new(PondStorageHandle { storage })),
        Err(_) => ptr::null_mut(),
    }
}

/// Free a PondStorageHandle. Safe on NULL.
#[no_mangle]
pub extern "C" fn pond_storage_free(handle: *mut PondStorageHandle) {
    if !handle.is_null() {
        unsafe { drop(Box::from_raw(handle)); }
    }
}

/// Get the active branch for a collection.
/// Returns a heap-allocated string (caller must free with pond_string_free).
/// Returns NULL if the collection has no active branch (defaults to "main").
#[no_mangle]
pub extern "C" fn pond_storage_get_active_branch(
    handle: *const PondStorageHandle,
    collection: *const c_char,
) -> *mut c_char {
    let handle = unsafe { match handle.as_ref() { Some(h) => h, None => return ptr::null_mut() }};
    if collection.is_null() { return ptr::null_mut(); }
    let coll = match unsafe { CStr::from_ptr(collection) }.to_str() {
        Ok(s) => s, Err(_) => return ptr::null_mut(),
    };
    let branch = handle.storage.get_active_branch(coll);
    CString::new(branch).map(|cs| cs.into_raw()).unwrap_or(ptr::null_mut())
}

/// Set the active branch for a collection (in-memory only).
#[no_mangle]
pub extern "C" fn pond_storage_set_active_branch(
    handle: *mut PondStorageHandle,
    collection: *const c_char,
    branch: *const c_char,
) {
    let handle = unsafe { match handle.as_mut() { Some(h) => h, None => return }};
    if collection.is_null() || branch.is_null() { return; }
    let coll = match unsafe { CStr::from_ptr(collection) }.to_str() { Ok(s) => s, Err(_) => return };
    let br = match unsafe { CStr::from_ptr(branch) }.to_str() { Ok(s) => s, Err(_) => return };
    handle.storage.set_active_branch(coll, br);
}

/// Write data to a collection on the active branch.
/// Returns the commit hash (heap-allocated, caller must free), or NULL on error.
#[no_mangle]
pub extern "C" fn pond_storage_write(
    handle: *mut PondStorageHandle,
    collection: *const c_char,
    data: *const u8,
    data_len: usize,
    message: *const c_char,
) -> *mut c_char {
    let handle = unsafe { match handle.as_ref() { Some(h) => h, None => return ptr::null_mut() }};
    if collection.is_null() || data.is_null() { return ptr::null_mut(); }
    let coll = match unsafe { CStr::from_ptr(collection) }.to_str() { Ok(s) => s, Err(_) => return ptr::null_mut() };
    let msg = if message.is_null() { "" } else {
        match unsafe { CStr::from_ptr(message) }.to_str() { Ok(s) => s, Err(_) => "" }
    };
    let data_slice = unsafe { std::slice::from_raw_parts(data, data_len) };
    let active = handle.storage.get_active_branch(coll);
    match write::write(handle.storage.kernel(), coll, &active, data_slice, msg) {
        Ok(hash) => CString::new(hash).map(|cs| cs.into_raw()).unwrap_or(ptr::null_mut()),
        Err(_) => ptr::null_mut(),
    }
}

/// Read data from a collection's active branch.
/// Writes the data pointer + length into out-params.
/// Returns 0 on success, -1 on error.
#[no_mangle]
pub extern "C" fn pond_storage_read(
    handle: *const PondStorageHandle,
    collection: *const c_char,
    out_data: *mut *const u8,
    out_len: *mut usize,
) -> i32 {
    let handle = unsafe { match handle.as_ref() { Some(h) => h, None => return -1 }};
    if collection.is_null() || out_data.is_null() || out_len.is_null() { return -1; }
    let coll = match unsafe { CStr::from_ptr(collection) }.to_str() { Ok(s) => s, Err(_) => return -1 };
    let active = handle.storage.get_active_branch(coll);
    match read::read(handle.storage.kernel(), coll, &active) {
        Ok(data) => {
            let mut boxed = data.into_boxed_slice();
            let p = boxed.as_ptr();
            let len = boxed.len();
            std::mem::forget(boxed);
            unsafe { *out_data = p; *out_len = len; }
            0
        }
        Err(_) => -1,
    }
}

/// Create a branch from the active branch.
/// Returns the commit hash (caller must free), or NULL on error.
#[no_mangle]
pub extern "C" fn pond_storage_branch(
    handle: *mut PondStorageHandle,
    collection: *const c_char,
    branch_name: *const c_char,
) -> *mut c_char {
    let handle = unsafe { match handle.as_ref() { Some(h) => h, None => return ptr::null_mut() }};
    if collection.is_null() || branch_name.is_null() { return ptr::null_mut(); }
    let coll = match unsafe { CStr::from_ptr(collection) }.to_str() { Ok(s) => s, Err(_) => return ptr::null_mut() };
    let br = match unsafe { CStr::from_ptr(branch_name) }.to_str() { Ok(s) => s, Err(_) => return ptr::null_mut() };
    let active = handle.storage.get_active_branch(coll);
    match branch::branch(handle.storage.kernel(), coll, br, &active) {
        Ok(hash) => CString::new(hash).map(|cs| cs.into_raw()).unwrap_or(ptr::null_mut()),
        Err(_) => ptr::null_mut(),
    }
}

/// Checkout a branch (verify it exists + set active).
/// Returns 0 on success, -1 on error.
#[no_mangle]
pub extern "C" fn pond_storage_checkout(
    handle: *mut PondStorageHandle,
    collection: *const c_char,
    branch_name: *const c_char,
) -> i32 {
    let handle = unsafe { match handle.as_mut() { Some(h) => h, None => return -1 }};
    if collection.is_null() || branch_name.is_null() { return -1; }
    let coll = match unsafe { CStr::from_ptr(collection) }.to_str() { Ok(s) => s, Err(_) => return -1 };
    let br = match unsafe { CStr::from_ptr(branch_name) }.to_str() { Ok(s) => s, Err(_) => return -1 };
    match branch::checkout(handle.storage.kernel(), coll, br) {
        Ok(()) => {
            handle.storage.set_active_branch(coll, br);
            0
        }
        Err(_) => -1,
    }
}

/// Merge a source branch into a target branch.
/// If target is NULL, uses the active branch.
/// Returns the merge commit hash (caller must free), or NULL on error.
#[no_mangle]
pub extern "C" fn pond_storage_merge(
    handle: *mut PondStorageHandle,
    collection: *const c_char,
    source_branch: *const c_char,
    target_branch: *const c_char,
    message: *const c_char,
) -> *mut c_char {
    let handle = unsafe { match handle.as_ref() { Some(h) => h, None => return ptr::null_mut() }};
    if collection.is_null() || source_branch.is_null() { return ptr::null_mut(); }
    let coll = match unsafe { CStr::from_ptr(collection) }.to_str() { Ok(s) => s, Err(_) => return ptr::null_mut() };
    let src = match unsafe { CStr::from_ptr(source_branch) }.to_str() { Ok(s) => s, Err(_) => return ptr::null_mut() };
    let tgt = if target_branch.is_null() {
        handle.storage.get_active_branch(coll)
    } else {
        match unsafe { CStr::from_ptr(target_branch) }.to_str() { Ok(s) => s.to_string(), Err(_) => return ptr::null_mut() }
    };
    let msg = if message.is_null() { "" } else {
        match unsafe { CStr::from_ptr(message) }.to_str() { Ok(s) => s, Err(_) => "" }
    };
    match branch::merge(handle.storage.kernel(), coll, src, &tgt, msg) {
        Ok(hash) => CString::new(hash).map(|cs| cs.into_raw()).unwrap_or(ptr::null_mut()),
        Err(_) => ptr::null_mut(),
    }
}

/// Undo the last N commits on the active branch.
/// Returns the new HEAD hash (caller must free), or NULL on error.
#[no_mangle]
pub extern "C" fn pond_storage_undo(
    handle: *mut PondStorageHandle,
    collection: *const c_char,
    steps: usize,
) -> *mut c_char {
    let handle = unsafe { match handle.as_ref() { Some(h) => h, None => return ptr::null_mut() }};
    if collection.is_null() { return ptr::null_mut(); }
    let coll = match unsafe { CStr::from_ptr(collection) }.to_str() { Ok(s) => s, Err(_) => return ptr::null_mut() };
    let active = handle.storage.get_active_branch(coll);
    match branch::undo(handle.storage.kernel(), coll, &active, steps) {
        Ok(hash) => CString::new(hash).map(|cs| cs.into_raw()).unwrap_or(ptr::null_mut()),
        Err(_) => ptr::null_mut(),
    }
}

/// Revert the active branch to a specific commit.
/// Returns 0 on success, -1 on error.
#[no_mangle]
pub extern "C" fn pond_storage_revert(
    handle: *mut PondStorageHandle,
    collection: *const c_char,
    commit_hash: *const c_char,
) -> i32 {
    let handle = unsafe { match handle.as_ref() { Some(h) => h, None => return -1 }};
    if collection.is_null() || commit_hash.is_null() { return -1; }
    let coll = match unsafe { CStr::from_ptr(collection) }.to_str() { Ok(s) => s, Err(_) => return -1 };
    let hash = match unsafe { CStr::from_ptr(commit_hash) }.to_str() { Ok(s) => s, Err(_) => return -1 };
    let active = handle.storage.get_active_branch(coll);
    match branch::revert(handle.storage.kernel(), coll, &active, hash) {
        Ok(()) => 0,
        Err(_) => -1,
    }
}

/// List branches for a collection.
/// Returns a newline-separated string of branch names (caller must free).
/// Returns NULL on error.
#[no_mangle]
pub extern "C" fn pond_storage_list_branches(
    handle: *const PondStorageHandle,
    collection: *const c_char,
) -> *mut c_char {
    let handle = unsafe { match handle.as_ref() { Some(h) => h, None => return ptr::null_mut() }};
    if collection.is_null() { return ptr::null_mut(); }
    let coll = match unsafe { CStr::from_ptr(collection) }.to_str() { Ok(s) => s, Err(_) => return ptr::null_mut() };
    let branches = branch::list_branches(handle.storage.kernel(), coll);
    let joined = branches.join("\n");
    CString::new(joined).map(|cs| cs.into_raw()).unwrap_or(ptr::null_mut())
}

/// Free a string returned by pond_storage_* functions.
#[no_mangle]
pub extern "C" fn pond_storage_string_free(s: *mut c_char) {
    if !s.is_null() {
        unsafe { drop(CString::from_raw(s)); }
    }
}

/// Free data returned by pond_storage_read.
#[no_mangle]
pub extern "C" fn pond_storage_data_free(data: *mut u8, len: usize) {
    if !data.is_null() && len > 0 {
        unsafe { drop(Vec::from_raw_parts(data, len, len)); }
    }
}

// =============================================================
// Layer 2b: Structured row operations (write_rows, read_rows)
// =============================================================

/// Write structured INT64 columns as a PND2 blob with column stats.
///
/// Args:
///   handle: Storage handle
///   collection: Collection name
///   message: Commit message
///   num_cols: Number of columns
///   col_names: Array of column names (num_cols pointers)
///   col_data: Array of pointers to column data arrays
///   col_lens: Array of column lengths (must all be equal)
///   col_types: Array of column type codes (0=i64, 1=f64, 2=str)
///   str_data: For string columns, array of pointers to string arrays
///             (each string column has col_lens[i] pointers to null-terminated strings)
///
/// Returns: commit hash (caller must free with pond_storage_string_free), or NULL on error.
#[no_mangle]
pub extern "C" fn pond_storage_write_rows(
    handle: *mut PondStorageHandle,
    collection: *const c_char,
    message: *const c_char,
    num_cols: usize,
    col_names: *const *const c_char,
    col_data: *const *const u8,
    col_lens: *const usize,
    col_types: *const u8,
    n_rows: usize,
) -> *mut c_char {
    let handle = unsafe { match handle.as_ref() { Some(h) => h, None => return ptr::null_mut() }};
    if collection.is_null() || num_cols == 0 { return ptr::null_mut(); }

    let coll = match unsafe { CStr::from_ptr(collection) }.to_str() { Ok(s) => s, Err(_) => return ptr::null_mut() };
    let msg = if message.is_null() { "" } else {
        match unsafe { CStr::from_ptr(message) }.to_str() { Ok(s) => s, Err(_) => "" }
    };

    let active = handle.storage.get_active_branch(coll);

    // Build typed columns from C arrays
    use pond_core::TypedColumn;
    let mut typed_cols: Vec<(&str, TypedColumn)> = Vec::with_capacity(num_cols);

    let names = unsafe { std::slice::from_raw_parts(col_names, num_cols) };
    let data_ptrs = unsafe { std::slice::from_raw_parts(col_data, num_cols) };
    let lens = unsafe { std::slice::from_raw_parts(col_lens, num_cols) };
    let types = unsafe { std::slice::from_raw_parts(col_types, num_cols) };

    // We need to own the column data for the lifetime of typed_cols
    let mut owned_strings: Vec<Vec<String>> = Vec::new();
    let mut owned_i64: Vec<Vec<i64>> = Vec::new();
    let mut owned_f64: Vec<Vec<f64>> = Vec::new();

    for i in 0..num_cols {
        let name = match unsafe { CStr::from_ptr(names[i]) }.to_str() { Ok(s) => s, Err(_) => return ptr::null_mut() };
        let vtype = types[i];
        let len = lens[i];

        match vtype {
            0 => {
                // INT64
                let data_ptr = data_ptrs[i] as *const i64;
                let vals = unsafe { std::slice::from_raw_parts(data_ptr, len) }.to_vec();
                owned_i64.push(vals);
            }
            1 => {
                // FLOAT64
                let data_ptr = data_ptrs[i] as *const f64;
                let vals = unsafe { std::slice::from_raw_parts(data_ptr, len) }.to_vec();
                owned_f64.push(vals);
            }
            2 => {
                // STRING — col_data[i] points to an array of char* pointers
                let str_ptrs = data_ptrs[i] as *const *const c_char;
                let mut strs = Vec::with_capacity(len);
                for j in 0..len {
                    let s = unsafe { CStr::from_ptr(*str_ptrs.add(j)) }
                        .to_string_lossy()
                        .to_string();
                    strs.push(s);
                }
                owned_strings.push(strs);
            }
            _ => return ptr::null_mut(),
        }
    }

    // Now build typed_cols referencing owned data
    let mut str_idx = 0;
    let mut i64_idx = 0;
    let mut f64_idx = 0;

    for i in 0..num_cols {
        let name = match unsafe { CStr::from_ptr(names[i]) }.to_str() { Ok(s) => s, Err(_) => return ptr::null_mut() };
        let vtype = types[i];

        let col = match vtype {
            0 => {
                let vals = &owned_i64[i64_idx];
                i64_idx += 1;
                TypedColumn::Int64(vals.clone())
            }
            1 => {
                let vals = &owned_f64[f64_idx];
                f64_idx += 1;
                TypedColumn::Float64(vals.clone())
            }
            2 => {
                let vals = &owned_strings[str_idx];
                str_idx += 1;
                TypedColumn::String(vals.clone())
            }
            _ => return ptr::null_mut(),
        };

        // Leak the name string so we have a &'static str (safe for the duration of this call)
        let name_leaked: &'static str = Box::leak(name.to_string().into_boxed_str());
        typed_cols.push((name_leaked, col));
    }

    match write::write_rows(handle.storage.kernel(), coll, &active, &typed_cols, msg) {
        Ok(hash) => CString::new(hash).map(|cs| cs.into_raw()).unwrap_or(ptr::null_mut()),
        Err(_) => ptr::null_mut(),
    }
}

/// Read structured INT64 columns from a collection.
///
/// Returns a PondResult (same as pond_pnd2_decode) that the caller
/// must free with pond_result_free.
///
/// Args:
///   handle: Storage handle
///   collection: Collection name
///
/// Returns: PondResult*, or NULL on error.
#[no_mangle]
pub extern "C" fn pond_storage_read_rows(
    handle: *mut PondStorageHandle,
    collection: *const c_char,
) -> *mut pond_core::PondResult {
    use pond_core::PondResult;

    let handle = unsafe { match handle.as_ref() { Some(h) => h, None => return ptr::null_mut() }};
    if collection.is_null() { return ptr::null_mut(); }

    let coll = match unsafe { CStr::from_ptr(collection) }.to_str() { Ok(s) => s, Err(_) => return ptr::null_mut() };
    let active = handle.storage.get_active_branch(coll);

    // Read HEAD data
    let head = handle.storage.kernel().resolve(&branch_ref(coll, &active));
    let head_hash = match head {
        Some(h) => h,
        None => return ptr::null_mut(),
    };

    let head_data = match handle.storage.kernel().read_blob(&head_hash) {
        Ok(d) => d,
        Err(_) => return ptr::null_mut(),
    };

    // Decode manifest
    let manifest_bytes = if crate::pond_pack::is_pack(&head_data) {
        let (_, manifest_bytes, _) = match crate::pond_pack::decode_pack(&head_data) {
            Some(d) => d,
            None => return ptr::null_mut(),
        };
        manifest_bytes
    } else {
        let commit = match crate::commit::read_commit(handle.storage.kernel(), &head_hash) {
            Some(c) => c,
            None => return ptr::null_mut(),
        };
        if commit.manifest.is_empty() { return ptr::null_mut(); }
        match handle.storage.kernel().read_blob(&commit.manifest) {
            Ok(d) => d,
            Err(_) => return ptr::null_mut(),
        }
    };

    let manifest = match crate::manifest::CollectionManifest::decode(&manifest_bytes) {
        Some(m) => m,
        None => return ptr::null_mut(),
    };

    // Read first row group and decode as PND2
    if let Some(rg) = manifest.row_groups.first() {
        let blob_data = match handle.storage.kernel().read_blob(&rg.blob_hash) {
            Ok(d) => d,
            Err(_) => return ptr::null_mut(),
        };

        // Use the C ABI decode function — it returns a *mut PondResult
        let result_ptr = pond_core::c_abi::pond_pnd2_decode(
            blob_data.as_ptr(),
            blob_data.len(),
        );
        result_ptr
    } else {
        ptr::null_mut()
    }
}
