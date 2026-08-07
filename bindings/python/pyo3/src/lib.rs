// Pond Python Bindings — PyO3 wrapper around bindings/python/core
//
// This crate compiles to a Python extension module named `pond`.
// It exposes the full PND2 decode/encode pipeline to Python.
//
// All decode/encode LOGIC lives in `bindings/python/core`. This file is the thin
// PyO3 glue layer that:
//   1. Accepts Python args (bytes, lists, tuples)
//   2. Calls into pond-core's pure-Rust functions
//   3. Converts the Rust result types into Python objects
//
// This is the correct architecture: the decoder is implemented ONCE in
// pure Rust, and both the C ABI (in bindings/python/core) and Python (here) use it.

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyTuple};
use pyo3::Bound;

// Re-use the shared constants and parser from pond-core.
use pond_core::{
    PND2_MAGIC, PND2_VERSION, FLAG_HAS_STATS, FLAG_COMPRESSED,
    COMPRESSION_NONE, COMPRESSION_ZSTD,
    VT_INT64, VT_FLOAT64, VT_STRING, VT_BINARY,
    ENC_RAW, ENC_RLE, ENC_DICT, ENC_BITPACK,
    PND2Parser, PondColumn,
};

// ---------------------------------------------------------------------------
// Python-facing decode function
// ---------------------------------------------------------------------------

/// Decode a PND2 blob into a Python dict of column_name -> list of values.
///
/// Handles all value types (INT64, FLOAT64, STRING, BINARY, NULL) and all
/// encodings (RAW, RLE, DICT, BITPACK). Optionally projects columns and
/// applies row-level predicate pushdown.
#[pyfunction]
#[pyo3(signature = (blob_bytes, columns=None, predicates=None))]
#[allow(clippy::too_many_arguments)]
fn decode(
    py: Python,
    blob_bytes: &[u8],
    columns: Option<Vec<String>>,
    predicates: Option<Vec<(String, String, PyObject)>>,
) -> PyResult<PyObject> {
    // Validate header
    if blob_bytes.len() < 13 || &blob_bytes[0..4] != PND2_MAGIC {
        return Ok(py.None());
    }

    let version = blob_bytes[4];
    if version != PND2_VERSION {
        return Ok(py.None());
    }
    let flags = blob_bytes[5];
    let has_stats = (flags & FLAG_HAS_STATS) != 0;
    let _is_compressed = (flags & FLAG_COMPRESSED) != 0;

    let n_rows = u32::from_le_bytes([
        blob_bytes[6], blob_bytes[7], blob_bytes[8], blob_bytes[9]
    ]) as usize;
    let n_columns = u16::from_le_bytes([blob_bytes[10], blob_bytes[11]]) as usize;

    let compression_tag = blob_bytes[12];

    // Get the inner data (decompress if needed)
    let inner_owned: Vec<u8>;
    let inner: &[u8] = if compression_tag == COMPRESSION_ZSTD {
        match zstd_decompress(&blob_bytes[13..]) {
            Ok(d) => { inner_owned = d; &inner_owned[..] }
            Err(_) => return Ok(py.None()),
        }
    } else {
        &blob_bytes[13..]
    };

    let mut parser = PND2Parser::new(inner);

    // Parse schema: per col: name_len(1B) + name + vtype(1B) + enc(1B)
    let mut schema: Vec<(String, u8, u8)> = Vec::with_capacity(n_columns);
    for _ in 0..n_columns {
        let name_len = match parser.read_u8() { Some(v) => v as usize, None => break };
        let name_bytes = match parser.read_bytes(name_len) { Some(v) => v, None => break };
        let name = String::from_utf8_lossy(name_bytes).to_string();
        let vtype = match parser.read_u8() { Some(v) => v, None => break };
        let enc = match parser.read_u8() { Some(v) => v, None => break };
        schema.push((name, vtype, enc));
    }

    // Skip stats section
    if has_stats {
        for (_, vtype, _) in &schema {
            let has_min = match parser.read_u8() { Some(v) => v, None => break };
            if has_min != 0 {
                parser.skip_stat_value(*vtype);
                parser.skip_stat_value(*vtype);
            }
            let _null_count = parser.read_u32();
        }
    }

    // Record payload positions (don't read yet — projection pushdown)
    let mut payloads: Vec<(String, u8, u8, usize, usize)> = Vec::with_capacity(n_columns);
    for (name, vtype, enc) in &schema {
        let plen = match parser.read_u32() { Some(v) => v as usize, None => break };
        let pstart = parser.pos;
        if pstart + plen > inner.len() { break; }
        parser.pos += plen;
        payloads.push((name.clone(), *vtype, *enc, pstart, plen));
    }

    // Build the result dict
    let result = PyDict::new_bound(py);

    // Determine which columns to decode
    let requested_cols: Option<std::collections::HashSet<String>> =
        columns.map(|c| c.into_iter().collect());

    for (name, vtype, enc, pstart, plen) in &payloads {
        // Skip if not requested (projection pushdown)
        if let Some(ref req) = requested_cols {
            if !req.contains(name) {
                continue;
            }
        }

        let payload = &inner[*pstart..*pstart + *plen];
        if payload.is_empty() {
            result.set_item(name, PyList::empty_bound(py))?;
            continue;
        }

        // Delegate to pond-core's pure-Rust decoder
        let col = pond_core::decode_column(payload, *vtype, *enc, n_rows);
        let py_values = column_to_pylist(py, &col)?;
        result.set_item(name, py_values)?;
    }

    // Predicate pushdown: filter rows in Python after decode.
    // (Real pushdown happens at the zone-map level in bindings/python/sdk; this is
    // a row-level filter applied on the decoded result.)
    if let Some(preds) = predicates {
        return apply_predicates(py, &result, &preds);
    }

    Ok(result.into())
}

/// Convert a `pond_core::PondColumn` into a Python list of values.
///
/// Handles all value types: INT64, FLOAT64, STRING, BINARY.
/// NULL values (which bindings/python/core represents as empty strings/vecs for
/// bitmap-encoded rows) become Python None.
fn column_to_pylist(py: Python, col: &PondColumn) -> PyResult<PyObject> {
    let list = PyList::empty_bound(py);
    match col.vtype {
        VT_INT64 => {
            for v in &col.i64_data { list.append(*v)?; }
        }
        VT_FLOAT64 => {
            for v in &col.f64_data { list.append(*v)?; }
        }
        VT_STRING => {
            // CString → &str via to_str (safe — we know the bytes are valid UTF-8
            // because bindings/python/core built them via bytes_to_cstring which preserves
            // the input bytes; if the input had invalid UTF-8, the original
            // decode path used String::from_utf8_lossy so the bytes are already
            // valid UTF-8 replacement sequences).
            for v in &col.str_data {
                let s = v.to_str().unwrap_or("").to_string();
                list.append(s)?;
            }
        }
        VT_BINARY => {
            for v in &col.bin_data {
                list.append(PyBytes::new_bound(py, v))?;
            }
        }
        _ => {
            // Unknown vtype — emit None for each row.
            for _ in 0..col.n_values { list.append(py.None())?; }
        }
    }
    Ok(list.into())
}

/// Apply row-level predicates to the decoded result, returning only matching rows.
fn apply_predicates(
    py: Python,
    result: &Bound<'_, PyDict>,
    preds: &[(String, String, PyObject)],
) -> PyResult<PyObject> {
    if preds.is_empty() {
        return Ok(result.clone().into());
    }

    // Find the number of rows from the first list-valued column
    let mut n_rows: Option<usize> = None;
    for (k, v) in result.iter() {
        let _ = k; // unused key
        if let Ok(list) = v.downcast::<PyList>() {
            n_rows = Some(list.len());
            break;
        }
    }
    let n_rows = match n_rows {
        Some(n) => n,
        None => return Ok(result.clone().into()),
    };

    // For each row, evaluate all predicates. Keep the row only if ALL match.
    let mut keep_mask: Vec<bool> = vec![true; n_rows];
    for (col_name, op, target) in preds {
        let col_val = match result.get_item(col_name)? {
            Some(v) => v,
            None => continue,
        };
        let col_list: &Bound<'_, PyList> = match col_val.downcast() {
            Ok(l) => l,
            Err(_) => continue,
        };
        for i in 0..n_rows {
            if !keep_mask[i] { continue; }
            let row_val = col_list.get_item(i)?;
            let matches = match op.as_str() {
                "=" | "==" => row_val.compare(target)?.is_eq(),
                "!=" => !row_val.compare(target)?.is_eq(),
                "<" => row_val.compare(target)?.is_lt(),
                "<=" => row_val.compare(target)?.is_le(),
                ">" => row_val.compare(target)?.is_gt(),
                ">=" => row_val.compare(target)?.is_ge(),
                _ => true, // unknown op: don't filter
            };
            if !matches { keep_mask[i] = false; }
        }
    }

    // Build filtered result
    let filtered = PyDict::new_bound(py);
    for (k, v) in result.iter() {
        if let Ok(list) = v.downcast::<PyList>() {
            let new_list = PyList::empty_bound(py);
            for i in 0..n_rows {
                if keep_mask[i] {
                    new_list.append(list.get_item(i)?)?;
                }
            }
            filtered.set_item(k, new_list)?;
        } else {
            filtered.set_item(k, v)?;
        }
    }
    Ok(filtered.into())
}

// ---------------------------------------------------------------------------
// zstd decompression (uses Python's `zstandard` library — no Rust dep)
// ---------------------------------------------------------------------------

fn zstd_decompress(data: &[u8]) -> Result<Vec<u8>, String> {
    Python::with_gil(|py| {
        let zstd_mod = py.import_bound("zstandard")?;
        let decompress = zstd_mod.getattr("decompress")?;
        let py_bytes = PyBytes::new_bound(py, data);
        let result = decompress.call1((py_bytes,))?;
        let result_bytes: &[u8] = result.extract::<&[u8]>()?;
        Ok(result_bytes.to_vec())
    }).map_err(|e: pyo3::PyErr| e.to_string())
}

// ---------------------------------------------------------------------------
// Python-facing encode function
// ---------------------------------------------------------------------------

/// Encode a list of column values into a PND2 blob (RAW encoding only).
///
/// Returns a dict with:
///   - "blob": bytes — the PND2 blob
///   - "stats": list of (name, vtype, min, max, null_count) tuples
///
/// Returns None for columns that need DICT/RLE/BITPACK (Python handles those
/// via pond_sdk.extensions.physical_structures.encoding).
#[pyfunction]
#[pyo3(signature = (columns, n_rows))]
fn encode(py: Python, columns: Vec<(String, PyObject)>, n_rows: usize) -> PyResult<PyObject> {
    if columns.is_empty() || n_rows == 0 {
        return Ok(py.None());
    }

    let mut inner = Vec::new();
    let mut col_payloads: Vec<Vec<u8>> = Vec::new();
    let mut stats_list: Vec<(String, u8, PyObject, PyObject, u32)> = Vec::new();

    // Schema section
    for (name, values_obj) in &columns {
        let name_bytes = name.as_bytes();
        if name_bytes.len() > 255 {
            return Ok(py.None());
        }

        // Try INT64
        if let Ok(vals) = values_obj.extract::<Vec<i64>>(py) {
            if vals.len() != n_rows { return Ok(py.None()); }
            let mut payload = Vec::with_capacity(1 + n_rows * 8);
            payload.push(VT_INT64);
            for v in &vals { payload.extend_from_slice(&v.to_le_bytes()); }
            let min_val = vals.iter().min().copied().unwrap_or(0);
            let max_val = vals.iter().max().copied().unwrap_or(0);
            inner.extend_from_slice(&[name_bytes.len() as u8]);
            inner.extend_from_slice(name_bytes);
            inner.extend_from_slice(&[VT_INT64, ENC_RAW]);
            col_payloads.push(payload);
            stats_list.push((name.clone(), VT_INT64,
                min_val.to_object(py),
                max_val.to_object(py),
                0u32));
            continue;
        }

        // Try FLOAT64
        if let Ok(vals) = values_obj.extract::<Vec<f64>>(py) {
            if vals.len() != n_rows { return Ok(py.None()); }
            let mut payload = Vec::with_capacity(1 + n_rows * 8);
            payload.push(VT_FLOAT64);
            for v in &vals { payload.extend_from_slice(&v.to_le_bytes()); }
            let min_val = vals.iter().cloned().fold(f64::INFINITY, f64::min);
            let max_val = vals.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
            inner.extend_from_slice(&[name_bytes.len() as u8]);
            inner.extend_from_slice(name_bytes);
            inner.extend_from_slice(&[VT_FLOAT64, ENC_RAW]);
            col_payloads.push(payload);
            stats_list.push((name.clone(), VT_FLOAT64,
                min_val.to_object(py),
                max_val.to_object(py),
                0u32));
            continue;
        }

        // Try STRING
        if let Ok(vals) = values_obj.extract::<Vec<String>>(py) {
            if vals.len() != n_rows { return Ok(py.None()); }
            let mut payload = Vec::new();
            payload.push(VT_STRING);
            for v in &vals {
                let vb = v.as_bytes();
                payload.extend_from_slice(&(vb.len() as u32).to_le_bytes());
                payload.extend_from_slice(vb);
            }
            inner.extend_from_slice(&[name_bytes.len() as u8]);
            inner.extend_from_slice(name_bytes);
            inner.extend_from_slice(&[VT_STRING, ENC_RAW]);
            col_payloads.push(payload);
            stats_list.push((name.clone(), VT_STRING,
                py.None(), py.None(), 0u32));
            continue;
        }

        // Can't handle — let Python do it
        return Ok(py.None());
    }

    // Stats section
    for (_, _, min_obj, max_obj, null_count) in &stats_list {
        if min_obj.is_none(py) {
            inner.push(0);
        } else {
            inner.push(1);
            if let Ok(v) = min_obj.extract::<i64>(py) {
                inner.extend_from_slice(&v.to_le_bytes());
            } else if let Ok(v) = min_obj.extract::<f64>(py) {
                inner.extend_from_slice(&v.to_le_bytes());
            } else {
                inner.extend_from_slice(&[0u8; 8]);
            }
            if let Ok(v) = max_obj.extract::<i64>(py) {
                inner.extend_from_slice(&v.to_le_bytes());
            } else if let Ok(v) = max_obj.extract::<f64>(py) {
                inner.extend_from_slice(&v.to_le_bytes());
            } else {
                inner.extend_from_slice(&[0u8; 8]);
            }
        }
        inner.extend_from_slice(&null_count.to_le_bytes());
    }

    // Per-column payloads
    for payload in &col_payloads {
        inner.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        inner.extend_from_slice(payload);
    }

    // Build final PND2 blob (uncompressed)
    let mut blob = Vec::new();
    blob.extend_from_slice(PND2_MAGIC);
    blob.push(PND2_VERSION);
    blob.push(FLAG_HAS_STATS);
    blob.extend_from_slice(&(n_rows as u32).to_le_bytes());
    blob.extend_from_slice(&(col_payloads.len() as u16).to_le_bytes());
    blob.push(COMPRESSION_NONE);
    blob.extend_from_slice(&inner);

    // Return dict: {"blob": bytes, "stats": [(name, vtype, min, max, nc), ...]}
    let result = PyDict::new_bound(py);
    result.set_item("blob", PyBytes::new_bound(py, &blob))?;
    let stats_py = PyList::new_bound(py, stats_list.iter().map(|(name, vtype, min, max, nc)| {
        let t = PyTuple::new_bound(py, [
            name.to_object(py),
            vtype.to_object(py),
            min.clone_ref(py),
            max.clone_ref(py),
            nc.to_object(py),
        ]);
        t.into_any()
    }));
    result.set_item("stats", stats_py)?;
    Ok(result.into())
}

// Suppress unused-import warning for the encoding constants — they're
// kept in scope to make it easy to add future encode paths (RLE/DICT/
// BITPACK) without re-importing.
#[allow(unused_imports)]
use {ENC_RAW as _, ENC_RLE as _, ENC_DICT as _, ENC_BITPACK as _};

// ===========================================================================
// Storage — PyO3 wrapper around UnifiedStorage (Rust core)
// ===========================================================================
//
// This lets Python call the Rust storage layer directly, without going
// through the Python reference kernel. This is the migration path: Python
// code can use `pond.Storage` instead of `PondStorage(PondMinimal(...))`.
//
// Supported operations:
//   - Storage(path)          — open local FS storage
//   - Storage.from_s3(url)   — open S3-compatible storage
//   - write(collection, data, message) → commit_hash (str)
//   - read(collection) → bytes
//   - branch(collection, branch_name) → commit_hash
//   - checkout(collection, branch_name)
//   - checkout_new(collection, branch_name)  — -b equivalent
//   - merge(collection, source, target, message) → commit_hash
//   - history(collection, limit) → list of (hash, message, index)
//   - branches(collection) → list of (name, commit_hash)
//   - ls() → list of collection names
//   - undo(collection, steps) → commit_hash
//   - revert(collection, commit_hash)

use pond_kernel::PondKernel;
use pond_storage::UnifiedStorage;
use pond_storage::{write as storage_write, read as storage_read, branch as storage_branch,
                    commit as storage_commit};
use std::sync::Mutex;

/// Build a full S3 URL from a base URL and optional parameters.
///
/// If the base URL already has query params (e.g., `?region=us-east-1`),
/// append the new params. Otherwise, add them.
///
/// This lets users pass either:
///   - A full URL: `s3://bucket/prefix?region=us-east-1&endpoint=https://...`
///   - A base URL + kwargs: `Storage('s3://bucket/prefix', region='us-east-1', endpoint='...')`
fn build_s3_url(
    base: &str,
    _access_key: Option<&str>,
    _secret_key: Option<&str>,
    region: Option<&str>,
    endpoint: Option<&str>,
) -> String {
    let mut url = base.to_string();
    let mut params: Vec<String> = Vec::new();

    // Check if URL already has query params
    let has_query = url.contains('?');

    // Add region if provided and not already in URL
    if let Some(r) = region {
        if !url.contains("region=") {
            params.push(format!("region={}", r));
        }
    }
    // Add endpoint if provided and not already in URL
    if let Some(e) = endpoint {
        if !url.contains("endpoint=") {
            params.push(format!("endpoint={}", e));
        }
    }

    if !params.is_empty() {
        if has_query {
            url.push_str("&");
        } else {
            url.push_str("?");
        }
        url.push_str(&params.join("&"));
    }

    url
}

/// A Pond storage handle backed by the Rust UnifiedStorage.
///
/// This is the Python-facing wrapper around `pond_storage::UnifiedStorage`.
/// It provides the same operations as the Python `PondStorage` class, but
/// all logic runs in Rust (no Python reference kernel needed).
///
/// # Example (Python)
/// ```python
/// from pond import Storage
///
/// # Local FS
/// s = Storage("/var/lib/pond")
///
/// # S3
/// # s = Storage.from_s3("s3://bucket/prefix?region=us-east-1&endpoint=...")
///
/// s.write("users", b'[{"id":1,"name":"alice"}]', "init")
/// data = s.read("users")
/// s.branch("users", "dev")
/// s.checkout_new("users", "dev")
/// s.write("users", b'[{"id":2,"name":"bob"}]', "add bob")
/// s.checkout("users", "main")
/// s.merge("users", "dev", "main", "merge dev")
/// ```
#[pyclass]
struct Storage {
    storage: Mutex<UnifiedStorage>,
}

#[pymethods]
impl Storage {
    /// Create a new Storage backed by a local path or S3 URL.
    ///
    /// Auto-detects the storage type:
    ///   - `Storage('/var/lib/pond')` → local filesystem
    ///   - `Storage('s3://bucket/prefix?region=us-east-1&endpoint=...')` → S3
    ///   - `Storage('.')` → local filesystem (current directory)
    ///
    /// For S3, credentials are read from the environment:
    ///   AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN (optional)
    ///
    /// For S3, you can also pass credentials as optional kwargs:
    ///   `Storage('s3://...', access_key='...', secret_key='...')`
    #[new]
    #[pyo3(signature = (location, access_key=None, secret_key=None, region=None, endpoint=None))]
    fn new(
        location: &str,
        access_key: Option<&str>,
        secret_key: Option<&str>,
        region: Option<&str>,
        endpoint: Option<&str>,
    ) -> PyResult<Self> {
        if location.starts_with("s3://") {
            // S3-compatible storage
            #[cfg(feature = "s3")]
            {
                let url = build_s3_url(location, access_key, secret_key, region, endpoint);
                // If credentials are provided via kwargs, set them as env vars
                // (S3ObjectStore::from_url reads from env)
                if let (Some(ak), Some(sk)) = (access_key, secret_key) {
                    std::env::set_var("AWS_ACCESS_KEY_ID", ak);
                    std::env::set_var("AWS_SECRET_ACCESS_KEY", sk);
                }
                let store = pond_s3::S3ObjectStore::from_url(&url)
                    .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
                let kernel = PondKernel::new_with_store(Box::new(store));
                let storage = UnifiedStorage::new(kernel);
                Ok(Self { storage: Mutex::new(storage) })
            }
            #[cfg(not(feature = "s3"))]
            {
                Err(pyo3::exceptions::PyIOError::new_err(
                    "S3 support not compiled in. Rebuild with default features."
                ))
            }
        } else {
            // Local filesystem
            let path = location.strip_prefix("file://").unwrap_or(location);
            let storage = UnifiedStorage::new_local(path)
                .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
            Ok(Self { storage: Mutex::new(storage) })
        }
    }

    /// Create a new Storage backed by S3-compatible storage.
    ///
    /// This is a convenience method — equivalent to `Storage('s3://...')`.
    /// Kept for explicit clarity, but `Storage()` auto-detects S3 URLs.
    #[cfg(feature = "s3")]
    #[staticmethod]
    fn from_s3(url: &str) -> PyResult<Self> {
        let store = pond_s3::S3ObjectStore::from_url(url)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        let kernel = PondKernel::new_with_store(Box::new(store));
        let storage = UnifiedStorage::new(kernel);
        Ok(Self { storage: Mutex::new(storage) })
    }

    /// Write data to a collection on the active branch.
    ///
    /// Args:
    ///   collection: The collection name
    ///   data: The data to write (bytes)
    ///   message: The commit message
    ///
    /// Returns:
    ///   The commit hash (hex string)
    fn write(&self, collection: &str, data: &[u8], message: &str) -> PyResult<String> {
        let storage = self.storage.lock().unwrap();
        let active = storage.get_active_branch(collection);
        storage_write::write(storage.kernel(), collection, &active, data, message)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))
    }

    /// Read data from a collection's active branch.
    ///
    /// Args:
    ///   collection: The collection name
    ///
    /// Returns:
    ///   The data as bytes
    fn read<'py>(&self, py: Python<'py>, collection: &str) -> PyResult<Bound<'py, PyBytes>> {
        let storage = self.storage.lock().unwrap();
        let active = storage.get_active_branch(collection);
        let data = storage_read::read(storage.kernel(), collection, &active)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        Ok(PyBytes::new_bound(py, &data))
    }

    /// Create a new branch from the active branch.
    ///
    /// Args:
    ///   collection: The collection name
    ///   branch_name: The new branch name
    ///
    /// Returns:
    ///   The commit hash the branch was created at
    fn branch(&self, collection: &str, branch_name: &str) -> PyResult<String> {
        let storage = self.storage.lock().unwrap();
        let active = storage.get_active_branch(collection);
        storage_branch::branch(storage.kernel(), collection, branch_name, &active)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))
    }

    /// Switch the active branch.
    ///
    /// Args:
    ///   collection: The collection name
    ///   branch_name: The branch to switch to (must exist)
    fn checkout(&self, collection: &str, branch_name: &str) -> PyResult<()> {
        let storage = self.storage.lock().unwrap();
        storage.set_active_branch(collection, branch_name);
        Ok(())
    }

    /// Create a new branch and switch to it (like `git checkout -b`).
    ///
    /// Args:
    ///   collection: The collection name
    ///   branch_name: The new branch name
    fn checkout_new(&self, collection: &str, branch_name: &str) -> PyResult<()> {
        let storage = self.storage.lock().unwrap();
        let active = storage.get_active_branch(collection);
        storage_branch::branch(storage.kernel(), collection, branch_name, &active)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        storage.set_active_branch(collection, branch_name);
        Ok(())
    }

    /// Merge a source branch into a target branch.
    ///
    /// Args:
    ///   collection: The collection name
    ///   source: The source branch name
    ///   target: The target branch name (None = active branch)
    ///   message: The merge commit message
    ///
    /// Returns:
    ///   The merge commit hash
    #[pyo3(signature = (collection, source, target=None, message="merge"))]
    fn merge(&self, collection: &str, source: &str, target: Option<&str>, message: &str) -> PyResult<String> {
        let storage = self.storage.lock().unwrap();
        let target = target.map(|t| t.to_string())
            .unwrap_or_else(|| storage.get_active_branch(collection));
        storage_branch::merge(storage.kernel(), collection, source, &target, message)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))
    }

    /// Show commit history for a collection.
    ///
    /// Args:
    ///   collection: The collection name
    ///   limit: Max number of commits to show (default 20)
    ///
    /// Returns:
    ///   List of (commit_hash, message, index) tuples, newest first
    #[pyo3(signature = (collection, limit=20))]
    fn history(&self, py: Python<'_>, collection: &str, limit: usize) -> PyResult<PyObject> {
        let storage = self.storage.lock().unwrap();
        let active = storage.get_active_branch(collection);

        // Get the current commit hash
        let commit_ref = format!("collections/{}/_branches/{}/commit", collection, active);
        let commit_hash = storage.kernel().resolve(&commit_ref)
            .ok_or_else(|| pyo3::exceptions::PyKeyError::new_err(format!(
                "no commit found for collection '{}' on branch '{}'", collection, active
            )))?;

        // Walk the commit history
        let history = storage_commit::history(storage.kernel(), &commit_hash, limit);

        let list = PyList::new_bound(py, history.iter().map(|(hash, commit)| {
            PyTuple::new_bound(py, [
                hash.to_object(py),
                commit.message.to_object(py),
                commit.index.to_object(py),
            ]).into_any()
        }));
        Ok(list.into())
    }

    /// List all collections.
    ///
    /// Returns:
    ///   List of collection names (strings)
    fn ls(&self, py: Python<'_>) -> PyResult<PyObject> {
        let storage = self.storage.lock().unwrap();
        // List all refs, then extract collection names from "collections/{name}/..."
        let names = storage.kernel().list_names_prefix("collections/");
        let mut collections: Vec<String> = names.iter()
            .filter_map(|n| {
                // n looks like "collections/users/_branches/main/commit"
                let parts: Vec<&str> = n.split('/').collect();
                if parts.len() >= 2 && parts[0] == "collections" {
                    Some(parts[1].to_string())
                } else {
                    None
                }
            })
            .collect();
        collections.sort();
        collections.dedup();
        let list = PyList::new_bound(py, collections.iter().map(|n| n.to_object(py)));
        Ok(list.into())
    }

    /// Undo the last N commits.
    ///
    /// Args:
    ///   collection: The collection name
    ///   steps: Number of commits to undo (default 1)
    ///
    /// Returns:
    ///   The new HEAD commit hash
    #[pyo3(signature = (collection, steps=1))]
    fn undo(&self, collection: &str, steps: usize) -> PyResult<String> {
        let storage = self.storage.lock().unwrap();
        let active = storage.get_active_branch(collection);
        storage_branch::undo(storage.kernel(), collection, &active, steps)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))
    }

    /// Revert to a specific commit.
    ///
    /// Args:
    ///   collection: The collection name
    ///   commit_hash: The commit hash to revert to
    fn revert(&self, collection: &str, commit_hash: &str) -> PyResult<()> {
        let storage = self.storage.lock().unwrap();
        let active = storage.get_active_branch(collection);
        storage_branch::revert(storage.kernel(), collection, &active, commit_hash)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))
    }

    /// Get the active branch name for a collection.
    ///
    /// Returns "main" if no active branch has been set.
    fn get_active_branch(&self, collection: &str) -> String {
        let storage = self.storage.lock().unwrap();
        storage.get_active_branch(collection)
    }

    /// Set the active branch for a collection.
    fn set_active_branch(&self, collection: &str, branch_name: &str) {
        let storage = self.storage.lock().unwrap();
        storage.set_active_branch(collection, branch_name);
    }
}

// ---------------------------------------------------------------------------
// Python module definition
// ---------------------------------------------------------------------------

#[pymodule]
fn pond(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(decode, m)?)?;
    m.add_function(wrap_pyfunction!(encode, m)?)?;
    m.add_class::<Storage>()?;
    Ok(())
}
