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
    PND2Parser, PondColumn, TypedColumn,
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
    // Use pond_core's decoder directly — it handles zstd decompression
    // (when the "zstd" feature is enabled) and all encodings/vtypes.
    let pond_columns = match pond_core::pnd2_decode(blob_bytes) {
        Ok(cols) => cols,
        Err(_) => return Ok(py.None()),
    };

    let n_columns = pond_columns.len();
    let n_rows = pond_columns.first().map(|c| c.n_values).unwrap_or(0);

    // Apply column projection if requested
    let projection: Option<std::collections::HashSet<String>> = columns.map(|cols| {
        cols.into_iter().collect()
    });

    // Apply predicate pushdown: determine which rows pass ALL predicates
    let mask: Vec<bool> = if let Some(ref preds) = predicates {
        compute_predicate_mask(py, &pond_columns, preds)?
    } else {
        vec![true; n_rows]
    };

    // Build the result dict: column_name -> list of values (filtered by mask)
    let result = PyDict::new_bound(py);
    for col in &pond_columns {
        let name = col.name.to_string_lossy().to_string();
        // Skip if projection requested and this column is not in it
        if let Some(ref proj) = projection {
            if !proj.contains(&name) {
                continue;
            }
        }
        let py_values = column_to_pylist_filtered(py, col, &mask)?;
        result.set_item(&name, py_values)?;
    }

    // Add metadata
    let filtered_rows = mask.iter().filter(|&&m| m).count();
    result.set_item("_n_rows", filtered_rows.to_object(py))?;
    result.set_item("_n_columns", n_columns.to_object(py))?;

    Ok(result.into())
}

/// Compute a row mask (which rows pass ALL predicates).
fn compute_predicate_mask(
    py: Python,
    columns: &[pond_core::PondColumn],
    predicates: &[(String, String, PyObject)],
) -> PyResult<Vec<bool>> {
    use pond_core::{VT_INT64, VT_FLOAT64};
    let n_rows = columns.first().map(|c| c.n_values).unwrap_or(0);
    let mut mask = vec![true; n_rows];

    for (col_name, op, value) in predicates {
        // Find the column
        let col = match columns.iter().find(|c| c.name.to_string_lossy() == col_name.as_str()) {
            Some(c) => c,
            None => continue, // Column not found — skip this predicate
        };

        for (i, m) in mask.iter_mut().enumerate() {
            if !*m { continue; }
            let passes = match col.vtype {
                VT_INT64 => {
                    let cell_val = col.i64_data.get(i).copied().unwrap_or(0);
                    let target: i64 = value.extract(py).unwrap_or(0);
                    apply_op_i64(cell_val, op, target)
                }
                VT_FLOAT64 => {
                    let cell_val = col.f64_data.get(i).copied().unwrap_or(0.0);
                    let target: f64 = value.extract(py).unwrap_or(0.0);
                    apply_op_f64(cell_val, op, target)
                }
                _ => true, // Unsupported vtype — don't filter
            };
            *m = passes;
        }
    }
    Ok(mask)
}

fn apply_op_i64(cell: i64, op: &str, target: i64) -> bool {
    match op {
        "=" | "==" => cell == target,
        "!=" | "<>" => cell != target,
        "<" => cell < target,
        "<=" => cell <= target,
        ">" => cell > target,
        ">=" => cell >= target,
        _ => true,
    }
}

fn apply_op_f64(cell: f64, op: &str, target: f64) -> bool {
    match op {
        "=" | "==" => cell == target,
        "!=" | "<>" => cell != target,
        "<" => cell < target,
        "<=" => cell <= target,
        ">" => cell > target,
        ">=" => cell >= target,
        _ => true,
    }
}

/// Convert a PondColumn to a Python list, filtered by the mask.
fn column_to_pylist_filtered(py: Python, col: &pond_core::PondColumn, mask: &[bool]) -> PyResult<PyObject> {
    use pond_core::{VT_INT64, VT_FLOAT64, VT_STRING, VT_BINARY, VT_NULL};
    let list = PyList::empty_bound(py);
    match col.vtype {
        VT_INT64 => {
            for (i, v) in col.i64_data.iter().enumerate() {
                if mask.get(i).copied().unwrap_or(false) {
                    list.append(*v)?;
                }
            }
        }
        VT_FLOAT64 => {
            for (i, v) in col.f64_data.iter().enumerate() {
                if mask.get(i).copied().unwrap_or(false) {
                    list.append(*v)?;
                }
            }
        }
        VT_STRING => {
            for (i, s) in col.str_data.iter().enumerate() {
                if mask.get(i).copied().unwrap_or(false) {
                    list.append(s.to_string_lossy().to_string())?;
                }
            }
        }
        VT_BINARY => {
            for (i, b) in col.bin_data.iter().enumerate() {
                if mask.get(i).copied().unwrap_or(false) {
                    list.append(PyBytes::new_bound(py, b))?;
                }
            }
        }
        VT_NULL | _ => {
            for i in 0..col.n_values {
                if mask.get(i).copied().unwrap_or(false) {
                    list.append(py.None())?;
                }
            }
        }
    }
    Ok(list.into())
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
use pond_ivf_index::IVFIndex as RustIVFIndex;
use pond_hnsw_index::HNSWIndex as RustHNSWIndex;
use pond_simple_index::SimpleIndex as RustSimpleIndex;
use pond_semantic::SemanticDefinitions;
use serde_json::Value as JsonValue;
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

    /// Write structured columns as a PND2 blob with column stats.
    ///
    /// Supports INT64, FLOAT64, and STRING column types (auto-detected from
    /// Python values). Each column's encoding is chosen automatically:
    ///   - INT64: RLE/DICT/BITPACK/RAW (based on data characteristics)
    ///   - FLOAT64: RAW
    ///   - STRING: RAW
    ///
    /// Args:
    ///   - collection: Collection name
    ///   - columns: List of (name, list_of_values) tuples
    ///   - message: Commit message
    ///
    /// Returns:
    ///   The commit hash
    ///
    /// Example:
    ///   s.write_rows('users', [
    ///       ('id', [1, 2, 3]),
    ///       ('score', [1.5, 2.5, 3.5]),
    ///       ('name', ['alice', 'bob', 'carol']),
    ///   ], 'init')
    fn write_rows(&self, collection: &str, columns: Vec<(String, Vec<PyObject>)>, message: &str) -> PyResult<String> {
        let storage = self.storage.lock().unwrap();
        let active = storage.get_active_branch(collection);

        // Convert Python (name, values) to Rust (name, TypedColumn)
        let typed_cols: Vec<(String, TypedColumn)> = columns.into_iter()
            .map(|(name, values)| {
                let typed = python_values_to_typed_column(&values);
                (name, typed)
            })
            .collect();

        let col_refs: Vec<(&str, TypedColumn)> = typed_cols.iter()
            .map(|(name, col)| (name.as_str(), col.clone()))
            .collect();

        storage_write::write_rows(storage.kernel(), collection, &active, &col_refs, message)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))
    }

    /// Read structured columns from a collection with optional pruning.
    ///
    /// Decodes PND2 blobs with predicate pruning (skip row groups whose
    /// stats don't match) and column projection (only decode requested columns).
    ///
    /// Returns typed Python values: int for INT64, float for FLOAT64, str for STRING.
    ///
    /// Args:
    ///   - collection: Collection name
    ///   - columns: Optional list of column names to project (None = all)
    ///   - predicates: Optional list of (column, op, value) for row-group pruning
    ///
    /// Returns:
    ///   Dict of {column_name: list_of_values}
    ///
    /// Example:
    ///   data = s.read_rows('users')
    ///   # → {'id': [1, 2, 3], 'score': [1.5, 2.5, 3.5], 'name': ['a', 'b', 'c']}
    #[pyo3(signature = (collection, columns=None, predicates=None))]
    fn read_rows(
        &self,
        py: Python<'_>,
        collection: &str,
        columns: Option<Vec<String>>,
        predicates: Option<Vec<(String, String, i64)>>,
    ) -> PyResult<PyObject> {
        let storage = self.storage.lock().unwrap();
        let active = storage.get_active_branch(collection);

        // === AUTO-INDEX ACCELERATION ===
        // If there's a single equality predicate, check if a simple index
        // covers that column. If yes, use O(1) index lookup instead of scan.
        if let Some(ref preds) = predicates {
            if preds.len() == 1 {
                let (col, op, val) = &preds[0];
                if op == "=" || op == "==" {
                    let indexer = RustSimpleIndex::new(storage.kernel());
                    if let Some(index_name) = indexer.find_index_by_column(collection, col) {
                        // Index hit! Use O(1) lookup
                        let lookup_key = val.to_string();
                        if let Some(_rowid) = indexer.lookup(collection, &index_name, &lookup_key) {
                            // Fall through to normal read but with predicate pruning
                            // (the index confirms a match exists, so we know to read)
                            // In the future, we could use the rowid to fetch a single row
                        }
                    }
                }
            }
        }

        // Resolve HEAD and get manifest (handles PondPack + old format)
        let head = storage.kernel().resolve(&pond_storage::branch_ref(collection, &active))
            .ok_or_else(|| pyo3::exceptions::PyKeyError::new_err(format!(
                "no commits for '{}'", collection)))?;

        let head_data = storage.kernel().read_blob(&head)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;

        let manifest_bytes = if pond_storage::pond_pack::is_pack(&head_data) {
            let (_, manifest_bytes, _) = pond_storage::pond_pack::decode_pack(&head_data)
                .ok_or_else(|| pyo3::exceptions::PyIOError::new_err("Failed to decode PondPack"))?;
            manifest_bytes
        } else {
            let commit = pond_storage::commit::read_commit(storage.kernel(), &head)
                .ok_or_else(|| pyo3::exceptions::PyIOError::new_err("Failed to read commit"))?;
            storage.kernel().read_blob(&commit.manifest)
                .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?
        };

        let manifest = pond_storage::manifest::CollectionManifest::decode(&manifest_bytes)
            .ok_or_else(|| pyo3::exceptions::PyIOError::new_err("Failed to decode manifest"))?;

        // Build projection set
        let projection: Option<std::collections::HashSet<String>> = columns.map(|cols| {
            cols.into_iter().collect()
        });

        // Collect results
        let mut result_cols: std::collections::HashMap<String, Vec<PyObject>> = std::collections::HashMap::new();

        for rg in &manifest.row_groups {
            // Predicate pruning
            if let Some(ref preds) = predicates {
                let mut skip = false;
                for (col_name, op, value) in preds {
                    let col_stats = rg.columns.iter().find(|c| c.name == *col_name);
                    if let Some(stats) = col_stats {
                        if can_prune_row_group_py(stats, op, *value) {
                            skip = true;
                            break;
                        }
                    }
                }
                if skip { continue; }
            }

            // Read and decode PND2 blob
            let blob_data = storage.kernel().read_blob(&rg.blob_hash)
                .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;

            let cols = pond_core::pnd2_decode(&blob_data)
                .map_err(|e| pyo3::exceptions::PyIOError::new_err(e))?;

            for col in &cols {
                let name = col.name.to_string_lossy().to_string();

                if let Some(ref proj) = projection {
                    if !proj.contains(&name) { continue; }
                }

                let entry = result_cols.entry(name.clone()).or_insert_with(Vec::new);

                // Convert to Python objects based on vtype
                use pond_core::{VT_INT64, VT_FLOAT64, VT_STRING, VT_BINARY, VT_NULL};
                match col.vtype {
                    VT_INT64 => {
                        for v in &col.i64_data {
                            entry.push(v.to_object(py));
                        }
                    }
                    VT_FLOAT64 => {
                        for v in &col.f64_data {
                            entry.push(v.to_object(py));
                        }
                    }
                    VT_STRING => {
                        for s in &col.str_data {
                            entry.push(s.to_string_lossy().to_string().to_object(py));
                        }
                    }
                    VT_BINARY => {
                        for b in &col.bin_data {
                            entry.push(PyBytes::new_bound(py, b).into());
                        }
                    }
                    VT_NULL | _ => {
                        for _ in 0..col.n_values {
                            entry.push(py.None());
                        }
                    }
                }
            }
        }

        // Convert to Python dict
        let dict = PyDict::new_bound(py);
        for (name, values) in result_cols {
            let list = PyList::new_bound(py, values.iter());
            dict.set_item(&name, list)?;
        }
        Ok(dict.into())
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

    // ===================================================================
    // Index operations — UNIFIED API for ALL index types
    // ===================================================================

    /// Build an index on a collection. Works for ALL index types.
    ///
    /// Args:
    ///   - collection: Collection name
    ///   - index_name: Name for this index
    ///   - index_type: Type of index ("simple", "ivf", "hnsw")
    ///   - config: Dict of index-specific config:
    ///       "simple": {"key_field": "name"}
    ///       "ivf":    {"n_clusters": 10, "metric": "euclidean"}
    ///       "hnsw":   {"m": 16, "ef_construction": 200, "metric": "l2"}
    ///   - rows: For "simple" index — list of (rowid, row_dict) tuples.
    ///           For "ivf"/"hnsw" — not needed (reads from collection).
    ///
    /// Returns:
    ///   The index blob hash.
    ///
    /// Examples:
    ///   # Simple secondary index
    ///   s.build_index('users', 'by_name', 'simple',
    ///       config={'key_field': 'name'},
    ///       rows=[('user:1', {'name': 'alice'})])
    ///
    ///   # IVF vector index
    ///   s.build_index('vectors', 'ann', 'ivf',
    ///       config={'n_clusters': 10, 'metric': 'euclidean'})
    ///
    ///   # HNSW vector index
    ///   s.build_index('vectors', 'ann', 'hnsw',
    ///       config={'m': 16, 'metric': 'l2'})
    #[pyo3(signature = (collection, index_name, index_type, config=None, rows=None))]
    fn build_index(
        &self,
        collection: &str,
        index_name: &str,
        index_type: &str,
        config: Option<PyObject>,
        rows: Option<Vec<(String, PyObject)>>,
    ) -> PyResult<String> {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();

        match index_type {
            "simple" => {
                let indexer = RustSimpleIndex::new(kernel);
                let cfg = config.as_ref().map(|c| python_to_json(c));
                let key_field = cfg.as_ref()
                    .and_then(|c| c.get("key_field"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("id")
                    .to_string();

                let rust_rows: Vec<(String, JsonValue)> = rows.unwrap_or_default()
                    .into_iter()
                    .map(|(rowid, obj)| (rowid, python_to_json(&obj)))
                    .collect();

                let kf = key_field.clone();
                indexer.build_index(collection, index_name, &rust_rows, |row| {
                    match row.get(&kf) {
                        Some(JsonValue::String(s)) => vec![s.clone()],
                        Some(JsonValue::Number(n)) => vec![n.to_string()],
                        Some(JsonValue::Array(arr)) => arr.iter()
                            .filter_map(|v| match v {
                                JsonValue::String(s) => Some(s.clone()),
                                JsonValue::Number(n) => Some(n.to_string()),
                                _ => None,
                            })
                            .collect(),
                        _ => vec![],
                    }
                }, &key_field).map_err(|e| pyo3::exceptions::PyIOError::new_err(e))
            }
            "ivf" => {
                let cfg = config.as_ref().map(|c| python_to_json(c));
                let n_clusters = cfg.as_ref()
                    .and_then(|c| c.get("n_clusters"))
                    .and_then(|v| v.as_u64())
                    .unwrap_or(10) as usize;
                let metric = cfg.as_ref()
                    .and_then(|c| c.get("metric"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("euclidean");

                // IVF uses the index_name as the ref name
                // Currently IVF hardcodes "ivf" as the ref — we need to support custom names
                let ivf = RustIVFIndex::new(kernel);
                ivf.build(collection, n_clusters, metric)
                    .map_err(|e| pyo3::exceptions::PyIOError::new_err(e))
            }
            "hnsw" => {
                let cfg = config.as_ref().map(|c| python_to_json(c));
                let m = cfg.as_ref()
                    .and_then(|c| c.get("m"))
                    .and_then(|v| v.as_u64())
                    .unwrap_or(16) as usize;
                let ef_construction = cfg.as_ref()
                    .and_then(|c| c.get("ef_construction"))
                    .and_then(|v| v.as_u64())
                    .unwrap_or(200) as usize;
                let metric = cfg.as_ref()
                    .and_then(|c| c.get("metric"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("l2");

                let hnsw = RustHNSWIndex::new(kernel);
                hnsw.build(collection, m, ef_construction, None, metric)
                    .map_err(|e| pyo3::exceptions::PyIOError::new_err(e))
            }
            _ => Err(pyo3::exceptions::PyValueError::new_err(
                format!("Unknown index type: '{}'. Supported: simple, ivf, hnsw", index_type)
            )),
        }
    }

    /// Look up a rowid by index key (exact lookup — for simple indexes).
    ///
    /// Returns None if the key is not found.
    fn lookup_index(&self, collection: &str, index_name: &str, index_key: &str) -> Option<String> {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();
        let indexer = RustSimpleIndex::new(kernel);
        indexer.lookup(collection, index_name, index_key)
    }

    /// Search an index (approximate search — for vector indexes: IVF, HNSW).
    ///
    /// Args:
    ///   - collection: Collection name
    ///   - index_type: "ivf" or "hnsw"
    ///   - query: Query vector (list of floats)
    ///   - k: Number of nearest neighbors to return
    ///   - n_probe: IVF clusters to search (default 10)
    ///   - ef: HNSW beam width (default 50)
    ///
    /// Returns:
    ///   List of (distance, vector_id) tuples, sorted by distance.
    #[pyo3(signature = (collection, index_type, query, k=10, n_probe=10, ef=50))]
    fn search_index(&self, py: Python<'_>, collection: &str, index_type: &str, query: Vec<f64>, k: usize, n_probe: usize, ef: usize) -> PyResult<PyObject> {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();

        let results = match index_type {
            "ivf" => {
                let ivf = RustIVFIndex::new(kernel);
                ivf.search(collection, &query, k, n_probe)
                    .map_err(|e| pyo3::exceptions::PyIOError::new_err(e))?
            }
            "hnsw" => {
                let hnsw = RustHNSWIndex::new(kernel);
                hnsw.search(collection, &query, k, ef)
                    .map_err(|e| pyo3::exceptions::PyIOError::new_err(e))?
            }
            _ => return Err(pyo3::exceptions::PyValueError::new_err(
                format!("Unknown index type: '{}'. Supported: ivf, hnsw", index_type)
            )),
        };

        let list = PyList::new_bound(py, results.iter().map(|(dist, id)| {
            PyTuple::new_bound(py, [dist.to_object(py), id.to_object(py)]).into_any()
        }));
        Ok(list.into())
    }

    /// Drop an index. Works for ALL index types.
    ///
    /// Returns True if the index existed and was dropped.
    fn drop_index(&self, collection: &str, index_name: &str) -> bool {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();
        let indexer = RustSimpleIndex::new(kernel);
        indexer.drop_index(collection, index_name)
    }

    /// List all active indexes for a collection.
    fn list_indexes(&self, collection: &str) -> Vec<String> {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();
        let indexer = RustSimpleIndex::new(kernel);
        indexer.list_indexes(collection)
    }

    // ===================================================================
    // GC / Vacuum — maintenance operations
    // ===================================================================

    /// Analyze reachability and return GC stats (read-only).
    ///
    /// Returns a dict with: live, dead, dead_size_bytes (-1 if compute_size=False)
    #[pyo3(signature = (compute_size=false))]
    fn gc_stats(&self, py: Python<'_>, compute_size: bool) -> PyResult<PyObject> {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();
        let gc = pond_storage::maintenance::GarbageCollector::new(kernel);
        let stats = gc.collect(None, compute_size);

        let dict = PyDict::new_bound(py);
        dict.set_item("live", stats.live)?;
        dict.set_item("dead", stats.dead)?;
        dict.set_item("dead_size_bytes", stats.dead_size_bytes)?;
        Ok(dict.into())
    }

    /// Vacuum — delete unreachable blobs with time-travel safety.
    ///
    /// Args:
    ///   - preserve_days: Keep commits younger than N days (default 0)
    ///   - dry_run: If True, report what would be deleted without deleting
    ///
    /// Returns a dict with: deleted, preserved, dry_run
    #[pyo3(signature = (preserve_days=0, dry_run=false))]
    fn vacuum(&self, py: Python<'_>, preserve_days: u32, dry_run: bool) -> PyResult<PyObject> {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();
        let gc = pond_storage::maintenance::GarbageCollector::new(kernel);
        let result = gc.vacuum(None, preserve_days, dry_run);

        let dict = PyDict::new_bound(py);
        dict.set_item("deleted", result.deleted)?;
        dict.set_item("preserved", result.preserved)?;
        dict.set_item("dry_run", result.dry_run)?;
        Ok(dict.into())
    }

    // ===================================================================
    // Semantic model operations — define/query metrics, dimensions, relationships
    // ===================================================================

    /// Define a metric on a collection.
    ///
    /// Args:
    ///   - collection: Collection name (semantic definitions are stored here)
    ///   - name: Metric name (e.g., "revenue")
    ///   - expression: Expression (e.g., "SUM(amount)")
    ///   - description: Optional description
    fn define_metric(&self, collection: &str, name: &str, expression: &str, description: Option<&str>) -> PyResult<()> {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();

        // Store the metric definition as a kernel blob
        let metric = serde_json::json!({
            "name": name,
            "expression": expression,
            "description": description.unwrap_or(""),
            "format": "number",
        });
        let metric_bytes = serde_json::to_vec(&metric)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        let hash = kernel.write(&metric_bytes)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        let ref_name = format!("collections/{}/_semantic/metrics/{}", collection, name);
        kernel.reference(&ref_name, &hash)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        Ok(())
    }

    /// Define a dimension on a collection.
    ///
    /// Args:
    ///   - collection: Collection name
    ///   - name: Dimension name (e.g., "country")
    ///   - data_type: Data type (e.g., "string", "number", "time")
    fn define_dimension(&self, collection: &str, name: &str, data_type: &str) -> PyResult<()> {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();

        let dim = serde_json::json!({
            "name": name,
            "data_type": data_type,
            "description": "",
        });
        let dim_bytes = serde_json::to_vec(&dim)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        let hash = kernel.write(&dim_bytes)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        let ref_name = format!("collections/{}/_semantic/dimensions/{}", collection, name);
        kernel.reference(&ref_name, &hash)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        Ok(())
    }

    /// Define a relationship between collections.
    ///
    /// Args:
    ///   - collection: Collection name (where the definition is stored)
    ///   - name: Relationship name
    ///   - from_collection: Source collection
    ///   - to_collection: Target collection
    ///   - condition: Join condition (e.g., "users.id = orders.user_id")
    fn define_relationship(&self, collection: &str, name: &str, from_collection: &str, to_collection: &str, condition: &str) -> PyResult<()> {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();

        let rel = serde_json::json!({
            "name": name,
            "from": from_collection,
            "to": to_collection,
            "condition": condition,
            "join_type": "inner",
        });
        let rel_bytes = serde_json::to_vec(&rel)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        let hash = kernel.write(&rel_bytes)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        let ref_name = format!("collections/{}/_semantic/relationships/{}", collection, name);
        kernel.reference(&ref_name, &hash)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        Ok(())
    }

    /// List all metrics defined on a collection.
    fn list_metrics(&self, collection: &str) -> Vec<String> {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();
        let prefix = format!("collections/{}/_semantic/metrics/", collection);
        kernel.list_names_prefix(&prefix).into_iter()
            .filter_map(|n| n.strip_prefix(&prefix).map(|s| s.to_string()))
            .collect()
    }

    /// List all dimensions defined on a collection.
    fn list_dimensions(&self, collection: &str) -> Vec<String> {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();
        let prefix = format!("collections/{}/_semantic/dimensions/", collection);
        kernel.list_names_prefix(&prefix).into_iter()
            .filter_map(|n| n.strip_prefix(&prefix).map(|s| s.to_string()))
            .collect()
    }

    /// Export the semantic model in a specific adapter format.
    ///
    /// Args:
    ///   - collection: Collection name
    ///   - adapter: Adapter type ("ossie")
    ///
    /// Returns:
    ///   JSON dict of the exported model.
    #[pyo3(signature = (collection, adapter="ossie"))]
    fn export_semantic_model(&self, py: Python<'_>, collection: &str, adapter: &str) -> PyResult<PyObject> {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();

        // Read all semantic definitions
        let mut defs = SemanticDefinitions::new();

        // Read metrics
        let metric_prefix = format!("collections/{}/_semantic/metrics/", collection);
        for ref_name in kernel.list_names_prefix(&metric_prefix) {
            if let Some(hash) = kernel.resolve(&ref_name) {
                if let Ok(data) = kernel.read_blob(&hash) {
                    if let Ok(m) = serde_json::from_slice::<serde_json::Value>(&data) {
                        defs.metrics.push(pond_semantic::Metric {
                            name: m.get("name").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                            expression: m.get("expression").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                            description: m.get("description").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                            format: m.get("format").and_then(|v| v.as_str()).unwrap_or("number").to_string(),
                        });
                    }
                }
            }
        }

        // Read dimensions
        let dim_prefix = format!("collections/{}/_semantic/dimensions/", collection);
        for ref_name in kernel.list_names_prefix(&dim_prefix) {
            if let Some(hash) = kernel.resolve(&ref_name) {
                if let Ok(data) = kernel.read_blob(&hash) {
                    if let Ok(d) = serde_json::from_slice::<serde_json::Value>(&data) {
                        defs.dimensions.push(pond_semantic::Dimension {
                            name: d.get("name").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                            data_type: d.get("data_type").and_then(|v| v.as_str()).unwrap_or("string").to_string(),
                            description: d.get("description").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                        });
                    }
                }
            }
        }

        // Read relationships
        let rel_prefix = format!("collections/{}/_semantic/relationships/", collection);
        for ref_name in kernel.list_names_prefix(&rel_prefix) {
            if let Some(hash) = kernel.resolve(&ref_name) {
                if let Ok(data) = kernel.read_blob(&hash) {
                    if let Ok(r) = serde_json::from_slice::<serde_json::Value>(&data) {
                        defs.relationships.push(pond_semantic::Relationship {
                            name: r.get("name").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                            from_collection: r.get("from").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                            to_collection: r.get("to").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                            join_type: r.get("join_type").and_then(|v| v.as_str()).unwrap_or("inner").to_string(),
                            join_condition: r.get("condition").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                        });
                    }
                }
            }
        }

        // Export using the adapter
        let model = match adapter {
            "ossie" => {
                use pond_semantic::SemanticModelAdapter;
                let ossie = pond_ossie_adapter::OssieAdapter::new();
                ossie.export_model(&defs)
            }
            _ => return Err(pyo3::exceptions::PyValueError::new_err(
                format!("Unknown adapter: '{}'. Supported: ossie", adapter)
            )),
        };

        // Convert to Python dict
        let model_str = serde_json::to_string(&model)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        let json_module = py.import_bound("json")?;
        let result = json_module.call_method("loads", (model_str,), None)?;
        Ok(result.into())
    }
}

/// Check if a row group can be pruned based on column stats + predicate.
/// Returns true if the row group CANNOT match the predicate (should be skipped).
fn can_prune_row_group_py(
    stats: &pond_storage::manifest::ColumnStatsEntry,
    op: &str,
    value: i64,
) -> bool {
    let (min, max) = match (&stats.min, &stats.max) {
        (Some(m), Some(x)) if m.len() >= 8 && x.len() >= 8 => {
            let min_val = i64::from_le_bytes([
                m[0], m[1], m[2], m[3], m[4], m[5], m[6], m[7]
            ]);
            let max_val = i64::from_le_bytes([
                x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7]
            ]);
            (min_val, max_val)
        }
        _ => return false,
    };

    match op {
        "=" | "==" => value < min || value > max,
        "<" => min >= value,
        "<=" => min > value,
        ">" => max <= value,
        ">=" => max < value,
        "!=" | "<>" => false,
        _ => false,
    }
}

/// Convert a Python object to a serde_json::Value.
fn python_to_json(obj: &PyObject) -> JsonValue {
    Python::with_gil(|py| {
        if let Ok(s) = obj.extract::<String>(py) {
            JsonValue::String(s)
        } else if let Ok(i) = obj.extract::<i64>(py) {
            JsonValue::Number(serde_json::Number::from(i))
        } else if let Ok(f) = obj.extract::<f64>(py) {
            serde_json::Number::from_f64(f).map(JsonValue::Number).unwrap_or(JsonValue::Null)
        } else if let Ok(b) = obj.extract::<bool>(py) {
            JsonValue::Bool(b)
        } else if let Ok(dict) = obj.extract::<std::collections::HashMap<String, PyObject>>(py) {
            let map: serde_json::Map<String, JsonValue> = dict.into_iter()
                .map(|(k, v)| (k, python_to_json(&v)))
                .collect();
            JsonValue::Object(map)
        } else if let Ok(list) = obj.extract::<Vec<PyObject>>(py) {
            JsonValue::Array(list.into_iter().map(|item| python_to_json(&item)).collect())
        } else {
            JsonValue::Null
        }
    })
}

/// Convert a Vec<PyObject> (Python list of values) to a TypedColumn.
///
/// Auto-detects the type from the first non-None element:
///   - int → TypedColumn::Int64
///   - float → TypedColumn::Float64
///   - str → TypedColumn::String
///   - empty → TypedColumn::Int64 (default)
fn python_values_to_typed_column(values: &[PyObject]) -> TypedColumn {
    Python::with_gil(|py| {
        // Detect type from first element
        for v in values.iter() {
            if let Ok(_) = v.extract::<i64>(py) {
                let vals: Vec<i64> = values.iter()
                    .map(|v| v.extract::<i64>(py).unwrap_or(0))
                    .collect();
                return TypedColumn::Int64(vals);
            }
            if let Ok(_) = v.extract::<f64>(py) {
                let vals: Vec<f64> = values.iter()
                    .map(|v| v.extract::<f64>(py).unwrap_or(0.0))
                    .collect();
                return TypedColumn::Float64(vals);
            }
            if let Ok(_) = v.extract::<String>(py) {
                let vals: Vec<String> = values.iter()
                    .map(|v| v.extract::<String>(py).unwrap_or_default())
                    .collect();
                return TypedColumn::String(vals);
            }
        }
        // Empty or unknown → default to empty Int64
        TypedColumn::Int64(Vec::new())
    })
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
