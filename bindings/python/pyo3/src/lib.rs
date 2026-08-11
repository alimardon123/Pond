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
    storage: Arc<Mutex<UnifiedStorage>>,
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
                Ok(Self { storage: Arc::new(Mutex::new(storage)) })
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
            Ok(Self { storage: Arc::new(Mutex::new(storage)) })
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
        Ok(Self { storage: Arc::new(Mutex::new(storage)) })
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
    /// **CRDT by default**: auto-adds `_rowid` (UUIDv7) and `_version` (HLC)
    /// columns if not already present. This makes all data written via
    /// write_rows compatible with upsert_shard / delete_shard (which match
    /// by _rowid). Set `crdt=False` to opt out (raw bulk load, no CRDT).
    ///
    /// Args:
    ///   - collection: Collection name
    ///   - columns: List of (name, list_of_values) tuples
    ///   - message: Commit message
    ///   - crdt: If True (default), auto-add _rowid + _version columns
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
    ///   # → automatically adds _rowid + _version columns
    ///   # → data is now compatible with upsert_shard / delete_shard
    #[pyo3(signature = (collection, columns, message, crdt=true))]
    fn write_rows(&self, collection: &str, columns: Vec<(String, Vec<PyObject>)>, message: &str, crdt: bool) -> PyResult<String> {
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

        if crdt {
            storage_write::write_rows(storage.kernel(), collection, &active, &col_refs, message)
                .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))
        } else {
            storage_write::write_rows_no_crdt(storage.kernel(), collection, &active, &col_refs, message)
                .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))
        }
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

        // === AUTO-INDEX ACCELERATION (multi-key + composite) ===
        // For EACH equality predicate, check if a simple index covers that column.
        // - Single-column index: O(1) exact lookup
        // - Composite index: prefix scan (check if any key contains the value)
        // If an index exists AND the lookup key is not found → return empty (early exit).
        if let Some(ref preds) = predicates {
            let indexer = RustSimpleIndex::new(storage.kernel());
            for (col, op, val) in preds {
                if op == "=" || op == "==" {
                    if let Some(index_name) = indexer.find_index_by_column(collection, col) {
                        let lookup_key = val.to_string();
                        // Check if this is a composite index
                        let key_fields = indexer.get_index_key_fields(collection, &index_name);
                        let is_composite = key_fields.as_ref()
                            .map(|f| f.len() > 1)
                            .unwrap_or(false);

                        if is_composite {
                            // Composite index: scan all keys for a match
                            // (can't do O(1) lookup on individual column of composite key)
                            // Read the full index and check if any key contains the value
                            let ref_name = format!("collections/{}/indexes/{}", collection, index_name);
                            if let Some(hash) = storage.kernel().resolve(&ref_name) {
                                if let Ok(data) = storage.kernel().read_blob(&hash) {
                                    if let Ok(index) = serde_json::from_slice::<std::collections::HashMap<String, String>>(&data) {
                                        // Check if any key contains the lookup value as a component
                                        let found = index.keys().any(|k| {
                                            // Split by unit separator and check each component
                                            k.split('\x1f').any(|comp| comp == lookup_key)
                                        });
                                        if !found {
                                            let dict = PyDict::new_bound(py);
                                            return Ok(dict.into());
                                        }
                                    }
                                }
                            }
                        } else {
                            // Single-column index: O(1) exact lookup
                            match indexer.lookup(collection, &index_name, &lookup_key) {
                                None => {
                                    let dict = PyDict::new_bound(py);
                                    return Ok(dict.into());
                                }
                                Some(_) => {}
                            }
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

        // Convert to Python dict — filter out CRDT metadata columns
        // (_rowid, _version, _deleted) unless the user explicitly requested
        // them via the columns= parameter.
        let crdt_cols: std::collections::HashSet<&str> = ["_rowid", "_version", "_deleted"]
            .iter().cloned().collect();
        let explicit_columns = projection.is_some();

        let dict = PyDict::new_bound(py);
        for (name, values) in result_cols {
            // Skip CRDT metadata columns unless explicitly requested
            if !explicit_columns && crdt_cols.contains(name.as_str()) {
                continue;
            }
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
    #[pyo3(signature = (collection, index_name, index_type, config=None))]
    fn build_index(
        &self,
        collection: &str,
        index_name: &str,
        index_type: &str,
        config: Option<PyObject>,
    ) -> PyResult<String> {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();

        match index_type {
            "simple" => {
                let indexer = RustSimpleIndex::new(kernel);
                let cfg = config.as_ref().map(|c| python_to_json(c));
                // Support both key_field (string) and key_fields (list) in config
                let key_fields: Vec<String> = if let Some(ref c) = cfg {
                    if let Some(arr) = c.get("key_fields").and_then(|v| v.as_array()) {
                        arr.iter()
                            .filter_map(|v| v.as_str().map(|s| s.to_string()))
                            .collect()
                    } else if let Some(s) = c.get("key_field").and_then(|v| v.as_str()) {
                        vec![s.to_string()]
                    } else {
                        vec!["id".to_string()]
                    }
                } else {
                    vec!["id".to_string()]
                };

                // AUTO-READ from the collection — no `rows` parameter needed.
                // Read all rows from HEAD + shards, convert to (rowid, JSON row) pairs.
                let rust_rows = read_collection_as_json_rows(&storage, collection, &key_fields)
                    .map_err(|e| pyo3::exceptions::PyIOError::new_err(e))?;

                let kf_refs: Vec<&str> = key_fields.iter().map(|s| s.as_str()).collect();
                indexer.build_index(collection, index_name, &rust_rows, |row| {
                    // Build composite key from all key_fields
                    let mut parts: Vec<String> = Vec::new();
                    for kf in &key_fields {
                        match row.get(kf) {
                            Some(JsonValue::String(s)) => parts.push(s.clone()),
                            Some(JsonValue::Number(n)) => parts.push(n.to_string()),
                            Some(JsonValue::Array(arr)) => {
                                for v in arr {
                                    match v {
                                        JsonValue::String(s) => parts.push(s.clone()),
                                        JsonValue::Number(n) => parts.push(n.to_string()),
                                        _ => {}
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                    // For composite keys: join with separator
                    if parts.len() > 1 {
                        vec![parts.join("\x1f")]  // ASCII unit separator
                    } else {
                        parts
                    }
                }, &kf_refs).map_err(|e| pyo3::exceptions::PyIOError::new_err(e))
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

                // IVF reads from the collection directly (internal)
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

                // HNSW reads from the collection directly (internal)
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
    // CRDT Shards — concurrent multi-writer without coordination
    //
    // Shards allow multiple writers to write concurrently without CAS:
    //   - Each writer writes its own shard to a unique path
    //   - Readers union HEAD + all live shards via read_with_shards
    //   - compact_shards merges shards into HEAD (clears the shard list)
    //
    // Row-level CRDT operations (upsert_shard, delete_shard) add _rowid
    // + _version to each row, enabling deterministic merge on conflict
    // (latest _version wins, tombstones suppress).
    // ===================================================================

    /// Append a CRDT shard to the active branch.
    ///
    /// The shard is written to a unique path. Readers will discover and
    /// merge it via read_with_shards. No CAS, no coordination — works
    /// on any object store (local FS, S3, R2, MinIO, ...).
    ///
    /// Args:
    ///   - collection: Collection name
    ///   - shard_name: Unique name for this shard (e.g., 'writer1_001')
    ///   - data: The shard data (raw bytes — JSON, PND2, anything)
    ///
    /// Returns: shard blob hash
    fn append_shard(&self, collection: &str, shard_name: &str, data: &[u8]) -> PyResult<String> {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();
        let active = storage.get_active_branch(collection);
        pond_storage::shard::append_shard(kernel, collection, &active, shard_name, data)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e))
    }

    /// Upsert rows as a CRDT shard with _rowid + _version.
    ///
    /// Each row gets:
    ///   - _rowid: UUIDv7 (stable across updates, generated if not present)
    ///   - _version: HLC (new per write, used for CRDT merge — latest wins)
    ///   - _deleted: false (tombstone marker)
    ///
    /// On merge (read_with_shards), rows with the same _rowid are
    /// deduplicated — the one with the latest _version wins.
    ///
    /// Args:
    ///   - collection: Collection name
    ///   - shard_name: Unique name for this shard
    ///   - rows: List of row dicts to upsert
    ///   - key_col: Optional key column name (for legacy non-CRDT rows)
    ///
    /// Returns: shard blob hash
    #[pyo3(signature = (collection, shard_name, rows, key_col=None))]
    fn upsert_shard(&self, collection: &str, shard_name: &str, rows: Vec<PyObject>, key_col: Option<&str>) -> PyResult<String> {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();
        let active = storage.get_active_branch(collection);

        // Convert Python rows to JSON values
        let json_rows: Vec<JsonValue> = rows.iter().map(|r| python_to_json(r)).collect();

        // Use a thread-local HLC (clock-skew-safe)
        use pond_kernel::crdt::HLC;
        let mut hlc = HLC::new();

        pond_storage::shard::upsert_shard(kernel, collection, &active, shard_name, &json_rows, key_col, &mut hlc)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e))
    }

    /// Delete rows by writing a tombstone shard.
    ///
    /// Each deleted _rowid gets a tombstone with _deleted=true and a new
    /// _version. On merge, if the tombstone's _version is later than any
    /// live row's _version, the row is suppressed.
    ///
    /// Args:
    ///   - collection: Collection name
    ///   - shard_name: Unique name for this tombstone shard
    ///   - rowids: List of _rowid values to tombstone
    ///   - key_col: Optional key column name
    ///
    /// Returns: shard blob hash
    #[pyo3(signature = (collection, shard_name, rowids, key_col=None))]
    fn delete_shard(&self, collection: &str, shard_name: &str, rowids: Vec<String>, key_col: Option<&str>) -> PyResult<String> {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();
        let active = storage.get_active_branch(collection);

        use pond_kernel::crdt::HLC;
        let mut hlc = HLC::new();

        pond_storage::shard::delete_shard(kernel, collection, &active, shard_name, &rowids, key_col, &mut hlc)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e))
    }

    /// Read HEAD + all live shards (CRDT read path).
    ///
    /// Returns a list of (shard_name, data_bytes) tuples. The first
    /// element is HEAD (name='__head__'), followed by all shards.
    /// The caller is responsible for merging rows by _rowid (latest
    /// _version wins, tombstones suppress).
    ///
    /// For simple raw-byte reads, use `read()` instead. For structured
    /// reads with auto-merge, use `read_rows()`.
    fn read_with_shards<'py>(&self, py: Python<'py>, collection: &str) -> PyResult<Bound<'py, PyList>> {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();
        let active = storage.get_active_branch(collection);

        let (head_manifest, shards) = pond_storage::shard::read_with_shards(kernel, collection, &active);

        let result = PyList::empty_bound(py);

        // Read HEAD data
        if let Some(manifest_hash) = head_manifest {
            if let Ok(manifest_bytes) = kernel.read_blob(&manifest_hash) {
                if let Ok(manifest) = pond_storage::manifest::CollectionManifest::decode(&manifest_bytes) {
                    for rg in &manifest.row_groups {
                        if let Ok(data) = kernel.read_blob(&rg.blob_hash) {
                            let tuple = PyTuple::new_bound(py, [
                                "__head__".to_object(py),
                                PyBytes::new_bound(py, &data).into_any(),
                            ]);
                            result.append(tuple)?;
                        }
                    }
                }
            }
        }

        // Read shard data
        for (name, hash) in &shards {
            if let Ok(data) = kernel.read_blob(hash) {
                let tuple = PyTuple::new_bound(py, [
                    name.to_object(py),
                    PyBytes::new_bound(py, &data).into_any(),
                ]);
                result.append(tuple)?;
            }
        }

        Ok(result)
    }

    /// Count the number of live shards for a collection's active branch.
    fn shard_count(&self, collection: &str) -> usize {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();
        let active = storage.get_active_branch(collection);
        pond_storage::shard::shard_count(kernel, collection, &active)
    }

    /// Compact shards — merge all shards into HEAD and clear the shard list.
    ///
    /// After compaction, all shard data is absorbed into HEAD (a new commit),
    /// and the shard refs are deleted. This reclaims storage space and
    /// simplifies future reads (no shard merge needed).
    ///
    /// Returns: number of shards compacted
    fn compact_shards(&self, collection: &str) -> PyResult<usize> {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();
        let active = storage.get_active_branch(collection);
        pond_storage::shard::clear_shards(kernel, collection, &active)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e))
    }

    // ===================================================================
    // Atomic Publication (Transactions)
    //
    // This is NOT full ACID. It provides ATOMIC VISIBILITY:
    //   - begin_tx() generates a transaction ID
    //   - Writers attach tx_id to their shards (shards are tentative)
    //   - commit_tx() writes a commit marker → all tentative shards
    //     become visible atomically
    //   - abort_tx() is a no-op (tentative shards are orphaned until GC)
    //
    // There is NO isolation, NO rollback, NO conflict detection.
    // See docs/HONEST_COMPETITOR_COMPARISON.md §3.
    // ===================================================================

    /// Begin a transaction. Returns a transaction ID.
    ///
    /// The tx_id is used to tag tentative writes. Until commit_tx() is
    /// called, the writes are invisible to readers. Once committed, all
    /// tagged writes become visible atomically.
    fn begin_tx(&self) -> String {
        pond_storage::transaction::begin_tx()
    }

    /// Commit a transaction. Writes a commit marker at transactions/{tx_id}.
    ///
    /// Once the marker exists, all tentative shards (tagged with tx_id)
    /// become visible to readers. This is ATOMIC PUBLICATION —
    /// all-or-nothing visibility.
    ///
    /// NOT full ACID: no isolation, no rollback, no conflict detection.
    fn commit_tx(&self, tx_id: &str, message: &str) -> PyResult<String> {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();
        pond_storage::transaction::commit_tx(kernel, tx_id, message)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e))
    }

    /// Abort a transaction. Currently a NO-OP.
    ///
    /// Tentative shards are orphaned until GC cleans them up. There is
    /// no real rollback — the shards remain on storage but are invisible
    /// to readers (because the commit marker doesn't exist).
    fn abort_tx(&self, tx_id: &str) {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();
        pond_storage::transaction::abort_tx(kernel, tx_id)
    }

    /// Check if a transaction has been committed.
    fn is_tx_committed(&self, tx_id: &str) -> bool {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();
        pond_storage::transaction::is_tx_committed(kernel, tx_id)
    }

    // ===================================================================
    // Optimize — compact shards + flatten delta manifests
    //
    // Delta/Iceberg-style optimize: merges small files into larger ones
    // for better read performance. Does TWO things:
    //   1. compact_shards: merge all shards into HEAD (clears shard list)
    //   2. (future) compact_manifest: flatten delta-manifest chains
    //
    // Currently only shard compaction is implemented in the Rust core.
    // Manifest flattening is a Python SDK feature pending port.
    // ===================================================================

    /// Optimize storage — compact shards + flatten delta manifests.
    ///
    /// Args:
    ///   - collection: if None, optimize ALL collections. If specified,
    ///     optimize only that collection.
    ///
    /// Returns: dict with collections_optimized, shards_compacted
    #[pyo3(signature = (collection=None))]
    fn optimize(&self, py: Python<'_>, collection: Option<&str>) -> PyResult<PyObject> {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();

        // Determine which collections to optimize
        let collections: Vec<String> = if let Some(c) = collection {
            vec![c.to_string()]
        } else {
            // List all collections by scanning refs
            kernel.list_names_prefix("collections/")
                .into_iter()
                .filter_map(|n| {
                    // Extract collection name from "collections/{name}/_branches/..."
                    n.strip_prefix("collections/")?
                        .split('/').next()
                        .map(|s| s.to_string())
                })
                .collect::<std::collections::HashSet<_>>()
                .into_iter()
                .collect()
        };

        let mut shards_compacted = 0usize;
        let mut optimized = 0usize;

        for coll in &collections {
            let active = storage.get_active_branch(coll);
            let shard_n = pond_storage::shard::shard_count(kernel, coll, &active);
            if shard_n > 0 {
                match pond_storage::shard::clear_shards(kernel, coll, &active) {
                    Ok(n) => shards_compacted += n,
                    Err(_) => continue,
                }
            }
            optimized += 1;
        }

        let dict = PyDict::new_bound(py);
        dict.set_item("collections_optimized", optimized)?;
        dict.set_item("shards_compacted", shards_compacted)?;
        dict.set_item("manifests_flattened", 0)?; // pending port from Python
        Ok(dict.into())
    }

    // ===================================================================
    // Semantic layers — cross-collection, handle-based API
    //
    // WHY "layer" (not "model"): the word "model" collides with ML models,
    // which Pond may host in the future. "Semantic Layer" is the industry-
    // standard term (dbt Semantic Layer, Cube Semantic Layer, Looker LookML).
    // ===================================================================

    /// Get a semantic layer handle (creates the layer if it doesn't exist).
    ///
    /// Returns a SemanticLayer object that groups all semantic operations:
    ///   m = s.layer('sales')
    ///   m.add_datasets(['orders', 'users'])
    ///   m.add_metrics({'revenue': 'SUM(orders.amount)'})
    ///   m.info()
    ///   m.export()
    ///
    /// Multiple adapters: pass a list to `adapters`. A layer can be exposed
    /// via Ossie + Cube + dbt simultaneously. Adapters can also be added /
    /// removed later via `m.add_adapter(name)` / `m.remove_adapter(name)`.
    #[pyo3(signature = (name, adapters=None, enable_reflection=false))]
    fn layer(&self, name: &str, adapters: Option<Vec<String>>, enable_reflection: bool) -> PyResult<SemanticLayer> {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();

        // Create layer metadata if it doesn't exist
        let layer_ref = format!("semantic_layers/{}/_meta", name);
        if kernel.resolve(&layer_ref).is_none() {
            // Default to ['ossie'] if no adapters specified
            let adapter_list = adapters.unwrap_or_else(|| vec!["ossie".to_string()]);
            let layer_meta = serde_json::json!({
                "name": name,
                "adapters": adapter_list,
                "enable_reflection": enable_reflection,
            });
            let meta_bytes = serde_json::to_vec(&layer_meta)
                .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
            let hash = kernel.write(&meta_bytes)
                .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
            kernel.reference(&layer_ref, &hash)
                .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        }

        Ok(SemanticLayer {
            storage: self.storage.clone(),
            name: name.to_string(),
        })
    }

    /// List all semantic layers.
    fn layers(&self) -> Vec<String> {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();
        let prefix = "semantic_layers/";
        let mut seen = std::collections::HashSet::new();
        kernel.list_names_prefix(prefix).into_iter()
            .filter_map(|n| {
                let rest = n.strip_prefix("semantic_layers/")?;
                let layer_name = rest.split('/').next()?;
                if layer_name == "_meta" { return None; }
                if seen.contains(layer_name) { return None; }
                seen.insert(layer_name.to_string());
                Some(layer_name.to_string())
            })
            .collect()
    }
}

// ===========================================================================
// SemanticLayer — handle for cross-collection semantic layer operations
//
// WHY "layer" (not "model"): avoids confusion with ML models. Industry
// standard (dbt Semantic Layer, Cube Semantic Layer, Looker LookML).
// ===========================================================================

use std::sync::Arc;

/// A handle to a semantic layer. All semantic operations go through this handle.
///
/// Get one via: `m = s.layer('sales')`
#[pyclass]
struct SemanticLayer {
    storage: Arc<Mutex<UnifiedStorage>>,
    name: String,
}

#[pymethods]
impl SemanticLayer {
    /// Add multiple datasets (collections) to the layer in one call.
    ///
    /// Args:
    ///   - datasets: List of collection names to add
    fn add_datasets(&self, datasets: Vec<String>) -> PyResult<()> {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();
        for ds in &datasets {
            let ds_json = serde_json::json!({"name": ds, "source": ds});
            let ds_bytes = serde_json::to_vec(&ds_json)
                .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
            let hash = kernel.write(&ds_bytes)
                .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
            let ref_name = format!("semantic_layers/{}/datasets/{}", self.name, ds);
            kernel.reference(&ref_name, &hash)
                .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        }
        Ok(())
    }

    /// Add multiple metrics to the model in one call.
    ///
    /// Args:
    ///   - metrics: Dict of {metric_name: expression}
    ///
    /// Example:
    ///   m.add_metrics({'revenue': 'SUM(orders.amount)', 'count': 'COUNT(orders.id)'})
    fn add_metrics(&self, metrics: std::collections::HashMap<String, String>) -> PyResult<()> {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();
        for (name, expr) in &metrics {
            let metric = serde_json::json!({
                "name": name,
                "expression": expr,
                "description": "",
                "format": "number",
            });
            let bytes = serde_json::to_vec(&metric)
                .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
            let hash = kernel.write(&bytes)
                .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
            let ref_name = format!("semantic_layers/{}/metrics/{}", self.name, name);
            kernel.reference(&ref_name, &hash)
                .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        }
        Ok(())
    }

    /// Add multiple dimensions to the model in one call.
    ///
    /// Args:
    ///   - dimensions: Dict of {dim_name: (collection, field, data_type)}
    ///
    /// Example:
    ///   m.add_dimensions({
    ///       'country': ('users', 'country', 'string'),
    ///       'order_date': ('orders', 'created_at', 'time'),
    ///   })
    fn add_dimensions(&self, dimensions: std::collections::HashMap<String, (String, String, String)>) -> PyResult<()> {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();
        for (name, (dataset, field, data_type)) in &dimensions {
            let dim = serde_json::json!({
                "name": name,
                "dataset": dataset,
                "field": field,
                "data_type": data_type,
            });
            let bytes = serde_json::to_vec(&dim)
                .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
            let hash = kernel.write(&bytes)
                .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
            let ref_name = format!("semantic_layers/{}/dimensions/{}", self.name, name);
            kernel.reference(&ref_name, &hash)
                .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        }
        Ok(())
    }

    /// Add multiple relationships to the model in one call.
    ///
    /// Args:
    ///   - relationships: Dict of {rel_name: (from, to, condition)}
    ///
    /// Example:
    ///   m.add_relationships({
    ///       'user_orders': ('users', 'orders', 'users.id = orders.user_id'),
    ///   })
    fn add_relationships(&self, relationships: std::collections::HashMap<String, (String, String, String)>) -> PyResult<()> {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();
        for (name, (from, to, condition)) in &relationships {
            let rel = serde_json::json!({
                "name": name,
                "from": from,
                "to": to,
                "condition": condition,
            });
            let bytes = serde_json::to_vec(&rel)
                .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
            let hash = kernel.write(&bytes)
                .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
            let ref_name = format!("semantic_layers/{}/relationships/{}", self.name, name);
            kernel.reference(&ref_name, &hash)
                .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        }
        Ok(())
    }

    /// Get a full overview of the layer.
    ///
    /// Returns a dict with: name, adapters, datasets, metrics, dimensions,
    /// relationships (each with count + names), reflection_enabled.
    fn info(&self, py: Python<'_>) -> PyResult<PyObject> {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();

        // Read layer metadata
        let layer_ref = format!("semantic_layers/{}/_meta", self.name);
        let layer_hash = kernel.resolve(&layer_ref)
            .ok_or_else(|| pyo3::exceptions::PyKeyError::new_err(
                format!("Semantic layer '{}' not found", self.name)))?;
        let layer_data = kernel.read_blob(&layer_hash)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        let layer_meta: serde_json::Value = serde_json::from_slice(&layer_data)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;

        let datasets = self.list_datasets_impl(kernel);
        let metrics = self.list_metrics_impl(kernel);
        let dimensions = self.list_dimensions_impl(kernel);
        let relationships = self.list_relationships_impl(kernel);

        // adapters: a JSON list (with backward compat for the legacy
        // single-string "adapter" field).
        let adapters_val: Vec<String> = if let Some(arr) = layer_meta.get("adapters").and_then(|v| v.as_array()) {
            arr.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect()
        } else if let Some(single) = layer_meta.get("adapter").and_then(|v| v.as_str()) {
            vec![single.to_string()]
        } else {
            vec!["ossie".to_string()]
        };

        let dict = PyDict::new_bound(py);
        dict.set_item("name", layer_meta.get("name").and_then(|v| v.as_str()).unwrap_or(&self.name))?;
        dict.set_item("adapters", adapters_val)?;
        dict.set_item("reflection_enabled", layer_meta.get("enable_reflection").and_then(|v| v.as_bool()).unwrap_or(false))?;
        dict.set_item("datasets", datasets)?;
        dict.set_item("metrics", metrics)?;
        dict.set_item("dimensions", dimensions)?;
        dict.set_item("relationships", relationships)?;
        Ok(dict.into())
    }

    /// List datasets in this layer.
    fn datasets(&self) -> Vec<String> {
        let storage = self.storage.lock().unwrap();
        self.list_datasets_impl(storage.kernel())
    }

    /// List metrics in this layer.
    fn metrics(&self) -> Vec<String> {
        let storage = self.storage.lock().unwrap();
        self.list_metrics_impl(storage.kernel())
    }

    /// List dimensions in this layer.
    fn dimensions(&self) -> Vec<String> {
        let storage = self.storage.lock().unwrap();
        self.list_dimensions_impl(storage.kernel())
    }

    /// List relationships in this layer.
    fn relationships(&self) -> Vec<String> {
        let storage = self.storage.lock().unwrap();
        self.list_relationships_impl(storage.kernel())
    }

    /// List the adapters currently enabled on this layer.
    ///
    /// A layer can be exposed via multiple adapters simultaneously
    /// (e.g., Ossie + Cube + dbt). Use `add_adapter` / `remove_adapter`
    /// to manage them independently of the spec.
    fn adapters(&self) -> PyResult<Vec<String>> {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();
        let layer_ref = format!("semantic_layers/{}/_meta", self.name);
        let hash = kernel.resolve(&layer_ref)
            .ok_or_else(|| pyo3::exceptions::PyKeyError::new_err("Layer not found"))?;
        let data = kernel.read_blob(&hash)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        let meta: serde_json::Value = serde_json::from_slice(&data)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        if let Some(arr) = meta.get("adapters").and_then(|v| v.as_array()) {
            Ok(arr.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect())
        } else if let Some(single) = meta.get("adapter").and_then(|v| v.as_str()) {
            Ok(vec![single.to_string()])
        } else {
            Ok(vec!["ossie".to_string()])
        }
    }

    /// Add an adapter to this layer. Idempotent.
    ///
    /// The layer becomes queryable via this adapter's protocol immediately
    /// (auto-exposure — no explicit export step needed).
    fn add_adapter(&self, adapter: String) -> PyResult<()> {
        let mut current = self.adapters()?;
        if !current.contains(&adapter) {
            current.push(adapter);
            self.set_adapters_field(current)?;
        }
        Ok(())
    }

    /// Remove an adapter from this layer. Returns True if it was present.
    fn remove_adapter(&self, adapter: String) -> PyResult<bool> {
        let mut current = self.adapters()?;
        let before = current.len();
        current.retain(|a| a != &adapter);
        let removed = current.len() < before;
        if removed {
            self.set_adapters_field(current)?;
        }
        Ok(removed)
    }

    /// Export the layer in a specific adapter format.
    ///
    /// If `adapter` is None, uses the first adapter in the layer's
    /// `adapters` list (the "default" for the layer).
    ///
    /// Auto-exposure: this method is OPTIONAL. Adapters can read the
    /// layer's spec directly from storage at query time. This method
    /// is for one-shot snapshots (file export, debugging, migration).
    #[pyo3(signature = (adapter=None))]
    fn export(&self, py: Python<'_>, adapter: Option<&str>) -> PyResult<PyObject> {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();

        // Determine adapter: explicit > first in adapters list > "ossie"
        let layer_ref = format!("semantic_layers/{}/_meta", self.name);
        let adapter_name = if let Some(a) = adapter {
            a.to_string()
        } else {
            let hash = kernel.resolve(&layer_ref)
                .ok_or_else(|| pyo3::exceptions::PyKeyError::new_err("Layer not found"))?;
            let data = kernel.read_blob(&hash)
                .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
            let meta: serde_json::Value = serde_json::from_slice(&data)
                .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
            if let Some(arr) = meta.get("adapters").and_then(|v| v.as_array()) {
                arr.first().and_then(|v| v.as_str()).unwrap_or("ossie").to_string()
            } else {
                meta.get("adapter").and_then(|v| v.as_str()).unwrap_or("ossie").to_string()
            }
        };

        // Read all definitions
        let mut defs = SemanticDefinitions::new();

        let metric_prefix = format!("semantic_layers/{}/metrics/", self.name);
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

        let dim_prefix = format!("semantic_layers/{}/dimensions/", self.name);
        for ref_name in kernel.list_names_prefix(&dim_prefix) {
            if let Some(hash) = kernel.resolve(&ref_name) {
                if let Ok(data) = kernel.read_blob(&hash) {
                    if let Ok(d) = serde_json::from_slice::<serde_json::Value>(&data) {
                        defs.dimensions.push(pond_semantic::Dimension {
                            name: d.get("name").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                            data_type: d.get("data_type").and_then(|v| v.as_str()).unwrap_or("string").to_string(),
                            description: d.get("field").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                        });
                    }
                }
            }
        }

        let rel_prefix = format!("semantic_layers/{}/relationships/", self.name);
        for ref_name in kernel.list_names_prefix(&rel_prefix) {
            if let Some(hash) = kernel.resolve(&ref_name) {
                if let Ok(data) = kernel.read_blob(&hash) {
                    if let Ok(r) = serde_json::from_slice::<serde_json::Value>(&data) {
                        defs.relationships.push(pond_semantic::Relationship {
                            name: r.get("name").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                            from_collection: r.get("from").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                            to_collection: r.get("to").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                            join_type: "inner".to_string(),
                            join_condition: r.get("condition").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                        });
                    }
                }
            }
        }

        // Export using the adapter
        let layer = match adapter_name.as_str() {
            "ossie" => {
                use pond_semantic::SemanticModelAdapter;
                pond_ossie_adapter::OssieAdapter::new().export_model(&defs)
            }
            _ => return Err(pyo3::exceptions::PyValueError::new_err(
                format!("Unknown adapter: '{}'. Supported: ossie", adapter_name)
            )),
        };

        let layer_str = serde_json::to_string(&layer)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        let json_module = py.import_bound("json")?;
        let result = json_module.call_method("loads", (layer_str,), None)?;
        Ok(result.into())
    }

    /// Enable reflection on this layer.
    fn enable_reflection(&self) -> PyResult<()> {
        self.set_reflection_flag(true)
    }

    /// Disable reflection on this layer.
    fn disable_reflection(&self) -> PyResult<()> {
        self.set_reflection_flag(false)
    }
}

impl SemanticLayer {
    fn list_datasets_impl(&self, kernel: &PondKernel) -> Vec<String> {
        let prefix = format!("semantic_layers/{}/datasets/", self.name);
        kernel.list_names_prefix(&prefix).into_iter()
            .filter_map(|n| n.strip_prefix(&prefix).map(|s| s.to_string()))
            .collect()
    }

    fn list_metrics_impl(&self, kernel: &PondKernel) -> Vec<String> {
        let prefix = format!("semantic_layers/{}/metrics/", self.name);
        kernel.list_names_prefix(&prefix).into_iter()
            .filter_map(|n| n.strip_prefix(&prefix).map(|s| s.to_string()))
            .collect()
    }

    fn list_dimensions_impl(&self, kernel: &PondKernel) -> Vec<String> {
        let prefix = format!("semantic_layers/{}/dimensions/", self.name);
        kernel.list_names_prefix(&prefix).into_iter()
            .filter_map(|n| n.strip_prefix(&prefix).map(|s| s.to_string()))
            .collect()
    }

    fn list_relationships_impl(&self, kernel: &PondKernel) -> Vec<String> {
        let prefix = format!("semantic_layers/{}/relationships/", self.name);
        kernel.list_names_prefix(&prefix).into_iter()
            .filter_map(|n| n.strip_prefix(&prefix).map(|s| s.to_string()))
            .collect()
    }

    fn set_reflection_flag(&self, enabled: bool) -> PyResult<()> {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();

        // Read existing meta
        let layer_ref = format!("semantic_layers/{}/_meta", self.name);
        let hash = kernel.resolve(&layer_ref)
            .ok_or_else(|| pyo3::exceptions::PyKeyError::new_err("Layer not found"))?;
        let data = kernel.read_blob(&hash)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        let mut meta: serde_json::Value = serde_json::from_slice(&data)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;

        // Update flag
        if let Some(obj) = meta.as_object_mut() {
            obj.insert("enable_reflection".to_string(), serde_json::json!(enabled));
        }

        // Write back
        let new_bytes = serde_json::to_vec(&meta)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        let new_hash = kernel.write(&new_bytes)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        kernel.reference(&layer_ref, &new_hash)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        Ok(())
    }

    /// Update the `adapters` list in the layer's _meta. Migrates the
    /// legacy single-string `adapter` field to the new `adapters` list
    /// if present.
    fn set_adapters_field(&self, adapters: Vec<String>) -> PyResult<()> {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();

        let layer_ref = format!("semantic_layers/{}/_meta", self.name);
        let hash = kernel.resolve(&layer_ref)
            .ok_or_else(|| pyo3::exceptions::PyKeyError::new_err("Layer not found"))?;
        let data = kernel.read_blob(&hash)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        let mut meta: serde_json::Value = serde_json::from_slice(&data)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;

        if let Some(obj) = meta.as_object_mut() {
            // Migrate: remove legacy single-string `adapter`, set `adapters` list
            obj.remove("adapter");
            obj.insert("adapters".to_string(), serde_json::json!(adapters));
        }

        let new_bytes = serde_json::to_vec(&meta)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        let new_hash = kernel.write(&new_bytes)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        kernel.reference(&layer_ref, &new_hash)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        Ok(())
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

/// Read all rows from a collection as (rowid, JSON row) pairs.
///
/// This is the auto-read helper used by `build_index` for simple indexes.
/// It reads HEAD + all shards, decodes PND2 blobs, and converts each row
/// to a JSON object. The rowid is taken from the first available key column
/// (tries _rowid, then the first key_field, then _key, then id).
///
/// For shard rows (CRDT), the _rowid field is used if present.
fn read_collection_as_json_rows(
    storage: &pond_storage::UnifiedStorage,
    collection: &str,
    key_fields: &[String],
) -> Result<Vec<(String, JsonValue)>, String> {
    use pond_storage::shard;
    use pond_storage::manifest::CollectionManifest;
    use pond_storage::commit;
    use pond_storage::branch_ref;

    let kernel = storage.kernel();
    let mut rows: Vec<(String, JsonValue)> = Vec::new();

    // Determine the active branch
    let active = storage.get_active_branch(collection);

    // --- Read HEAD data ---
    let head = kernel.resolve(&branch_ref(collection, &active));
    if let Some(ref head_hash) = head {
        let head_data = kernel.read_blob(head_hash)
            .map_err(|e| format!("Failed to read HEAD: {}", e))?;

        // Handle PondPack vs old format
        let manifest_bytes = if pond_storage::pond_pack::is_pack(&head_data) {
            let (_, manifest_bytes, _) = pond_storage::pond_pack::decode_pack(&head_data)
                .ok_or_else(|| "Failed to decode PondPack".to_string())?;
            manifest_bytes
        } else {
            let commit = commit::read_commit(kernel, head_hash)
                .ok_or_else(|| "Failed to read HEAD commit".to_string())?;
            if commit.manifest.is_empty() {
                return Ok(rows); // empty collection
            }
            kernel.read_blob(&commit.manifest)
                .map_err(|e| format!("Failed to read manifest: {}", e))?
        };

        let manifest = CollectionManifest::decode(&manifest_bytes)
            .ok_or_else(|| "Failed to decode manifest".to_string())?;

        // Read each row group, decode PND2, convert to JSON rows
        for rg in &manifest.row_groups {
            let blob_data = kernel.read_blob(&rg.blob_hash)
                .map_err(|e| format!("Failed to read data blob: {}", e))?;

            let cols = pond_core::pnd2_decode(&blob_data)
                .map_err(|e| format!("Failed to decode PND2: {}", e))?;

            // Determine the number of rows
            let n_rows = cols.first().map(|c| c.n_values).unwrap_or(0);

            // Convert columnar data to row-oriented JSON
            for row_idx in 0..n_rows {
                let mut row_obj = serde_json::Map::new();
                for col in &cols {
                    let name = col.name.to_string_lossy().to_string();
                    use pond_core::{VT_INT64, VT_FLOAT64, VT_STRING, VT_BINARY, VT_NULL};
                    let val = match col.vtype {
                        VT_INT64 => {
                            col.i64_data.get(row_idx)
                                .map(|v| JsonValue::Number(serde_json::Number::from(*v)))
                                .unwrap_or(JsonValue::Null)
                        }
                        VT_FLOAT64 => {
                            col.f64_data.get(row_idx)
                                .and_then(|v| serde_json::Number::from_f64(*v))
                                .map(JsonValue::Number)
                                .unwrap_or(JsonValue::Null)
                        }
                        VT_STRING => {
                            col.str_data.get(row_idx)
                                .map(|v| JsonValue::String(v.to_string_lossy().to_string()))
                                .unwrap_or(JsonValue::Null)
                        }
                        VT_BINARY | VT_NULL | _ => JsonValue::Null,
                    };
                    row_obj.insert(name, val);
                }

                // Determine the rowid for this row
                let rowid = determine_rowid(&JsonValue::Object(row_obj.clone()), key_fields);
                rows.push((rowid, JsonValue::Object(row_obj)));
            }
        }
    }

    // --- Read shard data (CRDT) ---
    let (_, shards) = shard::read_with_shards(kernel, collection, &active);
    for (_, shard_hash) in shards {
        if let Ok(data) = kernel.read_blob(&shard_hash) {
            // Shards are JSON arrays of row objects
            if let Ok(arr) = serde_json::from_slice::<Vec<JsonValue>>(&data) {
                for row in arr {
                    let rowid = determine_rowid(&row, key_fields);
                    rows.push((rowid, row));
                }
            }
        }
    }

    Ok(rows)
}

/// Determine the rowid for a row.
///
/// Tries (in order): _rowid, first key_field, _key, id, then a hash of the row.
fn determine_rowid(row: &JsonValue, key_fields: &[String]) -> String {
    // Try _rowid first (CRDT rows have this)
    if let Some(r) = row.get("_rowid").and_then(|v| v.as_str()) {
        return r.to_string();
    }
    if let Some(n) = row.get("_rowid").and_then(|v| v.as_i64()) {
        return n.to_string();
    }
    // Try the first key_field
    if let Some(kf) = key_fields.first() {
        if let Some(s) = row.get(kf).and_then(|v| v.as_str()) {
            return s.to_string();
        }
        if let Some(n) = row.get(kf).and_then(|v| v.as_i64()) {
            return n.to_string();
        }
    }
    // Try _key, id
    for fallback in &["_key", "id", "key"] {
        if let Some(s) = row.get(fallback).and_then(|v| v.as_str()) {
            return s.to_string();
        }
        if let Some(n) = row.get(fallback).and_then(|v| v.as_i64()) {
            return n.to_string();
        }
    }
    // Last resort: hash the row
    let s = serde_json::to_string(row).unwrap_or_default();
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    s.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

// ---------------------------------------------------------------------------
// Python module definition
// ---------------------------------------------------------------------------

#[pymodule]
fn pond(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(decode, m)?)?;
    m.add_function(wrap_pyfunction!(encode, m)?)?;
    m.add_class::<Storage>()?;
    m.add_class::<SemanticLayer>()?;
    Ok(())
}
