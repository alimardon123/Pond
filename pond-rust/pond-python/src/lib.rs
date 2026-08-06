// Pond Python Bindings — PyO3 wrapper around pond-core
//
// This crate compiles to a Python extension module named `pond_rust`.
// It exposes the full PND2 decode/encode pipeline to Python, including
// all encodings (RAW, RLE, DICT, BITPACK), all value types (INT64,
// FLOAT64, STRING, BINARY, NULL), zstd decompression, and predicate
// pushdown via Python-side filter functions.
//
// All pure-Rust logic (constants, parser, C ABI) lives in `pond-core`.
// This file only contains the PyO3-specific glue.

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyTuple};
use pyo3::Bound;

// Re-use the shared constants and parser from pond-core.
use pond_core::{
    PND2_MAGIC, PND2_VERSION, FLAG_HAS_STATS, FLAG_COMPRESSED,
    COMPRESSION_NONE, COMPRESSION_ZSTD,
    VT_INT64, VT_FLOAT64, VT_STRING, VT_BINARY,
    ENC_RAW, ENC_RLE, ENC_DICT, ENC_BITPACK,
    PND2Parser,
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

        let values = decode_column(py, payload, *vtype, *enc, n_rows)?;
        result.set_item(name, values)?;
    }

    // Predicate pushdown: filter rows in Python after decode.
    // (Real pushdown happens at the zone-map level in pond-sdk; this is
    // a row-level filter applied on the decoded result.)
    if let Some(preds) = predicates {
        return apply_predicates(py, &result, &preds);
    }

    Ok(result.into())
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

/// Decode a single column's payload into a Python list.
fn decode_column(
    py: Python,
    payload: &[u8],
    vtype: u8,
    enc: u8,
    n_rows: usize,
) -> PyResult<PyObject> {
    match enc {
        ENC_RAW => decode_raw(py, payload, vtype, n_rows),
        ENC_BITPACK => decode_bitpack(py, payload, n_rows),
        ENC_DICT => decode_dict(py, payload, vtype, n_rows),
        ENC_RLE => decode_rle(py, payload, vtype, n_rows),
        _ => Ok(PyList::empty_bound(py).into()),
    }
}

/// Decode RAW encoding: value_type(1B) + bitmap(optional) + raw values
fn decode_raw(py: Python, payload: &[u8], vtype: u8, n_rows: usize) -> PyResult<PyObject> {
    if payload.is_empty() {
        return Ok(PyList::empty_bound(py).into());
    }

    // BINARY (vtype=5) uses a DIFFERENT format:
    //   n_values(4B) + [length(4B) + bytes] * n_values
    if vtype == VT_BINARY {
        let list = PyList::empty_bound(py);
        if payload.len() < 4 {
            return Ok(list.into());
        }
        let n_values = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]) as usize;
        let mut off = 4;
        for _ in 0..n_values {
            if off + 4 > payload.len() { break; }
            let blen = u32::from_le_bytes([payload[off], payload[off+1], payload[off+2], payload[off+3]]);
            off += 4;
            if blen == 0xFFFFFFFF {
                list.append(py.None())?;
            } else if blen == 0 {
                list.append(PyBytes::new_bound(py, b""))?;
            } else if off + blen as usize <= payload.len() {
                list.append(PyBytes::new_bound(py, &payload[off..off+blen as usize]))?;
                off += blen as usize;
            } else {
                break;
            }
        }
        return Ok(list.into());
    }

    // Non-BINARY: first byte is value_type
    let data = &payload[1..];

    match vtype {
        VT_INT64 => {
            let n = data.len() / 8;
            let list = PyList::empty_bound(py);
            for i in 0..n.min(n_rows) {
                let o = i * 8;
                let v = i64::from_le_bytes([
                    data[o], data[o+1], data[o+2], data[o+3],
                    data[o+4], data[o+5], data[o+6], data[o+7]
                ]);
                list.append(v)?;
            }
            Ok(list.into())
        }
        VT_FLOAT64 => {
            let n = data.len() / 8;
            let list = PyList::empty_bound(py);
            for i in 0..n.min(n_rows) {
                let o = i * 8;
                let v = f64::from_le_bytes([
                    data[o], data[o+1], data[o+2], data[o+3],
                    data[o+4], data[o+5], data[o+6], data[o+7]
                ]);
                list.append(v)?;
            }
            Ok(list.into())
        }
        VT_STRING | VT_BINARY => {
            // String/Binary RAW format with optional null bitmap.
            let list = PyList::empty_bound(py);

            let vals_data = data;
            let mut vals: Vec<&[u8]> = Vec::with_capacity(n_rows);
            let mut off = 0;
            while off + 4 <= vals_data.len() && vals.len() < n_rows {
                let slen = u32::from_le_bytes([
                    vals_data[off], vals_data[off+1],
                    vals_data[off+2], vals_data[off+3]
                ]) as usize;
                off += 4;
                if slen == 0xFFFFFFFF {
                    vals.push(&[]);
                } else if off + slen <= vals_data.len() {
                    vals.push(&vals_data[off..off+slen]);
                    off += slen;
                } else {
                    break;
                }
            }

            if vals.len() == n_rows {
                for v in &vals {
                    if vtype == VT_STRING {
                        list.append(String::from_utf8_lossy(v).to_string())?;
                    } else {
                        list.append(PyBytes::new_bound(py, v))?;
                    }
                }
            } else if vals.len() < n_rows {
                // Maybe there's a null bitmap. Try parsing with bitmap.
                let bitmap_size = (n_rows + 7) / 8;
                if vals_data.len() > bitmap_size {
                    let bitmap = &vals_data[..bitmap_size];
                    let vals_after_bitmap = &vals_data[bitmap_size..];

                    let mut vals2: Vec<&[u8]> = Vec::with_capacity(n_rows);
                    let mut off2 = 0;
                    while off2 + 4 <= vals_after_bitmap.len() && vals2.len() < n_rows {
                        let slen = u32::from_le_bytes([
                            vals_after_bitmap[off2], vals_after_bitmap[off2+1],
                            vals_after_bitmap[off2+2], vals_after_bitmap[off2+3]
                        ]) as usize;
                        off2 += 4;
                        if slen == 0xFFFFFFFF {
                            vals2.push(&[]);
                        } else if off2 + slen <= vals_after_bitmap.len() {
                            vals2.push(&vals_after_bitmap[off2..off2+slen]);
                            off2 += slen;
                        } else {
                            break;
                        }
                    }

                    // Apply bitmap: 1=null, 0=valid (Arrow convention)
                    let mut val_idx = 0;
                    for i in 0..n_rows {
                        if bitmap[i / 8] & (1 << (i % 8)) != 0 {
                            list.append(py.None())?;
                        } else if val_idx < vals2.len() {
                            if vtype == VT_STRING {
                                list.append(String::from_utf8_lossy(vals2[val_idx]).to_string())?;
                            } else {
                                list.append(PyBytes::new_bound(py, vals2[val_idx]))?;
                            }
                            val_idx += 1;
                        } else {
                            list.append(py.None())?;
                        }
                    }
                } else {
                    for v in &vals {
                        if vtype == VT_STRING {
                            list.append(String::from_utf8_lossy(v).to_string())?;
                        } else {
                            list.append(PyBytes::new_bound(py, v))?;
                        }
                    }
                    while list.len() < n_rows {
                        list.append(py.None())?;
                    }
                }
            } else {
                for v in vals.iter().take(n_rows) {
                    if vtype == VT_STRING {
                        list.append(String::from_utf8_lossy(v).to_string())?;
                    } else {
                        list.append(PyBytes::new_bound(py, v))?;
                    }
                }
            }
            Ok(list.into())
        }
        _ => Ok(PyList::empty_bound(py).into()),
    }
}

/// Decode BITPACK encoding: bitwidth(1B) + offset(8B) + min(8B) + max(8B) + packed bits
fn decode_bitpack(py: Python, payload: &[u8], n_rows: usize) -> PyResult<PyObject> {
    if payload.len() < 25 {
        return Ok(PyList::empty_bound(py).into());
    }

    let bitwidth = payload[0] as usize;
    let offset = i64::from_le_bytes([
        payload[1], payload[2], payload[3], payload[4],
        payload[5], payload[6], payload[7], payload[8]
    ]);
    let packed = &payload[25..];

    if bitwidth == 0 || bitwidth > 64 {
        return Ok(PyList::empty_bound(py).into());
    }

    let list = PyList::empty_bound(py);
    let mut bit_pos = 0usize;

    for _ in 0..n_rows {
        let byte_pos = bit_pos / 8;
        if byte_pos >= packed.len() { break; }

        let mut val: u64 = 0;
        for b in 0..bitwidth {
            let bp = bit_pos + b;
            let bp_byte = bp / 8;
            if bp_byte >= packed.len() { break; }
            if packed[bp_byte] & (1 << (bp % 8)) != 0 {
                val |= 1u64 << b;
            }
        }

        list.append(val as i64 + offset)?;
        bit_pos += bitwidth;
    }

    Ok(list.into())
}

/// Decode DICT encoding: n_unique(4B) + value_type(1B) + [value_bytes]*N + code_bitwidth(1B) + packed_codes
fn decode_dict(py: Python, payload: &[u8], vtype: u8, n_rows: usize) -> PyResult<PyObject> {
    if payload.is_empty() {
        return Ok(PyList::empty_bound(py).into());
    }

    let data = payload;
    if data.len() < 5 {
        return Ok(PyList::empty_bound(py).into());
    }

    let n_unique = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
    let dict_vtype = data[4];
    let mut off = 5;

    let mut dict_int_vals: Vec<i64> = Vec::new();
    let mut dict_float_vals: Vec<f64> = Vec::new();
    let mut dict_str_vals: Vec<Vec<u8>> = Vec::new();

    match dict_vtype {
        VT_INT64 => {
            for _ in 0..n_unique {
                if off + 8 > data.len() { break; }
                dict_int_vals.push(i64::from_le_bytes([
                    data[off], data[off+1], data[off+2], data[off+3],
                    data[off+4], data[off+5], data[off+6], data[off+7]
                ]));
                off += 8;
            }
        }
        VT_FLOAT64 => {
            for _ in 0..n_unique {
                if off + 8 > data.len() { break; }
                dict_float_vals.push(f64::from_le_bytes([
                    data[off], data[off+1], data[off+2], data[off+3],
                    data[off+4], data[off+5], data[off+6], data[off+7]
                ]));
                off += 8;
            }
        }
        VT_STRING | VT_BINARY => {
            for _ in 0..n_unique {
                if off + 4 > data.len() { break; }
                let slen = u32::from_le_bytes([data[off], data[off+1], data[off+2], data[off+3]]) as usize;
                off += 4;
                if off + slen <= data.len() {
                    dict_str_vals.push(data[off..off+slen].to_vec());
                    off += slen;
                } else { break; }
            }
        }
        _ => {}
    }

    if off >= data.len() {
        return Ok(PyList::empty_bound(py).into());
    }

    let code_bitwidth = data[off] as usize;
    off += 1;
    let packed_codes = &data[off..];

    if code_bitwidth == 0 || code_bitwidth > 64 {
        return Ok(PyList::empty_bound(py).into());
    }

    let list = PyList::empty_bound(py);
    let mut bit_pos = 0usize;

    for _ in 0..n_rows {
        let byte_pos = bit_pos / 8;
        if byte_pos >= packed_codes.len() { break; }

        let mut code: u64 = 0;
        for b in 0..code_bitwidth {
            let bp = bit_pos + b;
            let bp_byte = bp / 8;
            if bp_byte >= packed_codes.len() { break; }
            if packed_codes[bp_byte] & (1 << (bp % 8)) != 0 {
                code |= 1u64 << b;
            }
        }

        let code_idx = code as usize;
        match dict_vtype {
            VT_INT64 => {
                if code_idx < dict_int_vals.len() {
                    list.append(dict_int_vals[code_idx])?;
                } else { list.append(py.None())?; }
            }
            VT_FLOAT64 => {
                if code_idx < dict_float_vals.len() {
                    list.append(dict_float_vals[code_idx])?;
                } else { list.append(py.None())?; }
            }
            VT_STRING => {
                if code_idx < dict_str_vals.len() {
                    list.append(String::from_utf8_lossy(&dict_str_vals[code_idx]).to_string())?;
                } else { list.append(py.None())?; }
            }
            VT_BINARY => {
                if code_idx < dict_str_vals.len() {
                    list.append(PyBytes::new_bound(py, &dict_str_vals[code_idx]))?;
                } else { list.append(py.None())?; }
            }
            _ => list.append(py.None())?,
        }
        bit_pos += code_bitwidth;
    }

    Ok(list.into())
}

/// Decode RLE encoding: n_runs(4B) + [value + run_length]*N
fn decode_rle(py: Python, payload: &[u8], vtype: u8, n_rows: usize) -> PyResult<PyObject> {
    if payload.is_empty() {
        return Ok(PyList::empty_bound(py).into());
    }

    let data = &payload[1..];
    if data.len() < 4 {
        return Ok(PyList::empty_bound(py).into());
    }

    let n_runs = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
    let mut off = 4;

    let list = PyList::empty_bound(py);
    let mut total_rows = 0usize;

    for _ in 0..n_runs {
        if total_rows >= n_rows { break; }

        match vtype {
            VT_INT64 => {
                if off + 8 > data.len() { break; }
                let v = i64::from_le_bytes([
                    data[off], data[off+1], data[off+2], data[off+3],
                    data[off+4], data[off+5], data[off+6], data[off+7]
                ]);
                off += 8;
                if off + 4 > data.len() { break; }
                let run_len = u32::from_le_bytes([data[off], data[off+1], data[off+2], data[off+3]]) as usize;
                off += 4;
                for _ in 0..run_len {
                    if total_rows >= n_rows { break; }
                    list.append(v)?;
                    total_rows += 1;
                }
            }
            VT_FLOAT64 => {
                if off + 8 > data.len() { break; }
                let v = f64::from_le_bytes([
                    data[off], data[off+1], data[off+2], data[off+3],
                    data[off+4], data[off+5], data[off+6], data[off+7]
                ]);
                off += 8;
                if off + 4 > data.len() { break; }
                let run_len = u32::from_le_bytes([data[off], data[off+1], data[off+2], data[off+3]]) as usize;
                off += 4;
                for _ in 0..run_len {
                    if total_rows >= n_rows { break; }
                    list.append(v)?;
                    total_rows += 1;
                }
            }
            VT_STRING | VT_BINARY => {
                if off + 4 > data.len() { break; }
                let slen = u32::from_le_bytes([data[off], data[off+1], data[off+2], data[off+3]]) as usize;
                off += 4;
                if off + slen > data.len() { break; }
                let val = &data[off..off+slen];
                off += slen;
                if off + 4 > data.len() { break; }
                let run_len = u32::from_le_bytes([data[off], data[off+1], data[off+2], data[off+3]]) as usize;
                off += 4;
                for _ in 0..run_len {
                    if total_rows >= n_rows { break; }
                    if vtype == VT_STRING {
                        list.append(String::from_utf8_lossy(val).to_string())?;
                    } else {
                        list.append(PyBytes::new_bound(py, val))?;
                    }
                    total_rows += 1;
                }
            }
            _ => break,
        }
    }

    Ok(list.into())
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
/// Returns None for columns that need DICT/RLE/BITPACK (Python handles those).
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

// ---------------------------------------------------------------------------
// Python module definition
// ---------------------------------------------------------------------------

#[pymodule]
fn pond_rust(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(decode, m)?)?;
    m.add_function(wrap_pyfunction!(encode, m)?)?;
    Ok(())
}
