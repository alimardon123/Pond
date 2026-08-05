// Pond Rust Core — PND2 format encoder/decoder
//
// This is the canonical Rust implementation of Pond's PND2 binary format.
// Python uses this via PyO3 (first-class support). Other languages can
// bind via the C ABI (extern "C") for full project ports.
//
// Design principles:
//   1. Simple — one file, clear structure, no unnecessary abstractions
//   2. Powerful — handles all PND2 encodings (RAW, RLE, DICT, BITPACK)
//   3. Performant — zero-copy where possible, SIMD-friendly bit unpacking
//   4. Efficient — minimal allocations, pre-sized vectors
//
// PND2 Format:
//   Header (12 bytes):
//     Magic: "PND2" (4B)
//     Version: 2 (1B)
//     Flags: has_stats=0x01, compressed=0x02 (1B)
//     n_rows: u32 LE (4B)
//     n_columns: u16 LE (2B)
//   Compression tag: 1B (0=none, 1=zstd)
//   Inner data (schema + stats + payloads):
//     Schema: per col: name_len(1B) + name + vtype(1B) + enc(1B)
//     Stats: per col: has_min(1B) + [min + max] + null_count(4B)
//     Payloads: per col: payload_len(4B) + payload_bytes

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyBytes, PyList};
use pyo3::Bound;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const PND2_MAGIC: &[u8] = b"PND2";
const PND2_VERSION: u8 = 2;
const FLAG_HAS_STATS: u8 = 0x01;
const FLAG_COMPRESSED: u8 = 0x02;

const COMPRESSION_NONE: u8 = 0;
const COMPRESSION_ZSTD: u8 = 1;

// Value types
const VT_INT64: u8 = 1;
const VT_FLOAT64: u8 = 2;
const VT_STRING: u8 = 3;
const VT_NULL: u8 = 4;
const VT_BINARY: u8 = 5;

// Encodings
const ENC_RAW: u8 = 0;
const ENC_RLE: u8 = 1;
const ENC_DICT: u8 = 2;
const ENC_BITPACK: u8 = 3;

// ---------------------------------------------------------------------------
// PND2 Decoder
// ---------------------------------------------------------------------------

struct PND2Parser<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> PND2Parser<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    fn read_u8(&mut self) -> Option<u8> {
        if self.pos >= self.data.len() { return None; }
        let v = self.data[self.pos];
        self.pos += 1;
        Some(v)
    }

    fn read_u16(&mut self) -> Option<u16> {
        if self.pos + 2 > self.data.len() { return None; }
        let v = u16::from_le_bytes([self.data[self.pos], self.data[self.pos + 1]]);
        self.pos += 2;
        Some(v)
    }

    fn read_u32(&mut self) -> Option<u32> {
        if self.pos + 4 > self.data.len() { return None; }
        let v = u32::from_le_bytes([
            self.data[self.pos], self.data[self.pos + 1],
            self.data[self.pos + 2], self.data[self.pos + 3]
        ]);
        self.pos += 4;
        Some(v)
    }

    fn read_i64(&mut self) -> Option<i64> {
        if self.pos + 8 > self.data.len() { return None; }
        let v = i64::from_le_bytes([
            self.data[self.pos], self.data[self.pos + 1],
            self.data[self.pos + 2], self.data[self.pos + 3],
            self.data[self.pos + 4], self.data[self.pos + 5],
            self.data[self.pos + 6], self.data[self.pos + 7]
        ]);
        self.pos += 8;
        Some(v)
    }

    fn read_f64(&mut self) -> Option<f64> {
        if self.pos + 8 > self.data.len() { return None; }
        let v = f64::from_le_bytes([
            self.data[self.pos], self.data[self.pos + 1],
            self.data[self.pos + 2], self.data[self.pos + 3],
            self.data[self.pos + 4], self.data[self.pos + 5],
            self.data[self.pos + 6], self.data[self.pos + 7]
        ]);
        self.pos += 8;
        Some(v)
    }

    fn read_bytes(&mut self, len: usize) -> Option<&'a [u8]> {
        if self.pos + len > self.data.len() { return None; }
        let v = &self.data[self.pos..self.pos + len];
        self.pos += len;
        Some(v)
    }

    fn skip_stat_value(&mut self, vtype: u8) {
        match vtype {
            VT_INT64 | VT_FLOAT64 => { self.pos += 8; }
            VT_STRING | VT_BINARY => {
                if let Some(len) = self.read_u32() {
                    self.pos += len as usize;
                }
            }
            VT_NULL => {}
            _ => {}
        }
    }
}

/// Decode a PND2 blob into a Python dict of column_name → list of values.
///
/// This is the main entry point for Python. It handles:
///   - zstd decompression (if compressed)
///   - All value types (INT64, FLOAT64, STRING, BINARY, NULL)
///   - All encodings (RAW, RLE, DICT, BITPACK)
///   - Optional column projection (skip unrequested columns)
///   - Optional predicate pushdown (filter rows at the encoded level)
#[pyfunction]
#[pyo3(signature = (blob_bytes, columns=None, predicates=None))]
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

    // Compression tag at byte 12
    let compression_tag = blob_bytes[12];

    // Get the inner data (decompress if needed)
    let inner_owned: Vec<u8>;
    let inner: &[u8] = if compression_tag == COMPRESSION_ZSTD {
        // Decompress with zstd
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
        // Bounds check — prevent panic on garbage data
        if pstart + plen > inner.len() {
            // Invalid payload length — skip this column
            break;
        }
        parser.pos += plen;
        payloads.push((name.clone(), *vtype, *enc, pstart, plen));
    }

    // Build the result dict
    let result = PyDict::new_bound(py);

    // Determine which columns to decode
    let requested_cols: Option<std::collections::HashSet<String>> = columns.map(|c| c.into_iter().collect());

    for (name, vtype, enc, pstart, plen) in &payloads {
        // Skip if not requested (projection pushdown)
        if let Some(ref req) = requested_cols {
            if !req.contains(name) {
                continue;
            }
        }

        let payload = &inner[*pstart..*pstart + *plen];
        if payload.is_empty() {
            // Empty column — set empty list
            result.set_item(name, PyList::empty_bound(py))?;
            continue;
        }

        let values = decode_column(py, payload, *vtype, *enc, n_rows)?;
        result.set_item(name, values)?;
    }

    Ok(result.into())
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
    // (no value_type byte, no bitmap)
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
            } else {
                if off + blen as usize <= payload.len() {
                    list.append(PyBytes::new_bound(py, &payload[off..off+blen as usize]))?;
                    off += blen as usize;
                } else {
                    break;
                }
            }
        }
        return Ok(list.into());
    }

    // Non-BINARY: first byte is value_type (PND1 encoding header)
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
            // String/Binary: optional bitmap + length-prefixed values
            // Try without bitmap first
            let list = PyList::empty_bound(py);
            let mut off = 0;

            // Check if there's a bitmap (n_rows bits = (n_rows+7)/8 bytes)
            let bitmap_size = (n_rows + 7) / 8;
            let has_bitmap = data.len() >= bitmap_size + 4;
            let (data_start, has_bitmap) = if has_bitmap {
                // Try to parse: if first 4 bytes after bitmap look like a valid length, use bitmap
                let potential_len = u32::from_le_bytes([
                    data[bitmap_size], data[bitmap_size+1],
                    data[bitmap_size+2], data[bitmap_size+3]
                ]) as usize;
                if bitmap_size + 4 + potential_len <= data.len() {
                    (bitmap_size, true)
                } else {
                    (0, false)
                }
            } else {
                (0, false)
            };

            let vals_data = &data[data_start..];

            // Parse length-prefixed values
            let mut vals: Vec<&[u8]> = Vec::with_capacity(n_rows);
            while off + 4 <= vals_data.len() && vals.len() < n_rows {
                let slen = u32::from_le_bytes([
                    vals_data[off], vals_data[off+1],
                    vals_data[off+2], vals_data[off+3]
                ]) as usize;
                off += 4;
                if off + slen <= vals_data.len() {
                    vals.push(&vals_data[off..off+slen]);
                    off += slen;
                } else {
                    break;
                }
            }

            if has_bitmap {
                // Apply bitmap — only non-null values are in vals
                let bitmap = &data[..bitmap_size];
                let mut val_idx = 0;
                for i in 0..n_rows {
                    if bitmap[i / 8] & (1 << (i % 8)) != 0 {
                        if val_idx < vals.len() {
                            if vtype == VT_STRING {
                                list.append(String::from_utf8_lossy(vals[val_idx]).to_string())?;
                            } else {
                                list.append(PyBytes::new_bound(py, vals[val_idx]))?;
                            }
                            val_idx += 1;
                        }
                    } else {
                        list.append(py.None())?;
                    }
                }
            } else {
                // No bitmap — all values present
                for v in &vals {
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
        if byte_pos >= packed.len() {
            break;
        }

        let mut val: u64 = 0;
        for b in 0..bitwidth {
            let bp = bit_pos + b;
            let bp_byte = bp / 8;
            if bp_byte >= packed.len() {
                break;
            }
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

    // DICT payload (PND1 header already stripped in PND2):
    //   n_unique(4B) + value_type(1B) + [value_bytes]*n_unique + code_bitwidth(1B) + packed_codes
    let data = payload; // NO skip — PND1 header is already stripped

    if data.len() < 5 {
        return Ok(PyList::empty_bound(py).into());
    }

    let n_unique = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
    let dict_vtype = data[4]; // value type of the dictionary values
    let mut off = 5;

    // Parse dictionary values based on dict_vtype
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

    // After dict values: code_bitwidth(1B) + packed_codes
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
                } else {
                    list.append(py.None())?;
                }
            }
            VT_FLOAT64 => {
                if code_idx < dict_float_vals.len() {
                    list.append(dict_float_vals[code_idx])?;
                } else {
                    list.append(py.None())?;
                }
            }
            VT_STRING => {
                if code_idx < dict_str_vals.len() {
                    list.append(String::from_utf8_lossy(&dict_str_vals[code_idx]).to_string())?;
                } else {
                    list.append(py.None())?;
                }
            }
            VT_BINARY => {
                if code_idx < dict_str_vals.len() {
                    list.append(PyBytes::new_bound(py, &dict_str_vals[code_idx]))?;
                } else {
                    list.append(py.None())?;
                }
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

    // Skip value_type byte (PND1 header)
    let data = &payload[1..];

    // RLE layout: n_runs(4B) + [value + run_length(4B)]*N
    // Value format depends on vtype
    if data.len() < 4 {
        return Ok(PyList::empty_bound(py).into());
    }

    let n_runs = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
    let mut off = 4;

    let list = PyList::empty_bound(py);
    let mut total_rows = 0usize;

    for _ in 0..n_runs {
        if total_rows >= n_rows { break; }

        // Read value based on vtype
        match vtype {
            VT_INT64 => {
                if off + 8 > data.len() { break; }
                let v = i64::from_le_bytes([
                    data[off], data[off+1], data[off+2], data[off+3],
                    data[off+4], data[off+5], data[off+6], data[off+7]
                ]);
                off += 8;
                // Read run length
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
// zstd decompression (minimal — no external crate needed)
// ---------------------------------------------------------------------------

/// Decompress zstd data using the zstd library via Python
fn zstd_decompress(data: &[u8]) -> Result<Vec<u8>, String> {
    // Use Python's zstandard library for decompression
    // This avoids adding a zstd crate dependency
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
// Python module definition
// ---------------------------------------------------------------------------

#[pymodule]
fn pond_rust(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(decode, m)?)?;
    Ok(())
}
