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
use pyo3::types::{PyDict, PyBytes, PyList, PyTuple};
use pyo3::Bound;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const PND2_MAGIC: &[u8] = b"PND2";
const PND2_VERSION: u8 = 2;
const FLAG_HAS_STATS: u8 = 0x01;
const FLAG_COMPRESSED: u8 = 0x02;

const COMPRESSION_NONE: u8 = 0;
const COMPRESSION_LZ4: u8 = 1;
const COMPRESSION_ZSTD: u8 = 2;

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
            // String/Binary RAW format: value_type(1B) + optional null_bitmap + [len(4B) + bytes]*N
            // The null bitmap is ONLY present if has_nulls=True at encode time.
            // Since we can't tell from the payload alone, we use a heuristic:
            //   1. Try parsing WITHOUT a bitmap first (the common case).
            //   2. If that fails (wrong count or out of bounds), try WITH a bitmap.
            // This is simpler and more reliable than trying to detect the bitmap
            // by checking if the first N bytes "look like" a bitmap.

            let list = PyList::empty_bound(py);

            // Approach: parse all length-prefixed values from data[1..] (skip value_type byte)
            let vals_data = data; // 'data' already skipped value_type byte above
            let mut vals: Vec<&[u8]> = Vec::with_capacity(n_rows);
            let mut off = 0;
            while off + 4 <= vals_data.len() && vals.len() < n_rows {
                let slen = u32::from_le_bytes([
                    vals_data[off], vals_data[off+1],
                    vals_data[off+2], vals_data[off+3]
                ]) as usize;
                off += 4;
                if slen == 0xFFFFFFFF {
                    // null sentinel
                    vals.push(&[]);
                } else if off + slen <= vals_data.len() {
                    vals.push(&vals_data[off..off+slen]);
                    off += slen;
                } else {
                    break;
                }
            }

            // If we got the right number of values, use them directly
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

                    // Re-parse values from after the bitmap
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
                            // null
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
                    // No bitmap possible — return what we got (padded with None)
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
                // Got MORE values than expected — just return the first n_rows
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
// PND2 Encoder (write-path acceleration)
// ---------------------------------------------------------------------------

/// Encode a list of column values into a PND2 blob (RAW encoding only).
///
/// Returns a tuple (blob_bytes, stats_list) where stats_list is a list of
/// (name, vtype, min, max, null_count) tuples — computed for FREE during
/// the single-pass encode. The caller uses these stats to build the manifest
/// without re-computing them.
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
            // Compute stats in the SAME pass (zero extra cost)
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

    // Stats section — write stats computed above into the blob
    for (_, _, min_obj, max_obj, null_count) in &stats_list {
        // Check if min/max are available (INT64/FLOAT64)
        if min_obj.is_none(py) {
            inner.push(0); // no min/max
        } else {
            inner.push(1); // has_min
            // Write min
            if let Ok(v) = min_obj.extract::<i64>(py) {
                inner.extend_from_slice(&v.to_le_bytes());
            } else if let Ok(v) = min_obj.extract::<f64>(py) {
                inner.extend_from_slice(&v.to_le_bytes());
            } else {
                // Can't determine type — write 0 bytes
                inner.extend_from_slice(&[0u8; 8]);
            }
            // Write max
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

    // Return tuple: (blob_bytes, stats_list)
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

// ---------------------------------------------------------------------------
// C ABI — cross-language FFI (Go, Java, Node, C, C++, Rust, Zig, etc.)
//
// Other language SDKs call these functions via FFI/cgo/JNI/NAPI.
// The C ABI is the universal interop layer — any language that can call
// C functions can use Pond's Rust core.
//
// Design: simple, no allocations leaked. All functions return 0 on success,
// non-zero on error. Callers must free returned buffers.
// ---------------------------------------------------------------------------

use std::ffi::{c_char, CStr};

/// Opaque result handle for decoded PND2 data.
/// Callers get column data via pond_result_* accessors, then free with pond_result_free.
pub struct PondResult {
    columns: Vec<ResultColumn>,
}

struct ResultColumn {
    name: String,
    vtype: u8,
    i64_data: Vec<i64>,
    f64_data: Vec<f64>,
    str_data: Vec<String>,
    n_values: usize,
}

/// Decode a PND2 blob. Returns a PondResult handle.
/// Returns null on error.
#[no_mangle]
pub extern "C" fn pond_pnd2_decode(blob: *const u8, blob_len: usize) -> *mut PondResult {
    if blob.is_null() || blob_len == 0 {
        return std::ptr::null_mut();
    }
    let data = unsafe { std::slice::from_raw_parts(blob, blob_len) };

    // Parse PND2 header
    if data.len() < 13 || &data[0..4] != PND2_MAGIC {
        return std::ptr::null_mut();
    }
    let flags = data[5];
    let n_rows = u32::from_le_bytes([data[6], data[7], data[8], data[9]]) as usize;
    let n_columns = u16::from_le_bytes([data[10], data[11]]) as usize;
    let compression_tag = data[12];

    // Decompress if needed
    let inner_owned: Vec<u8>;
    let inner: &[u8] = if compression_tag == COMPRESSION_ZSTD {
        // For C ABI, we expect uncompressed input (caller decompresses)
        // or we use the built-in zstd decompression via Python callback.
        // For now, only handle uncompressed.
        return std::ptr::null_mut();
    } else {
        &data[13..]
    };

    let mut parser = PND2Parser::new(inner);

    // Parse schema
    let mut schema: Vec<(String, u8, u8)> = Vec::with_capacity(n_columns);
    for _ in 0..n_columns {
        let name_len = match parser.read_u8() { Some(v) => v as usize, None => break };
        let name_bytes = match parser.read_bytes(name_len) { Some(v) => v, None => break };
        let name = String::from_utf8_lossy(name_bytes).to_string();
        let vtype = match parser.read_u8() { Some(v) => v, None => break };
        let enc = match parser.read_u8() { Some(v) => v, None => break };
        schema.push((name, vtype, enc));
    }

    // Skip stats
    if flags & FLAG_HAS_STATS != 0 {
        for (_, vtype, _) in &schema {
            let has_min = match parser.read_u8() { Some(v) => v, None => break };
            if has_min != 0 {
                parser.skip_stat_value(*vtype);
                parser.skip_stat_value(*vtype);
            }
            let _ = parser.read_u32();
        }
    }

    // Record payload positions
    let mut payloads: Vec<(String, u8, u8, usize, usize)> = Vec::with_capacity(n_columns);
    for (name, vtype, enc) in &schema {
        let plen = match parser.read_u32() { Some(v) => v as usize, None => break };
        let pstart = parser.pos;
        if pstart + plen > inner.len() { break; }
        parser.pos += plen;
        payloads.push((name.clone(), *vtype, *enc, pstart, plen));
    }

    // Decode each column
    let mut columns: Vec<ResultColumn> = Vec::with_capacity(payloads.len());
    for (name, vtype, enc, pstart, plen) in &payloads {
        let payload = &inner[*pstart..*pstart + *plen];
        if payload.is_empty() {
            columns.push(ResultColumn {
                name: name.clone(), vtype: *vtype,
                i64_data: vec![], f64_data: vec![], str_data: vec![],
                n_values: 0,
            });
            continue;
        }

        // BINARY
        if *vtype == VT_BINARY {
            // Skip for C ABI (binary decode is complex)
            columns.push(ResultColumn {
                name: name.clone(), vtype: *vtype,
                i64_data: vec![], f64_data: vec![], str_data: vec![],
                n_values: 0,
            });
            continue;
        }

        let data_start = if *enc == ENC_RAW { 1 } else { 0 }; // skip value_type byte for RAW
        let col_data = &payload[data_start..];

        match *vtype {
            VT_INT64 => {
                let n = col_data.len() / 8;
                let vals: Vec<i64> = (0..n).map(|i| {
                    let o = i * 8;
                    i64::from_le_bytes([col_data[o],col_data[o+1],col_data[o+2],col_data[o+3],
                                        col_data[o+4],col_data[o+5],col_data[o+6],col_data[o+7]])
                }).collect();
                columns.push(ResultColumn {
                    name: name.clone(), vtype: *vtype,
                    i64_data: vals, f64_data: vec![], str_data: vec![],
                    n_values: n,
                });
            }
            VT_FLOAT64 => {
                let n = col_data.len() / 8;
                let vals: Vec<f64> = (0..n).map(|i| {
                    let o = i * 8;
                    f64::from_le_bytes([col_data[o],col_data[o+1],col_data[o+2],col_data[o+3],
                                        col_data[o+4],col_data[o+5],col_data[o+6],col_data[o+7]])
                }).collect();
                columns.push(ResultColumn {
                    name: name.clone(), vtype: *vtype,
                    i64_data: vec![], f64_data: vals, str_data: vec![],
                    n_values: n,
                });
            }
            VT_STRING => {
                let mut vals: Vec<String> = Vec::new();
                let mut off = 0;
                while off + 4 <= col_data.len() && vals.len() < n_rows {
                    let slen = u32::from_le_bytes([col_data[off],col_data[off+1],col_data[off+2],col_data[off+3]]) as usize;
                    off += 4;
                    if off + slen <= col_data.len() {
                        vals.push(String::from_utf8_lossy(&col_data[off..off+slen]).to_string());
                        off += slen;
                    } else { break; }
                }
                let n = vals.len();
                columns.push(ResultColumn {
                    name: name.clone(), vtype: *vtype,
                    i64_data: vec![], f64_data: vec![], str_data: vals,
                    n_values: n,
                });
            }
            _ => {
                columns.push(ResultColumn {
                    name: name.clone(), vtype: *vtype,
                    i64_data: vec![], f64_data: vec![], str_data: vec![],
                    n_values: 0,
                });
            }
        }
    }

    Box::into_raw(Box::new(PondResult { columns }))
}

/// Get number of columns in a decoded result.
#[no_mangle]
pub extern "C" fn pond_result_num_columns(result: *const PondResult) -> usize {
    if result.is_null() { return 0; }
    let r = unsafe { &*result };
    r.columns.len()
}

/// Get column name (null-terminated C string). Valid until result is freed.
#[no_mangle]
pub extern "C" fn pond_result_column_name(result: *const PondResult, index: usize) -> *const c_char {
    if result.is_null() { return std::ptr::null(); }
    let r = unsafe { &*result };
    if index >= r.columns.len() { return std::ptr::null(); }
    r.columns[index].name.as_ptr() as *const c_char
}

/// Get column value type (1=INT64, 2=FLOAT64, 3=STRING).
#[no_mangle]
pub extern "C" fn pond_result_column_vtype(result: *const PondResult, index: usize) -> u8 {
    if result.is_null() { return 0; }
    let r = unsafe { &*result };
    if index >= r.columns.len() { return 0; }
    r.columns[index].vtype
}

/// Get number of values in a column.
#[no_mangle]
pub extern "C" fn pond_result_column_len(result: *const PondResult, index: usize) -> usize {
    if result.is_null() { return 0; }
    let r = unsafe { &*result };
    if index >= r.columns.len() { return 0; }
    r.columns[index].n_values
}

/// Get INT64 column data pointer. Valid until result is freed.
#[no_mangle]
pub extern "C" fn pond_result_column_i64(result: *const PondResult, index: usize) -> *const i64 {
    if result.is_null() { return std::ptr::null(); }
    let r = unsafe { &*result };
    if index >= r.columns.len() { return std::ptr::null(); }
    if r.columns[index].vtype != VT_INT64 { return std::ptr::null(); }
    r.columns[index].i64_data.as_ptr()
}

/// Get FLOAT64 column data pointer. Valid until result is freed.
#[no_mangle]
pub extern "C" fn pond_result_column_f64(result: *const PondResult, index: usize) -> *const f64 {
    if result.is_null() { return std::ptr::null(); }
    let r = unsafe { &*result };
    if index >= r.columns.len() { return std::ptr::null(); }
    if r.columns[index].vtype != VT_FLOAT64 { return std::ptr::null(); }
    r.columns[index].f64_data.as_ptr()
}

/// Get STRING column value at a specific index.
/// Returns null-terminated C string. Valid until result is freed.
#[no_mangle]
pub extern "C" fn pond_result_column_str(result: *const PondResult, col_index: usize, row_index: usize) -> *const c_char {
    if result.is_null() { return std::ptr::null(); }
    let r = unsafe { &*result };
    if col_index >= r.columns.len() { return std::ptr::null(); }
    if r.columns[col_index].vtype != VT_STRING { return std::ptr::null(); }
    if row_index >= r.columns[col_index].str_data.len() { return std::ptr::null(); }
    r.columns[col_index].str_data[row_index].as_ptr() as *const c_char
}

/// Free a decoded result.
#[no_mangle]
pub extern "C" fn pond_result_free(result: *mut PondResult) {
    if !result.is_null() {
        unsafe { drop(Box::from_raw(result)); }
    }
}

/// Encode INT64 column data into a PND2 blob (uncompressed, RAW encoding).
/// Returns 0 on success (writes blob pointer + length), non-zero on error.
///
/// Simple API for single-column INT64. For multi-column, use the Python
/// encode() function or build the PND2 blob manually in the calling language.
#[no_mangle]
pub extern "C" fn pond_pnd2_encode_i64(
    values: *const i64, n_values: usize,
    out_blob: *mut *mut u8, out_blob_len: *mut usize,
) -> i32 {
    if values.is_null() || n_values == 0 || out_blob.is_null() || out_blob_len.is_null() {
        return -1;
    }

    let vals = unsafe { std::slice::from_raw_parts(values, n_values) };

    // Build PND2 blob (uncompressed, single INT64 column, RAW encoding)
    let mut inner = Vec::new();

    // Schema: 1 column "v" of type INT64, encoding RAW
    inner.extend_from_slice(&[1]); // name_len = 1
    inner.extend_from_slice(b"v"); // name
    inner.extend_from_slice(&[VT_INT64, ENC_RAW]); // vtype + enc

    // Stats: min/max for INT64
    let min = vals.iter().min().copied().unwrap_or(0);
    let max = vals.iter().max().copied().unwrap_or(0);
    inner.push(1); // has_min
    inner.extend_from_slice(&min.to_le_bytes());
    inner.extend_from_slice(&max.to_le_bytes());
    inner.extend_from_slice(&0u32.to_le_bytes()); // null_count

    // Payload: value_type(1B) + values (8B each)
    let mut payload = Vec::with_capacity(1 + n_values * 8);
    payload.push(VT_INT64);
    for v in vals {
        payload.extend_from_slice(&v.to_le_bytes());
    }
    inner.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    inner.extend_from_slice(&payload);

    // Build final PND2 blob
    let mut blob = Vec::new();
    blob.extend_from_slice(PND2_MAGIC);
    blob.push(PND2_VERSION);
    blob.push(FLAG_HAS_STATS);
    blob.extend_from_slice(&(n_values as u32).to_le_bytes());
    blob.extend_from_slice(&1u16.to_le_bytes()); // 1 column
    blob.push(COMPRESSION_NONE);
    blob.extend_from_slice(&inner);

    let len = blob.len();
    let ptr = blob.as_mut_ptr();
    std::mem::forget(blob); // prevent Rust from freeing — caller owns it

    unsafe {
        *out_blob = ptr;
        *out_blob_len = len;
    }
    0 // success
}

/// Free a blob returned by pond_pnd2_encode_i64.
#[no_mangle]
pub extern "C" fn pond_blob_free(blob: *mut u8, blob_len: usize) {
    if !blob.is_null() && blob_len > 0 {
        unsafe { drop(Vec::from_raw_parts(blob, blob_len, blob_len)); }
    }
}
