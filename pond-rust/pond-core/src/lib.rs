// Pond Core — pure-Rust PND2 codec + C ABI
//
// This crate is the language-agnostic core of Pond's binary storage layer.
//   - Python binds to it via the `pond-python` crate (PyO3 wrapper).
//   - Go, Java, Node, C, C++, Zig, etc. bind to it directly via the C ABI
//     (`extern "C"` functions declared in `pond_core.h`).
//
// DESIGN PRINCIPLES
//   1. Zero external dependencies — so static linking from other languages
//      doesn't pull in transitive Rust crates.
//   2. Pure Rust only — no PyO3, no async runtime, no I/O.
//   3. The C ABI is the universal interop layer.
//   4. All heap allocations across the FFI boundary are explicitly owned by
//      the caller; every `*_free` function documents its contract.
//
// PND2 FORMAT
//   Header (13 bytes):
//     Magic: "PND2" (4B)
//     Version: 2 (1B)
//     Flags: has_stats=0x01, compressed=0x02 (1B)
//     n_rows: u32 LE (4B)
//     n_columns: u16 LE (2B)
//     Compression tag: u8 (0=none, 2=zstd)
//   Inner data (schema + stats + payloads):
//     Schema:  per col: name_len(1B) + name + vtype(1B) + enc(1B)
//     Stats:   per col: has_min(1B) + [min + max] + null_count(4B)
//     Payload: per col: payload_len(4B) + payload_bytes
//
// C ABI SUMMARY (see pond_core.h for full docs)
//   pond_pnd2_decode(blob, len)            -> *mut PondResult
//   pond_result_num_columns(result)        -> usize
//   pond_result_column_name(result, i)     -> *const c_char
//   pond_result_column_vtype(result, i)    -> u8
//   pond_result_column_len(result, i)      -> usize
//   pond_result_column_i64(result, i)      -> *const i64
//   pond_result_column_f64(result, i)      -> *const f64
//   pond_result_column_str(result, ci, ri) -> *const c_char
//   pond_result_free(result)
//   pond_pnd2_encode_i64(vals, n, &blob, &len) -> i32
//   pond_blob_free(blob, len)

#![allow(dead_code)]

use std::ffi::c_char;

// ---------------------------------------------------------------------------
// Constants — public so the PyO3 wrapper crate can reuse them
// ---------------------------------------------------------------------------

pub const PND2_MAGIC: &[u8] = b"PND2";
pub const PND2_VERSION: u8 = 2;
pub const FLAG_HAS_STATS: u8 = 0x01;
pub const FLAG_COMPRESSED: u8 = 0x02;

pub const COMPRESSION_NONE: u8 = 0;
pub const COMPRESSION_LZ4: u8 = 1;
pub const COMPRESSION_ZSTD: u8 = 2;

// Value types
pub const VT_INT64: u8 = 1;
pub const VT_FLOAT64: u8 = 2;
pub const VT_STRING: u8 = 3;
pub const VT_NULL: u8 = 4;
pub const VT_BINARY: u8 = 5;

// Encodings
pub const ENC_RAW: u8 = 0;
pub const ENC_RLE: u8 = 1;
pub const ENC_DICT: u8 = 2;
pub const ENC_BITPACK: u8 = 3;

// ---------------------------------------------------------------------------
// PND2 Parser — public so the PyO3 wrapper can use the same zero-copy reader
// ---------------------------------------------------------------------------

/// Zero-copy cursor over a PND2 inner-data byte slice.
pub struct PND2Parser<'a> {
    pub data: &'a [u8],
    pub pos: usize,
}

impl<'a> PND2Parser<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    pub fn read_u8(&mut self) -> Option<u8> {
        if self.pos >= self.data.len() { return None; }
        let v = self.data[self.pos];
        self.pos += 1;
        Some(v)
    }

    #[allow(dead_code)]
    pub fn read_u16(&mut self) -> Option<u16> {
        if self.pos + 2 > self.data.len() { return None; }
        let v = u16::from_le_bytes([self.data[self.pos], self.data[self.pos + 1]]);
        self.pos += 2;
        Some(v)
    }

    pub fn read_u32(&mut self) -> Option<u32> {
        if self.pos + 4 > self.data.len() { return None; }
        let v = u32::from_le_bytes([
            self.data[self.pos], self.data[self.pos + 1],
            self.data[self.pos + 2], self.data[self.pos + 3]
        ]);
        self.pos += 4;
        Some(v)
    }

    #[allow(dead_code)]
    pub fn read_i64(&mut self) -> Option<i64> {
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

    #[allow(dead_code)]
    pub fn read_f64(&mut self) -> Option<f64> {
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

    pub fn read_bytes(&mut self, len: usize) -> Option<&'a [u8]> {
        if self.pos + len > self.data.len() { return None; }
        let v = &self.data[self.pos..self.pos + len];
        self.pos += len;
        Some(v)
    }

    pub fn skip_stat_value(&mut self, vtype: u8) {
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

// ---------------------------------------------------------------------------
// Pure-Rust decode — produces Vec<PondColumn> (no Python types)
// ---------------------------------------------------------------------------

/// A decoded PND2 column. Owns its data so it can outlive the input blob.
#[derive(Clone, Debug)]
pub struct PondColumn {
    pub name: String,
    pub vtype: u8,
    pub i64_data: Vec<i64>,
    pub f64_data: Vec<f64>,
    pub str_data: Vec<String>,
    pub n_values: usize,
}

impl PondColumn {
    pub fn empty(name: String, vtype: u8) -> Self {
        Self {
            name, vtype,
            i64_data: vec![],
            f64_data: vec![],
            str_data: vec![],
            n_values: 0,
        }
    }
}

/// Decode an uncompressed PND2 blob into a vector of columns.
///
/// Handles RAW encoding for INT64, FLOAT64, and STRING value types.
/// Other encodings (RLE, DICT, BITPACK) and BINARY values currently return
/// empty columns — callers that need them should use the Python bindings,
/// which have full coverage. The C ABI is intentionally minimal so it can
/// stay dependency-free.
///
/// Returns `Err` on malformed input. Returns `Ok(vec)` for valid blobs
/// (possibly with empty columns for unsupported encodings).
pub fn pnd2_decode(blob: &[u8]) -> Result<Vec<PondColumn>, String> {
    if blob.len() < 13 || &blob[0..4] != PND2_MAGIC {
        return Err("not a PND2 blob".into());
    }
    if blob[4] != PND2_VERSION {
        return Err(format!("unsupported PND2 version: {}", blob[4]));
    }
    let flags = blob[5];
    let has_stats = (flags & FLAG_HAS_STATS) != 0;
    let n_rows = u32::from_le_bytes([blob[6], blob[7], blob[8], blob[9]]) as usize;
    let n_columns = u16::from_le_bytes([blob[10], blob[11]]) as usize;
    let compression_tag = blob[12];

    if compression_tag == COMPRESSION_ZSTD {
        // The C ABI expects uncompressed input — callers decompress first.
        return Err("zstd-compressed blobs are not supported by the C ABI; \
                    decompress before calling pond_pnd2_decode".into());
    }
    if compression_tag != COMPRESSION_NONE {
        return Err(format!("unknown compression tag: {}", compression_tag));
    }

    let inner = &blob[13..];
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
    if has_stats {
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

    // Decode each column (RAW only — see fn doc)
    let mut columns: Vec<PondColumn> = Vec::with_capacity(payloads.len());
    for (name, vtype, enc, pstart, plen) in &payloads {
        let payload = &inner[*pstart..*pstart + *plen];
        if payload.is_empty() {
            columns.push(PondColumn::empty(name.clone(), *vtype));
            continue;
        }

        // BINARY / non-RAW encodings: not decoded by the C ABI.
        if *vtype == VT_BINARY || *enc != ENC_RAW {
            columns.push(PondColumn::empty(name.clone(), *vtype));
            continue;
        }

        // RAW: skip value_type byte
        let data = &payload[1..];

        match *vtype {
            VT_INT64 => {
                let n = data.len() / 8;
                let vals: Vec<i64> = (0..n).map(|i| {
                    let o = i * 8;
                    i64::from_le_bytes([
                        data[o], data[o+1], data[o+2], data[o+3],
                        data[o+4], data[o+5], data[o+6], data[o+7],
                    ])
                }).collect();
                let n = vals.len();
                columns.push(PondColumn {
                    name: name.clone(), vtype: *vtype,
                    i64_data: vals, f64_data: vec![], str_data: vec![],
                    n_values: n,
                });
            }
            VT_FLOAT64 => {
                let n = data.len() / 8;
                let vals: Vec<f64> = (0..n).map(|i| {
                    let o = i * 8;
                    f64::from_le_bytes([
                        data[o], data[o+1], data[o+2], data[o+3],
                        data[o+4], data[o+5], data[o+6], data[o+7],
                    ])
                }).collect();
                let n = vals.len();
                columns.push(PondColumn {
                    name: name.clone(), vtype: *vtype,
                    i64_data: vec![], f64_data: vals, str_data: vec![],
                    n_values: n,
                });
            }
            VT_STRING => {
                let mut vals: Vec<String> = Vec::new();
                let mut off = 0;
                while off + 4 <= data.len() && vals.len() < n_rows {
                    let slen = u32::from_le_bytes([
                        data[off], data[off+1], data[off+2], data[off+3]
                    ]) as usize;
                    off += 4;
                    if off + slen <= data.len() {
                        vals.push(String::from_utf8_lossy(&data[off..off+slen]).to_string());
                        off += slen;
                    } else { break; }
                }
                let n = vals.len();
                columns.push(PondColumn {
                    name: name.clone(), vtype: *vtype,
                    i64_data: vec![], f64_data: vec![], str_data: vals,
                    n_values: n,
                });
            }
            _ => columns.push(PondColumn::empty(name.clone(), *vtype)),
        }
    }

    Ok(columns)
}

// ---------------------------------------------------------------------------
// Pure-Rust encode — single INT64 column, RAW encoding, no compression
// ---------------------------------------------------------------------------

/// Encode an array of i64 values into an uncompressed PND2 blob.
///
/// Schema: 1 column named "v", type INT64, encoding RAW, with stats.
pub fn pnd2_encode_i64(values: &[i64]) -> Vec<u8> {
    let n_values = values.len();

    let mut inner = Vec::new();

    // Schema: 1 column "v" of type INT64, encoding RAW
    inner.extend_from_slice(&[1]);           // name_len = 1
    inner.extend_from_slice(b"v");           // name
    inner.extend_from_slice(&[VT_INT64, ENC_RAW]);

    // Stats: min/max for INT64
    let min = values.iter().min().copied().unwrap_or(0);
    let max = values.iter().max().copied().unwrap_or(0);
    inner.push(1);                            // has_min
    inner.extend_from_slice(&min.to_le_bytes());
    inner.extend_from_slice(&max.to_le_bytes());
    inner.extend_from_slice(&0u32.to_le_bytes()); // null_count

    // Payload: value_type(1B) + values (8B each)
    let mut payload = Vec::with_capacity(1 + n_values * 8);
    payload.push(VT_INT64);
    for v in values {
        payload.extend_from_slice(&v.to_le_bytes());
    }
    inner.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    inner.extend_from_slice(&payload);

    // Final PND2 blob
    let mut blob = Vec::with_capacity(13 + inner.len());
    blob.extend_from_slice(PND2_MAGIC);
    blob.push(PND2_VERSION);
    blob.push(FLAG_HAS_STATS);
    blob.extend_from_slice(&(n_values as u32).to_le_bytes());
    blob.extend_from_slice(&1u16.to_le_bytes());  // 1 column
    blob.push(COMPRESSION_NONE);
    blob.extend_from_slice(&inner);
    blob
}

// ---------------------------------------------------------------------------
// C ABI — extern "C" wrappers around the pure-Rust functions above
// ---------------------------------------------------------------------------

/// Opaque handle for decoded PND2 data.
/// Callers get column data via `pond_result_*` accessors, then free with
/// `pond_result_free`.
pub struct PondResult {
    columns: Vec<PondColumn>,
}

/// Decode a PND2 blob into a `PondResult` handle.
///
/// Returns null on error or if the blob uses an unsupported encoding
/// (zstd compression, RLE/DICT/BITPACK, BINARY values).
///
/// The caller owns the handle and must free it with `pond_result_free`.
#[no_mangle]
pub extern "C" fn pond_pnd2_decode(blob: *const u8, blob_len: usize) -> *mut PondResult {
    if blob.is_null() || blob_len == 0 {
        return std::ptr::null_mut();
    }
    let data = unsafe { std::slice::from_raw_parts(blob, blob_len) };

    match pnd2_decode(data) {
        Ok(columns) => Box::into_raw(Box::new(PondResult { columns })),
        Err(_) => std::ptr::null_mut(),
    }
}

/// Get the number of columns in a decoded result.
#[no_mangle]
pub extern "C" fn pond_result_num_columns(result: *const PondResult) -> usize {
    if result.is_null() { return 0; }
    let r = unsafe { &*result };
    r.columns.len()
}

/// Get a column's name (null-terminated C string). Valid until the result
/// is freed. Returns NULL on out-of-bounds or null result.
#[no_mangle]
pub extern "C" fn pond_result_column_name(result: *const PondResult, index: usize) -> *const c_char {
    if result.is_null() { return std::ptr::null(); }
    let r = unsafe { &*result };
    if index >= r.columns.len() { return std::ptr::null(); }
    r.columns[index].name.as_ptr() as *const c_char
}

/// Get a column's value type.
/// Returns: 1=INT64, 2=FLOAT64, 3=STRING, 4=NULL, 5=BINARY, 0=error/null.
#[no_mangle]
pub extern "C" fn pond_result_column_vtype(result: *const PondResult, index: usize) -> u8 {
    if result.is_null() { return 0; }
    let r = unsafe { &*result };
    if index >= r.columns.len() { return 0; }
    r.columns[index].vtype
}

/// Get the number of values in a column.
#[no_mangle]
pub extern "C" fn pond_result_column_len(result: *const PondResult, index: usize) -> usize {
    if result.is_null() { return 0; }
    let r = unsafe { &*result };
    if index >= r.columns.len() { return 0; }
    r.columns[index].n_values
}

/// Get INT64 column data pointer. Valid until the result is freed.
/// Returns NULL if the column is not INT64, or on out-of-bounds/null result.
/// Use `pond_result_column_len()` to get the array length.
#[no_mangle]
pub extern "C" fn pond_result_column_i64(result: *const PondResult, index: usize) -> *const i64 {
    if result.is_null() { return std::ptr::null(); }
    let r = unsafe { &*result };
    if index >= r.columns.len() { return std::ptr::null(); }
    if r.columns[index].vtype != VT_INT64 { return std::ptr::null(); }
    r.columns[index].i64_data.as_ptr()
}

/// Get FLOAT64 column data pointer. Valid until the result is freed.
/// Returns NULL if the column is not FLOAT64, or on out-of-bounds/null result.
#[no_mangle]
pub extern "C" fn pond_result_column_f64(result: *const PondResult, index: usize) -> *const f64 {
    if result.is_null() { return std::ptr::null(); }
    let r = unsafe { &*result };
    if index >= r.columns.len() { return std::ptr::null(); }
    if r.columns[index].vtype != VT_FLOAT64 { return std::ptr::null(); }
    r.columns[index].f64_data.as_ptr()
}

/// Get a STRING column value at a specific row index.
/// Returns a null-terminated C string, valid until the result is freed.
/// Returns NULL on out-of-bounds, null result, or non-STRING column.
#[no_mangle]
pub extern "C" fn pond_result_column_str(
    result: *const PondResult,
    col_index: usize,
    row_index: usize,
) -> *const c_char {
    if result.is_null() { return std::ptr::null(); }
    let r = unsafe { &*result };
    if col_index >= r.columns.len() { return std::ptr::null(); }
    if r.columns[col_index].vtype != VT_STRING { return std::ptr::null(); }
    if row_index >= r.columns[col_index].str_data.len() { return std::ptr::null(); }
    r.columns[col_index].str_data[row_index].as_ptr() as *const c_char
}

/// Free a decoded result. Must be called exactly once per handle.
/// Passing NULL is a safe no-op.
#[no_mangle]
pub extern "C" fn pond_result_free(result: *mut PondResult) {
    if !result.is_null() {
        unsafe { drop(Box::from_raw(result)); }
    }
}

/// Encode an array of int64_t values into a PND2 blob (single column, RAW
/// encoding, no compression).
///
/// # Arguments
///   - `values`: pointer to int64_t array
///   - `n_values`: number of values
///   - `out_blob`: output param — receives a pointer to the blob bytes
///   - `out_blob_len`: output param — receives the blob length in bytes
///
/// # Returns
///   0 on success, -1 on invalid arguments.
///
/// # Ownership
///   The caller owns the returned blob and must free it with `pond_blob_free`.
#[no_mangle]
pub extern "C" fn pond_pnd2_encode_i64(
    values: *const i64,
    n_values: usize,
    out_blob: *mut *mut u8,
    out_blob_len: *mut usize,
) -> i32 {
    if values.is_null() || n_values == 0 || out_blob.is_null() || out_blob_len.is_null() {
        return -1;
    }

    let vals = unsafe { std::slice::from_raw_parts(values, n_values) };
    let mut blob = pnd2_encode_i64(vals);

    let len = blob.len();
    let ptr = blob.as_mut_ptr();
    std::mem::forget(blob); // caller owns it now

    unsafe {
        *out_blob = ptr;
        *out_blob_len = len;
    }
    0
}

/// Free a blob returned by `pond_pnd2_encode_i64`.
/// Passing NULL with blob_len=0 is a safe no-op.
#[no_mangle]
pub extern "C" fn pond_blob_free(blob: *mut u8, blob_len: usize) {
    if !blob.is_null() && blob_len > 0 {
        unsafe { drop(Vec::from_raw_parts(blob, blob_len, blob_len)); }
    }
}

// ---------------------------------------------------------------------------
// Tests — pure Rust unit tests for the encode/decode logic
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_i64_roundtrip() {
        let input: Vec<i64> = vec![1, 2, 3, 100, -50, 999999, 0, -1];
        let blob = pnd2_encode_i64(&input);
        assert_eq!(&blob[0..4], PND2_MAGIC);
        assert_eq!(blob[4], PND2_VERSION);

        let cols = pnd2_decode(&blob).expect("decode should succeed");
        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].name, "v");
        assert_eq!(cols[0].vtype, VT_INT64);
        assert_eq!(cols[0].n_values, input.len());
        assert_eq!(cols[0].i64_data, input);
    }

    #[test]
    fn test_encode_empty_returns_blob() {
        // Empty input is rejected by the C ABI wrapper, but the pure-Rust
        // function should still produce a valid (empty) blob.
        let blob = pnd2_encode_i64(&[]);
        assert_eq!(&blob[0..4], PND2_MAGIC);
        let cols = pnd2_decode(&blob).expect("decode should succeed");
        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].n_values, 0);
    }

    #[test]
    fn test_decode_rejects_bad_magic() {
        let garbage = [1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13];
        assert!(pnd2_decode(&garbage).is_err());
    }

    #[test]
    fn test_decode_rejects_zstd() {
        // Construct a minimal blob with compression_tag = ZSTD
        let mut blob = vec![b'P', b'N', b'D', b'2', 2, 0, 0, 0, 0, 0, 0, 0];
        blob.push(COMPRESSION_ZSTD);
        assert!(pnd2_decode(&blob).is_err());
    }
}
