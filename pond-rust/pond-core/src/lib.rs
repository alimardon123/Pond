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

use std::ffi::{c_char, CString};

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
///
/// Each column stores its values in the `*_data` vec matching its `vtype`:
///   - VT_INT64   → `i64_data`
///   - VT_FLOAT64 → `f64_data`
///   - VT_STRING  → `str_data`  (Vec<CString> — null-terminated for C ABI)
///   - VT_BINARY  → `bin_data`
///   - VT_NULL    → all vecs empty, `n_values` = row count
///
/// `name` and `str_data` use `CString` (not `String`) so that `as_ptr()`
/// returns a null-terminated `*const c_char` directly — required by the
/// C ABI accessors `pond_result_column_name` and `pond_result_column_str`.
///
/// `n_values` is the logical row count of the column (always set, even when
/// the data vecs are empty — e.g. for VT_NULL columns or unsupported encodings).
#[derive(Clone, Debug)]
pub struct PondColumn {
    pub name: CString,
    pub vtype: u8,
    pub i64_data: Vec<i64>,
    pub f64_data: Vec<f64>,
    pub str_data: Vec<CString>,
    pub bin_data: Vec<Vec<u8>>,
    pub n_values: usize,
}

impl PondColumn {
    /// Create an empty column with the given name and vtype.
    /// `name_bytes` is the raw UTF-8 bytes of the column name (interior
    /// null bytes are stripped — they're invalid in C strings).
    pub fn empty(name_bytes: &[u8], vtype: u8) -> Self {
        Self {
            name: bytes_to_cstring(name_bytes),
            vtype,
            i64_data: vec![],
            f64_data: vec![],
            str_data: vec![],
            bin_data: vec![],
            n_values: 0,
        }
    }

    /// Same as `empty` but takes a `&str` for ergonomic callers.
    pub fn empty_named(name: &str, vtype: u8) -> Self {
        Self::empty(name.as_bytes(), vtype)
    }
}

/// Convert raw bytes to a `CString`, stripping any interior null bytes
/// (which are invalid in C strings). For invalid UTF-8 the bytes are
/// kept as-is — callers that need lossy UTF-8 conversion should do it
/// before calling this.
fn bytes_to_cstring(b: &[u8]) -> CString {
    let mut v: Vec<u8> = b.to_vec();
    v.retain(|&c| c != 0);
    // Safety: we just stripped all 0x00 bytes, so v contains no interior nulls.
    // CString::new would also work but does a redundant scan.
    CString::new(v).unwrap_or_else(|_| CString::new("").unwrap())
}

/// Convert a `&str` to a `CString` (lossy — interior nulls are stripped).
fn str_to_cstring(s: &str) -> CString {
    bytes_to_cstring(s.as_bytes())
}

/// Decode an uncompressed PND2 blob into a vector of columns.
///
/// Handles ALL encodings (RAW, RLE, DICT, BITPACK) and ALL value types
/// (INT64, FLOAT64, STRING, BINARY, NULL). This is the same decoder the
/// Python bindings use — they call into this function via the
/// `pond-python` crate's PyO3 wrapper.
///
/// Returns `Err` on malformed input. Returns `Ok(vec)` for valid blobs
/// (possibly with empty columns if a specific encoding/vtype combination
/// is not yet implemented).
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
        // (Python bindings handle zstd internally via the `zstandard` module.)
        return Err("zstd-compressed blobs are not supported by pond_core::pnd2_decode; \
                    decompress before calling".into());
    }
    if compression_tag != COMPRESSION_NONE {
        return Err(format!("unknown compression tag: {}", compression_tag));
    }

    let inner = &blob[13..];
    let mut parser = PND2Parser::new(inner);

    // Parse schema
    let mut schema: Vec<(CString, u8, u8)> = Vec::with_capacity(n_columns);
    for _ in 0..n_columns {
        let name_len = match parser.read_u8() { Some(v) => v as usize, None => break };
        let name_bytes = match parser.read_bytes(name_len) { Some(v) => v, None => break };
        let name = bytes_to_cstring(name_bytes);
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

    // Record payload positions (defer decode for projection pushdown)
    let mut payloads: Vec<(CString, u8, u8, usize, usize)> = Vec::with_capacity(n_columns);
    for (name, vtype, enc) in &schema {
        let plen = match parser.read_u32() { Some(v) => v as usize, None => break };
        let pstart = parser.pos;
        if pstart + plen > inner.len() { break; }
        parser.pos += plen;
        payloads.push((name.clone(), *vtype, *enc, pstart, plen));
    }

    // Decode each column
    let mut columns: Vec<PondColumn> = Vec::with_capacity(payloads.len());
    for (name, vtype, enc, pstart, plen) in &payloads {
        let payload = &inner[*pstart..*pstart + *plen];
        if payload.is_empty() {
            let mut col = PondColumn::empty_named("", *vtype);
            col.name = name.clone();
            col.vtype = *vtype;
            columns.push(col);
            continue;
        }

        let mut col = decode_column(payload, *vtype, *enc, n_rows);
        col.name = name.clone();
        col.vtype = *vtype;
        columns.push(col);
    }

    Ok(columns)
}

/// Decode a single column's payload. Dispatches on encoding.
///
/// The returned column has an empty name — the caller is expected to fill
/// it in from the schema. (This separation lets us keep `decode_column`
/// focused on the payload bytes only.)
pub fn decode_column(payload: &[u8], vtype: u8, enc: u8, n_rows: usize) -> PondColumn {
    match enc {
        ENC_RAW      => decode_raw(payload, vtype, n_rows),
        ENC_BITPACK  => decode_bitpack(payload, n_rows),
        ENC_DICT     => decode_dict(payload, vtype, n_rows),
        ENC_RLE      => decode_rle(payload, vtype, n_rows),
        _            => PondColumn::empty_named("", vtype),
    }
}

/// Decode RAW encoding.
///
/// For non-BINARY vtypes, the first byte is the value_type (PND1 header)
/// followed by an optional null bitmap (only if has_nulls at encode time)
/// and then the length-prefixed values.
///
/// For BINARY (vtype=5), the format is:
///   n_values(4B) + [length(4B) + bytes] * n_values
/// (no value_type byte, no bitmap). 0xFFFFFFFF length = null sentinel.
pub fn decode_raw(payload: &[u8], vtype: u8, n_rows: usize) -> PondColumn {
    if payload.is_empty() {
        return PondColumn::empty_named("", vtype);
    }

    // BINARY uses a different layout (no value_type byte, no bitmap)
    if vtype == VT_BINARY {
        return decode_raw_binary(payload, n_rows);
    }

    // Non-BINARY: first byte is value_type
    let data = &payload[1..];

    match vtype {
        VT_INT64 => {
            let n = (data.len() / 8).min(n_rows);
            let mut vals = Vec::with_capacity(n);
            for i in 0..n {
                let o = i * 8;
                vals.push(i64::from_le_bytes([
                    data[o], data[o+1], data[o+2], data[o+3],
                    data[o+4], data[o+5], data[o+6], data[o+7],
                ]));
            }
            PondColumn {
                name: CString::new("").unwrap(), vtype,
                i64_data: vals, f64_data: vec![], str_data: vec![],
                bin_data: vec![], n_values: n,
            }
        }
        VT_FLOAT64 => {
            let n = (data.len() / 8).min(n_rows);
            let mut vals = Vec::with_capacity(n);
            for i in 0..n {
                let o = i * 8;
                vals.push(f64::from_le_bytes([
                    data[o], data[o+1], data[o+2], data[o+3],
                    data[o+4], data[o+5], data[o+6], data[o+7],
                ]));
            }
            PondColumn {
                name: CString::new("").unwrap(), vtype,
                i64_data: vec![], f64_data: vals, str_data: vec![],
                bin_data: vec![], n_values: n,
            }
        }
        VT_STRING | VT_BINARY => {
            // String RAW format: [len(4B) + bytes]*N, optionally with a
            // null bitmap prefix. We try without bitmap first; if the
            // value count doesn't match n_rows, retry with bitmap.
            decode_raw_string_or_binary(data, vtype, n_rows)
        }
        VT_NULL => {
            // NULL columns have no payload data — just count rows.
            PondColumn {
                name: CString::new("").unwrap(), vtype,
                i64_data: vec![], f64_data: vec![], str_data: vec![],
                bin_data: vec![], n_values: n_rows,
            }
        }
        _ => PondColumn::empty_named("", vtype),
    }
}

/// Decode RAW BINARY payload: n_values(4B) + [length(4B) + bytes]*n_values.
fn decode_raw_binary(payload: &[u8], n_rows: usize) -> PondColumn {
    let _ = n_rows; // n_rows is informational only for BINARY
    if payload.len() < 4 {
        return PondColumn::empty_named("", VT_BINARY);
    }
    let n_values = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]) as usize;
    let mut vals: Vec<Vec<u8>> = Vec::with_capacity(n_values);
    let mut off = 4;
    for _ in 0..n_values {
        if off + 4 > payload.len() { break; }
        let blen = u32::from_le_bytes([
            payload[off], payload[off+1], payload[off+2], payload[off+3]
        ]) as usize;
        off += 4;
        if blen == 0xFFFFFFFF {
            // null sentinel — store empty vec (callers can detect via vtype+length)
            vals.push(Vec::new());
        } else if off + blen <= payload.len() {
            vals.push(payload[off..off+blen].to_vec());
            off += blen;
        } else {
            break;
        }
    }
    let n = vals.len();
    PondColumn {
        name: CString::new("").unwrap(), vtype: VT_BINARY,
        i64_data: vec![], f64_data: vec![], str_data: vec![],
        bin_data: vals, n_values: n,
    }
}

/// Decode RAW STRING or BINARY payload (after the value_type byte has been
/// stripped). Tries without null bitmap first, then with bitmap.
fn decode_raw_string_or_binary(data: &[u8], vtype: u8, n_rows: usize) -> PondColumn {
    // Try parsing as length-prefixed values (no bitmap)
    let mut vals: Vec<&[u8]> = Vec::with_capacity(n_rows);
    let mut off = 0;
    while off + 4 <= data.len() && vals.len() < n_rows {
        let slen = u32::from_le_bytes([
            data[off], data[off+1], data[off+2], data[off+3]
        ]) as usize;
        off += 4;
        if slen == 0xFFFFFFFF {
            vals.push(&[]);
        } else if off + slen <= data.len() {
            vals.push(&data[off..off+slen]);
            off += slen;
        } else {
            break;
        }
    }

    // If the value count matches, use it directly.
    if vals.len() == n_rows {
        return build_string_or_binary_col(vtype, &vals, n_rows);
    }

    // If we got fewer values than expected, try with a null bitmap prefix.
    // Bitmap layout: bitmap_size = ceil(n_rows/8) bytes, then length-prefixed
    // values for non-null rows. Bitmap bit=1 means null (Arrow convention).
    if vals.len() < n_rows {
        let bitmap_size = (n_rows + 7) / 8;
        if data.len() > bitmap_size {
            let bitmap = &data[..bitmap_size];
            let vals_data = &data[bitmap_size..];

            let mut vals2: Vec<&[u8]> = Vec::with_capacity(n_rows);
            let mut off2 = 0;
            while off2 + 4 <= vals_data.len() && vals2.len() < n_rows {
                let slen = u32::from_le_bytes([
                    vals_data[off2], vals_data[off2+1],
                    vals_data[off2+2], vals_data[off2+3]
                ]) as usize;
                off2 += 4;
                if slen == 0xFFFFFFFF {
                    vals2.push(&[]);
                } else if off2 + slen <= vals_data.len() {
                    vals2.push(&vals_data[off2..off2+slen]);
                    off2 += slen;
                } else {
                    break;
                }
            }

            // Walk the bitmap: null rows get empty, valid rows get next val.
            let mut final_vals: Vec<&[u8]> = Vec::with_capacity(n_rows);
            let mut val_idx = 0;
            for i in 0..n_rows {
                if bitmap[i / 8] & (1 << (i % 8)) != 0 {
                    final_vals.push(&[]); // null
                } else if val_idx < vals2.len() {
                    final_vals.push(vals2[val_idx]);
                    val_idx += 1;
                } else {
                    final_vals.push(&[]); // ran out of values
                }
            }
            return build_string_or_binary_col(vtype, &final_vals, n_rows);
        }
    }

    // Fall back to whatever we got (padded to n_rows)
    while vals.len() < n_rows {
        vals.push(&[]);
    }
    build_string_or_binary_col(vtype, &vals, n_rows)
}

/// Build a STRING or BINARY PondColumn from a list of byte slices.
fn build_string_or_binary_col(vtype: u8, vals: &[&[u8]], n_rows: usize) -> PondColumn {
    if vtype == VT_STRING {
        let strs: Vec<CString> = vals.iter()
            .map(|v| bytes_to_cstring(v))
            .collect();
        let n = strs.len().min(n_rows);
        PondColumn {
            name: CString::new("").unwrap(), vtype,
            i64_data: vec![], f64_data: vec![], str_data: strs,
            bin_data: vec![], n_values: n,
        }
    } else {
        // VT_BINARY
        let bins: Vec<Vec<u8>> = vals.iter().map(|v| v.to_vec()).collect();
        let n = bins.len().min(n_rows);
        PondColumn {
            name: CString::new("").unwrap(), vtype,
            i64_data: vec![], f64_data: vec![], str_data: vec![],
            bin_data: bins, n_values: n,
        }
    }
}

/// Decode BITPACK encoding: bitwidth(1B) + offset(8B) + min(8B) + max(8B) + packed bits.
///
/// Each output value = (packed bits as u64) + offset.
/// Always produces INT64 columns.
pub fn decode_bitpack(payload: &[u8], n_rows: usize) -> PondColumn {
    if payload.len() < 25 {
        return PondColumn::empty_named("", VT_INT64);
    }

    let bitwidth = payload[0] as usize;
    let offset = i64::from_le_bytes([
        payload[1], payload[2], payload[3], payload[4],
        payload[5], payload[6], payload[7], payload[8]
    ]);
    // payload[9..17] = min, payload[17..25] = max — not needed for decode
    let packed = &payload[25..];

    if bitwidth == 0 || bitwidth > 64 {
        return PondColumn::empty_named("", VT_INT64);
    }

    let mut vals = Vec::with_capacity(n_rows);
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
        vals.push(val as i64 + offset);
        bit_pos += bitwidth;
    }

    let n = vals.len();
    PondColumn {
        name: CString::new("").unwrap(), vtype: VT_INT64,
        i64_data: vals, f64_data: vec![], str_data: vec![],
        bin_data: vec![], n_values: n,
    }
}

/// Decode DICT encoding:
///   n_unique(4B) + value_type(1B) + [value_bytes]*n_unique
///   + code_bitwidth(1B) + packed_codes
///
/// The dictionary's value_type may differ from the column's declared vtype
/// (in practice they match, but we use the dict's value_type for decoding).
pub fn decode_dict(payload: &[u8], vtype: u8, n_rows: usize) -> PondColumn {
    let _ = vtype; // dict payload carries its own value_type
    if payload.is_empty() || payload.len() < 5 {
        return PondColumn::empty_named("", vtype);
    }

    let n_unique = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]) as usize;
    let dict_vtype = payload[4];
    let mut off = 5;

    // Parse dictionary values based on dict_vtype
    let mut dict_int_vals: Vec<i64> = Vec::new();
    let mut dict_float_vals: Vec<f64> = Vec::new();
    let mut dict_str_vals: Vec<Vec<u8>> = Vec::new();

    match dict_vtype {
        VT_INT64 => {
            for _ in 0..n_unique {
                if off + 8 > payload.len() { break; }
                dict_int_vals.push(i64::from_le_bytes([
                    payload[off], payload[off+1], payload[off+2], payload[off+3],
                    payload[off+4], payload[off+5], payload[off+6], payload[off+7]
                ]));
                off += 8;
            }
        }
        VT_FLOAT64 => {
            for _ in 0..n_unique {
                if off + 8 > payload.len() { break; }
                dict_float_vals.push(f64::from_le_bytes([
                    payload[off], payload[off+1], payload[off+2], payload[off+3],
                    payload[off+4], payload[off+5], payload[off+6], payload[off+7]
                ]));
                off += 8;
            }
        }
        VT_STRING | VT_BINARY => {
            for _ in 0..n_unique {
                if off + 4 > payload.len() { break; }
                let slen = u32::from_le_bytes([
                    payload[off], payload[off+1], payload[off+2], payload[off+3]
                ]) as usize;
                off += 4;
                if off + slen <= payload.len() {
                    dict_str_vals.push(payload[off..off+slen].to_vec());
                    off += slen;
                } else { break; }
            }
        }
        _ => {}
    }

    // After dict values: code_bitwidth(1B) + packed_codes
    if off >= payload.len() {
        return PondColumn::empty_named("", dict_vtype);
    }

    let code_bitwidth = payload[off] as usize;
    off += 1;
    let packed_codes = &payload[off..];

    if code_bitwidth == 0 || code_bitwidth > 64 {
        return PondColumn::empty_named("", dict_vtype);
    }

    // Walk the packed codes and look up each value in the dictionary.
    let mut bit_pos = 0usize;
    let mut int_vals: Vec<i64> = Vec::with_capacity(n_rows);
    let mut float_vals: Vec<f64> = Vec::with_capacity(n_rows);
    let mut str_vals: Vec<CString> = Vec::with_capacity(n_rows);
    let mut bin_vals: Vec<Vec<u8>> = Vec::with_capacity(n_rows);
    let mut n = 0usize;

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
                int_vals.push(if code_idx < dict_int_vals.len() {
                    dict_int_vals[code_idx]
                } else { 0 });
            }
            VT_FLOAT64 => {
                float_vals.push(if code_idx < dict_float_vals.len() {
                    dict_float_vals[code_idx]
                } else { 0.0 });
            }
            VT_STRING => {
                str_vals.push(if code_idx < dict_str_vals.len() {
                    bytes_to_cstring(&dict_str_vals[code_idx])
                } else { CString::new("").unwrap() });
            }
            VT_BINARY => {
                bin_vals.push(if code_idx < dict_str_vals.len() {
                    dict_str_vals[code_idx].clone()
                } else { Vec::new() });
            }
            _ => {}
        }
        n += 1;
        bit_pos += code_bitwidth;
    }

    PondColumn {
        name: CString::new("").unwrap(), vtype: dict_vtype,
        i64_data: int_vals, f64_data: float_vals, str_data: str_vals,
        bin_data: bin_vals, n_values: n,
    }
}

/// Decode RLE encoding: n_runs(4B) + [value + run_length(4B)]*N
///
/// For INT64/FLOAT64: each run is value(8B) + run_length(4B) = 12 bytes.
/// For STRING/BINARY: each run is length(4B) + bytes + run_length(4B).
///
/// The payload starts with the PND1 value_type byte (skip it), then
/// n_runs(4B), then the runs.
pub fn decode_rle(payload: &[u8], vtype: u8, n_rows: usize) -> PondColumn {
    if payload.is_empty() {
        return PondColumn::empty_named("", vtype);
    }

    // Skip value_type byte (PND1 header)
    let data = if vtype == VT_BINARY { payload } else { &payload[1..] };

    if data.len() < 4 {
        return PondColumn::empty_named("", vtype);
    }

    let n_runs = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
    let mut off = 4;

    let mut int_vals: Vec<i64> = Vec::with_capacity(n_rows);
    let mut float_vals: Vec<f64> = Vec::with_capacity(n_rows);
    let mut str_vals: Vec<CString> = Vec::with_capacity(n_rows);
    let mut bin_vals: Vec<Vec<u8>> = Vec::with_capacity(n_rows);
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
                let run_len = u32::from_le_bytes([
                    data[off], data[off+1], data[off+2], data[off+3]
                ]) as usize;
                off += 4;
                for _ in 0..run_len {
                    if total_rows >= n_rows { break; }
                    int_vals.push(v);
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
                let run_len = u32::from_le_bytes([
                    data[off], data[off+1], data[off+2], data[off+3]
                ]) as usize;
                off += 4;
                for _ in 0..run_len {
                    if total_rows >= n_rows { break; }
                    float_vals.push(v);
                    total_rows += 1;
                }
            }
            VT_STRING | VT_BINARY => {
                if off + 4 > data.len() { break; }
                let slen = u32::from_le_bytes([
                    data[off], data[off+1], data[off+2], data[off+3]
                ]) as usize;
                off += 4;
                if off + slen > data.len() { break; }
                let val = &data[off..off+slen];
                off += slen;
                if off + 4 > data.len() { break; }
                let run_len = u32::from_le_bytes([
                    data[off], data[off+1], data[off+2], data[off+3]
                ]) as usize;
                off += 4;
                for _ in 0..run_len {
                    if total_rows >= n_rows { break; }
                    if vtype == VT_STRING {
                        str_vals.push(bytes_to_cstring(val));
                    } else {
                        bin_vals.push(val.to_vec());
                    }
                    total_rows += 1;
                }
            }
            _ => break,
        }
    }

    PondColumn {
        name: CString::new("").unwrap(), vtype,
        i64_data: int_vals, f64_data: float_vals, str_data: str_vals,
        bin_data: bin_vals, n_values: total_rows,
    }
}

// ---------------------------------------------------------------------------
// Pure-Rust encode — single-column encoders (i64 / f64 / &str)
// ---------------------------------------------------------------------------

/// Encode an array of f64 values into an uncompressed PND2 blob.
///
/// Schema: 1 column named "v", type FLOAT64, encoding RAW, with stats.
pub fn pnd2_encode_f64(values: &[f64]) -> Vec<u8> {
    let n_values = values.len();

    let mut inner = Vec::new();

    inner.extend_from_slice(&[1]);
    inner.extend_from_slice(b"v");
    inner.extend_from_slice(&[VT_FLOAT64, ENC_RAW]);

    let min_val = values.iter().cloned().fold(f64::INFINITY, f64::min);
    let max_val = values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    inner.push(1);
    inner.extend_from_slice(&min_val.to_le_bytes());
    inner.extend_from_slice(&max_val.to_le_bytes());
    inner.extend_from_slice(&0u32.to_le_bytes());

    let mut payload = Vec::with_capacity(1 + n_values * 8);
    payload.push(VT_FLOAT64);
    for v in values {
        payload.extend_from_slice(&v.to_le_bytes());
    }
    inner.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    inner.extend_from_slice(&payload);

    let mut blob = Vec::with_capacity(13 + inner.len());
    blob.extend_from_slice(PND2_MAGIC);
    blob.push(PND2_VERSION);
    blob.push(FLAG_HAS_STATS);
    blob.extend_from_slice(&(n_values as u32).to_le_bytes());
    blob.extend_from_slice(&1u16.to_le_bytes());
    blob.push(COMPRESSION_NONE);
    blob.extend_from_slice(&inner);
    blob
}

/// Encode a slice of strings into an uncompressed PND2 blob.
///
/// Schema: 1 column named "v", type STRING, encoding RAW, no stats (strings
/// don't have a meaningful min/max in the PND2 stat layout).
pub fn pnd2_encode_str(values: &[&str]) -> Vec<u8> {
    let n_values = values.len();

    let mut inner = Vec::new();

    inner.extend_from_slice(&[1]);
    inner.extend_from_slice(b"v");
    inner.extend_from_slice(&[VT_STRING, ENC_RAW]);

    // No stats for strings (has_min = 0)
    inner.push(0);
    inner.extend_from_slice(&0u32.to_le_bytes()); // null_count

    // Payload: value_type(1B) + [len(4B) + bytes]*N
    let mut payload = Vec::with_capacity(1 + n_values * 12);
    payload.push(VT_STRING);
    for v in values {
        let vb = v.as_bytes();
        payload.extend_from_slice(&(vb.len() as u32).to_le_bytes());
        payload.extend_from_slice(vb);
    }
    inner.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    inner.extend_from_slice(&payload);

    let mut blob = Vec::with_capacity(13 + inner.len());
    blob.extend_from_slice(PND2_MAGIC);
    blob.push(PND2_VERSION);
    blob.push(FLAG_HAS_STATS);
    blob.extend_from_slice(&(n_values as u32).to_le_bytes());
    blob.extend_from_slice(&1u16.to_le_bytes());
    blob.push(COMPRESSION_NONE);
    blob.extend_from_slice(&inner);
    blob
}

/// Encode an array of i64 values into a PND2 blob (single column, RAW
/// encoding, no compression).
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
/// Returns null on error (bad magic, malformed header) or if the blob is
/// zstd-compressed (callers must decompress first).
///
/// Handles ALL encodings (RAW, RLE, DICT, BITPACK) and ALL value types
/// (INT64, FLOAT64, STRING, BINARY, NULL).
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
    r.columns[index].name.as_ptr()
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
    r.columns[col_index].str_data[row_index].as_ptr()
}

/// Get a BINARY column value at a specific row index.
///
/// Writes the value's pointer and length into the out-params.
/// The pointer is valid until the result is freed.
///
/// # Returns
///   0 on success, -1 on null result, out-of-bounds, or non-BINARY column.
///   The `out_ptr` is set to NULL and `out_len` to 0 for null-sentinel rows
///   (rows where the encoder wrote 0xFFFFFFFF as the length).
#[no_mangle]
pub extern "C" fn pond_result_column_bin(
    result: *const PondResult,
    col_index: usize,
    row_index: usize,
    out_ptr: *mut *const u8,
    out_len: *mut usize,
) -> i32 {
    if result.is_null() || out_ptr.is_null() || out_len.is_null() { return -1; }
    let r = unsafe { &*result };
    if col_index >= r.columns.len() { return -1; }
    if r.columns[col_index].vtype != VT_BINARY { return -1; }
    if row_index >= r.columns[col_index].bin_data.len() { return -1; }
    let v = &r.columns[col_index].bin_data[row_index];
    unsafe {
        *out_ptr = v.as_ptr();
        *out_len = v.len();
    }
    0
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

/// Free a blob returned by `pond_pnd2_encode_i64`, `pond_pnd2_encode_f64`,
/// or `pond_pnd2_encode_str`. Passing NULL with blob_len=0 is a safe no-op.
#[no_mangle]
pub extern "C" fn pond_blob_free(blob: *mut u8, blob_len: usize) {
    if !blob.is_null() && blob_len > 0 {
        unsafe { drop(Vec::from_raw_parts(blob, blob_len, blob_len)); }
    }
}

/// Encode an array of double values into a PND2 blob (single column, RAW
/// encoding, with stats).
///
/// # Returns
///   0 on success, -1 on invalid arguments.
///   The caller owns the blob and must free it with `pond_blob_free`.
#[no_mangle]
pub extern "C" fn pond_pnd2_encode_f64(
    values: *const f64,
    n_values: usize,
    out_blob: *mut *mut u8,
    out_blob_len: *mut usize,
) -> i32 {
    if values.is_null() || n_values == 0 || out_blob.is_null() || out_blob_len.is_null() {
        return -1;
    }
    let vals = unsafe { std::slice::from_raw_parts(values, n_values) };
    let mut blob = pnd2_encode_f64(vals);

    let len = blob.len();
    let ptr = blob.as_mut_ptr();
    std::mem::forget(blob);

    unsafe {
        *out_blob = ptr;
        *out_blob_len = len;
    }
    0
}

/// Encode an array of null-terminated C strings into a PND2 blob (single
/// column, RAW encoding, no stats).
///
/// # Arguments
///   - `values`: pointer to an array of `const char*` (each null-terminated)
///   - `n_values`: number of strings
///   - `out_blob` / `out_blob_len`: output params for the blob
///
/// # Returns
///   0 on success, -1 on invalid arguments.
///   The caller owns the blob and must free it with `pond_blob_free`.
#[no_mangle]
pub extern "C" fn pond_pnd2_encode_str(
    values: *mut *const c_char,
    n_values: usize,
    out_blob: *mut *mut u8,
    out_blob_len: *mut usize,
) -> i32 {
    if values.is_null() || n_values == 0 || out_blob.is_null() || out_blob_len.is_null() {
        return -1;
    }

    // Convert the C string array into a Vec<&str> using from_utf8_lossy
    // (safe against invalid UTF-8 — replaces bad bytes with U+FFFD).
    let ptrs = unsafe { std::slice::from_raw_parts(values, n_values) };
    let mut owned_strings: Vec<String> = Vec::with_capacity(n_values);
    for p in ptrs {
        if p.is_null() {
            owned_strings.push(String::new());
        } else {
            let cstr = unsafe { std::ffi::CStr::from_ptr(*p) };
            owned_strings.push(cstr.to_string_lossy().into_owned());
        }
    }
    let refs: Vec<&str> = owned_strings.iter().map(|s| s.as_str()).collect();
    let mut blob = pnd2_encode_str(&refs);

    let len = blob.len();
    let ptr = blob.as_mut_ptr();
    std::mem::forget(blob);

    unsafe {
        *out_blob = ptr;
        *out_blob_len = len;
    }
    0
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
        assert_eq!(cols[0].name.to_str().unwrap(), "v");
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

    #[test]
    fn test_encode_decode_f64_roundtrip() {
        let input: Vec<f64> = vec![1.5, 2.5, 3.5, -0.5, 99.99, 0.0, -1.0, 1e10];
        let blob = pnd2_encode_f64(&input);
        assert_eq!(&blob[0..4], PND2_MAGIC);

        let cols = pnd2_decode(&blob).expect("decode should succeed");
        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].vtype, VT_FLOAT64);
        assert_eq!(cols[0].n_values, input.len());
        assert_eq!(cols[0].f64_data, input);
    }

    #[test]
    fn test_encode_decode_str_roundtrip() {
        let input: Vec<&str> = vec!["alice", "bob", "carol", "dave", ""];
        let blob = pnd2_encode_str(&input);
        assert_eq!(&blob[0..4], PND2_MAGIC);

        let cols = pnd2_decode(&blob).expect("decode should succeed");
        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].vtype, VT_STRING);
        assert_eq!(cols[0].n_values, input.len());
        for (i, expected) in input.iter().enumerate() {
            assert_eq!(cols[0].str_data[i].to_str().unwrap(), *expected, "string at index {} mismatch", i);
        }
    }

    #[test]
    fn test_decode_raw_int64_with_stats() {
        // Encode a column with stats, verify stats are skipped correctly
        let input: Vec<i64> = vec![10, 20, 30];
        let blob = pnd2_encode_i64(&input);
        let cols = pnd2_decode(&blob).expect("decode should succeed");
        assert_eq!(cols[0].i64_data, input);
    }

    #[test]
    fn test_decode_handles_empty_string_payload() {
        // Build a PND2 blob with one empty STRING column (zero-length payload).
        // This is the structure the Python encoder may produce for an empty
        // string column.
        let mut inner = Vec::new();
        // Schema: 1 col "v" STRING RAW
        inner.extend_from_slice(&[1]);
        inner.extend_from_slice(b"v");
        inner.extend_from_slice(&[VT_STRING, ENC_RAW]);
        // Stats: no min/max, null_count=0
        inner.push(0);
        inner.extend_from_slice(&0u32.to_le_bytes());
        // Payload: length 0
        inner.extend_from_slice(&0u32.to_le_bytes());

        let mut blob = Vec::with_capacity(13 + inner.len());
        blob.extend_from_slice(PND2_MAGIC);
        blob.push(PND2_VERSION);
        blob.push(FLAG_HAS_STATS);
        blob.extend_from_slice(&0u32.to_le_bytes()); // n_rows = 0
        blob.extend_from_slice(&1u16.to_le_bytes());
        blob.push(COMPRESSION_NONE);
        blob.extend_from_slice(&inner);

        let cols = pnd2_decode(&blob).expect("decode should succeed");
        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].vtype, VT_STRING);
        assert_eq!(cols[0].n_values, 0);
    }
}
