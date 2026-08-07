// encode.rs — Pure-Rust PND2 encoders.
//
// Two APIs:
//   - Single-column convenience: `pnd2_encode_i64`, `pnd2_encode_f64`,
//     `pnd2_encode_str` (each emits a 1-column blob with the appropriate
//     stats).
//   - Multi-column low-level: `pnd2_encode_multi` takes a slice of
//     `EncodeMultiColumn` specs (name + vtype + raw payload bytes + optional
//     stats) and assembles the outer PND2 container. This is the foundation
//     for cross-language SDK ports that need to build multi-column PND2
//     blobs without reimplementing the format assembly.

use crate::constants::*;

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
// Pure-Rust encode — multi-column encoder (RAW only, all numeric/string vtypes)
// ---------------------------------------------------------------------------

/// A column spec for `pnd2_encode_multi`. Each column carries its name,
/// value type, and a slice of raw bytes for its payload (which the caller
/// is responsible for laying out in PND2 RAW format).
///
/// This is a low-level API — callers must understand the RAW payload
/// layout for their chosen vtype. For single-column convenience, use
/// `pnd2_encode_i64` / `pnd2_encode_f64` / `pnd2_encode_str`.
pub struct EncodeMultiColumn<'a> {
    pub name: &'a str,
    pub vtype: u8,
    /// RAW payload bytes (NOT including the outer `payload_len` u32 — that's
    /// added by `pnd2_encode_multi`). For VT_INT64/FLOAT64 the payload is
    /// `value_type(1B) + values(N*8B)`. For VT_STRING it's
    /// `value_type(1B) + [len(4B) + bytes]*N`. For VT_BINARY it's
    /// `n_values(4B) + [len(4B) + bytes]*N`.
    pub payload: &'a [u8],
    /// Optional stats: (min_bytes, max_bytes, null_count).
    /// - INT64: 8 bytes each for min/max
    /// - FLOAT64: 8 bytes each
    /// - STRING/BINARY: None (no stats written)
    pub stats: Option<(&'a [u8], &'a [u8], u32)>,
}

/// Encode multiple columns into a single PND2 blob (RAW encoding only,
/// no compression). Each column's payload is provided directly by the
/// caller — this function just assembles the outer PND2 container.
///
/// Returns the assembled blob.
///
/// This is the foundation for cross-language SDK ports that need to build
/// multi-column PND2 blobs without reimplementing the format assembly.
pub fn pnd2_encode_multi(columns: &[EncodeMultiColumn], n_rows: usize) -> Vec<u8> {
    let mut inner = Vec::new();

    // Schema section
    for col in columns {
        let name_bytes = col.name.as_bytes();
        let name_len = name_bytes.len().min(255) as u8;
        inner.push(name_len);
        inner.extend_from_slice(&name_bytes[..name_len as usize]);
        inner.push(col.vtype);
        inner.push(ENC_RAW);
    }

    // Stats section
    for col in columns {
        match &col.stats {
            None => {
                inner.push(0); // has_min = 0
                inner.extend_from_slice(&0u32.to_le_bytes()); // null_count
            }
            Some((min_bytes, max_bytes, null_count)) => {
                inner.push(1); // has_min = 1
                inner.extend_from_slice(min_bytes);
                inner.extend_from_slice(max_bytes);
                inner.extend_from_slice(&null_count.to_le_bytes());
            }
        }
    }

    // Per-column payloads
    for col in columns {
        inner.extend_from_slice(&(col.payload.len() as u32).to_le_bytes());
        inner.extend_from_slice(col.payload);
    }

    // Final PND2 blob
    let mut blob = Vec::with_capacity(13 + inner.len());
    blob.extend_from_slice(PND2_MAGIC);
    blob.push(PND2_VERSION);
    blob.push(FLAG_HAS_STATS);
    blob.extend_from_slice(&(n_rows as u32).to_le_bytes());
    blob.extend_from_slice(&(columns.len() as u16).to_le_bytes());
    blob.push(COMPRESSION_NONE);
    blob.extend_from_slice(&inner);
    blob
}
