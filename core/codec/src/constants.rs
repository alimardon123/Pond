// constants.rs — Public constants for the PND2 binary format.
//
// These constants are `pub` so that the PyO3 wrapper crate (`pond-python`)
// and other downstream Rust crates can reuse them when constructing or
// inspecting PND2 blobs without redefining the magic bytes, value types, or
// encoding tags. They are also implicitly part of the C ABI contract —
// see `pond_core.h` for the matching `#define`s.

// ---------------------------------------------------------------------------
// Header constants
// ---------------------------------------------------------------------------

pub const PND2_MAGIC: &[u8] = b"PND2";
pub const PND2_VERSION: u8 = 2;
pub const FLAG_HAS_STATS: u8 = 0x01;
pub const FLAG_COMPRESSED: u8 = 0x02;

// ---------------------------------------------------------------------------
// Compression tags (byte 12 of the PND2 header)
// ---------------------------------------------------------------------------

pub const COMPRESSION_NONE: u8 = 0;
pub const COMPRESSION_LZ4: u8 = 1;
pub const COMPRESSION_ZSTD: u8 = 2;

// ---------------------------------------------------------------------------
// Value types (stored in the schema section, 1 byte per column)
// ---------------------------------------------------------------------------

pub const VT_INT64: u8 = 1;
pub const VT_FLOAT64: u8 = 2;
pub const VT_STRING: u8 = 3;
pub const VT_NULL: u8 = 4;
pub const VT_BINARY: u8 = 5;
pub const VT_VARIANT: u8 = 6;  // Mixed-type column — each value is a JSON-encoded string

// ---------------------------------------------------------------------------
// Encodings (stored in the schema section, 1 byte per column)
// ---------------------------------------------------------------------------

pub const ENC_RAW: u8 = 0;
pub const ENC_RLE: u8 = 1;
pub const ENC_DICT: u8 = 2;
pub const ENC_BITPACK: u8 = 3;
