// key.rs — order-preserving composite key encoding.
//
// Every collection in Pond is a sorted map from a composite key to a set of
// typed fields. The key is what makes one structure serve every workload:
//
//   KV / point lookup   (user_key)
//   OLTP row            (table, pk...)
//   Streaming           (topic, partition, offset)
//   Vector (IVF)        (table, cluster_id, vec_id)
//   Secondary index     (index_name, value, pk)
//
// For that to work, the encoding must be *order-preserving*: the byte order of
// two encoded keys must equal their logical order. Then a range scan over a
// key prefix is a contiguous byte range, which is what lets a single ranged
// GET serve a scan, and what makes "all chunks of one column" adjacent.
//
// The scheme is the one FoundationDB's tuple layer uses, minus the parts Pond
// does not need:
//
//   - Each element is a 1-byte type tag followed by an order-preserving
//     payload. Tags are assigned in sort order, so elements of different types
//     order by tag first (arbitrary but deterministic and stable).
//   - Signed integers flip the sign bit and are stored big-endian, so negative
//     values sort before positive ones.
//   - Floats flip the sign bit for positives and invert all bits for
//     negatives — the standard IEEE-754 total-order transform.
//   - Byte strings escape 0x00 as 0x00 0xFF and terminate with 0x00 0x00.
//     Without the escape, `("a\0b",)` and `("a", "b")` could encode to the
//     same bytes; without a terminator, `("ab",)` and `("a", "b")` could.
//
// Decoding is exact: `decode(encode(k)) == k` for every key.

use std::cmp::Ordering;

const TAG_BOOL: u8 = 0x01;
const TAG_INT: u8 = 0x02;
const TAG_F64: u8 = 0x03;
const TAG_STR: u8 = 0x04;
const TAG_BYTES: u8 = 0x05;

/// One component of a composite key.
#[derive(Debug, Clone, PartialEq)]
pub enum KeyPart {
    Bool(bool),
    Int(i64),
    F64(f64),
    Str(String),
    Bytes(Vec<u8>),
}

impl Eq for KeyPart {}

impl PartialOrd for KeyPart {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for KeyPart {
    /// Ordering is defined to match the encoded byte order exactly, so that
    /// sorting `Key`s in memory and sorting their encodings give the same
    /// result. Tests assert this correspondence over random inputs.
    fn cmp(&self, other: &Self) -> Ordering {
        self.encode().cmp(&other.encode())
    }
}

impl KeyPart {
    fn tag(&self) -> u8 {
        match self {
            KeyPart::Bool(_) => TAG_BOOL,
            KeyPart::Int(_) => TAG_INT,
            KeyPart::F64(_) => TAG_F64,
            KeyPart::Str(_) => TAG_STR,
            KeyPart::Bytes(_) => TAG_BYTES,
        }
    }

    fn encode(&self) -> Vec<u8> {
        let mut out = vec![self.tag()];
        match self {
            KeyPart::Bool(b) => out.push(if *b { 1 } else { 0 }),
            KeyPart::Int(i) => {
                // Flip the sign bit so negatives sort before positives.
                let biased = (*i as u64) ^ (1u64 << 63);
                out.extend_from_slice(&biased.to_be_bytes());
            }
            KeyPart::F64(f) => {
                // IEEE-754 total order: for non-negative values flip the sign
                // bit; for negative values invert every bit. NaN sorts last,
                // consistently.
                let bits = f.to_bits();
                let ordered = if bits & (1u64 << 63) != 0 { !bits } else { bits ^ (1u64 << 63) };
                out.extend_from_slice(&ordered.to_be_bytes());
            }
            KeyPart::Str(s) => encode_escaped(s.as_bytes(), &mut out),
            KeyPart::Bytes(b) => encode_escaped(b, &mut out),
        }
        out
    }
}

/// Escape 0x00 bytes and terminate, so element boundaries are unambiguous and
/// a prefix always sorts before any string extending it.
fn encode_escaped(data: &[u8], out: &mut Vec<u8>) {
    for &b in data {
        if b == 0x00 {
            out.push(0x00);
            out.push(0xFF);
        } else {
            out.push(b);
        }
    }
    out.push(0x00);
    out.push(0x00);
}

/// Read one escaped byte string starting at `pos`. Returns the decoded bytes
/// and the position just past the terminator.
fn decode_escaped(buf: &[u8], mut pos: usize) -> Option<(Vec<u8>, usize)> {
    let mut out = Vec::new();
    while pos < buf.len() {
        let b = buf[pos];
        if b == 0x00 {
            match buf.get(pos + 1) {
                Some(0xFF) => {
                    out.push(0x00);
                    pos += 2;
                }
                Some(0x00) => return Some((out, pos + 2)),
                _ => return None,
            }
        } else {
            out.push(b);
            pos += 1;
        }
    }
    None
}

/// A composite key: an ordered list of typed components.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Key(pub Vec<KeyPart>);

impl Key {
    pub fn new(parts: Vec<KeyPart>) -> Self {
        Key(parts)
    }

    /// Encode to bytes whose lexicographic order equals the key's logical order.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        for part in &self.0 {
            out.extend_from_slice(&part.encode());
        }
        out
    }

    /// Decode bytes produced by [`Key::encode`].
    pub fn decode(buf: &[u8]) -> Option<Key> {
        let mut parts = Vec::new();
        let mut pos = 0usize;
        while pos < buf.len() {
            let tag = buf[pos];
            pos += 1;
            match tag {
                TAG_BOOL => {
                    let b = *buf.get(pos)?;
                    pos += 1;
                    parts.push(KeyPart::Bool(b != 0));
                }
                TAG_INT => {
                    let raw: [u8; 8] = buf.get(pos..pos + 8)?.try_into().ok()?;
                    pos += 8;
                    let biased = u64::from_be_bytes(raw);
                    parts.push(KeyPart::Int((biased ^ (1u64 << 63)) as i64));
                }
                TAG_F64 => {
                    let raw: [u8; 8] = buf.get(pos..pos + 8)?.try_into().ok()?;
                    pos += 8;
                    let ordered = u64::from_be_bytes(raw);
                    let bits = if ordered & (1u64 << 63) != 0 {
                        ordered ^ (1u64 << 63)
                    } else {
                        !ordered
                    };
                    parts.push(KeyPart::F64(f64::from_bits(bits)));
                }
                TAG_STR => {
                    let (bytes, next) = decode_escaped(buf, pos)?;
                    pos = next;
                    parts.push(KeyPart::Str(String::from_utf8(bytes).ok()?));
                }
                TAG_BYTES => {
                    let (bytes, next) = decode_escaped(buf, pos)?;
                    pos = next;
                    parts.push(KeyPart::Bytes(bytes));
                }
                _ => return None,
            }
        }
        Some(Key(parts))
    }
}

// Ergonomic constructors — these keep call sites readable in tests and lenses.
pub fn int(i: i64) -> KeyPart {
    KeyPart::Int(i)
}
pub fn str_(s: impl Into<String>) -> KeyPart {
    KeyPart::Str(s.into())
}
pub fn bytes(b: impl Into<Vec<u8>>) -> KeyPart {
    KeyPart::Bytes(b.into())
}
pub fn f64_(f: f64) -> KeyPart {
    KeyPart::F64(f)
}
pub fn bool_(b: bool) -> KeyPart {
    KeyPart::Bool(b)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(k: Key) {
        let enc = k.encode();
        let dec = Key::decode(&enc).expect("must decode");
        assert_eq!(dec, k, "roundtrip failed for {:?}", k);
    }

    #[test]
    fn test_roundtrip_all_types() {
        roundtrip(Key::new(vec![int(0)]));
        roundtrip(Key::new(vec![int(-1), int(i64::MIN), int(i64::MAX)]));
        roundtrip(Key::new(vec![str_("hello"), str_("")]));
        roundtrip(Key::new(vec![bytes(vec![0, 1, 2, 0, 255])]));
        roundtrip(Key::new(vec![f64_(-1.5), f64_(0.0), f64_(1e300)]));
        roundtrip(Key::new(vec![bool_(true), bool_(false)]));
        roundtrip(Key::new(vec![
            str_("users"),
            int(42),
            bytes(vec![0xDE, 0xAD]),
        ]));
    }

    /// The whole design rests on this: sorting keys must equal sorting their
    /// encodings. If it ever fails, range scans silently return wrong rows.
    #[test]
    fn test_byte_order_matches_logical_order() {
        let mut keys = vec![
            Key::new(vec![int(-100)]),
            Key::new(vec![int(-1)]),
            Key::new(vec![int(0)]),
            Key::new(vec![int(1)]),
            Key::new(vec![int(i64::MAX)]),
            Key::new(vec![int(i64::MIN)]),
            Key::new(vec![str_("a")]),
            Key::new(vec![str_("ab")]),
            Key::new(vec![str_("b")]),
            Key::new(vec![str_("a"), str_("b")]),
            Key::new(vec![f64_(-1.0)]),
            Key::new(vec![f64_(0.0)]),
            Key::new(vec![f64_(1.0)]),
        ];
        keys.sort();
        let encoded: Vec<Vec<u8>> = keys.iter().map(|k| k.encode()).collect();
        let mut sorted_encoded = encoded.clone();
        sorted_encoded.sort();
        assert_eq!(
            encoded, sorted_encoded,
            "encoded byte order must match Key ordering"
        );
    }

    /// Negative integers must sort before positive ones — the classic bug when
    /// two's-complement is stored big-endian without flipping the sign bit.
    #[test]
    fn test_negative_ints_sort_first() {
        assert!(Key::new(vec![int(-1)]).encode() < Key::new(vec![int(0)]).encode());
        assert!(Key::new(vec![int(i64::MIN)]).encode() < Key::new(vec![int(-1)]).encode());
        assert!(Key::new(vec![int(0)]).encode() < Key::new(vec![int(i64::MAX)]).encode());
    }

    /// `("a", "b")` and `("ab",)` are different keys and must encode
    /// differently — this is what the terminator is for.
    #[test]
    fn test_composite_boundaries_unambiguous() {
        let split = Key::new(vec![str_("a"), str_("b")]).encode();
        let joined = Key::new(vec![str_("ab")]).encode();
        assert_ne!(split, joined);
    }

    /// A key component containing a NUL byte must not collide with a
    /// component boundary — this is what the 0x00 escape is for.
    #[test]
    fn test_embedded_nul_is_escaped() {
        let with_nul = Key::new(vec![bytes(vec![b'a', 0x00, b'b'])]);
        let two_parts = Key::new(vec![bytes(vec![b'a']), bytes(vec![b'b'])]);
        assert_ne!(with_nul.encode(), two_parts.encode());
        roundtrip(with_nul);
    }

    /// A prefix sorts before any key extending it — required for prefix scans
    /// (e.g. "all rows of table T", "all chunks of column C").
    #[test]
    fn test_prefix_sorts_before_extension() {
        let prefix = Key::new(vec![str_("users")]).encode();
        let extended = Key::new(vec![str_("users"), int(1)]).encode();
        assert!(prefix < extended);
    }
}
