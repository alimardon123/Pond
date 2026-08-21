// encode.rs — canonical record encoding.
//
// "Canonical" is the requirement, not a nicety: two replicas that hold the
// same record must produce the same bytes, or their content hashes differ and
// content-addressed dedup, convergence, and diff all quietly stop working.
// Two things guarantee it here — fields are stored in a `BTreeMap` so
// iteration order is fixed, and every length is written explicitly rather than
// inferred.
//
// Layout (all integers little-endian):
//
//   magic     "PREC"        4 bytes
//   version   1             1 byte
//   flags     bit 0 = has tombstone
//   [tombstone: physical u64, logical u64, writer u64]  if flagged
//   n_fields  u32
//   per field:
//     name_len  u16, name bytes
//     version   physical u64, logical u64, writer u64
//     type_tag  u8
//     payload   type-dependent, always length-prefixed where variable
//
// Every read is bounds-checked and no length from the buffer is used to
// pre-allocate. That is the same failure class that let one corrupted byte ask
// the PND2 decoder for a 28 GB allocation, which aborts the process rather
// than returning an error.

use std::collections::BTreeMap;

use crate::{Field, Record, Value, Version};

const MAGIC: &[u8; 4] = b"PREC";
const FORMAT_VERSION: u8 = 1;
const FLAG_TOMBSTONE: u8 = 0x01;

const T_NULL: u8 = 0;
const T_BOOL: u8 = 1;
const T_INT: u8 = 2;
const T_F64: u8 = 3;
const T_STR: u8 = 4;
const T_BYTES: u8 = 5;
const T_VECTOR: u8 = 6;
const T_JSON: u8 = 7;

/// Reject a declared field count that the buffer could not possibly hold.
/// The smallest encodable field is well over 8 bytes, so this is generous.
const MIN_FIELD_BYTES: usize = 8;

pub fn encode_record(r: &Record) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(MAGIC);
    out.push(FORMAT_VERSION);

    let flags = if r.deleted.is_some() { FLAG_TOMBSTONE } else { 0 };
    out.push(flags);

    if let Some(tomb) = r.deleted {
        put_version(&mut out, tomb);
    }

    out.extend_from_slice(&(r.fields.len() as u32).to_le_bytes());
    for (name, field) in &r.fields {
        out.extend_from_slice(&(name.len() as u16).to_le_bytes());
        out.extend_from_slice(name.as_bytes());
        put_version(&mut out, field.version);
        put_value(&mut out, &field.value);
    }
    out
}

pub fn decode_record(buf: &[u8]) -> Option<Record> {
    let mut r = Reader { buf, pos: 0 };
    if r.take(4)? != MAGIC {
        return None;
    }
    if r.u8()? != FORMAT_VERSION {
        return None;
    }
    let flags = r.u8()?;

    let deleted = if flags & FLAG_TOMBSTONE != 0 {
        Some(r.version()?)
    } else {
        None
    };

    let n_fields = r.u32()? as usize;
    // A field count the remaining bytes cannot support is malformed. Checking
    // here means the loop below can never be driven to spin on garbage.
    if n_fields.saturating_mul(MIN_FIELD_BYTES) > r.remaining() {
        return None;
    }

    let mut fields: BTreeMap<String, Field> = BTreeMap::new();
    for _ in 0..n_fields {
        let name_len = r.u16()? as usize;
        let name = String::from_utf8(r.take(name_len)?.to_vec()).ok()?;
        let version = r.version()?;
        let value = r.value()?;
        fields.insert(name, Field { value, version });
    }

    Some(Record { fields, deleted })
}

fn put_version(out: &mut Vec<u8>, v: Version) {
    out.extend_from_slice(&v.physical.to_le_bytes());
    out.extend_from_slice(&v.logical.to_le_bytes());
    out.extend_from_slice(&v.writer.to_le_bytes());
}

/// Encode one value on its own.
///
/// Exposed so a layer above can take a value out of a record, transform it —
/// encrypt it, for instance — and put the result back, without having to
/// re-implement the value encoding and risk disagreeing with it.
pub fn encode_value(v: &Value) -> Vec<u8> {
    let mut out = Vec::new();
    put_value(&mut out, v);
    out
}

/// Decode a value written by [`encode_value`].
///
/// Returns `None` for anything that is not a well-formed value, rather than
/// panicking: these bytes may have come back from storage, or from a
/// decryption that produced the wrong plaintext.
pub fn decode_value(bytes: &[u8]) -> Option<Value> {
    let mut r = Reader { buf: bytes, pos: 0 };
    r.value()
}

fn put_value(out: &mut Vec<u8>, v: &Value) {
    match v {
        Value::Null => out.push(T_NULL),
        Value::Bool(b) => {
            out.push(T_BOOL);
            out.push(*b as u8);
        }
        Value::Int(i) => {
            out.push(T_INT);
            out.extend_from_slice(&i.to_le_bytes());
        }
        Value::F64(f) => {
            out.push(T_F64);
            out.extend_from_slice(&f.to_bits().to_le_bytes());
        }
        Value::Str(s) => {
            out.push(T_STR);
            out.extend_from_slice(&(s.len() as u32).to_le_bytes());
            out.extend_from_slice(s.as_bytes());
        }
        Value::Bytes(b) => {
            out.push(T_BYTES);
            out.extend_from_slice(&(b.len() as u32).to_le_bytes());
            out.extend_from_slice(b);
        }
        Value::Vector(v) => {
            out.push(T_VECTOR);
            out.extend_from_slice(&(v.len() as u32).to_le_bytes());
            for f in v {
                out.extend_from_slice(&f.to_bits().to_le_bytes());
            }
        }
        Value::Json(s) => {
            out.push(T_JSON);
            out.extend_from_slice(&(s.len() as u32).to_le_bytes());
            out.extend_from_slice(s.as_bytes());
        }
    }
}

/// Bounds-checked reader. Every accessor returns None instead of panicking,
/// and no buffer-supplied length is ever used as an allocation size.
struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn remaining(&self) -> usize {
        self.buf.len().saturating_sub(self.pos)
    }
    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let end = self.pos.checked_add(n)?;
        let s = self.buf.get(self.pos..end)?;
        self.pos = end;
        Some(s)
    }
    fn u8(&mut self) -> Option<u8> {
        Some(self.take(1)?[0])
    }
    fn u16(&mut self) -> Option<u16> {
        Some(u16::from_le_bytes(self.take(2)?.try_into().ok()?))
    }
    fn u32(&mut self) -> Option<u32> {
        Some(u32::from_le_bytes(self.take(4)?.try_into().ok()?))
    }
    fn u64(&mut self) -> Option<u64> {
        Some(u64::from_le_bytes(self.take(8)?.try_into().ok()?))
    }
    fn version(&mut self) -> Option<Version> {
        Some(Version {
            physical: self.u64()?,
            logical: self.u64()?,
            writer: self.u64()?,
        })
    }
    fn value(&mut self) -> Option<Value> {
        Some(match self.u8()? {
            T_NULL => Value::Null,
            T_BOOL => Value::Bool(self.u8()? != 0),
            T_INT => Value::Int(self.u64()? as i64),
            T_F64 => Value::F64(f64::from_bits(self.u64()?)),
            T_STR => {
                let n = self.u32()? as usize;
                Value::Str(String::from_utf8(self.take(n)?.to_vec()).ok()?)
            }
            T_BYTES => {
                let n = self.u32()? as usize;
                Value::Bytes(self.take(n)?.to_vec())
            }
            T_VECTOR => {
                let n = self.u32()? as usize;
                // 4 bytes per f32 — verify the buffer holds them before
                // looping, so a bogus count cannot drive a long loop.
                let raw = self.take(n.checked_mul(4)?)?;
                let mut v = Vec::new();
                for c in raw.chunks_exact(4) {
                    v.push(f32::from_bits(u32::from_le_bytes(c.try_into().ok()?)));
                }
                Value::Vector(v)
            }
            T_JSON => {
                let n = self.u32()? as usize;
                Value::Json(String::from_utf8(self.take(n)?.to_vec()).ok()?)
            }
            _ => return None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(p: u64) -> Version {
        Version::new(p, 0, 7)
    }

    /// Encoding must be canonical: the same record always produces the same
    /// bytes, whatever order the fields were inserted in. Without this, two
    /// replicas holding identical data compute different content hashes.
    #[test]
    fn test_encoding_is_canonical_regardless_of_insertion_order() {
        let mut a = Record::new();
        a.set("zebra", Value::Int(1), v(1));
        a.set("alpha", Value::Int(2), v(1));
        a.set("middle", Value::Int(3), v(1));

        let mut b = Record::new();
        b.set("middle", Value::Int(3), v(1));
        b.set("alpha", Value::Int(2), v(1));
        b.set("zebra", Value::Int(1), v(1));

        assert_eq!(encode_record(&a), encode_record(&b));
    }

    #[test]
    fn test_empty_record_roundtrip() {
        let r = Record::new();
        assert_eq!(decode_record(&encode_record(&r)), Some(r));
    }

    #[test]
    fn test_tombstone_roundtrip() {
        let mut r = Record::new().with_field("x", Value::Int(1), v(1));
        r.delete(v(9));
        let decoded = decode_record(&encode_record(&r)).unwrap();
        assert_eq!(decoded, r);
        assert_eq!(decoded.deleted, Some(v(9)));
    }

    #[test]
    fn test_large_values_roundtrip() {
        let r = Record::new()
            .with_field("big_bytes", Value::Bytes(vec![7u8; 100_000]), v(1))
            .with_field(
                "big_vector",
                Value::Vector((0..4096).map(|i| i as f32).collect()),
                v(1),
            );
        assert_eq!(decode_record(&encode_record(&r)), Some(r));
    }

    /// Empty strings, empty byte arrays, and empty vectors must survive — the
    /// classic off-by-one in a length-prefixed format.
    #[test]
    fn test_empty_values_roundtrip() {
        let r = Record::new()
            .with_field("s", Value::Str(String::new()), v(1))
            .with_field("b", Value::Bytes(vec![]), v(1))
            .with_field("v", Value::Vector(vec![]), v(1));
        assert_eq!(decode_record(&encode_record(&r)), Some(r));
    }

    /// Special float values must round-trip bit-exactly; comparing them by
    /// value would be wrong for NaN, so the encoding stores raw bits.
    #[test]
    fn test_float_edge_cases_roundtrip() {
        for f in [0.0f64, -0.0, f64::INFINITY, f64::NEG_INFINITY, f64::MIN, f64::MAX] {
            let r = Record::new().with_field("f", Value::F64(f), v(1));
            let back = decode_record(&encode_record(&r)).unwrap();
            match back.get("f") {
                Some(Value::F64(g)) => assert_eq!(g.to_bits(), f.to_bits()),
                other => panic!("expected F64, got {:?}", other),
            }
        }
        // NaN survives as NaN (not comparable by ==, so check the predicate).
        let r = Record::new().with_field("f", Value::F64(f64::NAN), v(1));
        let back = decode_record(&encode_record(&r)).unwrap();
        assert!(matches!(back.get("f"), Some(Value::F64(g)) if g.is_nan()));
    }

    /// Malformed input must return None — never panic, never over-allocate.
    #[test]
    fn test_decode_rejects_malformed() {
        assert!(decode_record(&[]).is_none());
        assert!(decode_record(b"XXXX\x01\x00").is_none(), "bad magic");
        assert!(decode_record(b"PREC\xff\x00").is_none(), "bad version");

        // Claims 4 billion fields in a tiny buffer.
        let mut evil = Vec::from(*MAGIC);
        evil.push(FORMAT_VERSION);
        evil.push(0);
        evil.extend_from_slice(&u32::MAX.to_le_bytes());
        assert!(decode_record(&evil).is_none());

        // Every truncation of a valid record must decode to None, not panic.
        let good = encode_record(
            &Record::new()
                .with_field("field", Value::Str("value".into()), v(1))
                .with_field("vec", Value::Vector(vec![1.0, 2.0]), v(1)),
        );
        for cut in 0..good.len() {
            let _ = decode_record(&good[..cut]);
        }
    }

    /// Fuzz: random and mutated bytes must never panic the decoder.
    #[test]
    fn test_decode_survives_fuzzing() {
        let good = encode_record(
            &Record::new()
                .with_field("a", Value::Int(1), v(1))
                .with_field("b", Value::Bytes(vec![1, 2, 3]), v(2))
                .with_field("c", Value::Vector(vec![1.0]), v(3)),
        );

        let mut state: u64 = 0x5EED;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };

        for i in 0..50_000u32 {
            let mut b = good.clone();
            for _ in 0..(1 + next() % 4) {
                let pos = (next() as usize) % b.len();
                b[pos] = (next() >> 11) as u8;
            }
            if i % 5 == 0 {
                let cut = (next() as usize) % b.len();
                b.truncate(cut.max(1));
            }
            let _ = decode_record(&b);
        }
    }
}
