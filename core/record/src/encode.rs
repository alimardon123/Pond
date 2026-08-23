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
/// The format a new record is written in.
///
/// v1 wrote a full 24-byte version stamp beside every field and a spilled
/// field's hash as 64 characters of hex. Measured on a typical row — two small
/// columns and one spilled field, 192 bytes — that is 72 bytes of versions and
/// 64 bytes of hex against 12 bytes of actual payload
/// (`pond_bench --bin recordbytes`).
///
/// v2 changes neither what a record means nor how values are laid out. It
/// stores each distinct version once in a table and has fields reference it,
/// and it stores a hash as the 32 bytes it is rather than the 64 characters it
/// prints as. The same row encodes in 115 bytes.
///
/// v1 stays decodable forever: records are content-addressed, so a format
/// change cannot rewrite what is already stored, and anything that could not
/// read v1 would be unable to read data it wrote itself last week.
const FORMAT_VERSION: u8 = 2;
const FORMAT_VERSION_V1: u8 = 1;
const FLAG_TOMBSTONE: u8 = 0x01;
/// The version table is counted and indexed with 32 bits rather than 16.
///
/// Set only when a record holds more than 65,535 *distinct* versions, which
/// takes more than 65,535 fields. It exists so the encoder never has to fall
/// back to an older framing: a record written with v1 framing but current
/// value encoding would be internally inconsistent and unreadable, which is
/// what the first version of this did.
const FLAG_WIDE_VERSIONS: u8 = 0x02;

const T_NULL: u8 = 0;
const T_BOOL: u8 = 1;
const T_INT: u8 = 2;
const T_F64: u8 = 3;
const T_STR: u8 = 4;
const T_BYTES: u8 = 5;
const T_VECTOR: u8 = 6;
const T_JSON: u8 = 7;
/// A field stored elsewhere: the tag it stands for, then its content hash.
const T_SPILLED: u8 = 8;

/// Reject a declared field count that the buffer could not possibly hold.
/// The smallest encodable field is well over 8 bytes, so this is generous.
const MIN_FIELD_BYTES: usize = 8;

pub fn encode_record(r: &Record) -> Vec<u8> {
    // The version table is built by walking fields in name order, which a
    // `BTreeMap` fixes, and keeping first-seen order. That makes the table a
    // pure function of the record — two writers holding the same record
    // produce the same table and therefore the same bytes, which is the
    // property content addressing rests on.
    let mut table: Vec<Version> = Vec::new();
    for field in r.fields.values() {
        if !table.contains(&field.version) {
            table.push(field.version);
        }
    }

    // More distinct versions than a 16-bit index can name widens the index
    // rather than changing framing. Both the flag and the table are pure
    // functions of the record, so canonical encoding is unaffected.
    let wide = table.len() > u16::MAX as usize;

    let mut out = Vec::new();
    out.extend_from_slice(MAGIC);
    out.push(FORMAT_VERSION);

    let mut flags = if r.deleted.is_some() { FLAG_TOMBSTONE } else { 0 };
    if wide {
        flags |= FLAG_WIDE_VERSIONS;
    }
    out.push(flags);

    if let Some(tomb) = r.deleted {
        put_version(&mut out, tomb);
    }

    if wide {
        out.extend_from_slice(&(table.len() as u32).to_le_bytes());
    } else {
        out.extend_from_slice(&(table.len() as u16).to_le_bytes());
    }
    for v in &table {
        put_version(&mut out, *v);
    }

    // Where each version sits, so the per-field lookup is not a linear scan of
    // the table — which would make encoding a wide record quadratic.
    let mut index_of: BTreeMap<Version, usize> = BTreeMap::new();
    for (i, v) in table.iter().enumerate() {
        index_of.insert(*v, i);
    }

    out.extend_from_slice(&(r.fields.len() as u32).to_le_bytes());
    for (name, field) in &r.fields {
        out.extend_from_slice(&(name.len() as u16).to_le_bytes());
        out.extend_from_slice(name.as_bytes());
        let idx = *index_of
            .get(&field.version)
            .expect("every field's version is in the table by construction");
        if wide {
            out.extend_from_slice(&(idx as u32).to_le_bytes());
        } else {
            out.extend_from_slice(&(idx as u16).to_le_bytes());
        }
        put_value(&mut out, &field.value);
    }
    out
}

pub fn decode_record(buf: &[u8]) -> Option<Record> {
    let mut r = Reader {
        buf,
        pos: 0,
        format: FORMAT_VERSION,
    };
    if r.take(4)? != MAGIC {
        return None;
    }
    // v1 records are still out there and always will be: a record is
    // content-addressed, so nothing rewrites what is already stored.
    let format = match r.u8()? {
        FORMAT_VERSION_V1 => FORMAT_VERSION_V1,
        FORMAT_VERSION => FORMAT_VERSION,
        _ => return None,
    };
    // The value decoder needs this: `T_SPILLED` is laid out differently in the
    // two formats.
    r.format = format;
    let flags = r.u8()?;

    let deleted = if flags & FLAG_TOMBSTONE != 0 {
        Some(r.version()?)
    } else {
        None
    };

    // v2 stores each distinct version once and has fields index into it.
    let wide = flags & FLAG_WIDE_VERSIONS != 0;
    let table: Vec<Version> = if format == FORMAT_VERSION {
        let n = if wide { r.u32()? as usize } else { r.u16()? as usize };
        // 24 bytes each; refuse a count the buffer cannot hold rather than
        // pre-allocating for it.
        if n.saturating_mul(24) > r.remaining() {
            return None;
        }
        let mut t = Vec::with_capacity(n);
        for _ in 0..n {
            t.push(r.version()?);
        }
        t
    } else {
        Vec::new()
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
        let version = if format == FORMAT_VERSION {
            // An index outside the table is malformed. Substituting a default
            // version would silently change how this field merges, which is a
            // worse outcome than refusing the record.
            let idx = if wide { r.u32()? as usize } else { r.u16()? as usize };
            *table.get(idx)?
        } else {
            r.version()?
        };
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
    let mut r = Reader {
        buf: bytes,
        pos: 0,
        format: FORMAT_VERSION,
    };
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
        Value::Spilled { type_tag, hash } => {
            out.push(T_SPILLED);
            out.push(*type_tag);
            // A hash prints as hex and *is* bytes. Storing the 64 characters
            // rather than the 32 bytes doubles the largest non-payload item in
            // a row that has a spilled field — 64 of 192 bytes on the row the
            // floor was measured on.
            //
            // Anything that is not a clean 32-byte hex string is written as
            // text, so a future digest of another width still round-trips
            // rather than being silently truncated.
            match decode_hex32(hash) {
                Some(raw) => {
                    out.push(1u8);
                    out.extend_from_slice(&raw);
                }
                None => {
                    out.push(0u8);
                    out.extend_from_slice(&(hash.len() as u16).to_le_bytes());
                    out.extend_from_slice(hash.as_bytes());
                }
            }
        }
    }
}

/// A 64-character lowercase hex string as the 32 bytes it stands for.
///
/// Returns `None` for anything else, so a hash of a different width or casing
/// is stored as text rather than mangled.
fn decode_hex32(s: &str) -> Option<[u8; 32]> {
    if s.len() != 64 {
        return None;
    }
    let b = s.as_bytes();
    let mut out = [0u8; 32];
    for (i, chunk) in b.chunks_exact(2).enumerate() {
        let hi = hex_val(chunk[0])?;
        let lo = hex_val(chunk[1])?;
        out[i] = (hi << 4) | lo;
    }
    Some(out)
}

fn hex_val(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        _ => None,
    }
}

/// The inverse of [`decode_hex32`], always lowercase so a round trip is
/// byte-identical and two writers cannot disagree on casing.
fn encode_hex32(raw: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(64);
    for b in raw {
        s.push(HEX[(b >> 4) as usize] as char);
        s.push(HEX[(b & 0x0f) as usize] as char);
    }
    s
}

/// The tag a value encodes under — what a [`Value::Spilled`] placeholder has
/// to remember so a reader knows what resolving it will produce.
pub fn type_tag_of(v: &Value) -> u8 {
    match v {
        Value::Null => T_NULL,
        Value::Bool(_) => T_BOOL,
        Value::Int(_) => T_INT,
        Value::F64(_) => T_F64,
        Value::Str(_) => T_STR,
        Value::Bytes(_) => T_BYTES,
        Value::Vector(_) => T_VECTOR,
        Value::Json(_) => T_JSON,
        Value::Spilled { type_tag, .. } => *type_tag,
    }
}

/// How many bytes a value's payload weighs, without encoding it.
///
/// Used to decide whether a field is worth spilling. Only the variable-length
/// kinds can be: a bool or an int is smaller than the pointer replacing it,
/// and spilling one would cost a request to save nothing.
pub fn payload_len(v: &Value) -> usize {
    match v {
        Value::Str(s) | Value::Json(s) => s.len(),
        Value::Bytes(b) => b.len(),
        Value::Vector(f) => f.len() * 4,
        _ => 0,
    }
}

/// The bytes a spilled field's payload is stored as, and the tag to remember.
///
/// Encoded with the same `put_value` a inline field uses, so resolving one
/// yields a value byte-identical to the one that was stored — the round trip
/// is the encoder's own, not a second implementation of it.
pub fn spill_payload(v: &Value) -> (u8, Vec<u8>) {
    let mut out = Vec::new();
    put_value(&mut out, v);
    (type_tag_of(v), out)
}

/// Read back what [`spill_payload`] wrote.
pub fn unspill_payload(bytes: &[u8]) -> Option<Value> {
    let mut r = Reader {
        buf: bytes,
        pos: 0,
        format: FORMAT_VERSION,
    };
    r.value()
}

/// Bounds-checked reader. Every accessor returns None instead of panicking,
/// and no buffer-supplied length is ever used as an allocation size.
struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
    /// Which record format the value decoder should expect.
    ///
    /// `T_SPILLED` is laid out differently in v1 and v2, and a value decoder
    /// that does not know which it is reading misreads every spilled field in
    /// every record written before the change. That is not a theoretical
    /// break: a pond written by the previous build failed its first read with
    /// "corrupt data", and the unit test missed it because the v1 record it
    /// built by hand had no spilled field in it.
    format: u8,
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
            T_SPILLED => {
                let type_tag = self.u8()?;
                let hash = if self.format == FORMAT_VERSION_V1 {
                    // v1 wrote a length-prefixed hex string and no form
                    // marker.
                    let n = self.u16()? as usize;
                    String::from_utf8(self.take(n)?.to_vec()).ok()?
                } else {
                    match self.u8()? {
                        1 => encode_hex32(self.take(32)?.try_into().ok()?),
                        0 => {
                            let n = self.u16()? as usize;
                            String::from_utf8(self.take(n)?.to_vec()).ok()?
                        }
                        _ => return None,
                    }
                };
                Value::Spilled { type_tag, hash }
            }
            _ => return None,
        })
    }
}

#[cfg(test)]
mod tests {
    /// A complete v1 encoder, for compatibility tests only.
    ///
    /// Production has none: a record written with v1 framing but current value
    /// encoding is internally inconsistent and unreadable, which is exactly
    /// what the first attempt at a >65,535-version fallback produced. The
    /// encoder now widens its index instead of changing framing, so this lives
    /// here and nowhere else.
    fn encode_record_v1(r: &Record) -> Vec<u8> {
        let mut out = Vec::from(*MAGIC);
        out.push(FORMAT_VERSION_V1);
        out.push(if r.deleted.is_some() { FLAG_TOMBSTONE } else { 0 });
        if let Some(tomb) = r.deleted {
            put_version(&mut out, tomb);
        }
        out.extend_from_slice(&(r.fields.len() as u32).to_le_bytes());
        for (name, field) in &r.fields {
            out.extend_from_slice(&(name.len() as u16).to_le_bytes());
            out.extend_from_slice(name.as_bytes());
            put_version(&mut out, field.version);
            put_value_v1(&mut out, &field.value);
        }
        out
    }

    /// v1's value encoding. Identical to the current one except for
    /// `T_SPILLED`, which is the tag that changed.
    fn put_value_v1(out: &mut Vec<u8>, v: &Value) {
        match v {
            Value::Spilled { type_tag, hash } => {
                out.push(T_SPILLED);
                out.push(*type_tag);
                out.extend_from_slice(&(hash.len() as u16).to_le_bytes());
                out.extend_from_slice(hash.as_bytes());
            }
            other => put_value(out, other),
        }
    }

    /// The change that pays for itself: a version stamp is written once per
    /// distinct version, not once per field.
    #[test]
    fn fields_written_together_share_one_version_stamp() {
        let v = Version::new(1_700_000_000_000, 0, 7);
        let one = Record::new().with_field("a", Value::Int(1), v);
        let three = Record::new()
            .with_field("a", Value::Int(1), v)
            .with_field("b", Value::Int(2), v)
            .with_field("c", Value::Int(3), v);

        let grew_by = encode_record(&three).len() - encode_record(&one).len();
        // Two more fields: name (2+1), version index (2), tag (1), payload (8)
        // — 14 each, and no second or third 24-byte stamp.
        assert_eq!(
            grew_by, 28,
            "two extra fields sharing a version should cost 14 bytes each, \
             not 38"
        );
    }

    /// Fields written at different times keep their own versions — the sharing
    /// is an encoding detail and must not merge two distinct stamps.
    #[test]
    fn distinct_versions_survive_the_table() {
        let a = Version::new(100, 0, 1);
        let b = Version::new(200, 0, 2);
        let r = Record::new()
            .with_field("x", Value::Int(1), a)
            .with_field("y", Value::Int(2), b);
        let back = decode_record(&encode_record(&r)).unwrap();
        assert_eq!(back.fields["x"].version, a);
        assert_eq!(back.fields["y"].version, b);
        assert_eq!(back, r);
    }

    /// A hash is stored as the 32 bytes it is, not the 64 characters it prints
    /// as — and comes back identical.
    #[test]
    fn a_spilled_hash_round_trips_through_its_bytes() {
        let hash = "0123456789abcdef".repeat(4);
        assert_eq!(hash.len(), 64);
        let r = Record::new().with_field(
            "big",
            Value::Spilled {
                type_tag: 4,
                hash: hash.clone(),
            },
            Version::new(1, 0, 1),
        );
        let encoded = encode_record(&r);
        let back = decode_record(&encoded).unwrap();
        assert_eq!(back, r);
        assert!(
            !encoded.windows(64).any(|w| w == hash.as_bytes()),
            "the hex text must not appear in the encoding"
        );
    }

    /// A digest that is not 32 bytes of lowercase hex is stored as text rather
    /// than mangled — a future hash of another width still round-trips.
    #[test]
    fn an_unusual_hash_falls_back_to_text() {
        for odd in ["short", &"a".repeat(128), &"A".repeat(64), ""] {
            let r = Record::new().with_field(
                "big",
                Value::Spilled {
                    type_tag: 4,
                    hash: odd.to_string(),
                },
                Version::new(1, 0, 1),
            );
            assert_eq!(
                decode_record(&encode_record(&r)).as_ref(),
                Some(&r),
                "a hash of {:?} must survive a round trip",
                odd
            );
        }
    }

    /// A v1 record still decodes, and means exactly what it meant.
    ///
    /// Built by hand: no encoder in the tree produces v1 for an ordinary
    /// record any more, and a compatibility claim nothing exercises is a
    /// guess. Records are content-addressed, so every v1 record ever written
    /// is still out there byte-for-byte.
    #[test]
    fn a_v1_record_still_decodes() {
        let mut bytes = Vec::from(*MAGIC);
        bytes.push(FORMAT_VERSION_V1);
        bytes.push(0u8); // no tombstone
        bytes.extend_from_slice(&2u32.to_le_bytes());
        for (name, val) in [("age", 30i64), ("id", 7)] {
            bytes.extend_from_slice(&(name.len() as u16).to_le_bytes());
            bytes.extend_from_slice(name.as_bytes());
            bytes.extend_from_slice(&100u64.to_le_bytes());
            bytes.extend_from_slice(&0u64.to_le_bytes());
            bytes.extend_from_slice(&9u64.to_le_bytes());
            bytes.push(T_INT);
            bytes.extend_from_slice(&val.to_le_bytes());
        }

        let r = decode_record(&bytes).expect("a v1 record must decode");
        assert_eq!(r.fields.len(), 2);
        assert_eq!(r.fields["age"].value, Value::Int(30));
        assert_eq!(r.fields["id"].value, Value::Int(7));
        assert_eq!(r.fields["age"].version, Version::new(100, 0, 9));
    }

    /// A v1 record holding a *spilled* field still decodes.
    ///
    /// This is the case the first compatibility test missed, and missing it
    /// broke every existing pond that had a large field: `T_SPILLED` is laid
    /// out differently in the two formats, so a decoder that does not know
    /// which one it is reading misreads the hash and reports the whole record
    /// as corrupt. The hand-built v1 record in the test above carried only
    /// integers, so it exercised nothing that had changed.
    ///
    /// Found by reading a pond written by the previous build, not by the
    /// suite. This is the suite catching up.
    #[test]
    fn a_v1_record_with_a_spilled_field_still_decodes() {
        let hash = "0123456789abcdef".repeat(4);
        let mut bytes = Vec::from(*MAGIC);
        bytes.push(FORMAT_VERSION_V1);
        bytes.push(0u8);
        bytes.extend_from_slice(&1u32.to_le_bytes());
        bytes.extend_from_slice(&(4u16).to_le_bytes());
        bytes.extend_from_slice(b"body");
        bytes.extend_from_slice(&100u64.to_le_bytes());
        bytes.extend_from_slice(&0u64.to_le_bytes());
        bytes.extend_from_slice(&9u64.to_le_bytes());
        bytes.push(T_SPILLED);
        bytes.push(T_STR); // the tag it stands for
        // v1 layout: a length-prefixed hex string, with no form marker.
        bytes.extend_from_slice(&(hash.len() as u16).to_le_bytes());
        bytes.extend_from_slice(hash.as_bytes());

        let r = decode_record(&bytes).expect("a v1 spilled field must decode");
        assert_eq!(
            r.fields["body"].value,
            Value::Spilled {
                type_tag: T_STR,
                hash: hash.clone()
            },
            "the hash must come back exactly, or the payload cannot be found"
        );

        // And re-encoding it produces the smaller v2 form with the same
        // meaning — an old record read and written back is not corrupted.
        let round = decode_record(&encode_record(&r)).unwrap();
        assert_eq!(round, r);
    }

    /// Every value type survives a v1 round trip, so no other tag has the same
    /// problem `T_SPILLED` had.
    #[test]
    fn every_value_type_decodes_from_v1() {
        let cases = [
            Value::Null,
            Value::Bool(true),
            Value::Int(-5),
            Value::F64(2.5),
            Value::Str("hi".into()),
            Value::Bytes(vec![1, 2, 3]),
            Value::Vector(vec![1.0, 2.0]),
            Value::Json("{}".into()),
            Value::Spilled {
                type_tag: T_BYTES,
                hash: "f".repeat(64),
            },
        ];
        for value in cases {
            let r = Record::new().with_field("f", value.clone(), Version::new(1, 0, 1));
            let v1 = encode_record_v1(&r);
            assert_eq!(
                decode_record(&v1).as_ref(),
                Some(&r),
                "{:?} must survive a v1 round trip",
                value.type_name()
            );
        }
    }

    /// A v1 record and a v2 record with the same contents mean the same thing,
    /// even though their bytes differ.
    #[test]
    fn v1_and_v2_agree_on_meaning_while_differing_in_bytes() {
        let v = Version::new(100, 0, 9);
        let r = Record::new()
            .with_field("a", Value::Int(1), v)
            .with_field("b", Value::Str("x".into()), v);

        let v2 = encode_record(&r);
        let v1 = encode_record_v1(&r);
        assert_ne!(v1, v2, "v2 exists because it is smaller");
        assert!(v2.len() < v1.len());
        assert_eq!(decode_record(&v1), decode_record(&v2));
        assert_eq!(decode_record(&v2).as_ref(), Some(&r));
    }

    /// A version index outside the table is malformed. Defaulting it would
    /// silently change how that field merges, which is worse than refusing.
    #[test]
    fn an_out_of_range_version_index_is_refused() {
        let r = Record::new().with_field("a", Value::Int(1), Version::new(1, 0, 1));
        let mut bytes = encode_record(&r);
        // The index sits after magic(4) ver(1) flags(1) n_versions(2)
        // version(24) n_fields(4) name_len(2) name(1).
        let idx_at = 4 + 1 + 1 + 2 + 24 + 4 + 2 + 1;
        bytes[idx_at] = 0xff;
        bytes[idx_at + 1] = 0xff;
        assert_eq!(decode_record(&bytes), None);
    }

    /// A version-table count the buffer cannot hold must be refused without
    /// allocating for it.
    #[test]
    fn an_absurd_version_count_is_refused() {
        let mut bytes = Vec::from(*MAGIC);
        bytes.push(FORMAT_VERSION);
        bytes.push(0u8);
        bytes.extend_from_slice(&u16::MAX.to_le_bytes());
        assert_eq!(decode_record(&bytes), None);
    }

    /// Canonical encoding: the same record always produces the same bytes,
    /// whatever order its fields were added in.
    #[test]
    fn the_version_table_is_a_pure_function_of_the_record() {
        let a = Version::new(100, 0, 1);
        let b = Version::new(200, 0, 2);
        let one = Record::new()
            .with_field("z", Value::Int(1), b)
            .with_field("a", Value::Int(2), a);
        let two = Record::new()
            .with_field("a", Value::Int(2), a)
            .with_field("z", Value::Int(1), b);
        assert_eq!(
            encode_record(&one),
            encode_record(&two),
            "field insertion order must not reach the bytes"
        );
    }

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
