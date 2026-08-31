// column.rs — column chunk codec.
//
// A column chunk is one column's values for one leaf's worth of rows, in one
// content-addressed blob — see `docs/COLUMNAR_LAYOUT.md` for why it exists:
// reading two columns of fifty should fetch two chunks, not every leaf. This
// module is only the codec. It does not know about leaves, footers, or the
// read path — see that document for the order the rest gets built in, and why
// building the codec first, in isolation, with its bytes pinned, is the point.
//
// # One type per chunk
//
// A column is one field across many rows, so every *typed* value in it must
// agree on which [`pond_record::Value`] variant it is — a chunk cannot hold an
// `Int` in one row and a `Str` in the next, because there is no single payload
// format that could hold both. `encode` refuses a chunk that disagrees rather
// than picking a variant and lying about the rest.
//
// `Value::Spilled` never appears here: it is a placeholder for a payload that
// lives elsewhere, and a chunk stores resolved values. Its own doc comment
// says no consumer outside `pond_engine` should ever observe one; a chunk
// encoder is squarely inside that boundary, and encoding one would let it leak
// into an object nothing downstream resolves.
//
// # Two different nulls
//
// `values: &[Option<Value>]` has two ways to be "null" at row *i*, and they
// mean different things:
//
//   `None`             the row has no value for this column at all — the
//                       field was never written, or was resolved from a
//                       record that lacks it.
//   `Some(Value::Null)` the row's field is present and its value is the null
//                       value — a fact the record explicitly stores.
//
// Conflating them would silently turn an absent field into an explicit one or
// the reverse, and either direction changes what a later write sees when it
// merges: an absent field is filled in by a bystander merge without
// complaint, an explicit null is a value like any other and only a strictly
// newer write may replace it. So both are represented, distinctly, in every
// row: a presence bit (is this row in the chunk at all?) and, only when
// present, a null bit (is its value literally `Value::Null`, with no typed
// payload to read?). Neither bit constrains the chunk's type — a chunk of all
// nulls and absences carries the sentinel type tag [`TAG_NONE`], and a chunk
// mixing typed values with either kind of null is still single-typed as long
// as every *typed* value agrees.
//
// # Byte layout (little-endian lengths)
//
//   magic               "PCOL", 4 bytes
//   version             1 byte — see `FORMAT_VERSION`'s doc comment for why
//   type_tag: u8        the one variant every typed value in this chunk
//                       shares, or TAG_NONE if there are none
//   n: u32              row count
//   presence bitmap     ceil(n/8) bytes, bit i set means row i is `Some(_)`
//   null bitmap         ceil(n/8) bytes, bit i set means row i is
//                       `Some(Value::Null)`; meaningless where the presence
//                       bit is clear
//   payloads            one per row where presence is set and null is clear,
//                       in row order, encoded per `type_tag` with no further
//                       framing — the type is already fixed for the whole
//                       chunk, so no per-value tag byte is spent
//
// Fixed-width payloads (`Bool` 1 byte, `Int` 8, `F64` 8 via `to_bits`) are
// written plainly. Variable-width ones (`Str`, `Bytes`, `Json`, `Vector`) are
// length-prefixed the same way `pond_record::encode` prefixes them, so a
// truncated or hostile length is caught by a bounds check rather than trusted
// — the same discipline `core/index/src/node.rs` documents as the fix for the
// bug that let one corrupted byte in the PND2 decoder ask for a 28 GB
// allocation.
//
// Every length here is explicit and every read of it is bounds-checked before
// use, so a malformed buffer is refused rather than partially decoded — see
// `Reader` below, which mirrors the one in `pond_record::encode` and
// `pond_index::node`.

use pond_record::Value;

const MAGIC: &[u8; 4] = b"PCOL";
/// The format a chunk is written in.
///
/// Without a magic and a version, `encode(&[])` is `[00,00,00,00,00]` —
/// byte-for-byte identical to `pond_index::node::Node::Leaf { entries: [] }
/// .encode()`, which also puts a `u32` count right after its tag byte and
/// also accepts tags in 0..=7. Same bytes, same SHA-256, same name in the
/// shared, content-addressed object store, from two codecs that agree on
/// nothing else. `MAGIC` and `FORMAT_VERSION` close that off, laid out the
/// way `core/record/src/encode.rs:31,48` leads with `PREC` for the same
/// reason.
///
/// `docs/COLUMNAR_LAYOUT.md` used to claim this format "already carries a
/// version" before it did any such thing; that line is fixed to describe
/// what is now actually true.
const FORMAT_VERSION: u8 = 1;

const TAG_NONE: u8 = 0;
const TAG_BOOL: u8 = 1;
const TAG_INT: u8 = 2;
const TAG_F64: u8 = 3;
const TAG_STR: u8 = 4;
const TAG_BYTES: u8 = 5;
const TAG_VECTOR: u8 = 6;
const TAG_JSON: u8 = 7;

/// Cap on a chunk's declared row count, used to reject malformed chunks
/// before allocating.
///
/// A column chunk holds at most one leaf's worth of rows, and a leaf holds at
/// most [`pond_index::chunk::MAX_ENTRIES_PER_CHUNK`] entries — using that
/// constant directly, rather than a second number that happens to agree with
/// it today, means the two cannot drift apart the way `MAX_NODE_BYTES` and the
/// leaf bound can in `pond_index::node` (there the index cannot depend on the
/// engine, so it keeps a documented assumption instead; here nothing stops
/// referencing the real thing).
///
/// # The coupling is safe in one direction only
///
/// Raising the leaf bound is harmless: chunks already written stay under the
/// new, larger cap and keep decoding.
///
/// **Lowering it is a format break.** This cap is not a tuning parameter — it
/// is part of what `decode` accepts, so reducing it makes every already-written
/// chunk with more rows than the new value permanently undecodable, in a store
/// that by design cannot rewrite what it holds. Nothing in `pond_index` would
/// warn you: chunking's own constants are a size/latency trade there, and the
/// person tuning them has no reason to look here.
///
/// If the leaf bound ever needs to shrink, this constant must be pinned to the
/// old value instead of following it, and a comment must say why. That is the
/// cost of referencing the real thing rather than duplicating it — the drift
/// this coupling prevents is worth more than the hazard it introduces, but the
/// hazard is real and undocumented hazards are how formats break.
const MAX_DECLARED_ROWS: usize = pond_index::chunk::MAX_ENTRIES_PER_CHUNK;

/// The variant tag a typed (non-`Null`, non-`Spilled`) value encodes under.
fn tag_of(v: &Value) -> Option<u8> {
    match v {
        Value::Bool(_) => Some(TAG_BOOL),
        Value::Int(_) => Some(TAG_INT),
        Value::F64(_) => Some(TAG_F64),
        Value::Str(_) => Some(TAG_STR),
        Value::Bytes(_) => Some(TAG_BYTES),
        Value::Vector(_) => Some(TAG_VECTOR),
        Value::Json(_) => Some(TAG_JSON),
        Value::Null | Value::Spilled { .. } => None,
    }
}

fn set_bit(bitmap: &mut [u8], i: usize) {
    bitmap[i / 8] |= 1 << (i % 8);
}

fn get_bit(bitmap: &[u8], i: usize) -> bool {
    (bitmap[i / 8] >> (i % 8)) & 1 == 1
}

/// Writes a `u32` length prefix, refusing a length this format has no room
/// for rather than truncating it — `n as u32` on a length over 4 GiB wraps
/// to `n % 2^32` and writes a prefix for the wrong length entirely.
fn put_len_prefix(out: &mut Vec<u8>, len: usize) -> Option<()> {
    let len: u32 = len.try_into().ok()?;
    out.extend_from_slice(&len.to_le_bytes());
    Some(())
}

fn put_payload(out: &mut Vec<u8>, v: &Value) -> Option<()> {
    match v {
        Value::Bool(b) => out.push(*b as u8),
        Value::Int(i) => out.extend_from_slice(&i.to_le_bytes()),
        Value::F64(f) => out.extend_from_slice(&f.to_bits().to_le_bytes()),
        Value::Str(s) => {
            put_len_prefix(out, s.len())?;
            out.extend_from_slice(s.as_bytes());
        }
        Value::Bytes(b) => {
            put_len_prefix(out, b.len())?;
            out.extend_from_slice(b);
        }
        Value::Vector(fs) => {
            put_len_prefix(out, fs.len())?;
            for f in fs {
                out.extend_from_slice(&f.to_bits().to_le_bytes());
            }
        }
        Value::Json(s) => {
            put_len_prefix(out, s.len())?;
            out.extend_from_slice(s.as_bytes());
        }
        Value::Null | Value::Spilled { .. } => {
            unreachable!("encode() filters both out before calling put_payload")
        }
    }
    Some(())
}

/// Encode a column chunk. `None` at index `i` means row `i` has no value for
/// this column; `Some(Value::Null)` means it has one and the value is null —
/// see the module comment for why those are kept apart.
///
/// Returns `None` when the input cannot be represented as one chunk: a
/// `Value::Spilled` anywhere (a chunk stores resolved values, never
/// placeholders), two typed values that are not the same variant, more rows
/// than [`MAX_DECLARED_ROWS`] (this format's own decoder would refuse the
/// result, so encoding it would record a name for a permanently unreadable
/// object), or a `Str`, `Bytes`, `Vector`, or `Json` payload whose length
/// does not fit in the `u32` this format prefixes it with.
pub fn encode(values: &[Option<Value>]) -> Option<Vec<u8>> {
    let n = values.len();
    if n > MAX_DECLARED_ROWS {
        return None;
    }

    // One pass to find the chunk's type — the variant every typed value must
    // share — and to refuse anything this format cannot hold. `Value::Null`
    // and `None` are both skipped here: neither has a payload, so neither can
    // conflict with a type.
    let mut chunk_type: Option<u8> = None;
    for v in values {
        let Some(val) = v else { continue };
        if val.is_spilled() {
            return None;
        }
        let Some(t) = tag_of(val) else { continue }; // Value::Null
        match chunk_type {
            None => chunk_type = Some(t),
            Some(existing) if existing != t => return None,
            _ => {}
        }
    }
    let type_tag = chunk_type.unwrap_or(TAG_NONE);

    let bitmap_bytes = n.div_ceil(8);
    let mut present = vec![0u8; bitmap_bytes];
    let mut is_null = vec![0u8; bitmap_bytes];
    for (i, v) in values.iter().enumerate() {
        match v {
            Some(Value::Null) => {
                set_bit(&mut present, i);
                set_bit(&mut is_null, i);
            }
            Some(_) => set_bit(&mut present, i),
            None => {}
        }
    }

    let mut out = Vec::new();
    out.extend_from_slice(MAGIC);
    out.push(FORMAT_VERSION);
    out.push(type_tag);
    // `n <= MAX_DECLARED_ROWS`, checked above, so this cannot truncate.
    out.extend_from_slice(&(n as u32).to_le_bytes());
    out.extend_from_slice(&present);
    out.extend_from_slice(&is_null);
    for v in values {
        match v {
            Some(Value::Null) | None => {}
            Some(val) => put_payload(&mut out, val)?,
        }
    }
    Some(out)
}

/// The bits in a `ceil(n/8)`-byte bitmap at and above row `n` carry no
/// meaning — `encode` always leaves them zero, so a decoder that ignored
/// them would accept a set bit there as just another spelling of the same
/// chunk. Reject it instead: see the module comment on canonical decoding.
fn bitmap_padding_is_clear(bitmap: &[u8], n: usize) -> bool {
    let used = n % 8;
    if used == 0 {
        return true; // no partial last byte
    }
    bitmap[bitmap.len() - 1] & (!0u8 << used) == 0
}

/// Decode a chunk written by [`encode`].
///
/// Returns `None` for anything malformed — a truncated buffer, a declared row
/// count or payload length the buffer cannot support, an unknown type tag —
/// rather than guessing. Never allocates proportional to a count taken
/// straight from the buffer: `n` is checked against [`MAX_DECLARED_ROWS`]
/// before it drives anything, and the output vector is grown incrementally
/// rather than pre-sized from it, the same discipline `pond_index::node` uses
/// for its declared entry count.
///
/// # Canonical decoding
///
/// These bytes are an object's *name* — the store addresses a chunk by the
/// hash of exactly this encoding — so a decoder that accepts more than one
/// spelling of a value breaks the promise that the name proves the content:
/// re-encoding what you decoded would not reproduce the bytes you were
/// handed, and a second implementation that spells the same value
/// differently would silently mint a second name for it. `encode` already
/// produces only one spelling per value, so this decoder rejects every
/// other one rather than accepting it: trailing bytes past the last field,
/// set padding bits above `n` in either bitmap, a null bit set on a row
/// whose presence bit is clear, a bool payload byte other than 0 or 1, and
/// a non-`TAG_NONE` type tag on a chunk with no typed row (all eight tag
/// values are otherwise indistinguishable on an all-null-or-absent chunk,
/// since no payload is ever read to tell them apart).
///
/// `pond_index::node::decode_plain` does not do this, and that is a
/// deliberate difference, not an oversight this should match: it has no
/// bitmaps, no sentinel type tag, and no bool byte, so it has far fewer
/// spellings of a value to get wrong in the first place.
pub fn decode(buf: &[u8]) -> Option<Vec<Option<Value>>> {
    let mut r = Reader { buf, pos: 0 };
    if r.take(4)? != MAGIC {
        return None;
    }
    if r.u8()? != FORMAT_VERSION {
        return None;
    }
    let type_tag = r.u8()?;
    if type_tag > TAG_JSON {
        return None;
    }
    let n = r.u32()? as usize;
    if n > MAX_DECLARED_ROWS {
        return None;
    }
    let bitmap_bytes = n.div_ceil(8);
    let present = r.take(bitmap_bytes)?;
    let is_null = r.take(bitmap_bytes)?;
    if !bitmap_padding_is_clear(present, n) || !bitmap_padding_is_clear(is_null, n) {
        return None;
    }

    let mut saw_typed = false;
    let mut out = Vec::new();
    for i in 0..n {
        if !get_bit(present, i) {
            // The null bit is meaningless where presence is clear, but
            // `encode` always leaves it clear there too — a set bit is a
            // second spelling of the same absent row.
            if get_bit(is_null, i) {
                return None;
            }
            out.push(None);
            continue;
        }
        if get_bit(is_null, i) {
            out.push(Some(Value::Null));
            continue;
        }
        // A row claiming a typed value in a chunk whose type is TAG_NONE has
        // no defined payload format to read; `Reader::payload` already
        // returns `None` for `TAG_NONE`, so this falls through to that.
        saw_typed = true;
        out.push(Some(r.payload(type_tag)?));
    }
    // A chunk with no typed row has nothing to fix a type from, so `encode`
    // always writes TAG_NONE for it — any other tag is unconstrained, since
    // no payload is ever read to check it against, and would let one chunk
    // have as many as eight valid names.
    if !saw_typed && type_tag != TAG_NONE {
        return None;
    }
    if r.pos != buf.len() {
        return None;
    }
    Some(out)
}

/// Bounds-checked reader. Every accessor returns `None` rather than panicking
/// or over-allocating, so a corrupted chunk is an error, not a crash — see
/// the equivalent in `pond_record::encode` and `pond_index::node`.
struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let end = self.pos.checked_add(n)?;
        let slice = self.buf.get(self.pos..end)?;
        self.pos = end;
        Some(slice)
    }
    fn u8(&mut self) -> Option<u8> {
        Some(self.take(1)?[0])
    }
    fn u32(&mut self) -> Option<u32> {
        Some(u32::from_le_bytes(self.take(4)?.try_into().ok()?))
    }

    fn payload(&mut self, type_tag: u8) -> Option<Value> {
        Some(match type_tag {
            // `encode` writes exactly 0 or 1 — any other byte is a second
            // spelling of `true` this decoder must not accept.
            TAG_BOOL => Value::Bool(match self.u8()? {
                0 => false,
                1 => true,
                _ => return None,
            }),
            TAG_INT => Value::Int(i64::from_le_bytes(self.take(8)?.try_into().ok()?)),
            TAG_F64 => {
                Value::F64(f64::from_bits(u64::from_le_bytes(self.take(8)?.try_into().ok()?)))
            }
            TAG_STR => {
                let n = self.u32()? as usize;
                Value::Str(String::from_utf8(self.take(n)?.to_vec()).ok()?)
            }
            TAG_BYTES => {
                let n = self.u32()? as usize;
                Value::Bytes(self.take(n)?.to_vec())
            }
            TAG_VECTOR => {
                let n = self.u32()? as usize;
                // Bounds-check the whole block before looping, so a bogus
                // count cannot drive a long loop over a short buffer.
                let raw = self.take(n.checked_mul(4)?)?;
                let mut v = Vec::new();
                for c in raw.as_chunks::<4>().0 {
                    v.push(f32::from_bits(u32::from_le_bytes(*c)));
                }
                Value::Vector(v)
            }
            TAG_JSON => {
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

    // -----------------------------------------------------------------------
    // Round trips
    // -----------------------------------------------------------------------

    fn round_trip(values: Vec<Option<Value>>) {
        let encoded = encode(&values).expect("must encode");
        let decoded = decode(&encoded).expect("must decode");
        assert_eq!(decoded, values);
    }

    #[test]
    fn bool_values_round_trip() {
        round_trip(vec![Some(Value::Bool(true)), Some(Value::Bool(false))]);
    }

    #[test]
    fn int_edges_round_trip() {
        round_trip(vec![
            Some(Value::Int(i64::MIN)),
            Some(Value::Int(i64::MAX)),
            Some(Value::Int(0)),
            Some(Value::Int(-1)),
        ]);
    }

    /// `NaN != NaN`, so bit patterns are compared rather than the decoded
    /// values — the module comment for `test_float_edge_cases_roundtrip` in
    /// `pond_record` makes the same point.
    #[test]
    fn f64_edges_round_trip_by_bits() {
        let values = vec![
            Some(Value::F64(-0.0)),
            Some(Value::F64(0.0)),
            Some(Value::F64(f64::NAN)),
            Some(Value::F64(f64::INFINITY)),
            Some(Value::F64(f64::NEG_INFINITY)),
        ];
        let encoded = encode(&values).unwrap();
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded.len(), values.len());
        for (got, want) in decoded.iter().zip(&values) {
            match (got, want) {
                (Some(Value::F64(g)), Some(Value::F64(w))) => {
                    assert_eq!(g.to_bits(), w.to_bits(), "{:?} vs {:?}", g, w);
                }
                _ => panic!("expected F64 in both, got {:?} / {:?}", got, want),
            }
        }
        // -0.0 and 0.0 are distinct bit patterns and must not collapse.
        assert_ne!(0.0f64.to_bits(), (-0.0f64).to_bits());
    }

    #[test]
    fn str_values_round_trip_empty_ascii_and_multibyte() {
        round_trip(vec![
            Some(Value::Str(String::new())),
            Some(Value::Str("plain ascii".into())),
            Some(Value::Str("héllo wörld 日本語 🎉".into())),
        ]);
    }

    #[test]
    fn bytes_values_round_trip_empty_and_with_zero_bytes_inside() {
        round_trip(vec![
            Some(Value::Bytes(vec![])),
            Some(Value::Bytes(vec![0, 0, 1, 0, 255, 0])),
        ]);
    }

    #[test]
    fn vector_values_round_trip_empty_and_with_nan() {
        let values = vec![
            Some(Value::Vector(vec![])),
            Some(Value::Vector(vec![1.0, -2.5, 0.0, -0.0, f32::NAN, f32::INFINITY])),
        ];
        let encoded = encode(&values).unwrap();
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded.len(), 2);
        match (&decoded[0], &values[0]) {
            (Some(Value::Vector(g)), Some(Value::Vector(w))) => assert_eq!(g, w),
            other => panic!("empty vector mismatch: {:?}", other),
        }
        match (&decoded[1], &values[1]) {
            (Some(Value::Vector(g)), Some(Value::Vector(w))) => {
                assert_eq!(g.len(), w.len());
                for (gv, wv) in g.iter().zip(w) {
                    assert_eq!(gv.to_bits(), wv.to_bits());
                }
            }
            other => panic!("vector mismatch: {:?}", other),
        }
    }

    #[test]
    fn json_values_round_trip() {
        round_trip(vec![
            Some(Value::Json("{}".into())),
            Some(Value::Json(r#"{"a":[1,2,3],"b":null}"#.into())),
        ]);
    }

    /// `Value::Null` — an explicit null, present in the row — round trips as
    /// itself and is not confused with an absent row.
    #[test]
    fn explicit_null_values_round_trip() {
        round_trip(vec![Some(Value::Null), Some(Value::Null)]);
    }

    /// `None` — an absent row — and `Some(Value::Null)` — a present null value
    /// — must not collapse into each other, interleaved with typed values, in
    /// a chunk none of the earlier per-variant tests exercise together.
    #[test]
    fn absent_rows_and_explicit_nulls_interleave_with_typed_values() {
        round_trip(vec![
            Some(Value::Int(1)),
            None,
            Some(Value::Null),
            Some(Value::Int(2)),
            None,
            None,
            Some(Value::Null),
            Some(Value::Int(3)),
        ]);
    }

    #[test]
    fn empty_chunk_round_trips() {
        round_trip(vec![]);
    }

    // -----------------------------------------------------------------------
    // Refused input
    // -----------------------------------------------------------------------

    #[test]
    fn a_chunk_mixing_two_variants_is_refused() {
        assert_eq!(
            encode(&[Some(Value::Int(1)), Some(Value::Str("x".into()))]),
            None
        );
    }

    /// Nulls of either kind must not rescue a chunk that is genuinely mixed —
    /// the type check has to look past them, not stop at the first one.
    #[test]
    fn a_mixed_chunk_is_refused_even_with_nulls_between_the_conflicting_values() {
        assert_eq!(
            encode(&[
                Some(Value::Bool(true)),
                None,
                Some(Value::Null),
                Some(Value::F64(1.0)),
            ]),
            None
        );
    }

    /// A chunk of nothing but absences and explicit nulls has no typed value
    /// to disagree about, so it is not "mixed" — it encodes as a chunk with no
    /// type.
    #[test]
    fn a_chunk_of_only_nulls_and_absences_encodes() {
        round_trip(vec![None, Some(Value::Null), None]);
    }

    /// A `Value::Spilled` placeholder is refused outright: its own doc comment
    /// says no consumer outside `pond_engine` should ever observe one, and a
    /// chunk encoder is not the resolution point.
    #[test]
    fn a_spilled_placeholder_is_refused() {
        let spilled = Value::Spilled {
            type_tag: 2,
            hash: "a".repeat(64),
        };
        assert_eq!(encode(&[Some(spilled)]), None);
    }

    /// `decode` refuses a declared row count over [`MAX_DECLARED_ROWS`], so
    /// `encode` must refuse the same input rather than handing back bytes
    /// this module's own decoder cannot read — otherwise a writer records a
    /// name for a permanently unreadable object.
    #[test]
    fn encode_refuses_more_rows_than_the_declared_row_cap() {
        let at_cap = vec![None; MAX_DECLARED_ROWS];
        assert!(encode(&at_cap).is_some());

        let one_over = vec![None; MAX_DECLARED_ROWS + 1];
        assert_eq!(encode(&one_over), None);
    }

    /// `put_len_prefix` is what stands between a `Str`, `Bytes`, `Vector`, or
    /// `Json` value's real length and the `u32` this format prefixes it
    /// with — this pins the guard directly rather than by actually
    /// allocating a multi-gigabyte value in a test.
    #[test]
    fn put_len_prefix_refuses_a_length_that_does_not_fit_in_u32() {
        let mut ok = Vec::new();
        assert_eq!(put_len_prefix(&mut ok, u32::MAX as usize), Some(()));
        assert_eq!(ok, u32::MAX.to_le_bytes());

        let mut too_big = Vec::new();
        assert_eq!(put_len_prefix(&mut too_big, u32::MAX as usize + 1), None);
    }

    // -----------------------------------------------------------------------
    // Canonical encoding
    // -----------------------------------------------------------------------

    /// Two writers holding the same values must produce the same bytes, or
    /// structural sharing between leaves stops working — the same property
    /// `core/index/src/node.rs` pins for index nodes.
    #[test]
    fn encoding_equal_inputs_twice_is_byte_identical() {
        let values = vec![
            Some(Value::Str("shared".into())),
            None,
            Some(Value::Null),
            Some(Value::Str("column".into())),
        ];
        let once = encode(&values).unwrap();
        for _ in 0..10 {
            assert_eq!(encode(&values).unwrap(), once);
        }
        // And a value built independently, not cloned, agrees too.
        let rebuilt = vec![
            Some(Value::Str("shared".into())),
            None,
            Some(Value::Null),
            Some(Value::Str("column".into())),
        ];
        assert_eq!(encode(&rebuilt).unwrap(), once);
    }

    /// Offsets into a canonically-encoded buffer, so the tests below can name
    /// the byte they are mutating instead of a magic number.
    struct Layout {
        type_tag: usize,
        presence: usize,
        null: usize,
    }

    fn layout(n_rows: usize) -> Layout {
        let bitmap_bytes = n_rows.div_ceil(8);
        Layout {
            type_tag: 4 + 1,
            presence: 4 + 1 + 1 + 4,
            null: 4 + 1 + 1 + 4 + bitmap_bytes,
        }
    }

    /// Trailing bytes past the last field are never part of any encoding
    /// `encode` produces, so `decode` must not silently ignore them — a name
    /// derived from a hash of the whole buffer would otherwise be shared by
    /// two different objects, one of which is garbage `encode` never wrote.
    #[test]
    fn trailing_bytes_after_a_canonical_encoding_are_refused() {
        let values = vec![Some(Value::Int(1)), None, Some(Value::Null)];
        let canonical = encode(&values).unwrap();
        assert!(
            decode(&canonical).is_some(),
            "the canonical bytes must decode on their own"
        );

        let mut four_extra = canonical.clone();
        four_extra.extend_from_slice(&[0xde, 0xad, 0xbe, 0xef]);
        assert_eq!(decode(&four_extra), None);

        let mut one_extra = canonical.clone();
        one_extra.push(0x00);
        assert_eq!(decode(&one_extra), None);
    }

    /// The bits above row `n` in the presence bitmap's last byte carry no
    /// meaning, but `encode` always leaves them zero — a decoder that
    /// ignored them would accept a set padding bit as another spelling of
    /// the same chunk.
    #[test]
    fn presence_bitmap_padding_bits_above_n_are_refused() {
        let values = vec![Some(Value::Int(1)), None, Some(Value::Null)]; // n = 3
        let canonical = encode(&values).unwrap();
        let l = layout(3);
        let mut evil = canonical.clone();
        evil[l.presence] |= 0xF8; // bits 3..8 are padding when n = 3
        assert_eq!(decode(&evil), None);
    }

    /// Same as above, for the null bitmap's padding bits.
    #[test]
    fn null_bitmap_padding_bits_above_n_are_refused() {
        let values = vec![Some(Value::Int(1)), None, Some(Value::Null)]; // n = 3
        let canonical = encode(&values).unwrap();
        let l = layout(3);
        let mut evil = canonical.clone();
        evil[l.null] |= 0xF8;
        assert_eq!(decode(&evil), None);
    }

    /// The null bit is never inspected when a row's presence bit is clear —
    /// but that makes it a spare bit a decoder could accept two ways unless
    /// it is constrained too. `encode` always leaves it clear on an absent
    /// row.
    #[test]
    fn a_null_bit_set_on_an_absent_row_is_refused() {
        let values = vec![None, Some(Value::Int(1))]; // row 0 absent
        let canonical = encode(&values).unwrap();
        let l = layout(2);
        let mut evil = canonical.clone();
        evil[l.null] |= 0b01; // row 0's null bit, though row 0 is absent
        assert_eq!(decode(&evil), None);
    }

    /// `encode` writes a bool payload as exactly the byte `1` or `0`.
    /// `0xFF` must not decode as `true` by the same accident `!= 0` would
    /// make it.
    #[test]
    fn a_non_canonical_bool_payload_byte_is_refused() {
        let values = vec![Some(Value::Bool(true))];
        let canonical = encode(&values).unwrap();
        let payload = canonical.len() - 1; // the single bool byte, at the end
        assert_eq!(canonical[payload], 1);
        let mut evil = canonical.clone();
        evil[payload] = 0xFF;
        assert_eq!(decode(&evil), None);
    }

    /// The worst of the non-canonical shapes: on a chunk with no row that is
    /// both present and non-null, no payload is ever read, so nothing
    /// distinguishes `TAG_NONE` from any other tag — all eight would decode
    /// identically, giving one chunk eight valid names. `encode` always
    /// picks `TAG_NONE`; `decode` must require it.
    #[test]
    fn an_all_null_chunks_type_tag_must_be_tag_none() {
        let values = vec![None, Some(Value::Null), None];
        let canonical = encode(&values).unwrap();
        let l = layout(3);
        assert_eq!(canonical[l.type_tag], TAG_NONE);
        for tag in TAG_BOOL..=TAG_JSON {
            let mut evil = canonical.clone();
            evil[l.type_tag] = tag;
            assert_eq!(
                decode(&evil),
                None,
                "tag {} on an all-null chunk must be refused",
                tag
            );
        }
    }

    /// The direct statement of what all of the above are instances of:
    /// `decode`'s accepted set is exactly `encode`'s output set. Re-encoding
    /// what a canonical buffer decodes to must reproduce that same buffer,
    /// for every shape [`golden_inputs`] exercises.
    #[test]
    fn decoding_then_re_encoding_a_canonical_buffer_reproduces_it() {
        for (name, values) in golden_inputs() {
            let canonical = encode(&values).expect("golden inputs must encode");
            let decoded = decode(&canonical).expect("must decode");
            assert_eq!(
                encode(&decoded).unwrap(),
                canonical,
                "{:?} did not round trip byte-for-byte",
                name
            );
        }
    }

    /// The same property, at a scale hand-written shapes cannot reach.
    ///
    /// # Why the test above is not enough
    ///
    /// `decoding_then_re_encoding_a_canonical_buffer_reproduces_it` checks ten
    /// inputs the author chose, so it can only find spellings the author
    /// thought of. The six non-canonical spellings this codec used to accept
    /// were found by a fuzz, not by inspection, and one of them — an all-null
    /// chunk whose type tag was unconstrained, giving one object eight valid
    /// names — is not a shape anybody writes down on purpose.
    ///
    /// So: generate buffers that are *structurally plausible* rather than
    /// uniformly random, because uniform noise is rejected at the magic and
    /// never reaches the interesting checks. A valid header with a random
    /// tag, row count, bitmaps and payload bytes lands constantly on padding
    /// bits above `n`, null bits on absent rows, and tag/content mismatches —
    /// precisely the cases that were wrong.
    ///
    /// The assertion is the one that matters in a content-addressed store:
    /// every buffer `decode` accepts must re-encode to itself. If it does not,
    /// two byte strings name one value, a blob cannot be checked against its
    /// own name, and a second implementation that spells padding differently
    /// silently stops sharing objects with this one.
    ///
    /// Deterministic, and small enough to belong in the unit suite. The full
    /// sweep that established the property ran 12.28M buffers with 360,473
    /// accepted and zero non-canonical; this is the regression guard, not the
    /// original proof.
    #[test]
    fn every_buffer_the_decoder_accepts_re_encodes_to_itself() {
        let mut rng: u64 = 0x5eed_1234_abcd_ef01;
        let mut next = move || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };

        let mut accepted = 0usize;
        for _ in 0..40_000 {
            let tag = (next() % 9) as u8; // 8 is out of range on purpose
            let n = (next() % 9) as usize;
            let bitmap = n.div_ceil(8);

            // Random bitmaps are the point: padding bits above `n`, null bits
            // on absent rows, and all-null chunks under a typed tag all occur
            // naturally here, and each was a real non-canonical spelling.
            let present: Vec<u8> = (0..bitmap).map(|_| next() as u8).collect();
            let nulls: Vec<u8> = (0..bitmap).map(|_| next() as u8).collect();

            // How many rows carry a payload, by the same rule `decode` uses.
            let bit = |m: &[u8], i: usize| m[i / 8] & (1 << (i % 8)) != 0;
            let payload_rows = (0..n)
                .filter(|&i| bit(&present, i) && !bit(&nulls, i))
                .count();

            let mut buf = Vec::from(MAGIC);
            buf.push(FORMAT_VERSION);
            buf.push(tag);
            buf.extend_from_slice(&(n as u32).to_le_bytes());
            buf.extend_from_slice(&present);
            buf.extend_from_slice(&nulls);

            // Correctly *sized* payloads, so a buffer is rejected for its
            // bitmaps rather than for being the wrong length. Emitting random
            // trailing bytes instead makes almost everything fail at the
            // length check and the fuzz stops reaching the interesting code —
            // an earlier version of this test accepted 46 buffers out of
            // 40,000 for exactly that reason.
            for _ in 0..payload_rows {
                match tag {
                    TAG_BOOL => buf.push((next() % 3) as u8), // 2 is non-canonical
                    TAG_INT | TAG_F64 => {
                        buf.extend_from_slice(&next().to_le_bytes());
                    }
                    TAG_STR | TAG_BYTES | TAG_JSON => {
                        let len = (next() % 4) as u32;
                        buf.extend_from_slice(&len.to_le_bytes());
                        for _ in 0..len {
                            // Keep it ASCII so `Str` and `Json` have a chance
                            // of being valid UTF-8 rather than always failing.
                            buf.push(b'a' + (next() % 26) as u8);
                        }
                    }
                    TAG_VECTOR => {
                        let len = (next() % 3) as u32;
                        buf.extend_from_slice(&len.to_le_bytes());
                        for _ in 0..len {
                            buf.extend_from_slice(&(next() as u32).to_le_bytes());
                        }
                    }
                    _ => {}
                }
            }

            if let Some(values) = decode(&buf) {
                accepted += 1;
                let re = encode(&values).expect("anything decode accepts must encode");
                assert_eq!(
                    re, buf,
                    "decode accepted a buffer that is not encode's own output: \
                     {:02x?} re-encodes to {:02x?}",
                    buf, re
                );
            }
        }

        // A fuzz that accepts nothing proves nothing. This guards against a
        // future change making every generated buffer invalid, which would
        // leave the assertion above running zero times and passing.
        assert!(
            accepted > 200,
            "only {accepted} of 40000 generated buffers were accepted — the \
             generator is no longer producing plausible chunks, so this test \
             is not exercising the property it claims to"
        );
    }

    // -----------------------------------------------------------------------
    // Frozen golden digests
    // -----------------------------------------------------------------------

    /// Named, discriminating inputs — one per variant, one mixing nulls and
    /// absences, one large enough that lengths span more than one byte.
    ///
    /// Chosen the way `pack.rs`'s `golden_inputs` are: enough shapes that a
    /// change to the bitmap layout, the payload framing, or the sentinel type
    /// tag shows up as a digest mismatch rather than passing by accident.
    fn golden_inputs() -> Vec<(&'static str, Vec<Option<Value>>)> {
        vec![
            ("empty", vec![]),
            ("bools", vec![Some(Value::Bool(true)), Some(Value::Bool(false)), None]),
            (
                "ints",
                vec![
                    Some(Value::Int(i64::MIN)),
                    None,
                    Some(Value::Int(0)),
                    Some(Value::Int(i64::MAX)),
                ],
            ),
            (
                "floats with nan and signed zero",
                vec![
                    Some(Value::F64(-0.0)),
                    Some(Value::F64(0.0)),
                    Some(Value::F64(f64::NAN)),
                    None,
                ],
            ),
            (
                "strings",
                vec![
                    Some(Value::Str(String::new())),
                    Some(Value::Str("hello".into())),
                    None,
                    Some(Value::Str("日本語".into())),
                ],
            ),
            (
                "bytes",
                vec![
                    Some(Value::Bytes(vec![])),
                    Some(Value::Bytes(vec![0, 0, 1, 0, 255])),
                    None,
                    // Long enough that the length prefix spans more than one
                    // byte, so a change to how that prefix is written — a
                    // reserved byte inserted before it, its width changed —
                    // moves the digest.
                    Some(Value::Bytes((0..300).map(|i| i as u8).collect())),
                ],
            ),
            (
                "vectors",
                vec![
                    Some(Value::Vector(vec![])),
                    Some(Value::Vector(vec![1.0, -2.5, 0.0, -0.0, f32::NAN, f32::INFINITY])),
                    None,
                    Some(Value::Vector((0..80).map(|i| i as f32).collect())),
                ],
            ),
            (
                "json",
                vec![
                    Some(Value::Json("{}".into())),
                    Some(Value::Json(r#"{"a":[1,2,3],"b":null}"#.into())),
                    None,
                    Some(Value::Json(format!(
                        r#"{{"padding":"{}"}}"#,
                        "x".repeat(300)
                    ))),
                ],
            ),
            (
                "nulls and absences with no typed value",
                vec![None, Some(Value::Null), None, Some(Value::Null)],
            ),
            (
                "interleaved typed values, nulls and absences",
                (0..64)
                    .map(|i| match i % 4 {
                        0 => Some(Value::Int(i)),
                        1 => None,
                        2 => Some(Value::Null),
                        _ => Some(Value::Int(-i)),
                    })
                    .collect(),
            ),
        ]
    }

    /// `PartialEq` on `Value::F64` uses `==`, under which `NaN != NaN` — so a
    /// plain `assert_eq!` on decoded golden inputs containing NaN fails on a
    /// perfectly correct round trip. Compare bit patterns for the float-typed
    /// variants and structural equality for everything else, the same
    /// distinction `f64_edges_round_trip_by_bits` makes explicitly.
    fn values_round_tripped(a: &[Option<Value>], b: &[Option<Value>]) -> bool {
        a.len() == b.len()
            && a.iter().zip(b).all(|(x, y)| match (x, y) {
                (Some(Value::F64(x)), Some(Value::F64(y))) => x.to_bits() == y.to_bits(),
                (Some(Value::Vector(x)), Some(Value::Vector(y))) => {
                    x.len() == y.len() && x.iter().zip(y).all(|(f, g)| f.to_bits() == g.to_bits())
                }
                _ => x == y,
            })
    }

    /// The codec's output is frozen, on inputs that discriminate — the same
    /// argument `core/index/src/pack.rs`'s `the_encoders_output_is_frozen`
    /// makes: a chunk is content-addressed, so a change to its bytes that
    /// leaves round-tripping intact still moves every chunk hash and silently
    /// stops structural sharing between leaves that used to share a column.
    ///
    /// The expected digests were produced by running this implementation and
    /// pasting its output — not computed by hand.
    ///
    /// If this fails, the encoder changed. That is allowed, and it is a
    /// format change — see `docs/COLUMNAR_LAYOUT.md`. It is not something to
    /// fix by updating the expected digests.
    #[test]
    fn the_encoders_output_is_frozen() {
        use sha2::{Digest, Sha256};

        let expected: &[(&str, &str)] = &[
            ("empty", "21605a6206ddb02d24839d29a04ae0f5f2113e58fd843c438a88082427ee5cf3"),
            ("bools", "85af85a5843aecd6000496e1c344ae5447f1ceb9de5fe3b0d7306905fc48dfbb"),
            ("ints", "4370883392ea8c2f9878d911c398e90559575f0f2a7eca0eeee7d26e39db65c9"),
            (
                "floats with nan and signed zero",
                "b30b593260824abfd84e2dcda9847359e8b787ee2bc0c96aba6205d3fd9467ba",
            ),
            (
                "strings",
                "39f3718328364a05dbfb4e0b5e69a2a29f16bd0ce0058d9d2cba9e2863b1ded9",
            ),
            ("bytes", "5529f186e6afd536b6b0be2f2519d315e0f8b6f6f923617409a894df63536e2b"),
            ("vectors", "9f4327acc243628d6cf196ed17919737881b6dda574c192cd00adc5bf5c5fb36"),
            ("json", "b748d255a5d3da33586235dcf61526577728e792b44f7edc3377bfcb223a9e6b"),
            (
                "nulls and absences with no typed value",
                "f8a21e1cfe3b252deba6b68949dff8b1fb14b3890c8d8a12ead2e3c1cccd6879",
            ),
            (
                "interleaved typed values, nulls and absences",
                "361b90303ad8344772357118c492c8e6802f04a8eacc6c034776c1d104a532f2",
            ),
        ];

        for ((name, values), (ename, digest)) in golden_inputs().iter().zip(expected) {
            assert_eq!(name, ename, "golden inputs and expectations drifted apart");
            let encoded = encode(values).expect("golden inputs must encode");
            let got = format!("{:x}", Sha256::digest(&encoded));
            assert_eq!(
                &got, digest,
                "chunk bytes for {:?} changed — see this test's comment before \
                 touching it",
                name
            );
            assert!(
                values_round_tripped(&decode(&encoded).expect("must decode"), values),
                "{:?} must still round trip",
                name
            );
        }
    }

    // -----------------------------------------------------------------------
    // Malformed input
    // -----------------------------------------------------------------------

    /// The fixed prefix (`MAGIC` + `FORMAT_VERSION` + `type_tag`) every test
    /// below starts from, so each one only has to spell out the part of the
    /// buffer it is actually testing.
    fn header(type_tag: u8) -> Vec<u8> {
        let mut out = Vec::from(*MAGIC);
        out.push(FORMAT_VERSION);
        out.push(type_tag);
        out
    }

    #[test]
    fn an_empty_buffer_is_refused() {
        assert_eq!(decode(&[]), None);
    }

    #[test]
    fn a_buffer_with_the_wrong_magic_is_refused() {
        let mut evil = b"XXXX".to_vec();
        evil.push(FORMAT_VERSION);
        evil.push(TAG_NONE);
        evil.extend_from_slice(&0u32.to_le_bytes());
        assert_eq!(decode(&evil), None);
    }

    #[test]
    fn a_buffer_with_an_unknown_version_is_refused() {
        let mut evil = Vec::from(*MAGIC);
        evil.push(FORMAT_VERSION + 1);
        evil.push(TAG_NONE);
        evil.extend_from_slice(&0u32.to_le_bytes());
        assert_eq!(decode(&evil), None);
    }

    #[test]
    fn an_unknown_type_tag_is_refused() {
        let mut evil = header(99);
        evil.extend_from_slice(&0u32.to_le_bytes());
        assert_eq!(decode(&evil), None);
    }

    /// A row count the buffer could not possibly hold is refused — `decode`
    /// never allocates proportional to `n` (every read takes a bounded slice
    /// out of the buffer that already exists, and `out` grows one row at a
    /// time), so this is not a protection against an allocation the code
    /// would otherwise perform. It is the cap check below, exercised on a
    /// buffer where the declared count is absurd rather than merely large.
    #[test]
    fn a_declared_row_count_far_larger_than_the_buffer_is_refused() {
        let mut evil = header(TAG_INT);
        evil.extend_from_slice(&u32::MAX.to_le_bytes());
        assert_eq!(decode(&evil), None);
    }

    /// The declared-row cap itself, at its edge, built so the cap is the
    /// *only* thing that can refuse: both buffers carry the full presence
    /// and null bitmap bytes their declared row count calls for, so nothing
    /// downstream of the cap check can fail first and mask it. Without that,
    /// a header with no bitmap bytes fails on the next read regardless of
    /// the cap, which is how the two tests this replaced passed with the
    /// cap deleted, and with `>` loosened to `>=`.
    ///
    /// Mutation-tested: with `if n > MAX_DECLARED_ROWS { return None; }`
    /// deleted, `at_limit` still passes but `over` now decodes to
    /// `Some(vec![None; MAX_DECLARED_ROWS + 1])` instead of `None`, so this
    /// test fails. Changing `>` to `>=` makes `at_limit` decode to `None`
    /// instead of the full row vec, so this test fails on that edge too.
    /// Restoring the check as written makes it pass again.
    #[test]
    fn the_declared_row_cap_is_exact() {
        fn full_buffer(n: usize) -> Vec<u8> {
            let mut buf = header(TAG_NONE);
            buf.extend_from_slice(&(n as u32).to_le_bytes());
            let bitmap_bytes = n.div_ceil(8);
            buf.extend(vec![0u8; bitmap_bytes * 2]);
            buf
        }

        // Exactly at the cap: permitted, and decodes to `n` absent rows —
        // there is nothing past the bitmaps for a TAG_NONE chunk to fail on.
        let at_limit = full_buffer(MAX_DECLARED_ROWS);
        assert_eq!(decode(&at_limit), Some(vec![None; MAX_DECLARED_ROWS]));

        // One past it: refused by the cap itself, with every bitmap byte
        // that row count would need already present.
        let over = full_buffer(MAX_DECLARED_ROWS + 1);
        assert_eq!(decode(&over), None);
    }

    /// A string length that reaches past the end of the buffer is refused —
    /// the same class of bug the module comment traces to the PND2 decoder.
    #[test]
    fn a_string_length_exceeding_the_buffer_is_refused() {
        let mut evil = header(TAG_STR);
        evil.extend_from_slice(&1u32.to_le_bytes()); // n = 1 row
        evil.push(0b1); // present
        evil.push(0b0); // not null
        evil.extend_from_slice(&u32::MAX.to_le_bytes()); // claimed string length
        evil.extend_from_slice(b"short"); // far too little actually follows
        assert_eq!(decode(&evil), None);
    }

    #[test]
    fn a_bytes_length_exceeding_the_buffer_is_refused() {
        let mut evil = header(TAG_BYTES);
        evil.extend_from_slice(&1u32.to_le_bytes());
        evil.push(0b1);
        evil.push(0b0);
        evil.extend_from_slice(&(u32::MAX / 2).to_le_bytes());
        evil.extend_from_slice(&[1, 2, 3]);
        assert_eq!(decode(&evil), None);
    }

    #[test]
    fn a_vector_length_exceeding_the_buffer_is_refused() {
        let mut evil = header(TAG_VECTOR);
        evil.extend_from_slice(&1u32.to_le_bytes());
        evil.push(0b1);
        evil.push(0b0);
        evil.extend_from_slice(&(u32::MAX / 4).to_le_bytes());
        evil.extend_from_slice(&[0, 0, 0, 0]);
        assert_eq!(decode(&evil), None);
    }

    /// Every truncation of a real, non-trivial chunk must decode to `None`,
    /// never partially decode into a value that was never actually there.
    #[test]
    fn a_truncated_chunk_is_refused_not_partially_read() {
        let values: Vec<Option<Value>> = (0..40)
            .map(|i| match i % 5 {
                0 => None,
                1 => Some(Value::Null),
                2 => Some(Value::Str(format!("row-{}", i))),
                3 => Some(Value::Str(String::new())),
                _ => Some(Value::Str("日本語".into())),
            })
            .collect();
        let encoded = encode(&values).unwrap();
        for cut in 1..encoded.len() {
            assert_eq!(
                decode(&encoded[..encoded.len() - cut]),
                None,
                "cut {} bytes from the end must be refused, not partially decoded",
                cut
            );
        }
    }

    /// Random bytes must never panic the decoder, whether or not they happen
    /// to decode. A deterministic xorshift, so failures reproduce — no
    /// external crate, matching `pond_index::pack`'s fuzz test.
    #[test]
    fn fuzzing_the_decoder_never_panics() {
        let mut state: u64 = 0xC0FF_EE00_1234_5678;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for _ in 0..20_000 {
            let len = (next() % 200) as usize;
            let bytes: Vec<u8> = (0..len).map(|_| next() as u8).collect();
            let _ = decode(&bytes);
        }
    }

    /// The declared row bound tracks the actual bound a leaf can produce:
    /// [`pond_index::chunk::MAX_ENTRIES_PER_CHUNK`] directly, not a second
    /// number that happens to agree with it. This pins the reference itself
    /// so a future refactor that quietly duplicates the constant is caught.
    #[test]
    fn the_row_cap_is_the_leafs_own_entry_bound() {
        assert_eq!(MAX_DECLARED_ROWS, pond_index::chunk::MAX_ENTRIES_PER_CHUNK);
    }
}
