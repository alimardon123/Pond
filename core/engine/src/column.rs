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
//   [0]                type_tag: u8 — the one variant every typed value in
//                       this chunk shares, or TAG_NONE if there are none
//   [1..5]              n: u32 — row count
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

fn put_payload(out: &mut Vec<u8>, v: &Value) {
    match v {
        Value::Bool(b) => out.push(*b as u8),
        Value::Int(i) => out.extend_from_slice(&i.to_le_bytes()),
        Value::F64(f) => out.extend_from_slice(&f.to_bits().to_le_bytes()),
        Value::Str(s) => {
            out.extend_from_slice(&(s.len() as u32).to_le_bytes());
            out.extend_from_slice(s.as_bytes());
        }
        Value::Bytes(b) => {
            out.extend_from_slice(&(b.len() as u32).to_le_bytes());
            out.extend_from_slice(b);
        }
        Value::Vector(fs) => {
            out.extend_from_slice(&(fs.len() as u32).to_le_bytes());
            for f in fs {
                out.extend_from_slice(&f.to_bits().to_le_bytes());
            }
        }
        Value::Json(s) => {
            out.extend_from_slice(&(s.len() as u32).to_le_bytes());
            out.extend_from_slice(s.as_bytes());
        }
        Value::Null | Value::Spilled { .. } => {
            unreachable!("encode() filters both out before calling put_payload")
        }
    }
}

/// Encode a column chunk. `None` at index `i` means row `i` has no value for
/// this column; `Some(Value::Null)` means it has one and the value is null —
/// see the module comment for why those are kept apart.
///
/// Returns `None` when the input cannot be represented as one chunk: a
/// `Value::Spilled` anywhere (a chunk stores resolved values, never
/// placeholders), or two typed values that are not the same variant.
pub fn encode(values: &[Option<Value>]) -> Option<Vec<u8>> {
    let n = values.len();

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
    out.push(type_tag);
    out.extend_from_slice(&(n as u32).to_le_bytes());
    out.extend_from_slice(&present);
    out.extend_from_slice(&is_null);
    for v in values {
        match v {
            Some(Value::Null) | None => {}
            Some(val) => put_payload(&mut out, val),
        }
    }
    Some(out)
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
pub fn decode(buf: &[u8]) -> Option<Vec<Option<Value>>> {
    let mut r = Reader { buf, pos: 0 };
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

    let mut out = Vec::new();
    for i in 0..n {
        if !get_bit(present, i) {
            out.push(None);
            continue;
        }
        if get_bit(is_null, i) {
            out.push(Some(Value::Null));
            continue;
        }
        // A row claiming a typed value in a chunk whose type is TAG_NONE has
        // no defined payload format to read — this can only be a corrupt or
        // hand-crafted buffer, since `encode` never produces it.
        if type_tag == TAG_NONE {
            return None;
        }
        out.push(Some(r.payload(type_tag)?));
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
            TAG_BOOL => Value::Bool(self.u8()? != 0),
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
            ("empty", "8855508aade16ec573d21e6a485dfd0a7624085c1a14b5ecdd6485de0c6839a4"),
            ("bools", "317a78e887c1d036d69251ca0677dce855223c3a0e2bb13ca1ecfb2971506aac"),
            ("ints", "8a747de307769e716ea326eca0f5cdeed4f81082da4b4443b40ca07f97bb534f"),
            (
                "floats with nan and signed zero",
                "ebbcf2c712ee1ebfa0a9ccf7405ab18ed13a74f1a82a349f0b7d92247c12937f",
            ),
            (
                "strings",
                "e1c2ac964c58d6e92dc9118bdd2fd4f11db8878b3179366e53a8d22236e4e2fd",
            ),
            (
                "nulls and absences with no typed value",
                "0af399eca54faf2502a667eddcacabcb8a2e4ee5f7be7c84ac90f70a3a513575",
            ),
            (
                "interleaved typed values, nulls and absences",
                "3ccd0144b19f1bad86b60334922734a03845f5c02fb4baf4866bc24eb70e4ad9",
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

    #[test]
    fn an_empty_buffer_is_refused() {
        assert_eq!(decode(&[]), None);
    }

    #[test]
    fn an_unknown_type_tag_is_refused() {
        assert_eq!(decode(&[99, 0, 0, 0, 0]), None);
    }

    /// A row count the buffer could not possibly hold is refused before it
    /// drives any allocation — the same discipline `pond_index::node` uses
    /// for a declared entry count of 4 billion in a 5-byte buffer.
    #[test]
    fn a_declared_row_count_far_larger_than_the_buffer_is_refused() {
        let mut evil = vec![TAG_INT];
        evil.extend_from_slice(&u32::MAX.to_le_bytes());
        assert_eq!(decode(&evil), None);
    }

    /// The declared-row cap itself, at its edge — mutation testing on
    /// `pond_index::node` found that "at the limit" and "one past it" are the
    /// only inputs that distinguish `>` from `>=`.
    #[test]
    fn the_declared_row_cap_is_exact() {
        let mut at_limit = vec![TAG_NONE];
        at_limit.extend_from_slice(&(MAX_DECLARED_ROWS as u32).to_le_bytes());
        // Exactly at the cap is permitted by the count check and then fails
        // for lack of bitmap bytes, not on the bound itself.
        assert_eq!(decode(&at_limit), None);

        let mut over = vec![TAG_NONE];
        over.extend_from_slice(&((MAX_DECLARED_ROWS + 1) as u32).to_le_bytes());
        assert_eq!(decode(&over), None);
    }

    /// A string length that reaches past the end of the buffer is refused —
    /// the same class of bug the module comment traces to the PND2 decoder.
    #[test]
    fn a_string_length_exceeding_the_buffer_is_refused() {
        let mut evil = vec![TAG_STR];
        evil.extend_from_slice(&1u32.to_le_bytes()); // n = 1 row
        evil.push(0b1); // present
        evil.push(0b0); // not null
        evil.extend_from_slice(&u32::MAX.to_le_bytes()); // claimed string length
        evil.extend_from_slice(b"short"); // far too little actually follows
        assert_eq!(decode(&evil), None);
    }

    #[test]
    fn a_bytes_length_exceeding_the_buffer_is_refused() {
        let mut evil = vec![TAG_BYTES];
        evil.extend_from_slice(&1u32.to_le_bytes());
        evil.push(0b1);
        evil.push(0b0);
        evil.extend_from_slice(&(u32::MAX / 2).to_le_bytes());
        evil.extend_from_slice(&[1, 2, 3]);
        assert_eq!(decode(&evil), None);
    }

    #[test]
    fn a_vector_length_exceeding_the_buffer_is_refused() {
        let mut evil = vec![TAG_VECTOR];
        evil.extend_from_slice(&1u32.to_le_bytes());
        evil.push(0b1);
        evil.push(0b0);
        evil.extend_from_slice(&(u32::MAX / 4).to_le_bytes());
        evil.extend_from_slice(&[0, 0, 0, 0]);
        assert_eq!(decode(&evil), None);
    }

    /// Every truncation of a real, non-trivial chunk must decode to `None`,
    /// never panic.
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
            let _ = decode(&encoded[..encoded.len() - cut]);
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
