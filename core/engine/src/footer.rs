// footer.rs — leaf footer codec.
//
// A leaf footer is the metadata a row-group leaf carries about the column
// chunks that hold its rows — see `docs/COLUMNAR_LAYOUT.md`: reading two
// columns of fifty should fetch two chunks, not every leaf, and the footer
// is how a reader finds which chunks those are without fetching them first.
// It is always read — every query touches it, whatever columns it wants —
// so it has to stay small, which is why it holds hashes and a row count and
// nothing else.
//
// Like `column.rs`, this module is only the codec. It does not know about
// leaves, the row-major path, or the read path; see `docs/COLUMNAR_LAYOUT.md`
// for the order the rest gets built in, and why the codec is pinned in
// isolation, with its own frozen bytes, before anything depends on it.
//
// # Column order is the object's identity
//
// A footer is content-addressed like everything else the store holds, so two
// writers who produce the same footer must produce the same bytes — the same
// reasoning `pond_record::Head`'s doc comment gives for keeping its own maps
// in a `BTreeMap`. `Footer::columns` is a `BTreeMap<String, String>` for
// exactly that reason: iterating it already yields names in ascending order,
// so `encode` does not need to sort anything, and `decode` can check the
// order it reads back against the one thing that could make it a second,
// non-canonical spelling of the same footer — see "Canonical decoding" below.
//
// # Hashes are bytes, not hex text
//
// `core/record/src/head.rs` made this change to `Head` for the same
// reason spelled out there: a hash is 32 bytes, and storing it as 64
// characters of hex is the same information at twice the size, paid on
// every leaf's footer since the footer is always read. Unlike `Head`,
// which falls back to writing a non-hash string as text so nothing the
// engine hands it is ever unrepresentable, a footer's columns map is never
// handed anything but the output of `column::encode`'s own hashing, so
// `encode` here refuses outright rather than growing a second, rarer
// encoding path for an input that should never occur — see `encode`'s doc
// comment.
//
// # Byte layout (little-endian lengths)
//
//   magic               "PFTR", 4 bytes
//   version             1 byte — see `FORMAT_VERSION`'s doc comment for why
//                       this is here from the start rather than retrofitted
//   row_count: u32      rows this leaf's row group holds, capped at
//                       `MAX_ROWS`
//   n: u32              column count, capped at `MAX_COLUMNS`
//   columns, n times, in ascending name order:
//     name_len: u16     column name length in bytes
//     name              name_len bytes, UTF-8
//     hash              32 raw bytes — see "Hashes are bytes, not hex text"
//
// A column name is a short identifier, the same kind of string
// `core/record/src/head.rs` prefixes with a `u16` for a collection name, so
// this format follows that precedent rather than `column.rs`'s `u32`, which
// is sized for arbitrary value payloads instead.
//
// # Canonical decoding
//
// These bytes are an object's *name*, exactly as `column.rs`'s module
// comment explains for chunks: the store addresses a footer by the hash of
// exactly this encoding, so a decoder that accepts more than one spelling of
// a footer breaks the promise that the name proves the content. `encode`
// produces exactly one spelling — sorted, deduplicated by construction
// because `BTreeMap` cannot hold two entries under one key — so `decode`
// rejects everything else: trailing bytes past the last column (`pos ==
// buf.len()`), a row count or column count over its cap, a name length
// reaching past the buffer, and — the one specific to this format — names
// that are not strictly ascending. That single check catches both an
// out-of-order name and a duplicate one, because a spelling with either
// could not have come from iterating a `BTreeMap`.

use std::collections::BTreeMap;

const MAGIC: &[u8; 4] = b"PFTR";

/// The format a footer is written in.
///
/// `column.rs` shipped once without a magic or a version, and an empty chunk
/// turned out to be byte-identical to an empty `pond_index` leaf node — same
/// hash, same name, same store, from two codecs that agree on nothing else.
/// `MAGIC` and `FORMAT_VERSION` are here from the first version of this file
/// so a footer can never collide with a chunk, a leaf node, or a future
/// shape of itself the same way.
const FORMAT_VERSION: u8 = 1;

/// Cap on a footer's declared row count.
///
/// A footer describes one leaf's row group, and a leaf holds at most
/// [`pond_index::chunk::MAX_ENTRIES_PER_CHUNK`] entries — using that constant
/// directly, the way `column.rs`'s `MAX_DECLARED_ROWS` does, means this bound
/// cannot quietly drift from the leaf's own bound the way a second number
/// that merely happens to agree with it could.
///
/// As with `column.rs`: raising the leaf bound is harmless, already-written
/// footers stay under the new, larger cap. Lowering it is a format break —
/// this cap is part of what `decode` accepts, so shrinking it makes an
/// already-written footer with a larger row count permanently undecodable in
/// a store that cannot rewrite what it holds.
const MAX_ROWS: usize = pond_index::chunk::MAX_ENTRIES_PER_CHUNK;

/// Cap on the number of columns a footer can declare.
///
/// There is no bound on a collection's column count that falls naturally out
/// of the row group the way `MAX_ROWS` does — a schema could in principle
/// have any number of fields. But an unbounded `n` read straight from the
/// buffer is exactly the hazard `column.rs`'s module comment traces to the
/// PND2 decoder: a declared count drives a loop and, if it is not checked
/// first, a hostile or truncated buffer can ask for a great many iterations
/// over a short one. Rather than invent a second arbitrary number, this
/// reuses [`MAX_ROWS`]'s value: no collection in this system comes
/// remotely close to `pond_index::chunk::MAX_ENTRIES_PER_CHUNK` columns, so
/// the cap costs nothing real while still bounding the loop before it runs.
const MAX_COLUMNS: usize = MAX_ROWS;

/// The metadata a leaf's row group carries about its columns: how many rows
/// it holds, and which column chunk holds each column's values.
///
/// `BTreeMap` rather than `HashMap` — see the module comment on why column
/// order is the object's identity, the same reason `pond_record::Head` keeps
/// its own maps this way.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Footer {
    pub row_count: u32,
    /// column name -> the hex hash of that column's chunk, as produced by
    /// hashing `column::encode`'s output. `encode` refuses anything that is
    /// not exactly that shape — see its doc comment.
    pub columns: BTreeMap<String, String>,
}

/// A 64-character lowercase hex string as the 32 bytes it stands for, or
/// `None` for anything else — including uppercase hex, which is a different
/// spelling of the same bytes and would make `encode` non-canonical if it
/// were accepted here and normalized silently.
fn decode_hex32(s: &str) -> Option<[u8; 32]> {
    if s.len() != 64 {
        return None;
    }
    let b = s.as_bytes();
    let mut out = [0u8; 32];
    for (i, chunk) in b.as_chunks::<2>().0.iter().enumerate() {
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

/// The inverse of [`decode_hex32`], always lowercase so re-encoding what was
/// decoded reproduces the same string `encode` would have accepted.
fn encode_hex32(raw: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(64);
    for b in raw {
        s.push(HEX[(b >> 4) as usize] as char);
        s.push(HEX[(b & 0x0f) as usize] as char);
    }
    s
}

/// Writes a `u16` name-length prefix, refusing a name this format has no
/// room for rather than truncating it — the same discipline `column.rs`'s
/// `put_len_prefix` uses for its `u32` payload lengths.
fn put_name(out: &mut Vec<u8>, name: &str) -> Option<()> {
    let len: u16 = name.len().try_into().ok()?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(name.as_bytes());
    Some(())
}

/// Encode a leaf footer canonically.
///
/// Returns `None` when the input cannot be represented: more rows than
/// [`MAX_ROWS`] or more columns than [`MAX_COLUMNS`] (this format's own
/// decoder would refuse the result, so encoding it would record a name for a
/// permanently unreadable object — the same reasoning `column::encode` gives
/// for its own row cap), a column name longer than a `u16` can prefix, or a
/// hash that is not exactly 64 characters of lowercase hex — the only shape
/// `column::encode`'s output ever hashes to, so anything else reaching here
/// is refused rather than given a second, rarer encoding.
pub fn encode(f: &Footer) -> Option<Vec<u8>> {
    if f.row_count as usize > MAX_ROWS {
        return None;
    }
    if f.columns.len() > MAX_COLUMNS {
        return None;
    }

    let mut out = Vec::new();
    out.extend_from_slice(MAGIC);
    out.push(FORMAT_VERSION);
    out.extend_from_slice(&f.row_count.to_le_bytes());
    // `f.columns.len() <= MAX_COLUMNS`, checked above, and `MAX_COLUMNS` is
    // `pond_index::chunk::MAX_ENTRIES_PER_CHUNK`, far under `u32::MAX`, so
    // this cannot truncate.
    let n: u32 = f.columns.len().try_into().ok()?;
    out.extend_from_slice(&n.to_le_bytes());

    // `BTreeMap` iterates in ascending key order already — see the module
    // comment on why column order is the object's identity.
    for (name, hash) in &f.columns {
        put_name(&mut out, name)?;
        let raw = decode_hex32(hash)?;
        out.extend_from_slice(&raw);
    }
    Some(out)
}

/// Decode a footer written by [`encode`].
///
/// Returns `None` for anything malformed — a truncated buffer, a row or
/// column count over its cap, a name length the buffer cannot support, names
/// out of order or duplicated — rather than guessing. Never allocates
/// proportional to a count taken straight from the buffer: `n` is checked
/// against [`MAX_COLUMNS`] before it drives anything, and the map is built
/// incrementally rather than pre-sized from it, the same discipline
/// `column.rs` and `pond_index::node` use for their own declared counts.
pub fn decode(buf: &[u8]) -> Option<Footer> {
    let mut r = Reader { buf, pos: 0 };
    if r.take(4)? != MAGIC {
        return None;
    }
    if r.u8()? != FORMAT_VERSION {
        return None;
    }
    let row_count = r.u32()?;
    if row_count as usize > MAX_ROWS {
        return None;
    }
    let n = r.u32()? as usize;
    if n > MAX_COLUMNS {
        return None;
    }
    // Unlike `pond_record::Head`'s `decode_head`, which checks a declared
    // count against the buffer's remaining size before looping — because its
    // own count is a bare `u32` with no cap of its own — `n` here is already
    // bounded by `MAX_COLUMNS` above, so the loop below can run at most that
    // many times regardless of what the buffer actually holds. A second,
    // buffer-size check would be redundant, and redundant checks with no
    // test that isolates them are exactly the risk the row-cap deletion in
    // `column.rs` demonstrated.

    let mut columns = BTreeMap::new();
    let mut previous: Option<String> = None;
    for _ in 0..n {
        let name_len = u16::from_le_bytes(r.take(2)?.try_into().ok()?) as usize;
        let name = String::from_utf8(r.take(name_len)?.to_vec()).ok()?;
        // Strictly ascending, not merely non-descending: this is what
        // catches a duplicate name and an out-of-order one with the same
        // check, since `encode` iterates a `BTreeMap` and neither shape can
        // come from that — see the module comment on canonical decoding.
        if let Some(prev) = &previous {
            if name <= *prev {
                return None;
            }
        }
        let raw: [u8; 32] = r.take(32)?.try_into().ok()?;
        let hash = encode_hex32(raw);
        previous = Some(name.clone());
        columns.insert(name, hash);
    }

    if r.pos != buf.len() {
        return None;
    }
    Some(Footer { row_count, columns })
}

/// Bounds-checked reader. Every accessor returns `None` rather than
/// panicking or over-allocating — the same reader shape `column.rs`,
/// `pond_record::encode`, and `pond_index::node` each keep their own copy
/// of, so a corrupted footer is an error, not a crash.
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
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hash_of(byte: u8) -> String {
        encode_hex32([byte; 32])
    }

    fn footer(row_count: u32, columns: &[(&str, u8)]) -> Footer {
        let mut f = Footer { row_count, columns: BTreeMap::new() };
        for (name, byte) in columns {
            f.columns.insert(name.to_string(), hash_of(*byte));
        }
        f
    }

    // -----------------------------------------------------------------------
    // Round trips
    // -----------------------------------------------------------------------

    fn round_trip(f: Footer) {
        let encoded = encode(&f).expect("must encode");
        let decoded = decode(&encoded).expect("must decode");
        assert_eq!(decoded, f);
    }

    #[test]
    fn a_footer_with_zero_columns_round_trips() {
        round_trip(footer(10, &[]));
    }

    #[test]
    fn a_footer_with_one_column_round_trips() {
        round_trip(footer(10, &[("a", 1)]));
    }

    #[test]
    fn a_footer_with_many_columns_round_trips() {
        let names: Vec<(String, u8)> =
            (0..50).map(|i| (format!("col_{i:03}"), i as u8)).collect();
        let mut f = Footer { row_count: 500, columns: BTreeMap::new() };
        for (name, byte) in &names {
            f.columns.insert(name.clone(), hash_of(*byte));
        }
        round_trip(f);
    }

    #[test]
    fn a_row_count_of_zero_round_trips() {
        round_trip(footer(0, &[("only", 7)]));
    }

    #[test]
    fn multibyte_utf8_column_names_round_trip() {
        round_trip(footer(1, &[("héllo", 1), ("日本語", 2), ("🎉emoji", 3)]));
    }

    /// The longest name this format allows: `put_name` prefixes a name with
    /// a `u16`, so `u16::MAX` bytes is the edge, not a round number chosen
    /// for convenience.
    #[test]
    fn the_longest_allowed_column_name_round_trips() {
        let long_name = "x".repeat(u16::MAX as usize);
        let mut f = Footer { row_count: 1, columns: BTreeMap::new() };
        f.columns.insert(long_name, hash_of(9));
        round_trip(f);
    }

    /// [`MAX_ROWS`] and [`MAX_COLUMNS`], exactly at their boundary — one past
    /// either must be refused, checked separately below.
    #[test]
    fn the_row_and_column_caps_are_permitted_exactly_at_their_boundary() {
        let f = footer(MAX_ROWS as u32, &[]);
        assert!(encode(&f).is_some());

        let names: Vec<(String, u8)> =
            (0..MAX_COLUMNS).map(|i| (format!("c{i}"), (i % 256) as u8)).collect();
        let mut f = Footer { row_count: 0, columns: BTreeMap::new() };
        for (name, byte) in &names {
            f.columns.insert(name.clone(), hash_of(*byte));
        }
        round_trip(f);
    }

    // -----------------------------------------------------------------------
    // Refused input, on the encode side
    // -----------------------------------------------------------------------

    /// `decode` refuses a declared row count over [`MAX_ROWS`], so `encode`
    /// must refuse the same input rather than handing back bytes its own
    /// decoder cannot read — otherwise a writer records a name for a
    /// permanently unreadable object, the defect `column.rs`'s equivalent
    /// test guards against.
    #[test]
    fn encode_refuses_a_row_count_over_the_cap() {
        assert!(encode(&footer(MAX_ROWS as u32, &[])).is_some());
        assert_eq!(encode(&footer(MAX_ROWS as u32 + 1, &[])), None);
    }

    /// Same, for the column count — the cap is checked on both sides, not
    /// just the side that happens to be exercised by a test first.
    #[test]
    fn encode_refuses_more_columns_than_the_cap() {
        let names: Vec<(String, u8)> =
            (0..=MAX_COLUMNS).map(|i| (format!("c{i}"), (i % 256) as u8)).collect();
        let mut f = Footer { row_count: 0, columns: BTreeMap::new() };
        for (name, byte) in &names {
            f.columns.insert(name.clone(), hash_of(*byte));
        }
        assert_eq!(f.columns.len(), MAX_COLUMNS + 1);
        assert_eq!(encode(&f), None);
    }

    /// A hash that is not exactly 64 characters of lowercase hex is refused
    /// outright rather than given a second, text-shaped encoding the way
    /// `pond_record::Head`'s `put_root` falls back to for a non-hash root —
    /// see the module comment on why a footer's columns map does not need
    /// that fallback: it is never handed anything but the output of
    /// `column::encode`'s own hashing.
    #[test]
    fn encode_refuses_a_hash_that_is_not_32_bytes_of_lowercase_hex() {
        let mut too_short = Footer { row_count: 1, columns: BTreeMap::new() };
        too_short.columns.insert("a".to_string(), "ab".repeat(31)); // 62 chars
        assert_eq!(encode(&too_short), None);

        let mut uppercase = Footer { row_count: 1, columns: BTreeMap::new() };
        uppercase.columns.insert("a".to_string(), "AB".repeat(32));
        assert_eq!(encode(&uppercase), None);

        let mut not_hex = Footer { row_count: 1, columns: BTreeMap::new() };
        not_hex.columns.insert("a".to_string(), "not-a-hash".repeat(7)); // 70 chars, wrong shape either way
        assert_eq!(encode(&not_hex), None);
    }

    /// `put_name` is what stands between a column name's real length and the
    /// `u16` this format prefixes it with — this pins the guard directly
    /// rather than by actually allocating a 64 KiB name in a test, the same
    /// approach `column.rs`'s `put_len_prefix_refuses_a_length_that_does_not_fit_in_u32`
    /// takes for its own length prefix.
    #[test]
    fn put_name_refuses_a_length_that_does_not_fit_in_u16() {
        let mut ok = Vec::new();
        let max_name = "x".repeat(u16::MAX as usize);
        assert_eq!(put_name(&mut ok, &max_name), Some(()));

        let mut too_long = Vec::new();
        let over = "x".repeat(u16::MAX as usize + 1);
        assert_eq!(put_name(&mut too_long, &over), None);
    }

    // -----------------------------------------------------------------------
    // Canonical encoding
    // -----------------------------------------------------------------------

    /// Two writers holding the same footer, built in a different insertion
    /// order, must produce the same bytes — `BTreeMap` iterates by key
    /// regardless of insertion order, so this is the direct statement of why
    /// it, not `HashMap`, is the field's type.
    #[test]
    fn building_the_same_footer_two_ways_gives_identical_bytes() {
        let mut a = Footer { row_count: 9, columns: BTreeMap::new() };
        a.columns.insert("z".to_string(), hash_of(1));
        a.columns.insert("a".to_string(), hash_of(2));
        a.columns.insert("m".to_string(), hash_of(3));

        let mut b = Footer { row_count: 9, columns: BTreeMap::new() };
        b.columns.insert("m".to_string(), hash_of(3));
        b.columns.insert("a".to_string(), hash_of(2));
        b.columns.insert("z".to_string(), hash_of(1));

        assert_eq!(encode(&a).unwrap(), encode(&b).unwrap());
    }

    /// The direct statement of canonical decoding: re-encoding a decoded
    /// footer reproduces the exact bytes it was decoded from, for every
    /// shape [`golden_inputs`] exercises.
    #[test]
    fn decoding_then_re_encoding_a_canonical_buffer_reproduces_it() {
        for (name, f) in golden_inputs() {
            let canonical = encode(&f).expect("golden inputs must encode");
            let decoded = decode(&canonical).expect("must decode");
            assert_eq!(
                encode(&decoded).unwrap(),
                canonical,
                "{:?} did not round trip byte-for-byte",
                name
            );
        }
    }

    // -----------------------------------------------------------------------
    // Frozen golden digests
    // -----------------------------------------------------------------------

    /// Named, discriminating inputs — the same shape of test `column.rs`'s
    /// `golden_inputs` runs, chosen so a change to the header, the name
    /// framing, or the hash width shows up as a digest mismatch.
    fn golden_inputs() -> Vec<(&'static str, Footer)> {
        vec![
            ("empty", footer(0, &[])),
            ("single column", footer(3, &[("a", 1)])),
            (
                "several columns",
                footer(
                    128,
                    &[("age", 1), ("email", 2), ("id", 3), ("name", 4), ("zip", 5)],
                ),
            ),
            (
                "multibyte names",
                footer(7, &[("héllo", 9), ("日本語", 10), ("🎉", 11)]),
            ),
            (
                "large row count",
                footer(MAX_ROWS as u32, &[("only", 42)]),
            ),
        ]
    }

    /// The codec's output is frozen, on inputs that discriminate — the same
    /// argument `column.rs`'s `the_encoders_output_is_frozen` makes: a
    /// footer is content-addressed, so a change to its bytes that leaves
    /// round-tripping intact still moves every footer's hash and silently
    /// stops structural sharing between leaves that used to share one.
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
            ("empty", "fce659be07f0e4fd9bc0ce56902ec7066b143de60dbc2ef90b97e10fdffb04e3"),
            (
                "single column",
                "c5b03a72d3f18651bfe47f14ba887e8e60401d2d2ebfaabe38e1e5827c2952b7",
            ),
            (
                "several columns",
                "f494ab204d37c809628aad2d8cad92d294d6297f690a9ef9d3d7d18a7f934bf5",
            ),
            (
                "multibyte names",
                "9fe214aaaafb125cf18e0ead125e283da392dc78043ce07caf24b519c6a3d685",
            ),
            (
                "large row count",
                "656570680395ebb098fbe03f94f35a22e5218a489017a3e1ecb7ce6ebdc7dc2a",
            ),
        ];

        for ((name, f), (ename, digest)) in golden_inputs().iter().zip(expected) {
            assert_eq!(name, ename, "golden inputs and expectations drifted apart");
            let encoded = encode(f).expect("golden inputs must encode");
            let got = format!("{:x}", Sha256::digest(&encoded));
            assert_eq!(
                &got, digest,
                "footer bytes for {:?} changed — see this test's comment before \
                 touching it",
                name
            );
            assert_eq!(&decode(&encoded).expect("must decode"), f, "{:?} must still round trip", name);
        }
    }

    // -----------------------------------------------------------------------
    // Malformed input
    // -----------------------------------------------------------------------

    /// The fixed prefix every malformed-input test below starts from, so
    /// each one only has to spell out the part of the buffer it is actually
    /// testing.
    fn header(row_count: u32, n_columns: u32) -> Vec<u8> {
        let mut out = Vec::from(*MAGIC);
        out.push(FORMAT_VERSION);
        out.extend_from_slice(&row_count.to_le_bytes());
        out.extend_from_slice(&n_columns.to_le_bytes());
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
        evil.extend_from_slice(&0u32.to_le_bytes());
        evil.extend_from_slice(&0u32.to_le_bytes());
        assert_eq!(decode(&evil), None);
    }

    #[test]
    fn a_buffer_with_an_unknown_version_is_refused() {
        let mut evil = Vec::from(*MAGIC);
        evil.push(FORMAT_VERSION + 1);
        evil.extend_from_slice(&0u32.to_le_bytes());
        evil.extend_from_slice(&0u32.to_le_bytes());
        assert_eq!(decode(&evil), None);
    }

    /// A column count over [`MAX_COLUMNS`] with nothing behind it is refused
    /// — but not necessarily by the cap check itself: `evil` has no column
    /// bytes at all, so the loop's first `take(2)` already fails once `n`
    /// columns are attempted against an empty remainder, cap or no cap.
    /// Mutation-tested: deleting `if n > MAX_COLUMNS` does *not* fail this
    /// test, which is why it exists alongside, not instead of,
    /// [`the_row_and_column_caps_refuse_one_past_the_boundary`] below — that
    /// one supplies real bytes for every declared column and is what
    /// actually isolates the cap check.
    #[test]
    fn an_absurd_column_count_is_refused() {
        let evil = header(0, u32::MAX);
        assert_eq!(decode(&evil), None);
    }

    /// Same, for a declared row count the cap alone must catch — a `u32`
    /// row count needs no bitmap or payload to make it implausible the way
    /// `column.rs`'s row cap does, so this is the whole test.
    #[test]
    fn an_absurd_row_count_is_refused() {
        let evil = header(u32::MAX, 0);
        assert_eq!(decode(&evil), None);
    }

    /// [`MAX_ROWS`] and [`MAX_COLUMNS`], at the exact edge one past the cap —
    /// built so the cap is the *only* thing that can refuse: `column_over`
    /// carries real, strictly ascending names and real hash bytes for every
    /// declared column, so nothing downstream of the cap check — not a
    /// truncation, not the ordering check — can fail first and mask it. An
    /// earlier version of this test used a zero-length name for every
    /// column, which made every column but the first fail the ordering
    /// check instead of the cap: that version stayed green with the cap
    /// check deleted, which is exactly the false confidence
    /// `column.rs`'s history warns about.
    ///
    /// Mutation-tested: with the `row_count as usize > MAX_ROWS` check
    /// deleted, `row_over` decodes instead of being refused, so this test
    /// fails; restoring the check makes it pass again. Same for the `n >
    /// MAX_COLUMNS` check against `column_over`.
    #[test]
    fn the_row_and_column_caps_refuse_one_past_the_boundary() {
        let row_over = header(MAX_ROWS as u32 + 1, 0);
        assert_eq!(decode(&row_over), None);

        let mut column_over = header(0, MAX_COLUMNS as u32 + 1);
        for i in 0..=MAX_COLUMNS {
            // Fixed-width, zero-padded so ascending index order is also
            // ascending string order — a real, distinct, correctly ordered
            // name per column, not a shortcut that a different check would
            // catch first.
            let name = format!("{i:05}");
            column_over.extend_from_slice(&(name.len() as u16).to_le_bytes());
            column_over.extend_from_slice(name.as_bytes());
            column_over.extend_from_slice(&[i as u8; 32]);
        }
        assert_eq!(decode(&column_over), None);
    }

    /// A name length that reaches past the end of the buffer is refused —
    /// the same class of bug `column.rs`'s module comment traces to the
    /// PND2 decoder.
    #[test]
    fn a_name_length_exceeding_the_buffer_is_refused() {
        let mut evil = header(0, 1);
        evil.extend_from_slice(&u16::MAX.to_le_bytes()); // claimed name length
        evil.extend_from_slice(b"short"); // far too little actually follows
        assert_eq!(decode(&evil), None);
    }

    /// Names must be strictly ascending; two names in descending order are
    /// not a spelling `encode` — which iterates a `BTreeMap` — could ever
    /// produce.
    #[test]
    fn names_out_of_order_are_refused() {
        let mut evil = header(0, 2);
        evil.extend_from_slice(&1u16.to_le_bytes());
        evil.extend_from_slice(b"z");
        evil.extend_from_slice(&[1u8; 32]);
        evil.extend_from_slice(&1u16.to_le_bytes());
        evil.extend_from_slice(b"a");
        evil.extend_from_slice(&[2u8; 32]);
        assert_eq!(decode(&evil), None);
    }

    /// A duplicate name is the other shape the strict-ascending check
    /// catches: two entries under the same key, which a `BTreeMap` cannot
    /// hold in the first place.
    #[test]
    fn duplicate_names_are_refused() {
        let mut evil = header(0, 2);
        evil.extend_from_slice(&1u16.to_le_bytes());
        evil.extend_from_slice(b"a");
        evil.extend_from_slice(&[1u8; 32]);
        evil.extend_from_slice(&1u16.to_le_bytes());
        evil.extend_from_slice(b"a");
        evil.extend_from_slice(&[2u8; 32]);
        assert_eq!(decode(&evil), None);
    }

    /// A hash field cut short is a truncation like any other — `take(32)`
    /// fails when fewer than 32 bytes remain, refusing the buffer rather
    /// than reading a short, zero-padded hash.
    #[test]
    fn a_short_hash_is_refused() {
        let mut evil = header(0, 1);
        evil.extend_from_slice(&1u16.to_le_bytes());
        evil.extend_from_slice(b"a");
        evil.extend_from_slice(&[1u8; 16]); // 16 bytes of a 32-byte hash
        assert_eq!(decode(&evil), None);
    }

    /// Trailing bytes past the last column are never part of any encoding
    /// `encode` produces — the same argument `column.rs`'s equivalent test
    /// makes, applied here.
    #[test]
    fn trailing_bytes_after_a_canonical_encoding_are_refused() {
        let f = footer(3, &[("a", 1), ("b", 2)]);
        let canonical = encode(&f).unwrap();
        assert!(decode(&canonical).is_some(), "the canonical bytes must decode on their own");

        let mut extra = canonical.clone();
        extra.extend_from_slice(&[0xde, 0xad, 0xbe, 0xef]);
        assert_eq!(decode(&extra), None);

        let mut one_extra = canonical.clone();
        one_extra.push(0x00);
        assert_eq!(decode(&one_extra), None);
    }

    /// Every truncation of a real, non-trivial footer must decode to `None`,
    /// never partially decode into a footer that was never actually there.
    #[test]
    fn a_truncated_footer_is_refused_not_partially_read() {
        let f = footer(
            40,
            &[("alpha", 1), ("beta", 2), ("gamma", 3), ("日本語", 4)],
        );
        let encoded = encode(&f).unwrap();
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
    /// external crate, matching `column.rs`'s and `pond_index::pack`'s fuzz
    /// tests.
    #[test]
    fn fuzzing_the_decoder_never_panics() {
        let mut state: u64 = 0xF007_BA11_C0DE_1234;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for _ in 0..20_000 {
            let len = (next() % 300) as usize;
            let bytes: Vec<u8> = (0..len).map(|_| next() as u8).collect();
            let _ = decode(&bytes);
        }
    }

    /// The declared bounds track the leaf's own bound:
    /// [`pond_index::chunk::MAX_ENTRIES_PER_CHUNK`] directly, not a second
    /// number that happens to agree with it. This pins the reference itself
    /// so a future refactor that quietly duplicates the constant is caught.
    #[test]
    fn the_row_and_column_caps_are_the_leafs_own_entry_bound() {
        assert_eq!(MAX_ROWS, pond_index::chunk::MAX_ENTRIES_PER_CHUNK);
        assert_eq!(MAX_COLUMNS, pond_index::chunk::MAX_ENTRIES_PER_CHUNK);
    }

    /// A structurally plausible fuzz — valid magic and version, random row
    /// and column counts, names and hashes — rather than uniformly random
    /// bytes, which `column.rs`'s equivalent test explains gets rejected at
    /// the magic and never reaches the interesting checks (out-of-order
    /// names, duplicate names, the caps).
    ///
    /// The assertion that matters in a content-addressed store: every buffer
    /// `decode` accepts must re-encode to itself. If it does not, two byte
    /// strings name one footer, and a second implementation that spells the
    /// same footer differently would silently stop sharing objects with this
    /// one.
    #[test]
    fn every_buffer_the_decoder_accepts_re_encodes_to_itself() {
        let mut rng: u64 = 0xF00D_5EED_1234_ABCD;
        let mut next = move || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };

        let mut accepted = 0usize;
        for _ in 0..40_000 {
            // Row count: mostly small, occasionally at or over the cap.
            let row_count = match next() % 4 {
                0 => MAX_ROWS as u32,
                1 => MAX_ROWS as u32 + 1,
                _ => (next() % 20) as u32,
            };
            // Column count: small, so names are likely to collide or land
            // out of order — that is the point.
            let n = (next() % 5) as u32;

            let mut buf = Vec::from(MAGIC);
            buf.push(FORMAT_VERSION);
            buf.extend_from_slice(&row_count.to_le_bytes());
            buf.extend_from_slice(&n.to_le_bytes());
            for _ in 0..n {
                // Short, low-alphabet names so repeats — the interesting
                // case for the ordering check — happen often.
                let name_len = 1 + (next() % 3) as usize;
                buf.extend_from_slice(&(name_len as u16).to_le_bytes());
                for _ in 0..name_len {
                    buf.push(b'a' + (next() % 4) as u8);
                }
                for _ in 0..32 {
                    buf.push(next() as u8);
                }
            }

            if let Some(f) = decode(&buf) {
                accepted += 1;
                let re = encode(&f).expect("anything decode accepts must encode");
                assert_eq!(
                    re, buf,
                    "decode accepted a buffer that is not encode's own output: \
                     {:02x?} re-encodes to {:02x?}",
                    buf, re
                );
            }
        }

        // A fuzz that accepts nothing proves nothing — the same guard
        // `column.rs`'s equivalent test carries, against a future change
        // making every generated buffer invalid.
        assert!(
            accepted > 200,
            "only {accepted} of 40000 generated buffers were accepted — the \
             generator is no longer producing plausible footers, so this \
             test is not exercising the property it claims to"
        );
    }
}
