// pack.rs — a small LZ77, specified here rather than depended on.
//
// # Why a codec of our own rather than a library
//
// An index node compresses well. A real leaf — 2000 rows, three small columns,
// 362,399 bytes — goes to 51,492 with gzip. The redundancy is structural: the
// same field names in every record, versions a millisecond apart, values from a
// small domain. That is the same redundancy a column store exploits, and it is
// worth having without laying values out by column.
//
// The obvious way to get it is a compression crate, and it is wrong. A node is
// content-addressed, and the whole design rests on two writers holding the same
// data producing byte-identical nodes — that is what gives structural sharing,
// dedup, and an O(diff) merge. Compressed output is a property of *the
// compressor*, not of the data: a patch release, a different feature flag, or a
// different build of the same crate can emit different bytes for identical
// input. Two writers would then write different hashes for the same node.
// Nothing would be corrupt and nothing would fail loudly — sharing and dedup
// would quietly stop working.
//
// `Cargo.lock` does not fix it, because the writers are separate processes on
// separate machines built months apart. It is the same class of constraint as
// the chunk target and the chunk salt, which is exactly why those are pinned
// per collection rather than taken from the current build — and a library's
// exact output cannot be pinned that way, because it is not a parameter.
//
// A codec specified in this file has no such hazard. Its output is a function
// of its input and of code that ships with the reader. Changing it is then a
// deliberate format change, to be treated like a change to the chunk target.
//
// # The format
//
// A stream of tokens over the uncompressed bytes:
//
//   control < 0x80   literal run of `control + 1` bytes, which follow
//   control >= 0x80  match: length nibble in the low 7 bits, then a
//                    little-endian u16 distance
//
// A match length is `(control & 0x7f) + MIN_MATCH`. When that nibble is at its
// maximum the length continues in following bytes, each adding 255 until a byte
// below 255 ends it — so a long run of identical structure costs a few bytes
// however long it is.
//
// Greedy matching against a hash of the next [`MIN_MATCH`] bytes. Greedy rather
// than optimal on purpose: the search has to be *specified*, not merely good,
// and "take the longest match at the most recent candidate position" is a rule
// that can be written down in a sentence.
//
// # Mutants that survive here, and why they are meant to
//
// `cargo mutants -p pond_index --file core/index/src/pack.rs` leaves three
// surviving mutants in this module. All three are equivalent — they change the
// source without changing any output — and the note is here so nobody has to
// work that out twice:
//
//   - `pos > candidate` -> `>=`. A candidate is always a strictly earlier
//     position, since the table is read before it is written, so the two
//     comparisons never differ.
//   - `0x80 | short` -> `^` in `emit_match`. `short` is capped at
//     `MAX_SHORT_LEN` = `0x7f`, so the high bit is always clear and the two
//     operations are the same.
//   - the registration range `(pos + 1)..(pos + len)` -> `(pos - 1)..`. Both
//     extra positions were already registered with the same values earlier in
//     the same loop, so re-registering them is a no-op. Checked against 90
//     varied inputs; none produced different output.
//
// Two others were not equivalent and are gone: a redundant short-input guard
// and a redundant mask in `hash4`, both removed rather than tested, because a
// branch no input can distinguish is a branch that should not exist.

/// Bytes below which a match is not worth its three-byte token.
const MIN_MATCH: usize = 4;
/// The most bytes one match can name before its length spills into extra
/// bytes. `0x7f` is the largest value the control byte's low bits hold.
const MAX_SHORT_LEN: usize = 0x7f;
/// How far back a match may reach. A u16 distance.
const WINDOW: usize = u16::MAX as usize;
/// Literal runs are length-prefixed by one byte, so this is the longest a
/// single run token can carry.
const MAX_LITERAL_RUN: usize = 0x80;

/// Size of the match-candidate table. A power of two so the index is a mask.
const HASH_BITS: usize = 15;
const HASH_SIZE: usize = 1 << HASH_BITS;

/// Hash of the four bytes at `i`. Fixed here, because a different hash finds
/// different matches and therefore produces different output.
///
/// The shift alone bounds the result to `HASH_BITS` bits, so no mask follows
/// it. There used to be one; mutation testing showed that replacing its `&`
/// with `^` changed nothing observable, which is what a redundant operation
/// looks like from the outside — the mask could not narrow a value that was
/// already narrow, and inverting bits of a table index is a bijection that
/// leaves every collision exactly where it was.
fn hash4(buf: &[u8], i: usize) -> usize {
    let v = u32::from_le_bytes([buf[i], buf[i + 1], buf[i + 2], buf[i + 3]]);
    (v.wrapping_mul(2_654_435_761) >> (32 - HASH_BITS as u32)) as usize
}

/// Compress `input`.
///
/// Deterministic: the same bytes always produce the same output, on any
/// machine and any build, because every choice the encoder makes is fixed
/// above.
pub fn pack(input: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(input.len() / 2 + 16);

    // No short-input special case: the loop below cannot run when there are
    // fewer than MIN_MATCH bytes, and the trailing `emit_literals` then emits
    // the whole input — which is exactly what a special case would have done.
    // There used to be one. Mutation testing showed that neither weakening its
    // comparison nor narrowing it to a single length changed any output, which
    // is what a branch that cannot matter looks like from the outside.

    // Most recent position at which each 4-byte hash was seen. `usize::MAX`
    // means "never".
    let mut table = vec![usize::MAX; HASH_SIZE];
    let mut pos = 0usize;
    let mut literal_start = 0usize;

    while pos + MIN_MATCH <= input.len() {
        let h = hash4(input, pos);
        let candidate = table[h];
        table[h] = pos;

        let usable = candidate != usize::MAX
            && pos > candidate
            && pos - candidate <= WINDOW
            && input[candidate..candidate + MIN_MATCH] == input[pos..pos + MIN_MATCH];

        if !usable {
            pos += 1;
            continue;
        }

        // Extend the match as far as it goes, but not past the end.
        let mut len = MIN_MATCH;
        while pos + len < input.len() && input[candidate + len] == input[pos + len] {
            len += 1;
        }

        emit_literals(&mut out, &input[literal_start..pos]);
        emit_match(&mut out, len, pos - candidate);

        // Register the positions the match covered, so later matches can reach
        // into it. Skipping this costs ratio and nothing else, but doing it
        // consistently is part of the specification.
        for i in (pos + 1)..(pos + len) {
            if i + MIN_MATCH <= input.len() {
                table[hash4(input, i)] = i;
            }
        }
        pos += len;
        literal_start = pos;
    }

    emit_literals(&mut out, &input[literal_start..]);
    out
}

fn emit_literals(out: &mut Vec<u8>, mut lits: &[u8]) {
    while !lits.is_empty() {
        let n = lits.len().min(MAX_LITERAL_RUN);
        out.push((n - 1) as u8);
        out.extend_from_slice(&lits[..n]);
        lits = &lits[n..];
    }
}

fn emit_match(out: &mut Vec<u8>, len: usize, distance: usize) {
    debug_assert!(len >= MIN_MATCH);
    debug_assert!(distance >= 1 && distance <= WINDOW);
    let excess = len - MIN_MATCH;
    let short = excess.min(MAX_SHORT_LEN);
    out.push(0x80 | short as u8);
    out.extend_from_slice(&(distance as u16).to_le_bytes());
    if short == MAX_SHORT_LEN {
        let mut rest = excess - MAX_SHORT_LEN;
        while rest >= 255 {
            out.push(255);
            rest -= 255;
        }
        out.push(rest as u8);
    }
}

/// Decompress what [`pack`] produced.
///
/// Returns `None` for any stream that is malformed — a distance that reaches
/// before the start, a truncated token, a length the buffer cannot hold. A
/// decoder that guessed here would hand back bytes that were never written.
pub fn unpack(input: &[u8], expected_len: usize) -> Option<Vec<u8>> {
    let mut out: Vec<u8> = Vec::with_capacity(expected_len.min(1 << 26));
    let mut i = 0usize;

    while i < input.len() {
        let control = input[i];
        i += 1;

        if control < 0x80 {
            let n = control as usize + 1;
            let end = i.checked_add(n)?;
            out.extend_from_slice(input.get(i..end)?);
            i = end;
            continue;
        }

        let mut len = (control & 0x7f) as usize + MIN_MATCH;
        let d = u16::from_le_bytes([*input.get(i)?, *input.get(i + 1)?]) as usize;
        i += 2;
        if (control & 0x7f) as usize == MAX_SHORT_LEN {
            loop {
                let b = *input.get(i)? as usize;
                i += 1;
                len += b;
                if b != 255 {
                    break;
                }
            }
        }
        if d == 0 || d > out.len() {
            return None;
        }
        // A refusal, not a cap: a stream claiming more output than the node
        // could hold is malformed, and growing to meet it is how a corrupt
        // byte becomes an allocation the process cannot satisfy.
        if out.len().checked_add(len)? > expected_len {
            return None;
        }
        let start = out.len() - d;
        for k in 0..len {
            // Overlapping copies are meaningful — a distance of 1 repeats one
            // byte — so this copies one at a time rather than by slice.
            let b = out[start + k];
            out.push(b);
        }
    }

    (out.len() == expected_len).then_some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(data: &[u8]) {
        let packed = pack(data);
        let back = unpack(&packed, data.len());
        assert_eq!(
            back.as_deref(),
            Some(data),
            "round trip failed for {} bytes",
            data.len()
        );
    }

    /// Deterministic inputs chosen to make the encoder's decisions visible.
    ///
    /// One frozen input was not enough. The first version of this test froze a
    /// single very regular sequence, and mutation testing showed that changing
    /// `hash4` — the match finder itself — still produced identical bytes for
    /// it. A codec is only "specified" to the extent its output is pinned on
    /// inputs that discriminate.
    ///
    /// So: several inputs, spanning the branches. Sizes either side of
    /// `MIN_MATCH`, data with a small alphabet where match choice is
    /// sensitive, runs where matches overlap, and data with no matches at all.
    fn golden_inputs() -> Vec<(&'static str, Vec<u8>)> {
        // Record-shaped: what a leaf actually holds.
        let mut records = Vec::new();
        for i in 0..40u32 {
            records.extend_from_slice(b"PREC\x02\x00");
            records.extend_from_slice(&i.to_le_bytes());
            records.extend_from_slice(b"idstatuspending");
        }

        // One stream, drawn in this order, so the two pseudorandom inputs stay
        // fixed. Reordering these two loops changes the second input's bytes
        // and therefore its frozen digest.
        let mut rng: u64 = 0x1234_5678_9abc_def0;
        let mut next = move || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };

        // A small alphabet: matches are everywhere and *which* one the finder
        // picks changes the output. This is the case a regular input hides.
        let noisy: Vec<u8> = (0..4000).map(|_| (next() % 5) as u8 + b'a').collect();

        // Nothing to find: exercises the literal path end to end.
        let incompressible: Vec<u8> = (0..1500).map(|_| next() as u8).collect();

        vec![
            ("empty", Vec::new()),
            // Either side of the shortest match, which decides whether the
            // match loop runs at all.
            ("three", b"abc".to_vec()),
            ("four", b"abcd".to_vec()),
            ("five", b"abcde".to_vec()),
            ("records", records),
            ("small alphabet", noisy),
            // Overlapping runs, where distance is shorter than length.
            ("one repeated byte", vec![b'z'; 3000]),
            ("incompressible", incompressible),
        ]
    }

    /// The codec's output is frozen, on inputs that discriminate.
    ///
    /// This is what "specified in this repository" has to mean. Every other
    /// test checks that packing and unpacking agree with *each other*, which
    /// they would continue to do after any change to the hash function, the
    /// window, the match rule or the token layout — while producing different
    /// bytes. Different bytes mean different node hashes, which means two
    /// writers on different builds silently stop sharing structure. That is
    /// the exact hazard this codec exists to avoid, and nothing detected it
    /// until mutation testing pointed out that changing `hash4` broke no test.
    ///
    /// The expected values are digests rather than hex dumps, so the test
    /// stays readable while pinning every byte.
    ///
    /// If this fails, the encoder changed. That is allowed, and it is a format
    /// change — see `docs/LEAF_ENCODING.md`. It is not something to fix by
    /// updating the expected digests.
    #[test]
    fn the_encoders_output_is_frozen() {
        use sha2::{Digest, Sha256};

        let expected: &[(&str, &str)] = &[
            ("empty", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
            ("three", "909ac45e439911193205994d09399c29fede977ab212605f29ead5250a812e73"),
            ("four", "6c4cfe52b3df8856cdc78552d2cdd8fed278436caac3eeaf556bd1f2df138975"),
            ("five", "09d5553309841962565b3950d06dd5b8765a70c7514e8955bc7104b1cf4a68d5"),
            ("records", "460130eca183db6685a3069c3c562bf8f9607bf0112b722e1d607a1cd099a0c2"),
            ("small alphabet", "724e8bdf07d4a08ccd91c60e84d0517cb95d932e03561250c657421b10a6a8b1"),
            ("one repeated byte", "a1e8986c273cc967e5059735ec0fdfe0dd583bdd6aa871e0a04448cda62488c8"),
            ("incompressible", "d7ef9056482ff18fa99bd93c7adb7b803d92aa5ccabf6dccadda75ca2b092174"),
        ];

        for ((name, data), (ename, digest)) in golden_inputs().iter().zip(expected) {
            assert_eq!(name, ename, "golden inputs and expectations drifted apart");
            let packed = pack(data);
            let got = format!("{:x}", Sha256::digest(&packed));
            assert_eq!(
                &got, digest,
                "packed bytes for {:?} changed — see this test's comment before \
                 touching it",
                name
            );
            assert_eq!(
                unpack(&packed, data.len()).as_deref(),
                Some(data.as_slice()),
                "{:?} must still round trip",
                name
            );
        }
    }

    #[test]
    fn round_trips_the_edges() {
        round_trip(b"");
        round_trip(b"a");
        round_trip(b"ab");
        round_trip(b"abc");
        round_trip(b"abcd");
        round_trip(b"abcde");
        round_trip(&[0u8; 1]);
        round_trip(&[0u8; 100_000]);
    }

    #[test]
    fn round_trips_repetitive_data() {
        let unit = b"{\"id\":123,\"status\":\"pending\",\"region\":\"eu-west-1\"}";
        let mut data = Vec::new();
        for _ in 0..500 {
            data.extend_from_slice(unit);
        }
        round_trip(&data);
        assert!(
            pack(&data).len() * 8 < data.len(),
            "500 copies of one record should pack to well under an eighth"
        );
    }

    /// Overlapping matches — a distance shorter than the length — are how a
    /// run of one repeated byte is expressed. Copying by slice would get this
    /// wrong.
    #[test]
    fn round_trips_overlapping_runs() {
        round_trip(&vec![7u8; 10_000]);
        let mut alternating = Vec::new();
        for i in 0..10_000 {
            alternating.push((i % 3) as u8);
        }
        round_trip(&alternating);
    }

    /// Incompressible input must not blow up. One control byte per 128 bytes
    /// of literal is the worst case.
    #[test]
    fn incompressible_input_barely_grows() {
        let mut rng: u64 = 0x2545_F491_4F6C_DD1D;
        let mut data = Vec::new();
        for _ in 0..50_000 {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            data.push(rng as u8);
        }
        let packed = pack(&data);
        round_trip(&data);
        let overhead = packed.len() as f64 / data.len() as f64;
        assert!(
            overhead < 1.02,
            "packing random bytes grew them by {:.1}%",
            (overhead - 1.0) * 100.0
        );
    }

    /// The property the whole design rests on: the same bytes always pack to
    /// the same bytes. Not "usually", and not "for this build".
    #[test]
    fn packing_is_deterministic() {
        let unit = b"field-name-and-a-version-stamp-and-a-value";
        let mut data = Vec::new();
        for i in 0..300 {
            data.extend_from_slice(unit);
            data.push(i as u8);
        }
        let once = pack(&data);
        for _ in 0..20 {
            assert_eq!(pack(&data), once);
        }
    }

    /// Malformed streams are refused rather than guessed at.
    #[test]
    fn malformed_streams_are_refused() {
        let data = b"the quick brown fox jumps over the quick brown fox".repeat(20);
        let packed = pack(&data);

        // Truncation anywhere.
        for cut in 1..packed.len().min(40) {
            let _ = unpack(&packed[..packed.len() - cut], data.len());
        }
        // A wrong expected length is a mismatch, not a panic.
        assert_eq!(unpack(&packed, data.len() - 1), None);
        assert_eq!(unpack(&packed, data.len() + 1), None);
        // A match reaching before the start.
        assert_eq!(unpack(&[0x80, 0xff, 0xff], 100), None);
        // A zero distance names nothing.
        assert_eq!(unpack(&[0x80, 0x00, 0x00], 100), None);
    }

    /// Random inputs must never panic and must always round trip.
    #[test]
    fn fuzzing_round_trips_or_refuses() {
        let mut rng: u64 = 12345;
        let mut next = || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        for _ in 0..300 {
            let n = (next() % 4000) as usize;
            // A small alphabet, so matches actually occur.
            let alphabet = (next() % 8 + 1) as u8;
            let data: Vec<u8> = (0..n).map(|_| (next() % alphabet as u64) as u8).collect();
            round_trip(&data);

            // And arbitrary bytes fed to the decoder must not panic.
            let junk: Vec<u8> = (0..(next() % 200) as usize).map(|_| next() as u8).collect();
            let _ = unpack(&junk, 1000);
        }
    }
}
