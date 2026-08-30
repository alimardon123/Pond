// head.rs — the publish unit.
//
// A head is one writer's view of the pond: a map from collection name to that
// collection's root, plus the versions this writer has observed from others.
//
//     heads/<writer_id>  ->  { collection -> root, observed: {writer -> root} }
//
// # Why the head spans collections
//
// Because it makes atomic multi-collection publish free. Object stores give
// single-object write atomicity, so writing one head object publishes every
// collection in it at once: a reader sees either the whole previous state or
// the whole new one, never a mix. No transaction subsystem, no markers, no
// two-phase protocol — the feature falls out of putting the right things in
// one object.
//
// That is worth contrasting with what is in the tree today: `begin_tx` /
// `commit_tx` write a marker that *no read path ever consults*, and no writer
// tags its shards with a transaction id, so the transaction machinery has no
// effect on what readers see. This preserves the API and gives it meaning.
//
// # Why one head per writer
//
// A writer only ever writes its own head key, so two writers can never collide
// on it and last-writer-wins is never wrong. That is what removes the need for
// compare-and-swap — which matters because conditional writes exist on object
// stores but not on a local filesystem, and a primitive available on only some
// backends forks the correctness argument in two.
//
// Readers list the heads, merge them, and get a consistent view. Since no two
// writers share a key, syncing two stores — laptop to S3, region to region —
// is a plain bidirectional file copy with no conflict resolution at all.
//
// # What this does and does not give
//
// Gives: atomicity and snapshot semantics per publish, and convergence across
// writers. Does not give: serializability between concurrent writers — there
// is no global serialization point, so two writers can publish conflicting
// updates to the same key and the merge resolves it by version rather than by
// aborting one. That is the same trade WarpStream and Fluss make, and it
// belongs in NON_GOALS.md rather than in a footnote.

use std::collections::BTreeMap;

/// One writer's published state.
///
/// `BTreeMap` so the encoding is canonical — a head is content-addressed like
/// everything else, and two writers with the same state must produce the same
/// bytes.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Head {
    /// Stable identity of the writer that owns this head.
    pub writer_id: u64,
    /// collection name -> root hash of that collection's index.
    pub collections: BTreeMap<String, String>,
    /// An index holding `collection -> root`, when the map is too large to
    /// carry inline.
    ///
    /// # Why a head can stop listing its own collections
    ///
    /// A head is rewritten whole on every publish, so its size is paid on
    /// every write and every open. With the map inline that is linear in the
    /// number of collections the writer publishes: measured at ~40 bytes each,
    /// so 10^6 collections means ~40 MB moved to write a single row. `pond_bench
    /// --bin headscale` has the table.
    ///
    /// Above [`INLINE_COLLECTION_LIMIT`] the map moves into a content-addressed
    /// index and the head carries only its root. Publishing then rewrites the
    /// O(log C) nodes on one path plus a head of fixed size, and the head is
    /// still **one object** — which is the whole reason the map lived here in
    /// the first place, since single-object write atomicity is what makes
    /// multi-collection publish atomic.
    ///
    /// This type does not interpret the root. `pond_record` knows nothing about
    /// indexes, and giving it that dependency to hold a hash would be paying a
    /// large price for a small convenience — the engine owns the meaning.
    ///
    /// When this is set, `collections` is empty; when it is `None`, the map is
    /// inline. Both are read.
    pub collections_root: Option<String>,
    /// Roots this writer has seen from other writers, as a version vector.
    ///
    /// This is what turns "these two states differ" into "this state causally
    /// follows that one": a writer that has observed another's root knows its
    /// own writes came after, which makes fast-forward detection exact and
    /// distinguishes a genuine concurrent edit from a sequential one.
    pub observed: BTreeMap<u64, String>,
}

impl Head {
    pub fn new(writer_id: u64) -> Self {
        Self {
            writer_id,
            collections: BTreeMap::new(),
            collections_root: None,
            observed: BTreeMap::new(),
        }
    }

    pub fn root_of(&self, collection: &str) -> Option<&str> {
        self.collections.get(collection).map(|s| s.as_str())
    }

    /// Stage a collection root. Nothing is published until the head is written.
    pub fn set_root(&mut self, collection: &str, root: &str) {
        self.collections
            .insert(collection.to_string(), root.to_string());
    }

    pub fn remove(&mut self, collection: &str) -> bool {
        self.collections.remove(collection).is_some()
    }

    /// Record that this writer has seen `root` from `writer`.
    pub fn observe(&mut self, writer: u64, root: &str) {
        self.observed.insert(writer, root.to_string());
    }

    /// Has this writer already seen everything `other` had published?
    ///
    /// True when this head's observation of `other`'s writer id matches
    /// `other`'s current roots — meaning `other` contributes nothing new and
    /// the merge is a fast-forward.
    pub fn has_observed(&self, other: &Head) -> bool {
        match self.observed.get(&other.writer_id) {
            Some(seen) => other.collections.values().any(|r| r == seen),
            None => false,
        }
    }

    pub fn collection_names(&self) -> impl Iterator<Item = &String> {
        self.collections.keys()
    }

    pub fn is_empty(&self) -> bool {
        self.collections.is_empty()
    }
}

/// Encode a head canonically.
///
/// Layout: magic, version, writer_id, then length-prefixed maps. As with
/// records, every length is explicit and no buffer-supplied count is used to
/// pre-allocate.
/// Format version written by [`encode_head`].
///
/// v1 stored every root as its 64-character hex text. v2 stores the 32 raw
/// bytes instead, which is the same information at half the size — and the
/// head is rewritten whole on every publish, so its size is paid on every
/// write and every open. `pond_bench --bin headscale` measured the v1 cost at
/// about 73 bytes per collection: 729 KB to write one row into a pond with
/// 10,000 collections.
///
/// The same change was made to spilled-value hashes in the record encoder for
/// the same reason. A hash is bytes; hex is a display format.
///
/// This is a constant factor, not a fix for the growth. The head still carries
/// one entry per collection and is still rewritten whole, so the cost is still
/// linear in the number of collections a writer publishes — see
/// docs/CRITIQUE.md for the tree-backed head that would remove the linearity
/// while keeping single-object atomicity.
const VERSION_V2: u8 = 2;

/// v3 adds an optional index root for the collection map — see
/// [`Head::collections_root`]. Everything else is identical to v2, and v3 is
/// written only when the root is actually present, so a head that carries its
/// map inline still encodes exactly as v2 did and old readers keep working
/// for as long as that is true.
const VERSION_V3: u8 = 3;

/// A root that is not 32 bytes of hex, written as text.
///
/// Every root the engine produces is a 64-character hex hash, so this should
/// not occur. It exists because a format that cannot represent something it
/// is handed will either panic or silently truncate, and both are worse than
/// one extra byte.
const ROOT_TEXT: u8 = 0;
/// A root stored as its 32 raw bytes.
const ROOT_BINARY: u8 = 1;

fn put_root(out: &mut Vec<u8>, root: &str) {
    match crate::encode::decode_hex32(root) {
        Some(raw) => {
            out.push(ROOT_BINARY);
            out.extend_from_slice(&raw);
        }
        None => {
            out.push(ROOT_TEXT);
            out.extend_from_slice(&(root.len() as u16).to_le_bytes());
            out.extend_from_slice(root.as_bytes());
        }
    }
}

pub fn encode_head(h: &Head) -> Vec<u8> {
    const MAGIC: &[u8; 4] = b"PHED";
    let mut out = Vec::from(*MAGIC);
    // Only claim v3 when there is something only v3 can express. A pond that
    // never grows past the inline limit stays readable by anything that
    // understands v2.
    out.push(if h.collections_root.is_some() {
        VERSION_V3
    } else {
        VERSION_V2
    });
    out.extend_from_slice(&h.writer_id.to_le_bytes());

    out.extend_from_slice(&(h.collections.len() as u32).to_le_bytes());
    for (name, root) in &h.collections {
        out.extend_from_slice(&(name.len() as u16).to_le_bytes());
        out.extend_from_slice(name.as_bytes());
        put_root(&mut out, root);
    }

    out.extend_from_slice(&(h.observed.len() as u32).to_le_bytes());
    for (writer, root) in &h.observed {
        out.extend_from_slice(&writer.to_le_bytes());
        put_root(&mut out, root);
    }

    if let Some(root) = &h.collections_root {
        put_root(&mut out, root);
    }
    out
}

pub fn decode_head(buf: &[u8]) -> Option<Head> {
    let mut pos = 0usize;
    let take = |pos: &mut usize, n: usize| -> Option<&[u8]> {
        let end = pos.checked_add(n)?;
        let s = buf.get(*pos..end)?;
        *pos = end;
        Some(s)
    };

    if take(&mut pos, 4)? != b"PHED" {
        return None;
    }
    // Both versions are read. A content-addressed store cannot rewrite what
    // it already holds, so heads written before v2 stay on disk and stay
    // readable — the version byte is what makes that possible rather than a
    // guess about length.
    let version = take(&mut pos, 1)?[0];
    if version != 1 && version != VERSION_V2 && version != VERSION_V3 {
        return None;
    }
    let writer_id = u64::from_le_bytes(take(&mut pos, 8)?.try_into().ok()?);

    // One root, in whichever form this version stores them.
    let get_root = |pos: &mut usize| -> Option<String> {
        if version == 1 {
            let rl = u16::from_le_bytes(take(pos, 2)?.try_into().ok()?) as usize;
            return String::from_utf8(take(pos, rl)?.to_vec()).ok();
        }
        match take(pos, 1)?[0] {
            ROOT_BINARY => {
                let raw: [u8; 32] = take(pos, 32)?.try_into().ok()?;
                Some(crate::encode::encode_hex32(raw))
            }
            ROOT_TEXT => {
                let rl = u16::from_le_bytes(take(pos, 2)?.try_into().ok()?) as usize;
                String::from_utf8(take(pos, rl)?.to_vec()).ok()
            }
            _ => None,
        }
    };

    let n_coll = u32::from_le_bytes(take(&mut pos, 4)?.try_into().ok()?) as usize;
    // Smallest possible entry: a zero-length name plus the shortest root form.
    let min_entry = if version == 1 { 4 } else { 3 };
    if n_coll.saturating_mul(min_entry) > buf.len().saturating_sub(pos) {
        return None;
    }
    let mut collections = BTreeMap::new();
    for _ in 0..n_coll {
        let nl = u16::from_le_bytes(take(&mut pos, 2)?.try_into().ok()?) as usize;
        let name = String::from_utf8(take(&mut pos, nl)?.to_vec()).ok()?;
        let root = get_root(&mut pos)?;
        collections.insert(name, root);
    }

    let n_obs = u32::from_le_bytes(take(&mut pos, 4)?.try_into().ok()?) as usize;
    if n_obs.saturating_mul(if version == 1 { 10 } else { 9 })
        > buf.len().saturating_sub(pos)
    {
        return None;
    }
    let mut observed = BTreeMap::new();
    for _ in 0..n_obs {
        let w = u64::from_le_bytes(take(&mut pos, 8)?.try_into().ok()?);
        let root = get_root(&mut pos)?;
        observed.insert(w, root);
    }

    let collections_root = if version == VERSION_V3 {
        Some(get_root(&mut pos)?)
    } else {
        None
    };

    Some(Head {
        writer_id,
        collections,
        collections_root,
        observed,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Encode a head the way v1 did: roots as their hex text.
    ///
    /// Written out longhand rather than kept as a captured byte string,
    /// because the point is that the *shape* still decodes, and a reader
    /// should be able to see what that shape was.
    fn encode_head_v1(h: &Head) -> Vec<u8> {
        let mut out = Vec::from(*b"PHED");
        out.push(1u8);
        out.extend_from_slice(&h.writer_id.to_le_bytes());
        out.extend_from_slice(&(h.collections.len() as u32).to_le_bytes());
        for (name, root) in &h.collections {
            out.extend_from_slice(&(name.len() as u16).to_le_bytes());
            out.extend_from_slice(name.as_bytes());
            out.extend_from_slice(&(root.len() as u16).to_le_bytes());
            out.extend_from_slice(root.as_bytes());
        }
        out.extend_from_slice(&(h.observed.len() as u32).to_le_bytes());
        for (writer, root) in &h.observed {
            out.extend_from_slice(&writer.to_le_bytes());
            out.extend_from_slice(&(root.len() as u16).to_le_bytes());
            out.extend_from_slice(root.as_bytes());
        }
        out
    }

    fn sample() -> Head {
        let mut h = Head::new(42);
        h.set_root("users", &"a1".repeat(32));
        h.set_root("orders", &"b2".repeat(32));
        h.observe(7, &"c3".repeat(32));
        h
    }

    /// Heads written before v2 must still read. A content-addressed store
    /// cannot rewrite what it already holds, so old objects stay on disk
    /// forever and staying readable is not optional.
    #[test]
    fn a_v1_head_still_decodes() {
        let h = sample();
        let decoded = decode_head(&encode_head_v1(&h)).expect("v1 head must decode");
        assert_eq!(decoded, h);
    }

    /// And v2 is smaller, which is the whole reason for it.
    #[test]
    fn v2_is_smaller_than_v1() {
        let mut h = Head::new(1);
        for i in 0..100 {
            h.set_root(&format!("c{i}"), &format!("{:064x}", i));
        }
        let v1 = encode_head_v1(&h).len();
        let v2 = encode_head(&h).len();
        assert!(
            v2 * 3 < v1 * 2,
            "v2 should be well under two-thirds of v1: {v1} -> {v2}"
        );
        assert_eq!(decode_head(&encode_head(&h)).as_ref(), Some(&h));
    }

    /// A root that is not 32 bytes of hex must survive rather than be
    /// truncated or panicked on. No engine produces one, which is exactly why
    /// nothing else would catch it.
    #[test]
    fn a_root_that_is_not_a_hash_round_trips() {
        let mut h = Head::new(3);
        h.set_root("odd", "not-a-hash");
        h.set_root("empty", "");
        h.observe(9, "also-not-a-hash");
        assert_eq!(decode_head(&encode_head(&h)).as_ref(), Some(&h));
    }

    #[test]
    fn test_head_roundtrip() {
        let mut h = Head::new(0xABCD);
        h.set_root("users", &"a".repeat(64));
        h.set_root("events", &"b".repeat(64));
        h.observe(1, &"c".repeat(64));
        assert_eq!(decode_head(&encode_head(&h)), Some(h));
    }

    #[test]
    fn test_empty_head_roundtrip() {
        let h = Head::new(1);
        assert_eq!(decode_head(&encode_head(&h)), Some(h));
    }

    /// Encoding must be canonical, so two writers in the same state produce
    /// the same bytes and therefore the same content hash.
    #[test]
    fn test_head_encoding_is_canonical() {
        let mut a = Head::new(1);
        a.set_root("z", "1");
        a.set_root("a", "2");

        let mut b = Head::new(1);
        b.set_root("a", "2");
        b.set_root("z", "1");

        assert_eq!(encode_head(&a), encode_head(&b));
    }

    /// The property that makes atomic multi-collection publish work: all the
    /// roots live in one object, so one write publishes them together.
    #[test]
    fn test_multi_collection_publish_is_one_object() {
        let mut h = Head::new(1);
        h.set_root("users", &"1".repeat(64));
        h.set_root("orders", &"2".repeat(64));
        h.set_root("events", &"3".repeat(64));

        let bytes = encode_head(&h);
        let restored = decode_head(&bytes).unwrap();

        // Either all three roots are present, or (if the write never landed)
        // none are — there is no encoding in which a reader sees a subset.
        assert_eq!(restored.collections.len(), 3);
        assert_eq!(restored.root_of("users"), Some("1".repeat(64).as_str()));
        assert_eq!(restored.root_of("orders"), Some("2".repeat(64).as_str()));
        assert_eq!(restored.root_of("events"), Some("3".repeat(64).as_str()));
    }

    /// A partially-written head must be rejected outright rather than
    /// yielding a subset of the collections — that is what makes the
    /// all-or-nothing guarantee real under a crash.
    #[test]
    fn test_truncated_head_is_rejected_not_partially_read() {
        let mut h = Head::new(1);
        for i in 0..5 {
            h.set_root(&format!("c{}", i), &format!("{:064x}", i));
        }
        let good = encode_head(&h);

        for cut in 0..good.len() {
            match decode_head(&good[..cut]) {
                None => {}
                Some(partial) => panic!(
                    "truncation at {} decoded to a partial head with {} collections",
                    cut,
                    partial.collections.len()
                ),
            }
        }
    }

    #[test]
    fn test_observed_tracks_causality() {
        let mut a = Head::new(1);
        let mut b = Head::new(2);
        b.set_root("users", &"r2".repeat(32));

        assert!(!a.has_observed(&b), "nothing observed yet");
        a.observe(2, &"r2".repeat(32));
        assert!(a.has_observed(&b), "after observing, b adds nothing new");

        // b moves on; a's observation is now stale.
        b.set_root("users", &"r3".repeat(32));
        assert!(!a.has_observed(&b));
    }

    #[test]
    fn test_decode_rejects_malformed() {
        assert!(decode_head(&[]).is_none());
        assert!(decode_head(b"XXXX\x01").is_none());
        let mut evil = Vec::from(*b"PHED");
        evil.push(1);
        evil.extend_from_slice(&0u64.to_le_bytes());
        evil.extend_from_slice(&u32::MAX.to_le_bytes());
        assert!(decode_head(&evil).is_none());
    }

    #[test]
    fn test_decode_survives_fuzzing() {
        let mut h = Head::new(9);
        h.set_root("users", &"a".repeat(64));
        h.observe(3, &"b".repeat(64));
        let good = encode_head(&h);

        let mut state: u64 = 0xF00D;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for _ in 0..20_000 {
            let mut b = good.clone();
            for _ in 0..(1 + next() % 3) {
                let pos = (next() as usize) % b.len();
                b[pos] = (next() >> 11) as u8;
            }
            let _ = decode_head(&b);
        }
    }
}
