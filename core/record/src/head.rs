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
pub fn encode_head(h: &Head) -> Vec<u8> {
    const MAGIC: &[u8; 4] = b"PHED";
    let mut out = Vec::from(*MAGIC);
    out.push(1u8); // format version
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
    if take(&mut pos, 1)?[0] != 1 {
        return None;
    }
    let writer_id = u64::from_le_bytes(take(&mut pos, 8)?.try_into().ok()?);

    let n_coll = u32::from_le_bytes(take(&mut pos, 4)?.try_into().ok()?) as usize;
    // Smallest possible entry is 2 + 0 + 2 + 0 = 4 bytes.
    if n_coll.saturating_mul(4) > buf.len().saturating_sub(pos) {
        return None;
    }
    let mut collections = BTreeMap::new();
    for _ in 0..n_coll {
        let nl = u16::from_le_bytes(take(&mut pos, 2)?.try_into().ok()?) as usize;
        let name = String::from_utf8(take(&mut pos, nl)?.to_vec()).ok()?;
        let rl = u16::from_le_bytes(take(&mut pos, 2)?.try_into().ok()?) as usize;
        let root = String::from_utf8(take(&mut pos, rl)?.to_vec()).ok()?;
        collections.insert(name, root);
    }

    let n_obs = u32::from_le_bytes(take(&mut pos, 4)?.try_into().ok()?) as usize;
    if n_obs.saturating_mul(10) > buf.len().saturating_sub(pos) {
        return None;
    }
    let mut observed = BTreeMap::new();
    for _ in 0..n_obs {
        let w = u64::from_le_bytes(take(&mut pos, 8)?.try_into().ok()?);
        let rl = u16::from_le_bytes(take(&mut pos, 2)?.try_into().ok()?) as usize;
        let root = String::from_utf8(take(&mut pos, rl)?.to_vec()).ok()?;
        observed.insert(w, root);
    }

    Some(Head {
        writer_id,
        collections,
        observed,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

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
