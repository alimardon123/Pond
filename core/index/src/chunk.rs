// chunk.rs — content-defined chunk boundaries.
//
// This is the single decision that makes the index history-independent, and
// it is the one the archived prolly tree documented but never implemented:
// `archive/legacy-sdk/prolly_tree.py` describes rolling-hash boundaries in
// detail, defines `_rolling_hash_boundary()`, and then chunks with fixed
// 64-entry slices. A fixed-fanout tree has none of the properties below.
//
// The rule: an entry ends a chunk when a hash *of that entry's own bytes*
// lands on a boundary value. Nothing about the entry's position, the insertion
// order, or which writer produced it participates in the decision.
//
// Four consequences, all load-bearing:
//
//   1. History independence — the same set of entries always produces the same
//      chunks, hence the same node hashes, hence the same root. Two writers
//      who converge on the same data converge on the same bytes.
//   2. Local mutation — inserting one entry can only change the chunk it lands
//      in (and, rarely, shift one boundary). Cost is O(log n) rewritten nodes,
//      not O(n).
//   3. O(differences) diff — two trees sharing data share subtree hashes, so a
//      diff skips identical subtrees in O(1).
//   4. Deterministic merge — merge is a set operation over content-addressed
//      nodes, so merge(A,B) and merge(B,A) produce identical bytes.
//
// A max-size guard bounds pathological chunks (an adversarial or unlucky run
// with no boundary hit). It is a safety valve, not the mechanism: with the
// default target it fires rarely, and the history-independence test in
// `tree.rs` is what actually verifies the combination behaves.

/// Target average number of entries per chunk.
///
/// Chosen so an internal node holds a few hundred children: at ~80 bytes per
/// index entry, 512 entries is roughly a 40 KB node, which is a sensible unit
/// for one ranged GET against object storage. Fanout of this order is what
/// keeps the tree 2 levels deep from a gigabyte to a petabyte.
pub const DEFAULT_TARGET_ENTRIES: u32 = 512;

/// Hard cap on entries per chunk, so a run without a boundary hit cannot
/// produce an unbounded node.
pub const MAX_ENTRIES_PER_CHUNK: usize = 4 * DEFAULT_TARGET_ENTRIES as usize;

/// Minimum entries per chunk, so a pathological input cannot produce a tree of
/// one-entry nodes (which would destroy fanout and blow up depth).
pub const MIN_ENTRIES_PER_CHUNK: usize = 2;

/// Chunking parameters. These participate in content addressing — changing
/// them changes every hash — so they are fixed per index at creation and
/// recorded in the root, never adjusted at runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkConfig {
    pub target_entries: u32,
    pub max_entries: usize,
    pub min_entries: usize,
}

impl Default for ChunkConfig {
    fn default() -> Self {
        Self {
            target_entries: DEFAULT_TARGET_ENTRIES,
            max_entries: MAX_ENTRIES_PER_CHUNK,
            min_entries: MIN_ENTRIES_PER_CHUNK,
        }
    }
}

impl ChunkConfig {
    /// Smaller chunks, for tests that need multi-level trees without needing
    /// hundreds of thousands of entries.
    pub fn with_target(target: u32) -> Self {
        Self {
            target_entries: target,
            max_entries: (target as usize) * 4,
            min_entries: MIN_ENTRIES_PER_CHUNK,
        }
    }

    /// Does this entry end a chunk?
    ///
    /// `fingerprint` is a hash of the entry's content (see [`fingerprint`]).
    /// The probability of a boundary is 1/target_entries, so chunk sizes follow
    /// a geometric distribution with that mean.
    #[inline]
    pub fn is_boundary(&self, fingerprint: u64, entries_in_chunk: usize) -> bool {
        if entries_in_chunk < self.min_entries {
            return false;
        }
        if entries_in_chunk >= self.max_entries {
            return true;
        }
        // The high bits of the fingerprint are the best mixed; use them so the
        // decision is independent of any low-bit structure in the input.
        (fingerprint >> 40).is_multiple_of(self.target_entries as u64)
    }
}

/// Content fingerprint of one entry.
///
/// FNV-1a: a well-defined, stable, dependency-free mix. This function's exact
/// behaviour is part of the on-disk format — every reader must compute the
/// same boundaries — so it must never change without a format version bump.
#[inline]
pub fn fingerprint(bytes: &[u8]) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x1000_0000_01b3;
    let mut h = OFFSET;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(PRIME);
    }
    // Final avalanche (splitmix64 finalizer) so the high bits used by
    // `is_boundary` are well mixed even for short inputs.
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51_afd7_ed55_8ccd);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    h ^= h >> 33;
    h
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Boundaries must depend only on content, never on position.
    #[test]
    fn test_boundary_depends_only_on_content() {
        let cfg = ChunkConfig::default();
        let fp = fingerprint(b"some entry bytes");
        // Same fingerprint, different positions past the minimum — same answer.
        let a = cfg.is_boundary(fp, 10);
        let b = cfg.is_boundary(fp, 99);
        assert_eq!(a, b, "boundary decision must not depend on position");
    }

    /// Chunk sizes should average near the target. This is what keeps fanout —
    /// and therefore depth — predictable.
    #[test]
    fn test_average_chunk_size_near_target() {
        let cfg = ChunkConfig::with_target(64);
        let mut sizes = Vec::new();
        let mut in_chunk = 0usize;
        for i in 0..200_000u64 {
            in_chunk += 1;
            let fp = fingerprint(&i.to_be_bytes());
            if cfg.is_boundary(fp, in_chunk) {
                sizes.push(in_chunk);
                in_chunk = 0;
            }
        }
        assert!(!sizes.is_empty(), "expected some boundaries");
        let mean = sizes.iter().sum::<usize>() as f64 / sizes.len() as f64;
        assert!(
            mean > 32.0 && mean < 128.0,
            "mean chunk size {} should be near the 64 target",
            mean
        );
    }

    /// The minimum guard must actually prevent tiny chunks.
    #[test]
    fn test_min_entries_respected() {
        let cfg = ChunkConfig::default();
        for fp in 0..1000u64 {
            assert!(
                !cfg.is_boundary(fp, 1),
                "must never cut below the minimum chunk size"
            );
        }
    }

    /// The maximum guard must actually cap chunk size.
    #[test]
    fn test_max_entries_respected() {
        let cfg = ChunkConfig::default();
        assert!(cfg.is_boundary(1, cfg.max_entries));
    }

    /// The fingerprint must be stable — it is part of the format.
    #[test]
    fn test_fingerprint_is_deterministic() {
        assert_eq!(fingerprint(b"pond"), fingerprint(b"pond"));
        assert_ne!(fingerprint(b"pond"), fingerprint(b"pand"));
    }
}
