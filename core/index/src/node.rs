// node.rs — index node format.
//
// Two node kinds, both immutable and content-addressed:
//
//   Leaf     sorted (key, value) entries
//   Internal sorted (max_key, child_hash, subtree_count) entries
//
// The internal entry carries the child's max key *and* its subtree row count.
// That is what makes predicate pruning the descent itself rather than a
// separate zone-map structure: a reader comparing a predicate against the
// child's key range decides whether to fetch that child at all, and gets
// COUNT(*) for a range without reading any leaf.
//
// Byte layout (little-endian lengths, all offsets explicit so a truncated or
// corrupted node is rejected rather than trusted):
//
//   [0]      tag: 0 = leaf, 1 = internal
//   [1..5]   n_entries: u32
//   leaf entry:      key_len u32, key, val_len u32, val
//   internal entry:  key_len u32, max_key, hash_len u8, hash, count u64
//
// Every length is validated against the remaining buffer before use — the same
// class of bug that let one corrupted byte request a 28 GB allocation in the
// PND2 decoder.

use crate::store::Hash;

const TAG_LEAF: u8 = 0;
const TAG_INTERNAL: u8 = 1;

/// Cap on a single node's declared entry count, used to reject malformed
/// nodes before allocating. Well above any legitimate node.
const MAX_DECLARED_ENTRIES: usize = 1 << 24;

/// A pointer from an internal node to one child.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChildRef {
    /// Largest key in the child's subtree. Descent compares against this.
    pub max_key: Vec<u8>,
    pub hash: Hash,
    /// Number of entries in the child's subtree — gives range counts without
    /// touching leaves.
    pub count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Node {
    Leaf { entries: Vec<(Vec<u8>, Vec<u8>)> },
    Internal { children: Vec<ChildRef> },
}

impl Node {
    /// Total entries in this subtree.
    pub fn count(&self) -> u64 {
        match self {
            Node::Leaf { entries } => entries.len() as u64,
            Node::Internal { children } => children.iter().map(|c| c.count).sum(),
        }
    }

    /// Largest key in this subtree, or None if empty.
    pub fn max_key(&self) -> Option<Vec<u8>> {
        match self {
            Node::Leaf { entries } => entries.last().map(|(k, _)| k.clone()),
            Node::Internal { children } => children.last().map(|c| c.max_key.clone()),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Node::Leaf { entries } => entries.is_empty(),
            Node::Internal { children } => children.is_empty(),
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        match self {
            Node::Leaf { entries } => {
                out.push(TAG_LEAF);
                out.extend_from_slice(&(entries.len() as u32).to_le_bytes());
                for (k, v) in entries {
                    out.extend_from_slice(&(k.len() as u32).to_le_bytes());
                    out.extend_from_slice(k);
                    out.extend_from_slice(&(v.len() as u32).to_le_bytes());
                    out.extend_from_slice(v);
                }
            }
            Node::Internal { children } => {
                out.push(TAG_INTERNAL);
                out.extend_from_slice(&(children.len() as u32).to_le_bytes());
                for c in children {
                    out.extend_from_slice(&(c.max_key.len() as u32).to_le_bytes());
                    out.extend_from_slice(&c.max_key);
                    out.push(c.hash.len() as u8);
                    out.extend_from_slice(c.hash.as_bytes());
                    out.extend_from_slice(&c.count.to_le_bytes());
                }
            }
        }
        out
    }

    pub fn decode(buf: &[u8]) -> Option<Node> {
        let mut r = Reader { buf, pos: 0 };
        let tag = r.u8()?;
        let n = r.u32()? as usize;
        if n > MAX_DECLARED_ENTRIES {
            return None;
        }
        match tag {
            TAG_LEAF => {
                // Do not pre-allocate `n` — it came from the buffer. Growth is
                // amortized O(1) and a hostile count then costs nothing.
                let mut entries = Vec::new();
                for _ in 0..n {
                    let k = r.bytes_u32()?;
                    let v = r.bytes_u32()?;
                    entries.push((k, v));
                }
                Some(Node::Leaf { entries })
            }
            TAG_INTERNAL => {
                let mut children = Vec::new();
                for _ in 0..n {
                    let max_key = r.bytes_u32()?;
                    let hlen = r.u8()? as usize;
                    let hash_bytes = r.take(hlen)?;
                    let hash = String::from_utf8(hash_bytes.to_vec()).ok()?;
                    let count = r.u64()?;
                    children.push(ChildRef {
                        max_key,
                        hash,
                        count,
                    });
                }
                Some(Node::Internal { children })
            }
            _ => None,
        }
    }
}

/// Bounds-checked reader. Every accessor returns None rather than panicking
/// or over-allocating, so a corrupted node is an error, not a crash.
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
    fn u64(&mut self) -> Option<u64> {
        Some(u64::from_le_bytes(self.take(8)?.try_into().ok()?))
    }
    fn bytes_u32(&mut self) -> Option<Vec<u8>> {
        let len = self.u32()? as usize;
        Some(self.take(len)?.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_leaf_roundtrip() {
        let n = Node::Leaf {
            entries: vec![
                (b"a".to_vec(), b"1".to_vec()),
                (b"b".to_vec(), b"22".to_vec()),
            ],
        };
        assert_eq!(Node::decode(&n.encode()).unwrap(), n);
        assert_eq!(n.count(), 2);
        assert_eq!(n.max_key(), Some(b"b".to_vec()));
    }

    #[test]
    fn test_internal_roundtrip() {
        let n = Node::Internal {
            children: vec![
                ChildRef {
                    max_key: b"m".to_vec(),
                    hash: "a".repeat(64),
                    count: 10,
                },
                ChildRef {
                    max_key: b"z".to_vec(),
                    hash: "b".repeat(64),
                    count: 7,
                },
            ],
        };
        assert_eq!(Node::decode(&n.encode()).unwrap(), n);
        assert_eq!(n.count(), 17, "subtree counts sum without reading leaves");
        assert_eq!(n.max_key(), Some(b"z".to_vec()));
    }

    #[test]
    fn test_empty_node_roundtrip() {
        let n = Node::Leaf { entries: vec![] };
        assert_eq!(Node::decode(&n.encode()).unwrap(), n);
        assert!(n.is_empty());
    }

    /// Encoding is canonical: identical contents produce identical bytes, so
    /// identical contents produce the identical hash. Everything downstream
    /// (dedup, convergence, diff) depends on this.
    #[test]
    fn test_encoding_is_canonical() {
        let a = Node::Leaf {
            entries: vec![(b"k".to_vec(), b"v".to_vec())],
        };
        let b = Node::Leaf {
            entries: vec![(b"k".to_vec(), b"v".to_vec())],
        };
        assert_eq!(a.encode(), b.encode());
    }

    /// A corrupted or truncated node must decode to None, never panic and
    /// never allocate on a declared length.
    #[test]
    fn test_decode_rejects_malformed() {
        assert!(Node::decode(&[]).is_none());
        assert!(Node::decode(&[99, 0, 0, 0, 0]).is_none(), "unknown tag");

        // Declares 4 billion entries in a 5-byte buffer.
        let mut evil = vec![TAG_LEAF];
        evil.extend_from_slice(&u32::MAX.to_le_bytes());
        assert!(Node::decode(&evil).is_none());

        // Truncations of a valid node must all decode to None, not panic.
        let good = Node::Leaf {
            entries: vec![(b"key".to_vec(), b"value".to_vec())],
        }
        .encode();
        for cut in 0..good.len() {
            let _ = Node::decode(&good[..cut]);
        }
    }

    /// Fuzz the decoder: random bytes must never panic.
    #[test]
    fn test_decode_survives_random_bytes() {
        let mut state: u64 = 0xDEAD_BEEF;
        for _ in 0..20_000 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let len = (state % 64) as usize;
            let bytes: Vec<u8> = (0..len)
                .map(|i| (state >> (i % 56)) as u8)
                .collect();
            let _ = Node::decode(&bytes);
        }
    }
}
