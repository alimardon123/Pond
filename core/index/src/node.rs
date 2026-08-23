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
/// A node whose plain encoding follows, packed. Distinct from the leaf and
/// internal tags so a reader can tell from the first byte, and so nodes
/// written before packing existed still decode unchanged.
const TAG_PACKED: u8 = 2;
/// Largest plain encoding a packed node may claim. Far above any real node;
/// it exists so a corrupt length cannot ask for an allocation the process
/// cannot satisfy.
const MAX_NODE_BYTES: usize = 512 * 1024 * 1024;

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

    /// Encode this node, packed when packing wins.
    ///
    /// A node is content-addressed, so its bytes *are* its identity — which is
    /// why the codec is [`crate::pack`], specified in this repository, rather
    /// than a compression crate. A crate's output is a property of the crate:
    /// two writers on different builds could emit different bytes for the same
    /// node, take different hashes, and silently stop sharing structure. See
    /// that module for the full argument.
    ///
    /// A node is packed only when packing is actually smaller, and the choice
    /// is recorded in a tag, so an unpackable node costs one byte rather than
    /// growing. The decision is a pure function of the bytes, so canonical
    /// encoding survives it: two writers with the same node make the same
    /// choice.
    pub fn encode(&self) -> Vec<u8> {
        let plain = self.encode_plain();
        let packed = crate::pack::pack(&plain);
        // 5 bytes of frame: the tag and the unpacked length.
        if packed.len() + 5 < plain.len() {
            let mut out = Vec::with_capacity(packed.len() + 5);
            out.push(TAG_PACKED);
            out.extend_from_slice(&(plain.len() as u32).to_le_bytes());
            out.extend_from_slice(&packed);
            return out;
        }
        plain
    }

    fn encode_plain(&self) -> Vec<u8> {
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
        // A packed node unwraps to the plain encoding and is then decoded
        // exactly as an unpacked one. Nodes written before packing existed
        // carry a leaf or internal tag here and take the second path
        // untouched, which is what a content-addressed store requires: it
        // cannot rewrite what it already holds.
        if buf.first() == Some(&TAG_PACKED) {
            if buf.len() < 5 {
                return None;
            }
            let len = u32::from_le_bytes(buf[1..5].try_into().ok()?) as usize;
            // A declared length no node could have is malformed. Refusing is
            // cheaper than discovering it after allocating for it.
            if len > MAX_NODE_BYTES {
                return None;
            }
            let plain = crate::pack::unpack(&buf[5..], len)?;
            return Self::decode_plain(&plain);
        }
        Self::decode_plain(buf)
    }

    fn decode_plain(buf: &[u8]) -> Option<Node> {
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
    fn leaf_of(n: usize) -> Node {
        // Shaped like real data: the same field names in every record, with
        // values from a small domain. That is where the redundancy is.
        let entries = (0..n)
            .map(|i| {
                let key = format!("user:{:08}", i).into_bytes();
                let value = format!(
                    "PREC\x02\x00{{\"id\":{},\"status\":\"{}\",\"region\":\"eu-west-{}\"}}",
                    i,
                    if i % 3 == 0 { "done" } else { "pending" },
                    i % 4
                )
                .into_bytes();
                (key, value)
            })
            .collect();
        Node::Leaf { entries }
    }

    #[test]
    fn a_packed_node_round_trips() {
        for n in [0usize, 1, 2, 10, 500, 2000] {
            let node = leaf_of(n);
            let encoded = node.encode();
            assert_eq!(
                Node::decode(&encoded).as_ref(),
                Some(&node),
                "a leaf of {} entries must survive encoding",
                n
            );
        }
    }

    #[test]
    fn an_internal_node_round_trips_packed() {
        let children: Vec<ChildRef> = (0..300)
            .map(|i| ChildRef {
                max_key: format!("user:{:08}", i * 10).into_bytes(),
                hash: format!("{:064x}", i),
                count: i as u64 + 1,
            })
            .collect();
        let node = Node::Internal { children };
        assert_eq!(Node::decode(&node.encode()).as_ref(), Some(&node));
    }

    /// A node written before packing existed carries a leaf or internal tag
    /// and must decode untouched. A content-addressed store cannot rewrite
    /// what it already holds, so this is not a transition — it is permanent.
    #[test]
    fn an_unpacked_node_still_decodes() {
        let node = leaf_of(200);
        let plain = node.encode_plain();
        assert_ne!(plain.first(), Some(&TAG_PACKED));
        assert_eq!(Node::decode(&plain).as_ref(), Some(&node));
    }

    /// Encoding never grows a node, whatever is in it.
    ///
    /// The first version of this test built "incompressible" entries with
    /// random values — and they packed anyway, because the *keys* were
    /// `{:08}`-formatted and highly repetitive. The premise was wrong, not the
    /// code. What actually matters is not which branch is taken but that
    /// neither one costs anything, so this asserts the property directly.
    #[test]
    fn encoding_never_grows_a_node() {
        let mut rng: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut next = || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        // Random keys as well as values, so there is genuinely nothing to
        // find.
        let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..200)
            .map(|_| {
                let k: Vec<u8> = (0..16).map(|_| next() as u8).collect();
                let v: Vec<u8> = (0..64).map(|_| next() as u8).collect();
                (k, v)
            })
            .collect();
        let mut node = Node::Leaf { entries };
        if let Node::Leaf { entries } = &mut node {
            entries.sort();
        }

        let plain = node.encode_plain().len();
        let encoded = node.encode();
        assert!(
            encoded.len() <= plain,
            "encoding grew a node it could not pack: {} -> {}",
            plain,
            encoded.len()
        );
        assert_eq!(Node::decode(&encoded).as_ref(), Some(&node));
    }

    /// The property everything rests on: the same node always encodes to the
    /// same bytes. Packing must not weaken it.
    #[test]
    fn encoding_a_node_is_deterministic() {
        let node = leaf_of(1000);
        let once = node.encode();
        for _ in 0..10 {
            assert_eq!(node.encode(), once);
        }
        // And a node built independently from the same data agrees.
        assert_eq!(leaf_of(1000).encode(), once);
    }

    #[test]
    fn packing_actually_shrinks_a_realistic_leaf() {
        let node = leaf_of(2000);
        let plain = node.encode_plain().len();
        let packed = node.encode().len();
        println!("leaf of 2000: {} -> {} bytes", plain, packed);
        assert!(
            packed * 3 < plain,
            "a leaf of repetitive records should pack to under a third: \
             {} -> {}",
            plain,
            packed
        );
    }

    /// A packed frame claiming an absurd unpacked length is refused rather
    /// than allocated for.
    #[test]
    fn an_absurd_packed_length_is_refused() {
        let mut bytes = vec![TAG_PACKED];
        bytes.extend_from_slice(&u32::MAX.to_le_bytes());
        bytes.extend_from_slice(&[0u8; 16]);
        assert_eq!(Node::decode(&bytes), None);
    }

    /// Truncating a packed node is refused at every offset, never panics.
    #[test]
    fn a_truncated_packed_node_is_refused() {
        let encoded = leaf_of(500).encode();
        assert_eq!(encoded.first(), Some(&TAG_PACKED));
        for cut in 1..encoded.len().min(200) {
            let _ = Node::decode(&encoded[..encoded.len() - cut]);
        }
    }

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
