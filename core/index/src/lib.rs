// pond_index — a content-defined Merkle search tree over segments.
//
// This crate is the Phase 1 validation of Pond's storage design. It exists to
// answer four measurable questions before any of it is wired into the storage
// layer:
//
//   1. Is the index history-independent? (same data => same root hash, always)
//   2. Is merge deterministic and commutative?
//   3. Does lookup stay constant-depth as the collection grows?
//   4. What does a small write actually cost in nodes rewritten?
//
// Question 4 is the one that can kill the design, so it is measured, not
// assumed — see `tests/acceptance.rs`.
//
// # The shape
//
//   Backend      put / get(range) / list / delete   — the four operations
//                every store has: local FS, S3, R2, GCS, memory. No
//                conditional writes, no append, no rename.
//        |
//   Index        this crate: sorted map from an order-preserving composite
//                key to a value (in production: a segment locator), stored as
//                a content-defined, content-addressed Merkle tree.
//        |
//   Records      (k1, k2, ...) -> { field: typed_value }
//        |
//   Lenses       pure interpretation — a lens reads records, it does not own
//                an encoding, which is what lets any lens read any collection.
//
// # Why content-defined chunking
//
// Chunk boundaries are chosen by hashing each entry's own content, never by
// position or insertion order. That makes the tree a pure function of its
// contents, which in turn gives convergence without coordination: two writers
// who arrive at the same data arrive at the same bytes.
//
// An earlier prototype in this repo documented exactly this and then chunked
// by fixed position, so it had none of these properties — which is why the
// approach looked like it had been tried and found wanting when it had not.
// Worth knowing before re-litigating it.

pub mod chunk;
pub mod key;
pub mod node;
pub mod store;
pub mod tree;

pub mod pack;
pub use chunk::{fingerprint, min_entries_for, ChunkConfig, DEFAULT_TARGET_ENTRIES};
pub use key::{bool_, bytes, f64_, int, str_, Key, KeyPart};
pub use node::{ChildRef, Node};
pub use store::{hash_bytes, CachingStore, Hash, MemStore, NodeStore};
pub use tree::{get_from_roots, Diff, Tree};
