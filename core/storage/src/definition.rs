// definition.rs — what a collection *is*, as opposed to what it contains.
//
// Until now `definition_ref` existed and nothing wrote to it. That was fine
// while there was exactly one storage format, and stops being fine the moment
// there are two: a reader has to know which one it is looking at before it can
// read anything, and it must not have to guess from the bytes.
//
// The definition is deliberately tiny and read once per collection open, so it
// is stored as named bytes rather than as a name pointing at a blob — one
// round trip instead of two. See `ObjectStore::put_object`.
//
// It carries two things:
//
//   1. **The format.** Collections written before this existed have no
//      definition object at all, and the absence *is* the answer: legacy.
//      Nothing has to be migrated for old data to keep reading correctly.
//
//   2. **The column types.** The engine stores records — name/value pairs —
//      and several distinct column types collapse onto the same value
//      representation (a date, a timestamp and an integer are all i64 on the
//      wire). Without the declared type, a round trip through the engine would
//      silently turn a timestamp column into an integer column. The schema is
//      what makes the translation lossless, which is the property the whole
//      cutover rests on: both paths must agree, or the dispatch is a bug
//      generator rather than a migration.

use pond_kernel::PondKernel;

/// A fresh salt for a new collection.
fn random_salt() -> u64 {
    let mut b = [0u8; 8];
    getrandom::fill(&mut b).expect("OS CSPRNG unavailable — cannot salt a new collection");
    u64::from_le_bytes(b)
}

use crate::definition_ref;

/// Which storage path owns a collection's data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    /// PND2 blobs plus a `CollectionManifest`, reached through commit refs.
    /// Everything written before the engine existed.
    Legacy,
    /// Content-defined index over records, published as one head object.
    Engine,
}

impl Format {
    fn tag(self) -> u8 {
        match self {
            Format::Legacy => 1,
            Format::Engine => 2,
        }
    }

    fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            1 => Some(Format::Legacy),
            2 => Some(Format::Engine),
            _ => None,
        }
    }
}

/// A collection's format and declared column types.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Definition {
    pub format: Format,
    /// Target entries per index chunk, pinned at creation.
    ///
    /// This participates in content addressing: it decides where chunk
    /// boundaries fall, so it decides every node hash and therefore the root.
    /// It has to be stored rather than read from a constant, because a later
    /// change to that constant would rechunk an existing collection on its
    /// next insert — producing a tree that is still correct but no longer
    /// byte-identical to a rebuild, silently losing the structural sharing and
    /// the deterministic-merge property that the whole design rests on.
    ///
    /// Pinned per collection, so the default can be tuned for new collections
    /// without touching a single existing one.
    pub chunk_target: u32,
    /// Per-collection value mixed into the chunk boundary decision.
    ///
    /// Chosen randomly at creation. Without it the boundary function is public
    /// and fixed, so one search for keys that land on boundaries produces a
    /// key set that degrades every collection everywhere. With it, the search
    /// has to be redone per collection against a value the attacker must first
    /// obtain — which an append-only client cannot.
    ///
    /// Zero means unsalted, which is exactly what pre-salt collections were,
    /// so they keep chunking identically.
    pub chunk_salt: u64,
    /// Values at or above this size are spilled to their own object.
    ///
    /// Pinned for the same reason as the chunk target: it decides whether a
    /// value goes into a leaf or is replaced by a pointer to it, so two
    /// writers using different thresholds produce different index bytes for
    /// identical data — and stop converging.
    pub spill_threshold: u32,
    /// Column naming the subject each row's data belongs to, if this
    /// collection holds personal data.
    ///
    /// Setting it turns on per-subject encryption: every other column is
    /// sealed under a key belonging to the subject named here, so erasing that
    /// subject makes their values unreadable everywhere at once — in every
    /// branch, every historical root, and every replica that already copied
    /// them. See `docs/ERASURE.md`.
    ///
    /// The subject column itself stays in the clear. It is an identifier
    /// rather than personal detail, and sealing it would leave no way to tell
    /// which rows belong to whom — including for the erasure itself.
    ///
    /// `None` means the collection holds no subject data and nothing is
    /// sealed, which is what every collection written before this existed is.
    pub subject_column: Option<String>,
    /// The collection this one was branched from, if it was.
    ///
    /// A branch in the engine model is an independent collection that shares
    /// structure with its source rather than a ref inside one, which is what
    /// makes branching an O(1) pointer copy. The consequence is that nothing
    /// in the store records the relationship — so `pond branch` could report
    /// success while `pond branches` reported none, each telling the truth
    /// about a different model. This is the missing half.
    ///
    /// Provenance only. It confers no behaviour: a branch diverges freely and
    /// is deleted, read and written exactly like any other collection.
    pub branched_from: Option<String>,
    /// `(column name, PND2 value type)`, in declaration order.
    pub columns: Vec<(String, u8)>,
}

const MAGIC: &[u8; 4] = b"PDEF";

/// v1 had no `chunk_target`; it is read back as [`LEGACY_CHUNK_TARGET`].
const VERSION_V1: u8 = 1;
/// v2 predates per-subject encryption; it reads back with no subject column.
const VERSION_V2: u8 = 2;
const VERSION_V3: u8 = 3;
const VERSION: u8 = 4;

/// What v1 definitions were built with, and what they must keep using.
pub const LEGACY_CHUNK_TARGET: u32 = 512;

impl Definition {
    pub fn new(format: Format) -> Self {
        Self {
            format,
            chunk_target: pond_index::DEFAULT_TARGET_ENTRIES,
            chunk_salt: random_salt(),
            spill_threshold: pond_engine::SPILL_THRESHOLD as u32,
            subject_column: None,
            branched_from: None,
            columns: Vec::new(),
        }
    }

    pub fn with_columns(format: Format, columns: Vec<(String, u8)>) -> Self {
        Self {
            format,
            chunk_target: pond_index::DEFAULT_TARGET_ENTRIES,
            chunk_salt: random_salt(),
            spill_threshold: pond_engine::SPILL_THRESHOLD as u32,
            subject_column: None,
            branched_from: None,
            columns,
        }
    }

    /// The chunk configuration this collection was created with.
    pub fn chunk_config(&self) -> pond_index::ChunkConfig {
        pond_index::ChunkConfig::with_target(self.chunk_target).with_salt(self.chunk_salt)
    }

    /// The full engine configuration this collection was created with.
    pub fn engine_config(&self) -> pond_engine::EngineConfig {
        pond_engine::EngineConfig::default()
            .with_chunk(self.chunk_config())
            .with_spill_threshold(self.spill_threshold as usize)
    }

    /// Declared type of a column, if the definition names it.
    pub fn column_type(&self, name: &str) -> Option<u8> {
        self.columns
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, t)| *t)
    }

    /// Merge in any columns not already declared, preserving existing order.
    ///
    /// A write that introduces a column extends the schema rather than
    /// replacing it: a lens that does not know about a column must never cause
    /// that column to be forgotten. This is the same never-drop law the record
    /// merge enforces, one level up.
    /// Returns whether anything was added, so a caller can skip persisting a
    /// definition that has not changed. In steady state a write declares
    /// exactly the columns already on record, and rewriting the definition
    /// every time would put an avoidable object write on the commit path.
    pub fn declare(&mut self, columns: &[(String, u8)]) -> bool {
        let mut changed = false;
        for (name, vtype) in columns {
            if self.column_type(name).is_none() {
                self.columns.push((name.clone(), *vtype));
                changed = true;
            }
        }
        changed
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(16 + self.columns.len() * 24);
        out.extend_from_slice(MAGIC);
        out.push(VERSION);
        out.push(self.format.tag());
        out.extend_from_slice(&self.chunk_target.to_le_bytes());
        out.extend_from_slice(&self.chunk_salt.to_le_bytes());
        out.extend_from_slice(&self.spill_threshold.to_le_bytes());
        let subject = self.subject_column.as_deref().unwrap_or("");
        out.extend_from_slice(&(subject.len() as u32).to_le_bytes());
        out.extend_from_slice(subject.as_bytes());
        let parent = self.branched_from.as_deref().unwrap_or("");
        out.extend_from_slice(&(parent.len() as u32).to_le_bytes());
        out.extend_from_slice(parent.as_bytes());
        out.extend_from_slice(&(self.columns.len() as u32).to_le_bytes());
        for (name, vtype) in &self.columns {
            let bytes = name.as_bytes();
            out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            out.extend_from_slice(bytes);
            out.push(*vtype);
        }
        out
    }

    /// Decode, returning `None` for anything that is not a definition this
    /// version understands. A caller that gets `None` must treat the
    /// collection as legacy rather than assume a format.
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 10 || &bytes[..4] != MAGIC {
            return None;
        }
        let format = Format::from_tag(bytes[5])?;

        // v1 carried no chunk target. Reading it back as the value v1 was
        // built with is what keeps those collections rechunking identically.
        let (chunk_target, chunk_salt, spill_threshold, mut pos) = match bytes[4] {
            // v1 predates spilling entirely: every value was stored inline, so
            // a threshold of "never spill" is what keeps those collections
            // producing the bytes they already contain.
            VERSION_V1 => (LEGACY_CHUNK_TARGET, 0u64, u32::MAX, 6usize),
            VERSION_V2 | VERSION_V3 | VERSION => {
                if bytes.len() < 26 {
                    return None;
                }
                (
                    u32::from_le_bytes(bytes[6..10].try_into().ok()?),
                    u64::from_le_bytes(bytes[10..18].try_into().ok()?),
                    u32::from_le_bytes(bytes[18..22].try_into().ok()?),
                    22usize,
                )
            }
            _ => return None,
        };

        // v2 has no subject column, so it reads back as a collection holding
        // no subject data — which is what it is.
        let subject_column = if bytes[4] >= VERSION_V3 {
            if pos + 4 > bytes.len() {
                return None;
            }
            let len = u32::from_le_bytes(bytes[pos..pos + 4].try_into().ok()?) as usize;
            pos += 4;
            if pos + len > bytes.len() {
                return None;
            }
            let name = String::from_utf8(bytes[pos..pos + len].to_vec()).ok()?;
            pos += len;
            if name.is_empty() {
                None
            } else {
                Some(name)
            }
        } else {
            None
        };
        // v3 has no provenance field, so it reads back as a collection that
        // was not branched — which is all that can honestly be said about it.
        let branched_from = if bytes[4] >= VERSION {
            if pos + 4 > bytes.len() {
                return None;
            }
            let len = u32::from_le_bytes(bytes[pos..pos + 4].try_into().ok()?) as usize;
            pos += 4;
            if pos + len > bytes.len() {
                return None;
            }
            let name = String::from_utf8(bytes[pos..pos + len].to_vec()).ok()?;
            pos += len;
            if name.is_empty() {
                None
            } else {
                Some(name)
            }
        } else {
            None
        };

        if chunk_target == 0 {
            return None;
        }

        if pos + 4 > bytes.len() {
            return None;
        }
        let count = u32::from_le_bytes(bytes[pos..pos + 4].try_into().ok()?) as usize;
        pos += 4;

        let mut columns = Vec::with_capacity(count.min(1024));
        for _ in 0..count {
            if pos + 4 > bytes.len() {
                return None;
            }
            let len = u32::from_le_bytes(bytes[pos..pos + 4].try_into().ok()?) as usize;
            pos += 4;
            if pos + len + 1 > bytes.len() {
                return None;
            }
            let name = String::from_utf8(bytes[pos..pos + len].to_vec()).ok()?;
            pos += len;
            let vtype = bytes[pos];
            pos += 1;
            columns.push((name, vtype));
        }
        Some(Self {
            format,
            chunk_target,
            chunk_salt,
            spill_threshold,
            subject_column,
            branched_from,
            columns,
        })
    }
}

/// Read a collection's definition.
///
/// Absent means legacy, and that is not an error: every collection that
/// existed before this file did has no definition object, and must keep
/// reading exactly as it did.
pub fn load(kernel: &PondKernel, collection: &str) -> Definition {
    kernel
        .read_named(&definition_ref(collection))
        .and_then(|b| Definition::decode(&b))
        .unwrap_or_else(legacy)
}

/// What a collection with no definition object is.
///
/// Deterministic on purpose: `Definition::new` draws a random salt, which is
/// right for a collection being created and wrong for one being described. A
/// legacy collection does not use the index at all, but a value that differs
/// between two reads of the same collection is a bug waiting for a caller.
fn legacy() -> Definition {
    Definition {
        format: Format::Legacy,
        chunk_target: LEGACY_CHUNK_TARGET,
        chunk_salt: 0,
        spill_threshold: u32::MAX,
        subject_column: None,
        branched_from: None,
        columns: Vec::new(),
    }
}

/// Write a collection's definition. One object write.
pub fn store(kernel: &PondKernel, collection: &str, def: &Definition) -> Result<(), String> {
    kernel
        .write_named(&definition_ref(collection), &def.encode())
        .map_err(|e| format!("failed to write collection definition: {}", e))
}

/// Which path owns this collection.
pub fn format_of(kernel: &PondKernel, collection: &str) -> Format {
    load(kernel, collection).format
}

#[cfg(test)]
mod tests {
    use super::*;
    use pond_core::constants::{VT_INT64, VT_STRING, VT_TIMESTAMP};

    #[test]
    fn round_trips() {
        let def = Definition::with_columns(
            Format::Engine,
            vec![
                ("id".to_string(), VT_INT64),
                ("name".to_string(), VT_STRING),
                ("seen_at".to_string(), VT_TIMESTAMP),
            ],
        );
        let decoded = Definition::decode(&def.encode()).expect("decodes");
        assert_eq!(decoded, def);
        assert_eq!(decoded.column_type("seen_at"), Some(VT_TIMESTAMP));
        assert_eq!(decoded.column_type("absent"), None);
    }

    /// A collection with no definition object is legacy. This is the property
    /// that lets the engine land without migrating anything.
    #[test]
    fn absent_definition_means_legacy() {
        let dir = tempfile::tempdir().unwrap();
        let kernel = PondKernel::new_local(dir.path()).unwrap();
        assert_eq!(format_of(&kernel, "never-created"), Format::Legacy);

        // And describing an absent collection is deterministic — two reads of
        // the same collection must not differ.
        let a = load(&kernel, "never-created");
        let b = load(&kernel, "never-created");
        assert_eq!(a, b);
        assert_eq!(a.chunk_salt, 0, "an unsalted collection stays unsalted");
        assert_eq!(a.chunk_target, LEGACY_CHUNK_TARGET);
    }

    /// Two collections must not share a salt, or one mined key set degrades
    /// both.
    #[test]
    fn each_collection_gets_its_own_salt() {
        let salts: std::collections::HashSet<u64> = (0..32)
            .map(|_| Definition::new(Format::Engine).chunk_salt)
            .collect();
        assert!(
            salts.len() > 30,
            "salts must be drawn independently, saw {} distinct of 32",
            salts.len()
        );
        // And the salt reaches the chunker.
        let def = Definition::new(Format::Engine);
        assert_eq!(def.chunk_config().salt, def.chunk_salt);
    }

    #[test]
    fn stored_definition_is_read_back() {
        let dir = tempfile::tempdir().unwrap();
        let kernel = PondKernel::new_local(dir.path()).unwrap();
        let def = Definition::with_columns(Format::Engine, vec![("id".to_string(), VT_INT64)]);
        store(&kernel, "users", &def).unwrap();
        assert_eq!(load(&kernel, "users"), def);
        assert_eq!(format_of(&kernel, "users"), Format::Engine);
    }

    /// Declaring columns must never drop one that is already known.
    #[test]
    fn declare_is_additive() {
        let mut def = Definition::with_columns(Format::Engine, vec![("a".to_string(), VT_INT64)]);
        assert!(def.declare(&[("b".to_string(), VT_STRING)]), "adding is a change");
        assert!(
            !def.declare(&[("a".to_string(), VT_STRING)]),
            "re-declaring a known column changes nothing, so nothing needs writing"
        );
        assert_eq!(
            def.columns,
            vec![("a".to_string(), VT_INT64), ("b".to_string(), VT_STRING)]
        );
    }

    /// Garbage must not be mistaken for a definition.
    #[test]
    fn rejects_foreign_bytes() {
        assert!(Definition::decode(b"").is_none());
        assert!(Definition::decode(b"not a definition").is_none());
        // Truncated mid-column.
        let mut bytes = Definition::with_columns(Format::Engine, vec![("id".into(), VT_INT64)])
            .encode();
        bytes.truncate(bytes.len() - 1);
        assert!(Definition::decode(&bytes).is_none());
    }

    /// A v3 definition — written before provenance existed — must still
    /// decode, with everything else intact.
    ///
    /// Built by hand rather than by an old encoder, because no old encoder
    /// remains in the tree and a compatibility claim nothing exercises is a
    /// guess. This is the byte layout v3 actually produced.
    #[test]
    fn a_v3_definition_still_decodes() {
        let mut bytes = Vec::from(MAGIC);
        bytes.push(VERSION_V3);
        bytes.push(Format::Engine.tag());
        bytes.extend_from_slice(&2048u32.to_le_bytes());
        bytes.extend_from_slice(&0xDEADBEEFu64.to_le_bytes());
        bytes.extend_from_slice(&4096u32.to_le_bytes());
        bytes.extend_from_slice(&5u32.to_le_bytes());
        bytes.extend_from_slice(b"owner");
        bytes.extend_from_slice(&1u32.to_le_bytes());
        bytes.extend_from_slice(&2u32.to_le_bytes());
        bytes.extend_from_slice(b"id");
        bytes.push(1u8);

        let d = Definition::decode(&bytes).expect("a v3 definition must decode");
        assert_eq!(d.format, Format::Engine);
        assert_eq!(d.chunk_target, 2048);
        assert_eq!(d.chunk_salt, 0xDEADBEEF);
        assert_eq!(d.spill_threshold, 4096, "v3 keeps the threshold it was made with");
        assert_eq!(d.subject_column.as_deref(), Some("owner"));
        assert_eq!(d.columns, vec![("id".to_string(), 1u8)]);
        assert_eq!(
            d.branched_from, None,
            "v3 recorded no provenance, so the honest reading is 'not branched'"
        );
    }

    #[test]
    fn provenance_survives_a_round_trip() {
        let mut d = Definition::new(Format::Engine);
        d.branched_from = Some("trunk".to_string());
        let back = Definition::decode(&d.encode()).unwrap();
        assert_eq!(back.branched_from.as_deref(), Some("trunk"));
        assert_eq!(back, d, "nothing else may shift when provenance is set");
    }

    #[test]
    fn a_collection_that_was_not_branched_says_so() {
        let d = Definition::new(Format::Engine);
        assert_eq!(d.branched_from, None);
        let back = Definition::decode(&d.encode()).unwrap();
        assert_eq!(back.branched_from, None);
    }

    /// An empty name must not read back as `Some("")`, which would make a
    /// collection claim to be branched from one with no name.
    #[test]
    fn an_empty_provenance_name_is_none_not_empty() {
        let mut d = Definition::new(Format::Engine);
        d.branched_from = Some(String::new());
        assert_eq!(Definition::decode(&d.encode()).unwrap().branched_from, None);
    }

    /// Truncation anywhere in the new field must be refused, not guessed at.
    #[test]
    fn a_truncated_provenance_field_is_rejected() {
        let mut d = Definition::new(Format::Engine);
        d.branched_from = Some("trunk".to_string());
        let full = d.encode();
        for cut in 1..=8 {
            let bytes = &full[..full.len().saturating_sub(cut)];
            assert!(
                Definition::decode(bytes).is_none(),
                "a definition truncated by {} bytes must be refused",
                cut
            );
        }
    }
}
