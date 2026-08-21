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
    /// `(column name, PND2 value type)`, in declaration order.
    pub columns: Vec<(String, u8)>,
}

const MAGIC: &[u8; 4] = b"PDEF";

/// v1 had no `chunk_target`; it is read back as [`LEGACY_CHUNK_TARGET`].
const VERSION_V1: u8 = 1;
const VERSION: u8 = 2;

/// What v1 definitions were built with, and what they must keep using.
pub const LEGACY_CHUNK_TARGET: u32 = 512;

impl Definition {
    pub fn new(format: Format) -> Self {
        Self {
            format,
            chunk_target: pond_index::DEFAULT_TARGET_ENTRIES,
            chunk_salt: random_salt(),
            columns: Vec::new(),
        }
    }

    pub fn with_columns(format: Format, columns: Vec<(String, u8)>) -> Self {
        Self {
            format,
            chunk_target: pond_index::DEFAULT_TARGET_ENTRIES,
            chunk_salt: random_salt(),
            columns,
        }
    }

    /// The chunk configuration this collection was created with.
    pub fn chunk_config(&self) -> pond_index::ChunkConfig {
        pond_index::ChunkConfig::with_target(self.chunk_target).with_salt(self.chunk_salt)
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
        let (chunk_target, chunk_salt, mut pos) = match bytes[4] {
            VERSION_V1 => (LEGACY_CHUNK_TARGET, 0u64, 6usize),
            VERSION => {
                if bytes.len() < 22 {
                    return None;
                }
                (
                    u32::from_le_bytes(bytes[6..10].try_into().ok()?),
                    u64::from_le_bytes(bytes[10..18].try_into().ok()?),
                    18usize,
                )
            }
            _ => return None,
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
}
