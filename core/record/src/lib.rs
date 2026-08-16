// pond_record — the universal record model.
//
// Every collection in Pond, for every workload, is a sorted map from a
// composite key to a record: a sparse set of typed, named fields.
//
//     (k1, k2, ...) -> { field: typed_value }
//
// That single shape is what makes lenses interchangeable. A lens is an
// *interpretation* of records, not an encoding of its own: a lakehouse lens
// reads `(pk) -> columns` as a table, a streaming lens reads
// `(topic, partition, offset) -> payload` as a log, a KV lens reads
// `(key) -> value` as a map. They are all looking at the same records, so any
// lens can read — and write — any collection. A collection whose key shape
// does not match the lens renders awkwardly, never inaccessibly.
//
// # Two rules make that safe
//
// **Unknown fields are never dropped.** A lens that does not understand a
// field must carry it through reads, writes, and merges untouched. Without
// this, opening a collection with the "wrong" lens silently destroys data.
// This is not hypothetical: the current branch merge round-trips through JSON
// and handles four types, so merging a table with a BINARY or VECTOR column
// deletes that column.
//
// **Merge is per-field, not per-record.** Two writers editing different fields
// of the same record must both win. Whole-record last-writer-wins throws away
// one of them for no reason, and that is the common case in a system where
// several lenses and several users share a collection.
//
// # Convergence
//
// Field versions are `(physical_ms, logical, writer_id)`, a total order. That
// makes the per-field merge a join over a semilattice — commutative,
// associative, idempotent — so any two replicas that have seen the same writes
// hold byte-identical records regardless of the order they arrived in. The
// `writer_id` is load-bearing: without it two nodes ticking in the same
// millisecond produce equal versions, the tie is unbreakable, and merge stops
// being commutative.

use std::collections::BTreeMap;

pub mod encode;
pub mod head;

pub use encode::{decode_record, encode_record};
pub use head::{decode_head, encode_head, Head};

/// A typed field value.
///
/// Deliberately small and closed: these are the types every workload needs,
/// and a lens that wants something richer encodes it into `Bytes` or `Json`
/// rather than growing this enum. `Vector` is separate from `Bytes` because
/// vector search needs to know the element layout without a schema lookup.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    F64(f64),
    Str(String),
    Bytes(Vec<u8>),
    /// Fixed-width f32 vector, for embeddings.
    Vector(Vec<f32>),
    /// Semi-structured payload, stored verbatim. The lens that wrote it knows
    /// how to read it; every other lens carries it through untouched.
    Json(String),
}

impl Value {
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::Null => "null",
            Value::Bool(_) => "bool",
            Value::Int(_) => "int",
            Value::F64(_) => "f64",
            Value::Str(_) => "str",
            Value::Bytes(_) => "bytes",
            Value::Vector(_) => "vector",
            Value::Json(_) => "json",
        }
    }
}

/// A totally-ordered version stamp for one field write.
///
/// `writer` is what makes the order total. Two nodes writing in the same
/// millisecond with the same logical counter would otherwise be
/// indistinguishable, leaving last-writer-wins with a tie it can only break by
/// iteration order — which is not the same on every replica.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Version {
    pub physical: u64,
    pub logical: u64,
    pub writer: u64,
}

impl Version {
    pub fn new(physical: u64, logical: u64, writer: u64) -> Self {
        Self {
            physical,
            logical,
            writer,
        }
    }

    /// Parse the hex form produced by `pond_kernel::crdt::HLC::tick`.
    ///
    /// Accepts both the current 48-char form and the legacy 32-char form
    /// (no writer id), so records written before writer ids existed still
    /// order correctly — they simply sort before any writer-tagged value at
    /// the same instant.
    pub fn parse(s: &str) -> Option<Version> {
        if s.len() != 48 && s.len() != 32 {
            return None;
        }
        let physical = u64::from_str_radix(&s[0..16], 16).ok()?;
        let logical = u64::from_str_radix(&s[16..32], 16).ok()?;
        let writer = if s.len() == 48 {
            u64::from_str_radix(&s[32..48], 16).ok()?
        } else {
            0
        };
        Some(Version {
            physical,
            logical,
            writer,
        })
    }

    pub fn encode(&self) -> String {
        format!(
            "{:016x}{:016x}{:016x}",
            self.physical, self.logical, self.writer
        )
    }
}

/// A field: its value and the version at which it was written.
#[derive(Debug, Clone, PartialEq)]
pub struct Field {
    pub value: Value,
    pub version: Version,
}

impl Field {
    pub fn new(value: Value, version: Version) -> Self {
        Self { value, version }
    }
}

/// A record: a sparse, ordered set of named fields.
///
/// `BTreeMap` rather than `HashMap` so iteration order is deterministic —
/// encoding must be canonical for two replicas that agree on content to agree
/// on bytes, and therefore on the content hash.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Record {
    pub fields: BTreeMap<String, Field>,
    /// Tombstone: the record is deleted as of this version.
    ///
    /// Deletion has to be a versioned fact rather than an absence, otherwise a
    /// delete and a concurrent update cannot be ordered against each other and
    /// the row resurrects on merge.
    pub deleted: Option<Version>,
}

impl Record {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_field(mut self, name: &str, value: Value, version: Version) -> Self {
        self.fields
            .insert(name.to_string(), Field::new(value, version));
        self
    }

    pub fn get(&self, name: &str) -> Option<&Value> {
        self.fields.get(name).map(|f| &f.value)
    }

    pub fn set(&mut self, name: &str, value: Value, version: Version) {
        self.fields
            .insert(name.to_string(), Field::new(value, version));
    }

    /// Mark the record deleted at `version`.
    ///
    /// Fields are retained, not cleared: a later write to one field must be
    /// able to win against this tombstone, and merge needs the field versions
    /// to decide that.
    pub fn delete(&mut self, version: Version) {
        self.deleted = Some(version);
    }

    /// Is this record visible to readers?
    ///
    /// A tombstone hides the record only while it is newer than every field.
    /// A field written after the delete resurrects the record — which is the
    /// correct outcome for "delete, then update" arriving out of order.
    pub fn is_visible(&self) -> bool {
        match self.deleted {
            None => true,
            Some(tomb) => self.fields.values().any(|f| f.version > tomb),
        }
    }

    pub fn field_names(&self) -> impl Iterator<Item = &String> {
        self.fields.keys()
    }

    pub fn is_empty(&self) -> bool {
        self.fields.is_empty() && self.deleted.is_none()
    }
}

/// Merge two versions of a record, field by field.
///
/// For each field present in either side, the higher version wins; fields
/// present in only one side are carried through untouched. That last clause is
/// the never-drop-unknown-fields law at the merge layer: a writer that has
/// never heard of a field cannot delete it by omission.
///
/// The result is a semilattice join: commutative, associative, and idempotent,
/// because `Version` is a total order and `max` over a total order has all
/// three properties. Two replicas that have seen the same writes therefore
/// hold byte-identical records — which is what lets any node compact, and
/// lets two stores converge after a plain file copy.
pub fn merge_records(a: &Record, b: &Record) -> Record {
    let mut out = Record::new();

    for (name, field) in &a.fields {
        out.fields.insert(name.clone(), field.clone());
    }
    for (name, field) in &b.fields {
        match out.fields.get(name) {
            // Strictly greater, so equal versions keep the value already
            // present. Equal versions can only mean the same write, since
            // `writer` makes the order total.
            Some(existing) if existing.version >= field.version => {}
            _ => {
                out.fields.insert(name.clone(), field.clone());
            }
        }
    }

    out.deleted = match (a.deleted, b.deleted) {
        (Some(x), Some(y)) => Some(x.max(y)),
        (Some(x), None) => Some(x),
        (None, Some(y)) => Some(y),
        (None, None) => None,
    };

    out
}

/// Merge many records in any order.
pub fn merge_all<'a>(records: impl IntoIterator<Item = &'a Record>) -> Record {
    records
        .into_iter()
        .fold(Record::new(), |acc, r| merge_records(&acc, r))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(physical: u64, writer: u64) -> Version {
        Version::new(physical, 0, writer)
    }

    // -----------------------------------------------------------------------
    // Per-field merge
    // -----------------------------------------------------------------------

    /// Two writers editing *different* fields of the same record must both
    /// win. Whole-record last-writer-wins throws one away for no reason, and
    /// this is the common case when several users or lenses share a
    /// collection.
    #[test]
    fn test_concurrent_edits_to_different_fields_both_survive() {
        let alice = Record::new()
            .with_field("name", Value::Str("alice".into()), v(100, 1))
            .with_field("email", Value::Str("a@x.com".into()), v(100, 1));

        // Two writers, same millisecond, different fields.
        let w1 = alice
            .clone()
            .with_field("email", Value::Str("new@x.com".into()), v(200, 1));
        let w2 = alice
            .clone()
            .with_field("name", Value::Str("alicia".into()), v(200, 2));

        let merged = merge_records(&w1, &w2);
        assert_eq!(merged.get("email"), Some(&Value::Str("new@x.com".into())));
        assert_eq!(merged.get("name"), Some(&Value::Str("alicia".into())));
    }

    /// On the *same* field, the higher version wins deterministically.
    #[test]
    fn test_same_field_resolves_by_version() {
        let older = Record::new().with_field("x", Value::Int(1), v(100, 1));
        let newer = Record::new().with_field("x", Value::Int(2), v(200, 1));
        assert_eq!(merge_records(&older, &newer).get("x"), Some(&Value::Int(2)));
        assert_eq!(merge_records(&newer, &older).get("x"), Some(&Value::Int(2)));
    }

    /// Same instant, different writers: the tie breaks on writer id, and it
    /// breaks the same way on every replica. Without the writer id this case
    /// is unresolvable and merge stops being commutative.
    #[test]
    fn test_same_millisecond_different_writers_is_deterministic() {
        let w1 = Record::new().with_field("x", Value::Int(1), v(100, 1));
        let w2 = Record::new().with_field("x", Value::Int(2), v(100, 2));
        let ab = merge_records(&w1, &w2);
        let ba = merge_records(&w2, &w1);
        assert_eq!(ab, ba, "merge must be commutative even on a version tie");
        assert_eq!(ab.get("x"), Some(&Value::Int(2)), "higher writer id wins");
    }

    // -----------------------------------------------------------------------
    // Semilattice properties
    // -----------------------------------------------------------------------

    fn sample_records() -> (Record, Record, Record) {
        let a = Record::new()
            .with_field("a", Value::Int(1), v(100, 1))
            .with_field("shared", Value::Str("from-a".into()), v(150, 1));
        let b = Record::new()
            .with_field("b", Value::Bool(true), v(120, 2))
            .with_field("shared", Value::Str("from-b".into()), v(160, 2));
        let c = Record::new()
            .with_field("c", Value::Bytes(vec![1, 2, 3]), v(130, 3))
            .with_field("shared", Value::Str("from-c".into()), v(140, 3));
        (a, b, c)
    }

    #[test]
    fn test_merge_is_commutative() {
        let (a, b, _) = sample_records();
        assert_eq!(merge_records(&a, &b), merge_records(&b, &a));
    }

    #[test]
    fn test_merge_is_associative() {
        let (a, b, c) = sample_records();
        let left = merge_records(&merge_records(&a, &b), &c);
        let right = merge_records(&a, &merge_records(&b, &c));
        assert_eq!(left, right);
    }

    #[test]
    fn test_merge_is_idempotent() {
        let (a, b, _) = sample_records();
        let m = merge_records(&a, &b);
        assert_eq!(merge_records(&m, &m), m);
        assert_eq!(merge_records(&m, &a), m);
    }

    /// Any arrival order of the same writes converges on one record.
    #[test]
    fn test_all_orderings_converge() {
        let (a, b, c) = sample_records();
        let orders = [
            [&a, &b, &c],
            [&a, &c, &b],
            [&b, &a, &c],
            [&b, &c, &a],
            [&c, &a, &b],
            [&c, &b, &a],
        ];
        let results: Vec<Record> = orders.iter().map(|o| merge_all(o.iter().copied())).collect();
        for r in &results[1..] {
            assert_eq!(r, &results[0], "all arrival orders must converge");
        }
        // The highest-versioned writer wins the contested field.
        assert_eq!(
            results[0].get("shared"),
            Some(&Value::Str("from-b".into()))
        );
    }

    // -----------------------------------------------------------------------
    // The never-drop law
    // -----------------------------------------------------------------------

    /// A writer that has never heard of a field must not delete it by
    /// omission. This is the law that makes bidirectional lens access safe,
    /// and it is exactly what today's branch merge violates — it round-trips
    /// through JSON and handles four types, so BINARY and VECTOR columns are
    /// silently deleted by a merge.
    #[test]
    fn test_unknown_fields_survive_merge() {
        let full = Record::new()
            .with_field("id", Value::Int(1), v(100, 1))
            .with_field("embedding", Value::Vector(vec![0.1, 0.2, 0.3]), v(100, 1))
            .with_field("thumbnail", Value::Bytes(vec![0xDE, 0xAD]), v(100, 1))
            .with_field("meta", Value::Json(r#"{"k":1}"#.into()), v(100, 1));

        // A lens that only understands `id` writes an update.
        let naive = Record::new().with_field("id", Value::Int(2), v(200, 2));

        let merged = merge_records(&full, &naive);
        assert_eq!(merged.get("id"), Some(&Value::Int(2)), "known field updates");
        assert_eq!(
            merged.get("embedding"),
            Some(&Value::Vector(vec![0.1, 0.2, 0.3])),
            "VECTOR field must survive a lens that does not understand it"
        );
        assert_eq!(
            merged.get("thumbnail"),
            Some(&Value::Bytes(vec![0xDE, 0xAD])),
            "BINARY field must survive"
        );
        assert!(merged.get("meta").is_some(), "JSON field must survive");
    }

    /// Every value type survives an encode/decode round trip — the other half
    /// of the never-drop law, at the storage boundary rather than the merge.
    #[test]
    fn test_all_value_types_survive_roundtrip() {
        let r = Record::new()
            .with_field("null", Value::Null, v(1, 1))
            .with_field("bool", Value::Bool(true), v(1, 1))
            .with_field("int", Value::Int(-42), v(1, 1))
            .with_field("f64", Value::F64(1.5), v(1, 1))
            .with_field("str", Value::Str("hello".into()), v(1, 1))
            .with_field("bytes", Value::Bytes(vec![0, 255, 0]), v(1, 1))
            .with_field("vector", Value::Vector(vec![1.0, -2.5]), v(1, 1))
            .with_field("json", Value::Json(r#"{"a":[1]}"#.into()), v(1, 1));

        let decoded = decode_record(&encode_record(&r)).expect("must decode");
        assert_eq!(decoded, r);
    }

    // -----------------------------------------------------------------------
    // Deletion
    // -----------------------------------------------------------------------

    #[test]
    fn test_tombstone_hides_record() {
        let mut r = Record::new().with_field("x", Value::Int(1), v(100, 1));
        assert!(r.is_visible());
        r.delete(v(200, 1));
        assert!(!r.is_visible());
    }

    /// A field written after the tombstone resurrects the record. That is the
    /// right answer for "delete, then update" arriving out of order — the
    /// later write is the more recent intent.
    #[test]
    fn test_later_write_beats_tombstone() {
        let mut deleted = Record::new().with_field("x", Value::Int(1), v(100, 1));
        deleted.delete(v(200, 1));

        let update = Record::new().with_field("x", Value::Int(2), v(300, 2));
        let merged = merge_records(&deleted, &update);

        assert!(merged.is_visible(), "a write after the delete must win");
        assert_eq!(merged.get("x"), Some(&Value::Int(2)));
    }

    /// A tombstone newer than every field keeps the record hidden regardless
    /// of merge order.
    #[test]
    fn test_tombstone_merge_is_order_independent() {
        let live = Record::new().with_field("x", Value::Int(1), v(100, 1));
        let mut tomb = Record::new();
        tomb.delete(v(200, 1));

        let ab = merge_records(&live, &tomb);
        let ba = merge_records(&tomb, &live);
        assert_eq!(ab, ba);
        assert!(!ab.is_visible());
    }

    // -----------------------------------------------------------------------
    // Version ordering
    // -----------------------------------------------------------------------

    #[test]
    fn test_version_ordering_is_total() {
        let a = Version::new(100, 0, 1);
        let b = Version::new(100, 0, 2);
        let c = Version::new(100, 1, 0);
        let d = Version::new(200, 0, 0);
        assert!(a < b, "same instant: writer id breaks the tie");
        assert!(b < c, "logical counter outranks writer id");
        assert!(c < d, "physical clock outranks everything");
    }

    #[test]
    fn test_version_hex_roundtrip_and_legacy() {
        let v = Version::new(0x1234, 0x5, 0xABCD);
        assert_eq!(Version::parse(&v.encode()), Some(v));

        // Legacy 32-char values (written before writer ids) parse with
        // writer 0, so they sort before any writer-tagged value at the same
        // instant — deterministic, and never silently mis-ordered.
        let legacy = format!("{:016x}{:016x}", 0x1234, 0x5);
        let parsed = Version::parse(&legacy).expect("legacy must parse");
        assert_eq!(parsed.writer, 0);
        assert!(parsed < v);

        assert!(Version::parse("nonsense").is_none());
    }

    /// Versions produced by the kernel's HLC must parse — the two halves have
    /// to agree on the wire format or merge silently degrades to "unversioned".
    #[test]
    fn test_parses_kernel_hlc_output() {
        let mut clock = pond_kernel::crdt::HLC::with_writer_id(0xFEED);
        let stamp = clock.tick();
        let parsed = Version::parse(&stamp).expect("kernel HLC output must parse");
        assert_eq!(parsed.writer, 0xFEED);
    }
}
