// engine_path.rs — the collection paths that go through `core/engine`.
//
// This is the cutover. Until this file existed, `core/index`, `core/record`
// and `core/cache` were consumed only by benchmarks: validated, measured, and
// reachable by nothing that ships. Here they become the storage for any
// collection whose definition says so, while every collection written before
// keeps its old path byte for byte.
//
// The dispatch rule is deliberately one-way and absence-based: a collection
// with no definition object is legacy. Nothing has to be migrated, nothing has
// to be rewritten, and a reader that has never heard of the engine still reads
// every collection that predates it.
//
// What the engine buys, measured elsewhere in this repo:
//
//   - a commit is one object write instead of eight round trips across three
//     refs, and is therefore atomic rather than merely usually-consistent;
//   - lookups cost 2–3 GETs at any size, instead of a manifest scan that grows
//     with the collection;
//   - two writers converge with no coordination, because the tree is
//     content-defined and the merge is a set operation over hashes.

use pond_core::encode::TypedColumn;
use pond_engine::{Engine, Reader};
use pond_kernel::{ObjectStore, PondKernel};

use crate::columnar::{
    columns_to_records_with_nulls, records_to_columns_with_nulls, schema_of,
};
use crate::definition::{self, Definition, Format};

/// Errors are strings here to match the rest of `core/storage`'s API.
type Result<T> = std::result::Result<T, String>;

/// Mark a collection as engine-backed. Idempotent.
///
/// Creating the definition is what routes every later call; there is no
/// separate registry, and no way for the marker and the data to disagree,
/// because the marker is what decides where the data goes.
/// Create a collection whose rows belong to subjects, and seal them.
///
/// `subject_column` names the column holding each row's subject id. Every
/// other column is then sealed under that subject's key, so erasing the
/// subject makes their values unreadable everywhere at once. See
/// `docs/ERASURE.md`.
///
/// Refuses to turn sealing on for a collection that already holds rows: those
/// rows were written in the clear and would stay that way, so the collection
/// would be partly protected while reporting that it is protected. Refuses to
/// change the subject column for the same reason — existing rows would be
/// sealed under subjects nothing could name, and therefore never erasable.
pub fn create_for_subjects(
    kernel: &PondKernel,
    collection: &str,
    subject_column: &str,
) -> Result<()> {
    create(kernel, collection)?;
    let mut def = definition::load(kernel, collection);
    if def.subject_column.as_deref() == Some(subject_column) {
        return Ok(());
    }
    if def.subject_column.is_some() {
        return Err(format!(
            "collection '{}' already seals rows by column '{}'; changing it \
             would leave existing rows sealed under subjects nobody can name",
            collection,
            def.subject_column.as_deref().unwrap_or("")
        ));
    }
    if !def.columns.is_empty() {
        return Err(format!(
            "collection '{}' already holds rows written in the clear; turning \
             on sealing now would protect only what comes after",
            collection
        ));
    }
    def.subject_column = Some(subject_column.to_string());
    definition::store(kernel, collection, &def)
}

pub fn create(kernel: &PondKernel, collection: &str) -> Result<()> {
    if definition::load(kernel, collection).format == Format::Engine {
        return Ok(());
    }
    // Refuse to reformat a collection that already holds legacy data. The
    // definition alone cannot tell us — a legacy collection has none, which is
    // exactly how it is recognised — so the question has to be asked of the
    // data: does a branch ref exist? Writing an Engine definition over a
    // populated legacy collection would leave every one of its commits
    // unreachable while reporting success, which is the worst failure mode
    // available here.
    if has_legacy_data(kernel, collection) {
        return Err(format!(
            "collection '{}' already holds data in the legacy format; \
             create a new collection and copy into it rather than \
             reformatting in place",
            collection
        ));
    }
    definition::store(kernel, collection, &Definition::new(Format::Engine))
}

/// Create an engine-backed collection that spills values above `threshold`.
///
/// [`SPILL_THRESHOLD`] is a default tuned for a mixed point-read workload. A
/// lens that knows its own access pattern should say so instead of inheriting
/// it: an append-only stream, for instance, reads each segment at most once per
/// range scan and never point-reads it, which is the mix where spilling wins at
/// every size — and if its segments stay inline, every append rewrites the
/// whole right-most leaf and the append cost grows with the stream.
///
/// The value is recorded in the definition, so it survives for the life of the
/// collection and a later change to the default cannot alter how existing data
/// is read.
///
/// [`SPILL_THRESHOLD`]: pond_engine::SPILL_THRESHOLD
pub fn create_with_spill_threshold(
    kernel: &PondKernel,
    collection: &str,
    threshold: usize,
) -> Result<()> {
    if definition::load(kernel, collection).format == Format::Engine {
        return Ok(());
    }
    if has_legacy_data(kernel, collection) {
        return Err(format!(
            "collection '{}' already holds data in the legacy format; \
             create a new collection and copy into it rather than \
             reformatting in place",
            collection
        ));
    }
    let mut def = Definition::new(Format::Engine);
    def.spill_threshold = threshold.min(u32::MAX as usize) as u32;
    definition::store(kernel, collection, &def)
}

/// Does this collection have legacy commits on any branch?
fn has_legacy_data(kernel: &PondKernel, collection: &str) -> bool {
    let prefix = format!("collections/{}/_branches/", collection);
    kernel
        .list_names_prefix(&prefix)
        .iter()
        .any(|p| p.ends_with("/head") || p.contains("/_branches/"))
        || kernel
            .resolve(&crate::branch_ref(collection, "main"))
            .is_some()
}

/// As [`read_pnd2`], fetching only the payloads of the named columns.
///
/// Same encoder as the unprojected read, so a projected row and a full row are
/// presented identically — one function does the presenting, which is what
/// keeps two read paths from disagreeing about what a value looks like.
pub fn read_pnd2_projected(
    kernel: &PondKernel,
    collection: &str,
    columns: &[&str],
) -> Result<Vec<u8>> {
    let def = definition::load(kernel, collection);
    if def.format != Format::Engine {
        return Err(format!("collection '{}' is not engine-backed", collection));
    }
    let mut wanted: Vec<&str> = columns.to_vec();
    for internal in ["_rowid", "_version", "_deleted"] {
        if !wanted.contains(&internal) {
            wanted.push(internal);
        }
    }
    if let Some(subject) = def.subject_column.as_deref() {
        if !wanted.contains(&subject) {
            wanted.push(subject);
        }
    }

    let mut reader = Reader::open_with(store_of(kernel), pond_cache_config(), def.engine_config())
        .map_err(|e| format!("failed to open reader: {}", e))?;
    let records = open_all(
        kernel,
        &def,
        collection,
        reader
            .scan_projected(collection, &wanted)
            .map_err(|e| format!("failed to scan: {}", e))?,
    );
    let (columns, nulls) = records_to_columns_with_nulls(&records, &def);
    let borrowed: Vec<(&str, TypedColumn)> = columns
        .iter()
        .map(|(n, c)| (n.as_str(), c.clone()))
        .collect();
    Ok(pond_core::encode::pnd2_encode_multi_typed_with_nulls(
        &borrowed, &nulls,
    ))
}

/// Read a collection as it was at `root`, encoded exactly as a current read
/// would be.
///
/// The state a history entry names, read back. A root is a complete immutable
/// tree, so this is an ordinary scan starting somewhere other than the current
/// merged view — content addressing gives it for free, with no snapshot
/// machinery and nothing extra on the write path.
///
/// It returns PND2 for the same reason [`read_pnd2`] does: a row read at a
/// past root and the same row read now must be presented identically, and the
/// surest way to guarantee that is for one encoder to do the presenting.
pub fn read_pnd2_at(kernel: &PondKernel, collection: &str, root: &str) -> Result<Vec<u8>> {
    let def = definition::load(kernel, collection);
    if def.format != Format::Engine {
        return Err(format!("collection '{}' is not engine-backed", collection));
    }
    let reader = Reader::open_with(store_of(kernel), pond_cache_config(), def.engine_config())
        .map_err(|e| format!("failed to open reader: {}", e))?;
    let pairs = reader
        .scan_at(root, collection)
        .map_err(|e| format!("failed to read at root {}: {}", root, e))?;

    let (columns, nulls) = crate::columnar::records_to_columns_with_nulls(&pairs, &def);
    let borrowed: Vec<(&str, TypedColumn)> = columns
        .iter()
        .map(|(n, c)| (n.as_str(), c.clone()))
        .collect();
    Ok(pond_core::encode::pnd2_encode_multi_typed_with_nulls(
        &borrowed, &nulls,
    ))
}

/// Merge one engine collection into another.
///
/// # Why this is the cheap operation and a commit chain would not be
///
/// Merging two collections is merging two trees, and that is the operation
/// this whole design is built on: a semilattice join over content-addressed
/// nodes, so it touches only the subtrees that differ and shares everything
/// else by hash. Two collections that were branched from a common root and
/// then diverged in one leaf merge by rewriting that leaf's path and nothing
/// else.
///
/// It is also why merging needs no conflict resolution to *complete*. Merge is
/// commutative, associative and idempotent, and disagreements about a single
/// record are settled per field by version rather than by asking. Merging the
/// same source twice changes nothing the second time, which is what makes this
/// safe to retry after a failure.
///
/// The source is left exactly as it was — this is a merge *into* `target`, not
/// a join producing a third thing.
pub fn merge(kernel: &PondKernel, target: &str, source: &str, writer_id: u64) -> Result<()> {
    let def = definition::load(kernel, target);
    if def.format != Format::Engine {
        return Err(format!("collection '{}' is not engine-backed", target));
    }
    if definition::load(kernel, source).format != Format::Engine {
        return Err(format!("collection '{}' is not engine-backed", source));
    }
    if target == source {
        return Err(format!(
            "'{}' cannot be merged into itself; merge is idempotent, so this \
             would be a no-op that looks like work",
            target
        ));
    }

    let merged = {
        let mut reader =
            Reader::open_with(store_of(kernel), pond_cache_config(), def.engine_config())
                .map_err(|e| format!("failed to open reader: {}", e))?;
        let known = reader.collections();
        if !known.iter().any(|c| c == source) {
            return Err(format!("collection '{}' has nothing to merge", source));
        }
        if !known.iter().any(|c| c == target) {
            // Merging into an empty target is a copy, and saying so is more
            // useful than silently producing one.
            return Err(format!(
                "collection '{}' holds nothing yet; use `pond branch {} {}` to \
                 make a copy instead",
                target, source, target
            ));
        }
        reader.merge_roots(target, source)
    };

    let mut engine = open_engine(kernel, &def, writer_id)?;
    engine
        .branch_from_root(target, merged)
        .map_err(|e| format!("failed to stage merge: {}", e))?;
    engine
        .publish()
        .map_err(|e| format!("failed to publish merge: {}", e))
}

/// Write typed columns to an engine-backed collection and publish them.
///
/// The write is staged and then published as a single object write, so a
/// reader sees the whole batch or none of it. That is the transaction, and it
/// needs no transaction subsystem — it is one object, and object stores make
/// one object atomic.
pub fn write_rows(
    kernel: &PondKernel,
    collection: &str,
    columns: &[(&str, TypedColumn)],
    writer_id: u64,
) -> Result<()> {
    write_rows_with_nulls(kernel, collection, columns, &[], writer_id)
}

/// As [`write_rows`], recording which values are null.
///
/// Without the mask a null and the type's zero are the same bytes, so a store
/// that accepted one would return the other — a different fact, returned
/// without any error to notice.
pub fn write_rows_with_nulls(
    kernel: &PondKernel,
    collection: &str,
    columns: &[(&str, TypedColumn)],
    nulls: &[Option<Vec<bool>>],
    writer_id: u64,
) -> Result<()> {
    let mut def = definition::load(kernel, collection);
    if def.format != Format::Engine {
        return Err(format!(
            "collection '{}' is not engine-backed",
            collection
        ));
    }

    // Record the schema *before* the data, so a crash between the two leaves a
    // collection that can still be read: an over-declared column that no
    // record carries is dropped on read, whereas an undeclared column would
    // come back with a guessed type.
    //
    // Only when it actually changed. A steady-state write declares exactly the
    // columns already on record, and rewriting an unchanged definition would
    // add an object write to every commit for no information.
    if def.declare(&schema_of(columns)) {
        definition::store(kernel, collection, &def)?;
    }

    let physical = pond_kernel::crdt::current_time_ms();
    let records = columns_to_records_with_nulls(columns, nulls, writer_id, physical);

    // Seal before the records reach the index. Doing it here rather than
    // deeper means the engine never holds plaintext for a collection that
    // declared a subject column, so there is no path — a cache, a spill, a
    // scan — that could expose it.
    let (keys, plain): (Vec<_>, Vec<_>) = records.into_iter().unzip();
    let sealed = crate::subject::seal_records(kernel, &def, collection, plain)?;
    let records: Vec<_> = keys.into_iter().zip(sealed).collect();

    let mut engine = open_engine(kernel, &def, writer_id)?;
    engine
        .write_records(collection, records)
        .map_err(|e| format!("failed to stage records: {}", e))?;
    engine
        .publish()
        .map_err(|e| format!("failed to publish: {}", e))
}

/// Delete rows by `_rowid`.
///
/// A delete is a write. The record keeps its bytes and gains a tombstone
/// version, because a delete that removed the row could not converge: a writer
/// who never saw it would re-add the row and there would be nothing to compare
/// against. Readers skip tombstoned records, so the row is gone as far as
/// anyone can tell — and a field written *after* the delete brings it back,
/// which is the right answer when a delete and a later update cross in flight.
pub fn delete_rows(
    kernel: &PondKernel,
    collection: &str,
    rowids: &[String],
    writer_id: u64,
) -> Result<usize> {
    if rowids.is_empty() {
        return Ok(0);
    }
    let physical = pond_kernel::crdt::current_time_ms();
    let def = definition::load(kernel, collection);
    if def.format != Format::Engine {
        return Err(format!("collection '{}' is not engine-backed", collection));
    }
    let mut engine = open_engine(kernel, &def, writer_id)?;

    let mut records = Vec::with_capacity(rowids.len());
    for (i, id) in rowids.iter().enumerate() {
        let mut record = pond_record::Record::new();
        record.delete(pond_record::Version::new(physical, i as u64, writer_id));
        records.push((pond_index::Key::new(vec![pond_index::str_(id.clone())]), record));
    }
    let count = records.len();

    engine
        .write_records(collection, records)
        .map_err(|e| format!("failed to stage deletes: {}", e))?;
    engine
        .publish()
        .map_err(|e| format!("failed to publish: {}", e))?;
    Ok(count)
}

/// Branch a collection: a new name over the same tree.
///
/// O(1) whatever the size, because the tree is immutable and
/// content-addressed — the branch shares every node until it diverges, and
/// nothing is copied.
///
/// The root branched from is the *merged* one, not this writer's. Those differ
/// exactly when more than one writer has published to the collection, and
/// branching a partial view would silently produce a branch missing other
/// writers' rows — the kind of wrong answer that looks like a correct one.
pub fn branch(kernel: &PondKernel, from: &str, to: &str, writer_id: u64) -> Result<()> {
    let def = definition::load(kernel, from);
    if def.format != Format::Engine {
        return Err(format!("collection '{}' is not engine-backed", from));
    }
    if definition::load(kernel, to).format == Format::Engine {
        return Err(format!("collection '{}' already exists", to));
    }

    let root = {
        let mut reader = Reader::open_with(store_of(kernel), pond_cache_config(), def.engine_config())
            .map_err(|e| format!("failed to open reader: {}", e))?;
        if !reader.collections().iter().any(|c| c == from) {
            return Err(format!("collection '{}' has nothing to branch from", from));
        }
        reader.root_of(from)
    };

    // The branch inherits the source's schema and its pinned configuration.
    // A branch that chunked differently from its source would share no nodes
    // with it, which would defeat the entire point of branching.
    //
    // It also records where it came from. A branch here is an independent
    // collection sharing structure, not a ref inside one, so without this the
    // relationship exists only in the user's memory — and `pond branches` had
    // no way to tell a branch from any other collection.
    let mut def = def;
    def.branched_from = Some(from.to_string());
    definition::store(kernel, to, &def)?;

    let mut engine = open_engine(kernel, &def, writer_id)?;
    engine
        .branch_from_root(to, root)
        .map_err(|e| format!("failed to branch: {}", e))?;
    engine
        .publish()
        .map_err(|e| format!("failed to publish branch: {}", e))
}

/// Append rows whose integer keys are known to be greater than everything
/// already stored — a log, a stream, an event sequence.
///
/// Skips the read-merge an update needs, because nothing can already be at
/// these keys. That makes it the cheapest write the engine has: it touches the
/// right-most leaf and its ancestor path only, whatever the collection holds.
///
/// The caller is responsible for the keys actually being new. Appending at a
/// key that exists replaces the row rather than merging with it, which is the
/// correct behaviour for a log and the wrong one for a table.
pub fn append_binary_rows(
    kernel: &PondKernel,
    collection: &str,
    key_column: &str,
    keys: &[i64],
    value_column: &str,
    values: &[Vec<u8>],
    writer_id: u64,
) -> Result<()> {
    if keys.len() != values.len() {
        return Err(format!(
            "append needs one value per key, got {} keys and {} values",
            keys.len(),
            values.len()
        ));
    }
    if keys.is_empty() {
        return Ok(());
    }
    let def = definition::load(kernel, collection);
    if def.format != Format::Engine {
        return Err(format!("collection '{}' is not engine-backed", collection));
    }

    let physical = pond_kernel::crdt::current_time_ms();
    let records: Vec<(pond_index::Key, pond_record::Record)> = keys
        .iter()
        .zip(values)
        .enumerate()
        .map(|(i, (k, v))| {
            let version = pond_record::Version::new(physical, i as u64, writer_id);
            let record = pond_record::Record::new()
                .with_field(key_column, pond_record::Value::Int(*k), version)
                .with_field(value_column, pond_record::Value::Bytes(v.clone()), version);
            (pond_index::Key::new(vec![pond_index::int(*k)]), record)
        })
        .collect();

    let mut engine = open_engine(kernel, &def, writer_id)?;
    engine
        .append_records(collection, records)
        .map_err(|e| format!("failed to append: {}", e))?;
    engine
        .publish()
        .map_err(|e| format!("failed to publish: {}", e))
}

/// Rows addressed by a string key the caller chooses.
pub type NamedRow = (String, Vec<(String, pond_record::Value)>);

/// Write rows at explicit string keys, merging with whatever is there.
///
/// The key is the caller's, not a generated row id — which is what a key-value
/// collection needs, since its key *is* its identity rather than an attribute
/// of the row.
pub fn put_string_keyed_rows(
    kernel: &PondKernel,
    collection: &str,
    rows: &[NamedRow],
    writer_id: u64,
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let def = definition::load(kernel, collection);
    if def.format != Format::Engine {
        return Err(format!("collection '{}' is not engine-backed", collection));
    }

    let physical = pond_kernel::crdt::current_time_ms();
    let records: Vec<(pond_index::Key, pond_record::Record)> = rows
        .iter()
        .enumerate()
        .map(|(i, (key, fields))| {
            let version = pond_record::Version::new(physical, i as u64, writer_id);
            let mut record = pond_record::Record::new();
            for (name, value) in fields {
                record = record.with_field(name, value.clone(), version);
            }
            (
                pond_index::Key::new(vec![pond_index::str_(key.clone())]),
                record,
            )
        })
        .collect();

    let mut engine = open_engine(kernel, &def, writer_id)?;
    engine
        .write_records(collection, records)
        .map_err(|e| format!("failed to stage rows: {}", e))?;
    engine
        .publish()
        .map_err(|e| format!("failed to publish: {}", e))
}

/// Read one row by its string key.
///
/// This is the operation a key-value store is: a point lookup that costs the
/// tree's depth — two or three requests — whatever the collection holds. The
/// alternative it replaces is reading every row and filtering, which turns a
/// point lookup into a full scan.
pub fn get_string_keyed_row(
    kernel: &PondKernel,
    collection: &str,
    key: &str,
) -> Result<Option<std::collections::BTreeMap<String, pond_record::Value>>> {
    let def = definition::load(kernel, collection);
    if def.format != Format::Engine {
        return Err(format!("collection '{}' is not engine-backed", collection));
    }
    let mut reader = Reader::open_with(store_of(kernel), pond_cache_config(), def.engine_config())
        .map_err(|e| format!("failed to open reader: {}", e))?;
    let found = reader
        .get(
            collection,
            &pond_index::Key::new(vec![pond_index::str_(key)]),
        )
        .map_err(|e| format!("failed to read: {}", e))?;
    Ok(found.map(|rec| crate::columnar::record_to_map(&rec)))
}

/// Delete rows by string key, leaving tombstones.
pub fn delete_string_keyed_rows(
    kernel: &PondKernel,
    collection: &str,
    keys: &[String],
    writer_id: u64,
) -> Result<usize> {
    if keys.is_empty() {
        return Ok(0);
    }
    let def = definition::load(kernel, collection);
    if def.format != Format::Engine {
        return Err(format!("collection '{}' is not engine-backed", collection));
    }

    let physical = pond_kernel::crdt::current_time_ms();
    let records: Vec<(pond_index::Key, pond_record::Record)> = keys
        .iter()
        .enumerate()
        .map(|(i, key)| {
            let mut record = pond_record::Record::new();
            record.delete(pond_record::Version::new(physical, i as u64, writer_id));
            (
                pond_index::Key::new(vec![pond_index::str_(key.clone())]),
                record,
            )
        })
        .collect();
    let count = records.len();

    let mut engine = open_engine(kernel, &def, writer_id)?;
    engine
        .write_records(collection, records)
        .map_err(|e| format!("failed to stage deletes: {}", e))?;
    engine
        .publish()
        .map_err(|e| format!("failed to publish: {}", e))?;
    Ok(count)
}

/// Every row, with its string key.
pub fn scan_string_keyed_rows(
    kernel: &PondKernel,
    collection: &str,
) -> Result<Vec<NamedRow>> {
    let def = definition::load(kernel, collection);
    if def.format != Format::Engine {
        return Err(format!("collection '{}' is not engine-backed", collection));
    }
    let mut reader = Reader::open_with(store_of(kernel), pond_cache_config(), def.engine_config())
        .map_err(|e| format!("failed to open reader: {}", e))?;
    let rows = reader
        .scan(collection)
        .map_err(|e| format!("failed to scan: {}", e))?;

    Ok(rows
        .into_iter()
        .filter_map(|(key, rec)| {
            let name = match key.0.first() {
                Some(pond_index::KeyPart::Str(s)) => s.clone(),
                _ => return None,
            };
            let fields = crate::columnar::record_to_map(&rec)
                .into_iter()
                .collect::<Vec<_>>();
            Some((name, fields))
        })
        .collect())
}

/// Write one row at an explicit integer key, merging with whatever is there.
///
/// `write_rows` derives the key from `_rowid`, generating one when the caller
/// supplies none — which is right for table rows and wrong for anything whose
/// key *is* the data, like a stream offset or a well-known metadata slot. Those
/// callers need the key they chose, not one invented for them.
///
/// Merges rather than replaces, so a field this call does not mention survives
/// it, exactly as `write_rows` behaves.
pub fn put_int_keyed_row(
    kernel: &PondKernel,
    collection: &str,
    key: i64,
    fields: &[(&str, pond_record::Value)],
    writer_id: u64,
) -> Result<()> {
    let def = definition::load(kernel, collection);
    if def.format != Format::Engine {
        return Err(format!("collection '{}' is not engine-backed", collection));
    }

    let physical = pond_kernel::crdt::current_time_ms();
    let version = pond_record::Version::new(physical, 0, writer_id);
    let mut record = pond_record::Record::new();
    for (name, value) in fields {
        record = record.with_field(name, value.clone(), version);
    }

    let mut engine = open_engine(kernel, &def, writer_id)?;
    engine
        .write_records(
            collection,
            vec![(pond_index::Key::new(vec![pond_index::int(key)]), record)],
        )
        .map_err(|e| format!("failed to stage row: {}", e))?;
    engine
        .publish()
        .map_err(|e| format!("failed to publish: {}", e))
}

/// Read rows whose integer key falls in `[start, end)`.
///
/// The point of a range scan is that it reads the range and not the
/// collection: a stream that wants its last megabyte should not pay for the
/// terabyte before it.
pub fn read_range(
    kernel: &PondKernel,
    collection: &str,
    start: i64,
    end: i64,
) -> Result<Vec<std::collections::BTreeMap<String, pond_record::Value>>> {
    let def = definition::load(kernel, collection);
    if def.format != Format::Engine {
        return Err(format!("collection '{}' is not engine-backed", collection));
    }
    let mut reader = Reader::open_with(store_of(kernel), pond_cache_config(), def.engine_config())
        .map_err(|e| format!("failed to open reader: {}", e))?;
    let rows = reader
        .scan_range(
            collection,
            &pond_index::Key::new(vec![pond_index::int(start)]),
            &pond_index::Key::new(vec![pond_index::int(end)]),
        )
        .map_err(|e| format!("failed to scan range: {}", e))?;
    Ok(rows
        .into_iter()
        .map(|(_, rec)| crate::columnar::record_to_map(&rec))
        .collect())
}

/// Rotate one subject's key across a collection.
///
/// A key that never changes has unbounded exposure in time: anyone who
/// obtained it once can read everything that subject ever stored, including
/// rows written long afterwards.
///
/// The order is the safety argument. Rows are opened under the old key,
/// re-sealed under a new one, and **published** — and only then does the new
/// key replace the old. An interruption at any point leaves data readable
/// under a key that still exists. Destroying or replacing the key first and
/// then failing would leave rows nothing can open, which is an erasure nobody
/// requested.
///
/// Costs a rewrite of that subject's rows, unlike erasure which costs one key.
/// That is inherent: erasure may destroy the values, rotation must preserve
/// them.
pub fn rotate_subject(
    kernel: &PondKernel,
    collection: &str,
    subject: &str,
    writer_id: u64,
) -> Result<bool> {
    let def = definition::load(kernel, collection);
    if def.format != Format::Engine {
        return Err(format!("collection '{}' is not engine-backed", collection));
    }
    let Some(subject_column) = def.subject_column.clone() else {
        return Err(format!(
            "collection '{}' does not seal rows, so it has no keys to rotate",
            collection
        ));
    };

    let mut reader = Reader::open_with(store_of(kernel), pond_cache_config(), def.engine_config())
        .map_err(|e| format!("failed to open reader: {}", e))?;
    let rows = reader
        .scan(collection)
        .map_err(|e| format!("failed to scan: {}", e))?;
    let (keys, records): (Vec<_>, Vec<_>) = rows.into_iter().unzip();

    let fresh = pond_crypto::SubjectKey::generate();
    let (resealed, had_key) = crate::subject::reseal_for_rotation(
        kernel,
        &def,
        collection,
        subject,
        &subject_column,
        records,
        &fresh,
        writer_id,
    )?;
    if !had_key {
        // No key means erased, or never seen. Minting one would quietly
        // restore the ability to store data for a subject who asked to be
        // forgotten.
        return Ok(false);
    }

    // Publish the re-sealed rows before the key that opens them exists as the
    // subject's key. Until this succeeds, the old key still opens the old rows.
    let mut engine = open_engine(kernel, &def, writer_id)?;
    engine
        .write_records(collection, keys.into_iter().zip(resealed).collect())
        .map_err(|e| format!("failed to stage re-sealed rows: {}", e))?;
    engine
        .publish()
        .map_err(|e| format!("failed to publish re-sealed rows: {}", e))?;

    crate::subject::install_rotated_key(kernel, subject, &fresh)?;
    Ok(true)
}

/// Read an engine-backed collection as typed columns.
///
/// The read merges every writer's view, so a collection written by four
/// writers in four regions reads as one collection with no coordination having
/// taken place.
pub fn read_rows(kernel: &PondKernel, collection: &str) -> Result<Vec<(String, TypedColumn)>> {
    let def = definition::load(kernel, collection);
    if def.format != Format::Engine {
        return Err(format!("collection '{}' is not engine-backed", collection));
    }

    let mut reader = Reader::open_with(
        store_of(kernel),
        pond_cache_config(),
        def.engine_config(),
    )
    .map_err(|e| format!("failed to open reader: {}", e))?;
    let records = open_all(kernel, &def, collection, reader
        .scan(collection)
        .map_err(|e| format!("failed to scan: {}", e))?);
    Ok(records_to_columns_with_nulls(&records, &def).0)
}

/// As [`read_rows`], fetching only the payloads of the named columns.
///
/// # Why this is a different function and not a filter
///
/// Filtering after the read is what the CLI's `--columns` has always done, and
/// it produces the right answer at the wrong price: the payload of every
/// unwanted field is fetched, decoded and then discarded. For a row with a
/// large attachment beside small columns that is the whole cost of the query.
/// Measured over 200 such rows, asking for the small columns costs 40.7 KiB
/// where fetching everything and discarding costs 50 MiB.
///
/// The internal columns are always kept: `_rowid` identifies the row and the
/// version columns are what a merge is decided by, so dropping them would make
/// the result unusable for anything but display.
pub fn read_rows_projected(
    kernel: &PondKernel,
    collection: &str,
    columns: &[&str],
) -> Result<Vec<(String, TypedColumn)>> {
    let def = definition::load(kernel, collection);
    if def.format != Format::Engine {
        return Err(format!("collection '{}' is not engine-backed", collection));
    }

    // The subject column has to be resolved too, or sealed rows cannot be
    // opened — asking for a column and getting it back sealed would be a
    // strange kind of projection.
    let mut wanted: Vec<&str> = columns.to_vec();
    for internal in ["_rowid", "_version", "_deleted"] {
        if !wanted.contains(&internal) {
            wanted.push(internal);
        }
    }
    if let Some(subject) = def.subject_column.as_deref() {
        if !wanted.contains(&subject) {
            wanted.push(subject);
        }
    }

    let mut reader = Reader::open_with(store_of(kernel), pond_cache_config(), def.engine_config())
        .map_err(|e| format!("failed to open reader: {}", e))?;
    let records = open_all(
        kernel,
        &def,
        collection,
        reader
            .scan_projected(collection, &wanted)
            .map_err(|e| format!("failed to scan: {}", e))?,
    );
    Ok(records_to_columns_with_nulls(&records, &def).0)
}

/// Open every record's sealed fields.
///
/// A field whose subject key is gone drops out, so an erased subject reads as
/// absent rather than failing the scan — one erased subject must not make a
/// collection unreadable.
fn open_all(
    kernel: &PondKernel,
    def: &Definition,
    collection: &str,
    records: Vec<(pond_index::Key, pond_record::Record)>,
) -> Vec<(pond_index::Key, pond_record::Record)> {
    if def.subject_column.is_none() {
        return records;
    }
    let (keys, sealed): (Vec<_>, Vec<_>) = records.into_iter().unzip();
    let opened = crate::subject::open_records(kernel, def, collection, sealed);
    keys.into_iter().zip(opened).collect()
}

/// Read an engine-backed collection as typed columns plus their null masks.
pub fn read_rows_with_nulls(
    kernel: &PondKernel,
    collection: &str,
) -> Result<(Vec<(String, TypedColumn)>, Vec<Option<Vec<bool>>>)> {
    let def = definition::load(kernel, collection);
    if def.format != Format::Engine {
        return Err(format!("collection '{}' is not engine-backed", collection));
    }
    let mut reader = Reader::open_with(store_of(kernel), pond_cache_config(), def.engine_config())
        .map_err(|e| format!("failed to open reader: {}", e))?;
    let records = open_all(kernel, &def, collection, reader
        .scan(collection)
        .map_err(|e| format!("failed to scan: {}", e))?);
    Ok(records_to_columns_with_nulls(&records, &def))
}

/// Read an engine-backed collection as a PND2 blob, so callers that already
/// speak PND2 need no changes.
pub fn read_pnd2(kernel: &PondKernel, collection: &str) -> Result<Vec<u8>> {
    let (columns, nulls) = read_rows_with_nulls(kernel, collection)?;
    let borrowed: Vec<(&str, TypedColumn)> = columns
        .iter()
        .map(|(n, c)| (n.as_str(), c.clone()))
        .collect();
    // Carries the null masks, so a reader that goes through PND2 sees the same
    // nulls a reader that took the records directly would see.
    Ok(pond_core::encode::pnd2_encode_multi_typed_with_nulls(
        &borrowed, &nulls,
    ))
}

/// The engine takes an owned backend, and the kernel holds an `Arc` to one.
/// This hands the engine a cloned handle rather than a second connection, so
/// both see the same store and the same cache.
fn store_of(kernel: &PondKernel) -> std::sync::Arc<dyn ObjectStore> {
    kernel.store_handle()
}

fn pond_cache_config() -> pond_cache::CacheConfig {
    pond_cache::CacheConfig::default()
}

/// Open an engine using the chunk configuration this collection was created
/// with, not the current default.
///
/// The two differ as soon as the default is tuned, and using the wrong one
/// would rechunk the collection on its next write: still a correct tree, but
/// no longer byte-identical to a rebuild, which is what structural sharing and
/// deterministic merge depend on.
fn open_engine(
    kernel: &PondKernel,
    def: &Definition,
    writer_id: u64,
) -> Result<Engine<std::sync::Arc<dyn ObjectStore>>> {
    Engine::open_with(
        store_of(kernel),
        writer_id,
        pond_cache_config(),
        def.engine_config(),
    )
    .map_err(|e| format!("failed to open engine: {}", e))
}
