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

use crate::columnar::{columns_to_records, records_to_columns, schema_of};
use crate::definition::{self, Definition, Format};

/// Errors are strings here to match the rest of `core/storage`'s API.
type Result<T> = std::result::Result<T, String>;

/// Mark a collection as engine-backed. Idempotent.
///
/// Creating the definition is what routes every later call; there is no
/// separate registry, and no way for the marker and the data to disagree,
/// because the marker is what decides where the data goes.
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
    let records = columns_to_records(columns, writer_id, physical);

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
    definition::store(kernel, to, &def)?;

    let mut engine = open_engine(kernel, &def, writer_id)?;
    engine
        .branch_from_root(to, root)
        .map_err(|e| format!("failed to branch: {}", e))?;
    engine
        .publish()
        .map_err(|e| format!("failed to publish branch: {}", e))
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
    let records = reader
        .scan(collection)
        .map_err(|e| format!("failed to scan: {}", e))?;
    Ok(records_to_columns(&records, &def))
}

/// Read an engine-backed collection as a PND2 blob, so callers that already
/// speak PND2 need no changes.
pub fn read_pnd2(kernel: &PondKernel, collection: &str) -> Result<Vec<u8>> {
    let columns = read_rows(kernel, collection)?;
    let borrowed: Vec<(&str, TypedColumn)> = columns
        .iter()
        .map(|(n, c)| (n.as_str(), c.clone()))
        .collect();
    Ok(pond_core::encode::pnd2_encode_multi_typed(&borrowed))
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
