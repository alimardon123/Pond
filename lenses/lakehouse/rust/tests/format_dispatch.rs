// format_dispatch.rs — the lens must write where it reads.
//
// A collection is either legacy-format or engine-format, and the lens has to
// take the matching path for both. `read_table` did. `create_table` and
// `insert` did not: they called the legacy writer unconditionally.
//
// On an engine-backed collection that meant the rows went into a legacy
// manifest, the call returned `Ok` with a commit hash, and the very next read
// — which does dispatch — looked at the engine and found nothing. Measured
// before the fix: `create_table` returned `Ok("59845710…")` and `read_table`
// returned `Ok([])`. Rows accepted, acknowledged, and gone, with no error
// anywhere for a caller to notice.
//
// A write path and a read path that disagree about where the data lives is the
// worst shape a storage bug can take, because both halves individually look
// correct and every test that exercises one format alone passes.

use pond_core::TypedColumn;
use pond_kernel::PondKernel;
use pond_lakehouse_lens::LakehouseLens;
use pond_storage::{engine_path, UnifiedStorage};

fn ints(col: &TypedColumn) -> Vec<i64> {
    match col {
        TypedColumn::Int64(v) => v.clone(),
        other => panic!("expected Int64, got {:?}", std::mem::discriminant(other)),
    }
}

/// The `id` column, sorted.
///
/// Sorted deliberately, and it is worth saying why rather than quietly calling
/// `sort`. The two formats do not agree on row order: the legacy path stores a
/// column snapshot and gives rows back in insertion order, while the engine
/// orders by key, and a row written without an explicit `_rowid` gets a
/// generated one — so `[1, 2, 3]` came back as `[2, 3, 1]`. Every row is
/// present and every value is intact; only the order differs.
///
/// These tests are about rows surviving the write, so they compare as sets.
/// The ordering difference is a real gap in the "any lens over any collection"
/// claim and is recorded as such in docs/CRITIQUE.md — it is not something to
/// discover from a sort call in a test helper.
fn read_ids(lens: &LakehouseLens, table: &str) -> Vec<i64> {
    let cols = lens.read_table(table).expect("read_table");
    let (_, col) = cols.iter().find(|(n, _)| n == "id").expect("id column");
    let mut v = ints(col);
    v.sort_unstable();
    v
}

/// Rows written through the lens must be readable through the lens.
///
/// Run for both formats from one body, because the whole failure was the two
/// diverging — testing them separately is what let it through.
fn round_trip(engine: bool) {
    let dir = tempfile::tempdir().unwrap();
    let kernel = PondKernel::new_local(dir.path()).unwrap();
    if engine {
        engine_path::create(&kernel, "t").unwrap();
    }
    let lens = LakehouseLens::new(UnifiedStorage::new(kernel));

    lens.create_table("t", &[("id", TypedColumn::Int64(vec![1, 2, 3]))], "id", "seed")
        .expect("create_table");

    assert_eq!(
        read_ids(&lens, "t"),
        vec![1, 2, 3],
        "rows written through create_table are not readable ({} format)",
        if engine { "engine" } else { "legacy" }
    );
}

#[test]
fn create_table_round_trips_on_a_legacy_collection() {
    round_trip(false);
}

#[test]
fn create_table_round_trips_on_an_engine_collection() {
    round_trip(true);
}

/// `insert` appends rather than replacing, on both formats.
fn append(engine: bool) {
    let dir = tempfile::tempdir().unwrap();
    let kernel = PondKernel::new_local(dir.path()).unwrap();
    if engine {
        engine_path::create(&kernel, "t").unwrap();
    }
    let lens = LakehouseLens::new(UnifiedStorage::new(kernel));

    lens.create_table("t", &[("id", TypedColumn::Int64(vec![1, 2]))], "id", "seed")
        .expect("create_table");
    lens.insert("t", &[("id", TypedColumn::Int64(vec![3, 4]))], "more")
        .expect("insert");

    assert_eq!(
        read_ids(&lens, "t"),
        vec![1, 2, 3, 4],
        "insert lost rows ({} format)",
        if engine { "engine" } else { "legacy" }
    );
}

#[test]
fn insert_appends_on_a_legacy_collection() {
    append(false);
}

#[test]
fn insert_appends_on_an_engine_collection() {
    append(true);
}

/// The identifier handed back must name something real.
///
/// The engine publishes a head rather than a commit chain, so there is no
/// commit hash; the collection's root is returned instead. It has to change
/// when the data changes, or a caller using it to detect change would be
/// misled — which is worse than returning nothing.
#[test]
fn the_returned_identifier_changes_when_the_data_does() {
    let dir = tempfile::tempdir().unwrap();
    let kernel = PondKernel::new_local(dir.path()).unwrap();
    engine_path::create(&kernel, "t").unwrap();
    let lens = LakehouseLens::new(UnifiedStorage::new(kernel));

    let first = lens
        .create_table("t", &[("id", TypedColumn::Int64(vec![1, 2]))], "id", "seed")
        .expect("create_table");
    let second = lens
        .insert("t", &[("id", TypedColumn::Int64(vec![3]))], "more")
        .expect("insert");

    assert!(!first.is_empty(), "no identifier returned");
    assert_ne!(first, second, "the identifier did not change when the rows did");
}
