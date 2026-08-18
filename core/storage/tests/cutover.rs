// cutover.rs — do the two storage paths agree?
//
// A dispatch between two implementations is a bug generator unless something
// forces them to stay in agreement. That is what this file is: the same
// operations run through the legacy path and through the engine, with the
// results compared as data rather than as bytes.
//
// Bytes would be the wrong comparison. The two paths lay data out completely
// differently — that is the entire point of building the second one — so the
// question is not "are the encodings identical" but "does a caller see the
// same rows". These tests ask the second question.

use pond_core::constants::{VT_FLOAT64, VT_INT64, VT_STRING, VT_TIMESTAMP};
use pond_core::encode::TypedColumn;
use pond_kernel::PondKernel;
use pond_storage::definition::{self, Format};
use pond_storage::engine_path;

fn kernel(dir: &std::path::Path) -> PondKernel {
    PondKernel::new_local(dir).unwrap()
}

fn sample_columns() -> Vec<(&'static str, TypedColumn)> {
    vec![
        ("id", TypedColumn::Int64(vec![1, 2, 3])),
        (
            "name",
            TypedColumn::String(vec!["ada".into(), "grace".into(), "alan".into()]),
        ),
        ("score", TypedColumn::Float64(vec![1.5, 2.5, 3.5])),
        ("seen_at", TypedColumn::Timestamp(vec![100, 200, 300])),
    ]
}

/// Sort a column set into a comparable shape: rows keyed by `id`.
fn rows_by_id(columns: &[(String, TypedColumn)]) -> Vec<(i64, String, f64, i64)> {
    let ids = match columns.iter().find(|(n, _)| n == "id") {
        Some((_, TypedColumn::Int64(v))) => v.clone(),
        _ => panic!("id column missing or wrong type: {:?}", names(columns)),
    };
    let names_col = match columns.iter().find(|(n, _)| n == "name") {
        Some((_, TypedColumn::String(v))) => v.clone(),
        _ => panic!("name column missing or wrong type"),
    };
    let scores = match columns.iter().find(|(n, _)| n == "score") {
        Some((_, TypedColumn::Float64(v))) => v.clone(),
        _ => panic!("score column missing or wrong type"),
    };
    let seen = match columns.iter().find(|(n, _)| n == "seen_at") {
        Some((_, TypedColumn::Timestamp(v))) => v.clone(),
        _ => panic!("seen_at column missing or wrong type — declared type was lost"),
    };

    let mut rows: Vec<(i64, String, f64, i64)> = (0..ids.len())
        .map(|i| (ids[i], names_col[i].clone(), scores[i], seen[i]))
        .collect();
    rows.sort_by_key(|r| r.0);
    rows
}

fn names(columns: &[(String, TypedColumn)]) -> Vec<&str> {
    columns.iter().map(|(n, _)| n.as_str()).collect()
}

/// The headline: write the same columns both ways, read both back, get the
/// same rows.
#[test]
fn both_paths_agree_on_the_same_rows() {
    let legacy_dir = tempfile::tempdir().unwrap();
    let engine_dir = tempfile::tempdir().unwrap();

    // Legacy path.
    let lk = kernel(legacy_dir.path());
    pond_storage::write::write_rows_no_crdt(&lk, "users", "main", &sample_columns(), "seed")
        .unwrap();
    let legacy_blob = pond_storage::read::read(&lk, "users", "main").unwrap();
    let legacy_columns = decode_to_typed(&legacy_blob);

    // Engine path.
    let ek = kernel(engine_dir.path());
    engine_path::create(&ek, "users").unwrap();
    engine_path::write_rows(&ek, "users", &sample_columns(), 1).unwrap();
    let engine_columns = engine_path::read_rows(&ek, "users").unwrap();

    assert_eq!(
        rows_by_id(&legacy_columns),
        rows_by_id(&engine_columns),
        "the two paths must present the same rows\nlegacy: {:?}\nengine: {:?}",
        names(&legacy_columns),
        names(&engine_columns)
    );
}

/// A collection written before the engine existed has no definition object,
/// and must still be recognised as legacy. This is what makes the cutover
/// require no migration.
#[test]
fn collections_without_a_definition_stay_legacy() {
    let dir = tempfile::tempdir().unwrap();
    let k = kernel(dir.path());

    pond_storage::write::write_rows_no_crdt(&k, "old", "main", &sample_columns(), "seed").unwrap();

    assert_eq!(definition::format_of(&k, "old"), Format::Legacy);
    // The engine refuses to read it rather than reporting it as empty.
    assert!(engine_path::read_rows(&k, "old").is_err());

    // And it refuses to *reformat* it. Writing an engine definition over a
    // populated legacy collection would make every existing commit
    // unreachable while reporting success.
    let err = engine_path::create(&k, "old").expect_err("must refuse");
    assert!(
        err.contains("legacy format"),
        "the refusal should say why: {}",
        err
    );
    assert_eq!(definition::format_of(&k, "old"), Format::Legacy);

    // The legacy read is unaffected by any of this.
    assert!(!pond_storage::read::read(&k, "old", "main").unwrap().is_empty());
}

/// Declared column types survive a round trip through records.
///
/// A timestamp and an integer are the same bytes once they are values, so
/// without the schema in the definition this is exactly where a column would
/// silently change type.
#[test]
fn declared_types_survive_the_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let k = kernel(dir.path());
    engine_path::create(&k, "events").unwrap();
    engine_path::write_rows(&k, "events", &sample_columns(), 1).unwrap();

    let def = definition::load(&k, "events");
    assert_eq!(def.column_type("seen_at"), Some(VT_TIMESTAMP));
    assert_eq!(def.column_type("id"), Some(VT_INT64));
    assert_eq!(def.column_type("name"), Some(VT_STRING));
    assert_eq!(def.column_type("score"), Some(VT_FLOAT64));

    let columns = engine_path::read_rows(&k, "events").unwrap();
    assert!(
        matches!(
            columns.iter().find(|(n, _)| n == "seen_at"),
            Some((_, TypedColumn::Timestamp(_)))
        ),
        "a timestamp column must not come back as an integer column"
    );
}

/// A column the schema has not seen must still come back.
///
/// This is the never-drop law, applied at the column level: a writer that adds
/// a column, and a reader that has never heard of it, must not between them
/// cause the data to disappear.
#[test]
fn a_new_column_is_not_dropped() {
    let dir = tempfile::tempdir().unwrap();
    let k = kernel(dir.path());
    engine_path::create(&k, "users").unwrap();
    engine_path::write_rows(&k, "users", &sample_columns(), 1).unwrap();

    let mut extended = sample_columns();
    extended.push(("email", TypedColumn::String(vec!["a@x".into(), "g@x".into(), "t@x".into()])));
    engine_path::write_rows(&k, "users", &extended, 1).unwrap();

    let columns = engine_path::read_rows(&k, "users").unwrap();
    assert!(
        names(&columns).contains(&"email"),
        "a column added by a later write must be readable: {:?}",
        names(&columns)
    );
    // And the original columns are still there.
    assert!(names(&columns).contains(&"score"));
}

/// Two writers, no coordination, one collection.
///
/// This is the claim the whole design exists for, exercised through the
/// storage API rather than through the engine directly.
#[test]
fn two_writers_converge_without_coordination() {
    let dir = tempfile::tempdir().unwrap();

    let k1 = kernel(dir.path());
    engine_path::create(&k1, "users").unwrap();
    engine_path::write_rows(
        &k1,
        "users",
        &[
            ("_rowid", TypedColumn::String(vec!["r1".into()])),
            ("id", TypedColumn::Int64(vec![1])),
            ("name", TypedColumn::String(vec!["ada".into()])),
        ],
        1,
    )
    .unwrap();

    // A second writer, with its own id, never talking to the first.
    let k2 = kernel(dir.path());
    engine_path::write_rows(
        &k2,
        "users",
        &[
            ("_rowid", TypedColumn::String(vec!["r2".into()])),
            ("id", TypedColumn::Int64(vec![2])),
            ("name", TypedColumn::String(vec!["grace".into()])),
        ],
        2,
    )
    .unwrap();

    let columns = engine_path::read_rows(&k1, "users").unwrap();
    let ids = match columns.iter().find(|(n, _)| n == "id") {
        Some((_, TypedColumn::Int64(v))) => v.clone(),
        _ => panic!("id column missing"),
    };
    let mut ids = ids;
    ids.sort();
    assert_eq!(
        ids,
        vec![1, 2],
        "both writers' rows must be visible to a single reader"
    );
}

/// `read_pnd2` gives callers that already speak PND2 the engine's data
/// without any change on their side.
#[test]
fn engine_data_is_readable_as_pnd2() {
    let dir = tempfile::tempdir().unwrap();
    let k = kernel(dir.path());
    engine_path::create(&k, "users").unwrap();
    engine_path::write_rows(&k, "users", &sample_columns(), 1).unwrap();

    let blob = engine_path::read_pnd2(&k, "users").unwrap();
    let columns = decode_to_typed(&blob);
    assert_eq!(rows_by_id(&columns).len(), 3);
}

/// Decode a PND2 blob back into typed columns, for comparison.
fn decode_to_typed(blob: &[u8]) -> Vec<(String, TypedColumn)> {
    let decoded = pond_core::decode::pnd2_decode(blob).expect("decode PND2");
    decoded
        .into_iter()
        .map(|c| {
            let name = c.name.to_string_lossy().into_owned();
            let col = match c.vtype {
                VT_INT64 => TypedColumn::Int64(c.i64_data.clone()),
                VT_TIMESTAMP => TypedColumn::Timestamp(c.i64_data.clone()),
                VT_FLOAT64 => TypedColumn::Float64(c.f64_data.clone()),
                VT_STRING => TypedColumn::String(
                    c.str_data
                        .iter()
                        .map(|s| s.to_string_lossy().into_owned())
                        .collect(),
                ),
                other => panic!("unexpected column type {} for {}", other, name),
            };
            (name, col)
        })
        .collect()
}

/// A second write adds rows; it does not replace the first.
///
/// This is the failure the row-identity design exists to prevent. A column
/// batch has no notion of which row is which, and its ordinals restart at zero
/// on every write — so keying rows by position makes each write silently
/// overwrite the previous one, row for row, while reporting success. Rows
/// without a supplied `_rowid` get a generated one instead.
#[test]
fn a_second_write_appends_rather_than_replacing() {
    let dir = tempfile::tempdir().unwrap();
    let k = kernel(dir.path());
    engine_path::create(&k, "users").unwrap();

    engine_path::write_rows(
        &k,
        "users",
        &[
            ("id", TypedColumn::Int64(vec![1, 2])),
            ("name", TypedColumn::String(vec!["ada".into(), "grace".into()])),
        ],
        1,
    )
    .unwrap();

    engine_path::write_rows(
        &k,
        "users",
        &[
            ("id", TypedColumn::Int64(vec![3])),
            ("name", TypedColumn::String(vec!["alan".into()])),
        ],
        1,
    )
    .unwrap();

    let columns = engine_path::read_rows(&k, "users").unwrap();
    let mut ids = match columns.iter().find(|(n, _)| n == "id") {
        Some((_, TypedColumn::Int64(v))) => v.clone(),
        _ => panic!("id column missing"),
    };
    ids.sort();
    assert_eq!(
        ids,
        vec![1, 2, 3],
        "all three rows must survive; got {:?}",
        ids
    );
}

/// Naming a row by `_rowid` updates it in place.
///
/// The other half of the same rule: identity supplied means "this row", so a
/// second write to the same id is an update, not a duplicate.
#[test]
fn writing_the_same_rowid_updates_that_row() {
    let dir = tempfile::tempdir().unwrap();
    let k = kernel(dir.path());
    engine_path::create(&k, "users").unwrap();

    engine_path::write_rows(
        &k,
        "users",
        &[
            ("_rowid", TypedColumn::String(vec!["r1".into()])),
            ("id", TypedColumn::Int64(vec![1])),
            ("name", TypedColumn::String(vec!["ada".into()])),
        ],
        1,
    )
    .unwrap();

    engine_path::write_rows(
        &k,
        "users",
        &[
            ("_rowid", TypedColumn::String(vec!["r1".into()])),
            ("id", TypedColumn::Int64(vec![1])),
            ("name", TypedColumn::String(vec!["ada lovelace".into()])),
        ],
        1,
    )
    .unwrap();

    let columns = engine_path::read_rows(&k, "users").unwrap();
    let names_col = match columns.iter().find(|(n, _)| n == "name") {
        Some((_, TypedColumn::String(v))) => v.clone(),
        _ => panic!("name column missing"),
    };
    assert_eq!(
        names_col,
        vec!["ada lovelace".to_string()],
        "one row, updated — not two rows"
    );
}
