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

/// A deleted row stops being readable.
///
/// The record itself stays in the tree — a delete that erased the bytes could
/// not converge, because a writer who never saw it would re-add the row with
/// nothing to compare against. What makes it a delete is that readers skip it.
#[test]
fn deleted_rows_disappear_from_reads() {
    let dir = tempfile::tempdir().unwrap();
    let k = kernel(dir.path());
    engine_path::create(&k, "users").unwrap();

    engine_path::write_rows(
        &k,
        "users",
        &[
            ("_rowid", TypedColumn::String(vec!["r1".into(), "r2".into()])),
            ("id", TypedColumn::Int64(vec![1, 2])),
        ],
        1,
    )
    .unwrap();

    assert_eq!(engine_path::delete_rows(&k, "users", &["r1".to_string()], 1).unwrap(), 1);

    let columns = engine_path::read_rows(&k, "users").unwrap();
    let ids = match columns.iter().find(|(n, _)| n == "id") {
        Some((_, TypedColumn::Int64(v))) => v.clone(),
        _ => panic!("id column missing"),
    };
    assert_eq!(ids, vec![2], "the deleted row must not be returned");
}

/// A write that lands after a delete brings the row back.
///
/// This is not a quirk to be tolerated — it is the only answer that converges.
/// Two writers with no channel between them can produce a delete and an update
/// in either order, and both must reach the same state. Ordering by version
/// rather than by arrival is what makes that true.
#[test]
fn a_later_write_resurrects_a_deleted_row() {
    let dir = tempfile::tempdir().unwrap();
    let k = kernel(dir.path());
    engine_path::create(&k, "users").unwrap();

    engine_path::write_rows(
        &k,
        "users",
        &[
            ("_rowid", TypedColumn::String(vec!["r1".into()])),
            ("id", TypedColumn::Int64(vec![1])),
        ],
        1,
    )
    .unwrap();
    engine_path::delete_rows(&k, "users", &["r1".to_string()], 1).unwrap();
    assert!(engine_path::read_rows(&k, "users").unwrap().is_empty());

    // A later update to the same row.
    engine_path::write_rows(
        &k,
        "users",
        &[
            ("_rowid", TypedColumn::String(vec!["r1".into()])),
            ("id", TypedColumn::Int64(vec![99])),
        ],
        1,
    )
    .unwrap();

    let columns = engine_path::read_rows(&k, "users").unwrap();
    let ids = match columns.iter().find(|(n, _)| n == "id") {
        Some((_, TypedColumn::Int64(v))) => v.clone(),
        _ => panic!("id column missing"),
    };
    assert_eq!(ids, vec![99], "a write newer than the tombstone wins");
}

/// A collection keeps the chunk configuration it was created with.
///
/// The chunk target decides where boundaries fall, so it decides every node
/// hash and therefore the root. If a collection read the current default
/// instead of its own pinned value, tuning that default would rechunk existing
/// collections on their next write — still correct, but no longer
/// byte-identical to a rebuild, which is what structural sharing and
/// deterministic merge depend on.
#[test]
fn chunk_config_is_pinned_per_collection() {
    let dir = tempfile::tempdir().unwrap();
    let k = kernel(dir.path());
    engine_path::create(&k, "users").unwrap();

    let def = definition::load(&k, "users");
    assert_eq!(
        def.chunk_target,
        pond_index::DEFAULT_TARGET_ENTRIES,
        "a new collection is created with the current default"
    );

    // A definition written by an older version, before the target was stored,
    // must read back as the value that version actually used — not as today's
    // default.
    let v1 = {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"PDEF");
        bytes.push(1); // version 1
        bytes.push(2); // Format::Engine
        bytes.extend_from_slice(&0u32.to_le_bytes()); // no columns
        bytes
    };
    let decoded = definition::Definition::decode(&v1).expect("v1 must still decode");
    assert_eq!(decoded.format, Format::Engine);
    assert_eq!(
        decoded.chunk_target,
        definition::LEGACY_CHUNK_TARGET,
        "a v1 collection must keep chunking the way it always did"
    );
    assert_eq!(
        decoded.spill_threshold,
        u32::MAX,
        "v1 predates spilling, so it must keep storing every value inline"
    );

    // And the pinned value survives a write that extends the schema.
    engine_path::write_rows(
        &k,
        "users",
        &[("id", TypedColumn::Int64(vec![1]))],
        1,
    )
    .unwrap();
    assert_eq!(definition::load(&k, "users").chunk_target, def.chunk_target);
}


/// The spill threshold is pinned per collection, for the same reason the chunk
/// target is.
///
/// It decides whether a value is written into a leaf or replaced by a pointer
/// to it, so two writers using different thresholds produce different index
/// bytes for identical data — different leaf hashes, different roots, and no
/// convergence. A collection that read the current default instead of its own
/// pinned value would diverge from itself across a version upgrade.
#[test]
fn spill_threshold_is_pinned_per_collection() {
    let dir = tempfile::tempdir().unwrap();
    let k = kernel(dir.path());
    engine_path::create(&k, "docs").unwrap();

    let def = definition::load(&k, "docs");
    assert_eq!(def.spill_threshold as usize, pond_engine::SPILL_THRESHOLD);
    assert_eq!(
        def.engine_config().spill_threshold as u32,
        def.spill_threshold
    );

    // Survives a write that extends the schema.
    let big = "x".repeat(64 * 1024);
    engine_path::write_rows(
        &k,
        "docs",
        &[
            ("id", TypedColumn::Int64(vec![1])),
            ("body", TypedColumn::String(vec![big.clone()])),
        ],
        1,
    )
    .unwrap();
    assert_eq!(definition::load(&k, "docs").spill_threshold, def.spill_threshold);

    // And a spilled value round-trips through the columnar path unchanged.
    let columns = engine_path::read_rows(&k, "docs").unwrap();
    match columns.iter().find(|(n, _)| n == "body") {
        Some((_, TypedColumn::String(v))) => {
            assert_eq!(v, &vec![big], "a spilled value must read back intact")
        }
        other => panic!("body column missing or wrong type: {:?}", other.is_some()),
    }
}

/// Branching is a pointer copy: the branch sees the source's rows, then the
/// two diverge without either affecting the other.
#[test]
fn branching_shares_then_diverges() {
    let dir = tempfile::tempdir().unwrap();
    let k = kernel(dir.path());
    engine_path::create(&k, "users").unwrap();
    engine_path::write_rows(
        &k,
        "users",
        &[("id", TypedColumn::Int64(vec![1, 2]))],
        1,
    )
    .unwrap();

    engine_path::branch(&k, "users", "users_v2", 1).unwrap();

    let ids = |c: &str| -> Vec<i64> {
        let columns = engine_path::read_rows(&k, c).unwrap();
        match columns.iter().find(|(n, _)| n == "id") {
            Some((_, TypedColumn::Int64(v))) => {
                let mut v = v.clone();
                v.sort();
                v
            }
            _ => Vec::new(),
        }
    };

    assert_eq!(ids("users_v2"), vec![1, 2], "the branch inherits the rows");

    engine_path::write_rows(&k, "users_v2", &[("id", TypedColumn::Int64(vec![3]))], 1).unwrap();
    assert_eq!(ids("users_v2"), vec![1, 2, 3]);
    assert_eq!(ids("users"), vec![1, 2], "the source must not change");

    engine_path::write_rows(&k, "users", &[("id", TypedColumn::Int64(vec![4]))], 1).unwrap();
    assert_eq!(ids("users"), vec![1, 2, 4]);
    assert_eq!(ids("users_v2"), vec![1, 2, 3], "the branch must not change");
}

/// A branch inherits the source's pinned configuration.
///
/// A branch that chunked differently from its source would share no nodes with
/// it — which defeats the entire point, since sharing is what makes branching
/// free.
#[test]
fn a_branch_inherits_the_source_configuration() {
    let dir = tempfile::tempdir().unwrap();
    let k = kernel(dir.path());
    engine_path::create(&k, "users").unwrap();
    engine_path::write_rows(&k, "users", &[("id", TypedColumn::Int64(vec![1]))], 1).unwrap();
    engine_path::branch(&k, "users", "users_v2", 1).unwrap();

    let src = definition::load(&k, "users");
    let branched = definition::load(&k, "users_v2");
    assert_eq!(branched.chunk_target, src.chunk_target);
    assert_eq!(
        branched.chunk_salt, src.chunk_salt,
        "a different salt would place boundaries differently and share nothing"
    );
    assert_eq!(branched.spill_threshold, src.spill_threshold);
    assert_eq!(branched.columns, src.columns, "the schema comes with it");
}

/// Branching refuses rather than doing something surprising.
#[test]
fn branching_refuses_bad_targets() {
    let dir = tempfile::tempdir().unwrap();
    let k = kernel(dir.path());
    engine_path::create(&k, "users").unwrap();
    engine_path::write_rows(&k, "users", &[("id", TypedColumn::Int64(vec![1]))], 1).unwrap();
    engine_path::create(&k, "taken").unwrap();

    assert!(
        engine_path::branch(&k, "users", "taken", 1).is_err(),
        "must not silently overwrite an existing collection"
    );
    assert!(
        engine_path::branch(&k, "absent", "x", 1).is_err(),
        "must not branch a collection that does not exist"
    );

    // A legacy collection is not branchable through this path.
    pond_storage::write::write_rows_no_crdt(
        &k,
        "old",
        "main",
        &[("id", TypedColumn::Int64(vec![7]))],
        "seed",
    )
    .unwrap();
    assert!(engine_path::branch(&k, "old", "old_v2", 1).is_err());
}

/// A null must not read back as the type's zero.
///
/// A dense column has to put *something* in a null's slot, and without a mask
/// that placeholder is indistinguishable from a value the caller wrote. A
/// score of zero and an unrecorded score are different facts; returning one
/// for the other is wrong data with no error to notice.
#[test]
fn nulls_survive_the_round_trip_and_differ_from_zero() {
    let dir = tempfile::tempdir().unwrap();
    let k = kernel(dir.path());
    engine_path::create(&k, "t").unwrap();

    engine_path::write_rows_with_nulls(
        &k,
        "t",
        &[
            ("id", TypedColumn::Int64(vec![1, 2, 3])),
            ("score", TypedColumn::Int64(vec![0, 0, 5])),
        ],
        &[None, Some(vec![false, true, false])],
        1,
    )
    .unwrap();

    let (columns, nulls) = engine_path::read_rows_with_nulls(&k, "t").unwrap();
    let score_at = columns.iter().position(|(n, _)| n == "score").expect("score");
    let mask = nulls[score_at].as_ref().expect("score has a null mask");

    // Rows come back in key order, which for generated rowids is not input
    // order — so locate the rows by their id.
    let id_at = columns.iter().position(|(n, _)| n == "id").unwrap();
    let ids = match &columns[id_at].1 {
        TypedColumn::Int64(v) => v.clone(),
        _ => panic!("id must be Int64"),
    };
    let row_of = |id: i64| ids.iter().position(|x| *x == id).expect("row present");

    assert!(!mask[row_of(1)], "a real zero must not be marked null");
    assert!(mask[row_of(2)], "the null must be marked null");
    assert!(!mask[row_of(3)]);

    // And through PND2, which is how SQL, the lenses and Python read this.
    let blob = engine_path::read_pnd2(&k, "t").unwrap();
    let decoded = pond_core::decode::pnd2_decode(&blob).unwrap();
    let score = decoded
        .iter()
        .find(|c| c.name.to_string_lossy() == "score")
        .expect("score column");
    let bitmap = score
        .null_bitmap
        .as_ref()
        .expect("the null mask must survive PND2");
    assert!(!pond_core::decode::is_null_at(bitmap, row_of(1)));
    assert!(pond_core::decode::is_null_at(bitmap, row_of(2)));
}

/// A column with no nulls must carry no mask, so nothing changes for data that
/// has none.
#[test]
fn columns_without_nulls_carry_no_mask() {
    let dir = tempfile::tempdir().unwrap();
    let k = kernel(dir.path());
    engine_path::create(&k, "t").unwrap();
    engine_path::write_rows(&k, "t", &[("id", TypedColumn::Int64(vec![1, 2]))], 1).unwrap();

    let (_, nulls) = engine_path::read_rows_with_nulls(&k, "t").unwrap();
    assert!(
        nulls.iter().all(|m| m.is_none()),
        "no nulls written, so no masks should come back"
    );

    let blob = engine_path::read_pnd2(&k, "t").unwrap();
    let decoded = pond_core::decode::pnd2_decode(&blob).unwrap();
    assert!(decoded.iter().all(|c| c.null_bitmap.is_none()));
}

// ---------------------------------------------------------------------------
// Erasure — see docs/ERASURE.md
// ---------------------------------------------------------------------------

/// The contract, end to end: a subject's data is readable, then erased, and
/// what remains is unreadable — while everybody else's rows are untouched.
#[test]
fn erasing_a_subject_removes_their_values_and_no_one_elses() {
    let dir = tempfile::tempdir().unwrap();
    let k = kernel(dir.path());
    engine_path::create_for_subjects(&k, "people", "owner").unwrap();

    engine_path::write_rows(
        &k,
        "people",
        &[
            (
                "owner",
                TypedColumn::String(vec!["alice".into(), "bob".into()]),
            ),
            (
                "note",
                TypedColumn::String(vec!["alice's secret".into(), "bob's secret".into()]),
            ),
            ("score", TypedColumn::Int64(vec![10, 20])),
        ],
        1,
    )
    .unwrap();

    let notes = |k: &pond_kernel::PondKernel| -> Vec<(String, Option<String>)> {
        let columns = engine_path::read_rows(k, "people").unwrap();
        let owners = match columns.iter().find(|(n, _)| n == "owner") {
            Some((_, TypedColumn::String(v))) => v.clone(),
            _ => panic!("owner column must stay readable"),
        };
        let (_, masks) = engine_path::read_rows_with_nulls(k, "people").unwrap();
        let note_at = columns.iter().position(|(n, _)| n == "note");
        let notes = match note_at.map(|i| &columns[i].1) {
            Some(TypedColumn::String(v)) => v.clone(),
            _ => vec![String::new(); owners.len()],
        };
        let mask = note_at.and_then(|i| masks.get(i).cloned().flatten());
        owners
            .iter()
            .enumerate()
            .map(|(i, o)| {
                let erased = mask.as_ref().is_some_and(|m| m[i]);
                (o.clone(), if erased { None } else { Some(notes[i].clone()) })
            })
            .collect()
    };

    // Both readable to start.
    let before = notes(&k);
    assert_eq!(
        before.iter().find(|(o, _)| o == "alice").unwrap().1,
        Some("alice's secret".to_string())
    );
    assert_eq!(
        before.iter().find(|(o, _)| o == "bob").unwrap().1,
        Some("bob's secret".to_string())
    );

    // Erase one subject.
    assert!(pond_storage::subject::erase_subject(&k, "alice").unwrap());

    let after = notes(&k);
    assert_eq!(
        after.iter().find(|(o, _)| o == "alice").unwrap().1,
        None,
        "the erased subject's value must not be readable"
    );
    assert_eq!(
        after.iter().find(|(o, _)| o == "bob").unwrap().1,
        Some("bob's secret".to_string()),
        "erasing one subject must not touch another's data"
    );
}

/// An erased subject must not break the collection.
///
/// A scan that failed because one row's key was destroyed would hand a denial
/// of service to anyone exercising their right to deletion.
#[test]
fn a_scan_still_works_after_an_erasure() {
    let dir = tempfile::tempdir().unwrap();
    let k = kernel(dir.path());
    engine_path::create_for_subjects(&k, "people", "owner").unwrap();

    let owners: Vec<String> = (0..50).map(|i| format!("subject-{:03}", i)).collect();
    engine_path::write_rows(
        &k,
        "people",
        &[
            ("owner", TypedColumn::String(owners.clone())),
            (
                "note",
                TypedColumn::String((0..50).map(|i| format!("note {}", i)).collect()),
            ),
        ],
        1,
    )
    .unwrap();

    for victim in owners.iter().take(10) {
        pond_storage::subject::erase_subject(&k, victim).unwrap();
    }

    let columns = engine_path::read_rows(&k, "people").unwrap();
    let read_owners = match columns.iter().find(|(n, _)| n == "owner") {
        Some((_, TypedColumn::String(v))) => v.clone(),
        _ => panic!("owner column missing"),
    };
    assert_eq!(read_owners.len(), 50, "every row must still be returned");
}

/// Rows must actually be sealed on disk, not merely hidden on read.
#[test]
fn sealed_values_are_not_stored_in_the_clear() {
    let dir = tempfile::tempdir().unwrap();
    let k = kernel(dir.path());
    engine_path::create_for_subjects(&k, "people", "owner").unwrap();
    engine_path::write_rows(
        &k,
        "people",
        &[
            ("owner", TypedColumn::String(vec!["alice".into()])),
            ("note", TypedColumn::String(vec!["TOPSECRETVALUE".into()])),
        ],
        1,
    )
    .unwrap();

    // Walk every byte the store holds and look for the plaintext.
    let mut found = false;
    for entry in walkdir(dir.path()) {
        if let Ok(bytes) = std::fs::read(&entry) {
            if bytes
                .windows(b"TOPSECRETVALUE".len())
                .any(|w| w == b"TOPSECRETVALUE")
            {
                found = true;
            }
        }
    }
    assert!(!found, "the plaintext was written to disk somewhere");

    // Control: the same check against an unsealed collection must find it.
    // Without this, the test above would pass just as happily if the search
    // were broken.
    let plain_dir = tempfile::tempdir().unwrap();
    let pk = kernel(plain_dir.path());
    engine_path::create(&pk, "people").unwrap();
    engine_path::write_rows(
        &pk,
        "people",
        &[("note", TypedColumn::String(vec!["TOPSECRETVALUE".into()]))],
        1,
    )
    .unwrap();
    let plain_found = walkdir(plain_dir.path()).iter().any(|p| {
        std::fs::read(p).is_ok_and(|b| {
            b.windows(b"TOPSECRETVALUE".len())
                .any(|w| w == b"TOPSECRETVALUE")
        })
    });
    assert!(
        plain_found,
        "the search itself is broken — it cannot even find an unsealed value"
    );
}

fn walkdir(root: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for e in entries.flatten() {
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
            } else {
                out.push(p);
            }
        }
    }
    out
}

/// Turning sealing on for a collection that already holds cleartext rows must
/// be refused, not silently half-applied.
#[test]
fn sealing_cannot_be_switched_on_after_the_fact() {
    let dir = tempfile::tempdir().unwrap();
    let k = kernel(dir.path());
    engine_path::create(&k, "people").unwrap();
    engine_path::write_rows(&k, "people", &[("owner", TypedColumn::String(vec!["alice".into()]))], 1)
        .unwrap();

    let err = engine_path::create_for_subjects(&k, "people", "owner")
        .expect_err("must refuse");
    assert!(err.contains("in the clear"), "the refusal should say why: {}", err);
}

/// A row with no usable subject must be refused rather than stored unsealed.
#[test]
fn a_row_without_a_subject_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let k = kernel(dir.path());
    engine_path::create_for_subjects(&k, "people", "owner").unwrap();

    let err = engine_path::write_rows(
        &k,
        "people",
        &[("note", TypedColumn::String(vec!["no owner here".into()]))],
        1,
    )
    .expect_err("must refuse a row with no subject");
    assert!(err.contains("owner"), "the refusal should name the column: {}", err);
}

/// Sealing a batch must not consult the keystore once per row.
///
/// Rows in a batch overwhelmingly share a subject — that is what a batch *is*
/// for a per-subject collection — so a lookup per row turns one round trip
/// into thousands against an object store.
#[test]
fn sealing_a_batch_does_not_fetch_a_key_per_row() {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    /// Counts reads of the keystore specifically, which is what this test is
    /// about — `Metered` counts every read and cannot tell them apart.
    ///
    /// A purpose-built probe is fine; dropping the batch methods is not. The
    /// trait's batch defaults call the singular method on `self`, so a
    /// decorator that forgets to forward one unrolls it against itself and the
    /// backend's parallel implementation never runs — invisible to any request
    /// count, and the exact bug `BlobCache` shipped with. Everything is
    /// forwarded here, and `assert_forwards_batches` below checks it.
    #[derive(Default)]
    struct Counter {
        key_reads: AtomicU64,
    }

    struct Counting<S: pond_kernel::ObjectStore> {
        inner: S,
        c: Arc<Counter>,
    }

    impl<S: pond_kernel::ObjectStore> Counting<S> {
        fn count(&self, path: &str) {
            if path.starts_with("keys/") {
                self.c.key_reads.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    impl<S: pond_kernel::ObjectStore> pond_kernel::ObjectStore for Counting<S> {
        fn put_blob(&self, d: &[u8]) -> std::io::Result<String> {
            self.inner.put_blob(d)
        }
        fn get_blob(&self, h: &str) -> std::io::Result<Vec<u8>> {
            self.inner.get_blob(h)
        }
        fn put_blob_batch(&self, i: &[Vec<u8>]) -> std::io::Result<Vec<String>> {
            self.inner.put_blob_batch(i)
        }
        fn get_blob_batch(&self, h: &[String]) -> std::io::Result<Vec<Vec<u8>>> {
            self.inner.get_blob_batch(h)
        }
        fn delete_blob_batch(&self, h: &[String]) -> std::io::Result<usize> {
            self.inner.delete_blob_batch(h)
        }
        fn put_path(&self, p: &str, h: &str) -> std::io::Result<()> {
            self.inner.put_path(p, h)
        }
        fn get_path(&self, p: &str) -> Option<String> {
            self.inner.get_path(p)
        }
        fn put_object(&self, p: &str, b: &[u8]) -> std::io::Result<()> {
            self.inner.put_object(p, b)
        }
        fn get_object(&self, p: &str) -> Option<Vec<u8>> {
            self.count(p);
            self.inner.get_object(p)
        }
        fn get_object_batch(&self, p: &[String]) -> Vec<Option<Vec<u8>>> {
            for path in p {
                self.count(path);
            }
            self.inner.get_object_batch(p)
        }
        fn delete_path(&self, p: &str) -> std::io::Result<bool> {
            self.inner.delete_path(p)
        }
        fn delete_path_batch(&self, p: &[String]) -> std::io::Result<usize> {
            self.inner.delete_path_batch(p)
        }
        fn list_paths(&self, p: &str) -> std::io::Result<Vec<String>> {
            self.inner.list_paths(p)
        }
        fn blob_exists(&self, h: &str) -> bool {
            self.inner.blob_exists(h)
        }
        fn delete_blob(&self, h: &str) -> std::io::Result<bool> {
            self.inner.delete_blob(h)
        }
    }

    // The probe must not be the thing that makes the measurement wrong.
    pond_kernel::assert_forwards_batches(
        |probe| Counting {
            inner: probe,
            c: Arc::new(Counter::default()),
        },
        pond_kernel::LocalFSObjectStore::new(tempfile::tempdir().unwrap().path()).unwrap(),
    );

    const ROWS: usize = 200;
    let dir = tempfile::tempdir().unwrap();
    let c = Arc::new(Counter::default());
    let store = Counting {
        inner: pond_kernel::LocalFSObjectStore::new(dir.path()).unwrap(),
        c: c.clone(),
    };
    let k = pond_kernel::PondKernel::new_with_store(Box::new(store));
    engine_path::create_for_subjects(&k, "people", "owner").unwrap();

    // Every row belongs to the same subject — the common case.
    c.key_reads.store(0, Ordering::Relaxed);
    engine_path::write_rows(
        &k,
        "people",
        &[
            ("owner", TypedColumn::String(vec!["alice".into(); ROWS])),
            (
                "note",
                TypedColumn::String((0..ROWS).map(|i| format!("n{}", i)).collect()),
            ),
        ],
        1,
    )
    .unwrap();
    let writes = c.key_reads.load(Ordering::Relaxed);

    c.key_reads.store(0, Ordering::Relaxed);
    let _ = engine_path::read_rows(&k, "people").unwrap();
    let reads = c.key_reads.load(Ordering::Relaxed);

    println!(
        "keystore lookups for {} rows of one subject: {} sealing, {} opening",
        ROWS, writes, reads
    );
    assert!(
        writes < ROWS as u64,
        "sealing fetched the key {} times for {} rows — one per row",
        writes,
        ROWS
    );
    assert!(
        reads < ROWS as u64,
        "opening fetched the key {} times for {} rows — one per row",
        reads,
        ROWS
    );
}

/// Keys must be able to live somewhere other than the data store.
///
/// Erasure is exactly as complete as the destruction of the last copy of the
/// key. Keys held in the data store are copied by every backup, snapshot and
/// replica of the data — and restoring one undoes every erasure since it was
/// taken. The keystore is a few bytes per subject precisely so it can live
/// somewhere with a retention policy of its own.
#[test]
fn keys_can_be_held_apart_from_the_data() {
    let data_dir = tempfile::tempdir().unwrap();
    let keys_dir = tempfile::tempdir().unwrap();

    let keystore: std::sync::Arc<dyn pond_kernel::ObjectStore> = std::sync::Arc::new(
        pond_kernel::LocalFSObjectStore::new(keys_dir.path()).unwrap(),
    );
    let k = pond_kernel::PondKernel::new_local(data_dir.path())
        .unwrap()
        .with_keystore(keystore);
    assert!(k.keystore_is_separate());

    engine_path::create_for_subjects(&k, "people", "owner").unwrap();
    engine_path::write_rows(
        &k,
        "people",
        &[
            ("owner", TypedColumn::String(vec!["alice".into()])),
            ("note", TypedColumn::String(vec!["secret".into()])),
        ],
        1,
    )
    .unwrap();

    // The key is in the key store, and nowhere in the data store.
    let has_keys = |root: &std::path::Path| -> bool {
        walkdir(root)
            .iter()
            .any(|p| p.to_string_lossy().contains("keys/subject-"))
    };
    assert!(has_keys(keys_dir.path()), "the key should be in the keystore");
    assert!(
        !has_keys(data_dir.path()),
        "a key in the data store is copied by every backup of the data"
    );

    // And it still works.
    let columns = engine_path::read_rows(&k, "people").unwrap();
    match columns.iter().find(|(n, _)| n == "note") {
        Some((_, TypedColumn::String(v))) => assert_eq!(v, &vec!["secret".to_string()]),
        _ => panic!("note column missing"),
    }
}

/// An erasure must leave a record that survives it.
#[test]
fn an_erasure_is_auditable_afterwards() {
    let dir = tempfile::tempdir().unwrap();
    let k = kernel(dir.path());
    engine_path::create_for_subjects(&k, "people", "owner").unwrap();
    engine_path::write_rows(
        &k,
        "people",
        &[
            ("owner", TypedColumn::String(vec!["alice".into()])),
            ("note", TypedColumn::String(vec!["secret".into()])),
        ],
        1,
    )
    .unwrap();

    assert!(!pond_storage::subject::was_erased(&k, "alice").unwrap());

    pond_storage::subject::erase_subject_for(&k, "alice", "ticket-42").unwrap();

    assert!(
        pond_storage::subject::was_erased(&k, "alice").unwrap(),
        "the erasure must be provable afterwards"
    );
    assert!(!pond_storage::subject::was_erased(&k, "bob").unwrap());

    let log = pond_storage::subject::erasure_log(&k).unwrap();
    assert_eq!(log.len(), 1);
    assert_eq!(log[0].requested_by, "ticket-42");
    assert!(log[0].key_existed);
}

/// Rotating a subject's key must keep their data readable and make the old key
/// useless.
///
/// A key that never changes has unbounded exposure in time: anyone who
/// obtained it once can read everything that subject ever stored, including
/// rows written long afterwards.
#[test]
fn rotating_a_key_preserves_the_data_and_retires_the_old_key() {
    let dir = tempfile::tempdir().unwrap();
    let k = kernel(dir.path());
    engine_path::create_for_subjects(&k, "people", "owner").unwrap();
    engine_path::write_rows(
        &k,
        "people",
        &[
            (
                "owner",
                TypedColumn::String(vec!["alice".into(), "bob".into()]),
            ),
            (
                "note",
                TypedColumn::String(vec!["alice's note".into(), "bob's note".into()]),
            ),
        ],
        1,
    )
    .unwrap();

    // Capture the key an attacker might have taken before the rotation.
    let stolen = pond_crypto::KeyStore::new(k.keystore_handle())
        .get(&"alice".to_string())
        .unwrap()
        .expect("alice has a key");

    assert!(engine_path::rotate_subject(&k, "people", "alice", 1).unwrap());

    // The data still reads.
    let notes = |owner: &str| -> Option<String> {
        let columns = engine_path::read_rows(&k, "people").unwrap();
        let owners = match columns.iter().find(|(n, _)| n == "owner") {
            Some((_, TypedColumn::String(v))) => v.clone(),
            _ => return None,
        };
        let notes = match columns.iter().find(|(n, _)| n == "note") {
            Some((_, TypedColumn::String(v))) => v.clone(),
            _ => return None,
        };
        owners.iter().position(|o| o == owner).map(|i| notes[i].clone())
    };
    assert_eq!(notes("alice"), Some("alice's note".to_string()));
    assert_eq!(
        notes("bob"),
        Some("bob's note".to_string()),
        "rotating one subject must not disturb another"
    );

    // The old key is retired.
    let current = pond_crypto::KeyStore::new(k.keystore_handle())
        .get(&"alice".to_string())
        .unwrap()
        .expect("alice still has a key");
    assert_ne!(
        current.as_bytes(),
        stolen.as_bytes(),
        "rotation must actually change the key"
    );

    // And erasure still works afterwards, against the rotated key. A rotation
    // that left the subject unerasable would be worse than no rotation.
    pond_storage::subject::erase_subject(&k, "alice").unwrap();
    assert_eq!(
        notes("alice"),
        Some(String::new()),
        "after erasure the field is absent, which a dense column shows as empty"
    );
    assert_eq!(
        notes("bob"),
        Some("bob's note".to_string()),
        "erasing alice must still not touch bob"
    );
}

/// Rotating a subject who has no key must not mint one.
///
/// A subject with no key was erased, or never seen. Creating one would quietly
/// restore the ability to store data for somebody who asked to be forgotten.
#[test]
fn rotating_an_erased_subject_does_not_revive_them() {
    let dir = tempfile::tempdir().unwrap();
    let k = kernel(dir.path());
    engine_path::create_for_subjects(&k, "people", "owner").unwrap();
    engine_path::write_rows(
        &k,
        "people",
        &[
            ("owner", TypedColumn::String(vec!["alice".into()])),
            ("note", TypedColumn::String(vec!["secret".into()])),
        ],
        1,
    )
    .unwrap();

    pond_storage::subject::erase_subject(&k, "alice").unwrap();
    assert!(
        !engine_path::rotate_subject(&k, "people", "alice", 1).unwrap(),
        "rotation must report that there was nothing to rotate"
    );
    assert!(
        pond_crypto::KeyStore::new(k.keystore_handle())
            .get(&"alice".to_string())
            .unwrap()
            .is_none(),
        "an erased subject must not get a key back from a rotation"
    );
}

/// Rotation is refused on a collection that does not seal rows.
#[test]
fn rotation_is_refused_where_there_is_nothing_to_rotate() {
    let dir = tempfile::tempdir().unwrap();
    let k = kernel(dir.path());
    engine_path::create(&k, "plain").unwrap();
    engine_path::write_rows(&k, "plain", &[("id", TypedColumn::Int64(vec![1]))], 1).unwrap();

    let err = engine_path::rotate_subject(&k, "plain", "alice", 1).expect_err("must refuse");
    assert!(err.contains("does not seal"), "should say why: {}", err);
}

/// A write that races a rotation must survive it.
///
/// Rotation is not atomic: it scans, re-seals, publishes, then installs the
/// new key. A write landing inside that window seals under the *old* key, and
/// if the old key were simply discarded the value would be unreadable —
/// destroyed by an operation whose entire purpose is to preserve it.
///
/// Keeping the displaced key readable until the rotation is explicitly
/// finished is what makes that write survive.
#[test]
fn a_write_that_races_a_rotation_is_not_lost() {
    let dir = tempfile::tempdir().unwrap();
    let k = kernel(dir.path());
    engine_path::create_for_subjects(&k, "people", "owner").unwrap();

    engine_path::write_rows(
        &k,
        "people",
        &[
            ("_rowid", TypedColumn::String(vec!["r1".into()])),
            ("owner", TypedColumn::String(vec!["alice".into()])),
            ("note", TypedColumn::String(vec!["before".into()])),
        ],
        1,
    )
    .unwrap();

    // Simulate the race: a row sealed under the key that is current *now*,
    // published, and only afterwards does the rotation install a new key.
    // Writing it before `rotate_subject` scans is exactly the window.
    let racing = pond_crypto::KeyStore::new(k.keystore_handle())
        .get(&"alice".to_string())
        .unwrap()
        .expect("alice has a key");
    let sealed = pond_crypto::seal(
        &racing,
        b"people\x1fnote",
        &pond_record::encode_value(&pond_record::Value::Str("raced".into())),
    );

    assert!(engine_path::rotate_subject(&k, "people", "alice", 1).unwrap());

    // Now land the racing row, sealed under the pre-rotation key.
    {
        let mut rec = pond_record::Record::new();
        let v = pond_record::Version::new(pond_kernel::crdt::current_time_ms() + 1, 0, 9);
        rec = rec.with_field("owner", pond_record::Value::Str("alice".into()), v);
        rec = rec.with_field("note", pond_record::Value::Bytes(sealed), v);
        let mut e = pond_engine::Engine::open_with(
            k.store_handle(),
            9,
            pond_cache::CacheConfig::default(),
            definition::load(&k, "people").engine_config(),
        )
        .unwrap();
        e.write_records(
            "people",
            vec![(
                pond_index::Key::new(vec![pond_index::str_("r1")]),
                rec,
            )],
        )
        .unwrap();
        e.publish().unwrap();
    }

    let note = || -> Option<String> {
        let columns = engine_path::read_rows(&k, "people").unwrap();
        match columns.iter().find(|(n, _)| n == "note") {
            Some((_, TypedColumn::String(v))) => v.first().cloned(),
            _ => None,
        }
    };
    assert_eq!(
        note(),
        Some("raced".to_string()),
        "a write sealed under the pre-rotation key must still be readable"
    );

    // Once the rotation is finished the old key is gone, and with it the only
    // way to read anything still sealed under it. The whole column disappears
    // here because it was the only row carrying it — a field no record has is
    // not a column.
    assert!(pond_storage::subject::finish_rotation(&k, "alice").unwrap());
    assert!(
        matches!(note().as_deref(), None | Some("")),
        "after finishing, the displaced key must open nothing — got {:?}",
        note()
    );
    // The row itself is still there; only the field it could not open is gone.
    let columns = engine_path::read_rows(&k, "people").unwrap();
    match columns.iter().find(|(n, _)| n == "owner") {
        Some((_, TypedColumn::String(v))) => {
            assert_eq!(v, &vec!["alice".to_string()], "the row must survive")
        }
        _ => panic!("the owner column must remain readable"),
    }
}

/// Erasure must destroy the displaced key too.
///
/// An erasure that left a rotation's previous key behind would leave the
/// subject's older rows readable — which is not an erasure, however it is
/// reported.
#[test]
fn erasure_destroys_the_rotations_previous_key_as_well() {
    let dir = tempfile::tempdir().unwrap();
    let k = kernel(dir.path());
    engine_path::create_for_subjects(&k, "people", "owner").unwrap();
    engine_path::write_rows(
        &k,
        "people",
        &[
            ("owner", TypedColumn::String(vec!["alice".into()])),
            ("note", TypedColumn::String(vec!["secret".into()])),
        ],
        1,
    )
    .unwrap();

    engine_path::rotate_subject(&k, "people", "alice", 1).unwrap();
    let store = pond_crypto::KeyStore::new(k.keystore_handle());
    assert_eq!(
        store.get_all(&"alice".to_string()).unwrap().len(),
        2,
        "a rotation in flight keeps both keys"
    );

    pond_storage::subject::erase_subject(&k, "alice").unwrap();
    assert!(
        store.get_all(&"alice".to_string()).unwrap().is_empty(),
        "erasure must destroy every key that can open the subject's data"
    );
}

/// A rotation in flight must not make the subject appear twice.
#[test]
fn a_rotation_does_not_duplicate_the_subject_in_listings() {
    let dir = tempfile::tempdir().unwrap();
    let k = kernel(dir.path());
    engine_path::create_for_subjects(&k, "people", "owner").unwrap();
    engine_path::write_rows(
        &k,
        "people",
        &[
            ("owner", TypedColumn::String(vec!["alice".into()])),
            ("note", TypedColumn::String(vec!["x".into()])),
        ],
        1,
    )
    .unwrap();
    engine_path::rotate_subject(&k, "people", "alice", 1).unwrap();

    let subjects = pond_storage::subject::subjects(&k).unwrap();
    assert_eq!(
        subjects,
        vec!["alice".to_string()],
        "the displaced key is the same subject, not another one"
    );
}

/// Mirrors what the Python binding does: write two subjects, erase one, and
/// check the surviving subject's value is still there *through PND2*, which is
/// the path SQL, the lenses and Python all take.
#[test]
fn a_survivors_value_reads_through_pnd2_after_someone_else_is_erased() {
    let dir = tempfile::tempdir().unwrap();
    let k = kernel(dir.path());
    engine_path::create_for_subjects(&k, "people", "owner").unwrap();
    engine_path::write_rows(
        &k,
        "people",
        &[
            (
                "owner",
                TypedColumn::String(vec!["alice".into(), "bob".into()]),
            ),
            (
                "note",
                TypedColumn::String(vec!["alice secret".into(), "bob secret".into()]),
            ),
        ],
        1,
    )
    .unwrap();

    pond_storage::subject::erase_subject(&k, "alice").unwrap();

    let blob = engine_path::read_pnd2(&k, "people").unwrap();
    let cols = pond_core::decode::pnd2_decode(&blob).unwrap();
    let rows = pond_core::to_json::columns_to_json_rows(&cols, true);
    let notes: Vec<String> = rows
        .iter()
        .filter_map(|r| r.get("note").and_then(|v| v.as_str()).map(String::from))
        .collect();
    assert!(
        notes.contains(&"bob secret".to_string()),
        "the surviving subject's value must read through PND2: {:?} (columns: {:?})",
        notes,
        cols.iter().map(|c| c.name.to_string_lossy().into_owned()).collect::<Vec<_>>()
    );
}

/// Branching records where the branch came from.
///
/// A branch in the engine model is an independent collection that shares
/// structure with its source, not a ref inside one — which is what makes it an
/// O(1) pointer copy. The cost of that model is that nothing in the store knew
/// the relationship, so `pond branch` reported success while `pond branches`
/// reported none, each describing a different model truthfully.
#[test]
fn a_branch_records_its_source_and_still_diverges_freely() {
    let dir = tempfile::tempdir().unwrap();
    let k = pond_kernel::PondKernel::new_local(dir.path()).unwrap();

    engine_path::create(&k, "trunk").unwrap();
    engine_path::write_rows(
        &k,
        "trunk",
        &[("id", TypedColumn::Int64(vec![1, 2]))],
        1,
    )
    .unwrap();

    engine_path::branch(&k, "trunk", "feature", 1).unwrap();

    let trunk = pond_storage::definition::load(&k, "trunk");
    let feature = pond_storage::definition::load(&k, "feature");
    assert_eq!(trunk.branched_from, None, "a trunk was not branched from anything");
    assert_eq!(feature.branched_from.as_deref(), Some("trunk"));

    // The branch inherits the pinned configuration — a branch that chunked
    // differently would share no nodes with its source.
    assert_eq!(feature.chunk_salt, trunk.chunk_salt);
    assert_eq!(feature.chunk_target, trunk.chunk_target);
    assert_eq!(feature.spill_threshold, trunk.spill_threshold);

    // Provenance confers no behaviour: the branch diverges like any other
    // collection, and the source is untouched.
    engine_path::write_rows(&k, "feature", &[("id", TypedColumn::Int64(vec![3]))], 1).unwrap();
    let count = |c: &str| match engine_path::read_rows(&k, c)
        .unwrap()
        .into_iter()
        .find(|(n, _)| n == "id")
    {
        Some((_, TypedColumn::Int64(v))) => v.len(),
        _ => 0,
    };
    assert_eq!(count("feature"), 3);
    assert_eq!(count("trunk"), 2, "writing to a branch must not touch its source");

    // And a branch of a branch chains rather than flattening.
    engine_path::branch(&k, "feature", "nested", 1).unwrap();
    assert_eq!(
        pond_storage::definition::load(&k, "nested")
            .branched_from
            .as_deref(),
        Some("feature")
    );
}

/// Merging one engine collection into another, and the properties that make
/// it safe to do more than once.
#[test]
fn merging_a_branch_back_is_idempotent_and_leaves_the_source_alone() {
    let dir = tempfile::tempdir().unwrap();
    let k = pond_kernel::PondKernel::new_local(dir.path()).unwrap();

    let ids = |c: &str| -> Vec<i64> {
        match engine_path::read_rows(&k, c)
            .unwrap()
            .into_iter()
            .find(|(n, _)| n == "id")
        {
            Some((_, TypedColumn::Int64(mut v))) => {
                v.sort();
                v
            }
            _ => Vec::new(),
        }
    };

    engine_path::create(&k, "trunk").unwrap();
    engine_path::write_rows(&k, "trunk", &[("id", TypedColumn::Int64(vec![1, 2]))], 1).unwrap();
    engine_path::branch(&k, "trunk", "feature", 1).unwrap();

    // Each side gains a row the other has never seen.
    engine_path::write_rows(&k, "feature", &[("id", TypedColumn::Int64(vec![3]))], 2).unwrap();
    engine_path::write_rows(&k, "trunk", &[("id", TypedColumn::Int64(vec![4]))], 1).unwrap();
    assert_eq!(ids("trunk"), vec![1, 2, 4]);
    assert_eq!(ids("feature"), vec![1, 2, 3]);

    engine_path::merge(&k, "trunk", "feature", 1).unwrap();
    assert_eq!(ids("trunk"), vec![1, 2, 3, 4], "the union, by key");
    assert_eq!(
        ids("feature"),
        vec![1, 2, 3],
        "a merge *into* trunk must leave the source exactly as it was"
    );

    // Idempotent — which is what makes it safe to retry after a failure.
    engine_path::merge(&k, "trunk", "feature", 1).unwrap();
    assert_eq!(ids("trunk"), vec![1, 2, 3, 4]);

    // And the other direction converges on the same set, because merge is a
    // semilattice join rather than an ordered replay.
    engine_path::merge(&k, "feature", "trunk", 2).unwrap();
    assert_eq!(ids("feature"), vec![1, 2, 3, 4]);
    assert_eq!(ids("trunk"), vec![1, 2, 3, 4]);
}

/// A merge refuses the cases where it would look like work and do none.
#[test]
fn merge_refuses_what_it_cannot_meaningfully_do() {
    let dir = tempfile::tempdir().unwrap();
    let k = pond_kernel::PondKernel::new_local(dir.path()).unwrap();

    engine_path::create(&k, "a").unwrap();
    engine_path::write_rows(&k, "a", &[("id", TypedColumn::Int64(vec![1]))], 1).unwrap();

    let err = engine_path::merge(&k, "a", "a", 1).unwrap_err();
    assert!(err.contains("itself"), "self-merge should say so: {}", err);

    let err = engine_path::merge(&k, "a", "nonexistent", 1).unwrap_err();
    assert!(
        err.contains("nothing to merge") || err.contains("not engine-backed"),
        "merging from a collection that does not exist should say so: {}",
        err
    );

    // Merging into an empty collection is a copy; it should point at the
    // operation that means "copy" rather than silently performing one.
    engine_path::create(&k, "empty").unwrap();
    let err = engine_path::merge(&k, "empty", "a", 1).unwrap_err();
    assert!(err.contains("branch"), "should suggest branch: {}", err);
}

/// Per-field merge survives a collection merge, which is the property that
/// makes it a join rather than a last-writer-wins overwrite.
#[test]
fn merging_collections_keeps_per_field_resolution() {
    let dir = tempfile::tempdir().unwrap();
    let k = pond_kernel::PondKernel::new_local(dir.path()).unwrap();

    engine_path::create(&k, "trunk").unwrap();
    engine_path::write_rows(
        &k,
        "trunk",
        &[
            ("_rowid", TypedColumn::String(vec!["r1".into()])),
            ("left", TypedColumn::String(vec!["from-trunk".into()])),
        ],
        1,
    )
    .unwrap();
    engine_path::branch(&k, "trunk", "feature", 1).unwrap();

    // The branch sets a *different* field on the same row.
    engine_path::write_rows(
        &k,
        "feature",
        &[
            ("_rowid", TypedColumn::String(vec!["r1".into()])),
            ("right", TypedColumn::String(vec!["from-branch".into()])),
        ],
        2,
    )
    .unwrap();

    engine_path::merge(&k, "trunk", "feature", 1).unwrap();

    let cols = engine_path::read_rows(&k, "trunk").unwrap();
    let field = |name: &str| -> Option<String> {
        cols.iter().find(|(n, _)| n == name).and_then(|(_, c)| match c {
            TypedColumn::String(v) => v.first().cloned(),
            _ => None,
        })
    };
    assert_eq!(
        field("left").as_deref(),
        Some("from-trunk"),
        "the field only trunk set must survive the merge"
    );
    assert_eq!(
        field("right").as_deref(),
        Some("from-branch"),
        "and so must the field only the branch set — a whole-record overwrite \
         would have dropped one of them"
    );
}

/// A projected read must return the same values as an unprojected one, and
/// cost less.
///
/// The cost is the point, but correctness is the precondition: a projection
/// that changes an answer is worse than no projection at all.
#[test]
fn projecting_a_read_changes_the_cost_and_not_the_answer() {
    use std::sync::Arc;

    // Counted at the backend, so the disk cache has to be off. With it on, the
    // write below populates the cache and both reads then fetch the same bytes
    // from the store — the saving is real but invisible from here, because the
    // cache absorbed it. This measures the storage layer, not the cache in
    // front of it.
    std::env::set_var("POND_CACHE_DIR", "off");

    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(pond_kernel::Metered::new(
        pond_kernel::LocalFSObjectStore::new(dir.path()).unwrap(),
    ));
    let k = pond_kernel::PondKernel::new_with_store(Box::new(Arc::clone(&store)));

    engine_path::create(&k, "docs").unwrap();
    let big = "y".repeat(64 * 1024);
    engine_path::write_rows(
        &k,
        "docs",
        &[
            ("id", TypedColumn::Int64(vec![1, 2, 3])),
            (
                "tag",
                TypedColumn::String(vec!["a".into(), "b".into(), "c".into()]),
            ),
            (
                "attachment",
                TypedColumn::String(vec![big.clone(), big.clone(), big.clone()]),
            ),
        ],
        1,
    )
    .unwrap();

    let value_of = |cols: &[(String, TypedColumn)], name: &str| -> Vec<String> {
        match cols.iter().find(|(n, _)| n == name) {
            Some((_, TypedColumn::String(v))) => v.clone(),
            Some((_, TypedColumn::Int64(v))) => v.iter().map(|i| i.to_string()).collect(),
            _ => Vec::new(),
        }
    };

    store.reset();
    let full = engine_path::read_rows(&k, "docs").unwrap();
    let full_bytes = store.stats().bytes_read;

    store.reset();
    let projected = engine_path::read_rows_projected(&k, "docs", &["id", "tag"]).unwrap();
    let projected_bytes = store.stats().bytes_read;

    // Same answers for the columns that were asked for.
    assert_eq!(value_of(&full, "id"), value_of(&projected, "id"));
    assert_eq!(value_of(&full, "tag"), value_of(&projected, "tag"));
    // Sorted before comparing: rows are keyed by UUIDv7, so scan order is key
    // order and not insertion order. Asserting insertion order would be
    // asserting something the engine does not promise.
    let sorted = |mut v: Vec<String>| {
        v.sort();
        v
    };
    assert_eq!(sorted(value_of(&projected, "id")), vec!["1", "2", "3"]);
    assert_eq!(sorted(value_of(&projected, "tag")), vec!["a", "b", "c"]);

    // The column that was not asked for is absent, not blank — a blank would
    // be indistinguishable from a row that genuinely has no attachment.
    assert!(
        !projected.iter().any(|(n, _)| n == "attachment"),
        "an unprojected column must be absent, not empty: {:?}",
        projected.iter().map(|(n, _)| n).collect::<Vec<_>>()
    );
    assert_eq!(value_of(&full, "attachment").len(), 3);

    println!(
        "read of 3 rows with a 64 KiB attachment: {} bytes full, {} bytes projected",
        full_bytes, projected_bytes
    );
    assert!(
        projected_bytes * 4 < full_bytes,
        "a projection that does not fetch the large column should cost far \
         less: {} vs {} bytes",
        projected_bytes,
        full_bytes
    );
}
