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
