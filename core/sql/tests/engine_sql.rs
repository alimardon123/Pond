// engine_sql.rs — SQL over an engine-backed collection.
//
// The SQL executor is the surface most likely to expose a divergence between
// the two storage paths, because it is the one that reads rows back, mutates
// them, and reads them again. If the engine path presents rows differently, or
// loses a column on update, or fails to hide a deleted row, a SELECT after the
// fact is where it shows up.
//
// These are the same statements the legacy suite runs, against a collection
// created with `engine_path::create`.

use pond_sql::execute;
use pond_storage::UnifiedStorage;
use serde_json::Value as JsonValue;

fn storage(dir: &tempfile::TempDir) -> UnifiedStorage {
    UnifiedStorage::new_local(dir.path()).expect("open storage")
}

fn engine_users(s: &UnifiedStorage) {
    pond_storage::engine_path::create(s.kernel(), "users").expect("create engine collection");
    execute(
        s,
        "INSERT INTO users (id, name, age) VALUES (1, 'ada', 36), (2, 'grace', 45), (3, 'alan', 41)",
    )
    .expect("insert");
}

fn rows(result: &pond_sql::executor::SqlResult) -> &[JsonValue] {
    &result.rows
}

#[test]
fn select_all_from_an_engine_collection() {
    let dir = tempfile::tempdir().unwrap();
    let s = storage(&dir);
    engine_users(&s);

    let r = execute(&s, "SELECT * FROM users").expect("select");
    assert_eq!(rows(&r).len(), 3, "all three inserted rows must come back");
}

#[test]
fn select_with_where_and_projection() {
    let dir = tempfile::tempdir().unwrap();
    let s = storage(&dir);
    engine_users(&s);

    let r = execute(&s, "SELECT name FROM users WHERE age > 40").expect("select");
    let mut names: Vec<String> = rows(&r)
        .iter()
        .filter_map(|row| row.get("name").and_then(|v| v.as_str()).map(String::from))
        .collect();
    names.sort();
    assert_eq!(names, vec!["alan".to_string(), "grace".to_string()]);
}

/// An UPDATE must change the named columns and leave the rest alone.
///
/// On the engine path the update is a field-level merge rather than a rewrite,
/// so this is the test that would catch a column being dropped by a statement
/// that never mentioned it.
#[test]
fn update_changes_only_what_it_names() {
    let dir = tempfile::tempdir().unwrap();
    let s = storage(&dir);
    engine_users(&s);

    execute(&s, "UPDATE users SET age = 37 WHERE name = 'ada'").expect("update");

    let r = execute(&s, "SELECT * FROM users WHERE name = 'ada'").expect("select");
    assert_eq!(rows(&r).len(), 1, "still exactly one ada");
    let row = &rows(&r)[0];
    assert_eq!(row.get("age").and_then(|v| v.as_i64()), Some(37));
    assert_eq!(
        row.get("id").and_then(|v| v.as_i64()),
        Some(1),
        "a column the UPDATE did not mention must survive it"
    );

    // And nothing else changed.
    let all = execute(&s, "SELECT * FROM users").expect("select");
    assert_eq!(rows(&all).len(), 3);
}

#[test]
fn delete_removes_the_row_from_later_selects() {
    let dir = tempfile::tempdir().unwrap();
    let s = storage(&dir);
    engine_users(&s);

    execute(&s, "DELETE FROM users WHERE name = 'grace'").expect("delete");

    let r = execute(&s, "SELECT * FROM users").expect("select");
    let names: Vec<String> = rows(&r)
        .iter()
        .filter_map(|row| row.get("name").and_then(|v| v.as_str()).map(String::from))
        .collect();
    assert_eq!(rows(&r).len(), 2, "one row removed, got {:?}", names);
    assert!(!names.contains(&"grace".to_string()));
}

/// Aggregates read through the engine the same as through the legacy path.
#[test]
fn aggregates_work_over_engine_data() {
    let dir = tempfile::tempdir().unwrap();
    let s = storage(&dir);
    engine_users(&s);

    let r = execute(&s, "SELECT COUNT(*) FROM users").expect("count");
    assert_eq!(rows(&r).len(), 1);
}

/// A second INSERT adds rows rather than replacing the first batch.
#[test]
fn a_second_insert_accumulates() {
    let dir = tempfile::tempdir().unwrap();
    let s = storage(&dir);
    engine_users(&s);

    execute(&s, "INSERT INTO users (id, name, age) VALUES (4, 'edsger', 72)").expect("insert");

    let r = execute(&s, "SELECT * FROM users").expect("select");
    assert_eq!(rows(&r).len(), 4);
}

/// SQL and the columnar API must agree about nulls.
///
/// They did not: when per-value nulls were added, the CLI's converter learned
/// to report them and the SQL executor's copy did not, so `SELECT` returned a
/// zero where `read-rows` returned null for the same row. Three
/// implementations of one conversion is what allowed that, and there is now
/// one, in `pond_core::to_json`.
#[test]
fn sql_reports_nulls_rather_than_the_types_zero() {
    let dir = tempfile::tempdir().unwrap();
    let s = storage(&dir);
    pond_storage::engine_path::create(s.kernel(), "t").expect("create");

    // A real zero and a null, side by side.
    execute(&s, "INSERT INTO t (id, score) VALUES (1, 0)").expect("insert zero");
    pond_storage::engine_path::write_rows_with_nulls(
        s.kernel(),
        "t",
        &[
            ("id", pond_core::encode::TypedColumn::Int64(vec![2])),
            ("score", pond_core::encode::TypedColumn::Int64(vec![0])),
        ],
        &[None, Some(vec![true])],
        1,
    )
    .expect("insert null");

    let r = execute(&s, "SELECT * FROM t").expect("select");
    let by_id = |id: i64| -> &JsonValue {
        rows(&r)
            .iter()
            .find(|row| row.get("id").and_then(|v| v.as_i64()) == Some(id))
            .expect("row present")
    };

    assert_eq!(
        by_id(1).get("score"),
        Some(&JsonValue::Number(0.into())),
        "a zero the caller wrote must stay a zero"
    );
    assert_eq!(
        by_id(2).get("score"),
        Some(&JsonValue::Null),
        "a null must not be reported as the type's zero"
    );
}
