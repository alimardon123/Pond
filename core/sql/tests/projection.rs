// projection.rs — SELECT must not fetch what it does not need, and must never
// fetch too little.
//
// Pushing a projection into the scan is worth real money on rows with a large
// field. It is also the kind of optimisation whose failure mode is a *wrong*
// answer rather than a slow one: a predicate on a column that was never
// fetched evaluates against a missing value and matches nothing, and nothing
// about that looks like an error. So these tests care much more about the
// queries where projection must be declined than about the ones where it pays.

use std::sync::Arc;

use pond_core::encode::TypedColumn;
use pond_kernel::{LocalFSObjectStore, Metered, PondKernel};
use pond_storage::{engine_path, UnifiedStorage};

/// These tests count bytes at the *backend*, so the disk cache has to be off.
///
/// With it on, the seed's writes populate the cache and both queries then read
/// the same 209 bytes from the store — the projection saving is real but
/// invisible at this layer, because the cache absorbed it. Measuring the
/// storage layer's I/O means reading the storage layer, not the cache in front
/// of it.
///
/// Every test in this binary wants the same thing, and each integration test
/// file is its own process, so setting it once per test is safe: the value is
/// identical whoever writes it.
fn cold_backend() {
    std::env::set_var("POND_CACHE_DIR", "off");
}

fn pond(dir: &std::path::Path) -> (Arc<Metered<LocalFSObjectStore>>, UnifiedStorage) {
    cold_backend();
    let store = Arc::new(Metered::new(LocalFSObjectStore::new(dir).unwrap()));
    let kernel = PondKernel::new_with_store(Box::new(Arc::clone(&store)));
    (store, UnifiedStorage::new(kernel))
}

/// Three rows, each with two small columns and one large one.
fn seed(storage: &UnifiedStorage) {
    engine_path::create(storage.kernel(), "docs").unwrap();
    let big = "y".repeat(64 * 1024);
    engine_path::write_rows(
        storage.kernel(),
        "docs",
        &[
            ("id", TypedColumn::Int64(vec![1, 2, 3])),
            (
                "tag",
                TypedColumn::String(vec!["a".into(), "b".into(), "c".into()]),
            ),
            (
                "body",
                TypedColumn::String(vec![big.clone(), big.clone(), big]),
            ),
        ],
        1,
    )
    .unwrap();
}

fn run(storage: &UnifiedStorage, sql: &str) -> pond_sql::SqlResult {
    pond_sql::execute(storage, sql).unwrap_or_else(|e| panic!("{}: {}", sql, e))
}

#[test]
fn selecting_small_columns_does_not_fetch_the_large_one() {
    let dir = tempfile::tempdir().unwrap();
    let (store, storage) = pond(dir.path());
    seed(&storage);

    store.reset();
    let projected = run(&storage, "SELECT id, tag FROM docs");
    let projected_bytes = store.stats().bytes_read;

    store.reset();
    let full = run(&storage, "SELECT * FROM docs");
    let full_bytes = store.stats().bytes_read;

    assert_eq!(projected.rows.len(), 3);
    assert_eq!(full.rows.len(), 3);
    println!(
        "SELECT id, tag: {} bytes; SELECT *: {} bytes",
        projected_bytes, full_bytes
    );
    assert!(
        projected_bytes * 4 < full_bytes,
        "a projected SELECT should not pull the large column: {} vs {}",
        projected_bytes,
        full_bytes
    );
}

/// The case that would genuinely return nothing: filtering on a **large**
/// column the SELECT list does not mention.
///
/// This is the one that bites. Projection today drops the payloads of *spilled*
/// fields only — small columns ride along in the record whether or not they
/// were asked for — so a predicate on a small unselected column works by
/// accident. A predicate on a large one does not: its payload is exactly what
/// a naive projection declines to fetch, and the comparison then runs against
/// a field that is not there and matches nothing.
///
/// Verified to fail when `projection_for` stops folding in the WHERE columns.
#[test]
fn a_predicate_on_an_unselected_large_column_still_matches() {
    let dir = tempfile::tempdir().unwrap();
    let (_s, storage) = pond(dir.path());

    // One row's large column is distinguishable from the others.
    engine_path::create(storage.kernel(), "docs").unwrap();
    let needle = "n".repeat(64 * 1024);
    let filler = "f".repeat(64 * 1024);
    engine_path::write_rows(
        storage.kernel(),
        "docs",
        &[
            ("id", TypedColumn::Int64(vec![1, 2, 3])),
            ("tag", TypedColumn::String(vec!["a".into(), "b".into(), "c".into()])),
            (
                "body",
                TypedColumn::String(vec![filler.clone(), needle.clone(), filler]),
            ),
        ],
        1,
    )
    .unwrap();

    let r = run(&storage, &format!("SELECT id, tag FROM docs WHERE body = '{}'", needle));
    assert_eq!(
        r.rows.len(),
        1,
        "a predicate on an unselected large column must still match — a \
         projection that declines to fetch it makes this silently empty: {:?}",
        r.rows.len()
    );
    assert_eq!(r.rows[0].get("tag").and_then(|v| v.as_str()), Some("b"));

    // And ORDER BY on a large unselected column has the same shape.
    let r = run(&storage, "SELECT id FROM docs ORDER BY body DESC LIMIT 1");
    assert_eq!(
        r.rows[0].get("id").and_then(|v| v.as_i64()),
        Some(2),
        "ORDER BY on an unselected large column must still order by its value"
    );
}

/// The same on a small column. Passes today whether or not the WHERE columns
/// are folded in, because small fields ride along in the record — kept so the
/// behaviour is pinned if that ever changes.
#[test]
fn a_predicate_on_an_unselected_column_still_matches() {
    let dir = tempfile::tempdir().unwrap();
    let (_s, storage) = pond(dir.path());
    seed(&storage);

    let r = run(&storage, "SELECT tag FROM docs WHERE id = 2");
    assert_eq!(
        r.rows.len(),
        1,
        "WHERE on an unselected column must still match: {:?}",
        r.rows
    );
    assert_eq!(r.rows[0].get("tag").and_then(|v| v.as_str()), Some("b"));
}

/// ORDER BY on an unselected column has the same failure mode: it does not
/// error, it just sorts by a value that is not there.
#[test]
fn ordering_by_an_unselected_column_still_orders() {
    let dir = tempfile::tempdir().unwrap();
    let (_s, storage) = pond(dir.path());
    seed(&storage);

    let r = run(&storage, "SELECT tag FROM docs ORDER BY id DESC");
    let tags: Vec<&str> = r
        .rows
        .iter()
        .filter_map(|row| row.get("tag").and_then(|v| v.as_str()))
        .collect();
    assert_eq!(
        tags,
        vec!["c", "b", "a"],
        "ORDER BY on an unselected column must still order"
    );
}

/// GROUP BY and HAVING name columns too.
#[test]
fn grouping_by_an_unselected_column_still_groups() {
    let dir = tempfile::tempdir().unwrap();
    let (_s, storage) = pond(dir.path());
    seed(&storage);

    let r = run(&storage, "SELECT COUNT(*) FROM docs GROUP BY tag");
    assert_eq!(r.rows.len(), 3, "three distinct tags: {:?}", r.rows);
}

/// A projected query and an unprojected one must agree on every value they
/// both return. The optimisation is only allowed to change the cost.
#[test]
fn projection_changes_the_cost_and_not_the_answer() {
    let dir = tempfile::tempdir().unwrap();
    let (_s, storage) = pond(dir.path());
    seed(&storage);

    let projected = run(&storage, "SELECT id, tag FROM docs WHERE id > 1");
    let full = run(&storage, "SELECT * FROM docs WHERE id > 1");

    assert_eq!(projected.rows.len(), full.rows.len());
    let pick = |r: &pond_sql::SqlResult| -> Vec<(i64, String)> {
        let mut v: Vec<(i64, String)> = r
            .rows
            .iter()
            .map(|row| {
                (
                    row.get("id").and_then(|v| v.as_i64()).unwrap_or(-1),
                    row.get("tag")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string(),
                )
            })
            .collect();
        v.sort();
        v
    };
    assert_eq!(pick(&projected), pick(&full));
}

/// `SELECT *` must keep every column, including the large one.
#[test]
fn select_star_still_returns_everything() {
    let dir = tempfile::tempdir().unwrap();
    let (_s, storage) = pond(dir.path());
    seed(&storage);

    let r = run(&storage, "SELECT * FROM docs");
    let row = &r.rows[0];
    assert!(row.get("id").is_some());
    assert!(row.get("tag").is_some());
    assert_eq!(
        row.get("body").and_then(|v| v.as_str()).map(|s| s.len()),
        Some(64 * 1024),
        "SELECT * must return the large column whole"
    );
}

/// Selecting the large column explicitly must still return it.
#[test]
fn selecting_the_large_column_returns_it() {
    let dir = tempfile::tempdir().unwrap();
    let (_s, storage) = pond(dir.path());
    seed(&storage);

    let r = run(&storage, "SELECT id, body FROM docs WHERE id = 1");
    assert_eq!(r.rows.len(), 1);
    assert_eq!(
        r.rows[0].get("body").and_then(|v| v.as_str()).map(|s| s.len()),
        Some(64 * 1024)
    );
}
