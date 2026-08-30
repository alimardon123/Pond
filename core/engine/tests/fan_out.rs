// fan_out.rs — a scan must wait per level, not per node.
//
// This is the property with no correctness signal. A traversal that reads one
// node per round trip returns exactly the same rows as one that reads a level
// at a time; only the wall clock differs, and no assertion about *results* can
// see it. So it has to be asserted directly, or it regresses the moment
// someone reintroduces a recursive descent — which is what the code did until
// measured: 51 sequential waits for a 100,000-row scan, ~1.6 s at
// object-storage latency, with every leaf hash already known from the internal
// nodes before the first leaf was fetched.
//
// `Metered` separates the two numbers this is about. `requests` is what the
// object store bills — batching does not reduce it and is not meant to.
// `round_trips` is what the caller waits for. When the two track each other,
// nothing is running in parallel.

use std::sync::Arc;

use pond_engine::{Engine, Reader};
use pond_index::{int, Key};
use pond_kernel::{LocalFSObjectStore, Metered};
use pond_record::{Record, Value, Version};

const ROWS: usize = 50_000;

fn record(i: u64) -> Record {
    let mut r = Record::new();
    r.set("v", Value::Int(i as i64), Version::new(i, 0, 1));
    r
}

fn populated() -> (tempfile::TempDir, Arc<Metered<LocalFSObjectStore>>) {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Metered::new(LocalFSObjectStore::new(dir.path()).unwrap()));
    let mut engine = Engine::open(store.clone(), 1).unwrap();
    let rows: Vec<(Key, Record)> = (0..ROWS)
        .map(|i| (Key::new(vec![int(i as i64)]), record(i as u64)))
        .collect();
    for chunk in rows.chunks(10_000) {
        engine.write_records("t", chunk.to_vec()).unwrap();
    }
    engine.publish().unwrap();
    (dir, store)
}

/// A full scan must issue many requests across few waits.
///
/// The bound is deliberately loose — this is not a benchmark and should not
/// fail on a chunking change that alters the node count. What it refuses is
/// the shape the code had: one wait per node.
#[test]
fn a_full_scan_waits_per_level_not_per_node() {
    let (dir, _) = populated();

    let store = Arc::new(Metered::new(LocalFSObjectStore::new(dir.path()).unwrap()));
    let mut reader = Reader::open(store.clone()).unwrap();
    store.reset();

    let rows = reader.scan("t").unwrap();
    assert_eq!(rows.len(), ROWS, "the scan must still return everything");

    let s = store.stats();
    assert!(
        s.requests() > 20,
        "this collection is too small to be testing fan-out: {} requests",
        s.requests()
    );
    assert!(
        s.round_trips <= 8,
        "a scan of {} rows took {} round trips for {} requests (width {:.1}): \
         the traversal is waiting per node again, not per level",
        ROWS,
        s.round_trips,
        s.requests(),
        s.batch_width()
    );
}

/// The same for a range scan, which prunes as it goes and so takes a
/// different path through the tree.
#[test]
fn a_range_scan_waits_per_level_not_per_node() {
    let (dir, _) = populated();

    let store = Arc::new(Metered::new(LocalFSObjectStore::new(dir.path()).unwrap()));
    let mut reader = Reader::open(store.clone()).unwrap();
    store.reset();

    let lo = Key::new(vec![int(0)]);
    let hi = Key::new(vec![int(ROWS as i64)]);
    let rows = reader.scan_range("t", &lo, &hi).unwrap();
    assert_eq!(rows.len(), ROWS);

    let s = store.stats();
    assert!(
        s.round_trips <= 8,
        "a range scan over {} rows took {} round trips for {} requests",
        ROWS,
        s.round_trips,
        s.requests()
    );
}

/// Pruning must survive the change: a narrow range must not read the tree.
///
/// Batching a level is only a win if the level was pruned first. A version
/// that fetched every child before filtering would pass the round-trip
/// assertions above while reading the whole collection.
#[test]
fn a_narrow_range_still_reads_almost_nothing() {
    let (dir, _) = populated();

    let store = Arc::new(Metered::new(LocalFSObjectStore::new(dir.path()).unwrap()));
    let mut reader = Reader::open(store.clone()).unwrap();
    store.reset();

    let lo = Key::new(vec![int(100)]);
    let hi = Key::new(vec![int(110)]);
    let rows = reader.scan_range("t", &lo, &hi).unwrap();
    assert_eq!(rows.len(), 10);

    let s = store.stats();
    assert!(
        s.requests() < 20,
        "a 10-row range issued {} requests over {} rows — the range is not \
         being pruned before the batch is issued",
        s.requests(),
        ROWS
    );
}
