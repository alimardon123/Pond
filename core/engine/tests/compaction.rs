// compaction.rs — does folding heads bound the read path without losing writes?
//
// Coordination-free multi-writer costs one head per writer, read and merged by
// every reader forever. Compaction folds them into one. The interesting
// questions are not whether it makes reads cheaper — it obviously does — but
// whether it can ever lose a write, and whether it changes what readers see.

use std::sync::Arc;

use pond_engine::{compact_heads, Engine, EngineConfig, Reader, COMPACTOR_WRITER_ID};
use pond_index::{int, Key};
use pond_kernel::{LocalFSObjectStore, Metered, ObjectStore};
use pond_record::{Record, Value, Version};

fn store(root: &std::path::Path) -> LocalFSObjectStore {
    LocalFSObjectStore::new(root).unwrap()
}

fn key(id: i64) -> Key {
    Key::new(vec![int(id)])
}

fn row(w: u64, i: i64) -> (Key, Record) {
    (
        key(w as i64 * 1_000 + i),
        Record::new().with_field("w", Value::Int(w as i64), Version::new(100 + i as u64, w, 1)),
    )
}

/// Publish `rows_each` rows from each of `writers` writers.
fn seed(root: &std::path::Path, writers: u64, rows_each: i64) {
    for w in 1..=writers {
        let mut e = Engine::open(store(root), w).unwrap();
        e.write_records("t", (0..rows_each).map(|i| row(w, i)).collect())
            .unwrap();
        e.publish().unwrap();
    }
}

fn compact(root: &std::path::Path) -> pond_engine::CompactionReport {
    compact_heads(
        store(root),
        pond_cache::CacheConfig::default(),
        EngineConfig::default(),
    )
    .unwrap()
}

/// The property everything else depends on: compaction is a performance
/// change, not a semantic one.
#[test]
fn compaction_does_not_change_what_readers_see() {
    let dir = tempfile::tempdir().unwrap();
    seed(dir.path(), 8, 3);

    let before_rows = Reader::open(store(dir.path())).unwrap().scan("t").unwrap();
    let before_root = Reader::open(store(dir.path())).unwrap().root_of("t");

    compact(dir.path());

    let mut r = Reader::open(store(dir.path())).unwrap();
    assert_eq!(
        r.scan("t").unwrap(),
        before_rows,
        "compaction must not add, drop or reorder a single row"
    );
    assert_eq!(
        r.root_of("t"),
        before_root,
        "and the merged root must be byte-identical, or convergence is broken"
    );
}

/// The point of the exercise: readers stop merging the folded heads.
#[test]
fn compaction_collapses_the_roots_a_reader_merges() {
    let dir = tempfile::tempdir().unwrap();
    const WRITERS: u64 = 16;
    seed(dir.path(), WRITERS, 2);

    let store_m = Arc::new(Metered::new(store(dir.path())));
    let mut r = Reader::open(Arc::clone(&store_m)).unwrap();
    store_m.reset();
    r.scan("t").unwrap();
    let before = store_m.stats().round_trips;

    let report = compact(dir.path());
    assert_eq!(report.heads_seen, WRITERS as usize);
    assert_eq!(report.heads_absorbed, WRITERS as usize);
    assert_eq!(report.collections, 1);

    let store_m = Arc::new(Metered::new(store(dir.path())));
    let mut r = Reader::open(Arc::clone(&store_m)).unwrap();
    store_m.reset();
    r.scan("t").unwrap();
    let after = store_m.stats().round_trips;

    println!("first scan over {} writers: {} -> {} round trips", WRITERS, before, after);
    assert!(
        after * 4 < before,
        "a compacted pond should read far less to serve the same scan: \
         {} -> {} round trips",
        before,
        after
    );
}

/// The race that makes this design sound, run in the order that breaks a
/// naive implementation.
///
/// A scheme that recorded "writer W is absorbed" by writer id would drop this
/// writer's *new* head, because the id still matches. Recording the content
/// hash of the head bytes means the new head no longer matches what was
/// absorbed and is merged normally.
#[test]
fn a_writer_that_publishes_after_being_absorbed_is_not_lost() {
    let dir = tempfile::tempdir().unwrap();
    seed(dir.path(), 4, 2);
    compact(dir.path());

    // Writer 2 comes back and writes a row that did not exist at compaction.
    let mut e = Engine::open(store(dir.path()), 2).unwrap();
    e.write_records(
        "t",
        vec![(
            key(999_999),
            Record::new().with_field("late", Value::Int(7), Version::new(500, 2, 1)),
        )],
    )
    .unwrap();
    e.publish().unwrap();

    let mut r = Reader::open(store(dir.path())).unwrap();
    assert_eq!(
        r.get("t", &key(999_999))
            .unwrap()
            .and_then(|rec| rec.get("late").cloned()),
        Some(Value::Int(7)),
        "a write published after compaction absorbed that writer's head must \
         still be visible"
    );
    // And nothing else was lost along the way.
    assert_eq!(
        r.scan("t").unwrap().len(),
        4 * 2 + 1,
        "the pre-compaction rows must survive too"
    );
}

/// The same race, but with the publish landing *between* the compactor's read
/// and its write — the window a compare-and-swap would normally be needed for.
///
/// `compact_heads` opens its own reader, so calling it after the publish would
/// not reproduce this: the compactor would simply see the new head. The
/// compacted head is therefore built by hand here from a *stale* view, which
/// is exactly the state a compactor is in when a writer publishes underneath
/// it. Everything used is public API, so this is the real object a real
/// compactor writes — only its timing is forced.
#[test]
fn a_publish_racing_the_compactor_is_never_swallowed() {
    let dir = tempfile::tempdir().unwrap();
    seed(dir.path(), 4, 2);

    // The compactor reads the heads and computes its merge.
    let mut stale = Reader::open(store(dir.path())).unwrap();
    let absorbed = stale.head_identities();
    let merged_root = stale.root_of("t");
    assert!(
        absorbed.iter().any(|(w, _)| *w == 3),
        "writer 3 must be in the stale view, or the race is not set up"
    );

    // Writer 3 publishes *now*, while the compactor is still holding that view.
    let mut e = Engine::open(store(dir.path()), 3).unwrap();
    e.write_records(
        "t",
        vec![(
            key(888_888),
            Record::new().with_field("raced", Value::Int(1), Version::new(600, 3, 1)),
        )],
    )
    .unwrap();
    e.publish().unwrap();

    // Only now does the compactor write, claiming what it read a moment ago —
    // including writer 3, at the hash its head had *before* the publish.
    let mut head = pond_record::Head::new(COMPACTOR_WRITER_ID);
    head.set_root("t", &merged_root);
    for (w, hash) in &absorbed {
        head.observe(*w, hash);
    }
    store(dir.path())
        .put_object(
            &pond_engine::head_key(COMPACTOR_WRITER_ID),
            &pond_record::encode_head(&head),
        )
        .unwrap();

    let mut r = Reader::open(store(dir.path())).unwrap();
    assert_eq!(
        r.get("t", &key(888_888))
            .unwrap()
            .and_then(|rec| rec.get("raced").cloned()),
        Some(Value::Int(1)),
        "a publish landing between the compactor's read and its write must \
         survive — claiming heads by writer id instead of by content hash \
         would drop exactly this row, and would need a CAS to avoid it"
    );
    assert_eq!(
        r.scan("t").unwrap().len(),
        4 * 2 + 1,
        "and the rows that were there before the race must all still be there"
    );
}

/// Compacting twice must be a no-op, and must not shrink coverage.
///
/// The second pass republishes over the first compacted head, so if it failed
/// to re-claim what that head claimed, every absorbed writer would become live
/// again and the compaction would silently undo itself.
#[test]
fn compaction_is_idempotent_and_does_not_shrink_coverage() {
    let dir = tempfile::tempdir().unwrap();
    seed(dir.path(), 6, 2);

    let first = compact(dir.path());
    let rows_after_first = Reader::open(store(dir.path())).unwrap().scan("t").unwrap();

    let second = compact(dir.path());
    assert_eq!(
        second.heads_absorbed, first.heads_absorbed,
        "the second pass must still claim every head the first one did"
    );

    let mut r = Reader::open(store(dir.path())).unwrap();
    assert_eq!(r.scan("t").unwrap(), rows_after_first);

    // A third pass, to be sure the fixpoint is real rather than a coincidence
    // of two.
    compact(dir.path());
    assert_eq!(
        Reader::open(store(dir.path())).unwrap().scan("t").unwrap(),
        rows_after_first
    );
}

/// Nothing is deleted. The compacted head is purely additive, which is what
/// lets this work identically on a local filesystem and on object storage
/// without a conditional write.
#[test]
fn compaction_deletes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    seed(dir.path(), 5, 2);

    let before = store(dir.path()).list_paths("heads/").unwrap();
    compact(dir.path());
    let after = store(dir.path()).list_paths("heads/").unwrap();

    for h in &before {
        assert!(after.contains(h), "compaction removed head {}", h);
    }
    assert_eq!(
        after.len(),
        before.len() + 1,
        "exactly one head is added: the compacted one"
    );
    assert!(after
        .iter()
        .any(|h| h == &pond_engine::head_key(COMPACTOR_WRITER_ID)));
}

/// Multiple collections are folded in one pass, and stay independent.
#[test]
fn compaction_covers_every_collection() {
    let dir = tempfile::tempdir().unwrap();
    for w in 1..=4u64 {
        let mut e = Engine::open(store(dir.path()), w).unwrap();
        e.write_records("users", vec![row(w, 1)]).unwrap();
        e.write_records("events", vec![row(w, 2)]).unwrap();
        e.publish().unwrap();
    }

    let report = compact(dir.path());
    assert_eq!(report.collections, 2);

    let mut r = Reader::open(store(dir.path())).unwrap();
    assert_eq!(r.scan("users").unwrap().len(), 4);
    assert_eq!(r.scan("events").unwrap().len(), 4);
}

/// Compacting an empty pond is not an error.
#[test]
fn compacting_nothing_writes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let report = compact(dir.path());
    assert_eq!(report.heads_seen, 0);
    assert_eq!(report.heads_absorbed, 0);
    assert_eq!(report.collections, 0);

    // Not merely "does not error". An earlier version published an empty
    // compacted head here, which then counted as a head on every later pass —
    // `pond compact` on an untouched pond reported "0 absorbed of 1 seen".
    // Found by running the binary, not by this test, which is why the test now
    // checks the store rather than the return value.
    assert!(
        store(dir.path()).list_paths("heads/").unwrap().is_empty(),
        "compacting an empty pond must leave no head behind"
    );

    // And a second pass still sees nothing.
    assert_eq!(compact(dir.path()).heads_seen, 0);
}
