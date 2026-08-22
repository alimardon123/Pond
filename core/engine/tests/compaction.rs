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
    let bytes = pond_record::encode_head(&head);
    store(dir.path())
        .put_object(
            &pond_engine::head_key(COMPACTOR_WRITER_ID, 1, &pond_kernel::hash_bytes(&bytes)),
            &bytes,
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

/// Absorbed heads are retired, and only the exact keys the pass read.
///
/// This is what bounds the read path in objects as well as in merges. It is
/// safe with no conditional delete for the same reason the skip is safe: a
/// head key carries the content hash of its bytes, so a writer that published
/// during the pass wrote a key this pass never saw and cannot name.
#[test]
fn compaction_retires_the_heads_it_absorbed() {
    let dir = tempfile::tempdir().unwrap();
    seed(dir.path(), 5, 2);

    let before = store(dir.path()).list_paths("heads/").unwrap();
    assert_eq!(before.len(), 5, "one head per writer before compacting");

    let report = compact(dir.path());
    assert_eq!(report.heads_deleted, 5);

    let after = store(dir.path()).list_paths("heads/").unwrap();
    assert_eq!(
        after.len(),
        1,
        "one compacted head should be all that remains, not {:?}",
        after
    );
    assert!(after
        .iter()
        .filter_map(|h| pond_engine::parse_head_key(h))
        .all(|(id, _, _)| id == COMPACTOR_WRITER_ID));

    // And the data is all still there, which is the only reason the deletes
    // were permissible.
    assert_eq!(Reader::open(store(dir.path())).unwrap().scan("t").unwrap().len(), 10);
}

/// A head published while the pass is running is not deleted by it.
///
/// The delete list is keys, and the racing writer's key did not exist when the
/// pass read the listing. A scheme that deleted by writer prefix would remove
/// this head and lose the row.
#[test]
fn compaction_does_not_delete_a_head_it_never_saw() {
    let dir = tempfile::tempdir().unwrap();
    seed(dir.path(), 3, 2);

    // The pass reads the heads and computes its merge.
    let mut stale = Reader::open(store(dir.path())).unwrap();
    let absorbed = stale.head_identities();
    let merged_root = stale.root_of("t");
    let seen_paths = stale.head_paths();
    drop(stale);

    // A writer publishes now, creating a key the pass has never seen.
    let mut e = Engine::open(store(dir.path()), 2).unwrap();
    e.write_records(
        "t",
        vec![(
            key(777_777),
            Record::new().with_field("late", Value::Int(9), Version::new(700, 2, 1)),
        )],
    )
    .unwrap();
    e.publish().unwrap();

    // The pass now writes its head and retires exactly what it read.
    let mut head = pond_record::Head::new(COMPACTOR_WRITER_ID);
    head.set_root("t", &merged_root);
    for (w, hash) in &absorbed {
        head.observe(*w, hash);
    }
    let bytes = pond_record::encode_head(&head);
    let s = store(dir.path());
    s.put_object(
        &pond_engine::head_key(COMPACTOR_WRITER_ID, 1, &pond_kernel::hash_bytes(&bytes)),
        &bytes,
    )
    .unwrap();
    s.delete_path_batch(&seen_paths).unwrap();

    let mut r = Reader::open(store(dir.path())).unwrap();
    assert_eq!(
        r.get("t", &key(777_777))
            .unwrap()
            .and_then(|rec| rec.get("late").cloned()),
        Some(Value::Int(9)),
        "the head published during the pass must survive the pass's deletes"
    );
    assert_eq!(r.scan("t").unwrap().len(), 3 * 2 + 1);
}

/// The payoff: after compaction a reader opens in a constant number of
/// requests, not one per writer that has ever published.
#[test]
fn reader_open_stops_growing_with_the_writer_count() {
    let mut costs = Vec::new();
    for writers in [4u64, 32, 128] {
        let dir = tempfile::tempdir().unwrap();
        seed(dir.path(), writers, 1);
        compact(dir.path());

        let m = Arc::new(Metered::new(store(dir.path())));
        let mut r = Reader::open(Arc::clone(&m)).unwrap();
        r.scan("t").unwrap();
        let st = m.stats();
        println!(
            "{} writers, compacted: {} requests / {} round trips",
            writers,
            st.requests(),
            st.round_trips
        );
        costs.push(st.requests());

        assert_eq!(
            r.scan("t").unwrap().len(),
            writers as usize,
            "every writer's row must still be readable"
        );
    }

    assert_eq!(
        costs[0], costs[2],
        "opening and scanning a compacted pond must cost the same at 4 \
         writers and at 128: {:?}",
        costs
    );
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

/// A publish leaves its predecessor behind; compaction is what sweeps it.
///
/// `publish` deliberately does not delete the head it supersedes — that would
/// cost a round trip on the commit path to save a reader nothing, since
/// `latest_heads` never reads a superseded key. But "never read" is not "never
/// listed", so something has to remove them, and this is the test that says
/// which something.
#[test]
fn compaction_sweeps_superseded_heads_not_just_absorbed_ones() {
    let dir = tempfile::tempdir().unwrap();

    // One writer, five separate publishes: five head objects, one live.
    for i in 0..5i64 {
        let mut e = Engine::open(store(dir.path()), 1).unwrap();
        e.write_records("t", vec![row(1, i)]).unwrap();
        e.publish().unwrap();
    }

    let before = store(dir.path()).list_paths("heads/").unwrap();
    assert_eq!(
        before.len(),
        5,
        "each publish writes its own head object: {:?}",
        before
    );

    // All five rows are readable, from the newest head alone — heads are
    // cumulative, which is what makes retiring the older ones safe.
    let mut r = Reader::open(store(dir.path())).unwrap();
    assert_eq!(r.scan("t").unwrap().len(), 5);

    compact(dir.path());

    let after = store(dir.path()).list_paths("heads/").unwrap();
    assert_eq!(
        after.len(),
        1,
        "compaction must retire the superseded heads too, not only the live \
         one it absorbed: {:?}",
        after
    );
    assert_eq!(
        Reader::open(store(dir.path())).unwrap().scan("t").unwrap().len(),
        5,
        "and every row must survive the sweep"
    );
}

/// A reader never fetches a superseded head, even before compaction runs.
#[test]
fn superseded_heads_cost_a_listing_entry_and_no_request() {
    let dir = tempfile::tempdir().unwrap();
    for i in 0..8i64 {
        let mut e = Engine::open(store(dir.path()), 1).unwrap();
        e.write_records("t", vec![row(1, i)]).unwrap();
        e.publish().unwrap();
    }
    assert_eq!(store(dir.path()).list_paths("heads/").unwrap().len(), 8);

    // Opening only — a scan afterwards would add index-node reads, which are
    // not what this is about.
    let m = Arc::new(Metered::new(store(dir.path())));
    let mut r = Reader::open(Arc::clone(&m)).unwrap();
    let open = m.stats();

    assert_eq!(
        open.gets, 1,
        "eight heads on the listing, one live: exactly one should be fetched"
    );
    assert_eq!(open.lists, 1);

    // And it is the *live* one — all eight rows are there.
    assert_eq!(r.scan("t").unwrap().len(), 8);
}

/// A pond written before head keys carried a sequence and a hash still reads.
///
/// The old layout was one flat, overwritten object per writer,
/// `heads/writer-<id>`. Those keys carry no content hash, so they can never be
/// recognised as absorbed and are always fetched — a cost in requests, never
/// in rows. This writes one by hand, because no code path produces the old
/// shape any more and a compatibility claim nothing exercises is a guess.
#[test]
fn a_pond_with_pre_sequence_head_keys_still_reads() {
    let dir = tempfile::tempdir().unwrap();

    // Build a head the way the old publish path did, and put it at the old key.
    let mut e = Engine::open(store(dir.path()), 1).unwrap();
    e.write_records("t", vec![row(1, 1), row(1, 2)]).unwrap();
    e.publish().unwrap();

    let s = store(dir.path());
    let new_key = s.list_paths("heads/").unwrap().remove(0);
    let bytes = s.get_object(&new_key).unwrap();
    s.put_object("heads/writer-0000000000000001", &bytes).unwrap();
    s.delete_path(&new_key).unwrap();

    assert_eq!(
        s.list_paths("heads/").unwrap(),
        vec!["heads/writer-0000000000000001".to_string()],
        "the pond should now look exactly like one written before the change"
    );

    let mut r = Reader::open(store(dir.path())).unwrap();
    assert_eq!(r.scan("t").unwrap().len(), 2, "old heads must still be read");

    // A writer opening on top of it recovers nothing — the old key is not
    // under its prefix — so it starts a fresh head. The old head is still
    // there and still merged, so no row is lost.
    let mut e = Engine::open(store(dir.path()), 1).unwrap();
    e.write_records("t", vec![row(1, 3)]).unwrap();
    e.publish().unwrap();

    let mut r = Reader::open(store(dir.path())).unwrap();
    assert_eq!(
        r.scan("t").unwrap().len(),
        3,
        "rows from the old head and the new one must both be visible"
    );

    // And compaction folds the two together.
    compact(dir.path());
    let mut r = Reader::open(store(dir.path())).unwrap();
    assert_eq!(r.scan("t").unwrap().len(), 3);
}

/// Compaction sweeps heads, not everything that happens to sit under `heads/`.
///
/// The delete list comes from a listing of the head prefix, so "delete what
/// the listing returned" would also destroy a marker written by a future
/// version of this code, or anything else that came to live there. A sweep
/// should remove what it recognises and leave what it does not.
#[test]
fn compaction_leaves_alone_what_it_does_not_recognise() {
    let dir = tempfile::tempdir().unwrap();
    seed(dir.path(), 3, 2);

    let s = store(dir.path());
    s.put_object("heads/_marker", b"written by something else").unwrap();
    s.put_object("heads/writer/notavalidkey", b"unparseable").unwrap();

    compact(dir.path());

    let after = s.list_paths("heads/").unwrap();
    assert!(
        after.contains(&"heads/_marker".to_string()),
        "an unrecognised object under heads/ must survive: {:?}",
        after
    );
    assert!(
        after.contains(&"heads/writer/notavalidkey".to_string()),
        "so must a key that looks like a head but does not parse: {:?}",
        after
    );
    assert_eq!(
        s.get_object("heads/_marker").unwrap(),
        b"written by something else",
        "and its contents must be untouched"
    );

    // The real heads were still folded and swept.
    assert_eq!(
        Reader::open(store(dir.path())).unwrap().scan("t").unwrap().len(),
        6
    );
}

/// Two compaction passes overlapping must not lose a writer between them.
///
/// The claim on `compact_heads` is that anyone may run it, any number of
/// times, concurrently. That was easy to guarantee while it only ever added a
/// head. Now that a pass also deletes what it absorbed, the claim needs
/// checking against the case that breaks it:
///
///   - pass A lists {h1, h2, h3}, merges, publishes, deletes all three;
///   - pass B listed only {h1, h2} earlier, publishes *after* A, and so takes
///     a higher sequence.
///
/// If readers take only the newest compacted head, they see B's — which never
/// contained h3 — while A has already deleted h3. The row is gone.
#[test]
fn two_overlapping_compaction_passes_lose_nothing() {
    let dir = tempfile::tempdir().unwrap();
    seed(dir.path(), 2, 1);

    // Pass B reads the listing now, while only writers 1 and 2 exist.
    let mut b = Reader::open(store(dir.path())).unwrap();
    let b_absorbed = b.head_identities();
    let b_root = b.root_of("t");
    let b_paths = b.all_head_paths();
    drop(b);

    // Writer 3 appears and publishes.
    let mut e = Engine::open(store(dir.path()), 3).unwrap();
    e.write_records("t", vec![row(3, 0)]).unwrap();
    e.publish().unwrap();

    // Pass A runs to completion: it sees all three, folds them, retires them.
    compact(dir.path());
    assert_eq!(
        Reader::open(store(dir.path())).unwrap().scan("t").unwrap().len(),
        3,
        "pass A alone must leave all three rows readable"
    );

    // Now pass B finishes, publishing its stale merge at a *later* moment and
    // therefore a higher sequence, and retiring what it had listed.
    let s = store(dir.path());
    let mut head = pond_record::Head::new(COMPACTOR_WRITER_ID);
    head.set_root("t", &b_root);
    for (w, hash) in &b_absorbed {
        head.observe(*w, hash);
    }
    let bytes = pond_record::encode_head(&head);
    s.put_object(
        &pond_engine::head_key(
            COMPACTOR_WRITER_ID,
            u64::MAX - 1, // unambiguously later than pass A's
            &pond_kernel::hash_bytes(&bytes),
        ),
        &bytes,
    )
    .unwrap();
    for p in b_paths.iter().filter(|p| pond_engine::parse_head_key(p).is_some()) {
        let _ = s.delete_path(p);
    }

    let mut r = Reader::open(store(dir.path())).unwrap();
    let rows = r.scan("t").unwrap();
    assert_eq!(
        rows.len(),
        3,
        "writer 3's row must survive two overlapping passes; a reader that \
         takes only the newest compacted head sees B's, which never contained \
         it, while A has already deleted the original. Got {:?}",
        rows.iter().map(|(k, _)| k).collect::<Vec<_>>()
    );
}

/// Overlapping passes leave two compacted heads; the next pass folds them.
///
/// Reading every compacted head is what makes the overlap safe, so the
/// question this raises is whether they accumulate. They do not: a pass
/// retires every compacted head it read, so the fixpoint is one.
#[test]
fn overlapping_compacted_heads_converge_back_to_one() {
    let dir = tempfile::tempdir().unwrap();
    seed(dir.path(), 3, 1);

    // Two passes from the same starting view — the overlap, in its simplest
    // form. Both publish; neither can delete the other's head, because neither
    // listed it.
    // Both passes read *before* either publishes — that is what makes them
    // overlap. Reading one after the other would just be two sequential
    // passes, and the second would legitimately retire the first's head.
    let view = || {
        let mut r = Reader::open(store(dir.path())).unwrap();
        let v = (r.head_identities(), r.root_of("t"), r.all_head_paths());
        drop(r);
        v
    };
    let views = [view(), view()];

    let publish_from = |seq: u64, (absorbed, root, paths): &(Vec<(u64, String)>, String, Vec<String>)| {
        let s = store(dir.path());
        let mut head = pond_record::Head::new(COMPACTOR_WRITER_ID);
        head.set_root("t", root);
        for (w, hash) in absorbed {
            head.observe(*w, hash);
        }
        // A distinct sequence per pass, so the two heads are distinct objects
        // even though their contents are identical here.
        let bytes = pond_record::encode_head(&head);
        s.put_object(
            &pond_engine::head_key(COMPACTOR_WRITER_ID, seq, &pond_kernel::hash_bytes(&bytes)),
            &bytes,
        )
        .unwrap();
        for p in paths.iter().filter(|p| pond_engine::parse_head_key(p).is_some()) {
            let _ = s.delete_path(p);
        }
    };

    publish_from(10, &views[0]);
    publish_from(20, &views[1]);

    let heads = store(dir.path()).list_paths("heads/").unwrap();
    let compacted = heads
        .iter()
        .filter_map(|h| pond_engine::parse_head_key(h))
        .filter(|(id, _, _)| *id == COMPACTOR_WRITER_ID)
        .count();
    assert_eq!(compacted, 2, "two overlapping passes leave two: {:?}", heads);
    assert_eq!(
        Reader::open(store(dir.path())).unwrap().scan("t").unwrap().len(),
        3,
        "and every row is still readable across both"
    );

    // One ordinary pass folds them back together.
    compact(dir.path());
    let heads = store(dir.path()).list_paths("heads/").unwrap();
    assert_eq!(
        heads.len(),
        1,
        "the next pass must retire both and leave one: {:?}",
        heads
    );
    assert_eq!(
        Reader::open(store(dir.path())).unwrap().scan("t").unwrap().len(),
        3
    );
}
