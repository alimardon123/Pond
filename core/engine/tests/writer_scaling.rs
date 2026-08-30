// writer_scaling.rs — a point read must not get slower as writers accumulate.
//
// Every writer publishes its own tree and a reader's view is the merge of all
// of them. Building that merge to answer a point read is the obvious
// implementation and the expensive one, because merging is a write: a *read*
// on a fresh reader materialised W-1 merged trees and stored their nodes.
//
// Measured before the fix, on a collection with 200 rows per writer:
//
//     writers=1    waits=1     PUTs=0
//     writers=4    waits=8     PUTs=5
//     writers=16   waits=36    PUTs=26
//     writers=64   waits=141   PUTs=100     4.3 s modelled
//
// Linear in the number of writers, on the path a key-value or OLTP workload
// takes for every operation — and against a design whose headline property is
// that any number of writers converge without coordination. Converging is not
// worth much if reading afterwards costs a round trip per writer.
//
// A point read needs the values under one key, at most one per tree, not the
// merge. Descending every tree in lockstep costs one wait per level and writes
// nothing.

use std::sync::Arc;

use pond_engine::{Engine, Reader};
use pond_index::{int, Key};
use pond_kernel::{LocalFSObjectStore, Metered};
use pond_record::{Record, Value, Version};

const PER_WRITER: usize = 200;

fn rec(writer: u64, i: u64) -> Record {
    let mut r = Record::new();
    r.set("v", Value::Int(i as i64), Version::new(i + 1, 0, writer));
    r
}

/// `writers` writers, each publishing its own disjoint slice of the key space.
fn pond_with_writers(writers: usize) -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Metered::new(LocalFSObjectStore::new(dir.path()).unwrap()));
    for w in 0..writers {
        let mut e = Engine::open(store.clone(), w as u64 + 1).unwrap();
        let rows: Vec<(Key, Record)> = (0..PER_WRITER)
            .map(|i| {
                let k = (w * PER_WRITER + i) as i64;
                (Key::new(vec![int(k)]), rec(w as u64 + 1, i as u64))
            })
            .collect();
        e.write_records("t", rows).unwrap();
        e.publish().unwrap();
    }
    dir
}

/// What one point read costs on a fresh reader.
fn cost(dir: &std::path::Path, key: i64) -> (u64, u64, usize) {
    let store = Arc::new(Metered::new(LocalFSObjectStore::new(dir).unwrap()));
    let mut reader = Reader::open(store.clone()).unwrap();
    store.reset();
    let got = reader.get("t", &Key::new(vec![int(key)])).unwrap();
    let s = store.stats();
    (s.round_trips, s.puts, got.is_some() as usize)
}

/// Waits must not grow with the number of writers, and a read must not write.
#[test]
fn a_point_read_costs_the_same_at_one_writer_and_at_sixty_four() {
    let one = pond_with_writers(1);
    let many = pond_with_writers(64);

    let (w1, p1, found1) = cost(one.path(), 10);
    let (w64, p64, found64) = cost(many.path(), 10);

    assert_eq!(found1, 1, "the key must be there at 1 writer");
    assert_eq!(found64, 1, "the key must be there at 64 writers");

    assert_eq!(p1, 0, "a read wrote {} objects at 1 writer", p1);
    assert_eq!(
        p64, 0,
        "a read wrote {} objects at 64 writers — merging on the read path is \
         back",
        p64
    );

    // Not equality: tree depth may differ between a 200-row and a 12,800-row
    // collection, and paying for depth is legitimate. Paying for *writers* is
    // not. A small constant allows the former and refuses the latter, which at
    // 64 writers used to be 141.
    assert!(
        w64 <= w1 + 3,
        "a point read took {} round trips at 64 writers against {} at one: \
         the cost is growing with the number of writers",
        w64,
        w1
    );
}

/// The cheaper path must return the same answer as the merge it replaces.
///
/// Correctness is the precondition. Every writer holds a different version of
/// one shared key, so the answer depends on the fold across all of them —
/// exactly what the merged tree used to compute.
#[test]
fn the_multi_root_read_agrees_with_the_merged_scan() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Metered::new(LocalFSObjectStore::new(dir.path()).unwrap()));

    // Twelve writers all writing the same key, at increasing versions, plus a
    // row of their own so no tree is empty.
    let shared = Key::new(vec![int(-1)]);
    for w in 0..12u64 {
        let mut e = Engine::open(store.clone(), w + 1).unwrap();
        let mut r = Record::new();
        r.set("v", Value::Int(w as i64), Version::new(w + 1, 0, w + 1));
        e.write_records(
            "t",
            vec![
                (shared.clone(), r),
                (Key::new(vec![int(w as i64)]), rec(w + 1, w)),
            ],
        )
        .unwrap();
        e.publish().unwrap();
    }

    // The point read, on a reader that has built nothing.
    let mut fresh = Reader::open(store.clone()).unwrap();
    let by_point = fresh.get("t", &shared).unwrap().expect("shared key");

    // The same key out of a full scan, which goes through the merged tree.
    let mut scanner = Reader::open(store.clone()).unwrap();
    let by_scan = scanner
        .scan("t")
        .unwrap()
        .into_iter()
        .find(|(k, _)| *k == shared)
        .map(|(_, r)| r)
        .expect("shared key in scan");

    assert_eq!(
        by_point.get("v"),
        by_scan.get("v"),
        "the multi-root point read disagrees with the merged scan"
    );

    // And the reader that has already merged must still answer the same.
    let after_scan = scanner.get("t", &shared).unwrap().expect("shared key");
    assert_eq!(after_scan.get("v"), by_scan.get("v"));
}

/// A scan must not get slower as writers accumulate either.
///
/// Same cause as the point read: a scan that merged first paid a round trip
/// and a PUT per writer before reading anything. Its cost is legitimately
/// larger than a point read's — it reads every leaf — but the number of
/// *waits* should still be bounded by depth, not by writers.
#[test]
fn a_scan_costs_the_same_waits_at_one_writer_and_at_sixty_four() {
    let one = pond_with_writers(1);
    let many = pond_with_writers(64);

    let scan_cost = |dir: &std::path::Path, expect: usize| {
        let store = Arc::new(Metered::new(LocalFSObjectStore::new(dir).unwrap()));
        let mut reader = Reader::open(store.clone()).unwrap();
        store.reset();
        let rows = reader.scan("t").unwrap();
        assert_eq!(rows.len(), expect, "the scan must return every row");
        store.stats()
    };

    let s1 = scan_cost(one.path(), PER_WRITER);
    let s64 = scan_cost(many.path(), PER_WRITER * 64);

    assert_eq!(s1.puts, 0, "a scan wrote {} objects at 1 writer", s1.puts);
    assert_eq!(
        s64.puts, 0,
        "a scan wrote {} objects at 64 writers — merging on the read path is \
         back",
        s64.puts
    );
    assert!(
        s64.round_trips <= s1.round_trips + 3,
        "a scan took {} round trips at 64 writers against {} at one",
        s64.round_trips,
        s1.round_trips
    );
}

/// Writers that share history must not be read once per writer.
///
/// Nodes are content-addressed, so a subtree common to several trees has the
/// same hash in each. Reading it once per tree fetches identical bytes
/// repeatedly. Deduplicating each level of the walk makes the cost track the
/// number of *distinct* nodes, and collapsing duplicates cannot change an
/// answer because the record merge is idempotent.
#[test]
fn identical_writers_are_read_once_not_once_each() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Metered::new(LocalFSObjectStore::new(dir.path()).unwrap()));

    // Sixteen writers publishing byte-identical content. Content addressing
    // makes all sixteen trees the same tree.
    for w in 0..16u64 {
        let mut e = Engine::open(store.clone(), w + 1).unwrap();
        let rows: Vec<(Key, Record)> = (0..PER_WRITER)
            .map(|i| (Key::new(vec![int(i as i64)]), rec(1, i as u64)))
            .collect();
        e.write_records("t", rows).unwrap();
        e.publish().unwrap();
    }

    let probe = Arc::new(Metered::new(LocalFSObjectStore::new(dir.path()).unwrap()));
    let mut reader = Reader::open(probe.clone()).unwrap();
    probe.reset();
    let rows = reader.scan("t").unwrap();
    let s = probe.stats();

    assert_eq!(rows.len(), PER_WRITER, "identical writers must not multiply rows");
    assert!(
        s.requests() < 16,
        "sixteen identical trees cost {} requests — the shared subtrees are \
         being fetched once per writer instead of once",
        s.requests()
    );
}

/// A key no writer holds must read as absent, not as an error or a stale hit.
#[test]
fn a_missing_key_is_still_absent_across_many_writers() {
    let dir = pond_with_writers(16);
    let store = Arc::new(Metered::new(LocalFSObjectStore::new(dir.path()).unwrap()));
    let mut reader = Reader::open(store).unwrap();
    assert_eq!(reader.get("t", &Key::new(vec![int(999_999)])).unwrap(), None);
}
