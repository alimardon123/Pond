// collection_map.rs — a head must stop carrying every collection it owns.
//
// A head is one writer's whole view of the pond in one object, which is what
// makes multi-collection publish atomic: object stores guarantee single-object
// write atomicity, so one PUT publishes everything in it.
//
// The cost is that the object is rewritten whole when any one collection
// changes. Measured before this, by `pond_bench --bin headscale`: ~40 bytes per
// collection, on every publish and every open. 729 KB to write one row into a
// pond with 10,000 collections; ~40 MB at a million.
//
// Above a threshold the map moves into a content-addressed index and the head
// carries only its root. Publishing rewrites the O(log C) nodes on one path
// plus a fixed-size head — and the head is still one object, so the atomicity
// the design is built on is untouched.
//
// The threshold is deliberately high (see `INLINE_COLLECTION_LIMIT`): the
// index costs an extra round trip on publish and a descent on first access,
// which is a bad trade until the bytes saved exceed them. These tests set it
// low so the mechanism can be exercised without writing 50,000 collections.

use std::sync::Arc;

use pond_engine::{Engine, EngineConfig, Reader};
use pond_index::{int, Key};
use pond_kernel::{LocalFSObjectStore, Metered};
use pond_record::{Record, Value, Version};

const LIMIT: usize = 8;

fn row(i: u64) -> Record {
    let mut r = Record::new();
    r.set("v", Value::Int(i as i64), Version::new(i + 1, 0, 1));
    r
}

fn config() -> EngineConfig {
    EngineConfig::default().with_inline_collection_limit(LIMIT)
}

/// Build a pond with `n` collections, one row each.
fn pond(n: usize) -> (tempfile::TempDir, Arc<Metered<LocalFSObjectStore>>) {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Metered::new(LocalFSObjectStore::new(dir.path()).unwrap()));
    let mut engine =
        Engine::open_with(store.clone(), 1, Default::default(), config()).unwrap();
    for c in 0..n {
        engine
            .write_records(
                &format!("c{c:04}"),
                vec![(Key::new(vec![int(0)]), row(c as u64))],
            )
            .unwrap();
    }
    engine.publish().unwrap();
    (dir, store)
}

/// Every collection must still be readable once the map is externalised.
///
/// This is the property that matters: the map moving out of the head is an
/// encoding change, and nothing above it should be able to tell.
#[test]
fn every_collection_reads_back_after_the_map_is_externalised() {
    let n = LIMIT * 4;
    let (dir, _) = pond(n);

    let store = Arc::new(Metered::new(LocalFSObjectStore::new(dir.path()).unwrap()));
    let mut reader = Reader::open(store).unwrap();

    let mut listed = reader.collections();
    listed.sort();
    assert_eq!(listed.len(), n, "collections() lost entries: {listed:?}");

    for c in 0..n {
        let name = format!("c{c:04}");
        let got = reader
            .get(&name, &Key::new(vec![int(0)]))
            .unwrap_or_else(|e| panic!("{name}: {e}"))
            .unwrap_or_else(|| panic!("{name} is missing"));
        assert_eq!(got.get("v"), Some(&Value::Int(c as i64)), "{name} has the wrong row");
    }
}

/// And a publish must stop growing with the number of collections.
#[test]
fn publish_bytes_stop_growing_with_collection_count() {
    let small = LIMIT * 4;
    let large = LIMIT * 40;

    let cost = |n: usize| -> u64 {
        let (dir, store) = pond(n);
        let mut engine =
            Engine::open_with(store.clone(), 1, Default::default(), config()).unwrap();
        // A single-row write into one collection, which is the case that
        // should not care how many other collections exist.
        store.reset();
        engine
            .write_records("c0000", vec![(Key::new(vec![int(1)]), row(9_999))])
            .unwrap();
        engine.publish().unwrap();
        let b = store.stats().bytes_written;
        drop(dir);
        b
    };

    let s = cost(small);
    let l = cost(large);
    assert!(
        l < s * 2,
        "a publish at {large} collections wrote {l} bytes against {s} at \
         {small} — ten times the collections should not cost proportionally \
         more once the map is externalised"
    );
}

/// Below the limit nothing changes: the map stays in the head.
///
/// Worth pinning, because the externalised path costs an extra round trip and
/// a descent, and applying it to a small pond is a regression rather than a
/// fix — which is exactly what the first version of the threshold did.
#[test]
fn a_small_pond_keeps_its_map_inline() {
    let (dir, _) = pond(LIMIT);
    let store = Arc::new(Metered::new(LocalFSObjectStore::new(dir.path()).unwrap()));
    let probe = Arc::new(Metered::new(LocalFSObjectStore::new(dir.path()).unwrap()));

    let reader = Reader::open(store).unwrap();
    assert_eq!(reader.collections().len(), LIMIT);

    // A read from a fresh reader should touch only the head and the data.
    let mut r2 = Reader::open(probe.clone()).unwrap();
    probe.reset();
    assert!(r2.get("c0000", &Key::new(vec![int(0)])).unwrap().is_some());
    assert!(
        probe.stats().round_trips <= 2,
        "a small pond paid {} round trips for a point read — the map is being \
         descended when it should be inline",
        probe.stats().round_trips
    );
}
