// reader_is_read_only.rs — a reader must not write. Ever.
//
// Not a performance preference. A `Reader` is the thing you hand a read-only
// credential, a replica, or a caller you do not want mutating anything, and a
// PUT from it is a failed request in the first case and unexpected state in
// the others. On object storage it is also the expensive operation — roughly
// 12.5× a GET — so a read path that writes pays the worst rate for work
// nobody asked for.
//
// Two places did it. Merging is a write, and a reader's view is the merge of
// every writer's tree, so point reads and scans stored merged nodes — fixed by
// walking the roots together instead. And `Tree::build` with no entries writes
// a five-byte empty leaf, so asking a reader for the root of a collection that
// does not exist performed a PUT: a question that created state.
//
// The empty tree needs no node. `Tree::empty` returns one with no root at all,
// and the walks recognise it, so nothing has to be stored for it to behave
// exactly as an empty tree should.

use std::sync::Arc;

use pond_engine::{Engine, Reader};
use pond_index::{int, Key};
use pond_kernel::{LocalFSObjectStore, Metered};
use pond_record::{Record, Value, Version};

fn seeded() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Metered::new(LocalFSObjectStore::new(dir.path()).unwrap()));
    let mut e = Engine::open(store, 1).unwrap();
    let rows: Vec<(Key, Record)> = (0..500)
        .map(|i| {
            let mut r = Record::new();
            r.set("v", Value::Int(i), Version::new(i as u64 + 1, 0, 1));
            (Key::new(vec![int(i)]), r)
        })
        .collect();
    e.write_records("t", rows).unwrap();
    e.publish().unwrap();
    dir
}

/// Every read entry point, against a collection that exists and one that does
/// not — because the missing case is where the empty-tree write hid.
#[test]
fn no_read_path_writes_anything() {
    let dir = seeded();

    let cases: &[(&str, fn(&mut Reader<Arc<Metered<LocalFSObjectStore>>>))] = &[
        ("scan, exists", |r| {
            assert_eq!(r.scan("t").unwrap().len(), 500);
        }),
        ("get, exists", |r| {
            assert!(r.get("t", &Key::new(vec![int(1)])).unwrap().is_some());
        }),
        ("get, key absent", |r| {
            assert!(r.get("t", &Key::new(vec![int(99_999)])).unwrap().is_none());
        }),
        ("scan_range, exists", |r| {
            let lo = Key::new(vec![int(0)]);
            let hi = Key::new(vec![int(100)]);
            assert_eq!(r.scan_range("t", &lo, &hi).unwrap().len(), 100);
        }),
        ("scan_projected, exists", |r| {
            assert_eq!(r.scan_projected("t", &["v"]).unwrap().len(), 500);
        }),
        ("root_of, exists", |r| {
            assert!(!r.root_of("t").is_empty());
        }),
        ("scan, collection absent", |r| {
            assert!(r.scan("nope").unwrap().is_empty());
        }),
        ("get, collection absent", |r| {
            assert!(r.get("nope", &Key::new(vec![int(1)])).unwrap().is_none());
        }),
        ("root_of, collection absent", |r| {
            // The case that used to write: an empty tree materialised just to
            // have a root to name.
            let _ = r.root_of("nope");
        }),
        ("collections", |r| {
            assert!(r.collections().iter().any(|c| c == "t"));
        }),
    ];

    for (name, op) in cases {
        let store = Arc::new(Metered::new(LocalFSObjectStore::new(dir.path()).unwrap()));
        let mut reader = Reader::open(store.clone()).unwrap();
        store.reset();
        op(&mut reader);
        let s = store.stats();
        assert_eq!(
            s.puts, 0,
            "{name} wrote {} object(s) from a Reader",
            s.puts
        );
        assert_eq!(s.deletes, 0, "{name} deleted {} object(s)", s.deletes);
    }
}

/// An empty tree must behave like an empty tree without one existing.
#[test]
fn the_unmaterialised_empty_tree_answers_like_an_empty_one() {
    let dir = seeded();
    let store = Arc::new(Metered::new(LocalFSObjectStore::new(dir.path()).unwrap()));
    let mut reader = Reader::open(store.clone()).unwrap();

    assert!(reader.scan("nope").unwrap().is_empty());
    assert!(reader.get("nope", &Key::new(vec![int(1)])).unwrap().is_none());
    let lo = Key::new(vec![int(0)]);
    let hi = Key::new(vec![int(10)]);
    assert!(reader.scan_range("nope", &lo, &hi).unwrap().is_empty());
    assert!(reader.scan_projected("nope", &["v"]).unwrap().is_empty());

    // And none of it was an error, which is the other way this could go wrong:
    // a missing collection is empty, not broken.
    assert_eq!(store.stats().puts, 0);
}
