// engine.rs — does the engine deliver what the design promises?
//
// Asserts on request counts rather than timings, because against object
// storage the round trip is the cost.

use pond_engine::{Engine, Reader};
use pond_index::{int, str_, Key};
use pond_kernel::{LocalFSObjectStore, ObjectStore};
use pond_record::{Record, Value, Version};

fn store(dir: &std::path::Path) -> LocalFSObjectStore {
    LocalFSObjectStore::new(dir).unwrap()
}

fn v(p: u64, w: u64) -> Version {
    Version::new(p, 0, w)
}

fn user(id: i64) -> Key {
    Key::new(vec![str_("users"), int(id)])
}

#[test]
fn write_publish_read_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let mut e = Engine::open(store(dir.path()), 1).unwrap();
    e.write_records(
        "users",
        vec![(
            user(1),
            Record::new().with_field("name", Value::Str("alice".into()), v(100, 1)),
        )],
    )
    .unwrap();
    e.publish().unwrap();

    let mut r = Reader::open(store(dir.path())).unwrap();
    let got = r.get("users", &user(1)).unwrap().expect("record");
    assert_eq!(got.get("name"), Some(&Value::Str("alice".into())));
}

/// Staged writes must be invisible until published — that is what makes
/// `publish` an atomic boundary rather than a formality.
#[test]
fn staged_writes_are_invisible_until_published() {
    let dir = tempfile::tempdir().unwrap();
    let mut e = Engine::open(store(dir.path()), 1).unwrap();
    e.write_records(
        "users",
        vec![(user(1), Record::new().with_field("x", Value::Int(1), v(1, 1)))],
    )
    .unwrap();

    assert!(e.get("users", &user(1)).unwrap().is_some());
    let mut r = Reader::open(store(dir.path())).unwrap();
    assert!(r.get("users", &user(1)).unwrap().is_none());

    e.publish().unwrap();
    let mut r2 = Reader::open(store(dir.path())).unwrap();
    assert!(r2.get("users", &user(1)).unwrap().is_some());
}

#[test]
fn abort_discards_staged_writes() {
    let dir = tempfile::tempdir().unwrap();
    let mut e = Engine::open(store(dir.path()), 1).unwrap();
    e.write_records(
        "users",
        vec![(user(1), Record::new().with_field("x", Value::Int(1), v(1, 1)))],
    )
    .unwrap();
    e.abort();
    e.publish().unwrap();

    let mut r = Reader::open(store(dir.path())).unwrap();
    assert!(r.get("users", &user(1)).unwrap().is_none());
}

#[test]
fn writer_recovers_its_head_on_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut e = Engine::open(store(dir.path()), 7).unwrap();
        e.write_records(
            "users",
            vec![(user(1), Record::new().with_field("x", Value::Int(42), v(1, 7)))],
        )
        .unwrap();
        e.publish().unwrap();
    }
    let mut e = Engine::open(store(dir.path()), 7).unwrap();
    assert_eq!(e.collections(), vec!["users".to_string()]);
    assert_eq!(
        e.get("users", &user(1)).unwrap().unwrap().get("x"),
        Some(&Value::Int(42))
    );
}

/// A partial update must not delete fields the caller did not mention — the
/// law that makes lenses interchangeable, enforced at the engine boundary.
#[test]
fn partial_update_preserves_unknown_fields() {
    let dir = tempfile::tempdir().unwrap();
    let mut e = Engine::open(store(dir.path()), 1).unwrap();

    e.write_records(
        "assets",
        vec![(
            user(1),
            Record::new()
                .with_field("name", Value::Str("clip.mp4".into()), v(100, 1))
                .with_field("embedding", Value::Vector(vec![0.1, 0.2]), v(100, 1))
                .with_field("thumb", Value::Bytes(vec![0xDE, 0xAD]), v(100, 1)),
        )],
    )
    .unwrap();
    e.publish().unwrap();

    e.write_records(
        "assets",
        vec![(
            user(1),
            Record::new().with_field("name", Value::Str("renamed.mp4".into()), v(200, 1)),
        )],
    )
    .unwrap();
    e.publish().unwrap();

    let mut r = Reader::open(store(dir.path())).unwrap();
    let got = r.get("assets", &user(1)).unwrap().unwrap();
    assert_eq!(got.get("name"), Some(&Value::Str("renamed.mp4".into())));
    assert_eq!(
        got.get("embedding"),
        Some(&Value::Vector(vec![0.1, 0.2])),
        "a VECTOR field must survive a lens that does not understand it"
    );
    assert_eq!(got.get("thumb"), Some(&Value::Bytes(vec![0xDE, 0xAD])));
}

/// Independent writers on one store converge, with no coordination and no
/// compare-and-swap anywhere.
#[test]
fn independent_writers_converge() {
    let dir = tempfile::tempdir().unwrap();

    for (wid, lo, hi) in [(1u64, 0i64, 300i64), (2, 200, 500), (3, 450, 700)] {
        let mut e = Engine::open(store(dir.path()), wid).unwrap();
        let recs: Vec<(Key, Record)> = (lo..hi)
            .map(|i| {
                (
                    user(i),
                    Record::new().with_field("owner", Value::Int(wid as i64), v(100 + wid, wid)),
                )
            })
            .collect();
        e.write_records("users", recs).unwrap();
        e.publish().unwrap();
    }

    let mut a = Reader::open(store(dir.path())).unwrap();
    let mut b = Reader::open(store(dir.path())).unwrap();

    assert_eq!(
        a.root_of("users"),
        b.root_of("users"),
        "two readers must compute the same merged root"
    );
    assert_eq!(a.scan("users").unwrap().len(), 700);

    let contested = a.get("users", &user(475)).unwrap().unwrap();
    assert_eq!(contested.get("owner"), Some(&Value::Int(3)));
}

/// No two writers ever write the same key — the property that removes the need
/// for compare-and-swap.
#[test]
fn writers_never_share_a_head_key() {
    let dir = tempfile::tempdir().unwrap();
    for wid in [1u64, 2, 3] {
        let mut e = Engine::open(store(dir.path()), wid).unwrap();
        e.write_records(
            "c",
            vec![(
                user(wid as i64),
                Record::new().with_field("w", Value::Int(wid as i64), v(1, wid)),
            )],
        )
        .unwrap();
        e.publish().unwrap();
    }
    let heads = store(dir.path()).list_paths("heads/").unwrap();
    let unique: std::collections::HashSet<&String> = heads.iter().collect();
    assert_eq!(heads.len(), 3, "expected one head per writer, got {:?}", heads);
    assert_eq!(unique.len(), 3, "head keys must be distinct per writer");
}

/// Three collections advance in one PUT. A reader sees all or none.
#[test]
fn multi_collection_publish_is_atomic() {
    let dir = tempfile::tempdir().unwrap();
    let mut e = Engine::open(store(dir.path()), 1).unwrap();

    for c in ["users", "orders", "events"] {
        e.write_records(
            c,
            vec![(user(1), Record::new().with_field("v", Value::Int(1), v(100, 1)))],
        )
        .unwrap();
    }

    let mut r = Reader::open(store(dir.path())).unwrap();
    for c in ["users", "orders", "events"] {
        assert!(r.get(c, &user(1)).unwrap().is_none());
    }

    e.publish().unwrap();

    let mut r2 = Reader::open(store(dir.path())).unwrap();
    for c in ["users", "orders", "events"] {
        assert!(
            r2.get(c, &user(1)).unwrap().is_some(),
            "{} must be visible after the single publish",
            c
        );
    }
}

#[test]
fn branch_is_a_pointer_copy_and_diverges_independently() {
    let dir = tempfile::tempdir().unwrap();
    let mut e = Engine::open(store(dir.path()), 1).unwrap();

    let recs: Vec<(Key, Record)> = (0..100)
        .map(|i| (user(i), Record::new().with_field("v", Value::Int(i), v(100, 1))))
        .collect();
    e.write_records("main", recs).unwrap();
    e.publish().unwrap();

    e.branch("main", "dev").unwrap();
    e.write_records(
        "dev",
        vec![(user(0), Record::new().with_field("v", Value::Int(999), v(200, 1)))],
    )
    .unwrap();
    e.publish().unwrap();

    assert_eq!(
        e.get("dev", &user(0)).unwrap().unwrap().get("v"),
        Some(&Value::Int(999))
    );
    assert_eq!(
        e.get("main", &user(0)).unwrap().unwrap().get("v"),
        Some(&Value::Int(0)),
        "the branch must not disturb its origin"
    );
    assert_eq!(e.scan("dev").unwrap().len(), 100);
}

/// WAL append + page lookup — the two operations a database actually asks of
/// storage. See docs/POSTGRES_ON_POND.md.
#[test]
fn postgres_wal_and_page_access_pattern() {
    let dir = tempfile::tempdir().unwrap();
    let mut e = Engine::open(store(dir.path()), 1).unwrap();

    let wal: Vec<(Key, Record)> = (0..1000i64)
        .map(|lsn| {
            (
                Key::new(vec![int(lsn)]),
                Record::new().with_field(
                    "payload",
                    Value::Bytes(format!("wal-{}", lsn).into_bytes()),
                    v(lsn as u64, 1),
                ),
            )
        })
        .collect();
    e.append_records("wal", wal).unwrap();

    let mut pages = Vec::new();
    for rel in [16384i64, 16385] {
        for blk in 0..500i64 {
            pages.push((
                Key::new(vec![int(rel), int(0), int(blk)]),
                Record::new()
                    .with_field("page", Value::Bytes(vec![(blk % 251) as u8; 128]), v(1, 1))
                    .with_field("lsn", Value::Int(blk), v(1, 1)),
            ));
        }
    }
    e.write_records("pages", pages).unwrap();
    e.publish().unwrap();

    let mut r = Reader::open(store(dir.path())).unwrap();

    let page = r
        .get("pages", &Key::new(vec![int(16384), int(0), int(42)]))
        .unwrap()
        .expect("page must exist");
    assert_eq!(page.get("lsn"), Some(&Value::Int(42)));

    // A relation's blocks are contiguous in key space.
    let scan = r
        .scan_range(
            "pages",
            &Key::new(vec![int(16384), int(0), int(0)]),
            &Key::new(vec![int(16384), int(0), int(500)]),
        )
        .unwrap();
    assert_eq!(scan.len(), 500, "sequential scan must be a contiguous range");

    // WAL replay from an LSN is a range scan.
    let replay = r
        .scan_range("wal", &Key::new(vec![int(900)]), &Key::new(vec![int(1000)]))
        .unwrap();
    assert_eq!(replay.len(), 100);
    assert_eq!(
        replay[0].1.get("payload"),
        Some(&Value::Bytes(b"wal-900".to_vec()))
    );
}

/// A page lookup must stay constant-cost as the database grows.
#[test]
fn page_lookup_cost_is_flat_as_database_grows() {
    let mut costs = Vec::new();

    for n_blocks in [1_000i64, 10_000, 100_000] {
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(store(dir.path()), 1).unwrap();
        let pages: Vec<(Key, Record)> = (0..n_blocks)
            .map(|blk| {
                (
                    Key::new(vec![int(16384), int(0), int(blk)]),
                    Record::new().with_field("lsn", Value::Int(blk), v(1, 1)),
                )
            })
            .collect();
        e.append_records("pages", pages).unwrap();
        e.publish().unwrap();

        e.store().reset_counters();
        let probes = 20;
        for i in 0..probes {
            let blk = (i * (n_blocks / probes)).min(n_blocks - 1);
            assert!(e
                .get("pages", &Key::new(vec![int(16384), int(0), int(blk)]))
                .unwrap()
                .is_some());
        }
        let per = e.store().gets() as f64 / probes as f64;
        println!("  {:>7} blocks: {:.2} node reads / page lookup", n_blocks, per);
        costs.push(per);
    }

    let growth = costs.last().unwrap() / costs.first().unwrap();
    assert!(
        growth < 2.5,
        "page lookup cost grew {:.1}x over 100x more blocks; expected near-flat: {:?}",
        growth,
        costs
    );
}

/// A backend whose listing fails, to prove the reader does not confuse
/// "I could not ask" with "there is nothing".
struct FailingList<S: ObjectStore>(S);

impl<S: ObjectStore> ObjectStore for FailingList<S> {
    fn put_blob(&self, data: &[u8]) -> std::io::Result<String> {
        self.0.put_blob(data)
    }
    fn get_blob(&self, hash: &str) -> std::io::Result<Vec<u8>> {
        self.0.get_blob(hash)
    }
    fn put_path(&self, path: &str, hash: &str) -> std::io::Result<()> {
        self.0.put_path(path, hash)
    }
    fn get_path(&self, path: &str) -> Option<String> {
        self.0.get_path(path)
    }
    fn put_object(&self, path: &str, bytes: &[u8]) -> std::io::Result<()> {
        self.0.put_object(path, bytes)
    }
    fn get_object(&self, path: &str) -> Option<Vec<u8>> {
        self.0.get_object(path)
    }
    fn delete_path(&self, path: &str) -> std::io::Result<bool> {
        self.0.delete_path(path)
    }
    fn blob_exists(&self, hash: &str) -> bool {
        self.0.blob_exists(hash)
    }
    fn delete_blob(&self, hash: &str) -> std::io::Result<bool> {
        self.0.delete_blob(hash)
    }
    fn list_paths(&self, _prefix: &str) -> std::io::Result<Vec<String>> {
        Err(std::io::Error::other("backend unavailable"))
    }
}

/// A backend fault during head discovery must surface as an error.
///
/// The failure mode this guards against is the dangerous one: if a failed
/// listing returned an empty reader, a transient fault would present as "the
/// collection is empty", and a writer acting on that reading would publish on
/// top of history it never saw.
#[test]
fn reader_open_fails_loudly_when_listing_fails() {
    let dir = tempfile::tempdir().unwrap();
    let mut e = Engine::open(store(dir.path()), 1).unwrap();
    e.write_records(
        "users",
        vec![(
            user(1),
            Record::new().with_field("name", Value::Str("alice".into()), v(100, 1)),
        )],
    )
    .unwrap();
    e.publish().unwrap();

    // Sanity: the data really is there through a healthy backend.
    let mut ok = Reader::open(store(dir.path())).unwrap();
    assert!(ok.get("users", &user(1)).unwrap().is_some());

    let err = Reader::open(FailingList(store(dir.path())));
    assert!(
        err.is_err(),
        "a failed head listing must not be reported as an empty store"
    );
}

/// Counts the round trips a backend is asked for. The counters live behind an
/// `Arc` so the test can still read them after the store is moved into the
/// engine.
#[derive(Default)]
struct Counters {
    puts: std::sync::atomic::AtomicU64,
    gets: std::sync::atomic::AtomicU64,
    lists: std::sync::atomic::AtomicU64,
}

impl Counters {
    fn snapshot(&self) -> (u64, u64, u64) {
        use std::sync::atomic::Ordering::Relaxed;
        (
            self.puts.load(Relaxed),
            self.gets.load(Relaxed),
            self.lists.load(Relaxed),
        )
    }
    fn reset(&self) {
        use std::sync::atomic::Ordering::Relaxed;
        self.puts.store(0, Relaxed);
        self.gets.store(0, Relaxed);
        self.lists.store(0, Relaxed);
    }
}

struct Counting<S: ObjectStore> {
    inner: S,
    c: std::sync::Arc<Counters>,
}

impl<S: ObjectStore> Counting<S> {
    fn new(inner: S, c: std::sync::Arc<Counters>) -> Self {
        Self { inner, c }
    }
}

impl<S: ObjectStore> ObjectStore for Counting<S> {
    fn put_blob(&self, data: &[u8]) -> std::io::Result<String> {
        self.c.puts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.inner.put_blob(data)
    }
    fn get_blob(&self, hash: &str) -> std::io::Result<Vec<u8>> {
        self.c.gets.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.inner.get_blob(hash)
    }
    fn put_path(&self, path: &str, hash: &str) -> std::io::Result<()> {
        self.c.puts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.inner.put_path(path, hash)
    }
    fn get_path(&self, path: &str) -> Option<String> {
        self.c.gets.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.inner.get_path(path)
    }
    fn put_object(&self, path: &str, bytes: &[u8]) -> std::io::Result<()> {
        self.c.puts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.inner.put_object(path, bytes)
    }
    fn get_object(&self, path: &str) -> Option<Vec<u8>> {
        self.c.gets.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.inner.get_object(path)
    }
    fn delete_path(&self, path: &str) -> std::io::Result<bool> {
        self.inner.delete_path(path)
    }
    fn list_paths(&self, prefix: &str) -> std::io::Result<Vec<String>> {
        self.c.lists.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.inner.list_paths(prefix)
    }
    fn blob_exists(&self, hash: &str) -> bool {
        self.inner.blob_exists(hash)
    }
    fn delete_blob(&self, hash: &str) -> std::io::Result<bool> {
        self.inner.delete_blob(hash)
    }
}

/// Committing costs exactly one write, however many collections it spans.
///
/// This is the whole atomicity argument. Object stores make a single object
/// write atomic, so a commit that *is* one write needs no transaction
/// machinery to be all-or-nothing. Two writes — a blob, then a name pointing
/// at it — would be neither atomic nor cheap, and readers would never see the
/// first one on its own anyway.
#[test]
fn commit_is_exactly_one_write() {
    let dir = tempfile::tempdir().unwrap();
    let c = std::sync::Arc::new(Counters::default());
    let mut e = Engine::open(Counting::new(store(dir.path()), c.clone()), 1).unwrap();

    for collection in ["users", "orders", "events"] {
        e.write_records(
            collection,
            vec![(
                user(1),
                Record::new().with_field("v", Value::Int(1), v(100, 1)),
            )],
        )
        .unwrap();
    }

    c.reset();
    e.publish().unwrap();
    let (puts, _, lists) = c.snapshot();

    assert_eq!(
        puts, 1,
        "publishing three collections must be one object write, was {}",
        puts
    );
    assert_eq!(lists, 0, "the commit path must not list");
}

/// Opening a reader costs one LIST plus one read per writer — never a read
/// per writer *chained behind another read*, and never anything proportional
/// to how much data those writers hold.
#[test]
fn reader_open_cost_is_one_list_plus_one_read_per_writer() {
    let dir = tempfile::tempdir().unwrap();

    const WRITERS: u64 = 4;
    for w in 1..=WRITERS {
        let mut e = Engine::open(store(dir.path()), w).unwrap();
        e.write_records(
            "users",
            vec![(
                user(w as i64),
                Record::new().with_field("v", Value::Int(w as i64), v(100, w)),
            )],
        )
        .unwrap();
        e.publish().unwrap();
    }

    let c = std::sync::Arc::new(Counters::default());
    let r = Reader::open(Counting::new(store(dir.path()), c.clone())).unwrap();
    let (_, gets, lists) = c.snapshot();

    assert_eq!(lists, 1, "one LIST discovers every writer");
    assert_eq!(
        gets, WRITERS,
        "one read per head — not the two an indirection would cost"
    );
    assert_eq!(r.collections(), vec!["users".to_string()]);
}
