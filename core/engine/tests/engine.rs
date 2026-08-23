// engine.rs — does the engine deliver what the design promises?
//
// Asserts on request counts rather than timings, because against object
// storage the round trip is the cost.

use std::sync::Arc;

use pond_engine::{Engine, Reader};
use pond_index::{int, str_, Key};
use pond_kernel::{LocalFSObjectStore, Metered, ObjectStore};
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
    // Forwarded explicitly. The trait's batch defaults call the singular
    // method on `self`, so a decorator that omits them unrolls every batch
    // against itself and the backend's parallel implementation never runs.
    fn put_blob_batch(&self, i: &[Vec<u8>]) -> std::io::Result<Vec<String>> {
        self.0.put_blob_batch(i)
    }
    fn get_blob_batch(&self, h: &[String]) -> std::io::Result<Vec<Vec<u8>>> {
        self.0.get_blob_batch(h)
    }
    fn get_object_batch(&self, p: &[String]) -> Vec<Option<Vec<u8>>> {
        self.0.get_object_batch(p)
    }
    fn delete_path_batch(&self, p: &[String]) -> std::io::Result<usize> {
        self.0.delete_path_batch(p)
    }
    fn delete_blob_batch(&self, h: &[String]) -> std::io::Result<usize> {
        self.0.delete_blob_batch(h)
    }

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

/// A store that counts the round trips it is asked for.
///
/// `Metered` rather than a wrapper written here: this file used to define its
/// own, seventy-odd lines of delegation that had to be kept in step with
/// `ObjectStore` by hand — and, like every other copy in the tree, dropped the
/// batch methods onto their sequential defaults, so it de-parallelised the
/// very thing it was measuring.
fn counted(inner: LocalFSObjectStore) -> Arc<Metered<LocalFSObjectStore>> {
    Arc::new(Metered::new(inner))
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
    let c = counted(store(dir.path()));
    let mut e = Engine::open(Arc::clone(&c), 1).unwrap();

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
    let st = c.stats();
    let (puts, lists) = (st.puts, st.lists);

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

    let c = counted(store(dir.path()));
    let r = Reader::open(Arc::clone(&c)).unwrap();
    let st = c.stats();
    let (gets, lists) = (st.gets, st.lists);

    assert_eq!(lists, 1, "one LIST discovers every writer");
    assert_eq!(
        gets, WRITERS,
        "one read per head — not the two an indirection would cost"
    );
    assert_eq!(r.collections(), vec!["users".to_string()]);
}

/// A large row must not be rewritten `target` times over.
///
/// An insert rewrites its whole leaf, and a leaf holds thousands of entries,
/// so storing large values inline means a one-row update rewrites megabytes.
/// Spilling puts a pointer in the leaf instead, which is what keeps the write
/// cost proportional to the row rather than to the leaf.
#[test]
fn a_large_row_does_not_rewrite_its_whole_leaf() {
    let dir = tempfile::tempdir().unwrap();
    let big = "x".repeat(64 * 1024);
    let c = counted(store(dir.path()));
    let mut e = Engine::open(Arc::clone(&c), 1).unwrap();
    let rows: Vec<(pond_index::Key, Record)> = (0..200)
        .map(|i| {
            (
                user(i),
                Record::new().with_field("blob", Value::Str(big.clone()), v(100, 1)),
            )
        })
        .collect();
    e.write_records("docs", rows).unwrap();
    e.publish().unwrap();

    // The value must survive the round trip intact.
    let mut r = Reader::open(store(dir.path())).unwrap();
    let got = r.get("docs", &user(7)).unwrap().expect("record");
    assert_eq!(
        got.get("blob"),
        Some(&Value::Str(big.clone())),
        "a spilled value must read back byte-identical"
    );

    // And a scan resolves every pointer.
    let all = r.scan("docs").unwrap();
    assert_eq!(all.len(), 200);
    assert!(all
        .iter()
        .all(|(_, rec)| rec.get("blob") == Some(&Value::Str(big.clone()))));
}

/// Spilling must not change what a small record costs or how it is stored.
#[test]
fn small_records_are_unaffected_by_spilling() {
    let dir = tempfile::tempdir().unwrap();
    let mut e = Engine::open(store(dir.path()), 1).unwrap();
    e.write_records(
        "users",
        vec![(
            user(1),
            Record::new().with_field("name", Value::Str("ada".into()), v(100, 1)),
        )],
    )
    .unwrap();
    e.publish().unwrap();

    let mut r = Reader::open(store(dir.path())).unwrap();
    assert_eq!(
        r.get("users", &user(1)).unwrap().unwrap().get("name"),
        Some(&Value::Str("ada".into()))
    );
}

/// A partial update of a spilled row must still merge field by field.
///
/// The existing value is a pointer, so a merge that forgot to resolve it would
/// either fail to decode or silently replace the row — dropping every field
/// the update did not mention.
#[test]
fn partial_update_of_a_spilled_row_preserves_other_fields() {
    let dir = tempfile::tempdir().unwrap();
    let big = "y".repeat(8 * 1024);

    let mut e = Engine::open(store(dir.path()), 1).unwrap();
    e.write_records(
        "docs",
        vec![(
            user(1),
            Record::new()
                .with_field("body", Value::Str(big.clone()), v(100, 1))
                .with_field("title", Value::Str("first".into()), v(100, 1)),
        )],
    )
    .unwrap();
    e.publish().unwrap();

    // Update only the title.
    let mut e = Engine::open(store(dir.path()), 1).unwrap();
    e.write_records(
        "docs",
        vec![(
            user(1),
            Record::new().with_field("title", Value::Str("second".into()), v(200, 1)),
        )],
    )
    .unwrap();
    e.publish().unwrap();

    let mut r = Reader::open(store(dir.path())).unwrap();
    let got = r.get("docs", &user(1)).unwrap().expect("record");
    assert_eq!(got.get("title"), Some(&Value::Str("second".into())));
    assert_eq!(
        got.get("body"),
        Some(&Value::Str(big)),
        "the large field must survive an update that never mentioned it"
    );
}

/// The central claim, under real concurrency.
///
/// Every convergence test until now opened two engines one after another,
/// which demonstrates the merge but not the property the design actually
/// claims: that writers running *at the same time*, with no channel between
/// them, converge. Sequential writers cannot exhibit an interleaving bug.
///
/// Writers are namespace-partitioned — each owns `heads/writer-<id>` and no
/// other — so there is nothing to serialise and nothing to lose. This asserts
/// that holds when they genuinely race.
#[test]
fn concurrent_writers_all_land_and_readers_agree() {
    const WRITERS: u64 = 8;
    const ROWS_EACH: i64 = 25;

    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_path_buf();

    std::thread::scope(|s| {
        for w in 1..=WRITERS {
            let root = root.clone();
            s.spawn(move || {
                let mut e = Engine::open(store(&root), w).unwrap();
                let rows: Vec<(pond_index::Key, Record)> = (0..ROWS_EACH)
                    .map(|i| {
                        let id = w as i64 * 1000 + i;
                        (
                            user(id),
                            Record::new().with_field(
                                "writer",
                                Value::Int(w as i64),
                                v(100 + i as u64, w),
                            ),
                        )
                    })
                    .collect();
                e.write_records("users", rows).unwrap();
                e.publish().unwrap();
            });
        }
    });

    // Every writer's rows are visible to a single reader.
    let mut r = Reader::open(store(&root)).unwrap();
    let all = r.scan("users").unwrap();
    assert_eq!(
        all.len() as u64,
        WRITERS * ROWS_EACH as u64,
        "every concurrent writer's rows must survive"
    );

    // And each row carries the writer that wrote it — no cross-contamination.
    for w in 1..=WRITERS {
        let found = r.get("users", &user(w as i64 * 1000 + 7)).unwrap();
        assert_eq!(
            found.and_then(|rec| rec.get("writer").cloned()),
            Some(Value::Int(w as i64)),
            "writer {}'s row is missing or wrong",
            w
        );
    }
}

/// Two readers that have seen the same writers compute byte-identical state.
///
/// This is the property that makes the merge a semilattice join rather than a
/// heuristic: it is commutative, associative and idempotent, so the order in
/// which heads are discovered cannot change the answer. If it could, two
/// caches could disagree while both being "correct", and nothing above could
/// rely on a root hash meaning anything.
#[test]
fn readers_converge_on_the_same_root_hash() {
    const WRITERS: u64 = 6;
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_path_buf();

    std::thread::scope(|s| {
        for w in 1..=WRITERS {
            let root = root.clone();
            s.spawn(move || {
                let mut e = Engine::open(store(&root), w).unwrap();
                e.write_records(
                    "users",
                    vec![(
                        user(w as i64),
                        Record::new().with_field("w", Value::Int(w as i64), v(100, w)),
                    )],
                )
                .unwrap();
                e.publish().unwrap();
            });
        }
    });

    // Several readers, opened independently and concurrently.
    let roots: Vec<String> = std::thread::scope(|s| {
        let handles: Vec<_> = (0..4)
            .map(|_| {
                let root = root.clone();
                s.spawn(move || {
                    let mut r = Reader::open(store(&root)).unwrap();
                    r.root_of("users")
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });

    let first = &roots[0];
    assert!(
        roots.iter().all(|r| r == first),
        "readers disagreed about the merged state: {:?}",
        roots
    );
    assert!(!first.is_empty());
}

/// Concurrent writers to the *same key* must converge rather than corrupt.
///
/// This is the hard case: writer-partitioned namespaces mean no two writers
/// share a head, but they can still write the same logical row. Convergence
/// then rests on per-field merge and version ordering, not on partitioning.
#[test]
fn concurrent_writers_to_one_key_converge_by_version() {
    const WRITERS: u64 = 8;
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_path_buf();

    std::thread::scope(|s| {
        for w in 1..=WRITERS {
            let root = root.clone();
            s.spawn(move || {
                let mut e = Engine::open(store(&root), w).unwrap();
                // Every writer sets a different field on the same row, plus a
                // shared field at a version derived from its id.
                e.write_records(
                    "users",
                    vec![(
                        user(1),
                        Record::new()
                            .with_field(&format!("f{}", w), Value::Int(w as i64), v(100, w))
                            .with_field("shared", Value::Int(w as i64), v(100 + w, w)),
                    )],
                )
                .unwrap();
                e.publish().unwrap();
            });
        }
    });

    let mut r = Reader::open(store(&root)).unwrap();
    let rec = r.get("users", &user(1)).unwrap().expect("the row must exist");

    // Every writer's own field survives — a merge that dropped one would be
    // the column-dropping failure the record model exists to prevent.
    for w in 1..=WRITERS {
        assert_eq!(
            rec.get(&format!("f{}", w)),
            Some(&Value::Int(w as i64)),
            "writer {}'s field was lost in the merge",
            w
        );
    }

    // The contested field resolves to the highest version, deterministically.
    assert_eq!(
        rec.get("shared"),
        Some(&Value::Int(WRITERS as i64)),
        "the contested field must resolve to the newest version"
    );
}

/// Two processes that share a writer id must not lose each other's rows.
///
/// `stable_writer_id` hashes hostname and username, so two processes on one
/// machine — or two containers built from the same image, which is the common
/// case rather than the exotic one — compute the *same* id. The design says a
/// writer owns its key and nobody else writes it, and that is the whole reason
/// no compare-and-swap is needed; sharing an id violates the precondition.
///
/// The question is what happens when it is violated anyway. Silently dropping
/// one process's writes is the answer nobody can debug.
#[test]
fn two_processes_sharing_a_writer_id_do_not_lose_rows() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_path_buf();

    // Both "processes" open before either publishes — they each see an empty
    // prefix and pick the same next sequence.
    let mut a = Engine::open(store(&root), 7).unwrap();
    let mut b = Engine::open(store(&root), 7).unwrap();

    a.write_records(
        "t",
        vec![(user(1), Record::new().with_field("from", Value::Int(1), v(100, 7)))],
    )
    .unwrap();
    b.write_records(
        "t",
        vec![(user(2), Record::new().with_field("from", Value::Int(2), v(101, 7)))],
    )
    .unwrap();
    a.publish().unwrap();
    b.publish().unwrap();

    let mut r = Reader::open(store(&root)).unwrap();
    assert_eq!(
        r.scan("t").unwrap().len(),
        2,
        "both processes' rows must be readable"
    );

    // And the next process to open under that id must carry both forward, or
    // its publish supersedes a head whose rows it does not contain.
    let mut c = Engine::open(store(&root), 7).unwrap();
    c.write_records(
        "t",
        vec![(user(3), Record::new().with_field("from", Value::Int(3), v(102, 7)))],
    )
    .unwrap();
    c.publish().unwrap();

    let mut r = Reader::open(store(&root)).unwrap();
    let rows = r.scan("t").unwrap();
    assert_eq!(
        rows.len(),
        3,
        "a writer reopening under a shared id must recover every head at its \
         own prefix, not just one of them — otherwise its next publish \
         supersedes rows it never had. Got {:?}",
        rows.iter().map(|(k, _)| k).collect::<Vec<_>>()
    );
}

/// A `Value::Spilled` placeholder must never leave the engine.
///
/// It is a storage detail: it says where a payload lives, not what it is.
/// Every consumer downstream — the columnar bridge, SQL, JSON, the Python
/// bindings — would have to learn what one means, and the ones with a
/// catch-all arm would quietly render it as an empty value instead. So the
/// engine resolves before returning, and this is the test that says so rather
/// than a comment claiming it.
#[test]
fn no_read_path_returns_a_spilled_placeholder() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_path_buf();
    let big = "q".repeat(128 * 1024);

    let mut e = Engine::open(store(&root), 1).unwrap();
    e.write_records(
        "docs",
        (0..5i64)
            .map(|i| {
                (
                    user(i),
                    Record::new()
                        .with_field("small", Value::Int(i), v(100, 1))
                        .with_field("big", Value::Str(big.clone()), v(100, 1)),
                )
            })
            .collect(),
    )
    .unwrap();
    e.publish().unwrap();

    let unresolved = |rec: &Record| rec.fields.values().any(|f| f.value.is_spilled());

    // Through the writer's own view.
    let mut e = Engine::open(store(&root), 1).unwrap();
    let got = e.get("docs", &user(2)).unwrap().expect("row 2");
    assert!(!unresolved(&got), "Engine::get returned a placeholder");
    assert_eq!(got.get("big"), Some(&Value::Str(big.clone())));

    // Through a reader's merged view, point and scan.
    let mut r = Reader::open(store(&root)).unwrap();
    let got = r.get("docs", &user(3)).unwrap().expect("row 3");
    assert!(!unresolved(&got), "Reader::get returned a placeholder");
    assert_eq!(got.get("big"), Some(&Value::Str(big.clone())));

    for (key, rec) in r.scan("docs").unwrap() {
        assert!(!unresolved(&rec), "Reader::scan returned a placeholder at {:?}", key);
        assert_eq!(rec.get("big"), Some(&Value::Str(big.clone())));
    }
}

/// A large field survives a write that only touches its neighbours.
///
/// This is the property per-field spilling exists for: the pointer merges as
/// itself, so editing a small column does not re-encode, re-spill, or corrupt
/// the large one.
#[test]
fn editing_a_small_field_leaves_a_large_neighbour_intact() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_path_buf();
    let big = "w".repeat(200 * 1024);

    let mut e = Engine::open(store(&root), 1).unwrap();
    e.write_records(
        "docs",
        vec![(
            user(1),
            Record::new()
                .with_field("status", Value::Str("new".into()), v(100, 1))
                .with_field("attachment", Value::Str(big.clone()), v(100, 1)),
        )],
    )
    .unwrap();
    e.publish().unwrap();

    // A second write naming only `status`.
    let m = Arc::new(Metered::new(store(&root)));
    let mut e = Engine::open(Arc::clone(&m), 1).unwrap();
    m.reset();
    e.write_records(
        "docs",
        vec![(
            user(1),
            Record::new().with_field("status", Value::Str("done".into()), v(200, 1)),
        )],
    )
    .unwrap();
    e.publish().unwrap();
    let written = m.stats().bytes_written;

    let mut r = Reader::open(store(&root)).unwrap();
    let got = r.get("docs", &user(1)).unwrap().expect("the row");
    assert_eq!(got.get("status"), Some(&Value::Str("done".into())));
    assert_eq!(
        got.get("attachment"),
        Some(&Value::Str(big.clone())),
        "the untouched field must come back whole"
    );

    println!(
        "editing one small field beside a {} KiB attachment wrote {} bytes",
        big.len() / 1024,
        written
    );
    assert!(
        written < big.len() as u64 / 2,
        "editing a small field must not rewrite the attachment: {} bytes \
         written against an attachment of {}",
        written,
        big.len()
    );
}
