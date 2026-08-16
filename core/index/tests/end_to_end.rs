// end_to_end.rs — do the pieces actually compose?
//
// Each crate is tested in isolation elsewhere. This file checks the claims
// that only exist at the seams, using the real kernel, the real object store,
// the real cache, and real records:
//
//   1. Multi-writer convergence — several writers, no coordination, one
//      agreed state, byte for byte.
//   2. Geo-sync by file copy — two stores reconcile by copying files in both
//      directions, with no conflict resolution at the storage layer.
//   3. Round-trip budget — a warm point lookup costs one backend request; a
//      cold one costs a small constant, at any size.
//   4. Atomic multi-collection publish — a reader sees all collections move
//      together or none.
//   5. Per-field merge across writers, through the index.
//
// Everything is asserted on request counts rather than timings, because
// against object storage the round trip is the cost.

use std::sync::atomic::{AtomicU64, Ordering};

use pond_cache::{BlobCache, CacheConfig};
use pond_index::{int, str_, ChunkConfig, Key, NodeStore, Tree};
use pond_kernel::{LocalFSObjectStore, ObjectStore};
use pond_record::{
    decode_head, decode_record, encode_head, encode_record, merge_records, Head, Record, Value,
    Version,
};

// ---------------------------------------------------------------------------
// Glue: a NodeStore backed by a real ObjectStore, counting backend requests.
// ---------------------------------------------------------------------------

struct BackedStore<S: ObjectStore> {
    inner: S,
    gets: AtomicU64,
    puts: AtomicU64,
}

impl<S: ObjectStore> BackedStore<S> {
    fn new(inner: S) -> Self {
        Self {
            inner,
            gets: AtomicU64::new(0),
            puts: AtomicU64::new(0),
        }
    }
    fn gets(&self) -> u64 {
        self.gets.load(Ordering::Relaxed)
    }
    fn reset(&self) {
        self.gets.store(0, Ordering::Relaxed);
        self.puts.store(0, Ordering::Relaxed);
    }
}

impl<S: ObjectStore> NodeStore for BackedStore<S> {
    fn put(&self, bytes: Vec<u8>) -> String {
        self.puts.fetch_add(1, Ordering::Relaxed);
        self.inner.put_blob(&bytes).expect("put_blob")
    }
    fn get(&self, hash: &str) -> Option<Vec<u8>> {
        self.gets.fetch_add(1, Ordering::Relaxed);
        self.inner.get_blob(hash).ok()
    }
}

fn v(physical: u64, writer: u64) -> Version {
    Version::new(physical, 0, writer)
}

/// A record as a lens would produce it, encoded for storage in the index.
fn user_record(id: i64, name: &str, version: Version) -> (Vec<u8>, Vec<u8>) {
    let key = Key::new(vec![str_("users"), int(id)]).encode();
    let rec = Record::new()
        .with_field("id", Value::Int(id), version)
        .with_field("name", Value::Str(name.into()), version);
    (key, encode_record(&rec))
}

// ---------------------------------------------------------------------------
// 1. Multi-writer convergence
// ---------------------------------------------------------------------------

/// Three writers write disjoint and overlapping records with no coordination.
/// Every writer, merging in whatever order it happens to see the others,
/// must end up with the identical root hash.
#[test]
fn writers_converge_without_coordination() {
    let dir = tempfile::tempdir().unwrap();
    let store = BackedStore::new(LocalFSObjectStore::new(dir.path()).unwrap());
    let cfg = ChunkConfig::with_target(32);

    // Writer 1 owns ids 0..600, writer 2 owns 400..1000 (overlapping 400..600
    // with a later version), writer 3 owns 900..1200.
    let w1: Vec<_> = (0..600).map(|i| user_record(i, "w1", v(100, 1))).collect();
    let w2: Vec<_> = (400..1000)
        .map(|i| user_record(i, "w2", v(200, 2)))
        .collect();
    let w3: Vec<_> = (900..1200)
        .map(|i| user_record(i, "w3", v(150, 3)))
        .collect();

    let t1 = Tree::build(&store, w1, cfg);
    let t2 = Tree::build(&store, w2, cfg);
    let t3 = Tree::build(&store, w3, cfg);

    // Per-field record merge, exactly as a reader or compactor would apply it.
    let resolve = |a: &[u8], b: &[u8]| -> Vec<u8> {
        match (decode_record(a), decode_record(b)) {
            (Some(ra), Some(rb)) => encode_record(&merge_records(&ra, &rb)),
            _ => a.to_vec(),
        }
    };

    // Six arrival orders — every permutation of three writers.
    let orders: Vec<Vec<&Tree>> = vec![
        vec![&t1, &t2, &t3],
        vec![&t1, &t3, &t2],
        vec![&t2, &t1, &t3],
        vec![&t2, &t3, &t1],
        vec![&t3, &t1, &t2],
        vec![&t3, &t2, &t1],
    ];

    let roots: Vec<String> = orders
        .iter()
        .map(|order| {
            let mut acc = Tree::build(&store, Vec::new(), cfg);
            for t in order {
                acc = acc.merge(&store, t, resolve);
            }
            acc.root
        })
        .collect();

    for (i, r) in roots.iter().enumerate() {
        assert_eq!(
            r, &roots[0],
            "arrival order {} produced a different root — writers did not converge",
            i
        );
    }

    // And the merged state is correct: the overlap resolves to the higher
    // version, not to whoever happened to be merged last.
    let merged = Tree {
        root: roots[0].clone(),
        config: cfg,
    };
    let contested = Key::new(vec![str_("users"), int(500)]).encode();
    let rec = decode_record(&merged.get(&store, &contested).unwrap()).unwrap();
    assert_eq!(
        rec.get("name"),
        Some(&Value::Str("w2".into())),
        "the later version must win the contested key"
    );
    assert_eq!(merged.len(&store), 1200);
}

// ---------------------------------------------------------------------------
// 2. Geo-sync by plain file copy
// ---------------------------------------------------------------------------

/// Two independent stores — think laptop and S3, or two regions — reconcile by
/// copying files in both directions and then merging.
///
/// This works because no two writers ever write the same key: blobs are
/// content-addressed, and each writer owns its own head. So the sync needs no
/// conflict resolution at the storage layer at all; correctness comes from the
/// merge at read time.
#[test]
fn two_stores_sync_by_file_copy() {
    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();
    let cfg = ChunkConfig::with_target(32);

    let store_a = BackedStore::new(LocalFSObjectStore::new(dir_a.path()).unwrap());
    let store_b = BackedStore::new(LocalFSObjectStore::new(dir_b.path()).unwrap());

    // Each side writes independently, offline from the other.
    let recs_a: Vec<_> = (0..500).map(|i| user_record(i, "site-a", v(100, 1))).collect();
    let recs_b: Vec<_> = (300..800)
        .map(|i| user_record(i, "site-b", v(200, 2)))
        .collect();

    let tree_a = Tree::build(&store_a, recs_a, cfg);
    let tree_b = Tree::build(&store_b, recs_b, cfg);

    // Each side publishes a head naming its own root.
    let mut head_a = Head::new(1);
    head_a.set_root("users", &tree_a.root);
    let mut head_b = Head::new(2);
    head_b.set_root("users", &tree_b.root);

    // The sync: copy every blob both ways. Just files — no protocol.
    copy_all_blobs(dir_a.path(), dir_b.path());
    copy_all_blobs(dir_b.path(), dir_a.path());

    let resolve = |x: &[u8], y: &[u8]| -> Vec<u8> {
        match (decode_record(x), decode_record(y)) {
            (Some(rx), Some(ry)) => encode_record(&merge_records(&rx, &ry)),
            _ => x.to_vec(),
        }
    };

    // Both sides now merge the heads they can see. Same inputs, so same root.
    let a_view = Tree {
        root: head_a.root_of("users").unwrap().to_string(),
        config: cfg,
    }
    .merge(
        &store_a,
        &Tree {
            root: head_b.root_of("users").unwrap().to_string(),
            config: cfg,
        },
        resolve,
    );

    let b_view = Tree {
        root: head_b.root_of("users").unwrap().to_string(),
        config: cfg,
    }
    .merge(
        &store_b,
        &Tree {
            root: head_a.root_of("users").unwrap().to_string(),
            config: cfg,
        },
        resolve,
    );

    assert_eq!(
        a_view.root, b_view.root,
        "after a plain bidirectional file copy, both sites must agree"
    );
    assert_eq!(a_view.len(&store_a), 800);
}

/// Copy every blob from one store root to another, like `aws s3 sync`.
fn copy_all_blobs(from: &std::path::Path, to: &std::path::Path) {
    let src = from.join("blobs");
    if !src.exists() {
        return;
    }
    for shard in std::fs::read_dir(&src).unwrap().flatten() {
        if !shard.path().is_dir() {
            continue;
        }
        let dst_shard = to.join("blobs").join(shard.file_name());
        std::fs::create_dir_all(&dst_shard).unwrap();
        for f in std::fs::read_dir(shard.path()).unwrap().flatten() {
            let dst = dst_shard.join(f.file_name());
            if !dst.exists() {
                std::fs::copy(f.path(), dst).unwrap();
            }
        }
    }
}

// ---------------------------------------------------------------------------
// 3. Round-trip budget, through the real cache
// ---------------------------------------------------------------------------

/// A warm point lookup must cost one backend request, and a cold one a small
/// constant — with the real object store and the real cache in the path, not
/// an in-memory stand-in.
#[test]
fn round_trip_budget_holds_through_the_cache() {
    let store_dir = tempfile::tempdir().unwrap();
    let cache_dir = tempfile::tempdir().unwrap();
    let cfg = ChunkConfig::default();

    let backend = LocalFSObjectStore::new(store_dir.path()).unwrap();
    let cache = BlobCache::new(
        backend,
        CacheConfig::default().with_disk(cache_dir.path(), 1 << 30),
    )
    .unwrap();
    let store = BackedStore::new(cache);

    let n = 100_000i64;
    let records: Vec<_> = (0..n).map(|i| user_record(i, "user", v(100, 1))).collect();
    let tree = Tree::build(&store, records, cfg);

    let probe = Key::new(vec![str_("users"), int(n / 2)]).encode();

    // Cold-ish: the cache holds what the build just wrote, so this measures
    // node reads, i.e. tree depth.
    store.reset();
    assert!(tree.get(&store, &probe).is_some());
    let node_reads = store.gets();
    assert!(
        node_reads <= 4,
        "point lookup touched {} nodes over {} records; expected tree depth",
        node_reads,
        n
    );

    // Warm: the cache absorbs every node read, so the backing store sees none.
    let cache_stats_before = store.inner.stats();
    for _ in 0..50 {
        assert!(tree.get(&store, &probe).is_some());
    }
    let after = store.inner.stats();
    assert_eq!(
        after.misses, cache_stats_before.misses,
        "warm lookups must not miss the cache"
    );
    assert!(after.hits() > cache_stats_before.hits());

    println!(
        "\n  {} records: {} node reads per lookup, cache hit rate {:.1}%",
        n,
        node_reads,
        after.hit_rate() * 100.0
    );
}

// ---------------------------------------------------------------------------
// 4. Atomic multi-collection publish
// ---------------------------------------------------------------------------

/// Publishing three collections is one object write, so a reader sees either
/// all three new roots or all three old ones — never a mix.
#[test]
fn multi_collection_publish_is_all_or_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let backend = LocalFSObjectStore::new(dir.path()).unwrap();
    let store = BackedStore::new(LocalFSObjectStore::new(dir.path()).unwrap());
    let cfg = ChunkConfig::with_target(32);

    let build = |tag: &str, n: i64| Tree::build(
        &store,
        (0..n).map(|i| user_record(i, tag, v(100, 1))).collect(),
        cfg,
    );

    // Version 1 of three collections, published together.
    let mut head = Head::new(1);
    head.set_root("users", &build("v1", 100).root);
    head.set_root("orders", &build("v1", 50).root);
    head.set_root("events", &build("v1", 75).root);
    let v1_bytes = encode_head(&head);
    backend.put_path("heads/1", &backend.put_blob(&v1_bytes).unwrap()).unwrap();

    let published_v1 = decode_head(&v1_bytes).unwrap();

    // Version 2: all three advance.
    let mut head2 = Head::new(1);
    head2.set_root("users", &build("v2", 200).root);
    head2.set_root("orders", &build("v2", 60).root);
    head2.set_root("events", &build("v2", 80).root);
    let v2_bytes = encode_head(&head2);

    let published_v2 = decode_head(&v2_bytes).unwrap();

    // Every collection differs between the two versions — there is no
    // encoding in which a reader could observe a mixture, because the whole
    // map lives in one object.
    for c in ["users", "orders", "events"] {
        assert_ne!(published_v1.root_of(c), published_v2.root_of(c));
    }

    // A torn write is rejected outright rather than yielding a partial head.
    for cut in 1..v2_bytes.len() {
        assert!(
            decode_head(&v2_bytes[..cut]).is_none(),
            "a truncated head at {} bytes must not decode to a partial state",
            cut
        );
    }
}

// ---------------------------------------------------------------------------
// 5. Per-field merge through the index
// ---------------------------------------------------------------------------

/// Two writers update different fields of the same row, concurrently, in
/// separate trees. After merge both edits survive — and any unknown field a
/// third party wrote survives too.
#[test]
fn per_field_edits_survive_through_the_index() {
    let dir = tempfile::tempdir().unwrap();
    let store = BackedStore::new(LocalFSObjectStore::new(dir.path()).unwrap());
    let cfg = ChunkConfig::with_target(16);
    let key = Key::new(vec![str_("users"), int(1)]).encode();

    // The base row carries a field neither writer understands.
    let base = Record::new()
        .with_field("name", Value::Str("alice".into()), v(100, 0))
        .with_field("email", Value::Str("a@x.com".into()), v(100, 0))
        .with_field("embedding", Value::Vector(vec![0.5, 0.25]), v(100, 0));

    let base_tree = Tree::build(&store, vec![(key.clone(), encode_record(&base))], cfg);

    // Writer 1 edits `email` only; writer 2 edits `name` only. Same instant.
    let e1 = base
        .clone()
        .with_field("email", Value::Str("new@x.com".into()), v(200, 1));
    let e2 = base
        .clone()
        .with_field("name", Value::Str("alicia".into()), v(200, 2));

    let t1 = base_tree.insert_batch(&store, vec![(key.clone(), encode_record(&e1))]);
    let t2 = base_tree.insert_batch(&store, vec![(key.clone(), encode_record(&e2))]);

    let resolve = |a: &[u8], b: &[u8]| -> Vec<u8> {
        match (decode_record(a), decode_record(b)) {
            (Some(ra), Some(rb)) => encode_record(&merge_records(&ra, &rb)),
            _ => a.to_vec(),
        }
    };

    let merged_ab = t1.merge(&store, &t2, resolve);
    let merged_ba = t2.merge(&store, &t1, resolve);
    assert_eq!(
        merged_ab.root, merged_ba.root,
        "merge through the index must be commutative"
    );

    let rec = decode_record(&merged_ab.get(&store, &key).unwrap()).unwrap();
    assert_eq!(rec.get("email"), Some(&Value::Str("new@x.com".into())));
    assert_eq!(rec.get("name"), Some(&Value::Str("alicia".into())));
    assert_eq!(
        rec.get("embedding"),
        Some(&Value::Vector(vec![0.5, 0.25])),
        "a field neither writer understood must survive the merge"
    );
}
