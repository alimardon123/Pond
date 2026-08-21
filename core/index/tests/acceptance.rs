// acceptance.rs — the tests that decide whether the design survives.
//
// Each of these corresponds to a claim in the storage plan. They are written
// as pass/fail criteria rather than smoke tests, because the point of Phase 1
// is to validate or kill the direction cheaply, before anything is wired into
// the storage layer.
//
//   1. History independence  — same data => one root hash, always
//   2. Incremental == bulk   — insert_batch and build agree byte for byte
//   3. Merge convergence     — merge(A,B) == merge(B,A), byte-identical
//   4. Constant depth        — lookup cost flat across 4 orders of magnitude
//   5. Write amplification   — what a small write actually costs (the risk)
//   6. O(differences) diff   — diff cost tracks changes, not collection size
//
// Test 5 is the one that can kill the design. It is measured, not asserted
// loosely, and the numbers are printed so they can be tracked over time.

use pond_index::{int, str_, ChunkConfig, Key, MemStore, NodeStore, Tree};

/// Deterministic PRNG so any failure reproduces exactly from the seed.
struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed)
    }
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
    fn shuffle<T>(&mut self, v: &mut [T]) {
        for i in (1..v.len()).rev() {
            let j = self.below(i + 1);
            v.swap(i, j);
        }
    }
}

/// Records shaped like a real collection: `(table, id) -> segment locator`.
fn records(n: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    (0..n)
        .map(|i| {
            let k = Key::new(vec![str_("users"), int(i as i64)]).encode();
            let v = format!("seg{:06}:{}:{}", i / 1000, i * 64, 64).into_bytes();
            (k, v)
        })
        .collect()
}

// ---------------------------------------------------------------------------
// 1. History independence
// ---------------------------------------------------------------------------

/// The same set of records must produce the same root hash no matter what
/// order it was inserted in, how it was batched, or which writer produced it.
///
/// This is the property the whole design rests on: without it there is no
/// convergence, no dedup between versions, and no cheap diff. It is also
/// precisely the property the archived prolly tree could not have had, since
/// it chunked by fixed position rather than by content.
#[test]
fn history_independence_1000_insertion_orders() {
    let cfg = ChunkConfig::with_target(32);
    let base = records(2_000);

    let mut roots = std::collections::HashSet::new();
    let mut rng = Rng::new(0xA11CE);

    for _ in 0..1_000 {
        let store = MemStore::new();
        let mut shuffled = base.clone();
        rng.shuffle(&mut shuffled);
        let tree = Tree::build(&store, shuffled, cfg);
        roots.insert(tree.root);
    }

    assert_eq!(
        roots.len(),
        1,
        "1000 insertion orders produced {} distinct roots; expected exactly 1",
        roots.len()
    );
}

/// Same data arriving as different *batches* (not just different order) must
/// also converge — this is the multi-writer case.
#[test]
fn history_independence_across_batch_splits() {
    let cfg = ChunkConfig::with_target(32);
    let base = records(1_500);
    let mut rng = Rng::new(0xB0B);
    let mut roots = std::collections::HashSet::new();

    for _ in 0..100 {
        let store = MemStore::new();
        let mut shuffled = base.clone();
        rng.shuffle(&mut shuffled);

        // Split into 1..5 arbitrary batches, applied in sequence.
        let n_batches = 1 + rng.below(5);
        let chunk = shuffled.len().div_ceil(n_batches);
        let mut tree = Tree::build(&store, Vec::new(), cfg);
        for batch in shuffled.chunks(chunk) {
            tree = tree.insert_batch(&store, batch.to_vec());
        }
        roots.insert(tree.root);
    }

    assert_eq!(
        roots.len(),
        1,
        "different batch splits produced {} distinct roots; expected 1",
        roots.len()
    );
}

// ---------------------------------------------------------------------------
// 2. Incremental == bulk
// ---------------------------------------------------------------------------

/// Building a tree from all entries at once must produce byte-identical
/// output to building part of it and inserting the rest.
///
/// If these ever diverge, two writers who saw the same writes in a different
/// order would hold different bytes, and content-addressed dedup would silently
/// stop working.
#[test]
fn incremental_insert_matches_bulk_build() {
    let cfg = ChunkConfig::with_target(32);
    let all = records(3_000);
    let mut rng = Rng::new(0xC0FFEE);

    for _ in 0..50 {
        let split = 1 + rng.below(all.len() - 1);
        let mut shuffled = all.clone();
        rng.shuffle(&mut shuffled);
        let (first, rest) = shuffled.split_at(split);

        let s1 = MemStore::new();
        let incremental = Tree::build(&s1, first.to_vec(), cfg).insert_batch(&s1, rest.to_vec());

        let s2 = MemStore::new();
        let bulk = Tree::build(&s2, all.clone(), cfg);

        assert_eq!(
            incremental.root, bulk.root,
            "incremental build diverged from bulk build at split {}",
            split
        );
    }
}

/// Updating a value in place must not reshape the tree beyond the affected
/// path — boundaries key off the key, not the value.
#[test]
fn value_update_preserves_structure() {
    let cfg = ChunkConfig::with_target(32);
    let store = MemStore::new();
    let all = records(2_000);
    let tree = Tree::build(&store, all.clone(), cfg);
    let depth_before = tree.depth(&store);

    let victim = all[900].0.clone();
    let updated = tree.insert_batch(&store, vec![(victim.clone(), b"CHANGED".to_vec())]);

    assert_eq!(updated.depth(&store), depth_before);
    assert_eq!(updated.get(&store, &victim).unwrap(), b"CHANGED");
    assert_eq!(updated.len(&store), tree.len(&store));
    // The old tree is untouched — this is what makes branching free.
    assert_eq!(tree.get(&store, &victim).unwrap(), all[900].1);
}

// ---------------------------------------------------------------------------
// 3. Merge convergence
// ---------------------------------------------------------------------------

/// merge(A, B) and merge(B, A) must produce byte-identical trees.
///
/// This is the property that lets any node compact, lets racing compactors be
/// harmless, and lets two regions sync by plain file copy. It requires the
/// conflict resolver to be commutative, which is why versions carry a writer
/// id — without it, two writers in the same millisecond produce equal versions
/// and last-writer-wins has a tie it cannot break deterministically.
#[test]
fn merge_is_commutative_and_deterministic() {
    let cfg = ChunkConfig::with_target(32);
    let store = MemStore::new();

    // Two writers: overlapping key ranges, distinct values.
    let a: Vec<(Vec<u8>, Vec<u8>)> = (0..1500)
        .map(|i| {
            (
                Key::new(vec![int(i)]).encode(),
                format!("a-{:04}", i).into_bytes(),
            )
        })
        .collect();
    let b: Vec<(Vec<u8>, Vec<u8>)> = (1000..2500)
        .map(|i| {
            (
                Key::new(vec![int(i)]).encode(),
                format!("b-{:04}", i).into_bytes(),
            )
        })
        .collect();

    let ta = Tree::build(&store, a, cfg);
    let tb = Tree::build(&store, b, cfg);

    // Commutative resolver: lexicographically greater value wins. Stands in
    // for LWW over (physical, logical, writer_id), which has the same shape.
    let resolve = |x: &[u8], y: &[u8]| if x >= y { x.to_vec() } else { y.to_vec() };

    let ab = ta.merge(&store, &tb, resolve);
    let ba = tb.merge(&store, &ta, resolve);

    assert_eq!(
        ab.root, ba.root,
        "merge(A,B) and merge(B,A) must be byte-identical"
    );
    assert_eq!(ab.len(&store), 2500);
}

/// Merge must be idempotent and associative — the semilattice properties that
/// make it a CRDT join.
#[test]
fn merge_is_idempotent_and_associative() {
    let cfg = ChunkConfig::with_target(32);
    let store = MemStore::new();
    let resolve = |x: &[u8], y: &[u8]| if x >= y { x.to_vec() } else { y.to_vec() };

    let mk = |lo: i64, hi: i64, tag: &str| -> Vec<(Vec<u8>, Vec<u8>)> {
        (lo..hi)
            .map(|i| {
                (
                    Key::new(vec![int(i)]).encode(),
                    format!("{}-{:04}", tag, i).into_bytes(),
                )
            })
            .collect()
    };
    let ta = Tree::build(&store, mk(0, 800, "a"), cfg);
    let tb = Tree::build(&store, mk(600, 1400, "b"), cfg);
    let tc = Tree::build(&store, mk(1200, 2000, "c"), cfg);

    // Idempotent: A ∨ A == A
    assert_eq!(ta.merge(&store, &ta, resolve).root, ta.root);

    // Associative: (A ∨ B) ∨ C == A ∨ (B ∨ C)
    let left = ta.merge(&store, &tb, resolve).merge(&store, &tc, resolve);
    let right = ta.merge(&store, &tb.merge(&store, &tc, resolve), resolve);
    assert_eq!(left.root, right.root, "merge must be associative");
}

// ---------------------------------------------------------------------------
// 4. Constant depth
// ---------------------------------------------------------------------------

/// Lookup cost must stay flat as the collection grows.
///
/// Depth grows logarithmically with a large fanout, and the upper levels are
/// small enough to stay cached permanently (they are immutable and
/// content-addressed, so the cache never needs invalidation). The claim is
/// therefore: cold reads per lookup stay small and constant, and warm reads
/// converge to 1 — at any scale.
#[test]
fn lookup_cost_is_flat_across_four_orders_of_magnitude() {
    // Production config: ~512 entries per node. With 10^6 index entries that
    // is depth 3; each level up multiplies capacity by ~512.
    let cfg = ChunkConfig::default();

    println!("\n  n_entries |  depth | cold reads/lookup | index nodes");
    println!("  ----------+--------+-------------------+------------");

    let mut depths = Vec::new();
    let mut cold_reads = Vec::new();

    for exp in [3u32, 4, 5, 6] {
        let n = 10usize.pow(exp);
        let store = MemStore::new();
        let tree = Tree::build(&store, records(n), cfg);
        let depth = tree.depth(&store);

        // Cold: nothing cached. Measure reads for a spread of lookups.
        store.reset_counters();
        let probes = 100;
        let mut rng = Rng::new(42);
        for _ in 0..probes {
            let i = rng.below(n);
            let k = Key::new(vec![str_("users"), int(i as i64)]).encode();
            assert!(tree.get(&store, &k).is_some(), "lookup must find the key");
        }
        let per_lookup = store.reads() as f64 / probes as f64;

        println!(
            "  {:>9} | {:>6} | {:>17.2} | {:>11}",
            n,
            depth,
            per_lookup,
            store.len()
        );

        depths.push(depth);
        cold_reads.push(per_lookup);
    }

    // 1000x more data must not cost 1000x more round trips.
    let growth = cold_reads.last().unwrap() / cold_reads.first().unwrap();
    assert!(
        growth < 3.0,
        "cold reads/lookup grew {:.1}x from 10^3 to 10^6 entries; expected near-flat",
        growth
    );
    assert!(
        *depths.last().unwrap() <= 4,
        "depth {} at 10^6 entries is higher than expected for fanout ~512",
        depths.last().unwrap()
    );
}

/// With the upper levels warm — which is the steady state, since they are tiny
/// and immutable — a point lookup should cost about one read.
#[test]
fn warm_lookup_costs_about_one_read() {
    let cfg = ChunkConfig::default();
    let store = MemStore::new();
    let n = 200_000;
    let tree = Tree::build(&store, records(n), cfg);

    // Warm every node except the leaves: that is the "index top is cached"
    // assumption, and it is only a few hundred KB at this scale.
    let cached = pond_index::CachingStore::new(&store);
    warm_internal_levels(&store, &cached, &tree.root);

    store.reset_counters();
    let probes = 200;
    let mut rng = Rng::new(7);
    for _ in 0..probes {
        let i = rng.below(n);
        let k = Key::new(vec![str_("users"), int(i as i64)]).encode();
        assert!(tree.get(&cached, &k).is_some());
    }
    let per_lookup = store.reads() as f64 / probes as f64;
    println!(
        "\n  warm lookup: {:.2} uncached reads/lookup over {} entries ({} nodes cached)",
        per_lookup,
        n,
        cached.cached_nodes()
    );
    assert!(
        per_lookup <= 1.05,
        "warm lookup cost {:.2} reads; expected ~1 (the leaf only)",
        per_lookup
    );
}

/// Warm all internal nodes into the cache, leaving leaves uncached.
fn warm_internal_levels<S: NodeStore>(
    store: &S,
    cache: &pond_index::CachingStore<'_, S>,
    hash: &str,
) {
    let Some(node) = store.get(hash).and_then(|b| pond_index::Node::decode(&b)) else {
        return;
    };
    if let pond_index::Node::Internal { children } = node {
        cache.warm(hash);
        for c in children {
            warm_internal_levels(store, cache, &c.hash);
        }
    }
}

// ---------------------------------------------------------------------------
// 5. Write amplification — the biggest risk
// ---------------------------------------------------------------------------

/// How many nodes does a small write actually rewrite?
///
/// This is the design's main risk: an insert rewrites its leaf and every
/// ancestor, so a single-row write costs ~depth node PUTs. If that number is
/// large, the design is too expensive for object storage, where PUTs cost
/// ~12x GETs. The mitigation is batching — writes land in a shard/tail first
/// and only compaction pays the tree cost — so what matters is that the cost
/// *amortizes* across a batch.
///
/// The numbers are printed rather than tightly asserted, so they can be
/// tracked as the design evolves.
///
/// Scope note: this measures *writes*, which is the cost that decides
/// affordability on object storage. `Tree::insert_batch` currently rebuilds
/// from a full scan, so its read cost is O(n) — see the TODO on that method.
/// Content-addressed dedup means the write numbers below are already the real
/// ones; only the read path is pending.
#[test]
fn write_amplification_is_bounded_and_amortizes() {
    let cfg = ChunkConfig::default();
    let base_n = 100_000;

    println!("\n  batch size | new nodes | nodes/record | bytes/record");
    println!("  -----------+-----------+--------------+-------------");

    let mut per_record = Vec::new();

    for batch in [1usize, 10, 100, 1_000, 10_000] {
        let store = MemStore::new();
        let tree = Tree::build(&store, records(base_n), cfg);
        let depth = tree.depth(&store);

        // New keys interleaved into the existing range, which is the worst
        // realistic case (appends would touch only the rightmost path).
        let mut rng = Rng::new(99);
        let updates: Vec<(Vec<u8>, Vec<u8>)> = (0..batch)
            .map(|_| {
                let i = rng.below(base_n);
                (
                    Key::new(vec![str_("users"), int(i as i64), int(1)]).encode(),
                    b"new".to_vec(),
                )
            })
            .collect();

        store.reset_counters();
        let _ = tree.insert_batch(&store, updates);

        let nodes = store.writes() as f64 / batch as f64;
        let bytes = store.bytes_written() as f64 / batch as f64;
        println!(
            "  {:>10} | {:>9} | {:>12.2} | {:>12.0}",
            batch,
            store.writes(),
            nodes,
            bytes
        );
        per_record.push((batch, nodes, depth));
    }

    // Amortization is the property that matters: cost per record must fall
    // substantially as the batch grows.
    let single = per_record[0].1;
    let batched = per_record.last().unwrap().1;
    assert!(
        batched < single / 5.0,
        "batching must amortize tree cost: {:.2} nodes/record at batch=1 vs {:.2} at batch=10000",
        single,
        batched
    );

    // A single-record write should cost on the order of the tree depth, not
    // the whole tree.
    let depth = per_record[0].2 as f64;
    assert!(
        single <= depth * 3.0,
        "single-record write cost {:.1} nodes for a depth-{} tree; expected ~depth",
        single,
        depth
    );
}

/// A small insert must not read the whole tree.
///
/// `insert_batch` used to scan everything and rebuild: write cost was already
/// minimal thanks to content-addressed dedup, but read cost was O(n), which
/// would have been inherited by every layer built on top. The splice reads the
/// internal nodes (about 1/fanout of the tree) plus only the leaves it
/// touches, so read cost tracks the tree's *shape* rather than its size.
#[test]
fn insert_read_cost_tracks_depth_not_size() {
    let cfg = ChunkConfig::default();

    println!("\n  entries | leaves | reads for a 1-row insert | reads as % of leaves");
    println!("  --------+--------+--------------------------+---------------------");

    let mut ratios = Vec::new();

    for n in [10_000usize, 100_000, 500_000] {
        let store = MemStore::new();
        let tree = Tree::build(&store, records(n), cfg);
        let leaves = tree.len(&store) as f64 / 512.0; // approx, fanout ~512

        let key = Key::new(vec![str_("users"), int((n / 2) as i64), int(1)]).encode();
        store.reset_counters();
        let _ = tree.insert_batch(&store, vec![(key, b"spliced".to_vec())]);
        let reads = store.reads();

        println!(
            "  {:>7} | {:>6.0} | {:>24} | {:>18.1}%",
            n,
            leaves,
            reads,
            100.0 * reads as f64 / leaves.max(1.0)
        );
        ratios.push(reads as f64 / leaves.max(1.0));
    }

    // The decisive check: reads must not grow proportionally with the data.
    // A full scan would read every leaf, giving a ratio near 1.0 at every
    // scale; the splice should stay far below that and *fall* as n grows,
    // because the internal-node overhead is amortized over more leaves.
    for (i, r) in ratios.iter().enumerate() {
        assert!(
            *r < 0.5,
            "insert read {:.0}% of the leaf count at scale {} — that is a scan, not a splice",
            r * 100.0,
            i
        );
    }
    assert!(
        ratios.last().unwrap() < ratios.first().unwrap(),
        "read cost should become a smaller fraction of the tree as it grows, got {:?}",
        ratios
    );
}

/// Two versions of a tree must share almost all of their nodes — that sharing
/// is what makes branching, time travel, and incremental sync cheap.
#[test]
fn versions_share_nearly_all_nodes() {
    let cfg = ChunkConfig::default();
    let store = MemStore::new();
    let n = 100_000;
    let tree = Tree::build(&store, records(n), cfg);
    let nodes_before = store.len();

    let updated = tree.insert_batch(
        &store,
        vec![(
            Key::new(vec![str_("users"), int(500), int(1)]).encode(),
            b"x".to_vec(),
        )],
    );
    let new_nodes = store.len() - nodes_before;

    println!(
        "\n  one-row insert into {} entries: {} new nodes ({} total), {:.4}% of the tree",
        n,
        new_nodes,
        store.len(),
        100.0 * new_nodes as f64 / nodes_before as f64
    );
    assert_ne!(updated.root, tree.root);
    // The invariant is O(depth), not a fraction of the tree. Expressing it as
    // a percentage made it depend on how many nodes the tree happens to have,
    // so a *shallower* tree — strictly better, fewer nodes rewritten in
    // absolute terms — could fail it. What must hold is that the rewrite is
    // bounded by the path from root to leaf and does not grow with the data.
    let depth = updated.depth(&store);
    assert!(
        new_nodes <= depth,
        "a one-row insert rewrote {} nodes for a tree of depth {}; \
         only the leaf and its ancestors should change",
        new_nodes,
        depth
    );
}

// ---------------------------------------------------------------------------
// 6. O(differences) diff
// ---------------------------------------------------------------------------

/// Diff cost must track the number of changes, not the size of the trees.
///
/// Equal subtrees share a hash and are skipped without being read, so diffing
/// two 100k-entry trees that differ in 5 places should read a handful of nodes.
#[test]
fn diff_cost_tracks_changes_not_size() {
    let cfg = ChunkConfig::default();
    let store = MemStore::new();
    let n = 100_000;
    let tree = Tree::build(&store, records(n), cfg);

    let mut rng = Rng::new(31337);
    let changes: Vec<(Vec<u8>, Vec<u8>)> = (0..5)
        .map(|j| {
            let i = rng.below(n);
            (
                Key::new(vec![str_("users"), int(i as i64)]).encode(),
                format!("changed-{}", j).into_bytes(),
            )
        })
        .collect();
    let changed = tree.insert_batch(&store, changes.clone());

    store.reset_counters();
    let diffs = tree.diff(&store, &changed);
    let reads = store.reads();

    println!(
        "\n  diff of two {}-entry trees differing in <= {} keys: {} reads, {} diffs",
        n,
        changes.len(),
        reads,
        diffs.len()
    );

    assert!(!diffs.is_empty(), "diff must find the changes");
    assert!(
        diffs.len() <= changes.len(),
        "expected at most {} diffs, got {}",
        changes.len(),
        diffs.len()
    );
    assert!(
        reads < 200,
        "diff read {} nodes for <= {} changes in a {}-entry tree; \
         expected cost proportional to differences",
        reads,
        changes.len(),
        n
    );
}

/// Diffing a tree against itself must be free — no reads at all.
#[test]
fn diff_of_identical_trees_is_free() {
    let cfg = ChunkConfig::default();
    let store = MemStore::new();
    let tree = Tree::build(&store, records(50_000), cfg);

    store.reset_counters();
    let diffs = tree.diff(&store, &tree);
    assert!(diffs.is_empty());
    assert_eq!(
        store.reads(),
        0,
        "identical roots must short-circuit without reading anything"
    );
}

// ---------------------------------------------------------------------------
// Correctness of the operations themselves
// ---------------------------------------------------------------------------

#[test]
fn get_scan_and_range_are_correct() {
    let cfg = ChunkConfig::with_target(16);
    let store = MemStore::new();
    let all = records(5_000);
    let tree = Tree::build(&store, all.clone(), cfg);

    // Every key resolves to its value.
    for (k, v) in &all {
        assert_eq!(tree.get(&store, k).as_ref(), Some(v));
    }
    // A missing key resolves to None.
    let missing = Key::new(vec![str_("users"), int(999_999)]).encode();
    assert!(tree.get(&store, &missing).is_none());

    // Scan returns everything, in key order.
    let scanned = tree.scan(&store);
    assert_eq!(scanned.len(), all.len());
    assert!(scanned.windows(2).all(|w| w[0].0 < w[1].0));

    // Range scan returns exactly the requested window.
    let start = Key::new(vec![str_("users"), int(100)]).encode();
    let end = Key::new(vec![str_("users"), int(200)]).encode();
    let ranged = tree.scan_range(&store, &start, &end);
    assert_eq!(ranged.len(), 100);
    assert!(ranged.iter().all(|(k, _)| k >= &start && k < &end));

    assert_eq!(tree.len(&store), all.len() as u64);
}

#[test]
fn delete_removes_only_the_named_keys() {
    let cfg = ChunkConfig::with_target(16);
    let store = MemStore::new();
    let all = records(2_000);
    let tree = Tree::build(&store, all.clone(), cfg);

    let victims: Vec<Vec<u8>> = all.iter().step_by(100).map(|(k, _)| k.clone()).collect();
    let after = tree.delete_batch(&store, &victims);

    assert_eq!(after.len(&store), (all.len() - victims.len()) as u64);
    for k in &victims {
        assert!(after.get(&store, k).is_none());
    }
    // Deleting the same keys twice is idempotent and converges.
    let again = after.delete_batch(&store, &victims);
    assert_eq!(again.root, after.root);
}

/// An empty tree is valid and behaves.
#[test]
fn empty_tree_is_well_defined() {
    let cfg = ChunkConfig::default();
    let store = MemStore::new();
    let empty = Tree::build(&store, Vec::new(), cfg);
    assert_eq!(empty.len(&store), 0);
    assert!(empty.scan(&store).is_empty());
    assert!(empty.get(&store, b"anything").is_none());

    // Two independently created empty trees have the same root.
    let store2 = MemStore::new();
    let empty2 = Tree::build(&store2, Vec::new(), cfg);
    assert_eq!(empty.root, empty2.root);
}
