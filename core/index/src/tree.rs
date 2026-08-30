// tree.rs — the content-defined Merkle search tree.
//
// A tree is identified by the hash of its root node. Because chunk boundaries
// are content-defined (see `chunk.rs`) and node encoding is canonical (see
// `node.rs`), the root hash is a pure function of the tree's contents:
//
//     same entries  =>  same root hash, always
//
// regardless of insertion order, batching, or which writer produced them. That
// single property is what gives Pond convergence without coordination: two
// writers who end up with the same data end up with the same bytes, so merge
// is a set operation and racing compactors are harmless.
//
// The tree indexes *segments*, not rows — a value here is a locator (segment
// hash + byte range + stats), so at 1 PB with 128 MB segments the index holds
// ~8M entries, not ~10^11. That is what keeps depth at 2 and the upper level
// under a megabyte, hence permanently cacheable.

use crate::chunk::{fingerprint, ChunkConfig};
use crate::node::{ChildRef, Node};
use crate::store::{Hash, NodeStore};

/// A sorted, content-defined Merkle index.
pub struct Tree {
    pub root: Hash,
    pub config: ChunkConfig,
}

/// One difference between two trees.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Diff {
    Added(Vec<u8>, Vec<u8>),
    Removed(Vec<u8>, Vec<u8>),
    /// (key, old_value, new_value)
    Changed(Vec<u8>, Vec<u8>, Vec<u8>),
}

impl Tree {
    /// Build a tree from sorted, deduplicated entries.
    ///
    /// Entries must be sorted by key. Use [`Tree::build`] which sorts for you
    /// if the input order is not guaranteed.
    pub fn build_sorted<S: NodeStore>(
        store: &S,
        entries: Vec<(Vec<u8>, Vec<u8>)>,
        config: ChunkConfig,
    ) -> Tree {
        debug_assert!(
            entries.windows(2).all(|w| w[0].0 < w[1].0),
            "build_sorted requires strictly sorted, deduplicated keys"
        );

        // Level 0: chunk the entries into leaves.
        //
        // Nodes are collected before being written so the whole level goes out
        // in one batch. On object storage that turns one PUT round trip per
        // node into one per level, which is the difference between minutes and
        // seconds when bulk-loading a large index.
        let mut pending: Vec<Node> = Vec::new();
        let mut current: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        for (k, v) in entries {
            // The boundary decision uses the key only. Using the value too
            // would mean an in-place value update reshapes the tree, losing
            // the "only adding or removing keys moves boundaries" property.
            let fp = fingerprint(&k);
            current.push((k, v));
            if config.is_boundary(fp, current.len()) {
                pending.push(Node::Leaf {
                    entries: std::mem::take(&mut current),
                });
            }
        }
        if !current.is_empty() {
            pending.push(Node::Leaf { entries: current });
        }
        let mut level = write_level(store, pending);

        if level.is_empty() {
            let node = Node::Leaf { entries: vec![] };
            return Tree {
                root: store.put(node.encode()),
                config,
            };
        }

        // Build internal levels bottom-up until a single root remains.
        while level.len() > 1 {
            level = chunk_level(store, level, config);
        }
        Tree {
            root: level.pop().unwrap().hash,
            config,
        }
    }

    /// Build a tree from arbitrary entries. Later duplicates of a key win.
    pub fn build<S: NodeStore>(
        store: &S,
        mut entries: Vec<(Vec<u8>, Vec<u8>)>,
        config: ChunkConfig,
    ) -> Tree {
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        entries.dedup_by(|a, b| a.0 == b.0);
        Tree::build_sorted(store, entries, config)
    }

    /// Look up one key. Costs one node read per level.
    pub fn get<S: NodeStore>(&self, store: &S, key: &[u8]) -> Option<Vec<u8>> {
        let mut hash = self.root.clone();
        loop {
            let node = Node::decode(&store.get(&hash)?)?;
            match node {
                Node::Leaf { entries } => {
                    return entries
                        .binary_search_by(|(k, _)| k.as_slice().cmp(key))
                        .ok()
                        .map(|i| entries[i].1.clone());
                }
                Node::Internal { children } => {
                    // Descend into the first child whose max_key >= key. The
                    // comparison against the child's key range is the pruning:
                    // subtrees that cannot contain the key are never fetched.
                    let idx = children.partition_point(|c| c.max_key.as_slice() < key);
                    if idx >= children.len() {
                        return None;
                    }
                    hash = children[idx].hash.clone();
                }
            }
        }
    }

    /// Total entries, read from the root's child counts without touching leaves.
    pub fn len<S: NodeStore>(&self, store: &S) -> u64 {
        store
            .get(&self.root)
            .and_then(|b| Node::decode(&b))
            .map(|n| n.count())
            .unwrap_or(0)
    }

    /// Depth in levels (1 = a single leaf).
    pub fn depth<S: NodeStore>(&self, store: &S) -> usize {
        let mut d = 1;
        let mut hash = self.root.clone();
        while let Some(Node::Internal { children }) =
            store.get(&hash).and_then(|b| Node::decode(&b))
        {
            match children.first() {
                Some(c) => {
                    hash = c.hash.clone();
                    d += 1;
                }
                None => break,
            }
        }
        d
    }

    /// All entries in key order.
    pub fn scan<S: NodeStore>(&self, store: &S) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut out = Vec::new();
        collect(store, &self.root, &mut out);
        out
    }

    /// Entries whose key falls in `[start, end)`.
    ///
    /// Subtrees entirely below `start` are skipped without being read — this
    /// is why a prefix scan costs bytes proportional to the range, not to the
    /// collection.
    pub fn scan_range<S: NodeStore>(
        &self,
        store: &S,
        start: &[u8],
        end: &[u8],
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut out = Vec::new();
        collect_range(store, &self.root, start, end, &mut out);
        out
    }

    /// Insert or update entries, returning a new tree. The old tree remains
    /// valid — that is what makes branching and time travel free.
    ///
    /// # Write cost
    ///
    /// Only nodes whose content actually changed are written: everything else
    /// hashes to a node the store already holds and is deduplicated. Measured
    /// at 3 new nodes for a one-row insert into a 100k-entry tree (~1.6% of
    /// it), amortizing to 0.02 nodes per record at batch size 10k. Since PUTs
    /// are the expensive operation against object storage, this is the number
    /// that decides whether the design is affordable, and it holds.
    ///
    /// # Read cost
    ///
    /// The splice reads the internal nodes — about 1/fanout of the tree — plus
    /// only the leaves it actually touches. It never reads the rest of the
    /// data. Measured: a one-row insert costs 6 reads at 10k entries, 9 at
    /// 100k, 12 at 500k, against 977 leaves at that last size. The cost tracks
    /// the tree's shape, not its size, and falls as a *fraction* of the tree
    /// as the tree grows.
    ///
    /// # Why this is exactly equivalent to a rebuild
    ///
    /// Two properties make the local splice produce byte-identical output to
    /// `build`, rather than merely similar output:
    ///
    /// - **Leaf boundaries are stable under insertion.** A boundary depends on
    ///   the entry's own fingerprint and on having at least `min_entries`
    ///   since the chunk started. Inserting entries can only increase that
    ///   count, so an entry that ended a chunk still ends it. The affected
    ///   span therefore still terminates where it did, and re-chunking it in
    ///   isolation gives the same splits the global pass would.
    /// - **The spine is rebuilt over the whole leaf list.** Internal boundaries
    ///   key off child hashes, which do change when a child changes, so those
    ///   levels cannot be spliced locally — they are re-chunked in full. That
    ///   is affordable precisely because it needs no leaf reads.
    ///
    /// The span is widened by one leaf on each side so an insert near an edge
    /// settles back onto the existing boundaries instead of leaving a seam.
    ///
    /// `incremental_insert_matches_bulk_build` and
    /// `history_independence_across_batch_splits` in `tests/acceptance.rs` are
    /// the oracle for all of this: they assert byte-identical roots across
    /// random split points and random batch splits, so a subtly wrong splice
    /// fails loudly rather than silently diverging.
    pub fn insert_batch<S: NodeStore>(
        &self,
        store: &S,
        mut updates: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Tree {
        if updates.is_empty() {
            return Tree {
                root: self.root.clone(),
                config: self.config,
            };
        }
        updates.sort_by(|a, b| a.0.cmp(&b.0));
        updates.dedup_by(|a, b| a.0 == b.0);

        // Walk the internal nodes to get the leaf pointer list without reading
        // a single leaf. Internal nodes are ~1/fanout of the tree — at fanout
        // 512 a million-entry index has ~1950 leaves described by about 5
        // internal nodes — so this is cheap, and it is what makes the splice
        // exactly equivalent to a rebuild rather than approximately so.
        // One descent establishes the depth, so the walk below knows where
        // the leaf level is without probing each child.
        let depth = self.depth(store);
        let mut leaves: Vec<ChildRef> = Vec::new();
        if !collect_leaf_refs(store, &self.root, depth.saturating_sub(1), &mut leaves) {
            // Unreadable tree: fall back to a rebuild from whatever scans.
            let existing = self.scan(store);
            return Tree::build_sorted(store, merge_sorted(existing, updates), self.config);
        }

        if leaves.is_empty() {
            return Tree::build_sorted(store, updates, self.config);
        }

        // The affected span is every leaf that could hold one of the updated
        // keys. `max_key` is the largest key in a leaf, so the first leaf that
        // can hold key k is the first whose max_key >= k.
        let first_key = &updates.first().unwrap().0;
        let last_key = &updates.last().unwrap().0;
        let start = leaves.partition_point(|l| l.max_key.as_slice() < first_key.as_slice());
        let start = start.min(leaves.len() - 1);
        let end = leaves
            .partition_point(|l| l.max_key.as_slice() < last_key.as_slice())
            .min(leaves.len() - 1);

        // Widen by one leaf on each side. A boundary is a property of the
        // entry run, so an insert near a leaf edge can shift where the split
        // falls; including the neighbours lets the re-chunk settle back onto
        // the existing boundaries instead of leaving a seam.
        let start = start.saturating_sub(1);
        let end = (end + 1).min(leaves.len() - 1);

        // Read only the affected leaves.
        let mut span_entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        for leaf in &leaves[start..=end] {
            match store.get(&leaf.hash).and_then(|b| Node::decode(&b)) {
                Some(Node::Leaf { entries }) => span_entries.extend(entries),
                _ => {
                    // Not a leaf, or unreadable — fall back rather than guess.
                    let existing = self.scan(store);
                    return Tree::build_sorted(
                        store,
                        merge_sorted(existing, updates),
                        self.config,
                    );
                }
            }
        }

        // Re-chunk the span with the updates merged in. Chunking restarts at
        // the span's first entry, which is where a chunk started in the
        // original build too, so the boundary decisions line up.
        let merged = merge_sorted(span_entries, updates);
        let mut pending: Vec<Node> = Vec::new();
        let mut current: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        for (k, val) in merged {
            let fp = fingerprint(&k);
            current.push((k, val));
            if self.config.is_boundary(fp, current.len()) {
                pending.push(Node::Leaf {
                    entries: std::mem::take(&mut current),
                });
            }
        }
        if !current.is_empty() {
            pending.push(Node::Leaf { entries: current });
        }
        let rechunked = write_level(store, pending);

        // Splice the new leaves in place of the old span. Leaves outside it
        // are reused by hash: not read, not rewritten, and deduplicated by the
        // store, which is why a one-row insert costs a handful of PUTs.
        let mut new_level: Vec<ChildRef> = Vec::with_capacity(leaves.len());
        new_level.extend_from_slice(&leaves[..start]);
        new_level.extend(rechunked);
        new_level.extend_from_slice(&leaves[end + 1..]);

        // Rebuild the spine over the full leaf list. Internal boundaries key
        // off child hashes, which change when a child changes, so the levels
        // above must be re-chunked as a whole — exactly as a fresh build does.
        // No leaves are read here.
        if new_level.is_empty() {
            let node = Node::Leaf { entries: vec![] };
            return Tree {
                root: store.put(node.encode()),
                config: self.config,
            };
        }
        let mut level = new_level;
        while level.len() > 1 {
            level = chunk_level(store, level, self.config);
        }
        Tree {
            root: level.pop().unwrap().hash,
            config: self.config,
        }
    }

    /// Delete keys, returning a new tree.
    pub fn delete_batch<S: NodeStore>(&self, store: &S, keys: &[Vec<u8>]) -> Tree {
        let drop: std::collections::HashSet<&Vec<u8>> = keys.iter().collect();
        let kept: Vec<(Vec<u8>, Vec<u8>)> = self
            .scan(store)
            .into_iter()
            .filter(|(k, _)| !drop.contains(k))
            .collect();
        Tree::build_sorted(store, kept, self.config)
    }

    /// Differences between this tree and `other`.
    ///
    /// Identical subtrees are skipped in O(1) by comparing child hashes, so
    /// the cost is proportional to the number of differences, not to the size
    /// of either tree. `diff_node_reads` in the tests measures this directly.
    pub fn diff<S: NodeStore>(&self, store: &S, other: &Tree) -> Vec<Diff> {
        let mut mine = Vec::new();
        let mut theirs = Vec::new();
        collect_differing(store, &self.root, &other.root, &mut mine, &mut theirs);

        let mut out = Vec::new();
        let mut i = 0usize;
        let mut j = 0usize;
        while i < mine.len() || j < theirs.len() {
            match (mine.get(i), theirs.get(j)) {
                (Some((k1, v1)), Some((k2, v2))) => match k1.cmp(k2) {
                    std::cmp::Ordering::Equal => {
                        if v1 != v2 {
                            out.push(Diff::Changed(k1.clone(), v1.clone(), v2.clone()));
                        }
                        i += 1;
                        j += 1;
                    }
                    std::cmp::Ordering::Less => {
                        out.push(Diff::Removed(k1.clone(), v1.clone()));
                        i += 1;
                    }
                    std::cmp::Ordering::Greater => {
                        out.push(Diff::Added(k2.clone(), v2.clone()));
                        j += 1;
                    }
                },
                (Some((k, v)), None) => {
                    out.push(Diff::Removed(k.clone(), v.clone()));
                    i += 1;
                }
                (None, Some((k, v))) => {
                    out.push(Diff::Added(k.clone(), v.clone()));
                    j += 1;
                }
                (None, None) => break,
            }
        }
        out
    }

    /// Merge two trees into one.
    ///
    /// `resolve` decides which value wins when both sides hold the same key.
    /// It must be a pure function of the two values and must be commutative
    /// (`resolve(a,b) == resolve(b,a)`) — with a total order on versions
    /// (physical, logical, writer_id) last-writer-wins satisfies that, which
    /// is exactly why the writer id had to exist.
    ///
    /// Given that, merge is a join over a semilattice: commutative,
    /// associative, idempotent. Two replicas merging the same inputs produce
    /// byte-identical roots, so racing compactors converge for free.
    pub fn merge<S: NodeStore, F>(&self, store: &S, other: &Tree, resolve: F) -> Tree
    where
        F: Fn(&[u8], &[u8]) -> Vec<u8>,
    {
        let a = self.scan(store);
        let b = other.scan(store);
        let mut out: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(a.len() + b.len());
        let (mut i, mut j) = (0usize, 0usize);
        while i < a.len() || j < b.len() {
            match (a.get(i), b.get(j)) {
                (Some((k1, v1)), Some((k2, v2))) => match k1.cmp(k2) {
                    std::cmp::Ordering::Equal => {
                        out.push((k1.clone(), resolve(v1, v2)));
                        i += 1;
                        j += 1;
                    }
                    std::cmp::Ordering::Less => {
                        out.push((k1.clone(), v1.clone()));
                        i += 1;
                    }
                    std::cmp::Ordering::Greater => {
                        out.push((k2.clone(), v2.clone()));
                        j += 1;
                    }
                },
                (Some(e), None) => {
                    out.push(e.clone());
                    i += 1;
                }
                (None, Some(e)) => {
                    out.push(e.clone());
                    j += 1;
                }
                (None, None) => break,
            }
        }
        Tree::build_sorted(store, out, self.config)
    }
}

// ---------------------------------------------------------------------------
// Internals
// ---------------------------------------------------------------------------

/// Chunk one level of child pointers into the level above.
fn chunk_level<S: NodeStore>(
    store: &S,
    level: Vec<ChildRef>,
    config: ChunkConfig,
) -> Vec<ChildRef> {
    // A level small enough to fit in one node is never worth splitting.
    //
    // Boundaries are a coin flip with probability 1/target per child, so near
    // the top of the tree — where a level holds far fewer than `target`
    // children — the expected number of boundaries is well under one. When one
    // fires anyway it does not divide the level usefully; it just inserts a
    // level, and a level is a dependent round trip that cannot be parallelised
    // because each node names the next. Measured: a 128-child level at
    // target 8192 split in two, turning a 2-deep tree into a 3-deep one.
    //
    // Collapsing is still history-independent. The decision is a function of
    // the level's contents alone — the same set of children always produces
    // the same answer — which is the property that matters, not whether a hash
    // was consulted. Two writers who converge on the same data still converge
    // on the same bytes.
    if level.len() <= config.target_entries as usize {
        return write_level(store, vec![Node::Internal { children: level }]);
    }

    let mut pending: Vec<Node> = Vec::new();
    let mut current: Vec<ChildRef> = Vec::new();
    for child in level {
        // Boundary on the child's hash: content-defined at every level, so an
        // internal node's shape is as history-independent as a leaf's.
        let fp = fingerprint(child.hash.as_bytes());
        current.push(child);
        if config.is_boundary(fp, current.len()) {
            pending.push(Node::Internal {
                children: std::mem::take(&mut current),
            });
        }
    }
    if !current.is_empty() {
        pending.push(Node::Internal { children: current });
    }
    write_level(store, pending)
}

/// Encode a whole level and write it in one batch, returning the pointers.
///
/// Splitting "build the nodes" from "write the nodes" is what allows the write
/// to be batched at all: every node of a level is known before any node of the
/// level above, so there is no dependency forcing them out one at a time.
fn write_level<S: NodeStore>(store: &S, nodes: Vec<Node>) -> Vec<ChildRef> {
    if nodes.is_empty() {
        return Vec::new();
    }
    let meta: Vec<(Vec<u8>, u64)> = nodes
        .iter()
        .map(|n| (n.max_key().unwrap_or_default(), n.count()))
        .collect();
    let encoded: Vec<Vec<u8>> = nodes.iter().map(|n| n.encode()).collect();
    let hashes = store.put_batch(encoded);
    meta.into_iter()
        .zip(hashes)
        .map(|((max_key, count), hash)| ChildRef {
            max_key,
            hash,
            count,
        })
        .collect()
}

/// Collect the leaf-level child pointers, reading internal nodes only.
///
/// Internal nodes are roughly 1/fanout of the tree, so this is the cheap part
/// of the structure to traverse: it is what lets an insert rebuild the spine
/// exactly (rather than approximately) without paying to read the data.
///
/// `levels_below` is how many levels sit under this node (0 = this node is a
/// leaf). It is what lets the walk stop one level above the leaves and take
/// their pointers without fetching them — checking each child's type by
/// reading it would be the very scan this avoids.
///
/// Returns false if the tree could not be walked, so callers can fall back
/// rather than silently producing a wrong tree.
fn collect_leaf_refs<S: NodeStore>(
    store: &S,
    hash: &str,
    levels_below: usize,
    out: &mut Vec<ChildRef>,
) -> bool {
    let Some(node) = store.get(hash).and_then(|b| Node::decode(&b)) else {
        return false;
    };
    match node {
        Node::Leaf { entries } => {
            // A single-leaf tree: the root is the only leaf.
            out.push(ChildRef {
                max_key: entries.last().map(|(k, _)| k.clone()).unwrap_or_default(),
                hash: hash.to_string(),
                count: entries.len() as u64,
            });
            true
        }
        Node::Internal { children } => {
            if levels_below <= 1 {
                // These children are leaves. Take their pointers as-is —
                // reading them here is exactly the scan this function exists
                // to avoid, and the pointer already carries the key range and
                // count that the caller needs.
                out.extend(children);
                return true;
            }
            for c in children {
                if !collect_leaf_refs(store, &c.hash, levels_below - 1, out) {
                    return false;
                }
            }
            true
        }
    }
}

/// Every entry under `hash`, in key order.
///
/// Walked a level at a time rather than a node at a time. The recursive
/// descent this replaces read one node per round trip, so a scan cost one wait
/// per *node* — 51 of them for a 100,000-row collection, ~1.6 s at
/// object-storage latency — even though the hashes of every node on a level
/// are known as soon as the level above is decoded. Reading them together
/// costs one wait per level, which is depth rather than width: the same 51
/// nodes over a tree of depth 3 become 3 waits.
///
/// Order is preserved because a level is decoded left to right and each node's
/// children are appended in order, so the frontier is always in key order and
/// leaves are drained in the order they are met.
/// Drop repeats from a level, preserving first-seen order.
///
/// Writers branch from common history, so their trees share subtrees — and a
/// shared subtree has the same hash in every tree that contains it, because
/// nodes are content-addressed. Reading it once per tree is the same bytes
/// fetched repeatedly. Deduplicating the frontier turns "requests grow with
/// the number of writers" into "requests grow with the number of *distinct*
/// nodes", which for writers that share history is much closer to flat.
///
/// It is safe for the same reason it is possible: the merge of a value with
/// itself is that value. Idempotence is one of the three laws the record merge
/// is tested against, so collapsing duplicates cannot change an answer.
fn dedup_preserving_order(hashes: Vec<String>) -> Vec<String> {
    let mut seen = std::collections::HashSet::with_capacity(hashes.len());
    let mut out = Vec::with_capacity(hashes.len());
    for h in hashes {
        if seen.insert(h.clone()) {
            out.push(h);
        }
    }
    out
}

/// Every entry in several trees at once, grouped by key, one wait per level.
///
/// The scan counterpart of [`get_from_roots`], and it exists for the same
/// reason: a reader's view is the merge of every writer's tree, and building
/// that merge is a write. A scan that merges first pays a round trip and a PUT
/// per writer before it reads anything.
///
/// A scan does not need the merged tree either. It needs every entry, and
/// which values share a key. Walking all the trees together — batch a level,
/// collect the next — costs one wait per level however many trees there are,
/// and writes nothing. Entries are returned grouped by key so the caller can
/// fold each group with whatever resolution it would have used inside the
/// merge.
///
/// Keys come back in order. Entries arrive interleaved across trees, so they
/// are sorted here rather than emerging sorted as they do from a single tree.
/// That is CPU against I/O, which is the right trade by orders of magnitude at
/// object-storage latency.
pub fn scan_from_roots<S: NodeStore>(
    store: &S,
    roots: &[String],
) -> Vec<(Vec<u8>, Vec<Vec<u8>>)> {
    let mut frontier = dedup_preserving_order(roots.to_vec());
    let mut entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();

    while !frontier.is_empty() {
        let mut next = Vec::new();
        for bytes in store.get_batch(&frontier) {
            let Some(node) = bytes.and_then(|b| Node::decode(&b)) else {
                continue;
            };
            match node {
                Node::Leaf { entries: e } => entries.extend(e),
                Node::Internal { children } => {
                    next.extend(children.into_iter().map(|c| c.hash));
                }
            }
        }
        frontier = dedup_preserving_order(next);
    }

    entries.sort_by(|a, b| a.0.cmp(&b.0));

    let mut out: Vec<(Vec<u8>, Vec<Vec<u8>>)> = Vec::new();
    for (k, v) in entries {
        match out.last_mut() {
            Some((last_key, vals)) if *last_key == k => vals.push(v),
            _ => out.push((k, vec![v])),
        }
    }
    out
}

/// As [`scan_from_roots`], restricted to `[start, end)`.
///
/// Subtrees outside the range are dropped from the frontier before the batch
/// is issued, so a narrow range still reads only the nodes it needs.
pub fn scan_range_from_roots<S: NodeStore>(
    store: &S,
    roots: &[String],
    start: &[u8],
    end: &[u8],
) -> Vec<(Vec<u8>, Vec<Vec<u8>>)> {
    let mut frontier = dedup_preserving_order(roots.to_vec());
    let mut entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();

    while !frontier.is_empty() {
        let mut next = Vec::new();
        for bytes in store.get_batch(&frontier) {
            let Some(node) = bytes.and_then(|b| Node::decode(&b)) else {
                continue;
            };
            match node {
                Node::Leaf { entries: e } => {
                    for (k, v) in e {
                        if k.as_slice() >= start && k.as_slice() < end {
                            entries.push((k, v));
                        }
                    }
                }
                Node::Internal { children } => {
                    for c in children {
                        if c.max_key.as_slice() < start {
                            continue;
                        }
                        next.push(c.hash);
                        if c.max_key.as_slice() >= end {
                            break;
                        }
                    }
                }
            }
        }
        frontier = dedup_preserving_order(next);
    }

    entries.sort_by(|a, b| a.0.cmp(&b.0));

    let mut out: Vec<(Vec<u8>, Vec<Vec<u8>>)> = Vec::new();
    for (k, v) in entries {
        match out.last_mut() {
            Some((last_key, vals)) if *last_key == k => vals.push(v),
            _ => out.push((k, vec![v])),
        }
    }
    out
}

/// Look one key up in several trees at once, one wait per level.
///
/// # Why this exists
///
/// Every writer publishes its own tree, and a reader's view is the merge of
/// all of them. Building that merge to answer a point read is the obvious
/// implementation and the expensive one: merging is a write, so a *read* on a
/// fresh reader was materialising W-1 merged trees and storing their nodes.
/// Measured at 64 writers: 141 round trips, 100 PUTs, 4.3 s modelled, against
/// 1 round trip at a single writer. Linear in the number of writers, on the
/// path a key-value or OLTP workload takes for every operation.
///
/// A point read does not need the merge. It needs the handful of values
/// stored under one key, which is at most one per tree. Descending all the
/// trees in lockstep — batch the current level, take each node's one relevant
/// child, batch the next — costs one wait per *level* and writes nothing. The
/// caller merges the values it gets back, which for a single key is cheap and
/// needs no storage.
///
/// Returns one value per tree that holds the key, in the order the roots were
/// given, so a caller can fold them with the same resolution it would have
/// used inside the merge.
///
/// The pruning and the leaf lookup are the same rules as [`Tree::get`]: the
/// first child whose `max_key` is not below the key, then a binary search in
/// the leaf. They have to stay the same rules — a point read that descended
/// differently from the merge would answer differently, which is worse than
/// being slow.
pub fn get_from_roots<S: NodeStore>(store: &S, roots: &[String], key: &[u8]) -> Vec<Vec<u8>> {
    let mut frontier = dedup_preserving_order(roots.to_vec());
    let mut found = Vec::new();

    while !frontier.is_empty() {
        let mut next = Vec::with_capacity(frontier.len());
        for bytes in store.get_batch(&frontier) {
            let Some(node) = bytes.and_then(|b| Node::decode(&b)) else {
                continue;
            };
            match node {
                Node::Leaf { entries } => {
                    if let Ok(i) = entries.binary_search_by(|(k, _)| k.as_slice().cmp(key)) {
                        found.push(entries[i].1.clone());
                    }
                }
                Node::Internal { children } => {
                    // The first child whose range can contain the key. If none
                    // can, this tree has nothing under it and drops out of the
                    // descent — which is how the frontier narrows.
                    let idx = children.partition_point(|c| c.max_key.as_slice() < key);
                    if let Some(c) = children.get(idx) {
                        next.push(c.hash.clone());
                    }
                }
            }
        }
        frontier = dedup_preserving_order(next);
    }

    found
}

fn collect<S: NodeStore>(store: &S, hash: &str, out: &mut Vec<(Vec<u8>, Vec<u8>)>) {
    let mut frontier = vec![hash.to_string()];
    while !frontier.is_empty() {
        let mut next = Vec::new();
        for bytes in store.get_batch(&frontier) {
            // A node that could not be read contributes nothing here, exactly
            // as before. Whether that is data loss or an empty subtree is not
            // decidable at this layer; `EngineStore` counts the failures and
            // the caller refuses a result one passed through.
            let Some(node) = bytes.and_then(|b| Node::decode(&b)) else {
                continue;
            };
            match node {
                Node::Leaf { entries } => out.extend(entries),
                Node::Internal { children } => {
                    next.extend(children.into_iter().map(|c| c.hash));
                }
            }
        }
        frontier = next;
    }
}

/// The entries under `hash` within `[start, end)`, in key order.
///
/// Level at a time, like [`collect`], and for the same reason — but the
/// pruning is what makes this worth doing rather than just faster. Subtrees
/// outside the range are dropped from the frontier *before* the batch is
/// issued, so a narrow range still reads only the nodes it needs, and it reads
/// them in one wait per level instead of one per node.
fn collect_range<S: NodeStore>(
    store: &S,
    hash: &str,
    start: &[u8],
    end: &[u8],
    out: &mut Vec<(Vec<u8>, Vec<u8>)>,
) {
    let mut frontier = vec![hash.to_string()];
    while !frontier.is_empty() {
        let mut next = Vec::new();
        for bytes in store.get_batch(&frontier) {
            let Some(node) = bytes.and_then(|b| Node::decode(&b)) else {
                continue;
            };
            match node {
                Node::Leaf { entries } => {
                    for (k, v) in entries {
                        if k.as_slice() >= start && k.as_slice() < end {
                            out.push((k, v));
                        }
                    }
                }
                Node::Internal { children } => {
                    for c in children {
                        // Below the range: skip without reading.
                        if c.max_key.as_slice() < start {
                            continue;
                        }
                        next.push(c.hash);
                        // Once a subtree's max key reaches the end bound,
                        // later siblings are all above the range.
                        if c.max_key.as_slice() >= end {
                            break;
                        }
                    }
                }
            }
        }
        frontier = next;
    }
}

/// Walk two trees in parallel, collecting only the entries under subtrees
/// whose hashes differ.
///
/// Equal hashes mean byte-identical content, so those subtrees are skipped in
/// O(1) with no reads at all — that is what makes diff cost proportional to
/// the number of changes rather than to the size of the trees.
///
/// Children are matched by key range rather than by position, because a change
/// can shift a chunk boundary and leave the two levels with different child
/// counts. Ranges that overlap are recursed into; a child with no overlapping
/// counterpart is wholly added or removed. Recursion can visit a node from
/// more than one pairing when boundaries have shifted, so results are
/// deduplicated by key at the end.
fn collect_differing<S: NodeStore>(
    store: &S,
    a: &str,
    b: &str,
    out_a: &mut Vec<(Vec<u8>, Vec<u8>)>,
    out_b: &mut Vec<(Vec<u8>, Vec<u8>)>,
) {
    descend_differing(store, a, b, out_a, out_b);
    dedup_by_key(out_a);
    dedup_by_key(out_b);
}

fn descend_differing<S: NodeStore>(
    store: &S,
    a: &str,
    b: &str,
    out_a: &mut Vec<(Vec<u8>, Vec<u8>)>,
    out_b: &mut Vec<(Vec<u8>, Vec<u8>)>,
) {
    if a == b {
        return; // identical subtrees — the whole point of Merkle addressing
    }
    let na = store.get(a).and_then(|x| Node::decode(&x));
    let nb = store.get(b).and_then(|x| Node::decode(&x));

    match (na, nb) {
        (Some(Node::Internal { children: ca }), Some(Node::Internal { children: cb })) => {
            let (mut i, mut j) = (0usize, 0usize);
            while i < ca.len() && j < cb.len() {
                // Identical child subtrees: skip both, no reads.
                if ca[i].hash == cb[j].hash {
                    i += 1;
                    j += 1;
                    continue;
                }
                match ca[i].max_key.cmp(&cb[j].max_key) {
                    std::cmp::Ordering::Equal => {
                        descend_differing(store, &ca[i].hash, &cb[j].hash, out_a, out_b);
                        i += 1;
                        j += 1;
                    }
                    std::cmp::Ordering::Less => {
                        // A's child ends first, so it overlaps B's current
                        // child. Recurse into the pair, then advance A.
                        descend_differing(store, &ca[i].hash, &cb[j].hash, out_a, out_b);
                        i += 1;
                    }
                    std::cmp::Ordering::Greater => {
                        descend_differing(store, &ca[i].hash, &cb[j].hash, out_a, out_b);
                        j += 1;
                    }
                }
            }
            // Trailing children on either side have no counterpart at all.
            for c in &ca[i..] {
                collect(store, &c.hash, out_a);
            }
            for c in &cb[j..] {
                collect(store, &c.hash, out_b);
            }
        }
        (a_node, b_node) => {
            if let Some(n) = a_node {
                flatten(store, n, out_a);
            }
            if let Some(n) = b_node {
                flatten(store, n, out_b);
            }
        }
    }
}

/// Sort by key and drop duplicates, which can arise when a shifted chunk
/// boundary causes the same leaf to be reached through two pairings.
fn dedup_by_key(v: &mut Vec<(Vec<u8>, Vec<u8>)>) {
    v.sort_by(|x, y| x.0.cmp(&y.0));
    v.dedup_by(|x, y| x.0 == y.0);
}

fn flatten<S: NodeStore>(store: &S, node: Node, out: &mut Vec<(Vec<u8>, Vec<u8>)>) {
    match node {
        Node::Leaf { entries } => out.extend(entries),
        Node::Internal { children } => {
            for c in children {
                collect(store, &c.hash, out);
            }
        }
    }
}

/// Merge two sorted entry lists; entries from `updates` win on equal keys.
fn merge_sorted(
    base: Vec<(Vec<u8>, Vec<u8>)>,
    updates: Vec<(Vec<u8>, Vec<u8>)>,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut out = Vec::with_capacity(base.len() + updates.len());
    let (mut i, mut j) = (0usize, 0usize);
    while i < base.len() || j < updates.len() {
        match (base.get(i), updates.get(j)) {
            (Some((k1, v1)), Some((k2, v2))) => match k1.cmp(k2) {
                std::cmp::Ordering::Equal => {
                    out.push((k2.clone(), v2.clone()));
                    i += 1;
                    j += 1;
                }
                std::cmp::Ordering::Less => {
                    out.push((k1.clone(), v1.clone()));
                    i += 1;
                }
                std::cmp::Ordering::Greater => {
                    out.push((k2.clone(), v2.clone()));
                    j += 1;
                }
            },
            (Some(e), None) => {
                out.push(e.clone());
                i += 1;
            }
            (None, Some(e)) => {
                out.push(e.clone());
                j += 1;
            }
            (None, None) => break,
        }
    }
    out
}
