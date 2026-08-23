// pond_engine — the API a database, a lens, or an application needs.
//
// This is the layer that turns four validated pieces into something callable:
//
//     pond_index    content-defined Merkle index    (constant-depth lookup)
//     pond_record   typed fields + per-field merge  (convergence)
//     pond_cache    hash-keyed two-tier cache       (no coherence protocol)
//     pond_kernel   put / get(range) / list / delete on any backend
//
// # What an engine is
//
// One writer's view of a pond. It owns a `writer_id` and writes **only** keys
// derived from it, which is the whole concurrency story: two writers can never
// collide on a key, so last-writer-wins is never wrong and no compare-and-swap
// is needed anywhere. That matters beyond elegance — conditional writes exist
// on object storage but not on a local filesystem, and a primitive available
// on only some backends would fork the correctness argument in two.
//
// ```text
// <collection>/index/<hash>      immutable index nodes
// <collection>/segments/<hash>   immutable data
// heads/writer/<id>/<seq>.<hash> {collection -> root} — the publish unit
// history/<collection>           the roots it has been published at
// ```
//
// # Why the head spans collections
//
// Publishing any number of collections is a single object write, and object
// stores already give single-object atomicity. So all-or-nothing across
// collections costs nothing and needs no transaction subsystem: a reader sees
// either the whole previous state or the whole new one.
//
// # Reading
//
// `open_reader` merges every writer's head, so a reader sees all writers
// without any of them coordinating. Merge is a semilattice join over
// content-addressed trees, so it is commutative, associative and idempotent —
// two readers that have seen the same heads compute byte-identical state.

use std::collections::BTreeMap;
use std::io;

use pond_cache::{BlobCache, CacheConfig};
use pond_index::{ChunkConfig, Key, Tree};
use pond_kernel::ObjectStore;
use pond_record::{decode_record, encode_record, merge_records, Head, Record};

pub mod spill;
pub mod store;
pub mod history;
pub use history::{Entry as HistoryEntry, History};
pub use spill::SPILL_THRESHOLD;
pub use store::EngineStore;

/// Errors an engine operation can produce.
#[derive(Debug)]
pub enum EngineError {
    Io(io::Error),
    /// A stored node or record could not be decoded — corruption, or a format
    /// from a future version.
    Corrupt(String),
    NotFound(String),
}

impl std::fmt::Display for EngineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EngineError::Io(e) => write!(f, "io error: {}", e),
            EngineError::Corrupt(m) => write!(f, "corrupt data: {}", m),
            EngineError::NotFound(m) => write!(f, "not found: {}", m),
        }
    }
}

impl std::error::Error for EngineError {}

impl From<io::Error> for EngineError {
    fn from(e: io::Error) -> Self {
        EngineError::Io(e)
    }
}

pub type Result<T> = std::result::Result<T, EngineError>;

/// The prefix every head of one writer shares.
///
/// A writer only ever writes under its own prefix, which is what makes both
/// publishing and deleting safe without a compare-and-swap.
pub fn head_prefix(writer_id: u64) -> String {
    // `heads/writer/<id>/`, not `heads/writer-<id>/`. The old layout put a flat
    // object at `heads/writer-<id>`; on a local filesystem that is a *file*,
    // and a directory cannot share its path, so an upgraded pond failed its
    // first publish with EISDIR. Object stores have no directories and would
    // never have shown it. A separate path segment cannot collide with any old
    // key, and `list_paths("heads/")` still walks both.
    //
    // It is a directory prefix rather than a string prefix for a second
    // reason: `list_paths` is specified as a prefix listing and S3 implements
    // one, while the local backend once listed a directory, so a partial name
    // matched on one backend and nothing on the other. That divergence is
    // fixed, but a hierarchical layout reads the same under either.
    format!("heads/writer/{:016x}/", writer_id)
}

/// Where one published head lives: `heads/writer/<id>/<seq>.<content hash>`.
///
/// # Why three parts
///
/// The writer id partitions the namespace, so no two writers ever contend and
/// last-writer-wins is never wrong. That much is the original design.
///
/// The sequence number orders one writer's publishes, so a reader can pick the
/// latest of them from the listing alone.
///
/// The content hash is what lets compaction finish its job. A compacted head
/// records the heads it absorbed by content hash; if that hash is only inside
/// the head object, a reader has to fetch every head to discover it can ignore
/// it, and the read path stays linear in writers no matter how much compaction
/// runs. Putting it in the key makes absorption decidable from the LIST, so
/// absorbed heads are never fetched — and it makes deleting them safe, because
/// a writer that publishes again produces different bytes, a different hash,
/// and therefore a different key that the compactor never saw and cannot
/// delete. That is the same argument that removes the need for a conditional
/// write on the publish path, applied to the delete path.
pub fn head_key(writer_id: u64, seq: u64, content_hash: &str) -> String {
    format!("{}{:016x}.{}", head_prefix(writer_id), seq, content_hash)
}

/// A head key taken apart: (writer id, sequence, content hash).
///
/// Accepts the original `heads/writer-<id>` form as sequence 0 with no hash,
/// so a pond written before head keys carried a sequence still reads. Such a
/// head can never be recognised as absorbed — it has no hash in its key — so
/// it is always fetched and merged, which is the safe direction.
pub fn parse_head_key(path: &str) -> Option<(u64, u64, Option<&str>)> {
    if let Some(rest) = path.strip_prefix("heads/writer/") {
        let (id, tail) = rest.split_once('/')?;
        let (seq, hash) = tail.split_once('.')?;
        if hash.is_empty() || hash.contains('/') {
            return None;
        }
        return Some((
            u64::from_str_radix(id, 16).ok()?,
            u64::from_str_radix(seq, 16).ok()?,
            Some(hash),
        ));
    }
    // The original flat form, `heads/writer-<id>`.
    let id = path.strip_prefix("heads/writer-")?;
    Some((u64::from_str_radix(id, 16).ok()?, 0, None))
}

/// The live heads from a listing: for each writer, every head at its highest
/// sequence.
///
/// Usually that is exactly one per writer, and superseded sequences are
/// dropped without being read — which is what keeps the read path flat.
///
/// *Every* head at the highest sequence, not one of them, because a tie is not
/// a duplicate. Two heads at the same sequence under one writer id are two
/// processes that computed the same next sequence from the same view — which
/// happens whenever two processes share an id, and `stable_writer_id` hashes
/// hostname and username, so two containers from one image share one. Keeping
/// a single head there silently discarded one process's writes. They are
/// concurrent publishes and merging both is correct: merge is a commutative,
/// idempotent join.
///
/// Compacted heads are never collapsed against each other at all — see the
/// grouping below.
pub fn latest_heads(paths: &[String]) -> Vec<(u64, u64, Option<String>, String)> {
    // Grouped by (writer, is-current-layout) rather than by writer alone. A
    // pre-sequence head is not a *stale* version of a current one — the writer
    // that wrote it may never run again, and nothing else holds its rows — so
    // it must never be superseded by one. It stays in its own group and is
    // always read, until a compaction pass folds it in and retires it.
    // Compacted heads are never collapsed against each other, because they are
    // not versions of one another. A writer's heads are cumulative — sequence
    // N+1 contains everything N did — so keeping the newest is right. Two
    // compaction passes that overlapped are not: each merged the heads *it*
    // listed, so a later one can be missing a writer an earlier one absorbed
    // and already deleted. Giving each its own group keeps them all.
    let group_of = |id: u64, seq: u64, has_hash: bool| {
        (id, has_hash, if id == COMPACTOR_WRITER_ID { seq } else { 0 })
    };

    // Highest sequence seen per group.
    let mut top: BTreeMap<(u64, bool, u64), u64> = BTreeMap::new();
    for path in paths {
        if let Some((id, seq, hash)) = parse_head_key(path) {
            let g = group_of(id, seq, hash.is_some());
            let e = top.entry(g).or_insert(seq);
            *e = (*e).max(seq);
        }
    }

    let mut out: Vec<(u64, u64, Option<String>, String)> = paths
        .iter()
        .filter_map(|path| {
            let (id, seq, hash) = parse_head_key(path)?;
            (top.get(&group_of(id, seq, hash.is_some())) == Some(&seq))
                .then(|| (id, seq, hash.map(|h| h.to_string()), path.clone()))
        })
        .collect();
    // Deterministic order, so two readers build the same view from the same
    // listing however the backend happened to return it.
    out.sort();
    out
}

const HEADS_PREFIX: &str = "heads/";

/// The writer id a compacted head is published under.
///
/// Reserved rather than allocated: `stable_writer_id` derives ids from a hash
/// of hostname and username, so the chance of a real writer landing here is the
/// chance of a 64-bit hash collision, and using a sentinel keeps compaction
/// from needing a registry.
pub const COMPACTOR_WRITER_ID: u64 = u64::MAX;

/// How many merges of one reduction level run at once.
///
/// Matches the S3 backend's batch width: the requests these merges issue are
/// grouped into batches of that size underneath, so going wider adds threads
/// without adding parallelism at the layer that actually waits.
const MERGE_FANOUT: usize = 32;

/// Everything about a collection that decides what bytes it produces.
///
/// Both fields participate in content addressing, and that is the whole reason
/// this type exists rather than the values being read from constants. The
/// chunk configuration decides where boundaries fall; the spill threshold
/// decides whether a value is written into a leaf or replaced by a pointer to
/// it. Either one differing between two writers means the same logical row
/// produces different index bytes, different leaf hashes, and different roots
/// — so the two stop converging, stop sharing structure, and stop merging
/// deterministically.
///
/// A collection therefore pins these at creation and keeps them for life. See
/// `pond_storage::definition`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EngineConfig {
    pub chunk: ChunkConfig,
    /// Values at or above this size are spilled to their own object.
    pub spill_threshold: usize,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            chunk: ChunkConfig::default(),
            spill_threshold: spill::SPILL_THRESHOLD,
        }
    }
}

impl EngineConfig {
    pub fn with_chunk(mut self, chunk: ChunkConfig) -> Self {
        self.chunk = chunk;
        self
    }

    pub fn with_spill_threshold(mut self, threshold: usize) -> Self {
        self.spill_threshold = threshold;
        self
    }
}

/// One writer's handle on a pond.
pub struct Engine<S: ObjectStore> {
    /// This writer's publish counter, so its heads sort by recency.
    seq: u64,
    /// Key of the head currently published by this writer, if any. Kept so a
    /// publish can retire the one it supersedes.
    published_at: Option<String>,
    store: EngineStore<BlobCache<S>>,
    writer_id: u64,
    head: Head,
    config: EngineConfig,
    /// Trees staged since the last publish, by collection.
    staged: BTreeMap<String, Tree>,
}

impl<S: ObjectStore> Engine<S> {
    /// Open a pond as `writer_id`, with a default cache.
    pub fn open(backend: S, writer_id: u64) -> Result<Self> {
        Self::open_with(backend, writer_id, CacheConfig::default(), EngineConfig::default())
    }

    pub fn open_with(
        backend: S,
        writer_id: u64,
        cache: CacheConfig,
        config: EngineConfig,
    ) -> Result<Self> {
        let cached = BlobCache::new(backend, cache)?;
        let store = EngineStore::new(cached);

        // Recover this writer's own head, if it has published before.
        //
        // One LIST of this writer's own prefix, then one read of the latest
        // key it names. That is one round trip more than the single GET a
        // fixed key allowed, and it is paid once per process rather than per
        // write — `publish` is still exactly one PUT. What it buys is a key
        // that names its own content hash, which is what lets compaction skip
        // and then delete absorbed heads without a conditional write.
        let own = store.inner().list_paths(&head_prefix(writer_id))?;
        let mut mine: Vec<(u64, String)> = latest_heads(&own)
            .into_iter()
            .filter(|(id, _, _, _)| *id == writer_id)
            .map(|(_, seq, _, path)| (seq, path))
            .collect();

        // Nothing under this writer's prefix may mean it has never published —
        // or that it last published before head keys had a sequence, when its
        // head was one flat object. Falling back keeps an upgraded pond's
        // writer cumulative: without it the writer starts empty, publishes a
        // head at a higher sequence, and its old head is superseded by one that
        // does not contain its rows.
        //
        // One extra GET, and only on a writer's first publish under the new
        // layout. A miss is cheap and a silent loss is not.
        if mine.is_empty() {
            let legacy = format!("heads/writer-{:016x}", writer_id);
            if store.inner().get_object(&legacy).is_some() {
                mine.push((0, legacy));
            }
        }

        let seq = mine.iter().map(|(s, _)| *s).max().unwrap_or(0);
        let published_at = mine.first().map(|(_, p)| p.clone());

        // Usually one head. More than one means two processes are publishing
        // under the same writer id — which the design says must not happen, and
        // which `stable_writer_id` makes easy to do by accident since it hashes
        // only hostname and username. Recovering just one of them would be the
        // worst response: this writer's next publish takes a higher sequence
        // and supersedes a head whose rows it never contained.
        //
        // So every head at the prefix is folded in, and the collision heals
        // itself on the next publish instead of quietly costing rows. It is
        // still a misconfiguration — see `stable_writer_id` — but it is a
        // recoverable one.
        let paths: Vec<String> = mine.iter().map(|(_, p)| p.clone()).collect();
        let bodies = store.inner().get_object_batch(&paths);
        let recovered: Vec<Head> = bodies
            .into_iter()
            .flatten()
            .filter_map(|b| pond_record::decode_head(&b))
            .collect();
        if !paths.is_empty() && recovered.is_empty() {
            return Err(EngineError::Corrupt("head".into()));
        }

        let head = match recovered.len() {
            0 => Head::new(writer_id),
            1 => recovered.into_iter().next().unwrap(),
            _ => {
                let mut merged = Head::new(writer_id);
                let mut by_collection: BTreeMap<String, Vec<String>> = BTreeMap::new();
                for h in &recovered {
                    for (c, root) in &h.collections {
                        by_collection.entry(c.clone()).or_default().push(root.clone());
                    }
                    for (w, hash) in &h.observed {
                        merged.observe(*w, hash);
                    }
                }
                for (collection, roots) in by_collection {
                    let mut iter = roots.into_iter().map(|root| Tree {
                        root,
                        config: config.chunk,
                    });
                    let mut acc = iter.next().expect("a collection has at least one root");
                    for other in iter {
                        acc = acc.merge(&store, &other, resolve_records);
                    }
                    merged.set_root(&collection, &acc.root);
                }
                merged
            }
        };

        Ok(Self {
            store,
            writer_id,
            head,
            seq,
            published_at,
            config,
            staged: BTreeMap::new(),
        })
    }

    pub fn writer_id(&self) -> u64 {
        self.writer_id
    }

    /// The backing store, for callers that need the raw object interface.
    pub fn store(&self) -> &EngineStore<BlobCache<S>> {
        &self.store
    }

    /// Load a collection's current tree — staged if it has uncommitted writes,
    /// otherwise this writer's last published root, otherwise empty.
    pub fn tree_for(&mut self, collection: &str) -> Tree {
        if let Some(t) = self.staged.get(collection) {
            return Tree {
                root: t.root.clone(),
                config: self.config.chunk,
            };
        }
        match self.head.root_of(collection) {
            Some(root) => Tree {
                root: root.to_string(),
                config: self.config.chunk,
            },
            None => Tree::build(&self.store, Vec::new(), self.config.chunk),
        }
    }

    /// Stage records into a collection. Nothing is visible to other readers
    /// until `publish`.
    ///
    /// Records already present are merged field-by-field, so a partial update
    /// cannot silently drop fields the caller did not mention — the law that
    /// makes lenses interchangeable.
    pub fn write_records(
        &mut self,
        collection: &str,
        records: Vec<(Key, Record)>,
    ) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let tree = self.tree_for(collection);

        let mut keys: Vec<Vec<u8>> = Vec::with_capacity(records.len());
        let mut values: Vec<Vec<u8>> = Vec::with_capacity(records.len());
        for (key, incoming) in records {
            let k = key.encode();
            // Merge with what is already there rather than replacing, so a
            // caller that knows about two fields cannot delete a third. The
            // existing value may be a spill pointer, so it is resolved first —
            // merging a pointer would produce nonsense.
            let existing = tree
                .get(&self.store, &k)
                .map(|bytes| spill::resolve(self.store.inner(), bytes))
                .transpose()?
                .and_then(|bytes| decode_record(&bytes));
            let merged = match existing {
                Some(existing) => merge_records(&existing, &incoming),
                None => incoming,
            };
            keys.push(k);
            values.push(encode_record(&merged));
        }

        // Spill the large ones before they reach a leaf. One batched write.
        let values = spill::store_batch(self.store.inner(), values, self.config.spill_threshold)?;
        let updates: Vec<(Vec<u8>, Vec<u8>)> = keys.into_iter().zip(values).collect();

        let new_tree = tree.insert_batch(&self.store, updates);
        self.staged.insert(collection.to_string(), new_tree);
        Ok(())
    }

    /// Append records whose keys are known to be greater than everything
    /// present — a WAL, a streaming topic, an event log.
    ///
    /// Skips the read-merge that `write_records` performs, because there is
    /// nothing to merge with. That makes an append the cheapest write the
    /// engine has: it touches the right-most leaf and its ancestor path only.
    pub fn append_records(
        &mut self,
        collection: &str,
        records: Vec<(Key, Record)>,
    ) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let tree = self.tree_for(collection);
        let (keys, values): (Vec<Vec<u8>>, Vec<Vec<u8>>) = records
            .into_iter()
            .map(|(k, r)| (k.encode(), encode_record(&r)))
            .unzip();
        let values = spill::store_batch(self.store.inner(), values, self.config.spill_threshold)?;
        let updates: Vec<(Vec<u8>, Vec<u8>)> = keys.into_iter().zip(values).collect();
        let new_tree = tree.insert_batch(&self.store, updates);
        self.staged.insert(collection.to_string(), new_tree);
        Ok(())
    }

    /// Read one record from this writer's view (staged writes included).
    ///
    /// A record hidden by a tombstone reads as absent, the same as one that
    /// was never written. The bytes stay in the tree — see `decode_pairs` for
    /// why a delete cannot erase them and still merge.
    pub fn get(&mut self, collection: &str, key: &Key) -> Result<Option<Record>> {
        let tree = self.tree_for(collection);
        match tree.get(&self.store, &key.encode()) {
            Some(bytes) => {
                let bytes = spill::resolve(self.store.inner(), bytes)?;
                let rec = decode_record(&bytes)
                    .ok_or_else(|| EngineError::Corrupt(format!("record in {}", collection)))?;
                Ok(rec.is_visible().then_some(rec))
            }
            None => Ok(None),
        }
    }

    /// Scan a half-open key range `[start, end)`.
    ///
    /// Because keys are order-preserving, a range is contiguous in the index
    /// and subtrees outside it are skipped without being read.
    pub fn scan_range(
        &mut self,
        collection: &str,
        start: &Key,
        end: &Key,
    ) -> Result<Vec<(Key, Record)>> {
        let tree = self.tree_for(collection);
        let raw = tree.scan_range(&self.store, &start.encode(), &end.encode());
        decode_pairs(self.store.inner(), raw, collection)
    }

    /// Every record in a collection, in key order.
    pub fn scan(&mut self, collection: &str) -> Result<Vec<(Key, Record)>> {
        let tree = self.tree_for(collection);
        decode_pairs(self.store.inner(), tree.scan(&self.store), collection)
    }

    /// Publish every staged collection **atomically**.
    ///
    /// One PUT of one head object. Object stores give single-object atomicity,
    /// so a reader sees either all of these collections at their new roots or
    /// all of them at their old ones — never a mixture — with no transaction
    /// subsystem involved.
    pub fn publish(&mut self) -> Result<()> {
        if self.staged.is_empty() {
            return Ok(());
        }
        for (collection, tree) in &self.staged {
            self.head.set_root(collection, &tree.root);
        }
        let bytes = pond_record::encode_head(&self.head);
        self.seq += 1;
        let key = head_key(self.writer_id, self.seq, &pond_kernel::hash_bytes(&bytes));

        // One write, and it is the one readers see. Storing the head as a blob
        // and then binding a name to its hash would be two sequential round
        // trips on the commit path, and the second is the one that publishes,
        // so the first buys nothing.
        self.store.inner().put_object(&key, &bytes)?;

        // The previous head of this writer is now superseded, and is
        // deliberately *not* deleted here. Retirement is a maintenance
        // concern, the same as garbage collection: a superseded key is never
        // read — `latest_heads` takes the highest sequence per writer — so it
        // costs a listing entry and nothing else, and compaction sweeps it.
        //
        // Paying a round trip for it on the commit path would make every write
        // slower to save a reader nothing, which is the wrong trade for a
        // system whose commit is otherwise exactly one PUT.
        self.published_at = Some(key);

        self.staged.clear();
        Ok(())
    }

    /// Discard staged writes. The blobs they wrote become unreachable and are
    /// reclaimed by GC — there is nothing to roll back, because nothing was
    /// published.
    pub fn abort(&mut self) {
        self.staged.clear();
    }

    /// Collections this writer has published.
    pub fn collections(&self) -> Vec<String> {
        self.head.collection_names().cloned().collect()
    }

    /// Branch a collection: copy one pointer.
    ///
    /// O(1) regardless of size, because the tree is immutable and
    /// content-addressed — the branch shares every node until it diverges.
    pub fn branch(&mut self, from: &str, to: &str) -> Result<()> {
        let root = self
            .head
            .root_of(from)
            .ok_or_else(|| EngineError::NotFound(from.to_string()))?
            .to_string();
        self.branch_from_root(to, root)
    }

    /// Branch a collection from an explicit root.
    ///
    /// The plain `branch` copies *this writer's* root, which is only the whole
    /// collection when this writer is the only one. Branching what a reader
    /// actually sees means branching the merged root — see
    /// [`Reader::root_of`] — so the caller supplies it rather than the engine
    /// silently branching a partial view.
    pub fn branch_from_root(&mut self, to: &str, root: String) -> Result<()> {
        self.head.set_root(to, &root);
        self.staged.insert(
            to.to_string(),
            Tree {
                root,
                config: self.config.chunk,
            },
        );
        Ok(())
    }
}

/// A read-only view merging every writer's published state.
///
/// This is how a reader sees a pond that many writers are writing to, without
/// any of them coordinating: list the heads, merge the trees. Merge is a
/// semilattice join, so the result does not depend on the order the heads were
/// discovered in.
pub struct Reader<S: ObjectStore> {
    store: EngineStore<BlobCache<S>>,
    roots: BTreeMap<String, Vec<String>>,
    config: EngineConfig,
    merged: BTreeMap<String, Tree>,
    /// Every head this reader read, as (content hash, head) — including the
    /// ones it skipped as already absorbed. Compaction needs the skipped ones
    /// too: it republishes over the previous compacted head, so it has to
    /// re-claim everything that head claimed or the coverage would shrink.
    heads: Vec<(String, Head)>,
    /// Keys the above were read from, in the same order.
    head_paths: Vec<String>,
    /// Every key the listing returned, whether it was read or skipped.
    all_head_paths: Vec<String>,
}

impl<S: ObjectStore> Reader<S> {
    pub fn open(backend: S) -> Result<Self> {
        Self::open_with(backend, CacheConfig::default(), EngineConfig::default())
    }

    pub fn open_with(backend: S, cache: CacheConfig, config: EngineConfig) -> Result<Self> {
        let cached = BlobCache::new(backend, cache)?;
        let store = EngineStore::new(cached);

        // One LIST to discover writers. This is the only place the engine
        // lists anything, and its cost is O(writers), never O(data).
        //
        // The error is propagated rather than defaulted away: a failed listing
        // and an empty store are indistinguishable in the result, and treating
        // the first as the second means a transient backend fault presents as
        // "your data is gone" — then a subsequent publish writes on top of a
        // history the reader never saw.
        let paths = store.inner().list_paths(HEADS_PREFIX)?;
        let latest = latest_heads(&paths);

        // The compacted head is read first and alone, because what it says
        // decides how much of the rest has to be read at all. It is one object
        // however many writers there are.
        //
        // Its `observed` map names the heads it folded in, by the content hash
        // of their bytes — the same hash their keys carry. So absorption is
        // decidable from the listing, and an absorbed head is never fetched.
        // That is what bounds the read path: after a compaction pass a reader
        // opens in a constant number of requests no matter how many writers
        // have ever published.
        //
        // Matching on content hash rather than on writer id is also what makes
        // it safe without a compare-and-swap. A writer that published again —
        // during the compaction or after it — wrote different bytes, so a
        // different hash, so a different key, which is not in `observed` and is
        // read normally. A concurrent publish cannot be skipped.
        //
        // If a skip were wrong in the other direction the cost is a slower
        // read, not a wrong one: merge is idempotent, so folding a writer's
        // root in twice yields the same tree.
        let mut absorbed: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut heads: Vec<(String, Head)> = Vec::new();
        let mut head_paths: Vec<String> = Vec::new();

        // Every compacted head, not just the newest — see `latest_heads` for
        // why there can legitimately be more than one, and why taking only the
        // newest loses data. In steady state this is one object.
        let compacted_keys: Vec<&(u64, u64, Option<String>, String)> = latest
            .iter()
            .filter(|(id, _, _, _)| *id == COMPACTOR_WRITER_ID)
            .collect();
        let compacted_paths: Vec<String> = compacted_keys
            .iter()
            .map(|(_, _, _, path)| path.clone())
            .collect();
        for (bytes, (_, _, hash, path)) in store
            .inner()
            .get_object_batch(&compacted_paths)
            .into_iter()
            .zip(&compacted_keys)
        {
            let Some(bytes) = bytes else { continue };
            let Some(head) = pond_record::decode_head(&bytes) else {
                continue;
            };
            absorbed.extend(head.observed.values().cloned());
            let own = hash
                .clone()
                .unwrap_or_else(|| pond_kernel::hash_bytes(&bytes));
            heads.push((own, head));
            head_paths.push(path.clone());
        }

        // A head whose key carries no content hash was written before keys
        // carried one. It can never be recognised as absorbed, so it is always
        // read — a cost in requests, never in rows.
        let unabsorbed: Vec<&(u64, u64, Option<String>, String)> = latest
            .iter()
            .filter(|(id, _, _, _)| *id != COMPACTOR_WRITER_ID)
            .filter(|(_, _, hash, _)| match hash {
                Some(h) => !absorbed.contains(h),
                None => true,
            })
            .collect();

        // Then one batched read of what is left. Those heads are independent
        // of each other, so nothing forces a round trip per writer in
        // sequence — S3 issues the batch in parallel.
        let to_read: Vec<String> = unabsorbed
            .iter()
            .map(|(_, _, _, path)| path.clone())
            .collect();
        let bodies = store.inner().get_object_batch(&to_read);

        for (bytes, (_, _, key_hash, path)) in bodies.into_iter().zip(&unabsorbed) {
            // A head that does not decode is skipped, not fatal: one writer
            // publishing a corrupt head must not make every other writer's
            // data unreadable.
            let Some(bytes) = bytes else { continue };
            let Some(head) = pond_record::decode_head(&bytes) else {
                continue;
            };
            let hash = key_hash
                .clone()
                .unwrap_or_else(|| pond_kernel::hash_bytes(&bytes));
            heads.push((hash, head));
            head_paths.push(path.clone());
        }

        let mut roots: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for (_, head) in &heads {
            for (collection, root) in &head.collections {
                roots
                    .entry(collection.clone())
                    .or_default()
                    .push(root.clone());
            }
        }


        Ok(Self {
            store,
            roots,
            config,
            merged: BTreeMap::new(),
            heads,
            head_paths,
            all_head_paths: paths,
        })
    }

    /// Collections visible across all writers.
    pub fn collections(&self) -> Vec<String> {
        self.roots.keys().cloned().collect()
    }

    /// Fold every writer's root into one tree, halving the set at each step
    /// rather than sweeping it left to right.
    ///
    /// # What this buys, measured
    ///
    /// A left fold `((a⊕b)⊕c)⊕d` and a balanced reduction `(a⊕b)⊕(c⊕d)` do the
    /// same total work — request counts are identical. What differs is how much
    /// of it has to *wait*: every step of a left fold needs the result of the
    /// one before it, while the merges within a reduction level are independent
    /// and can run at once.
    ///
    /// `cargo run --release -p pond_bench --bin writerscale`, with every
    /// backend request delayed to model an object-storage round trip:
    ///
    /// | writers | scan requests | wall clock | fully sequential | speedup |
    /// |---|---|---|---|---|
    /// | 16  | 31  | 11 ms | 16 ms  | 1.5x |
    /// | 32  | 63  | 16 ms | 32 ms  | 2.0x |
    /// | 64  | 127 | 23 ms | 64 ms  | 2.8x |
    /// | 128 | 255 | 44 ms | 128 ms | 2.9x |
    /// | 256 | 511 | 86 ms | 256 ms | 3.0x |
    ///
    /// # Why it is a constant factor and not `log W`
    ///
    /// The obvious argument — `log2(W)` levels, so `log2(W)` waits — is wrong,
    /// and the measurement is what says so. Level `k` holds `W/2^k` merges, but
    /// each one joins trees that have grown to about `2^k` entries, and a merge
    /// descends both trees sequentially. So the levels get cheaper in count and
    /// more expensive in depth at the same rate, and the serial depth stays
    /// `O(W)`. What actually improves is the wide, cheap bottom of the
    /// reduction, and that plateaus around 3x.
    ///
    /// Raising [`MERGE_FANOUT`] past 32 makes it *worse* — 2.5x at 256 writers
    /// with a fanout of 256 — because thread scheduling costs more than the
    /// extra overlap returns.
    ///
    /// # What this does not fix
    ///
    /// Reader cost is still linear in the number of writers that have *ever*
    /// published, because heads are never removed and never folded. 3x off a
    /// line is still a line. Bounding it needs head compaction — periodically
    /// merging many heads into one published root — which does not exist yet
    /// and is the real ceiling on "any user worldwide". See `docs/REVIEW_BY_ROLE.md`.
    ///
    /// # Why re-associating is allowed
    ///
    /// Only because merge is a semilattice join: associative, so the fold can
    /// be re-bracketed, and commutative, so the order does not matter.
    /// `merge_is_idempotent_and_associative` in `pond_index`'s acceptance tests
    /// asserts both on the root hash rather than on logical contents, so this
    /// produces a byte-identical root to the left fold it replaces — and
    /// `readers_converge_on_the_same_root_hash` would fail if it did not.
    ///
    /// The algebra was stated as the reason merge is *correct*. It is equally
    /// what makes it re-orderable, and that half went unused.
    fn reduce(&self, roots: Vec<String>) -> Tree {
        let cfg = self.config.chunk;
        let mut level: Vec<Tree> = roots
            .into_iter()
            .map(|root| Tree { root, config: cfg })
            .collect();

        if level.is_empty() {
            return Tree::build(&self.store, Vec::new(), cfg);
        }

        while level.len() > 1 {
            let mut next = Vec::with_capacity(level.len().div_ceil(2));
            // Bounded fan-out: a level can be thousands of pairs wide, and
            // spawning a thread for each would cost more in scheduling than
            // the round trips it saves. The width matches the backend's own
            // batch width, since that is what the requests underneath will be
            // grouped into anyway.
            for group in level.chunks(MERGE_FANOUT * 2) {
                std::thread::scope(|scope| {
                    let store = &self.store;
                    let handles: Vec<_> = group
                        .chunks(2)
                        .map(|pair| {
                            scope.spawn(move || match pair {
                                [a, b] => a.merge(store, b, resolve_records),
                                // An odd one out carries to the next level
                                // untouched. Merging it with an empty tree
                                // would be equivalent but would cost a wave.
                                [a] => Tree {
                                    root: a.root.clone(),
                                    config: cfg,
                                },
                                _ => unreachable!("chunks(2) yields 1 or 2"),
                            })
                        })
                        .collect();
                    for h in handles {
                        next.push(h.join().expect("a merge thread panicked"));
                    }
                });
            }
            level = next;
        }

        level.pop().expect("a non-empty level always reduces to one tree")
    }

    /// How many heads this reader read.
    pub fn head_count(&self) -> usize {
        self.heads.len()
    }

    /// Every head key this reader's view was built from — the compacted head
    /// it read plus every unabsorbed head it fetched.
    pub fn head_paths(&self) -> Vec<String> {
        self.head_paths.clone()
    }

    /// Every (writer, sequence, collection, root) the store's heads name,
    /// **including the superseded ones this reader did not fetch**.
    ///
    /// A publish deliberately leaves its predecessor in place — retiring it
    /// would cost a round trip on the commit path to save readers nothing,
    /// since a superseded key is listed and never fetched. So between
    /// compactions the superseded heads are the record of every root the
    /// collection has had.
    ///
    /// Reading them costs one batched request and is done in exactly one
    /// place: the compaction pass that is about to delete them. Paying to read
    /// what is being destroyed is the honest price of recording it, and it
    /// stays off the read path entirely — an ordinary reader still fetches
    /// only live heads.
    pub fn all_published_roots(&self) -> Vec<(u64, u64, String, String)> {
        let keys: Vec<(u64, u64, String)> = self
            .all_head_paths
            .iter()
            .filter_map(|p| {
                let (id, seq, _) = parse_head_key(p)?;
                // A compacted head is a merge of others, not a state any
                // writer ever published, so it is not history.
                (id != COMPACTOR_WRITER_ID).then(|| (id, seq, p.clone()))
            })
            .collect();

        let paths: Vec<String> = keys.iter().map(|(_, _, p)| p.clone()).collect();
        let bodies = self.store.inner().get_object_batch(&paths);

        let mut out = Vec::new();
        for (bytes, (id, seq, _)) in bodies.into_iter().zip(&keys) {
            let Some(bytes) = bytes else { continue };
            let Some(head) = pond_record::decode_head(&bytes) else {
                continue;
            };
            for (collection, root) in &head.collections {
                out.push((*id, *seq, collection.clone(), root.clone()));
            }
        }
        out
    }

    /// Every head key the reader's listing returned, fetched or not.
    ///
    /// This is what compaction retires. It is a superset of [`head_paths`]:
    /// it also covers heads that were skipped as already absorbed, and
    /// superseded sequences of writers whose latest head was read.
    ///
    /// Crucially it is a snapshot of one listing. A head published after that
    /// listing is not in it and therefore cannot be deleted by the pass that
    /// took it — which is what makes retirement safe without a conditional
    /// delete.
    ///
    /// [`head_paths`]: Self::head_paths
    pub fn all_head_paths(&self) -> Vec<String> {
        self.all_head_paths.clone()
    }

    /// Every head identity this reader's view covers, as (writer, content
    /// hash of that head's bytes).
    ///
    /// Includes the transitive closure — the heads read directly *and* every
    /// head those heads already claimed to have absorbed. Compaction
    /// republishes over the single compacted head key, so anything the
    /// previous compacted head claimed has to be re-claimed here or those
    /// writers would become live again on the next read.
    ///
    /// The compactor's own head is excluded: it is the thing being replaced,
    /// and a head claiming to have absorbed itself is meaningless.
    pub fn head_identities(&self) -> Vec<(u64, String)> {
        let mut out: BTreeMap<u64, String> = BTreeMap::new();
        for (hash, head) in &self.heads {
            for (writer, absorbed) in &head.observed {
                out.insert(*writer, absorbed.clone());
            }
            if head.writer_id != COMPACTOR_WRITER_ID {
                out.insert(head.writer_id, hash.clone());
            }
        }
        out.into_iter().collect()
    }

    /// Merge every writer's tree for a collection, memoised per reader.
    pub fn tree_for(&mut self, collection: &str) -> Tree {
        if let Some(t) = self.merged.get(collection) {
            return Tree {
                root: t.root.clone(),
                config: self.config.chunk,
            };
        }
        let roots = self.roots.get(collection).cloned().unwrap_or_default();
        let acc = self.reduce(roots);
        self.merged.insert(
            collection.to_string(),
            Tree {
                root: acc.root.clone(),
                config: self.config.chunk,
            },
        );
        acc
    }

    /// Read one record from the merged view of every writer.
    ///
    /// A record hidden by a tombstone reads as absent — including a tombstone
    /// written by a different writer than the one that created the record,
    /// which is the case a delete has to survive.
    pub fn get(&mut self, collection: &str, key: &Key) -> Result<Option<Record>> {
        let tree = self.tree_for(collection);
        match tree.get(&self.store, &key.encode()) {
            Some(bytes) => {
                let bytes = spill::resolve(self.store.inner(), bytes)?;
                let rec = decode_record(&bytes)
                    .ok_or_else(|| EngineError::Corrupt(format!("record in {}", collection)))?;
                Ok(rec.is_visible().then_some(rec))
            }
            None => Ok(None),
        }
    }

    pub fn scan(&mut self, collection: &str) -> Result<Vec<(Key, Record)>> {
        let tree = self.tree_for(collection);
        decode_pairs(self.store.inner(), tree.scan(&self.store), collection)
    }

    pub fn scan_range(
        &mut self,
        collection: &str,
        start: &Key,
        end: &Key,
    ) -> Result<Vec<(Key, Record)>> {
        let tree = self.tree_for(collection);
        let raw = tree.scan_range(&self.store, &start.encode(), &end.encode());
        decode_pairs(self.store.inner(), raw, collection)
    }

    /// Every record at a specific root, rather than at the merged current
    /// state.
    ///
    /// This is what makes a retained history worth retaining: without it the
    /// log is a list of hashes nobody can do anything with. A root names a
    /// complete immutable tree, so reading one back is an ordinary scan that
    /// starts somewhere else — no snapshot machinery, no undo log, no special
    /// case in the write path. That is a property of content addressing rather
    /// than a feature bolted onto it.
    ///
    /// The root must still exist. `pond gc` keeps the ones a history names;
    /// one evicted by the history cap is gone, and this returns whatever the
    /// store says rather than pretending otherwise.
    pub fn scan_at(&self, root: &str, collection: &str) -> Result<Vec<(Key, Record)>> {
        // A root that is not there must be an error, not an empty scan. The
        // tree walk returns nothing for a missing node, which reads as "the
        // collection was empty at that point" — a wrong answer that looks like
        // a right one, and the failure mode this codebase has been bitten by
        // more than once.
        if !root.is_empty() && !self.store.inner().blob_exists(root) {
            return Err(EngineError::Corrupt(format!(
                "no such root '{}' — it was never published, or it has been \
                 reclaimed since",
                root
            )));
        }
        let tree = Tree {
            root: root.to_string(),
            config: self.config.chunk,
        };
        decode_pairs(self.store.inner(), tree.scan(&self.store), collection)
    }

    /// The root produced by merging `source`'s tree into `target`'s.
    ///
    /// Neither collection is modified — this computes the result and leaves
    /// publishing it to the caller, so a merge is staged and published like any
    /// other write and is atomic for the same reason.
    pub fn merge_roots(&mut self, target: &str, source: &str) -> String {
        let a = self.tree_for(target);
        let b = self.tree_for(source);
        a.merge(&self.store, &b, resolve_records).root
    }

    /// Root hash of the merged view — the identity of what this reader sees.
    /// Two readers that have seen the same heads produce the same hash.
    pub fn root_of(&mut self, collection: &str) -> String {
        self.tree_for(collection).root
    }
}

/// Conflict resolution between two writers' versions of one record.
///
/// Per-field last-writer-wins over a total order `(physical, logical, writer)`.
/// What one compaction pass did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactionReport {
    /// Heads read.
    pub heads_seen: usize,
    /// Heads folded into the compacted head, and therefore skippable by
    /// readers from now on.
    pub heads_absorbed: usize,
    /// Collections in the compacted head.
    pub collections: usize,
    /// Head objects retired, now that a compacted head holds their data.
    pub heads_deleted: usize,
}

/// Fold every writer's head into one, so readers stop paying for writers that
/// have gone away.
///
/// # The problem this exists for
///
/// Heads are what makes coordination-free multi-writer possible: each writer
/// owns one key, nobody contends, no compare-and-swap is needed. The price is
/// that a head, once written, is read by every reader forever — a writer that
/// published once and vanished still costs a read and a merge on every open.
/// Measured with `pond_bench --bin writerscale`, the first read of a collection
/// with 256 writers merged 511 roots; that number tracks the writers that have
/// *ever* published, not the data and not the active writers.
///
/// # How this bounds it without a compare-and-swap
///
/// A compaction pass reads every head, merges their roots per collection, and
/// publishes the result as a single head under [`COMPACTOR_WRITER_ID`] whose
/// `observed` map names each head it absorbed by **the content hash of that
/// head's bytes**. Readers drop any head sitting at an absorbed hash.
///
/// Naming heads by content hash rather than by writer id is the whole safety
/// argument, and it is what the hash in the key buys. The dangerous case is a
/// writer publishing while compaction runs: its new head has different bytes,
/// so a different hash, so a different key — one this pass never listed. It is
/// therefore neither skipped by readers nor deleted by the pass. There is no
/// window in which a publish can be swallowed, and so no need for a conditional
/// write or a conditional delete, neither of which a local filesystem offers.
///
/// The pass then retires every key its own listing returned: the absorbed
/// heads, and the superseded sequences of writers whose latest head it read.
/// That is bounded by a snapshot, which is what makes deleting safe at all.
///
/// If a head is wrongly *not* skipped, the merge just includes it twice, and
/// merge is idempotent. The optimisation can cost time; it cannot cost
/// correctness.
///
/// # Who runs it
///
/// Anyone, any number of times, concurrently. Running it is a maintenance
/// choice, not a protocol obligation — a pond that never compacts is slower,
/// never wrong.
///
/// Concurrency is safe for a narrower reason than it first appears, and the
/// original version of this comment got it wrong. Two overlapping passes each
/// merge the heads *they* listed, so a pass that publishes later can be
/// missing a writer that an earlier pass absorbed and has already deleted. If
/// readers took only the newest compacted head, that writer's rows would be
/// gone — which they were, until
/// `two_overlapping_compaction_passes_lose_nothing` was written to check the
/// claim instead of repeating it.
///
/// What makes it safe is that readers merge *every* compacted head, because
/// compacted heads are not versions of one another. They do not accumulate: a
/// pass retires every compacted head it read, so overlapping passes leave two
/// and the next ordinary pass folds them back into one.
pub fn compact_heads<S: ObjectStore>(
    backend: S,
    cache: CacheConfig,
    config: EngineConfig,
) -> Result<CompactionReport> {
    let mut reader = Reader::open_with(backend, cache, config)?;

    // Taken from the reader's own view, so exactly what it merged is what gets
    // recorded — re-listing here would open a window between the two.
    //
    // Nothing to fold means nothing to write. Publishing an empty compacted
    // head anyway would leave a head behind that every later pass counts as
    // one it has "seen" — which is how `pond compact` on an empty pond came to
    // report "0 heads absorbed of 1 seen".
    let absorbed = reader.head_identities();
    if absorbed.is_empty() {
        return Ok(CompactionReport {
            heads_seen: 0,
            heads_absorbed: 0,
            collections: 0,
            heads_deleted: 0,
        });
    }

    let collections = reader.collections();

    let mut head = Head::new(COMPACTOR_WRITER_ID);
    for collection in &collections {
        let tree = reader.tree_for(collection);
        head.set_root(collection, &tree.root);
    }
    for (writer, hash) in &absorbed {
        head.observe(*writer, hash);
    }

    let bytes = pond_record::encode_head(&head);
    let seq = pond_kernel::crdt::current_time_ms();
    let key = head_key(
        COMPACTOR_WRITER_ID,
        seq,
        &pond_kernel::hash_bytes(&bytes),
    );
    reader.store.inner().put_object(&key, &bytes)?;

    // Retire what has been folded in — but only the exact keys this pass saw,
    // and only after the compacted head that contains their data is durable.
    //
    // Deleting by key is what the content hash in the key buys. A writer that
    // published while this ran wrote a *different* key, which is not in this
    // list and is therefore untouched: there is no window in which a live head
    // can be removed, and so no need for a conditional delete — which object
    // stores do not offer and a local filesystem does not either.
    //
    // Record what is about to be destroyed. A superseded head is the only
    // evidence a collection was ever at that root, and this pass is what
    // removes it — so the history is written before the deletes, and a failure
    // to write it stops the deletes rather than losing the record silently.
    let mut by_collection: BTreeMap<String, Vec<history::Entry>> = BTreeMap::new();
    for (writer_id, seq, collection, root) in reader.all_published_roots() {
        by_collection.entry(collection).or_default().push(history::Entry {
            seq,
            writer_id,
            root,
        });
    }
    for (collection, entries) in by_collection {
        let mut log = history::load(reader.store.inner(), &collection);
        log.absorb(entries);
        history::store_history(reader.store.inner(), &collection, &log)?;
    }

    // Every key in the listing this pass took, not only the ones it read: the
    // rest are superseded heads of writers whose latest *was* read, and a
    // head is cumulative, so a later sequence contains everything an earlier
    // one did.
    //
    // Best-effort by design. A key that survives costs a reader one listing
    // entry and the next pass sweeps it; failing the compaction because a
    // delete did not land would be worse than leaving garbage.
    let mut retired: Vec<String> = reader
        .all_head_paths()
        .into_iter()
        .filter(|p| p != &key)
        // Only keys this code recognises as heads. Deleting everything under
        // `heads/` would make compaction destroy anything else that ever came
        // to live there — a future marker, a stray file, a key written by a
        // newer version of this code that this one cannot parse. A sweep
        // should remove what it understands and leave what it does not.
        .filter(|p| parse_head_key(p).is_some())
        .collect();
    retired.sort();
    let deleted = reader
        .store
        .inner()
        .delete_path_batch(&retired)
        .unwrap_or(0);

    Ok(CompactionReport {
        heads_seen: reader.head_count(),
        heads_absorbed: absorbed.len(),
        collections: collections.len(),
        heads_deleted: deleted,
    })
}

/// Commutative, so `merge(a,b) == merge(b,a)` and readers converge regardless
/// of the order heads were discovered in.
fn resolve_records(a: &[u8], b: &[u8]) -> Vec<u8> {
    match (decode_record(a), decode_record(b)) {
        (Some(ra), Some(rb)) => encode_record(&merge_records(&ra, &rb)),
        // If one side is undecodable, prefer the side that is: dropping a
        // readable record because its counterpart is corrupt would turn one
        // bad object into data loss.
        (Some(_), None) => a.to_vec(),
        (None, Some(_)) => b.to_vec(),
        (None, None) => a.to_vec(),
    }
}

/// Decode a scan's raw pairs, dropping records a tombstone hides.
///
/// Deletion is a write here, not an erasure: the record stays in the tree
/// carrying a tombstone version, because a delete that removed the bytes could
/// not merge — a writer that never saw the delete would simply re-add the row,
/// and there would be nothing to compare against. What makes it a delete is
/// that readers do not return it.
///
/// `is_visible` is deliberately not "has a tombstone": a field written *after*
/// the delete resurrects the record, which is the correct answer when a delete
/// and a later update arrive out of order.
fn decode_pairs<S: ObjectStore>(
    backend: &S,
    raw: Vec<(Vec<u8>, Vec<u8>)>,
    collection: &str,
) -> Result<Vec<(Key, Record)>> {
    let (keys, values): (Vec<Vec<u8>>, Vec<Vec<u8>>) = raw.into_iter().unzip();

    // Resolve every spilled value in one batch rather than one round trip
    // each. A scan that touched a thousand large rows would otherwise pay a
    // thousand sequential GETs, which is the cost this whole design exists to
    // avoid.
    let values = spill::resolve_batch(backend, values)?;

    let mut out = Vec::with_capacity(keys.len());
    for (k, v) in keys.into_iter().zip(values) {
        let key = Key::decode(&k)
            .ok_or_else(|| EngineError::Corrupt(format!("key in {}", collection)))?;
        let rec = decode_record(&v)
            .ok_or_else(|| EngineError::Corrupt(format!("record in {}", collection)))?;
        if !rec.is_visible() {
            continue;
        }
        out.push((key, rec));
    }
    Ok(out)
}

#[cfg(test)]
mod head_key_tests {
    use super::*;

    #[test]
    fn a_key_round_trips_through_its_parser() {
        let h = "a".repeat(64);
        let k = head_key(0x1234, 7, &h);
        assert_eq!(parse_head_key(&k), Some((0x1234, 7, Some(h.as_str()))));
    }

    /// A writer's prefix must be a path segment boundary, so that listing it
    /// means the same thing on a backend that does prefix matching (S3) and on
    /// one that lists a directory (local).
    #[test]
    fn a_writers_prefix_is_a_directory() {
        let p = head_prefix(9);
        assert!(p.ends_with('/'), "{} must be a directory prefix", p);
        assert!(head_key(9, 1, "abc").starts_with(&p));
        // And it must not also be a prefix of a *different* writer's keys,
        // which is what a bare hex id without a separator would allow.
        assert!(!head_key(0x99, 1, "abc").starts_with(&head_prefix(9)));
    }

    /// Ponds written before keys carried a sequence still read.
    #[test]
    fn the_original_flat_key_still_parses() {
        assert_eq!(
            parse_head_key("heads/writer-000000000000002a"),
            Some((42, 0, None)),
            "a head written before keys carried a sequence must still be found"
        );
    }

    #[test]
    fn nonsense_keys_are_rejected_rather_than_guessed_at() {
        for bad in [
            "heads/",
            "heads/writer-",
            "heads/writer/zzz/0000000000000001.abc",
            "heads/writer/0000000000000001/0000000000000001.",
            "heads/writer/0000000000000001/nothex.abc",
            "heads/writer/0000000000000001",
            "collections/users/main",
        ] {
            assert_eq!(parse_head_key(bad), None, "{} should not parse", bad);
        }
    }

    #[test]
    fn the_latest_head_of_each_writer_wins() {
        let keys = vec![
            head_key(1, 1, "aaa"),
            head_key(1, 9, "bbb"),
            head_key(1, 3, "ccc"),
            head_key(2, 4, "ddd"),
        ];
        let latest = latest_heads(&keys);
        assert_eq!(latest.len(), 2);
        assert_eq!(latest[0].0, 1);
        assert_eq!(latest[0].1, 9, "highest sequence wins for writer 1");
        assert_eq!(latest[1].1, 4);
    }

    /// Sequences are hex-padded so that string order is numeric order — a
    /// listing arrives sorted as text, and 10 must not sort before 9.
    #[test]
    fn sequence_order_survives_string_sorting() {
        let mut keys: Vec<String> = (1..=20).map(|i| head_key(1, i, "x")).collect();
        keys.sort();
        let latest = latest_heads(&keys);
        assert_eq!(latest[0].1, 20);
    }

    /// A pre-sequence head is never superseded by a current-layout one.
    ///
    /// It is not an older version of the same thing: the writer that wrote it
    /// may never run again, and no other object holds its rows. Dropping it
    /// because a newer key exists for the same writer id would lose them.
    #[test]
    fn a_legacy_head_is_not_superseded_by_a_new_one() {
        let keys = vec![
            "heads/writer-0000000000000001".to_string(),
            head_key(1, 1, "new"),
        ];
        let latest = latest_heads(&keys);
        assert_eq!(
            latest.len(),
            2,
            "both must be read: {:?}",
            latest
        );
        assert!(latest.iter().any(|(_, _, h, _)| h.is_none()));
        assert!(latest.iter().any(|(_, _, h, _)| h.as_deref() == Some("new")));
    }
}
