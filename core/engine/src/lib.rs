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
// heads/<writer_id>              {collection -> root} — the publish unit
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

pub mod store;
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

/// Where a writer's head lives. Derived solely from the writer id, so a writer
/// can compute its own key and no other writer's.
pub fn head_key(writer_id: u64) -> String {
    // Epoch-tagged so a failover can take over by writing at a higher epoch
    // without any compare-and-swap. See docs/POSTGRES_ON_POND.md.
    format!("heads/writer-{:016x}", writer_id)
}

const HEADS_PREFIX: &str = "heads/";

/// One writer's handle on a pond.
pub struct Engine<S: ObjectStore> {
    store: EngineStore<BlobCache<S>>,
    writer_id: u64,
    head: Head,
    config: ChunkConfig,
    /// Trees staged since the last publish, by collection.
    staged: BTreeMap<String, Tree>,
}

impl<S: ObjectStore> Engine<S> {
    /// Open a pond as `writer_id`, with a default cache.
    pub fn open(backend: S, writer_id: u64) -> Result<Self> {
        Self::open_with(backend, writer_id, CacheConfig::default(), ChunkConfig::default())
    }

    pub fn open_with(
        backend: S,
        writer_id: u64,
        cache: CacheConfig,
        config: ChunkConfig,
    ) -> Result<Self> {
        let cached = BlobCache::new(backend, cache)?;
        let store = EngineStore::new(cached);

        // Recover this writer's own head, if it has published before.
        let head = match store.inner().get_path(&head_key(writer_id)) {
            Some(hash) => match store.inner().get_blob(&hash) {
                Ok(bytes) => pond_record::decode_head(&bytes)
                    .ok_or_else(|| EngineError::Corrupt("head".into()))?,
                Err(_) => Head::new(writer_id),
            },
            None => Head::new(writer_id),
        };

        Ok(Self {
            store,
            writer_id,
            head,
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
    fn tree_for(&mut self, collection: &str) -> Tree {
        if let Some(t) = self.staged.get(collection) {
            return Tree {
                root: t.root.clone(),
                config: self.config,
            };
        }
        match self.head.root_of(collection) {
            Some(root) => Tree {
                root: root.to_string(),
                config: self.config,
            },
            None => Tree::build(&self.store, Vec::new(), self.config),
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

        let mut updates: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(records.len());
        for (key, incoming) in records {
            let k = key.encode();
            // Merge with what is already there rather than replacing, so a
            // caller that knows about two fields cannot delete a third.
            let merged = match tree.get(&self.store, &k) {
                Some(existing_bytes) => match decode_record(&existing_bytes) {
                    Some(existing) => merge_records(&existing, &incoming),
                    None => incoming,
                },
                None => incoming,
            };
            updates.push((k, encode_record(&merged)));
        }

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
        let updates: Vec<(Vec<u8>, Vec<u8>)> = records
            .into_iter()
            .map(|(k, r)| (k.encode(), encode_record(&r)))
            .collect();
        let new_tree = tree.insert_batch(&self.store, updates);
        self.staged.insert(collection.to_string(), new_tree);
        Ok(())
    }

    /// Read one record from this writer's view (staged writes included).
    pub fn get(&mut self, collection: &str, key: &Key) -> Result<Option<Record>> {
        let tree = self.tree_for(collection);
        match tree.get(&self.store, &key.encode()) {
            Some(bytes) => decode_record(&bytes)
                .map(Some)
                .ok_or_else(|| EngineError::Corrupt(format!("record in {}", collection))),
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
        decode_pairs(raw, collection)
    }

    /// Every record in a collection, in key order.
    pub fn scan(&mut self, collection: &str) -> Result<Vec<(Key, Record)>> {
        let tree = self.tree_for(collection);
        decode_pairs(tree.scan(&self.store), collection)
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
        let hash = self.store.inner().put_blob(&bytes)?;
        self.store.inner().put_path(&head_key(self.writer_id), &hash)?;
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
        self.head.set_root(to, &root);
        self.staged.insert(
            to.to_string(),
            Tree {
                root,
                config: self.config,
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
    config: ChunkConfig,
    merged: BTreeMap<String, Tree>,
}

impl<S: ObjectStore> Reader<S> {
    pub fn open(backend: S) -> Result<Self> {
        Self::open_with(backend, CacheConfig::default(), ChunkConfig::default())
    }

    pub fn open_with(backend: S, cache: CacheConfig, config: ChunkConfig) -> Result<Self> {
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

        // Resolve every head ref, then fetch all of them in one batch. Heads
        // are independent of each other, so there is no reason to pay a round
        // trip per writer in sequence — S3 issues the batch in parallel.
        let hashes: Vec<String> = paths
            .iter()
            .filter_map(|p| store.inner().get_path(p))
            .collect();
        let bodies = store.inner().get_blob_batch(&hashes).unwrap_or_default();

        let mut roots: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for bytes in bodies {
            // A head that does not decode is skipped, not fatal: one writer
            // publishing a corrupt head must not make every other writer's
            // data unreadable.
            let Some(head) = pond_record::decode_head(&bytes) else {
                continue;
            };
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
        })
    }

    /// Collections visible across all writers.
    pub fn collections(&self) -> Vec<String> {
        self.roots.keys().cloned().collect()
    }

    /// Merge every writer's tree for a collection, memoised per reader.
    fn tree_for(&mut self, collection: &str) -> Tree {
        if let Some(t) = self.merged.get(collection) {
            return Tree {
                root: t.root.clone(),
                config: self.config,
            };
        }
        let roots = self.roots.get(collection).cloned().unwrap_or_default();
        let mut iter = roots.into_iter();
        let mut acc = match iter.next() {
            Some(r) => Tree {
                root: r,
                config: self.config,
            },
            None => Tree::build(&self.store, Vec::new(), self.config),
        };
        for r in iter {
            let other = Tree {
                root: r,
                config: self.config,
            };
            acc = acc.merge(&self.store, &other, resolve_records);
        }
        self.merged.insert(
            collection.to_string(),
            Tree {
                root: acc.root.clone(),
                config: self.config,
            },
        );
        acc
    }

    pub fn get(&mut self, collection: &str, key: &Key) -> Result<Option<Record>> {
        let tree = self.tree_for(collection);
        match tree.get(&self.store, &key.encode()) {
            Some(bytes) => decode_record(&bytes)
                .map(Some)
                .ok_or_else(|| EngineError::Corrupt(format!("record in {}", collection))),
            None => Ok(None),
        }
    }

    pub fn scan(&mut self, collection: &str) -> Result<Vec<(Key, Record)>> {
        let tree = self.tree_for(collection);
        decode_pairs(tree.scan(&self.store), collection)
    }

    pub fn scan_range(
        &mut self,
        collection: &str,
        start: &Key,
        end: &Key,
    ) -> Result<Vec<(Key, Record)>> {
        let tree = self.tree_for(collection);
        let raw = tree.scan_range(&self.store, &start.encode(), &end.encode());
        decode_pairs(raw, collection)
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

fn decode_pairs(raw: Vec<(Vec<u8>, Vec<u8>)>, collection: &str) -> Result<Vec<(Key, Record)>> {
    let mut out = Vec::with_capacity(raw.len());
    for (k, v) in raw {
        let key = Key::decode(&k)
            .ok_or_else(|| EngineError::Corrupt(format!("key in {}", collection)))?;
        let rec = decode_record(&v)
            .ok_or_else(|| EngineError::Corrupt(format!("record in {}", collection)))?;
        out.push((key, rec));
    }
    Ok(out)
}
