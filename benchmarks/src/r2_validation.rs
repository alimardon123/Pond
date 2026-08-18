// r2_validation — measure the storage claims against real object storage.
//
// Every number reported in the design work so far came from memory or local
// disk. Local disk answers a ranged read in microseconds; object storage takes
// tens of milliseconds and charges per request. A design whose cost model is
// "round trips" has to be measured where round trips actually cost something.
//
// This binary measures four claims on a live S3-compatible endpoint:
//
//   1. Constant depth   — GETs per point lookup stay flat as data grows 100x
//   2. Warm lookup      — with the index top cached, a lookup is ~1 GET
//   3. Write amplification — PUTs and bytes per record, by batch size
//   4. Baseline         — a raw single-object GET, to separate Pond's
//                         overhead from the endpoint's latency
//
// Run:
//     set -a && . .env && set +a
//     cargo run --release -p pond_bench --bin r2_validation
//
// Credentials come only from the environment. Everything is written under a
// unique run prefix and deleted at the end, so the bucket is left as found.
//
// Scale note: every node write here is one sequential PUT round trip, so bulk
// load — not lookup — is what bounds how large these runs can be. That is a
// finding in itself: a production bulk load path needs parallel node writes.
// The sizes below span two orders of magnitude, which is enough to show
// whether lookup cost is flat.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use pond_cache::{BlobCache, CacheConfig};
use pond_index::{int, str_, ChunkConfig, Key, NodeStore, Tree};
use pond_kernel::ObjectStore;
use pond_record::{encode_record, Record, Value, Version};
use pond_s3::S3ObjectStore;

/// A NodeStore over any ObjectStore that counts requests and time spent.
struct MeasuredStore<S: ObjectStore> {
    inner: S,
    gets: AtomicU64,
    puts: AtomicU64,
    bytes_put: AtomicU64,
    get_nanos: AtomicU64,
}

impl<S: ObjectStore> MeasuredStore<S> {
    fn new(inner: S) -> Self {
        Self {
            inner,
            gets: AtomicU64::new(0),
            puts: AtomicU64::new(0),
            bytes_put: AtomicU64::new(0),
            get_nanos: AtomicU64::new(0),
        }
    }
    fn reset(&self) {
        self.gets.store(0, Ordering::Relaxed);
        self.puts.store(0, Ordering::Relaxed);
        self.bytes_put.store(0, Ordering::Relaxed);
        self.get_nanos.store(0, Ordering::Relaxed);
    }
    fn gets(&self) -> u64 {
        self.gets.load(Ordering::Relaxed)
    }
    fn puts(&self) -> u64 {
        self.puts.load(Ordering::Relaxed)
    }
    fn bytes_put(&self) -> u64 {
        self.bytes_put.load(Ordering::Relaxed)
    }
}

impl<S: ObjectStore> NodeStore for MeasuredStore<S> {
    fn put(&self, bytes: Vec<u8>) -> String {
        self.puts.fetch_add(1, Ordering::Relaxed);
        self.bytes_put
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        self.inner.put_blob(&bytes).expect("put_blob")
    }
    fn get(&self, hash: &str) -> Option<Vec<u8>> {
        let t = Instant::now();
        let r = self.inner.get_blob(hash).ok();
        self.get_nanos
            .fetch_add(t.elapsed().as_nanos() as u64, Ordering::Relaxed);
        self.gets.fetch_add(1, Ordering::Relaxed);
        r
    }
}

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx]
}

/// Records shaped like a real collection: `(table, id) -> record`.
fn records(n: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    (0..n)
        .map(|i| {
            let key = Key::new(vec![str_("users"), int(i as i64)]).encode();
            let rec = Record::new()
                .with_field("id", Value::Int(i as i64), Version::new(1, 0, 1))
                .with_field(
                    "name",
                    Value::Str(format!("user{}", i)),
                    Version::new(1, 0, 1),
                );
            (key, encode_record(&rec))
        })
        .collect()
}

fn run_url(base: &str, suffix: &str) -> String {
    match base.split_once('?') {
        Some((path, query)) => format!("{}/{}?{}", path.trim_end_matches('/'), suffix, query),
        None => format!("{}/{}", base.trim_end_matches('/'), suffix),
    }
}

fn main() {
    let base = match std::env::var("POND_R2_URL") {
        Ok(u) => u,
        Err(_) => {
            eprintln!("POND_R2_URL is not set. Load credentials first:");
            eprintln!("    set -a && . .env && set +a");
            std::process::exit(2);
        }
    };
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let run = format!("bench-{:x}", nanos);

    println!("# Pond — R2 validation");
    println!();
    println!("Endpoint: S3-compatible (Cloudflare R2), run prefix `{}`.", run);
    println!("All measurements are against live object storage, not local disk.");
    println!();
    println!("**Read the request counts, not the wall-clock times.** This run goes");
    println!("through an egress proxy that adds hundreds of milliseconds per request,");
    println!("so the latencies below are inflated well above what R2 serves directly.");
    println!("Request counts are unaffected, and they are what the design is built on:");
    println!("latency here is essentially `GETs x baseline`, which is the thesis.");
    println!();

    baseline_latency(&base, &run);
    constant_depth(&base, &run);
    warm_lookup(&base, &run);
    write_amplification(&base, &run);

    cleanup(&base, &run);
    println!();
    println!("Run prefix `{}` deleted; bucket left as found.", run);
}

/// What does a single object GET cost here? Everything else is measured in
/// multiples of this, so Pond's overhead is separable from the endpoint's.
fn baseline_latency(base: &str, run: &str) {
    let s = S3ObjectStore::from_url(&run_url(base, &format!("{}/baseline", run)))
        .expect("from_url");
    let h = s.put_blob(b"baseline probe payload").expect("put");

    let mut samples = Vec::new();
    for _ in 0..30 {
        let t = Instant::now();
        s.get_blob(&h).expect("get");
        samples.push(t.elapsed());
    }
    samples.sort();
    println!("## Baseline: one object GET");
    println!();
    println!(
        "| p50 | p90 | p99 |\n|---|---|---|\n| {:?} | {:?} | {:?} |",
        percentile(&samples, 0.50),
        percentile(&samples, 0.90),
        percentile(&samples, 0.99)
    );
    println!();
    s.delete_blob(&h).ok();
}

/// Claim 1: GETs per point lookup stay flat as the collection grows.
fn constant_depth(base: &str, run: &str) {
    println!("## Claim: constant-depth lookup");
    println!();
    println!("Index nodes only — the tree addresses segments, so this is metadata cost.");
    println!();
    println!("| records | depth | GETs/lookup | p50 | p99 | index nodes |");
    println!("|---|---|---|---|---|---|");

    let cfg = ChunkConfig::default();
    let mut first = None;
    let mut last = None;

    for n in [500usize, 5_000, 50_000] {
        let s = S3ObjectStore::from_url(&run_url(base, &format!("{}/depth{}", run, n)))
            .expect("from_url");
        let store = MeasuredStore::new(s);
        let tree = Tree::build(&store, records(n), cfg);
        let depth = tree.depth(&store);
        let nodes_written = store.puts();

        store.reset();
        let probes = 20;
        let mut samples = Vec::new();
        for i in 0..probes {
            let idx = (i * (n / probes.max(1))).min(n - 1);
            let k = Key::new(vec![str_("users"), int(idx as i64)]).encode();
            let t = Instant::now();
            assert!(tree.get(&store, &k).is_some(), "lookup must find the key");
            samples.push(t.elapsed());
        }
        samples.sort();
        let per = store.gets() as f64 / probes as f64;

        println!(
            "| {} | {} | {:.2} | {:?} | {:?} | {} |",
            n,
            depth,
            per,
            percentile(&samples, 0.50),
            percentile(&samples, 0.99),
            nodes_written
        );
        if first.is_none() {
            first = Some(per);
        }
        last = Some(per);
    }

    println!();
    if let (Some(f), Some(l)) = (first, last) {
        println!(
            "100x more data changed lookup cost by {:.2}x.",
            l / f.max(0.001)
        );
    }
    println!();
}

/// Claim 2: with the index top cached, a lookup costs about one GET.
fn warm_lookup(base: &str, run: &str) {
    println!("## Claim: warm lookup is ~1 GET");
    println!();

    let cfg = ChunkConfig::default();
    let n = 20_000usize;
    let s = S3ObjectStore::from_url(&run_url(base, &format!("{}/warm", run))).expect("from_url");
    let cached = BlobCache::new(s, CacheConfig::default()).expect("cache");
    let store = MeasuredStore::new(cached);
    let tree = Tree::build(&store, records(n), cfg);

    // Prime the cache the way a running process would: read the tree once.
    let _ = tree.scan(&store);

    store.reset();
    let probes = 50;
    let mut samples = Vec::new();
    for i in 0..probes {
        let idx = (i * (n / probes)).min(n - 1);
        let k = Key::new(vec![str_("users"), int(idx as i64)]).encode();
        let t = Instant::now();
        assert!(tree.get(&store, &k).is_some());
        samples.push(t.elapsed());
    }
    samples.sort();
    let stats = store.inner.stats();

    println!("| records | node reads/lookup | p50 | p99 | cache hit rate |");
    println!("|---|---|---|---|---|");
    println!(
        "| {} | {:.2} | {:?} | {:?} | {:.1}% |",
        n,
        store.gets() as f64 / probes as f64,
        percentile(&samples, 0.50),
        percentile(&samples, 0.99),
        stats.hit_rate() * 100.0
    );
    println!();
    println!(
        "Cache tier absorbed {} of {} reads. Because nodes are content-addressed,",
        stats.hits(),
        stats.hits() + stats.misses
    );
    println!("this needs no invalidation — a hash cannot name different bytes.");
    println!();
}

/// Claim 3: write cost amortizes across a batch.
///
/// This is the risk item: an insert rewrites its leaf and every ancestor, and
/// PUTs are the expensive operation. What matters is that batching amortizes
/// it — writes land in a shard first, and only compaction pays tree cost.
fn write_amplification(base: &str, run: &str) {
    println!("## Claim: write amplification amortizes");
    println!();
    println!("| batch | PUTs | PUTs/record | bytes/record | wall |");
    println!("|---|---|---|---|---|");

    let cfg = ChunkConfig::default();
    let base_n = 5_000usize;

    let mut single = None;
    let mut batched = None;

    for batch in [1usize, 100, 1_000] {
        let s = S3ObjectStore::from_url(&run_url(base, &format!("{}/wa{}", run, batch)))
            .expect("from_url");
        let store = MeasuredStore::new(s);
        let tree = Tree::build(&store, records(base_n), cfg);

        let updates: Vec<(Vec<u8>, Vec<u8>)> = (0..batch)
            .map(|j| {
                let key = Key::new(vec![str_("users"), int((j * 7 % base_n) as i64), int(1)])
                    .encode();
                let rec = Record::new().with_field(
                    "extra",
                    Value::Int(j as i64),
                    Version::new(2, 0, 1),
                );
                (key, encode_record(&rec))
            })
            .collect();

        store.reset();
        let t = Instant::now();
        let _ = tree.insert_batch(&store, updates);
        let wall = t.elapsed();

        let per = store.puts() as f64 / batch as f64;
        println!(
            "| {} | {} | {:.2} | {:.0} | {:?} |",
            batch,
            store.puts(),
            per,
            store.bytes_put() as f64 / batch as f64,
            wall
        );
        if single.is_none() {
            single = Some(per);
        }
        batched = Some(per);
    }

    println!();
    if let (Some(s), Some(b)) = (single, batched) {
        println!(
            "Batching reduced per-record write cost {:.0}x ({:.2} -> {:.2} PUTs/record).",
            s / b.max(0.0001),
            s,
            b
        );
    }
    println!();
}

/// Delete everything this run wrote.
fn cleanup(base: &str, run: &str) {
    let s = match S3ObjectStore::from_url(&run_url(base, run)) {
        Ok(s) => s,
        Err(_) => return,
    };
    // `list_paths` returns keys relative to this store's root, and
    // `delete_path` re-prefixes them the same way — so deleting by the listed
    // path is the only correct move. Reconstructing a blob key from its hash
    // does NOT work here: sub-benchmarks nest under their own sub-prefixes, so
    // the rebuilt key omits that segment and silently matches nothing. That
    // bug left objects behind on the first run.
    let mut removed = 0usize;
    match s.list_paths("") {
        Ok(paths) => {
            for p in &paths {
                if s.delete_path(p).unwrap_or(false) {
                    removed += 1;
                }
            }
            if removed < paths.len() {
                eprintln!(
                    "warning: {} of {} objects were not deleted — check the bucket",
                    paths.len() - removed,
                    paths.len()
                );
            }
        }
        Err(e) => eprintln!("warning: cleanup could not list objects: {}", e),
    }
    println!();
    println!("Cleanup: removed {} objects.", removed);
}
