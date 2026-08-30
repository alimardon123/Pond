// roundtrips.rs — the round-trip budget for every canonical operation.
//
// # Why this exists
//
// Round trips are the objective function of this design, and the repository
// has stated them in a document since July: `docs/ROUND_TRIP_AUDIT.md`, a
// hand-written table for a `CollectionManifest` read path, said to be
// "verified by scripts/benchmark_round_trips.py". That script is gone, and
// the path it described is now the legacy path — new collections go through
// `pond_engine`, which the table never covered. So the one number the product
// is designed around has been unmeasured for the whole of the engine's life.
//
// This replaces the table with a measurement. It reports, for each operation
// the system actually offers, the round trips (waits), the requests (the
// bill), the bytes moved, and a modelled wall clock — cold and warm, across
// three scales — and it emits the markdown that becomes the audit document,
// so the document cannot drift from the code again.
//
// # Requests are not round trips
//
// A 32-wide batch is 32 billable requests and one wait. `Metered` counts both
// (see `core/kernel/src/metered.rs`); the ratio between them is how wide the
// batching is really running. A column where the two are equal is a column
// with no parallelism in it, whatever the code looks like.
//
// # What "modelled" means, and what it does not
//
// The wall clock here is arithmetic, not a stopwatch: `round_trips * latency
// + bytes * ms_per_MiB`, with the constants below. Local disk cannot measure
// object-storage latency, and pretending otherwise is how a benchmark comes
// to certify something it never ran. The *counts* are exact and are the real
// output; the milliseconds are those counts priced at a documented rate. For
// numbers measured against real object storage, see `r2_validation` and
// `docs/R2_VALIDATION.md`.
//
//   cargo run --release -p pond_bench --bin roundtrips
//   cargo run --release -p pond_bench --bin roundtrips -- --markdown

use std::collections::BTreeMap;
use std::sync::Arc;

use pond_cache::CacheConfig;
use pond_engine::{Engine, EngineConfig, Reader};
use pond_index::{int, Key};
use pond_kernel::{LocalFSObjectStore, Metered, StoreStats};
use pond_record::{Record, Value, Version};

/// First-byte latency of one object-storage round trip, in milliseconds.
///
/// Taken from this repository's own R2 measurements rather than a vendor
/// figure: see `docs/R2_VALIDATION.md`. Same-region S3 is faster and
/// cross-region is slower; the counts do not change, only the price per
/// count, so a reader who wants a different number can multiply.
const LATENCY_MS: f64 = 30.0;

/// Marginal cost of moving a MiB once the first byte has arrived.
const MS_PER_MIB: f64 = 20.0;

/// Row counts to profile at.
///
/// Three, spanning three orders of magnitude, because the property being
/// tested is the *shape* of the curve — flat or growing — and two points
/// cannot show a shape. Kept to 100k at the top so the whole profile runs in
/// under a minute; the tree depth at 100k already exercises the multi-level
/// descent that 10^9 would, since depth grows as log(n).
const SCALES: &[usize] = &[1_000, 10_000, 100_000];

/// Writer counts to profile the multi-writer read cost at.
///
/// A separate axis from `SCALES`, because they answer different questions.
/// Row count asks whether cost grows with data; writer count asks whether it
/// grows with *concurrency*, which is the property the design actually claims
/// — any number of writers converging without coordination. A reader that
/// paid per writer would make that convergence worthless, and for a while it
/// did: a point read cost 141 round trips and 100 PUTs at 64 writers.
///
/// Held at a fixed, small row count per writer so the writer axis is not
/// confounded by the data axis.
const WRITER_COUNTS: &[usize] = &[1, 4, 16, 64];

/// Rows each writer publishes in the multi-writer profile.
const ROWS_PER_WRITER: usize = 200;

/// One measured operation.
struct Row {
    op: &'static str,
    scale: usize,
    warm: bool,
    stats: StoreStats,
}

impl Row {
    fn millis(&self) -> f64 {
        self.stats.modelled_millis(LATENCY_MS, MS_PER_MIB)
    }
}

/// A record with `fields` small columns, so a projection has something to skip.
fn record(seq: u64, fields: usize) -> Record {
    let mut r = Record::new();
    for f in 0..fields {
        r.set(
            &format!("col{f}"),
            Value::Str(format!("value-{seq}-{f}")),
            Version::new(seq, 1, 1),
        );
    }
    r
}

/// A single-part integer key.
fn k(i: i64) -> Key {
    Key::new(vec![int(i)])
}

fn rows(n: usize, fields: usize) -> Vec<(Key, Record)> {
    (0..n).map(|i| (k(i as i64), record(i as u64, fields))).collect()
}

/// Build a collection of `n` rows and return the directory holding it.
fn build(n: usize, fields: usize) -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFSObjectStore::new(dir.path()).unwrap();
    let mut engine = Engine::open(store, 1).unwrap();
    // In batches, because one 100k-row call is not how a workload arrives and
    // would measure a bulk load rather than a steady state.
    for chunk in rows(n, fields).chunks(10_000) {
        engine.write_records("t", chunk.to_vec()).unwrap();
    }
    engine.publish().unwrap();
    dir
}

/// Run `f` against a metered store rooted at `dir`, and report what it cost.
///
/// # What "warm" is allowed to mean
///
/// The tempting definition — run the closure twice on one reader and measure
/// the second — measures a process repeating a query it just ran, which
/// answers with its own in-memory cache and reports zero. That number is true
/// and useless: no workload reads the same rows twice in a row and stops.
///
/// The number the design actually claims is the local-disk cache one: a
/// *fresh* reader, with an empty memory tier, over a disk tier some earlier
/// reader populated. That is what a second process, a restarted process, or a
/// second query touching overlapping data really pays, and it is the figure
/// behind "single-digit-millisecond reads". So warm here means: run once
/// through a reader whose disk cache is `cache`, drop it, then measure a new
/// reader over the same `cache`.
///
/// Cold means a disk cache that does not yet exist.
fn measure<F>(dir: &std::path::Path, cache: &std::path::Path, warm: bool, mut f: F) -> StoreStats
where
    F: FnMut(&mut Reader<Arc<Metered<LocalFSObjectStore>>>),
{
    let store = Arc::new(Metered::new(LocalFSObjectStore::new(dir).unwrap()));
    let cfg = || CacheConfig::default().with_disk(cache, 8 * 1024 * 1024 * 1024);

    if warm {
        let mut prime =
            Reader::open_with(store.clone(), cfg(), EngineConfig::default()).unwrap();
        f(&mut prime);
        drop(prime);
        store.reset();
    }

    let mut reader = Reader::open_with(store.clone(), cfg(), EngineConfig::default()).unwrap();
    f(&mut reader);
    store.stats()
}

/// A cache directory for one measurement, fresh unless it is being reused.
fn cache_dir(root: &std::path::Path, name: &str) -> std::path::PathBuf {
    let d = root.join(name);
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn main() {
    let markdown = std::env::args().any(|a| a == "--markdown");
    let mut out: Vec<Row> = Vec::new();

    for &n in SCALES {
        let fields = 8;
        let dir = build(n, fields);
        let path = dir.path();

        let caches = tempfile::tempdir().unwrap();

        for warm in [false, true] {
            let tag = if warm { "warm" } else { "cold" };

            // Opening is the cost paid before any data is touched: find every
            // head and merge them. It is the floor under every other number
            // here, and the one that must not grow with anything.
            out.push(Row {
                op: "open",
                scale: n,
                warm,
                stats: measure(path, &cache_dir(caches.path(), &format!("open-{tag}")), warm, |_| {}),
            });

            let mid = k((n / 2) as i64);
            out.push(Row {
                op: "point read",
                scale: n,
                warm,
                stats: measure(path, &cache_dir(caches.path(), &format!("point-{tag}")), warm, |r| {
                    assert!(r.get("t", &mid).unwrap().is_some());
                }),
            });

            let lo = k(0);
            let hi = k(1_000.min(n) as i64);
            out.push(Row {
                op: "range scan 1k",
                scale: n,
                warm,
                stats: measure(path, &cache_dir(caches.path(), &format!("range-{tag}")), warm, |r| {
                    let got = r.scan_range("t", &lo, &hi).unwrap();
                    assert!(!got.is_empty());
                }),
            });

            out.push(Row {
                op: "full scan",
                scale: n,
                warm,
                stats: measure(path, &cache_dir(caches.path(), &format!("scan-{tag}")), warm, |r| {
                    assert_eq!(r.scan("t").unwrap().len(), n);
                }),
            });

            out.push(Row {
                op: "projected scan 2/8",
                scale: n,
                warm,
                stats: measure(path, &cache_dir(caches.path(), &format!("proj-{tag}")), warm, |r| {
                    let got = r.scan_projected("t", &["col0", "col1"]).unwrap();
                    assert_eq!(got.len(), n);
                }),
            });
        }

        // Writes are measured on their own store, since a write mutates what a
        // reader would then see and the two must not share a run.
        for (op, batch) in [("write 1 row", 1usize), ("write 1k rows", 1_000)] {
            let store = Arc::new(Metered::new(LocalFSObjectStore::new(path).unwrap()));
            let mut engine =
                Engine::open_with(store.clone(), 7, Default::default(), EngineConfig::default())
                    .unwrap();
            // Touch the tree first so the open cost is not billed to the write.
            let _ = engine.get("t", &k(0)).unwrap();
            store.reset();
            let start = n as i64 + 1;
            let recs: Vec<(Key, Record)> = (0..batch)
                .map(|i| (k(start + i as i64), record(i as u64, 8)))
                .collect();
            engine.write_records("t", recs).unwrap();
            engine.publish().unwrap();
            out.push(Row { op, scale: n, warm: false, stats: store.stats() });
        }
    }

    // The writer axis. Reported with `scale` holding the writer count, since
    // that is what varies; the row count per writer is fixed.
    for &w in WRITER_COUNTS {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path();
        for writer in 0..w {
            let store = Arc::new(Metered::new(LocalFSObjectStore::new(path).unwrap()));
            let mut engine = Engine::open_with(
                store,
                writer as u64 + 1,
                Default::default(),
                EngineConfig::default(),
            )
            .unwrap();
            let rows: Vec<(Key, Record)> = (0..ROWS_PER_WRITER)
                .map(|i| {
                    let key = (writer * ROWS_PER_WRITER + i) as i64;
                    (k(key), record(i as u64, 8))
                })
                .collect();
            engine.write_records("t", rows).unwrap();
            engine.publish().unwrap();
        }

        let caches = tempfile::tempdir().unwrap();
        out.push(Row {
            op: "point read @writers",
            scale: w,
            warm: false,
            stats: measure(path, &cache_dir(caches.path(), "pw"), false, |r| {
                assert!(r.get("t", &k(10)).unwrap().is_some());
            }),
        });
        out.push(Row {
            op: "full scan @writers",
            scale: w,
            warm: false,
            stats: measure(path, &cache_dir(caches.path(), "sw"), false, |r| {
                assert_eq!(r.scan("t").unwrap().len(), w * ROWS_PER_WRITER);
            }),
        });
    }

    if markdown {
        print_markdown(&out);
    } else {
        print_table(&out);
    }
}

fn print_table(rows: &[Row]) {
    println!(
        "{:<20} {:>8} {:>6} {:>7} {:>9} {:>7} {:>10} {:>9}",
        "operation", "rows/W", "cache", "waits", "requests", "width", "KiB", "ms"
    );
    for r in rows {
        println!(
            "{:<20} {:>8} {:>6} {:>7} {:>9} {:>7.1} {:>10.1} {:>9.1}",
            r.op,
            r.scale,
            if r.warm { "warm" } else { "cold" },
            r.stats.round_trips,
            r.stats.requests(),
            r.stats.batch_width(),
            (r.stats.bytes_read + r.stats.bytes_written) as f64 / 1024.0,
            r.millis(),
        );
    }
}

/// Emit the audit document's table, so it is generated rather than written.
fn print_markdown(rows: &[Row]) {
    let mut by_op: BTreeMap<&str, Vec<&Row>> = BTreeMap::new();
    let mut order: Vec<&str> = Vec::new();
    for r in rows {
        if !by_op.contains_key(r.op) {
            order.push(r.op);
        }
        by_op.entry(r.op).or_default().push(r);
    }

    println!("| operation | rows (or writers, for the @writers rows) | cache | round trips | requests | batch width | KiB | modelled ms |");
    println!("| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: |");
    for op in order {
        for r in &by_op[op] {
            println!(
                "| {} | {} | {} | {} | {} | {:.1} | {:.1} | {:.1} |",
                r.op,
                r.scale,
                if r.warm { "warm" } else { "cold" },
                r.stats.round_trips,
                r.stats.requests(),
                r.stats.batch_width(),
                (r.stats.bytes_read + r.stats.bytes_written) as f64 / 1024.0,
                r.millis(),
            );
        }
    }
}
