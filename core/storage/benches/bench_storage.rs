// Benchmark suite for pond_storage.
//
// Groups:
//   - write_rows    — 100 / 1K / 10K / 100K rows (PND2 encode + commit + manifest)
//   - read_rows     — 10K rows pre-written, measure decode + manifest load
//   - crdt_merge    — 100 / 1K / 10K rows with forced conflicts
//   - vector_search — 10K × 384-dim vectors, L2 / cosine / dot
//   - upsert_shard  — 100 / 1K / 10K rows (CRDT shard append)
//
// All benchmarks use `criterion::black_box` to prevent the compiler from
// optimizing away the computation. Run with:
//
//   cargo bench -p pond_storage
//
// To just compile (no run):
//
//   cargo bench -p pond_storage --no-run

use criterion::black_box;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use pond_core::vector;
use pond_kernel::crdt::HLC;
use pond_storage::{read, shard, write, UnifiedStorage};
use serde_json::{json, Value};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build a 32-char HLC hex string from (physical_ms, logical).
fn hlc_value(physical: u64, logical: u64) -> String {
    format!("{:016x}{:016x}", physical, logical)
}

/// Generate `n` i64 values 0..n.
fn make_i64_column(n: usize) -> Vec<i64> {
    (0..n as i64).collect()
}

// ---------------------------------------------------------------------------
// Benchmark: write_rows — 100 / 1K / 10K / 100K rows
// ---------------------------------------------------------------------------

fn bench_write_rows(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_rows");

    for n in [100usize, 1_000, 10_000, 100_000] {
        // Pre-create column data (shared across iterations — not measured)
        let ids = make_i64_column(n);
        let vals: Vec<i64> = (0..n as i64).map(|i| i * 2).collect();

        group.bench_with_input(BenchmarkId::from_parameter(n), &(), |b, _| {
            b.iter_batched(
                || {
                    // Fresh storage per iteration (so each write starts from empty)
                    let dir = tempfile::tempdir().unwrap();
                    let storage = UnifiedStorage::new_local(dir.path()).unwrap();
                    (dir, storage)
                },
                |(_dir, storage)| {
                    let kernel = storage.kernel();
                    let cols: [(&str, &[i64]); 2] = [("id", &ids), ("val", &vals)];
                    let hash = write::write_rows_i64(kernel, "bench", "main", &cols, "bench")
                        .unwrap();
                    black_box(hash);
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Benchmark: read_rows — 10K rows pre-written
// ---------------------------------------------------------------------------

fn bench_read_rows(c: &mut Criterion) {
    // Set up 10K rows once (not measured)
    let dir = tempfile::tempdir().unwrap();
    let storage = UnifiedStorage::new_local(dir.path()).unwrap();
    let kernel = storage.kernel();

    let n = 10_000usize;
    let ids = make_i64_column(n);
    let vals: Vec<i64> = (0..n as i64).map(|i| i * 2).collect();
    write::write_rows_i64(kernel, "bench_read", "main", &[("id", &ids), ("val", &vals)], "init")
        .unwrap();

    c.bench_function("read_rows/10K", |b| {
        b.iter(|| {
            let cols = read::read_rows_i64(kernel, "bench_read", "main", None, None).unwrap();
            black_box(cols);
        });
    });
}

// ---------------------------------------------------------------------------
// Benchmark: crdt_merge — 100 / 1K / 10K rows with forced conflicts
//
// Each rowid has 5 versions, so the merge must resolve conflicts.
// ---------------------------------------------------------------------------

fn bench_crdt_merge(c: &mut Criterion) {
    let mut group = c.benchmark_group("crdt_merge");

    for n in [100usize, 1_000, 10_000] {
        // Generate n rows with forced conflicts: 5 versions per rowid
        let n_unique = (n / 5).max(1);
        let rows: Vec<Value> = (0..n)
            .map(|i| {
                let rowid_idx = i % n_unique;
                let version = (i / n_unique) as u64 + 1; // 1, 2, 3, 4, 5
                json!({
                    "_rowid": format!("row-{}", rowid_idx),
                    "_version": hlc_value(version * 1000, 0),
                    "_deleted": false,
                    "id": format!("row-{}", rowid_idx),
                    "value": i,
                    "version_num": version,
                })
            })
            .collect();

        group.bench_with_input(BenchmarkId::from_parameter(n), &rows, |b, rows| {
            b.iter(|| {
                let merged = shard::merge_rows_by_rowid(black_box(rows), Some("id"));
                black_box(merged);
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Benchmark: vector_search — 10K × 384-dim vectors, L2 / cosine / dot
// ---------------------------------------------------------------------------

fn bench_vector_search(c: &mut Criterion) {
    let n_vectors = 10_000usize;
    let dim = 384usize;

    // Generate deterministic vectors (no external rand crate)
    // Each vector is a simple arithmetic sequence — reproducible across runs.
    let vectors: Vec<Vec<f32>> = (0..n_vectors)
        .map(|i| {
            (0..dim)
                .map(|j| {
                    let v = ((i * dim + j) as f32) * 0.001;
                    // Keep values in [-1, 1] for cosine/dot stability
                    (v % 2.0) - 1.0
                })
                .collect()
        })
        .collect();

    let query: Vec<f32> = (0..dim)
        .map(|j| {
            let v = (j as f32) * 0.001;
            (v % 2.0) - 1.0
        })
        .collect();

    let mut group = c.benchmark_group("vector_search");
    group.sample_size(20); // 10K × 384 is heavy — fewer samples for speed

    for metric in ["l2", "cosine", "dot"] {
        group.bench_function(metric, |b| {
            b.iter(|| {
                let results = vector::search_vectors(
                    black_box(&query),
                    black_box(&vectors),
                    metric,
                    10,
                );
                black_box(results);
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Benchmark: upsert_shard — 100 / 1K / 10K rows
//
// Each iteration starts from a fresh storage with a placeholder HEAD commit,
// then appends one CRDT shard with n rows.
// ---------------------------------------------------------------------------

fn bench_upsert_shard(c: &mut Criterion) {
    let mut group = c.benchmark_group("upsert_shard");

    for n in [100usize, 1_000, 10_000] {
        // Pre-create the rows (shared across iterations — not measured)
        let rows: Vec<Value> = (0..n)
            .map(|i| {
                json!({
                    "_rowid": format!("r{}", i),
                    "id": format!("r{}", i),
                    "val": i,
                })
            })
            .collect();

        group.bench_with_input(BenchmarkId::from_parameter(n), &rows, |b, rows| {
            b.iter_batched(
                || {
                    let dir = tempfile::tempdir().unwrap();
                    let storage = UnifiedStorage::new_local(dir.path()).unwrap();
                    // Need an initial commit so the branch exists
                    write::write(storage.kernel(), "bench", "main", b"init", "init").unwrap();
                    (dir, storage)
                },
                |(_dir, storage)| {
                    let kernel = storage.kernel();
                    let mut hlc = HLC::new();
                    let hash = shard::upsert_shard(
                        kernel,
                        "bench",
                        "main",
                        "s1",
                        rows,
                        Some("id"),
                        &mut hlc,
                    )
                    .unwrap();
                    black_box(hash);
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_write_rows,
    bench_read_rows,
    bench_crdt_merge,
    bench_vector_search,
    bench_upsert_shard,
);
criterion_main!(benches);
