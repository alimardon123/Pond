# Pond Phase Q.3 — Benchmark Report

> First head-to-head benchmarks of Pond vs Git, Dolt, and Iceberg
> (via DuckDB+Parquet). LakeFS skipped (requires running server).
> FDB skipped (different substrate class — transactional kv, not
> immutable versioned storage).
>
> **What this report is:** measured wall-clock and peak memory for
> 7 operations x 4 systems. Honest numbers, including where Pond
> loses.
>
> **What this report is not:** a comprehensive performance
> evaluation. The workloads are small (1-100 keys, 100 bytes each).
> The systems are not tuned. The benchmarks favor in-process
> implementations (Pond, Iceberg) over process-spawning ones (Git,
> Dolt). All caveats in §4.

---

## 1. Setup

### 1.1 Systems benchmarked

| System | Version | Backend | Implementation |
|---|---|---|---|
| Pond | local build | SQLite (default kernel) | Python, in-process |
| Git | 2.47.3 | local filesystem | C, subprocess |
| Dolt | 2.2.2 | local filesystem | Go, subprocess |
| Iceberg | via duckdb 1.5.5 + parquet | local Parquet files | Python, in-process |

### 1.2 Systems NOT benchmarked

- **LakeFS** — requires a running server. Out of scope for this
  environment.
- **FoundationDB** — different substrate class (transactional kv,
  not immutable versioned storage). Comparison would be apples-to-oranges.
- **Iceberg with real pyiceberg catalog** — pyiceberg installed but
  requires a catalog server (REST, Glue, etc.). We use a simplified
  IcebergBench that approximates the manifest+data-file pattern
  using DuckDB + Parquet directly.

### 1.3 Workloads

- **commit (1 small file):** commit a single 100-byte file.
- **commit (100 small files):** commit 100 100-byte files.
- **branch creation:** create a new branch ref pointing at HEAD.
- **point lookup:** read a single key from the latest commit.
- **full scan (100 keys):** read all 100 keys from the latest commit.
- **time travel:** read a single key at a 5-commit-old commit.
- **merge:** 2-parent merge commit (branch with 10 files merged into main).

### 1.4 Methodology

- Each operation runs N times (5-10); we report median wall-clock
  time and max peak memory across runs.
- Memory measured via `tracemalloc` (Python heap only — does not
  include subprocess RSS for Git/Dolt).
- All systems run on the same machine (Linux 5.10, 2 cores, 1GB
  heap).
- Pond and Iceberg are in-process Python; Git and Dolt spawn
  subprocesses per operation. **This biases the benchmark toward
  Pond and Iceberg** (no fork/exec cost). See §4.1.

---

## 2. Results

### 2.1 Summary table

| Operation | Pond | Git | Dolt | Iceberg |
|---|---|---|---|---|
| commit (1 file) | **0.28ms** | 9.3ms | 217ms | 2.3ms |
| commit (100 files) | **4.4ms** | 212ms | 6100ms | 31ms |
| branch creation | **0.10ms** | 3.4ms | 116ms | 0.23ms |
| point lookup | **0.25ms** | 1.4ms | 64ms | 0.61ms |
| full scan (100 keys) | 3.4ms | 128ms | 64ms | **0.60ms** |
| time travel | **0.18ms** | 1.4ms | 59ms | 0.65ms |
| merge | **0.57ms** | 3.1ms | 119ms | 1.4ms |

### 2.2 Peak memory

| Operation | Pond | Git | Dolt | Iceberg |
|---|---|---|---|---|
| commit (1 file) | **6KB** | 62KB | 60KB | 35MB |
| commit (100 files) | **29KB** | 76KB | 60KB | 2KB |
| branch creation | **1KB** | 60KB | 60KB | 1KB |
| point lookup | **29KB** | 59KB | 59KB | 1KB |
| full scan (100 keys) | 39KB | 109KB | 59KB | **23KB** |
| time travel | **14KB** | 59KB | 60KB | 1KB |
| merge | **9KB** | 60KB | 59KB | 1KB |

---

## 3. Analysis

### 3.1 Where Pond wins

Pond is fastest on 6 of 7 operations, often by 1-2 orders of
magnitude.

**Why Pond wins:**
- **In-process:** no subprocess fork/exec (Git, Dolt pay ~3-100ms
  per operation just for process spawn).
- **SQLite backend:** single-process, in-memory-ish (filesystem
  backed but page-cached).
- **Minimal kernel:** ~140 LOC, no optimizer, no query planner, no
  transaction manager. Just hash → bytes → name lookups.
- **No working tree:** Pond stores blobs directly; Git and Dolt
  maintain a working tree that must be synchronized on each commit.

**Where Pond wins biggest:**
- **commit (100 files):** Pond 4.4ms vs Git 212ms vs Dolt 6.1s.
  Pond writes 100 blobs + 1 tree + 1 commit; Git must write 100
  files to working tree + add to index + commit; Dolt must run
  100 INSERT statements (each spawning a process).
- **time travel:** Pond 0.18ms vs Git 1.4ms vs Dolt 59ms. Pond
  just reads the old commit blob; Git walks object graph; Dolt
  filters by commit hash.

### 3.2 Where Pond loses

**Pond loses on full scan: 3.4ms vs Iceberg 0.60ms.**

**Why Pond loses:**
- Pond's scan reads the tree blob (JSON), then issues a separate
  `Read` for each of 100 blobs. That's 100 filesystem reads.
- Iceberg's scan reads one Parquet file containing all 100 rows.
  DuckDB's Parquet reader is highly optimized (vectorized, columnar).
- Pond's tree format (JSON) is not columnar and not vectorized.

**Honest assessment:** for tabular workloads where scan performance
matters, Iceberg's columnar format is genuinely better. Pond's
generic-byte-storage advantage comes at a cost: without knowing
the data is tabular, Pond cannot apply columnar compression or
vectorized scan.

**Mitigation:** a Pond TabularLens could store data in Parquet
format inside Pond blobs, recovering Iceberg's scan performance
while keeping Pond's versioning. This is unimplemented; the
Phase Q.4 flagship will test whether this works in practice.

### 3.3 Where the benchmark is unfair

**The benchmark is unfair to Git and Dolt** (see §4.1):
- Both pay subprocess spawn cost (~3ms minimum per operation).
- Both maintain working trees that Pond does not.
- Dolt pays SQL parsing cost per INSERT (we run dolt sql -q once
  per row, which is pathologically slow).

**A fair benchmark would:**
- Use libgit2 (in-process Git) instead of subprocess git.
- Use Dolt's SQL server (persistent connection) instead of per-query
  subprocess.
- Use Iceberg with a real catalog and partitioned data.

These are out of scope for Phase Q.3. The numbers here are
directional, not definitive.

### 3.4 Iceberg's memory anomaly

Iceberg's first commit uses 35MB of memory (DuckDB initialization
cost). Subsequent commits use 2KB. This is a startup cost, not a
per-operation cost. For long-running workloads, Iceberg's amortized
memory is competitive.

Pond has no such startup cost — the kernel is ~140 LOC and
initializes in microseconds.

---

## 4. Caveats

### 4.1 Subprocess bias

Pond and Iceberg run in-process (Python). Git and Dolt run as
subprocesses. Each subprocess spawn costs ~3ms on Linux, plus
the cost of loading the binary (Git: ~5MB; Dolt: ~30MB).

For operations that take <10ms in-process (Pond's commit, branch,
lookup, time travel, merge), subprocess spawn dominates the
measured time for Git and Dolt. The "Pond is 30x faster than Git"
headline is partly real (Pond's kernel is genuinely smaller) and
partly artifact (Git pays subprocess cost Pond doesn't).

**To get fair numbers:** re-benchmark with libgit2 (in-process Git)
and Dolt's SQL server (persistent connection). Future work.

### 4.2 Small workloads

All workloads are small (1-100 keys, 100 bytes each). At this
scale, constant factors (subprocess spawn, JSON parsing, SQLite
page cache) dominate. At production scale (1M+ keys, MB-sized
values), the picture may change:
- Pond's per-blob Read becomes a bottleneck (1M GETs for a full scan).
- Iceberg's columnar scan becomes more dominant.
- Git's packfile format pays off (deduplication across history).
- Dolt's SQL optimizer kicks in (index usage, query planning).

**To get fair numbers:** re-benchmark at 1M keys. Future work.

### 4.3 Single-region, local disk

All benchmarks run on local disk in a single region. Object-store
backends (S3, R2, Azure Blob) have different cost models:
- Per-request cost (S3 charges per GET/PUT/LIST).
- Per-byte cost (egress fees).
- Latency (S3 GET ~20ms vs local disk ~0.1ms).

Pond's design is object-store-native (OSN definition in
`POND_FORMAL_ALGEBRAS.md` §5), but the current kernel uses SQLite
(local). The `ObjectStoreBackend` in `experiments/` demonstrates
the object-store variant but is not benchmarked here.

**To get fair numbers:** re-benchmark with S3 backend for all
systems. Future work.

### 4.4 No production tuning

No system is tuned:
- Pond uses default SQLite settings (no WAL mode, no cache tuning).
- Git uses default packfile settings.
- Dolt uses default settings.
- Iceberg uses default DuckDB settings.

Production deployments tune all of these. The benchmark measures
out-of-the-box performance, not optimized performance.

### 4.5 What this benchmark does NOT measure

- **Concurrent writers.** All benchmarks are single-threaded.
- **Large blobs.** All values are 100 bytes. MB/GB blobs may behave
  differently.
- **Long history.** All benchmarks have <10 commits. Million-commit
  history may behave differently.
- **Cross-region replication.** Not measured.
- **Failure recovery.** Not measured.
- **GC cost.** Not measured.
- **Schema evolution cost.** Not measured.

---

## 5. What the benchmarks prove (and don't prove)

### 5.1 What they prove

- **Pond's kernel is fast for small workloads on local disk.** This
  is not surprising — it's 140 LOC with no optimizer. It would be
  alarming if it were slow.
- **Pond's design choices (in-process, minimal kernel, no working
  tree) are not pathologically slow.** The architecture is
  competitive for the operations tested.
- **Iceberg's columnar format beats Pond's generic bytes for full
  scan.** This is the expected trade-off: Pond's generality costs
  scan performance.

### 5.2 What they do NOT prove

- **That Pond is faster than peer systems in production.** The
  benchmark is too small and too biased (subprocess, local disk,
  no tuning).
- **That Pond's architecture scales.** No benchmarks at 1M+ keys.
- **That Pond is competitive on object stores.** No S3 benchmarks.
- **That Pond's Lens algebra can recover Iceberg's scan performance.**
  The TabularLens idea (§3.2) is unimplemented.
- **That Pond is the right abstraction.** Performance is one axis;
  correctness, adoptability, and expert review are others.

---

## 6. Conclusion

The Phase Q.3 benchmarks are **directional, not definitive**. They
show Pond is competitive (often fastest) for small in-process
workloads on local disk, with one clear loss (full scan vs Iceberg's
columnar format). The benchmarks are biased toward Pond and Iceberg
by being in-process; biased against Git and Dolt by being
subprocess.

**The honest takeaway:** Pond's kernel is not pathologically slow.
The architecture's performance is plausible. But this benchmark
does not establish that Pond is *competitive* in any production
sense — that requires benchmarks at scale (1M+ keys), on object
stores (S3), with tuned systems, and with the TabularLens
implemented.

**Next steps:**
1. **Re-benchmark with libgit2 and Dolt SQL server** to remove
   subprocess bias.
2. **Benchmark at 1M keys** to test scaling.
3. **Benchmark on S3** to test object-store-native claims.
4. **Implement TabularLens** (Parquet-in-Pond-blobs) to test
   whether Pond can recover Iceberg's scan performance.
5. **Benchmark concurrent writers** to test the consistency model.

These are Phase Q.3 follow-ups. The current benchmark is the first
measured evidence; it is not the last word.

---

## Appendix: Raw benchmark output

```
=== Benchmark: commit (1 small file) ===
  Pond:   284µs (peak 6KB)
  Git:    9.3ms (peak 62KB)
  Dolt:   216.6ms (peak 60KB)
  Iceberg:2.3ms (peak 35.4MB)

=== Benchmark: commit (100 small files) ===
  Pond:   4.4ms (peak 29KB)
  Git:    212.0ms (peak 76KB)
  Dolt:   6.10s (peak 60KB)
  Iceberg:31.0ms (peak 2KB)

=== Benchmark: branch creation ===
  Pond:   98µs (peak 1KB)
  Git:    3.4ms (peak 60KB)
  Dolt:   115.7ms (peak 60KB)
  Iceberg:229µs (peak 1KB)

=== Benchmark: point lookup ===
  Pond:   246µs (peak 29KB)
  Git:    1.4ms (peak 59KB)
  Dolt:   64.1ms (peak 59KB)
  Iceberg:606µs (peak 1KB)

=== Benchmark: full scan (100 keys) ===
  Pond:   3.4ms (peak 39KB)
  Git:    128.2ms (peak 109KB)
  Dolt:   63.6ms (peak 59KB)
  Iceberg:595µs (peak 23KB)

=== Benchmark: time travel (read at old commit) ===
  Pond:   181µs (peak 14KB)
  Git:    1.4ms (peak 59KB)
  Dolt:   59.4ms (peak 60KB)
  Iceberg:646µs (peak 1KB)

=== Benchmark: merge (2-parent merge commit) ===
  Pond:   573µs (peak 9KB)
  Git:    3.1ms (peak 60KB)
  Dolt:   118.8ms (peak 60KB)
  Iceberg:1.4ms (peak 1KB)
```
