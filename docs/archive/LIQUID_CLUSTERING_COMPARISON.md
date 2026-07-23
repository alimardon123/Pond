# Liquid Clustering vs Pond — Architectural Comparison

> **Status:** Research note. Compares Databricks Liquid Clustering
> (generally available in Delta Lake 15.4 LTS+, Public Preview for
> Iceberg in 16.4 LTS+) with Pond's content-addressed Prolly tree
> approach. Answers three questions: (1) what Liquid Clustering does,
> (2) what Pond does that overlaps, (3) what Pond can learn.

---

## 1. The three user questions, answered briefly

### Q1: Do our indexes support multi-key indexes?

**No — currently single-key only.** Looking at `pond-sdk/auto_index.py`:

```python
def register_index(self, name: str, extractor: Callable[[Any], str], ...)
```

The extractor returns a single `str` per data entry. So one row → one
index key. Two meanings of "multi-key" are NOT supported:

- **Composite keys** (index on `(col_a, col_b)`): the extractor returns
  a string, so you'd have to do `lambda d: f"{d['a']}:{d['b']}"` —
  stringified, not a true tuple. Range queries on `a` alone work
  (prefix-scan), but range queries on `b` alone don't.
- **Multi-valued indexes** (one row → multiple index keys, e.g.,
  indexing a list field): the extractor returns one string, so each
  row appears under at most one index key. To index a list field,
  you'd need to register one index per list element externally.

**Recommendation:** extend the extractor signature to
`Callable[[Any], str | list[str]]` (return one or many keys). This is
a backward-compatible change (single-string return still works) and
unlocks multi-valued indexes (the more important case). Composite
keys are already expressible via string concatenation; a future
extension could add tuple-of-strings for true multi-dimensional
range queries. This becomes a candidate for a future SDK polish
iteration.

### Q2: Can Views be created without a primary key column?

**No — keys are required.** Looking at the SDK:

```python
view.put(key: str, data: Any) -> str
```

`put` requires a key. The View model is fundamentally key→value.
Without a key, you cannot `put` (and therefore cannot `commit`).

But this is a soft constraint, not a hard one. Three workarounds:

1. **Auto-generated keys (recommended):** the Lens author generates
   keys (UUID, sequence, hash of content). The "primary key" exists
   but is invisible to the user. This is what most databases do
   internally when no PK is declared.
2. **Use the row's content hash as the key:** `view.put(hash(data), data)`.
   This deduplicates for free but breaks if two rows have the same
   content (they'd be the same row).
3. **Use a sequence number:** `view.put(f"row:{n}", data)`. Simple,
   but requires the Lens to track `n` across commits.

**Recommendation:** add an `auto_key: bool = False` parameter to
`put` that generates a UUID4 if `key is None`. This is a small,
non-breaking addition. Most other databases (Postgres, SQLite, Mongo)
support auto-generated PKs; Pond should too. Note: this is a
Layer 2 SDK convenience, NOT a kernel feature (the kernel still
requires keys; the SDK generates them on the caller's behalf).

### Q3: Liquid Clustering — what is it, and is our approach better?

**Short answer:** Liquid Clustering and Pond solve DIFFERENT problems.
Liquid Clustering is a *data layout optimization* for multi-dimensional
range queries on a single table. Pond is a *storage algebra* for
composing multiple workloads over one copy of data. Pond's Prolly
tree is better for point lookups, versioning, and content-addressing;
Liquid Clustering is better for multi-column range scans. The two
are complementary — Pond could absorb Liquid Clustering as a
materialization (RFC-0005). Detailed comparison in §3.

---

## 2. What Liquid Clustering actually is

### 2.1. The problem it solves

Databricks had two prior layout techniques:

1. **Partitioning** — physically split data by a column's value
   (e.g., `PARTITION BY date`). Fast for `WHERE date = X` predicates,
   but fails for high-cardinality columns (too many partitions) and
   is rigid (changing partition keys requires rewriting all data).
2. **ZORDER** — multi-dimensional clustering using the Z-order curve
   (Morton code). Maps N-dimensional points to 1-D while preserving
   locality. Good for `WHERE x = A AND y = B` predicates. But it
   **rewrites the entire Z-cube** on every OPTIMIZE — high write
   amplification — and Z-order curves have a known weakness: large
   jumps between certain data points reduce data-skipping
   effectiveness.

Liquid Clustering (GA in Databricks Runtime 15.4 LTS+) replaces both.
Its three innovations:

### 2.2. Innovation 1: Hilbert curves instead of Z-order curves

The Z-order (Morton) curve maps 2-D `(x, y)` to 1-D by interleaving
bits: `x0 y0 x1 y1 x2 y2 ...`. Adjacent 2-D points can be far apart
in 1-D — specifically, the curve has "jumps" where consecutive 1-D
positions span the entire 2-D space. This means some files end up
with min-max ranges that cover the entire dataset, defeating data
skipping.

The **Hilbert curve** is a different space-filling curve that
guarantees adjacent 1-D positions are also adjacent in 2-D (distance
≤ some small constant). For a 2-D quadrant, a Hilbert range covers
only TWO adjacent quadrants; a Z-order range of the same length can
span all four. Result: better data skipping, smaller min-max ranges
per file.

### 2.3. Innovation 2: Incremental clustering via "stable" and "unstable" Z-cubes

ZORDER rewrites the entire Z-cube on every OPTIMIZE. Liquid Clustering
introduces:

- **Z-cube:** a group of files clustered together by one OPTIMIZE job.
  Tagged with `ZCUBE_ID` (UUID) and `ZCUBE_ZORDER_BY` (clustering
  columns) in file metadata.
- **Target size:** each Z-cube has a target size (default 1 GB).
- **Stable Z-cube:** total file size ≥ target. NOT re-clustered on
  next OPTIMIZE.
- **Unstable Z-cube:** total file size < target. Re-clustered with
  new files on next OPTIMIZE.

Result: after the first OPTIMIZE pass, subsequent OPTIMIZEs only
touch unstable Z-cubes + new data. Write amplification drops
dramatically. Changing clustering keys (`ALTER TABLE ... CLUSTER BY
(newcol)`) only affects new data; old data keeps its old layout.

### 2.4. Innovation 3: Cluster keys are mutable, layout evolves

With partitioning and ZORDER, changing the layout column requires
rewriting all data. With Liquid Clustering, `ALTER TABLE ... CLUSTER
BY (newcol)` is metadata-only; new writes use the new key, old data
keeps the old key. Queries against `newcol` skip old files (which
don't have stats for `newcol`'s clustering); queries against the
old key still benefit from old clustering.

### 2.5. What it does NOT do

- **No content addressing.** Files are identified by UUID, not by
  content hash. Two files with identical content have different UUIDs
  (no dedup for free).
- **No versioning / history.** Liquid Clustering optimizes the current
  layout; it doesn't preserve old layouts as queryable versions.
  Delta Lake has time travel separately, but it's not a property of
  the clustering.
- **No structural sharing across versions.** Two versions of a table
  share data files only when those files are unchanged; there's no
  content-addressed deduplication of clustered layouts.
- **No cross-table sharing.** Each table has its own Z-cubes; two
  tables with the same data have separate physical files.
- **Single-table only.** Liquid Clustering operates within one table.
  It does not address cross-workload composition (e.g., the same
  data being readable as SQL, Arrow, and vectors simultaneously).

---

## 3. Comparison: Pond vs Liquid Clustering

| Dimension | Pond (Prolly tree) | Liquid Clustering (Hilbert) |
|---|---|---|
| **Primary problem solved** | Storage algebra for multi-workload composition | Data layout for multi-column range queries on one table |
| **Data model** | Key → bytes (kernel); View-defined shape above | Row-oriented table with columns |
| **Sort order** | 1-D (by key, lexicographic) | N-D (Hilbert curve on cluster keys) |
| **Point lookup** | O(log N) via Prolly tree binary search | O(file_count) — must check each file's min-max stats |
| **Range scan, single column** | O(log N + result_size) | O(file_count) → skip via min-max stats |
| **Range scan, multi-column** | Not natively supported (would need N-D index) | **Better** — Hilbert curve co-locates multi-dim points |
| **Versioning** | Built-in (commit DAG, branches, time travel) | Separate (Delta's time travel, not a clustering property) |
| **Content addressing** | Yes (SHA-256 of bytes; dedup for free) | No (UUIDs) |
| **Structural sharing across versions** | Yes (same chunks → same hash) | Limited (only unchanged files shared) |
| **Cross-table / cross-workload sharing** | Yes (CrossView; one copy serves all Lenses) | No (each table is isolated) |
| **Layout mutability** | New commit = new layout; old layouts preserved as history | ALTER TABLE changes cluster keys; old data keeps old layout |
| **Write amplification on layout change** | O(N) (new Prolly tree) — but old tree preserved | O(unstable Z-cubes) — incremental, lower for new data |
| **Incremental optimization** | Yes (delta commits; ≤K deltas between snapshots) | Yes (unstable Z-cubes; only new + unstable files re-clustered) |
| **Backend** | Any object store (FS, S3, FDB, Redis, SQLite, memory) | Databricks runtime + Delta Lake |
| **Scope** | Storage algebra (Layer 0–2) | Layout optimization (Layer 3+, on top of Delta) |

### 3.1. Where Pond is better

1. **Multi-workload composition.** Pond's reason for existing: one
   copy of data, many Views, no duplication. Liquid Clustering is
   per-table and per-workload. If you want SQL + vectors + streaming
   over the same data, Liquid Clustering gives you three copies
   (three tables, each with its own layout); Pond gives you one.
2. **Point lookups.** Pond's Prolly tree is a sorted B-tree with
   O(log N) binary search. Liquid Clustering relies on min-max stats
   per file — point lookups still require checking each file's range.
3. **Versioning and history.** Pond's commit DAG preserves every
   version forever (until GC). Time travel is O(K) where K is
   deltas-since-snapshot (≤4 by default). Liquid Clustering doesn't
   version layouts; Delta's time travel is separate and works at
   the snapshot level, not the layout level.
4. **Content addressing.** Two Pond Lenss with the same data share
   blobs (same hash). Two Delta tables with the same data have
   separate files. This matters for: deduplication (free in Pond),
   cross-View references (Pond's `put_raw` is zero-copy), and
   verifiable integrity (Pond's hash IS the address).
5. **Backend independence.** Pond runs on 6 backends (FS, memory,
   SQLite, Redis, S3, FDB) unchanged. Liquid Clustering requires
   Databricks runtime + Delta Lake.

### 3.2. Where Liquid Clustering is better

1. **Multi-column range queries.** This is the big one. A query like
   `WHERE x BETWEEN 10 AND 20 AND y BETWEEN 50 AND 60` on a Pond
   View with a Prolly tree sorted by `x` would scan all rows with
   `x` in [10, 20] and filter `y` in memory. Liquid Clustering's
   Hilbert curve co-locates `(x, y)` pairs in 2-D, so the scan can
   skip entire files whose 2-D range doesn't intersect the query
   rectangle. For wide tables with multi-predicate queries, this is
   a 10–100× speedup.
2. **Layout mutability without rewrite.** Changing cluster keys is
   metadata-only in Liquid Clustering. In Pond, changing the sort
   order requires building a new Prolly tree (O(N)). Pond preserves
   the old tree as history, but the rewrite cost is real.
3. **Incremental layout optimization.** Liquid Clustering's
   "stable Z-cube" concept means re-optimizing after a small insert
   only touches the new + unstable files. Pond's incremental index
   update (RFC-0005) achieves something similar for indexes, but
   the primary data tree still requires O(N) rebuild on layout
   change.
4. **Production maturity at PB scale.** Liquid Clustering runs in
   production at Databricks with PB-scale tables. Pond's prototype
   has been benchmarked at thousands of rows, not billions. This is
   an engineering gap, not an architectural one — but it's real.

### 3.3. What Pond can learn

Three concrete lessons, in priority order:

**Lesson 1: Multi-dimensional clustering as a materialization.**
Pond's Prolly tree is 1-D. For multi-column range queries, Pond
should add an `HilbertIndexView` (or similar) as a Layer 2
materialization per RFC-0005. The materialization is
`f(snapshot) → Hilbert-sorted layout`; it's derived, rebuildable,
and discardable. The kernel stays at 3 primitives; the SDK gains a
new materialization type. This is exactly the right shape for the
materialization calculus.

Concretely: a `ClusteredView` subclass that, on commit, sorts rows
by their Hilbert-curve key (computed from N cluster columns) and
builds a Prolly tree over that key. Point lookups by primary key
still use the underlying View's tree; range queries by cluster
columns use the Hilbert materialization.

**Lesson 2: Incremental layout with "stable" vs "unstable" chunks.**
Pond's Prolly tree already has chunk boundaries determined by a
rolling hash on keys. The analog of Liquid Clustering's "stable
Z-cube" would be: chunks that have reached a target size and have
not been modified since the last compaction are "stable"; new
writes go into "unstable" chunks; compaction merges unstable chunks
into stable ones.

This is *almost* what Pond's delta journal + COMPACTION_THRESHOLD
already does. The difference: Liquid Clustering's stable Z-cubes
don't get re-clustered on layout change, while Pond's compaction
rewrites the full snapshot. Adding a "stable chunk" concept that
survives compaction could reduce write amplification on layout-
heavy workloads. This is a future optimization, not a current gap.

**Lesson 3: Mutable cluster keys as a metadata-only operation.**
Pond's cluster-key change requires rebuilding the Prolly tree (the
sort order changes). Liquid Clustering's `ALTER TABLE ... CLUSTER BY`
is metadata-only; old data keeps the old layout, new data uses the
new layout.

Pond could support this by treating the cluster-key choice as a
commit-metadata field, not a tree-structure field. A query would
walk the commit DAG, find the cluster-key-in-effect at each commit,
and dispatch to the appropriate materialization. Old materializations
remain queryable; new commits build new materializations. This is
more complex than Liquid Clustering's approach (because Pond preserves
all history) but it's the versioning-aware analog.

### 3.4. What Pond should NOT learn

- **The Hilbert curve as a kernel-level sort order.** The kernel
  stays at 3 primitives; multi-dimensional clustering is a Layer 2
  materialization. (Already covered above.)
- **UUID-based file identification.** Pond's content addressing is
  strictly better — dedup, verifiable integrity, structural sharing.
  Do not regress to UUIDs.
- **Tight coupling to a single runtime.** Liquid Clustering only
  works in Databricks. Pond's backend independence is a core design
  goal; do not give it up.
- **Per-table optimization as the unit.** Liquid Clustering optimizes
  one table at a time. Pond's CrossView enables cross-View sharing;
  this is a higher-order capability that Liquid Clustering doesn't
  have. Do not regress to per-View isolation.

---

## 4. Conclusion

Pond and Liquid Clustering are not competitors. They solve different
problems:

- **Liquid Clustering** is the best-in-class *single-table layout
  optimizer* for analytical workloads with multi-column predicates.
  If you have a 1 PB Delta table and queries like
  `WHERE date BETWEEN X AND Y AND region = 'US' AND product = 'Widget'`,
  Liquid Clustering is the right tool.
- **Pond** is a *storage algebra* for composing multiple workloads
  (SQL, vectors, streaming, Git, etc.) over one copy of data. If you
  want the same data to be queryable as SQL, searchable as vectors,
  and branchable like Git — without duplication — Pond is the right
  tool.

**The right move for Pond:** absorb Liquid Clustering's *key
innovation* (Hilbert-curve multi-dimensional clustering) as a Layer 2
materialization (`ClusteredView`), while keeping the kernel at 3
primitives. This adds Liquid Clustering's analytical-query strength
to Pond's multi-workload composition strength, without inheriting
Liquid Clustering's limitations (per-table, single-runtime, no
content-addressing, no versioning).

This becomes a candidate RFC for Phase D or E (after the Arrow
adapter, which is the more foundational compatibility target).
