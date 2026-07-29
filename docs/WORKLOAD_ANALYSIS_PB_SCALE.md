# Workload Analysis: StatsIndex at PB Scale

> Honest analysis of how the current StatsIndex design performs for
> OLTP, OLAP, point lookup, streaming, and future workloads at PB scale.
> Identifies where the design excels and where it breaks.

## The scaling math

| Scale | Row groups | Stats blob size | Verdict |
|-------|-----------|----------------|---------|
| 100K rows | 10 | 2KB | ✅ One fetch, instant |
| 1M rows | 100 | 20KB | ✅ One fetch, instant |
| 1B rows | 100K | 20MB | ⚠️ One fetch, but 20MB on S3 = ~200ms |
| 1TB data | 100M | 20GB | ❌ Can't fetch a 20GB stats blob |
| 1PB data | 100B | 20TB | ❌ Impossible |

**The single-blob stats index breaks at ~1TB of data** (100M row groups
= 20GB stats blob). This is the fundamental limit of "one blob for all
stats."

## How other systems solve this at PB scale

### Iceberg (PB-scale lakehouse)
- **Hierarchical manifests**: A manifest lists ~thousands of data files.
  A manifest-list lists manifests. The reader fetches the manifest-list
  (small), evaluates predicates against manifest stats, then fetches
  only the 1-2 relevant manifests.
- **Round trips**: 3 (manifest-list → manifest → data file)
- **Scales to PB**: manifest-list is ~KB, each manifest is ~MB

### Delta Lake (PB-scale lakehouse)
- **Transaction log checkpoints**: The log is periodically compacted
  into a checkpoint file (Parquet) that contains all file stats.
  The reader fetches the checkpoint, evaluates predicates, fetches data.
- **Round trips**: 2 (checkpoint → data file)
- **Scales to PB**: checkpoint is a Parquet file, can be predicate-pushed

### Database B-tree (PB-scale OLTP)
- **Hierarchical index pages**: Root page → internal pages → leaf pages.
  Each level prunes the search space by 1/tree_fanout.
- **Round trips**: O(log_fanout(N)) — typically 3-4 for billions of rows
- **Scales to PB**: each page is 4-16KB, tree depth stays ~4

### HBase / Bigtable (PB-scale KV/streaming)
- **Hierarchical tablet metadata**: Root tablet → metadata tablets →
  data tablets. Each level prunes by key range.
- **Round trips**: 3 (root → metadata → data)
- **Scales to PB**: metadata is distributed across tablets

## The pattern: HIERARCHY

All PB-scale systems use **hierarchy**: a small top-level index points
to mid-level indexes that point to data. The top level is always small
enough to fetch in one round trip. Each level prunes the search space.

Pond's ProllyTreeIndex is already hierarchical (B-tree-like), but the
StatsIndex is a flat single blob. The fix: make StatsIndex hierarchical.

## Proposed: Hierarchical StatsIndex

### Level 0: Stats Manifest (1 blob, ~KB)
- Aggregates stats across ALL row groups in the collection
- Contains: per-row-group key + blob_hash + per-column min/max
- Size: ~200 bytes × N row groups
- At PB scale (100B row groups): 20TB → STILL TOO BIG

**This doesn't work.** A flat manifest can't scale to PB.

### Level 0: Stats B-tree (hierarchical, ~4 fetches)
- Use the ProllyTreeIndex itself as the stats index
- Each internal node stores the UNION of its children's stats
  (max of maxs, min of mins)
- The reader walks the tree top-down, pruning branches whose stats
  don't match the predicate
- At each level, fetch 1 node (4-16KB), evaluate predicate, descend
  only into matching children

**Round trips**: O(log_fanout(N)) — typically 3-4 for billions of rows
**Scales to PB**: each node is small, tree depth stays ~4

This is what the ZoneMapIndex was TRYING to do (store stats in a
ProllyTreeIndex), but it stored stats as LEAF entries (one per row
group), not as INTERNAL NODE annotations (aggregated stats per
subtree). The innovation: annotate internal nodes with subtree-level
stats so the reader can prune entire subtrees without fetching them.

### Workload analysis with Hierarchical StatsIndex

## 1. OLTP (point lookups, small reads, frequent writes)

**Workload**: Read 1 row by primary key. Write 1 row.

**Current Pond approach**:
- ProllyTreeIndex already supports O(log N) key lookup
- The key (e.g., "rg/999") tells you the max PK in that row group
- Point lookup: walk the ProllyTreeIndex to find the row group
  containing the key → 1 data blob fetch

**With StatsIndex**: No benefit for point lookups. The ProllyTreeIndex
key-range lookup already provides O(log N) access. StatsIndex is for
PREDICATE pruning (WHERE age > 30), not for key lookups.

**Verdict**: ✅ Pond already handles OLTP well via ProllyTreeIndex.
StatsIndex is orthogonal — it helps OLAP, not OLTP.

**Write overhead**: Writing 1 row to a ProllyTreeIndex is O(log N)
(key insertion + tree rebalance). No stats overhead (stats are computed
at row-group granularity, not per-row).

**PB scale**: ProllyTreeIndex scales to PB (tree depth ~4 for billions
of entries). Each node is 4-16KB. Point lookup = 4 fetches.

## 2. OLAP (scans with predicates, aggregations)

**Workload**: SELECT SUM(sales) FROM events WHERE region = 'US' AND date >= '2024-01-01'

**With flat StatsIndex** (current):
- 1 fetch for stats blob → evaluate predicate → fetch surviving blobs
- At 1M row groups: 1 fetch (20KB) + ~10 data fetches = 11 total
- At 100M row groups (1TB): 1 fetch (20GB) → ❌ BLOWS UP

**With hierarchical StatsIndex** (proposed):
- Walk stats tree top-down: fetch root (16KB) → prune → fetch 1-2
  internal nodes (16KB each) → prune → fetch 1-2 leaf nodes (16KB)
  → identify surviving row groups → fetch data blobs
- Round trips: ~4 (tree walk) + ~10 (data) = ~14 total
- At PB scale: SAME — tree depth is ~4 regardless of data size

**Verdict**: ✅ Hierarchical StatsIndex handles OLAP at any scale.
Flat StatsIndex works up to ~1TB, then breaks.

## 3. Point Lookup (by non-primary key)

**Workload**: SELECT * FROM users WHERE email = 'alice@example.com'

**With StatsIndex**: StatsIndex stores min/max per column. For a
point lookup on a non-key column, the stats can prune row groups
where email ∉ [min, max]. But email is a string — min/max is only
useful for range queries, not equality on high-cardinality strings.

**Better approach**: Use a CollectionIndex (secondary index) — a
ProllyTreeIndex that maps email → rowid. O(log N) lookup, 2 fetches.

**Verdict**: ⚠️ StatsIndex helps for range predicates (age >= 30),
but point lookups on high-cardinality columns need a secondary index
(CollectionIndex — already exists in pond-sdk/extensions/indexing/).

## 4. Streaming (append-only writes, range reads)

**Workload**: Append 1MB of log data. Read bytes [50MB, 60MB].

**With StreamingLens**: Segments are stored in ProllyTreeIndex.
Range read = compute segment indices → fetch only overlapping segments.
No stats needed — the key (segment number) directly tells you which
segments to read.

**StatsIndex benefit**: None. Streaming is key-range access, not
predicate-based access. The ProllyTreeIndex key range is sufficient.

**Write overhead**: Zero. StreamingLens doesn't use StatsIndex.

**PB scale**: ✅ StreamingLens scales to PB (ProllyTreeIndex tree
depth ~4, each segment is independent).

## 5. Vector Search (k-NN with bounding-box pruning)

**Workload**: Find k=5 nearest vectors to query [1.0, 2.0].

**With VectorLens**: Per-dimension bounding-box zone maps enable
skipping chunks whose box can't contain top-k vectors. This IS
predicate-based pruning (the "predicate" is a distance lower bound).

**StatsIndex benefit**: ✅ The stats index stores per-dimension
min/max, which is exactly the bounding box. The VectorLens can use
StatsIndex to skip non-matching chunks in 1 fetch (stats blob) +
N fetches (surviving chunks).

**PB scale**: Same OLAP analysis — flat StatsIndex breaks at 1TB,
hierarchical StatsIndex scales to PB.

## Summary Table

| Workload | StatsIndex benefit? | Round trips (current) | Round trips (PB scale) | Scales? |
|----------|--------------------|-----------------------|------------------------|---------|
| OLTP (key lookup) | None (ProllyTreeIndex handles it) | 4 (tree walk) | 4 | ✅ |
| OLAP (predicate scan) | ✅ Critical | 2 (flat) | 4 (hierarchical) | ✅ with hierarchy |
| Point lookup (non-key) | ⚠️ Partial (need secondary index) | 2 + N | 4 + N | ⚠️ Need CollectionIndex |
| Streaming (range read) | None (key range sufficient) | 2 (segment lookup) | 4 | ✅ |
| Vector search (k-NN) | ✅ Bounding-box prune | 2 (flat) | 4 (hierarchical) | ✅ with hierarchy |

## The path forward

1. **Keep the flat StatsIndex for small-medium collections** (< 1M row
   groups). It's simpler, faster, and covers 99% of use cases.

2. **Add hierarchical StatsIndex for PB-scale**: annotate ProllyTreeIndex
   internal nodes with subtree-level stats (union of children's min/max).
   The reader walks the tree top-down, pruning subtrees. O(log N) fetches.

3. **Don't force one approach**: let the lens choose. Small collections
   use flat StatsIndex (1 fetch). Large collections use hierarchical
   (4 fetches). The API is the same: `scan_with_pruning(collection, predicates)`.

4. **OLTP and streaming don't need StatsIndex at all** — they use the
   ProllyTreeIndex key for direct access. StatsIndex is for OLAP and
   vector search (predicate-based pruning).

## Write overhead analysis

| Workload | Write path | Stats overhead | Total write cost |
|----------|-----------|---------------|-----------------|
| OLTP (1 row) | ProllyTreeIndex insert | 0 (stats at row-group level) | O(log N) |
| OLAP (batch 10K rows) | Write row group + stats entry | ~1μs (compute min/max) | O(chunk_size) |
| Streaming (1MB append) | Write segment + ProllyTreeIndex | 0 (no stats) | O(1) |
| Vector (1 vector) | Write blob + ProllyTreeIndex | 0 (stats at chunk level) | O(log N) |

**Stats overhead is ZERO for OLTP and streaming** (no stats computation
per write). For OLAP, it's ~1μs per row group (one min/max pass) —
negligible compared to the encoding + compression + kernel write cost.

## Efficiency

| Metric | Value |
|--------|-------|
| Stats blob size (flat, 100 row groups) | ~20KB |
| Stats blob size (flat, 1M row groups) | ~20MB |
| Stats tree depth (hierarchical, 1B row groups) | ~4 |
| Stats tree node size | ~16KB (same as ProllyTree) |
| Write overhead (OLTP) | 0 |
| Write overhead (OLAP batch) | ~1μs per row group |
| Read overhead (predicate eval) | ~1μs per row group (in-memory) |

## Simplicity

| Component | LOC | Replaces |
|-----------|-----|----------|
| StatsIndex (flat) | 130 | ZoneMapIndex (460 LOC) |
| StatsIndex (hierarchical) | ~200 (future) | ZoneMapIndex + manifest |
| Total simplification | -260 LOC | -56% reduction |

## Scalability

| Scale | Flat StatsIndex | Hierarchical StatsIndex |
|-------|----------------|------------------------|
| 100K rows | ✅ 1 fetch (2KB) | ✅ 4 fetches (overkill) |
| 1M rows | ✅ 1 fetch (20KB) | ✅ 4 fetches |
| 1B rows | ⚠️ 1 fetch (20MB) | ✅ 4 fetches |
| 1TB data | ❌ 20GB blob | ✅ 4 fetches |
| 1PB data | ❌ 20TB blob | ✅ 4 fetches |

**Conclusion**: Flat StatsIndex is perfect for < 1B rows. Hierarchical
StatsIndex (future work) extends to PB. Both are simpler than the
current ZoneMapIndex design.
