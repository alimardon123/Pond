# Architect Review #2 — Ultimate Unified Storage

**Date:** 2026-07-29
**Reviewer:** Senior distributed systems architect (first-time, unaware of project history)
**Verdict:** "The current design is 60% there. The kernel, binary encoding, and embedded_stats are already right. What's wrong is the StatsIndex layer: flat where it should be hierarchical, eager where it should be lazy, JSON where it should be binary, thin where it should be rich, and ad-hoc where it should be unified under a manifest."

## Top 3 Changes Needed

### 1. Collection Manifest — unify all indexes under one blob
A single per-collection, per-commit blob (`collections/{name}/manifest`) listing all index roots: clustered tree, stats tree, secondary indexes, bloom filters, schema, sort_order, counts. This is Iceberg's manifest-list, Delta's checkpoint — the same pattern across all PB-scale systems. Reads always start at the manifest (one fetch, ~1KB), then dispatch to the right index.

### 2. Lazy stats — zero write overhead, honestly
- Remove eager StatsIndex.update() from the write path
- Stats computed once per data chunk at write time (embedded_stats.py — 0.75% overhead, already built)
- Stats tree built lazily on first OLAP read, cached via content addressing
- "Zero write overhead" = no index maintenance on write path, not "stats are free"

### 3. Separate hierarchical stats tree (NOT annotated data tree)
- A separate Prolly/B+ tree keyed by row_group_key
- Leaves: rich per-row-group stats (min/max/null_count/cardinality/bloom_ref/sort_flag)
- Internal nodes: aggregated stats (max-of-max, min-of-min, union-bloom)
- Binary encoded (PND1 stays frozen)
- O(log N) reads AND O(log N) incremental writes via Prolly structural sharing

## Key Findings

- **The flat StatsIndex breaks at ~200GB** (not 1TB as the docs claim) for wide tables
- **JSON serialization** in stats_index.py loses float precision (default=str) — should be binary
- **Overwrite semantics** in StatsIndex.update() = O(N) write amplification per commit — regression from ZoneMapIndex's incremental ProllyTree writes
- **Annotating the data ProllyTree's internal nodes is wrong** — conflates OLTP key-index with OLAP stats-index, breaks the frozen format, couples OLTP reads with OLAP overhead
- **"One unified storage" is correct at the storage layer (kernel), wrong at the index layer** — different workloads need different indexes (clustered tree for OLTP, stats tree for OLAP, bloom for membership, secondary index for non-PK lookup)
- **The right principle: ONE STORAGE, MANY INDEXES, ONE MANIFEST**
- **COMPACTION_THRESHOLD bug**: `prolly_tree.py:73` says `= 1` but `:333` uses literal `16` — the "always snapshot" comment is false
