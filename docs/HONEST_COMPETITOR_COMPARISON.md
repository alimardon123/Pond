# Honest Competitor Comparison

> **Date:** 2026-07-30
> **Purpose:** Answer the user's question: "Does it really support any
> workload at PB scale with less round trips performantly (at least equal
> or more performant than competitors)?"
>
> **Answer: No.** Pond is a research prototype with a solid storage
> foundation but the lenses are not yet competitive with production
> systems in 4 of 5 workloads. This document is honest about where we
> win, where we lose, and what it would take to close each gap.

---

## Summary table

| Workload | Pond cold RTT | Competitor RTT | Competitor latency | Verdict |
|---|---|---|---|---|
| Lakehouse point lookup | 4 GETs (UnifiedStorage) | 3 GETs (Iceberg) | <50ms | Close, ~1.3x worse |
| Lakehouse full scan (N=100) | 103 GETs | 101 GETs (Iceberg) | ~5s | ~Equal on RTT |
| Vector k-NN @ 10M | **10M GETs** (linear scan) | 5-100 GETs (HNSW/IVF) | <100ms | **100,000x worse** |
| KV point lookup | 4 GETs (UnifiedStorage) | <1ms (Redis) | <1ms | 200x worse latency |
| Streaming append | N+3 PUTs ≈ 200ms | <5ms (Kafka) | <5ms | 40x worse, no consumer groups |
| Git | N/A (archived) | N/A | N/A | Not shipped |

**Bottom line:** Pond is directionally competitive on **tabular lakehouse
workloads** (point lookups and scans) when using the UnifiedStorage path.
It is **dramatically worse** on vector search (no HNSW/IVF), KV (no
transactions, single-writer), and streaming (no consumer groups, no
partitioning). The "PB scale" claim is **unverified** — max tested is 30K
row groups (smoke test).

---

## 1. Lakehouse (vs Iceberg, Delta Lake, Hudi)

### Pond's actual capability (verified from code)

- **Kernel:** `LakehouseLens` still imports `from kernel import PondMinimal`
  (SQLite). `ObjectStoreNativeKernel` exists but LakehouseLens doesn't use it.
- **Storage:** Parquet blobs via ProllyTreeIndex. Has `CollectionManifest`
  (one blob per commit with inline stats) — similar to Iceberg's manifest-list.
- **Unified storage:** LakehouseLens has manifest-aware read paths
  (`read_table_via_manifest`, `read_with_pruning_via_manifest`) but does NOT
  use the PND2 `UnifiedStorage` class. It still writes Parquet blobs.
- **Cold point lookup (manifest path):** 5 GETs (root pointer + root ref +
  commit + manifest + 1 data blob) on ObjectStoreNativeKernel. On
  PondMinimal (SQLite): 2 GETs (commit + manifest) but SQLite doesn't work on S3.
- **PB-scale:** Manifest delegates to StatsTree above 25K row groups.
  Manifest blob stays at ~64 bytes. O(log N) point lookups via stats tree.
  **But no real PB-scale benchmark exists** (max tested: 30K row groups).
- **Missing vs competitors:** No catalog service (Glue/REST/Nessie), no
  partitioning, no Z-Order/Liquid Clustering, no schema evolution protocol,
  no production deployments.

### Competitor capability

- **Iceberg:** Manifest-list → manifest → data files. Cold point lookup =
  3 GETs (metadata.json + manifest-list + 1 data file). PB-scale at Netflix,
  Stripe, Apple. Real catalogs, partition evolution, sort-order, Z-Order.
- **Delta Lake:** Optimized transaction log (checkpointed JSON). PB-scale
  at Databricks. Z-Ordering + Liquid Clustering for data skipping.
- **Hudi:** Copy-on-write + merge-on-read. PB-scale at Uber, ByteDance.
  Bloom index, record-level index, clustering.

### Gap

- **RTT:** Pond 4-5 GETs vs Iceberg 3 GETs → **~1.3-1.7x worse** cold.
  Warm (cached manifest): Pond 1 GET vs Iceberg 1 GET → **equal**.
- **Ecosystem:** Pond has no catalog, no SQL optimizer, no partitioning.
  Iceberg/Delta/Hudi have all three plus production deployments.
- **PB-scale:** Pond's stats tree design is sound but **unverified**.
  Iceberg/Delta/Hudi are deployed at PB scale.

### What it would take to be competitive

1. Switch LakehouseLens from PondMinimal to ObjectStoreNativeKernel.
2. Replace Parquet-blob storage with PND2 UnifiedStorage (already built).
3. Add partitioning (Hive-style or Liquid-like).
4. Run a real PB-scale benchmark (≥100M rows on real S3).
5. Add a catalog service (Iceberg REST or Nessie-compatible).

**Effort:** ~2-3 weeks. **Result:** would be RTT-competitive with Iceberg
on point lookups and scans. Ecosystem gap (catalog, SQL, partitioning)
would remain.

---

## 2. Vector search (vs FAISS, Milvus, Pinecone, Weaviate)

### Pond's actual capability (verified from code)

- **Search algorithm:** `VectorLens.search()` is a **linear scan**.
  Module docstring (line 35): *"Search is a linear scan over all vectors
  (suitable for small collections)."*
- **No HNSW, no IVF, no PQ, no DiskANN.** No ANN algorithm of any kind.
- **Pruning variant** (`search_with_pruning`): uses per-dimension bounding
  boxes. **Ineffective for high-dimensional vectors** (768-dim sentence
  embeddings) because bounding boxes are huge in high-D space.
- **Round trips for 10M vectors, k=10:** `search()` reads EVERY vector
  blob = **10M GETs**. At 50ms S3 RTT = **138 hours per query**.
- **Kernel:** `from kernel import PondMinimal` (SQLite). Has optional
  `use_unified_storage=True` but that doesn't help — search still scans all
  vectors.

### Competitor capability

- **FAISS:** HNSW (O(log N) ≈ 50-200 distance computations), IVF-PQ
  (O(√N) ≈ 3,000 distance comps at 10M). 10M-vector k-NN: **1 GET
  (memory) or ~10-100 GETs (DiskANN)**.
- **Milvus:** HNSW/IVF/DiskANN. 10M-vector k-NN: **~5-50 GETs, sub-100ms**.
- **Pinecone:** HNSW/IVF. **~5-50 GETs, <50ms**.
- **Weaviate:** HNSW. **~10-100 GETs, <100ms**.

### Gap

**Pond is 100,000x to 10,000,000x worse on RTTs for 10M vectors.**
This is the largest gap in the entire audit. Pond's vector lens is
**not competitive at any scale > 100K vectors**.

### What it would take to be competitive

1. Implement HNSW (graph-based ANN) as a Physical Structure — ~2,000-4,000 LOC.
2. Or implement IVF-PQ (product quantization) — ~1,500 LOC.
3. Or integrate DiskANN for disk-resident vector search.
4. Add GPU support (RAPIDS RAFT) for >1B vectors.

**Effort:** ~4-8 weeks for HNSW alone. **Result:** would be RTT-competitive
with Milvus/Weaviate. Without this, **Pond has no vector product**.

`docs/NON_GOALS.md` honestly admits: *"Pond's kernel has no HNSW, no IVF,
no ANN algorithm. VectorView does linear scan."* But the lens docstring
markets itself as *"production-ready vector database lens"* — that's overclaim.

---

## 3. KV (vs Redis, DynamoDB, FoundationDB, RocksDB)

### Pond's actual capability (verified from code)

- **Point lookup (UnifiedStorage path, opt-in):** 4 GETs cold = 200ms at S3
  RTT. Warm (cached manifest): 1 GET = 50ms.
- **Point lookup (legacy ProllyTreeIndex, default):** O(log N) via Prolly
  tree. On SQLite (local disk): ~0.25ms. On S3: impossible (SQLite doesn't
  run on S3).
- **Transactions:** **NO.** Single-collection atomic commit only.
  Cross-collection is out of model (A7).
- **Concurrent writers:** **NO.** Single-writer per Ref. Last-writer-wins
  on conflict.
- **Throughput:** ≤10 TPS per key (single-writer bottleneck).

### Competitor capability

- **Redis:** Sub-ms point lookup, single-threaded event loop = serializable,
  ACID via MULTI/EXEC, 100K+ QPS per shard.
- **DynamoDB:** Single-digit ms at any scale, millions of TPS, per-row
  transactions, on-demand billing.
- **FoundationDB:** ACID serializable at planet scale (Apple iCloud,
  Snowflake), ~5M ops/sec, MVCC, strict serializability.
- **RocksDB:** ~100K-1M ops/sec single-node, LSM-tree with memtable + SST
  + compaction, writes optimized.

### Gap

- **Latency:** Pond cold 200ms vs Redis <1ms → **200x worse**.
- **Throughput:** Pond ≤10 TPS vs Redis 100K+ TPS → **10,000x worse**.
- **Transactions:** Pond has none. FDB has full ACID.
- **Concurrent writers:** Pond has none. DynamoDB handles millions.

### What it would take to be competitive

1. Implement OLTP Lens (memtable + SST + compaction) — `WHERE_POND_FAILS.md`
   estimates 2-4 weeks.
2. Implement Counter CRDT Lens for hot-key contention — 1 week.
3. Wire `use_unified_storage=True` as default (currently OFF).
4. Switch kernel from PondMinimal to ObjectStoreNativeKernel.
5. Add SDK-level caching (manifest cache, root ref cache).

**Effort:** ~4-6 weeks. **Result:** would be a "RocksDB with versioning"
niche. Will NOT match Redis/FDB for OLTP — different design point.

---

## 4. Streaming (vs Kafka, Redpanda, Pulsar)

### Pond's actual capability (verified from code)

- **Storage:** Chunked segments (default 1MB each). ProllyTreeIndex maps
  `seg/0000000000` → blob_hash. Does NOT use UnifiedStorage/PND2.
- **Append latency:** Each append = N blob PUTs + 1 commit + 2 ref PUTs
  = N+3 PUTs. For a 1KB append at 50ms S3 RTT = 4 PUTs × 50ms = **200ms**.
- **Consumer groups:** **NO.** Zero matches for "consumer", "group",
  "partition" in streaming_lens.py.
- **Offsets:** Implicit (segment index = offset). No watermark, no
  exactly-once, no replay-from-offset API.
- **Kernel:** `from kernel import PondMinimal` (SQLite).

### Competitor capability

- **Kafka:** Partitioned logs, consumer groups with rebalancing,
  exactly-once via transactions, <5ms producer ack, millions of msgs/sec.
- **Redpanda:** Kafka-compatible, no JVM, Raft-based, ~10x throughput of
  Kafka, sub-ms latency.
- **Pulsar:** Tiered storage (BookKeeper + S3), <5ms, geo-replication.

### Gap

- **Append latency:** Pond 200ms vs Kafka/Redpanda <5ms → **40x worse**.
- **Throughput:** Pond single-writer ≤10 appends/sec vs Kafka millions/sec
  → **100,000x worse**.
- **Consumer groups:** Pond has none. All competitors have them.
- **Exactly-once:** Pond has none. Kafka has it.
- **Partitioning:** Pond has none. All competitors have them.

### What it would take to be competitive

1. Implement partitioning (multiple ProllyTreeIndex trees per topic).
2. Implement consumer groups (offset tracking per consumer-id).
3. Implement at-least-once semantics (offset commit protocol).
4. Switch to ObjectStoreNativeKernel + UnifiedStorage.
5. Add a Kafka wire-protocol adapter (like Redpanda does).

**Effort:** ~6-8 weeks. **Result:** would be a WarpStream-like design
(Kafka-on-S3). Without partitioning + consumer groups, **Pond is not a
streaming system** — it's chunked blob storage.

---

## 5. Git (vs Git, libgit2)

### Pond's actual capability

- **Status:** **ARCHIVED.** `archive/pond-git/pond_git.py` — 63-line
  prototype. Not in `lenses/`.
- **Missing vs real Git:** No packfiles (10x storage bloat), no line-based
  3-way merge, no shallow clone, no LFS, no submodules, no signing, no
  hooks, no HTTP/SSH protocol.

### Gap

Pond's git is an archived prototype. Not comparable to Git/libgit2.

### What it would take

Essentially "build Git from scratch" — ~6-12 months minimum. Not a
near-term priority.

---

## Where Pond DOES win

1. **Unified format across workloads** — PND2 is genuinely one format for
   tabular, KV, vector, and binary data. Iceberg can't store KV; Redis can't
   store Parquet; Kafka can't do point lookups. Pond's unified storage is a
   real architectural differentiator, even if the lenses aren't yet
   competitive.

2. **Versioning is free** — every write is a commit with a parent pointer.
   Time travel, branching, and merging are built into the kernel. Iceberg
   has time travel but no branching. Redis has neither. Kafka has offsets
   but no branching.

3. **Object-store-native** — `ObjectStoreNativeKernel` stores refs as
   content-addressed blobs. No SQLite, no local state. This is the right
   design for S3/GCS/Azure. (Iceberg uses a catalog service; Pond's approach
   is simpler but less mature.)

4. **Cold point lookup RTTs (tabular)** — 4 GETs via UnifiedStorage is
   close to Iceberg's 3 GETs. At PB scale, the stats tree provides O(log N)
   lookups. This is competitive on the RTT axis (though not on ecosystem).

5. **PB-scale stats tree** — the hierarchical stats tree design (aggregated
   min/max at internal nodes, content-addressed, lazy-built) is sound and
   would deliver O(log N) reads at 10M+ row groups. The design is right;
   the benchmark is missing.

---

## The honest path forward

Pond's storage foundation (kernel + PND2 + CollectionManifest + StatsTree)
is architecturally sound. The gap is in **workload-specific acceleration
structures** that live above the storage layer:

| Workload | Missing acceleration | Effort | Impact |
|---|---|---|---|
| Vector | HNSW or IVF | 4-8 weeks | 100,000x improvement |
| KV | Memtable + SST + compaction | 4-6 weeks | 200x latency improvement |
| Streaming | Partitions + consumer groups | 6-8 weeks | 40x latency + throughput |
| Lakehouse | Partitioning + catalog | 2-3 weeks | Ecosystem parity with Iceberg |

These are all **lens-level** work, not kernel changes. The kernel stays
frozen. The unified storage layer stays as-is. Each lens adds its own
acceleration structure on top.

**Until these ship, Pond is a research prototype with a good storage
foundation but no competitive workload.**
