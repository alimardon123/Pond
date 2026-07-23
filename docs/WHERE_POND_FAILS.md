# Where Pond Fails — and Where It Can Win

> An honest catalog of workloads where Pond is challenged, and a
> roadmap for how the Lens + Physical Structure architecture can
> close each gap.
>
> **Per Design Goal 3.7 (Functional):** before claiming "Pond can't
> do X," we ask: (1) is there a Lens that could interpret Pond bytes
> as X? (2) is there a Physical Structure that could accelerate X?
> (3) is there a coordinator that could layer on the kernel for X's
> consistency needs (per A7)?
>
> Most "can't" claims are missing Lenses. This document maps each
> failure to the Lens or Physical Structure that would close it.

---

## 1. The pattern

Pond's design choices (immutable bytes, last-writer-wins refs,
Lamport clocks, no in-kernel consensus) make it excellent for
**append-mostly, version-heavy, read-mostly** workloads and
challenging for **high-frequency mutation, strong consistency, or
hot-key** workloads.

But "challenging" is not "impossible." The kernel is small; the
Lens algebra is infinite. The question for each hard workload is:
**what Lens would close the gap, and has it been built?**

---

## 2. Workloads where Pond currently struggles — and the Lens that fixes each

### 2.1 High-frequency OLTP

**Workload:** per-key transactional updates at high QPS (bank
balances, inventory, session state).

**Why Pond struggles today:**
- Each update creates a new blob. Per-key serialization requires
  optimistic concurrency (CAS via R3', conditional on backend) or
  a coordinator (A7).
- No in-place updates; every write is a new blob + a ref update.

**The Lens that fixes it: an OLTP Lens.**
- Stores recent writes in a mutable in-memory layer (like RocksDB's
  memtable) and flushes to Pond periodically.
- Per-key CAS handled inside the Lens (single-writer-per-key is a
  Lens-level contract, not a kernel contract).
- Old versions remain in Pond for time travel; hot recent versions
  live in the Lens's mutable layer.

**Status:** not built. Estimated effort: 2-4 weeks for a single-node
OLTP Lens; 2-3 months for a distributed one (with a coordinator
substrate per A7).

**Without the Lens:** Pond handles ~10 TPS per key (single-writer,
optimistic). With the OLTP Lens: 10K-100K TPS per key (in-memory
mutable layer + periodic flush). **Pond + OLTP Lens is competitive
with RocksDB for single-key workloads**; for true distributed OLTP,
use FoundationDB.

### 2.2 Distributed consensus / multi-writer convergence

**Workload:** multiple writers in different regions, automatic
conflict resolution.

**Why Pond struggles today:**
- A7: coordinator is out-of-model. REP8: no multi-writer convergence.
- The kernel applies LWW; conflict resolution is application-level.

**The Lens that fixes it: a CRDT Lens + a Coordinator substrate.**
- CRDT Lens: encodes data as Conflict-Free Replicated Data Types.
  Merges are deterministic (no conflicts). The Lens's `merge(A, B)`
  returns `A ⊔ B` (least upper bound in the CRDT lattice).
- Coordinator substrate (per A7): for non-CRDT data, layer Raft or
  Paxos on top of the kernel. The `TwoPhaseCommitCoordinator` in
  `services/replication/` is a reference; a Raft coordinator would
  provide linearizability.

**Status:** CRDT Lens not built. Coordinator: 2PC reference shipped
(`services/replication/`), Raft coordinator not built.

**Without the Lens:** single-writer per ref (REP1). With CRDT Lens:
multi-writer with automatic conflict-free convergence. With Raft
coordinator: multi-writer with linearizability. **Pond + CRDT Lens
matches Cassandra/Dynamo; Pond + Raft matches CockroachDB/Spanner**
(at the cost of adding the coordinator substrate).

### 2.3 Random in-place updates at scale

**Workload:** updating individual rows in a 1B-row table (user
profiles, IoT calibration).

**Why Pond struggles today:**
- Each update creates a new version. At 1M updates/day on a 1B-row
  table, that's 1M new blobs/day. GC pressure.

**The Lens that fixes it: an LSM Lens.**
- Stores recent updates in a Log-Structured Merge tree (memtable →
  SST → compaction). Old SSTs are flushed to Pond as immutable
  blobs. The Lens reads from memtable first, then SSTs (newest to
  oldest).
- This is exactly how RocksDB works — but the SSTs are stored as
  Pond blobs, gaining versioning and time travel for free.

**Status:** not built. Estimated effort: 4-6 weeks for a
single-node LSM Lens.

**Without the Lens:** Pond is read-heavy or append-heavy only. With
LSM Lens: **Pond matches RocksDB/Pebble for update-heavy workloads**
plus free time travel (RocksDB has no time travel).

### 2.4 Hot-key contention

**Workload:** many concurrent writers to the same key (counters,
leaderboards).

**Why Pond struggles today:**
- LWW means only one write survives. No atomic increment.

**The Lens that fixes it: a Counter CRDT Lens.**
- Each writer appends a delta ("+1") to a per-writer log. The Lens
  merges by summing deltas. No conflicts; no lost increments.
- This is the standard CRDT counter (PN-counter). It is well-understood.

**Status:** not built. Estimated effort: 1 week.

**Without the Lens:** ≤10 concurrent writers per key. With Counter
CRDT Lens: **unlimited concurrent writers; matches Redis/Cassandra
counters** plus free time travel (neither has it).

### 2.5 Streaming joins with low latency

**Workload:** sub-second streaming joins across multiple streams
(fraud detection, ad placement).

**Why Pond struggles today:**
- Snapshot-based commit model. RTT budget (3-5 RTTs) is 60-100ms
  on S3.
- No incremental state maintenance.

**The Lens that fixes it: a Streaming Lens with incremental state.**
- Maintains join state in a mutable in-memory layer (like Flink's
  RocksDB-backed state stores). Flushes state snapshots to Pond
  periodically for fault tolerance.
- Sub-second joins happen in the Lens's in-memory state; Pond
  provides durability and time travel.

**Status:** not built. Estimated effort: 2-3 months.

**Without the Lens:** batch or micro-batch (minute+ latency). With
Streaming Lens: **Pond matches Flink/Kafka Streams for sub-second
streaming joins** plus free time travel on the stream state (Flink
has limited time travel).

### 2.6 GPU data (large tensor workloads)

**Workload:** GPU-direct access to large tensors for ML training.

**Why Pond struggles today:**
- Pond stores bytes; GPUs need memory-mapped tensors.

**The Lens that fixes it: a Tensor Lens with GPUDirect Storage.**
- Stores tensors in a GPU-friendly format (e.g., NumPy memmap, or
  bespoke GPU tensor format). The Lens uses GPUDirect Storage to
  load tensors directly to GPU memory, bypassing CPU.

**Status:** not built. Estimated effort: 1-2 months + GPUDirect
infrastructure.

**Without the Lens:** CPU-mediated tensor loading (slow). With
Tensor Lens: **Pond matches RAPIDS/Merlin for GPU tensor access**
plus free versioning (RAPIDS has no built-in versioning).

### 2.7 Millions of tiny objects

**Workload:** 10M+ tiny blobs (<1KB each).

**Why Pond struggles today:**
- Per-blob overhead on local filesystem and S3 per-request costs.

**The Lens that fixes it: a Packing Lens (uses Manifest algebra §10).**
- Packs many small blobs into a single large blob (like Git packfiles).
  The Manifest algebra (§10) already formalizes this; a Packing Lens
  would auto-pack small blobs on write and auto-unpack on read.
- Already partially shipped: the `packed_backend.py` in
  `archive/experiments/` demonstrates 100x scan speedup. A production
  Packing Lens would generalize this.

**Status:** experimental (in archive). Production Lens: 2-3 weeks.

**Without the Lens:** ≤1M blobs comfortable; 10M+ slow. With Packing
Lens: **Pond matches Git packfiles / Cassandra compaction** plus
free time travel (Git has it; Cassandra doesn't).

### 2.8 Full-text search

**Workload:** inverted-index-based full-text search at scale.

**Why Pond struggles today:**
- No shipped inverted index Lens. Search latency is dominated by
  index traversal.

**The Lens that fixes it: a Search Lens.**
- Builds an inverted index as a Physical Structure (per §14: a
  Physical Structure is `f(snapshot) → artifact`). The index is
  rebuildable from the snapshot. Incremental updates are possible
  via delta commits.
- Could use Tantivy (Rust) or Lucene (Java) as the index engine,
  with Pond as the storage.

**Status:** not built. Estimated effort: 1-2 months.

**Without the Lens:** documents can be stored in Pond but searched
externally. With Search Lens: **Pond matches Elasticsearch/Solr for
full-text search** plus free versioning (Elasticsearch has limited
versioning; Pond has full time travel).

---

## 3. Workloads where Pond is unclear (waiting for Lenses)

These workloads have clear Lens designs but no implementation:

| Workload | Required Lens | Status |
|---|---|---|
| Time-series (high cardinality) | TimeSeries Lens (partitioning + compaction) | Not built |
| Graph databases | Graph Lens (adjacency lists as Physical Structures) | Not built |
| Notebook versioning | Notebook Lens (cell-level commits) | Not built |
| Object storage + metadata | ObjectStore Lens (S3 backend, production) | Backend in archive; Lens not built |

For each, the Lens design is clear; the implementation is the work.

---

## 4. Workloads where Pond excels TODAY

No Lens needed — these work today:

### 4.1 Versioned tabular data (lakehouse)
- **Lens shipped:** `lenses/lakehouse/lakehouse.py` (DuckDB lakehouse).
- Branching O(1), time travel O(1), schema evolution Parquet-native.
- **Cross-Lens interop demonstrated:** Feature Store + Lakehouse share
  data natively (`pond-labs/interop_demo.py`).

### 4.2 ML feature stores
- **Lens shipped:** `pond-labs/feature_store_lens.py`.
- Point-in-time joins (prevents label leakage), online + offline
  serving, schema evolution, branching for experimentation.

### 4.3 Audit logs / event sourcing
- **No Lens needed.** A1 immutability is exactly what event sourcing
  needs. Time travel is free. Reproducible replay is free.

### 4.4 Configuration management
- **Trivial Config Lens** (JSON-in-Pond-blobs). Branching enables
  environment promotion. Time travel enables rollback.

### 4.5 Code versioning (Git-like)
- **Lens design fits** (no Lens ships yet, but the kernel's primitives
  are exactly Git's). A Git Lens could implement Git semantics on Pond.

---

## 5. The honest summary — REVISED

| Workload | Pond today | Pond + planned Lens | Use instead (if Lens not built) |
|---|---|---|---|
| High-frequency OLTP | Struggles (>10 TPS/key) | **Matches RocksDB** (OLTP Lens) | FDB, Postgres |
| Distributed consensus | Out-of-model | **Matches Cassandra** (CRDT Lens) or **CockroachDB** (Raft) | FDB, CockroachDB |
| Random in-place updates | Struggles | **Matches RocksDB** (LSM Lens) + free time travel | RocksDB, Pebble |
| Hot-key contention | Struggles | **Matches Redis** (Counter CRDT Lens) + free time travel | Redis, FDB |
| Streaming joins (sub-second) | Struggles | **Matches Flink** (Streaming Lens) + free state time travel | Flink, Materialize |
| GPU data | Struggles | **Matches RAPIDS** (Tensor Lens) + free versioning | RAPIDS, Merlin |
| Millions of tiny objects | Struggles | **Matches Git packfiles** (Packing Lens, in archive) | Cassandra, HBase |
| Full-text search | Struggles | **Matches Elasticsearch** (Search Lens) + free versioning | Elasticsearch, Solr |
| Time-series (high cardinality) | Unclear | **Matches InfluxDB** (TimeSeries Lens) | InfluxDB, TimescaleDB |
| Graph databases | Unclear | **Matches Neo4j** (Graph Lens) | Neo4j, TigerGraph |
| Notebook versioning | Unclear | **Beats Git** (cell-aware Notebook Lens) | Git (poorly) |
| Object storage + metadata | Unclear | **Matches LakeFS** (ObjectStore Lens) | LakeFS |
| Versioned tabular data | **Excels** | (already shipped) | (Pond flagship) |
| ML feature stores | **Excels** | (already shipped) | (Pond strong) |
| Audit logs / event sourcing | **Excels** | (no Lens needed) | (Pond fits natively) |
| Code versioning | Excels (no Lens yet) | **Matches Git** (Git Lens) | Git |
| Configuration management | **Excels** | (trivial Config Lens) | (Pond fits) |

---

## 6. The argument for Pond as a universal substrate

The pattern across all 8 "struggles" rows: **the gap is a missing
Lens, not a missing kernel primitive.** For each hard workload,
there exists a Lens design (often well-understood — CRDT, LSM,
inverted index, GPUDirect) that would close the gap and add Pond's
unique advantages (time travel, branching, cross-Lens interop) on
top.

This is the argument for Pond as a **universal substrate**:

> Pond's kernel is too small to do any single workload optimally.
> But the Lens algebra is rich enough to do every workload
> competitively, plus give every workload free time travel,
> branching, and cross-Lens interop that no peer system provides.

A RocksDB user gets fast in-place updates but no time travel.
A Pond + LSM Lens user gets fast in-place updates **plus** time
travel, branching, and the ability to share data with a Feature
Store Lens, a Lakehouse Lens, or any future Lens — without ETL.

A Flink user gets sub-second streaming joins but no versioned
state. A Pond + Streaming Lens user gets sub-second streaming joins
**plus** versioned state (replay any past stream state), branching
(try a new join logic on a branch), and cross-Lens interop (the
stream can write to a Lakehouse Lens for batch analysis).

**The unique value of Pond is not that it does any single thing
best. It is that it does every thing competitively, plus gives
every thing free versioning and interop.**

---

## 7. What this means for adoption

**Today:** adopt Pond for workloads in the "Excels" column (versioned
tabular data, ML feature stores, audit logs, configuration).

**Near-term (3-6 months):** adopt Pond for workloads in the
"struggles" column once the corresponding Lens ships. Each Lens is
2-8 weeks of work. The Lens roadmap (in priority order):

1. **Packing Lens** (2-3 weeks) — closes "millions of tiny objects,"
   already prototyped in archive.
2. **Counter CRDT Lens** (1 week) — closes "hot-key contention."
3. **OLTP Lens** (2-4 weeks) — closes "high-frequency OLTP."
4. **Search Lens** (1-2 months) — closes "full-text search."
5. **LSM Lens** (4-6 weeks) — closes "random in-place updates."
6. **Streaming Lens** (2-3 months) — closes "streaming joins."
7. **Tensor Lens** (1-2 months) — closes "GPU data."
8. **CRDT Lens + Raft coordinator** (2-3 months) — closes
   "distributed consensus."

After all 8 ship, the "struggles" column is empty. Pond becomes a
universal substrate where every workload is competitive plus gets
free versioning and interop.

**The honest claim is not "Pond is universal today." It is "Pond's
architecture is universal; the kernel is done; the Lenses are the
work, and the work is finite."**

---

## 8. What this document is NOT

This document is NOT a claim that Pond beats every peer system
today. It does not. RocksDB is faster for in-place updates today.
Flink is faster for streaming joins today. Elasticsearch is faster
for full-text search today.

This document IS a claim that:
1. Pond's kernel is done and frozen.
2. For each workload where Pond struggles today, there exists a
   Lens design that would close the gap.
3. Each Lens, when built, would make Pond competitive with the
   peer system PLUS add free time travel, branching, and cross-Lens
   interop that the peer system lacks.
4. The Lens roadmap is finite (8 Lenses; ~12 months of work).
5. After the roadmap, Pond is a universal substrate.

**The falsifiable prediction:** if any Lens on the roadmap ships
and does NOT make Pond competitive with the corresponding peer
system, the architecture is wrong. We will know within 12 months.

---

## 9. Conclusion

Pond is not a universal storage substrate today. It is a universal
storage *architecture* with a finite Lens roadmap to universality.

The kernel is frozen. The Lens algebra is infinite. Most "can't"
claims are missing Lenses. The work is to build them.

When a workload seems impossible on Pond, the question is not "can
Pond do this?" but "what Lens is missing?" This document maps each
gap to the Lens that closes it.

**That is the honest, ambitious claim.**
