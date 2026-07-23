# Where Pond Fails

> An honest catalog of workloads where Pond is the wrong tool.
>
> The goal of this document is **not** to defend Pond. It is to
> falsify the claim that "Pond is a universal storage substrate."
> Pond is not universal. It is good for a specific class of
> workloads and bad for others. This document maps both.
>
> **If your workload is in the "Pond fails" column, do not use Pond.**
> Use the recommended alternative.

---

## 1. The pattern of failure

Pond's design choices (immutable bytes, last-writer-wins refs,
Lamport clocks, no in-kernel consensus) make it excellent for
**append-mostly, version-heavy, read-mostly** workloads and
terrible for **high-frequency mutation, strong consistency, or
hot-key** workloads.

The failure pattern is consistent: any workload that requires
*in-place updates*, *distributed consensus*, or *per-key
serializability* will perform badly on Pond.

---

## 2. Workloads where Pond fundamentally fails

### 2.1 High-frequency OLTP

**Workload:** per-key transactional updates at high QPS (e.g.,
bank account balances, inventory counts, session state).

**Why Pond fails:**
- Each update creates a new blob (A1 immutability). The
  namespace ref is updated LWW (A3). There is no in-place update.
- Per-key serialization requires either:
  - Optimistic concurrency via CAS (R3'), which the kernel
    doesn't expose natively (CC2: conditional on backend).
  - A coordinator substrate (A7: out-of-model).
- At 1000 TPS on a single key, Pond creates 1000 blobs/sec and
  1000 ref updates/sec. Each ref update is a SQLite write (or S3
  PUT). This is 10-100x slower than an OLTP engine that updates
  in place.

**Use instead:** FoundationDB, Postgres, MySQL, CockroachDB,
Spanner. Any system with in-place updates and per-key
serializability.

**Pond's honest bound:** Pond is suitable for OLTP up to ~10 TPS
per key (single-writer, optimistic concurrency). Beyond that, use
a real OLTP database.

### 2.2 Distributed consensus / multi-writer convergence

**Workload:** multiple writers in different regions writing to
the same key, with automatic conflict resolution.

**Why Pond fails:**
- A7 (coordinator out-of-model): the kernel provides no Raft,
  Paxos, or 2PC. Multi-writer convergence is explicitly out-of-model.
- REP8 (no multi-writer convergence): if two regions write to
  the same ref, the kernel applies LWW. The model does not define
  a merge.
- Conflict resolution must be done at the application level, which
  defeats the purpose of using a storage substrate.

**Use instead:** FoundationDB, CockroachDB, Spanner, Cassandra
(with LWT), DynamoDB (with transactions).

**Pond's honest bound:** Pond is suitable for single-writer-per-ref
workloads (REP1). For multi-writer, layer a coordinator (the
`TwoPhaseCommitCoordinator` in `pond-replication/` is a reference,
not production). If you need true multi-writer convergence, use a
system that has it built-in.

### 2.3 Random in-place updates at scale

**Workload:** updating individual rows in a 1B-row table (e.g.,
user profile updates, IoT sensor calibration).

**Why Pond fails:**
- Each update creates a new version of the entire table (or, with
  the TabularLens, a new delta). At 1M updates/day on a 1B-row
  table, this is 1M new blobs/day. GC pressure is enormous.
- In-place updates require either:
  - Copy-on-write at the row level (Pond's Lens could do this,
    but no Lens ships with row-level COW).
  - A log-structured merge tree (Pond has no LSM; that's a
    Pebble/RocksDB concern).
- The model's immutability axiom (A1) makes in-place updates
  fundamentally a new write, not a modification.

**Use instead:** RocksDB, Pebble, Cassandra, HBase, any LSM-based
system.

**Pond's honest bound:** Pond is suitable for read-heavy or
append-heavy workloads. For update-heavy workloads, the immutability
tax dominates. Use an LSM engine.

### 2.4 Hot-key contention

**Workload:** many concurrent writers to the same key (e.g.,
counter increments, popularity leaderboards).

**Why Pond fails:**
- Each write to a hot key creates a new blob. LWW means only one
  write survives. The others are lost.
- Optimistic concurrency (CAS via R3') requires read-modify-write;
  under contention, this is O(contention) retries per write.
- There is no atomic increment primitive. Counter workloads require
  either:
  - A CRDT Lens (which Pond's model supports but no Lens ships).
  - An external counter service (Redis, etc.).

**Use instead:** Redis, FoundationDB (with atomic ops), Cassandra
(counters), any system with atomic increment.

**Pond's honest bound:** Pond is suitable for low-contention
workloads (≤10 concurrent writers per key). For hot keys, use a
counter service.

### 2.5 Streaming joins with low latency

**Workload:** sub-second streaming joins across multiple streams
(e.g., real-time fraud detection, ad placement).

**Why Pond fails:**
- Pond's commit model is snapshot-based. A join requires reading
  multiple Collections, which means multiple HEAD resolutions and
  multiple blob reads. Each blob read is ~0.1ms local, ~20ms on S3.
- A streaming join needs sub-100ms latency. Pond's RTT budget
  (3-5 RTTs for a lookup) is 60-100ms on S3 — already at the limit.
- Stateful stream processing requires incremental maintenance
  ( RocksDB-backed state stores in Flink). Pond has no incremental
  state maintenance.

**Use instead:** Flink, Kafka Streams, Materialize, Spark
Structured Streaming. Any system with incremental state and
low-latency joins.

**Pond's honest bound:** Pond is suitable for batch or
micro-batch workloads (minute+ latency). For sub-second streaming
joins, use a stream processor.

### 2.6 GPU data (large tensor workloads)

**Workload:** GPU-direct access to large tensors for ML training
(e.g., 100GB embedding tables for recommendation models).

**Why Pond fails:**
- Pond stores bytes; GPUs need memory-mapped tensors. The bridge
  (read blob → CPU memory → GPU memory) is slow.
- No support for GPU-aware storage (GPUDirect Storage, NVMe-oF).
- No support for partial tensor loading (a Lens could do this,
  but none ships).

**Use instead:** A system with GPUDirect Storage support
(RAPIDS, NVIDIA Merlin, bespoke GPU memory pools).

**Pond's honest bound:** Pond is suitable for ML feature storage
and small tensors. For GPU-resident tensors, use a GPU-aware
system.

### 2.7 Millions of tiny objects

**Workload:** 10M+ tiny blobs (<1KB each), e.g., per-event
storage, per-user metadata.

**Why Pond fails:**
- Each blob is a separate file (on local disk) or a separate S3
  object. 10M tiny blobs = 10M files = filesystem overhead.
- S3 charges per-request ($0.0004/GET). 10M GETs = $4000 just in
  request fees.
- The Manifest algebra (§10) mitigates this for packed blobs, but
  no Lens ships with auto-packing.

**Use instead:** A system with built-in compaction (Cassandra,
HBase) or a packfile format (Git packfiles).

**Pond's honest bound:** Pond is suitable for workloads with
<1M blobs. For 10M+ tiny objects, use a system with built-in
compaction, or build a Packing Lens (the Manifest algebra
supports this; no Lens ships).

### 2.8 Full-text search

**Workload:** inverted-index-based full-text search at scale
(e.g., Elasticsearch, Solr).

**Why Pond fails:**
- Inverted indexes are highly optimized data structures (postings
  lists, term dictionaries). Storing them as immutable blobs
  loses the in-place update capability that search engines rely on.
- Search latency is dominated by index traversal, not by storage.
  Pond's storage is fine; the index is the issue.
- The Physical Structure algebra (§14) supports search indexes as
  a category, but no Lens ships with an inverted index.

**Use instead:** Elasticsearch, Solr, Meilisearch, Typesense,
Tantivy. Any system with a purpose-built inverted index.

**Pond's honest bound:** Pond is suitable for storing the
*documents* being searched. For the *index*, use a search engine.
A Search Lens (inverted index as a Physical Structure) is
buildable but not shipped.

---

## 3. Workloads where Pond is unclear

These workloads might work on Pond, but the answer is "it depends."

### 3.1 Time-series at high cardinality

**Workload:** millions of unique series (e.g., per-container
metrics, per-device telemetry).

**Why unclear:**
- The TabularLens with partitioning could handle this.
- But no Lens ships with high-cardinality partitioning.
- The Manifest algebra supports efficient range scans, but no Lens
  ships with time-series partitioning.

**Verdict:** buildable on Pond, but no shipped Lens. Use InfluxDB,
TimescaleDB, or Prometheus until a TimeSeries Lens ships.

### 3.2 Graph databases

**Workload:** property graph queries (Cypher, Gremlin, SPARQL).

**Why unclear:**
- A Graph Lens could store nodes and edges as Parquet (or bespoke
  format) in Pond blobs.
- Adjacency lists could be Physical Structures.
- But no Graph Lens ships.

**Verdict:** buildable on Pond. Use Neo4j, TigerGraph, or
Memgraph until a Graph Lens ships.

### 3.3 Notebook versioning

**Workload:** Jupyter notebook history with cell-level branching
and merging.

**Why unclear:**
- A Notebook Lens could store notebooks as JSON in Pond blobs,
  with cell-level commit semantics.
- But no Notebook Lens ships.
- Git handles notebooks poorly (JSON diffs are messy); Pond could
  do better with a cell-aware Lens.

**Verdict:** buildable on Pond. This is a promising direction for
a future Lens.

### 3.4 Object storage with metadata

**Workload:** S3-like object storage with rich metadata and
versioning.

**Why unclear:**
- Pond's design is object-store-native (OSN).
- But the kernel's default backend is SQLite, not S3.
- The `ObjectStoreBackend` in `experiments/` demonstrates the
  design but is not production.

**Verdict:** Pond's design fits this workload. The implementation
needs the object-store backend to be production-quality. Use
LakeFS until Pond's S3 backend ships.

---

## 4. Workloads where Pond excels

For completeness, the workloads where Pond is genuinely the right
tool:

### 4.1 Versioned tabular data (lakehouse)

**Workload:** data lakehouse with branching, time travel, schema
evolution.

**Why Pond excels:**
- The Lakehouse Lens (Phase Q.4) demonstrates this.
- Branching is O(1) (a ref update).
- Time travel is O(1) (read an old commit's tree).
- Schema evolution is Parquet-native.
- Cross-Lens interop: feature stores and lakehouses share data
  (Phase Q.5 interop demo).

**Use Pond.** This is the flagship workload.

### 4.2 ML feature stores

**Workload:** versioned features with point-in-time joins,
online + offline serving.

**Why Pond excels:**
- The Feature Store Lens (pond-labs) demonstrates this.
- Point-in-time joins prevent label leakage.
- Schema evolution adds features without breaking old training data.
- Branching enables feature experimentation.
- Cross-Lens interop: features are queryable by DuckDB directly.

**Use Pond.** This is a strong workload.

### 4.3 Audit logs / event sourcing

**Workload:** append-only event logs with time travel and
reproducible replay.

**Why Pond excels:**
- A1 (immutability) is exactly what event sourcing needs.
- Time travel is free (read old commits).
- Reproducible replay is free (re-read the commit chain).
- No special Lens needed; the kernel is sufficient.

**Use Pond.**

### 4.4 Code versioning (Git-like)

**Workload:** versioned source code with branching and merging.

**Why Pond excels:**
- The kernel's primitives (Write, Read, Ref) are exactly Git's.
- A Git Lens could implement Git semantics on Pond (no Lens ships,
  but the design fits).

**Verdict:** buildable on Pond. Git itself is more mature; use
Git until a Pond Git Lens ships.

### 4.5 Configuration management

**Workload:** versioned configuration with branching (dev/staging/prod).

**Why Pond excels:**
- Config is small, append-mostly, version-heavy.
- Branching enables environment promotion.
- Time travel enables rollback.

**Use Pond.** Build a Config Lens (trivial; just JSON-in-Pond-blobs).

---

## 5. The honest summary

| Workload | Pond verdict | Use instead |
|---|---|---|
| High-frequency OLTP | **Fails** | FDB, Postgres, CockroachDB |
| Distributed consensus | **Fails** | FDB, CockroachDB, Spanner |
| Random in-place updates | **Fails** | RocksDB, Pebble, Cassandra |
| Hot-key contention | **Fails** | Redis, FDB |
| Streaming joins (sub-second) | **Fails** | Flink, Materialize |
| GPU data | **Fails** | RAPIDS, Merlin |
| Millions of tiny objects | **Fails** | Cassandra, HBase |
| Full-text search | **Fails** | Elasticsearch, Solr |
| Time-series (high cardinality) | Unclear (no Lens) | InfluxDB, TimescaleDB |
| Graph databases | Unclear (no Lens) | Neo4j, TigerGraph |
| Notebook versioning | Unclear (no Lens) | Git (poorly) |
| Object storage + metadata | Unclear (no S3 backend) | LakeFS |
| Versioned tabular data | **Excels** | (Pond flagship) |
| ML feature stores | **Excels** | (Pond strong workload) |
| Audit logs / event sourcing | **Excels** | (Pond fits natively) |
| Code versioning | Excels (no Lens yet) | Git |
| Configuration management | **Excels** | (trivial Config Lens) |

**Pond is not a universal storage substrate.** It is a specialized
substrate for **append-mostly, version-heavy, read-mostly** workloads.
For OLTP, consensus, hot keys, or in-place updates, use something else.

This document is the most important credibility exercise in the
project. **Pond's value proposition is stronger when scoped honestly
than when oversold.**

---

## 6. What this means for adoption

If your workload is in the "excels" column, try Pond.
If your workload is in the "fails" column, do not use Pond.
If your workload is in the "unclear" column, wait for a Lens.

The worst outcome for Pond would be adoption by users whose
workloads are in the "fails" column. They would be disappointed,
and their disappointment would obscure Pond's genuine strengths.

The best outcome is adoption by users whose workloads are in the
"excels" column. They would find Pond genuinely useful, and their
success would validate the architecture.

**Recommendation:** lead with this document. Don't claim Pond is
universal. Claim it is the best tool for a specific class of
workloads, and prove it.
