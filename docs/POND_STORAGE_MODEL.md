# The Pond Storage Model

**A minimal immutable object graph with universal history, upon which
multiple semantic interpretations and deterministic physical structures
coexist without changing the underlying data.**

---

## Abstract

Existing storage systems — lakehouses (Delta Lake, Apache Iceberg, Apache
Hudi), databases (PostgreSQL, FoundationDB), version control (Git), and
object stores (S3) — each impose a single interpretation on the data they
store. When multiple workloads need to access the same data, they either
duplicate it (online + offline stores in feature stores), translate it
(Apache XTable writes format-specific metadata), or accept format lock-in
(Iceberg tables can only be read by Iceberg readers).

Pond proposes a different model: the storage substrate stores only
immutable bytes with a commit DAG and mutable name references. Everything
above — SQL tables, Git repositories, notebooks, feature stores, vector
indexes — is a Lens: an interpretation layer that reads and writes bytes
without owning them. Multiple Lenses share the same byte graph. No
translation metadata is written. No format lock-in occurs.

This paper formalizes the Pond Storage Model, proves its properties
through executable architecture laws, and compares it against existing
systems to show where it is stronger, where it is weaker, and where it
represents a genuinely different way of thinking about storage.

---

## Table of Contents

1. [The Problem: Metadata Duplication](#1-the-problem-metadata-duplication)
2. [The Kernel: Three Primitives](#2-the-kernel-three-primitives)
3. [Collections: Named Objects](#3-collections-named-objects)
4. [Lenses: Interpretation, Not Ownership](#4-lenses-interpretation-not-ownership)
5. [Physical Structures: Acceleration Without Authority](#5-physical-structures-acceleration-without-authority)
6. [Branches and History](#6-branches-and-history)
7. [Cross-Lens Interoperability](#7-cross-lens-interoperability)
8. [Why Bytes Remain Immutable](#8-why-bytes-remain-immutable)
9. [Why No Translation Metadata Is Required](#9-why-no-translation-metadata-is-required)
10. [Architecture Laws](#10-architecture-laws)
11. [Comparison with Existing Systems](#11-comparison-with-existing-systems)
12. [Where Pond Fails](#12-where-pond-fails)
13. [Open Questions](#13-open-questions)

---

## 1. The Problem: Metadata Duplication

Modern data systems suffer from a recurring problem: the same data must
be readable by multiple systems, each with its own format.

**Feature stores** (Feast, Tecton, Hopsworks) maintain separate online
stores (Redis, DynamoDB) and offline stores (Parquet, BigQuery). An ETL
pipeline synchronizes between them. Data is duplicated.

**Lakehouse format translation** (Apache XTable, Delta Uniform) writes
metadata for each format: a Delta table gets Iceberg manifest files,
Hudi metadata, and catalog entries. Every write triggers metadata writes
for every supported format. Overhead grows linearly with format count.

**Multi-engine analytics** (DuckDB reading Iceberg, Spark reading Delta,
Trino reading Hudi) requires each engine to understand each format's
metadata. Adding a new format requires modifying every engine.

The root cause is the same in all cases: **the storage format is coupled
to the interpretation.** If you store data as Delta, you can only read
it as Delta (unless you write translation metadata). The format IS the
interpretation.

Pond separates them completely.

---

## 2. The Kernel: Three Primitives

The Pond kernel owns exactly three things:

| Primitive | Signature | Description |
|---|---|---|
| **Write** | `Write(bytes) → hash` | Create an immutable, content-addressed blob. SHA-256 of the bytes is the address. Same bytes → same hash → dedup for free. |
| **Read** | `Read(hash \| name) → bytes` | Fetch a blob by its hash, or resolve a name to a hash then fetch. |
| **Reference** | `Reference(name, hash)` | Set a mutable name → hash mapping. The ONLY mutation in the kernel. |

**Nothing else.** No codec IDs. No envelopes. No manifests. No type tags.
No schema registry. No format awareness. No compression. No caching.
No query planner. No scheduler. No transactions. No replication.

The kernel is ~140 lines of code. It has been frozen since the architecture
stabilized. Every higher-level capability is built on top of these three
primitives, without modification.

### Why three?

We proved (RFC-0003, FORMAL_ALGEBRA.md) that three is the lower bound:

- **Without Write**: no data exists.
- **Without Read**: data exists but is inaccessible.
- **Without Reference**: data is accessible by hash only — no mutable
  state, no branching, no history, no "current version." The system is
  a static content-addressed store (like IPFS without IPNS), not a database.

Any two-primitive merge fails to express either immutability, addressability,
or name-mutability. Three is both necessary and sufficient.

### What the kernel does NOT know

The kernel does not know:
- What format the bytes are in (JSON, Arrow, Git tree, JPEG, MP4, raw binary)
- What the bytes mean (SQL rows, notebook cells, feature vectors, commit objects)
- How many Lenses are reading the bytes
- Whether a blob is data, a tree node, a commit, or an index

This ignorance is the kernel's strength. It means:
- Any format can be stored without kernel modification
- Any Lens can be added without kernel modification
- The kernel never needs to be "updated to support format X"
- Cross-format interoperability is a Lens concern, not a kernel concern

---

## 3. Collections: Named Objects

A **Collection** is a named object in the kernel — like a table in a
database, a repository in Git, or a notebook in Jupyter. It has:

- A **name** (possibly namespaced: `analytics/orders`, `ml/features/stats`)
- A **type** (which Lens family created it: "sql", "git", "feature_store")
- A **description** (human-readable)
- An optional **source** (parent collection, for materialized views)
- A **commit DAG** (history, branches, snapshots)
- Zero or more **physical structures** (indexes, statistics)

The metadata is ONE small JSON blob per collection, stored as a kernel
reference (`{name}__meta`). NOT per record. NOT per blob. The blob bytes
stay pure.

```python
Collection.create(kernel, "analytics/orders", type="sql",
                   description="Customer orders table")
```

Collections live in a **namespace hierarchy** (using `/` as a path
separator, like a filesystem):

```
analytics/orders          ← SQL collection
analytics/customers       ← SQL collection
ml/features/user_stats    ← Feature store collection
repo/main                 ← Git collection
notebooks/analysis        ← Notebook collection
```

`Collection.list(kernel)` shows all collections with their types — like
listing tables in a database. `Collection.list(kernel, prefix="analytics/")`
filters by namespace.

### Materialized views

A materialized view is a Collection with a `source` field pointing to
its parent. No special API — just pass `source` when creating:

```python
Collection.create(kernel, "analytics/orders_by_region",
                   type="sql", source="analytics/orders")
```

This records lineage (orders_by_region ← orders) without any special
machinery. The materialized view has its own commit DAG; it's a separate
collection that tracks its provenance.

---

## 4. Lenses: Interpretation, Not Ownership

A **Lens** is an interpretation layer over immutable bytes. It encodes
data into bytes (for writing) and decodes bytes into data (for reading).
The encoding is the Lens's choice. The bytes belong to the kernel.

### Key properties

| Property | Rule |
|---|---|
| **Interprets** | A Lens encodes/decodes bytes. It chooses the format (JSON, Arrow, Git tree, CSV, raw). |
| **Never owns** | The bytes belong to the kernel. The Lens is a translation layer. |
| **Never modifies** | A Lens may interpret bytes during reading. It may never modify stored bytes. |
| **Shares** | Multiple Lenses with the same Collection name share the same byte graph. |
| **Reads any** | Any Lens can read any blob via `get_raw(key)` — raw bytes, no interpretation. |

### Context-based interpretation

The interpretation comes from the **key prefix** (context), not from the
blob itself. Keys like `sql/user:1`, `git/tree:main`, `arrow/orders_table`
carry a prefix that tells the Resolver which codec to use.

This is like Git: Git knows whether it's requesting a blob, tree, commit,
or tag from the context (which command asked, which reference it resolved).
The object itself doesn't carry its type.

A **Resolver** (code, not data) maps key prefixes to codecs. It lives in
the application, not in the kernel. Different deployments can have
different Resolvers. No global registry. No Pond Binary Format.

### What this means

- **No format lock-in**: the same bytes can be read by any Lens that
  knows the format. A SQL Lens and a Notebook Lens can both read JSON
  data (if both registered the JSON codec).
- **No translation metadata**: no manifest, no enable_view, no sidecar
  files. The "enablement" is in the code (having a Lens instance with
  the right Resolver), not in the data.
- **No duplication**: one copy of bytes serves all Lenses. Content-
  addressed dedup is free.
- **Cross-lens transforms**: read via Lens A (decoded), transform in
  application code, write via Lens B (re-encoded). Both versions coexist
  in the same byte graph.

---

## 5. Physical Structures: Acceleration Without Authority

Physical structures accelerate access. They never own data. They are
deterministic functions of a snapshot — given the same state, they
always produce the same output.

| Structure | Purpose |
|---|---|
| Secondary indexes | O(log N) lookup by non-primary-key field |
| Bloom filters | Skip unnecessary reads |
| Zone maps | Skip irrelevant chunks |
| Statistics | Query optimization |
| Caches | Reduce latency |
| Histograms | Cost-based optimization |

All are **rebuildable from the base data**. Deleting every physical
structure must never change the reconstructed dataset. Rebuilding a
physical structure from the same state always produces the same hash.

This is formalized as Architecture Law 5 (Derived Law): deleting all
physical structures never changes the dataset.

### Incremental updates

Physical structures support incremental updates (not just full rebuilds).
The `IndexedLens` class tracks pending additions and applies them via
`_incremental_update_index()`, which is 15× faster than a full rebuild
(measured: 4.29ms vs 66ms for 5K records).

---

## 6. Branches and History

The commit DAG is a linked list of commits, each pointing to its parent.
A commit is either:

- A **snapshot commit**: contains a full Prolly tree root (the complete
  state at that point). O(log N) lookup.
- A **delta commit**: contains only the changes (additions and deletions)
  since the parent. O(1) write.

The `COMPACTION_THRESHOLD` (default 4) controls when a snapshot is
written instead of a delta. After every 4 delta commits, the next
commit is a snapshot (full state).

### Branching

A branch is just a kernel Reference. `branch("experiment")` creates a
new name (`{collection}__branch__experiment`) pointing to the current
HEAD. This is O(1) — no data is copied. Measured: 0.04ms.

### Merging

Merge is union with last-writer-wins semantics. The merged branch's
values override the current branch's values for matching keys. This is
the simplest correct merge policy. A future RFC may introduce 3-way
merge with conflict detection.

### History

History is a linear walk of the commit DAG from HEAD backwards. Each
step is O(1) (follow the parent pointer). History is shared by all
Lenses on the same Collection — if Lens A commits, Lens B sees the
commit in its history.

---

## 7. Cross-Lens Interoperability

This is Pond's defining feature. Multiple Lenses share the same byte
graph. Each reads what it can; the rest is accessible as raw bytes.

### Verified patterns (14 tests, all pass)

| Pattern | Description |
|---|---|
| Cross-lens writing | Multiple Lenses write to the same byte graph |
| Cross-lens reading | Any Lens reads any blob (native decode or raw bytes) |
| Cross-lens branching | Lens A branches, Lens B commits on it |
| Cross-lens merging | Lens A merges Lens B's branch |
| Cross-lens indexing | Index over data from multiple Lens sources |
| Transform-later | Read via Lens A, transform, write via Lens B |
| Restart | All Lenses' data survives process restart |
| Namespace patterns | Multiple Collections in different namespaces |
| Materialized views | Source lineage tracking |
| Independent impls | ConfigLens + MetricsLens (built separately) coexist |
| Cross-lens history | All Lenses see the same commit DAG |
| Cross-lens count | All Lenses see the same key set |
| Delete visibility | Lens A deletes, Lens B sees the deletion |
| Unstructured data | JSON + JPEG + MP4 in the same byte graph |

### How it works without metadata

The key insight: **the interpretation layer lives in code, not in data.**

Each Lens registers its codec with a Resolver (a code-level construct).
The Resolver maps key prefixes to codecs. When any Lens reads a blob,
the Resolver uses the key prefix to determine which codec to use. No
metadata is written to the blob. No manifest is maintained. No
enable_view is needed.

This is fundamentally different from:
- **Apache XTable**: writes format-specific metadata for each format
- **Delta Uniform**: writes sidecar metadata files
- **Feature stores**: maintain separate online + offline stores

Pond writes ZERO extra metadata. The "enablement" is having a Lens
instance with the right Resolver — which is code, not data.

---

## 8. Why Bytes Remain Immutable

Immutability is the foundation. Once a blob is written, its contents
never change. This is Architecture Law 1 (Identity Law).

### Benefits

- **Content-addressed dedup**: same bytes → same hash → one copy.
  Measured: 100 identical records → 5 blobs (vs 100 without dedup).
- **Verifiable integrity**: the hash IS the address. Tampering changes
  the hash, breaking the reference.
- **Structural sharing**: two versions of a Collection share all
  unchanged blobs. Only the delta is new.
- **Crash safety**: committed data is never modified, so a crash
  never corrupts existing data. Verified by 8 crash tests.
- **Time travel**: history is a walk of the commit DAG. Old versions
  are always readable (their blobs are never deleted).

### What about updates?

Updates are new writes. `put("key", new_data)` writes a new blob and
updates the Reference to point to it. The old blob is still there
(until GC). The Reference mutation is the only change.

### What about deletes?

Deletes are data, not a kernel primitive (RFC-0008). A delete is
expressed as `Reference(name, TOMBSTONE_HASH)` — rebinding the name to
a special marker hash. The previously-pointed-to blob becomes
unreachable and is swept by PondGC on the next collection.

This is the same pattern as Git (removing a ref → `git gc` reclaims the
orphaned objects) and PostgreSQL (`DROP TABLE` → `VACUUM` reclaims).

---

## 9. Why No Translation Metadata Is Required

This is the key differentiator from existing systems.

### The XTable / Delta Uniform approach

```
Write data as Delta
  → write Delta metadata
  → write Iceberg manifest files
  → write Hudi metadata
  → write catalog entries
  → every format gets its own metadata
```

Overhead: O(F) metadata writes per data write, where F = number of
supported formats. Adding a format requires modifying the writer.

### The Pond approach

```
Write data as bytes (any format)
  → done.
```

No metadata. No manifests. No sidecar files. No catalog entries.

The "translation" happens at read time: the Resolver (code) uses the
key prefix to determine which codec to use. If the codec is registered,
the blob is decoded. If not, the raw bytes are returned (transform-later).

### Why this works

The key insight is that **format information is contextual, not
intrinsic.** A blob of JSON bytes is JSON because a Lens chose to
encode it as JSON — not because the blob carries a "format=json" tag.
The key prefix (`json/user:1`) provides the context. The Resolver
uses the context to choose the codec.

This is the same principle as Unix: a file's format is determined by
the application that reads it, not by the file itself. The `file`
command sniffs magic bytes, but the filesystem doesn't enforce that
a `.py` file is Python. Python reads it because Python chose to.

### Measured evidence

The falsification test (`experiments/resolver_comparison/falsification_context.py`)
proved that context-based interpretation provides all 8 capabilities
(universal readability, bidirectional write/read, branch/merge/history,
derived structures, zero metadata, pure bytes, transform-later, kernel
purity) without any blob-level metadata.

---

## 10. Architecture Laws

Pond's executable specification. If any law fails, the architecture
is violated. These are NOT unit tests — they are architectural truths
encoded as executable checks.

| Law | Statement |
|---|---|
| 1. Identity | Once a blob hash exists, its contents never change. |
| 2. Reachability | Every committed reference resolves to exactly one blob. |
| 3. History | Replaying history reconstructs the same snapshot. |
| 4. Lens | A Lens may interpret bytes; it may never modify them during reading. |
| 5. Derived | Deleting all physical structures never changes the dataset. |
| 6. Branch | Branch creation never duplicates blobs. |
| 7. Merge | Merge changes references, not blob contents. |
| 8. Determinism | Same writes, same ordering, same blob hashes. |
| 9. Scale | At scale (10K+), count equals the number written. |
| 10. Index | Index rebuild at scale succeeds without errors. |

All 10 laws pass. They run on every CI commit.

---

## 11. Comparison with Existing Systems

### vs. Git

| Dimension | Git | Pond |
|---|---|---|
| Primitives | blob, tree, commit, tag (4 object types) | Write, Read, Reference (3 primitives) |
| Format awareness | Kernel knows object types | Kernel is format-agnostic |
| Cross-domain | Git only | Any Lens (SQL, Git, Notebook, etc.) |
| Branching | O(1) ref creation | O(1) ref creation (same) |
| History | Linked list of commits | Linked list of commits (same) |
| Merge | 3-way merge | Union, last-writer-wins (simpler) |
| Multiple domains | No (Git is Git-only) | Yes (any Lens can share the byte graph) |

**Where Git wins:** mature tooling, distributed protocol, 20 years of
optimization.

**Where Pond wins:** multi-domain (Git + SQL + Notebook on the same
bytes), format-agnostic kernel, no object-type coupling.

### vs. Delta Lake / Iceberg / Hudi

| Dimension | Lakehouse formats | Pond |
|---|---|---|
| Metadata | Per-format metadata (manifests, logs, etc.) | Zero metadata (context-based interpretation) |
| Multi-format | Requires XTable/Uniform (translation metadata) | Native (any Lens reads any blob) |
| Schema | Enforced by the format | Enforced by the Lens (schema validation) |
| Time travel | Format-specific | Universal (commit DAG) |
| Branching | LakeFS/Project Nessie (external) | Native (kernel Reference) |
| Storage overhead | ~10-20% metadata | ~48% (Prolly tree + commit structure) |

**Where lakehouse formats win:** production maturity, ecosystem (Spark,
Trino, Snowflake integration), optimized file formats (Parquet columnar).

**Where Pond wins:** zero translation metadata, native branching,
multi-domain (not just tabular), format-agnostic kernel.

### vs. FoundationDB

| Dimension | FoundationDB | Pond |
|---|---|---|
| Primitives | get, set, clear, range read | Write, Read, Reference |
| Data model | Key-value (ordered) | Content-addressed blobs + names |
| Transactions | ACID, serializable | None (single-key, last-writer-wins) |
| Distribution | Native (Raft-based) | None (single-node) |
| Performance | ~100K+ ops/sec | ~18K rec/sec write, 0.1ms lookup |
| History | No (overwrite in place) | Yes (commit DAG) |

**Where FoundationDB wins:** distributed coordination, ACID transactions,
ordered range scans, production scale (Apple, Snowflake).

**Where Pond wins:** immutable history (time travel), content-addressed
dedup, multi-domain Lenses, no format lock-in.

### vs. DuckDB

| Dimension | DuckDB | Pond |
|---|---|---|
| Role | In-process analytical database | Storage substrate |
| Data format | Arrow (in-memory), Parquet (on-disk) | Any (Lens chooses) |
| Query engine | Full SQL optimizer | None (Lens provides query) |
| Storage | Owns the data files | Stores bytes, Lens interprets |

**Where DuckDB wins:** SQL optimization, vectorized execution, in-process
analytics.

**Where Pond wins:** DuckDB can READ Pond data via the ArrowLens (zero-
copy Arrow IPC). Pond provides the storage + versioning; DuckDB provides
the query engine. They are complementary, not competitive.

### vs. Datomic

| Dimension | Datomic | Pond |
|---|---|---|
| Philosophy | Immutable facts, time travel | Immutable bytes, time travel |
| Data model | Entity-Attribute-Value triples | Content-addressed blobs |
| Query | Datalog | None (Lens provides) |
| Storage | Any KV store (pluggable) | Content-addressed object store |

**Similarity:** both are immutable, time-traveling, and separate storage
from query. Datomic is the closest philosophical cousin to Pond.

**Difference:** Datomic is EAV-triples (structured); Pond is raw bytes
(unstructured). Datomic has a query language; Pond delegates to Lenses.

---

## 12. Where Pond Fails

Honest assessment of limitations.

### No distributed coordination
Pond is single-node. No Raft, no Paxos, no replication. Multi-writer
coordination requires external tooling. This is a deliberate choice —
the question "what is replicated?" must be answered before choosing a
mechanism.

### No ACID transactions
References are single-key, last-writer-wins. No multi-key atomicity.
Cross-collection operations are not transactional. A future
Transaction/Workspace layer could provide this.

### Filesystem backend limits
The current filesystem backend creates one file per blob. At ~600K
records, it hits disk space limits (~2.6GB). A SQLite or packed backend
would handle millions. This is an engineering issue, not an architecture
issue — the backend is replaceable.

### Lookup latency at scale
Point lookup is 0.1ms at 10K records but 14.8ms at 500K. The delta
journal walk + Prolly tree traversal grows with history depth. A
snapshot-on-every-commit strategy (or skip pointers) would keep lookups
O(log N) regardless of history depth.

### No streaming ingestion
Pond is batch-oriented. Streaming (sub-second ingestion) requires a
different commit strategy (append-only logs, not full-state snapshots).
A StreamingLens could address this.

### Merge is naive
Union with last-writer-wins. No conflict detection, no 3-way merge.
For workloads with concurrent edits to the same keys, this is
insufficient.

### No query engine
Pond stores and retrieves bytes. It does not execute SQL, Datalog, or
any query language. Query is the Lens's (or the application's) job.
This is deliberate — Pond is a storage substrate, not a database.

### Staging belongs to Lens, not a separate layer
Each Lens instance has its own staging area. Cross-lens atomic writes
are not possible. A Transaction/Workspace layer (not yet built) would
solve this.

---

## 13. Open Questions

### Should staging belong to a Workspace/Transaction layer?
Currently, each Lens stages writes independently. If a NotebookLens
and a SQLLens both want to write atomically to the same Collection,
they can't. A Workspace layer (owning the staging area, shared across
Lenses) would solve this. This is the most important missing
abstraction.

### Should the Lens hierarchy be inverted?
Currently: Kernel → Collection → Physical Structures → Lens → Application.
But Lens is interpretation — it shouldn't own data. Perhaps:
Kernel → Collection → Physical Structures → (Workspace →) Lens → Application.
The Lens becomes the top layer (how applications interact), not the
middle layer (how data is stored).

### Should Namespace become a first-class object?
Currently, namespaces are just path prefixes in Collection names. A
real Namespace would own permissions, policies, defaults, branching
configuration, and quotas. This is likely necessary for multi-tenant
deployments.

### What should be replicated?
Before choosing Raft/CRDT/external coordination, the question "what
is replicated?" must be answered. Options: snapshots, references,
materializations, or some combination. The answer determines the
replication strategy.

### Can every optimization be expressed as a Physical Structure?
The Physical Structure calculus (RFC-0005) proposes that every
optimization (indexes, statistics, bloom filters, zone maps,
histograms, caches, embeddings) is `f(snapshot) → stored result`.
If this holds universally, it's a significant conceptual contribution.
Pushing this idea further is the biggest remaining research opportunity.

---

## Conclusion

Pond is not a database. It is not a lakehouse. It is not a version
control system. It is a **storage model**: a formalization of how
immutable bytes, universal history, and interpretation layers compose
to serve any workload without duplication.

The kernel is 3 primitives and ~140 lines of code. It has been frozen.
Every higher-level capability — SQL tables, Git repositories, notebooks,
feature stores, vector indexes — is a Lens that interprets the same
bytes differently. No translation metadata is written. No format
lock-in occurs. No data is duplicated.

The architecture has been validated through:
- 1000 differential tests (correctness vs. reference implementation)
- 10 executable architecture laws
- 8 crash tests
- 100K/500K scale validation (zero data loss)
- 2 independent implementations (converged on the same design)
- 14 cross-lens pattern tests (all pass)
- Performance benchmarks (0.1ms lookup, 0.14ms commit, 0.04ms branch)

The remaining work is not adding features. It is proving that the
abstractions are correct under adversarial pressure, formalizing the
open questions, and eventually choosing a path for distributed
coordination — but only after answering "what is replicated?"

Pond's defining contribution is this: **immutable bytes and history
are the only universal substrate, and every higher-level capability
is simply a different Lens over that substrate.**

---

*This document is the canonical specification of the Pond Storage Model.
For implementation details, see the codebase. For formal proofs, see
RFC-0003 (Kernel Specification) and RFC-0007 (View/Lens Algebra). For
the Lens contract, see RFC-0013.*

---

## 14. Why Not Universal Schema?

A natural question: why not define one canonical metadata model that
all systems can agree on? Why not Apache Arrow? Why not Protobuf?
Why not Avro? Why not Iceberg metadata?

### The temptation

A universal schema seems elegant: define one format, everyone agrees,
no translation needed. Apache Arrow aims for this (a canonical
in-memory columnar format). Iceberg defines a universal table
metadata model. Protobuf defines a universal serialization format.

### Why Pond rejects this

**1. Universal schemas become lowest-common-denominator formats.**

Arrow is excellent for columnar analytics. But it's terrible for:
- Git trees (hierarchical, not tabular)
- Notebook cells (mixed types: code, markdown, output, attachments)
- Feature vectors (numeric arrays, not rows)
- Unstructured data (JPEG, MP4, PDF)

Forcing everything into Arrow would make non-tabular workloads
unnaturally complex. Forcing everything into JSON would make tabular
workloads inefficient. Any universal format favors some workloads
and penalizes others.

**2. Universal schemas create a coordination problem.**

If Arrow is the universal format, every new workload must wait for
Arrow to support its use case. Adding graph data to Arrow requires
a community RFC. Adding notebook cells requires another. The format
becomes a bottleneck.

Pond's approach: each Lens chooses its own format. No coordination
needed. A new workload can be added without modifying any existing
component.

**3. Universal schemas become permanent dependencies.**

If Pond standardized on Arrow, every Pond deployment would depend
on Arrow. If Arrow changes its format, Pond changes. If Arrow is
abandoned, Pond is orphaned.

Pond's approach: the kernel has no format dependency. Lenses choose
their formats independently. If Arrow is replaced by something
better, only the ArrowLens changes — the kernel and all other Lenses
are unaffected.

**4. Universal schemas conflict with the "bytes are just bytes" principle.**

The kernel stores bytes. It doesn't know what format they're in.
A universal schema would require the kernel to know the format —
violating the core principle that makes the architecture work.

### What Pond does instead

Pond allows EMERGENT compatibility. Lenses that choose the same
format (e.g., both use JSON) can read each other's data for free —
not because a universal schema was imposed, but because both
independently chose JSON. Lenses that choose different formats
coexist without interference.

This is exactly how Unix works: the filesystem doesn't mandate a
universal file format. Applications choose their formats. When two
applications happen to use the same format (e.g., both read UTF-8
text), they interoperate for free. When they don't, they coexist.

---

## 15. What Pond Does NOT Know

This is perhaps the most important section. Pond's power comes from
what it doesn't know.

### Pond does NOT know:

- SQL
- Tables
- Rows
- Columns
- Schemas
- Git
- Trees
- Commits (as a concept — the kernel stores commit-like blobs, but
  doesn't know they're "commits")
- JSON
- XML
- Arrow
- Parquet
- Images
- Videos
- Vectors
- Embeddings
- Notebooks
- Feature stores
- Indexes (the kernel stores index-like blobs, but doesn't know
  they're "indexes")
- Statistics
- Bloom filters
- Caches
- Histograms
- Compression
- Encryption
- Transactions
- Permissions
- Users
- Tenants

### Pond ONLY knows:

- **Bytes** — immutable, content-addressed blobs
- **References** — mutable name → hash mappings
- **History** — the commit DAG (which is itself just bytes + references)

### Everything else is interpretation.

This is not a limitation. This is the architecture's defining
strength. By knowing nothing about the data, the kernel:

1. Never needs to be updated for new formats
2. Never creates format-specific metadata
3. Never imposes format lock-in
4. Never becomes a bottleneck for new workloads
5. Never needs to be "extended" to support a new domain

The cost of this ignorance is that higher layers (Lenses, Physical
Structures, Applications) must provide their own interpretation. But
that cost is exactly what makes the architecture composable: each
layer adds exactly one capability, without coupling to the others.

### The Unix analogy

The Unix filesystem stores bytes. It doesn't know:
- Python (.py files)
- JPEG (.jpg files)
- ELF binaries
- SQLite databases
- tar archives

Applications interpret the bytes. When two applications agree on a
format (e.g., both read UTF-8), they interoperate. When they don't,
they coexist. The filesystem never needs to be updated to support a
new file format.

Pond is the same: the kernel stores bytes. Lenses interpret them.
When two Lenses agree on a format, they interoperate. When they
don't, they coexist. The kernel never needs to be updated to support
a new domain.

This is Pond's defining contribution: **a storage substrate that
knows nothing about what it stores, enabling universal
interoperability through interpretation rather than standardization.**

---

*End of The Pond Storage Model*
