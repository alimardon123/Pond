# Pond: A Small-Substrate Storage Kernel

### A Whitepaper with Formal Comparison to Git, Iceberg, Dolt, FoundationDB, and LakeFS

> **Status:** Draft for external review. Not a publication. Not a
> claim of correctness. An invitation to falsify.
>
> **What this paper is:** a rigorous first-principles description
> of Pond's architecture, a formal comparison to five peer systems,
> and an honest accounting of what is and is not established.
>
> **What this paper is not:** a proof that Pond is right. Pond's
> internal consistency is established (implementation matches
> model; invariants hold in a finite TLA+ model; tested behaviors
> match peer systems for specific invariants). Whether the
> architecture is *right* — competitive, adoptable, necessary — is
> the open question this paper invites reviewers to attack.

---

## Table of Contents

1. [Introduction](#1-introduction)
2. [The kernel in five minutes](#2-the-kernel-in-five-minutes)
3. [The six substrates](#3-the-six-substrates)
4. [The seventeen algebras](#4-the-seventeen-algebras)
5. [Formal comparison to peer systems](#5-formal-comparison-to-peer-systems)
6. [What Pond does NOT do](#6-what-pond-does-not-do)
7. [What is established vs. what is not](#7-what-is-established-vs-what-is-not)
8. [Open questions for reviewers](#8-open-questions-for-reviewers)
9. [Related work](#9-related-work)
10. [Conclusion](#10-conclusion)

---

## 1. Introduction

### 1.1 The problem

Modern storage systems are overbuilt. A data engineer in 2026
routinely juggles Git (for code), Iceberg (for tables), Dolt (for
versioned SQL), FoundationDB (for transactions), S3 (for blobs),
LakeFS (for lakehouse versioning), and a half-dozen other systems.
Each has its own metadata format, its own consistency model, its
own replication protocol, its own garbage collector. Each stores
its own copy of data that is, in spirit, the same immutable bytes.

The question this paper investigates: **is there a small storage
kernel from which all of these can be composed?** Not "is there a
universal database" — that has been tried and failed. Rather: is
there a minimal set of primitives that, when composed, recover the
essential behaviors of these systems without reimplementing their
complexity?

### 1.2 The hypothesis

Pond is built on a single hypothesis:

> **A small storage kernel — three operations over six substrates
> — is sufficient to compose the essential behaviors of versioned
> storage systems. All higher-level semantics (SQL, Git trees,
> Iceberg manifests, transactions, replication, compression,
> encryption, schema evolution) can be implemented as layers above
> the kernel, without modifying the kernel.**

This is a strong hypothesis. It might be false. The purpose of
this paper is to describe the architecture precisely enough that
reviewers can attack it.

### 1.3 What "small" means

The kernel exposes three operations to the user:
- `Write(bytes) → hash`
- `Read(hash) → bytes`
- `Ref(name, hash) → ()` (the only mutation)

These operations sit on six substrates (each with its own axioms):
Bytes, Names, Time, Coordination, Range-Read, and Key. The kernel
implementation is ~140 lines of Python. The honesty note
(`DESIGN_GOALS.md` §1) explains why the original "three primitives"
claim was rhetorical: the model silently depended on Time,
Coordination, Range-Read, and Key substrates without naming them.
The honest count is six substrates, three operations.

### 1.4 What this paper does not claim

This paper does **not** claim:
- That Pond is faster than peer systems. (No benchmarks vs. peers
  yet — see §7.)
- That Pond is correct. (TLA+ checks invariants over 56 reachable
  states in a finite model; this proves consistency, not
  correctness.)
- That Pond is necessary. (No lower-bound proof that fewer
  substrates wouldn't suffice.)
- That Pond is adoptable. (No production use; no external expert
  review at time of writing.)
- That Pond's Lens algebra covers all real workloads. (No flagship
  application has shipped on Pond yet.)

This paper **does** claim:
- That Pond's implementation matches its model (683 checks pass).
- That Pond's invariants hold in a finite TLA+ model.
- That Pond's tested behaviors match Git, Dolt, and Iceberg for
  the specific invariants tested.
- That Pond's algebras are implementable as libraries on the
  frozen kernel (4 engineering packages built).

These are claims of **internal consistency**, not architectural
correctness. The distinction matters.

---

## 2. The kernel in five minutes

Pond is an immutable object-store kernel built from three
operations on six substrates. Everything else — versioning,
schemas, replication, transport, indexes, lenses, views — is
implemented as layers above the kernel rather than embedded inside
it.

### 2.1 The three operations

```
Write : bytes → hash           -- create immutable blob, content-addressed
Read  : hash → bytes           -- fetch blob by hash
Ref   : name × hash → ()       -- mutable name→hash mapping (only mutation)
```

`hash = SHA-256(bytes)`. `name ∈ String`. Names form a flat
namespace; hierarchical naming is a convention, not a kernel
concept.

### 2.2 The six substrates

Each substrate has its own axioms and can be replaced
independently:

| Substrate | Axioms | Operations |
|---|---|---|
| Bytes | A1 Immutability, A2 Content-addressing | Write, Read |
| Names | A3 Last-writer-wins, A4 Referential integrity | Ref, get, list, delete |
| Time | A5 Monotonic logical clock (Lamport) | now, compare |
| Coordination | A6 Atomic commit blob, A7 Coordinator out-of-model | (commit blob pattern) |
| Range-Read | A8' Transport-layer (demoted from kernel primitive) | (folded into Transport) |
| Key | (Transport-layer; envelope encryption) | wrap, unwrap |

Substrates are coupled only where axioms require: Names reference
Bytes (A4: a Ref must point at an existing blob); Time timestamps
appear in commit blobs (which are Bytes). No other substrate
references another. This is what makes the model composable:
backends can vary per substrate.

### 2.3 The layering

```
Application
    ↓
Lens (encode/decode; schema-aware)
    ↓
Transport (compress → encrypt → checksum; per §17)
    ↓
Kernel (Write, Read, Ref — three operations, six substrates)
    ↓
Backend (local disk, S3, IPFS, FoundationDB, …)
```

The Lens sees plaintext, uncompressed bytes. The Kernel stores
opaque bytes. The Transport Layer (between Lens and Kernel) handles
compression, encryption, and checksumming. The Backend choice is
independent for each substrate.

### 2.4 What the kernel does not know

The kernel has no concept of:
- Format (JSON, Arrow, Parquet, Git tree, JPEG, …)
- Domain (SQL, Git, Notebook, Feature Store, …)
- Structure (table, row, column, tree, graph, …)
- Schema (types, fields, constraints, …)
- Optimization (index, cache, statistics, bloom filter, …)
- Coordination (transaction, lock, consensus, …)
- Policy (retention, GC, access control, …)

This is the model's defining axiom. By knowing nothing about the
data, the kernel never needs to be updated for new formats,
domains, or workloads.

---

## 3. The six substrates

### 3.1 Bytes

**Axioms:**
- **A1 (Immutability).** `∀ b. Write(b) = h ⟹ ∀ t > t₀. Read(h) = b`.
  Once written, a blob never changes.
- **A2 (Content-addressing).** `Write(b₁) = Write(b₂) ⟺ b₁ = b₂`.
  Same bytes → same hash. Dedup is free.

**Operations:** `Write(bytes) → hash`, `Read(hash) → bytes`.

**Consequences:** crash safety for committed data (A1); verifiable
integrity (A1+A2 ⟹ hash = address = checksum); time travel (A1 ⟹
old blobs never deleted while referenced); structural sharing (A2 ⟹
shared bytes share one blob).

### 3.2 Names

**Axioms:**
- **A3 (Name mutability).** `Ref(name, h)` is the only mutation.
  Last-writer-wins: `Ref(name, h₁) ; Ref(name, h₂) ⟹ resolve(name) = h₂`.
- **A4 (Referential integrity).** `Ref(name, h)` requires `h` to
  exist (i.e., `h = Write(b)` for some `b`).

**Operations:** `Ref(name, h)`, `get(name) → hash | ∅`,
`list(prefix) → [name]`, `delete(name)` (expressed as
`Ref(name, TOMBSTONE)`), `compare_and_swap(name, expected, new)`
(conditional on backend support — see §6.2).

**Consequences:** branching = creating a new name (O(1), no data
copied); HEAD = a name pointing at the latest commit; snapshot
pointer = a name pointing at the latest snapshot commit; index
pointer = a name pointing at an index tree root. All of these are
just `Ref(name, hash)`.

### 3.3 Time

**Axiom:**
- **A5 (Monotonic logical clock).** `now()` returns a timestamp
  `t`. Within a process, `now()` is monotonic. Across processes,
  the clock is only *causally* consistent: if `o₁ → o₂`
  (happens-before) then `now(o₁) < now(o₂)`. This is Lamport's
  clock.

**Operations:** `now() → t`, `compare(t₁, t₂) → order`.

**Why not wall-clock?** Wall-clock comparisons across processes
require synchronized clocks (NTP, TrueTime). Lamport clocks are
the weakest clock that supports timestamp-merge and freshness
checks. The model explicitly does **not** support wall-clock
comparisons across processes (CC4 in `POND_FORMAL_ALGEBRAS.md`
§15.4).

### 3.4 Coordination

**Axioms:**
- **A6 (Atomic commit blob).** A "commit blob" is a single blob
  listing a set of `(name, hash)` writes. Updating a single Ref
  (e.g., HEAD) to point at a commit blob is atomic (by A3). Readers
  observe either all writes or none.
- **A7 (Coordinator out-of-model).** Cross-Collection atomic
  writes, distributed transactions, and linearizable reads require
  a coordinator substrate (2PC, Raft, Paxos). The model does not
  specify one. Applications requiring these must layer a coordinator
  on top of the kernel.

**Consequences:** within-Collection atomicity is free (commit blob
+ HEAD update). Cross-Collection atomicity is **not** provided;
applications must layer a coordinator. The `TwoPhaseCommitCoordinator`
in `pond-replication/` demonstrates this is buildable, but it is
not part of the kernel.

### 3.5 Range-Read

**Axiom:**
- **A8' (Range reads are transport-layer).** Range reads are
  implemented at the Transport Layer, not the Kernel. The Kernel
  exposes only `Read(h) → b` (full blob read). The Transport Layer
  may decompose `Read` into range reads on the backend (e.g., S3
  `Range` header) for efficiency.

**Demotion note:** Earlier versions of the model listed `ReadRange`
as a kernel primitive (A8). Phase N.1 demoted it to Transport-layer
because the kernel's job is bytes-in, bytes-out; how the backend
delivers those bytes is a backend concern. The model shrinks from
4 operations to 3; the kernel implementation is unchanged.

### 3.6 Key

**Operations:** `wrap(DEK, master_key_id) → wrapped_DEK`,
`unwrap(wrapped_DEK, master_key_id) → DEK`.

The Key substrate supports envelope encryption: a master key
(stored in a KMS) wraps data encryption keys (DEKs); DEKs encrypt
blob blocks. The kernel does not hold the master key; it calls the
KMS (via the Key substrate) to unwrap DEKs. The Key substrate is
optional — a Collection may have no encryption.

---

## 4. The seventeen algebras

The model formalizes 17 algebras on top of the kernel. This
section lists them; full definitions are in
`POND_FORMAL_ALGEBRAS.md` (Parts I-IV).

### 4.1 Part I (Phase K.1 — formalization)

| Algebra | What it formalizes |
|---|---|
| Reference | Names are the only mutable state. All roles (HEAD, branch, snapshot, tag, lock) are `Ref(name, hash)` with different naming conventions. |
| Merge | Three layers: kernel topology, Lens semantics, application policy. |
| GC | Tracing GC (mark + sweep). Tombstone interaction (G5). |
| RTT Calculus | Every operation has a cost vector (GET, PUT, LIST, HEAD, RANGE, bytes). Theorems T1-T4 bound RTTs. |
| Object Store Native | One definition (consistency under object-store semantics) + 7 derived properties. |
| Physical Structure Taxonomy | 5 categories (Search, Statistics, Layout, Derived, Execution) + Cache (separate). |
| Workspace | Staging independent of Lens. W1-W5 laws. |
| History | History is a Physical Structure sourced from commit set. |

### 4.2 Part II (Phase K.3 — post-Second-Red-Team)

| Algebra | Closes | What it formalizes |
|---|---|---|
| Substrate | Hidden primitives | Promotes 5 substrates (later 6); demotes "3 primitives" to API. |
| Manifest | GC circularity | Logical vs physical reachability; MAN1 equivalence. |
| Range Read | Missing primitive | `ReadRange` (later demoted to Transport). |
| State vs Bytes | Hidden primitive | Settles: bytes primary, state derived. |
| GC with Packs | GC circularity | MARK-LR vs MARK-PR; compaction. |
| Physical Structure Dep Graph | Tautology | Collapses PS algebra to def + theorem; adds dep graph. |
| Concurrency & Consistency | Hidden primitives | 5 consistency levels (C0-C5); CAS is only primitive. |

### 4.3 Part III (Phase K.4 — post-Third-Red-Team)

| Algebra | Closes | What it formalizes |
|---|---|---|
| Replication | Operational hazards | Single-writer per Ref; tombstone barrier; failover contract. |
| Transport | Operational hazards | Compress + encrypt + checksum as one layer; block index. |
| Schema Evolution | Operational hazards | Versioned codecs; Schema Registry on Names substrate. |

### 4.4 Part IV (Phase N.1 — demotions)

| Change | Closes | What it does |
|---|---|---|
| ReadRange demotion | Phase L §3.1 | A8 → A8'; Range Read moves from Kernel to Transport. |
| CAS demotion | Phase L §3.2 | R3 → R3'; CAS is derived, not primitive. |

### 4.5 Honest assessment

17 algebras is **a lot**. The red team reviews (`POND_SECOND_RED_TEAM.md`,
`POND_THIRD_RED_TEAM.md`) collapsed several tautological algebras
(Physical Structure properties → one definition; OSN1-OSN8 → one
definition + 7 derived). The current count is honest, but a future
red team might collapse more. The model is **not** minimal in any
proven sense; it is the smallest count the project has reached
after three rounds of falsification.

---

## 5. Formal comparison to peer systems

This section compares Pond to five peer systems: Git, Iceberg,
Dolt, FoundationDB, and LakeFS. The comparison is rigorous but
not exhaustive. For each system: what it does well, where Pond
differs, what Pond cannot do that the peer can.

### 5.1 Capability matrix

| Capability | Git | Iceberg | Dolt | FDB | LakeFS | Pond |
|---|---|---|---|---|---|---|
| Immutable blobs | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Content-addressed | ✓ (SHA-1/256) | ✓ (hash refs) | ✓ (Prolly) | ✗ (kv) | ✓ (SHA-256) | ✓ (SHA-256) |
| Generic byte storage | ✓ | ✗ (tables only) | ✗ (SQL only) | ✓ (kv) | ✓ | ✓ |
| Object-store native | Partial | ✓ | ✗ | ✗ | ✓ | ✓ (designed for) |
| Versioning | ✓ (commits) | ✓ (snapshots) | ✓ (commits) | ✗ | ✓ (commits) | ✓ (commits) |
| Branching | ✓ | ✗ | ✓ | ✗ | ✓ | ✓ |
| Merging | ✓ (manual) | ✗ | ✓ (SQL) | ✗ | ✓ (manual) | ✓ (Lens-defined) |
| Time travel | ✓ | ✓ | ✓ | ✗ | ✓ | ✓ |
| SQL semantics | ✗ | ✗ (schema only) | ✓ | ✗ | ✗ | Lens-level |
| ACID transactions | ✗ | ✗ | ✓ (per-table) | ✓ | ✗ | Within-Collection (A6) |
| Cross-Collection atomicity | ✗ | ✗ | ✗ | ✓ | ✗ | ✗ (out-of-model, A7) |
| Schema evolution | ✗ | ✓ | ✓ | ✗ | ✗ | ✓ (Lens-level) |
| Compression | ✓ (packfile) | ✓ (Parquet) | ✓ | ✗ | ✓ | ✓ (Transport) |
| Encryption | ✗ | ✗ | ✗ | ✗ | ✗ | ✓ (Transport) |
| Replication | ✗ (clone) | ✓ (catalog) | ✓ (remote) | ✓ (logs) | ✓ | ✓ (single-writer per Ref) |
| Pluggable semantics | Partial | ✗ (table only) | ✗ (SQL only) | ✓ (layers) | Limited | ✓ (Lens) |
| Formal model | Limited | Partial | Limited | ✓ (Atomik) | Limited | ✓ (claimed; TLA+ consistency only) |

### 5.2 Git

**What Git does well:**
- Mature, universally adopted for source code.
- Excellent at tree diffs and 3-way merges for line-oriented text.
- Packfile format is well-engineered for storage density.
- Branching is O(1) and intuitive.

**Where Pond differs:**
- Pond is generic byte storage; Git is tree-of-files. Pond can
  store SQL tables, vectors, notebooks; Git stores files.
- Pond is object-store-native (no local metadata dependence for
  correctness — see §5.7); Git assumes a local filesystem.
- Pond has pluggable merge semantics (Lens defines merge); Git's
  merge is line-oriented 3-way.
- Pond supports compression and encryption as Transport-layer
  concerns; Git has packfile compression but no encryption.

**What Pond cannot do that Git can:**
- Git's line-oriented 3-way merge is genuinely better for source
  code than anything Pond's Lens algebra provides out of the box.
  A Pond Lens could implement Git's merge algorithm, but no such
  Lens ships with Pond.
- Git's tooling (GitHub, GitLab, IDE integration) is unmatched.
  Pond has no equivalent ecosystem.

**Honest assessment:** For source code, Git is the right tool.
Pond's value proposition is for *non-code* workloads (tables,
vectors, streaming, ML features) where Git's tree-of-files model
is the wrong abstraction.

### 5.3 Iceberg

**What Iceberg does well:**
- Industry-standard table format; supported by Spark, Trino, Flink,
  DuckDB, Snowflake, BigQuery.
- Excellent snapshot model: atomic snapshot replacement, schema
  evolution, partition evolution.
- Manifest-based planning is fast and parallelizable.
- Catalog abstraction (Hive, Glue, REST, Nexus) decouples metadata
  from compute.

**Where Pond differs:**
- Pond is generic byte storage; Iceberg is table-only. Pond can
  store non-tabular data (vectors, notebooks, code) in the same
  kernel.
- Pond's Lens algebra is pluggable: a Pond Lens can implement
  Iceberg's table semantics, but Pond can also host non-table
  Lenses. Iceberg cannot.
- Pond's versioning is per-Collection (commit chain); Iceberg's is
  per-table (snapshot log). Both support time travel.
- Pond's manifest algebra (§10 of `POND_FORMAL_ALGEBRAS.md`) is
  isomorphic to Iceberg's manifest list, but generalized to
  non-tabular data.

**What Pond cannot do that Iceberg can:**
- Iceberg's partition pruning and column statistics are deeply
  integrated into query engines. Pond's Physical Structure algebra
  is more general but less optimized for the specific case of
  tabular queries.
- Iceberg's catalog ecosystem is mature. Pond has no equivalent
  catalog service; it relies on the Names substrate (which is
  simpler but less featured).
- Iceberg is supported by every major query engine. Pond has no
  query engine integration beyond DuckDB (via Arrow Lens).

**Honest assessment:** For tabular data lakehouse workloads,
Iceberg is more mature and better integrated. Pond's value
proposition is for workloads that mix tabular and non-tabular data
(SQL + vectors + code + features), where Iceberg's table-only
model forces multiple systems.

### 5.4 Dolt

**What Dolt does well:**
- SQL database with Git-like versioning. Genuine innovation.
- Per-row history; can `SELECT ... AS OF` any commit.
- Merge semantics defined for SQL (row-level 3-way merge).
- Excellent for data quality, audit, and collaboration on
  structured data.

**Where Pond differs:**
- Pond is generic byte storage; Dolt is SQL-only. Pond can store
  non-SQL data; Dolt cannot.
- Pond's merge is Lens-defined (any semantics); Dolt's is SQL
  row-level. Pond could host a Dolt-like Lens, but no such Lens
  ships.
- Pond's versioning is per-Collection; Dolt's is per-database.
  Both support branching and time travel.

**What Pond cannot do that Dolt can:**
- Dolt's SQL optimizer is mature; Pond has no SQL optimizer (a
  Pond SQL Lens would need to bring its own).
- Dolt's row-level merge is genuinely useful for structured data.
  A Pond SQL Lens would need to implement this.
- Dolt ships with a working database; Pond ships with a kernel and
  Lenses. Dolt is a product; Pond is a substrate.

**Honest assessment:** For versioned SQL workloads, Dolt is the
right tool. Pond's value proposition is for non-SQL workloads that
Dolt cannot serve.

### 5.5 FoundationDB

**What FoundationDB does well:**
- Strict serializability at scale. The gold standard for
  transactional kv stores.
- Layer system (BlobLayer, DocumentLayer, RecordLayer) demonstrates
  that complex semantics can be built on a simple kv substrate.
- Battle-tested at Apple, Snowflake, etc.
- Formal model (Atomik) is rigorous.

**Where Pond differs:**
- Pond is immutable byte storage with versioning; FDB is mutable
  kv storage with transactions. Fundamentally different substrate.
- Pond's consistency model is weaker (C0-C5 in §15: blob
  immutability, ref eventual propagation, single-ref atomicity,
  commit-blob atomicity, within-collection snapshot isolation, no
  cross-collection guarantee). FDB provides strict serializability.
- Pond's coordinator is out-of-model (A7); FDB's consensus is
  built-in.
- Pond's Lens algebra is pluggable; FDB's Layer system is also
  pluggable but on a different substrate.

**What Pond cannot do that FDB can:**
- FDB provides distributed ACID transactions across keys. Pond
  does not (A7; coordinator is out-of-model).
- FDB provides linearizable reads. Pond does not.
- FDB provides strict serializability. Pond provides
  within-Collection snapshot isolation, nothing more.
- FDB is battle-tested at Apple/Snowflake scale. Pond is
  unvalidated.

**Honest assessment:** FDB is a fundamentally stronger substrate
for transactional workloads. Pond's value proposition is for
*immutable, versioned* workloads where transactions are not
required and the Lens algebra's pluggability matters more than
FDB's stronger consistency.

### 5.6 LakeFS

**What LakeFS does well:**
- Object-store-native versioning for data lakes.
- Branch-based workflows for data engineering (dev/test/prod
  branches on S3 data).
- Atomic commits across many objects.
- Integrates with Spark, Iceberg, Delta, etc.

**Where Pond differs:**
- Pond is generic byte storage; LakeFS is data-lake-focused.
- Pond's Lens algebra is pluggable; LakeFS's semantics are fixed
  (commit, branch, merge over S3 objects).
- Pond's versioning is per-Collection; LakeFS's is per-repository.
- Pond supports compression and encryption as Transport concerns;
  LakeFS relies on S3's server-side encryption.

**What Pond cannot do that LakeFS can:**
- LakeFS is production-deployed at scale. Pond is not.
- LakeFS has a polished UI and CLI for data engineers. Pond has
  neither.
- LakeFS integrates with Spark/Iceberg/Delta out of the box. Pond
  has no such integrations.

**Honest assessment:** LakeFS and Pond overlap most closely. For
object-store-native data lake versioning, LakeFS is more mature.
Pond's value proposition is the pluggable Lens algebra — but this
is a *theoretical* advantage until a Pond Lens ships that LakeFS
cannot replicate.

### 5.7 Object-store-native comparison

| Property | Git | Iceberg | Dolt | FDB | LakeFS | Pond |
|---|---|---|---|---|---|---|
| Append-only writes | ✓ | ✓ | ✗ (in-place) | ✗ (in-place) | ✓ | ✓ |
| No rename | ✓ | ✓ | ✗ | ✗ | ✓ | ✓ |
| No directory assumptions | ✓ | ✓ | ✗ | ✗ | ✓ | ✓ |
| Bounded RTT | ✗ (history walk) | ✓ | ✗ | ✗ | ✓ | ✓ (with caveats) |
| Eventual consistency tolerant | ✓ | ✓ | ✗ | ✗ | ✓ | ✓ |
| Resumable | Partial | ✓ | ✗ | ✗ | ✓ | Partial |
| No local metadata dependence | ✗ (.git) | ✓ | ✗ (DB) | ✗ (logs) | ✓ | ✓ (designed for; current impl uses SQLite) |

**Honest assessment:** Pond's object-store-native design is
competitive with Iceberg and LakeFS on paper. The current kernel
implementation uses SQLite for the Names substrate, which violates
OSN7 ("no local metadata dependence"). The `ObjectStoreBackend`
in `experiments/object_store_backend.py` demonstrates the design
is buildable, but it is not the default kernel backend. **This is
a gap between model and implementation.**

---

## 6. What Pond does NOT do (in the kernel)

This section is intentionally honest. Reviewers should read it
carefully.

**Important framing (per Design Goal 3.7 Functional):** the
statements below describe what the *kernel* does not do. They are
NOT claims about what Pond cannot do. For each "kernel does not
do X," there is usually a Lens or Physical Structure that provides
X on top of the kernel. See `WHERE_POND_FAILS.md` for the mapping
from each gap to the Lens that closes it.

The kernel is small by design. The Lens algebra is infinite by
construction. Most "Pond can't do X" claims are missing Lenses.

### 6.1 No distributed consensus

The kernel provides no Raft, no Paxos, no 2PC. Multi-writer
convergence is out-of-model (A7). Applications requiring
multi-writer semantics must layer a coordinator; the
`TwoPhaseCommitCoordinator` in `pond-replication/` demonstrates
this is possible but does not make it part of the kernel.

**Implication:** Pond is not a replacement for FoundationDB,
CockroachDB, Spanner, or any system that requires distributed
ACID transactions. If your workload needs linearizable writes
across multiple keys, Pond is the wrong substrate.

### 6.2 No native CAS

The kernel's `Ref(name, h)` is unconditional last-writer-wins.
Compare-and-swap (R3 in the model) is **conditional on backend
support** (R3' after demotion). On SQLite (the default backend),
CAS is achievable via the optimistic-loop pattern but not exposed
as a kernel primitive. On S3 without conditional writes, even the
optimistic-loop pattern has a race window.

**Implication:** High-contention workloads (many writers to the
same Ref) will suffer. Pond is designed for low-contention,
append-mostly workloads.

### 6.3 No wall-clock time

The Time substrate is Lamport (A5), not wall-clock. Timestamps
are monotonic within a process and causally consistent across
processes, but not wall-clock-comparable across processes.

**Implication:** "Latest timestamp wins" merge policies are
well-defined only within a Lamport order, not globally. Workloads
requiring wall-clock ordering (e.g., "the event with the latest
wall-clock timestamp wins") need an external time authority.

### 6.4 No query engine

Pond is a storage kernel, not a query engine. It has no SQL
parser, no optimizer, no execution engine. The Lens algebra
defines encode/decode; query execution is the application's
responsibility.

**Implication:** Pond is not a replacement for DuckDB, Spark, or
any query engine. The planned flagship (Phase Q.4) is a DuckDB-based
lakehouse *on top of* Pond — Pond is the substrate, DuckDB is the
engine.

### 6.5 No production validation

Pond has no production deployments. The 683 passing tests verify
internal consistency; they do not verify production behavior under
real workloads, real failure modes, or real scale.

**Implication:** All claims about Pond's behavior under load are
theoretical. The Phase Q.3 benchmark suite is the first attempt
to measure Pond against peer systems.

### 6.6 No external expert review

At time of writing, Pond has been reviewed only by its own author
(via three rounds of self-red-team). No external distributed-systems
engineer has reviewed the model. The Phase Q.5 external review
track is pending.

**Implication:** The model may contain flaws that the author
cannot see. This paper is an invitation for reviewers to find
them.

### 6.7 No lower-bound proof

The model claims six substrates are necessary. There is no proof.
A future red team might show that fewer substrates suffice (e.g.,
folding Coordination into Time, or eliminating the Key substrate
by making encryption Lens-level).

**Implication:** "Six substrates, three operations" is the smallest
count the project has reached after three rounds of falsification.
It is not proven minimal.

---

## 7. What is established vs. what is not

### 7.1 Established (internal consistency)

| Claim | Evidence |
|---|---|
| Implementation matches model | 683 checks pass (562 property + 61 differential + 23 hazard + 53 engineering) |
| Invariants hold in finite model | TLA+ checks 6 invariants across 56 reachable states |
| Tested behaviors match Git | 45 differential tests pass (content-addressing, commit chain, branch, time travel, merge topology, tree determinism) |
| Tested behaviors match Dolt | 10 differential tests pass (content-addressing, commit chain, branch, time travel, merge topology) |
| Tested behaviors match Iceberg | 6 differential tests pass (manifest rebuildable, snapshot reproducible, schema evolution) |
| Algebras are implementable | 4 engineering packages built (Schema Registry, Production Transport, Replication Coordinator, real differential suite) |
| Kernel is small | ~140 LOC, stdlib only |
| Hazards are survived | 9 hazards simulated (read-after-write lag, list-after-put, replica lag, partial write, partial read, delete race, clock skew, partition, disk corruption, Byzantine, replay, hash collision, concurrent compaction+replication) |

### 7.2 Not established (external validation)

| Claim | Status |
|---|---|
| Architecture is *correct* | Not proven. TLA+ proves consistency, not correctness. |
| Architecture is *necessary* | Not proven. No lower-bound proof. |
| Architecture is *competitive* | Not measured. No benchmarks vs. peer systems. |
| Architecture is *adoptable* | Not demonstrated. No production use. |
| Architecture survives *expert review* | Not tested. No external review yet. |
| Lens algebra covers *real workloads* | Not demonstrated. No flagship app. |
| Model is *minimal* | Not proven. Six substrates is the smallest count after three red-team rounds, but not proven minimal. |

### 7.3 The gap

The gap between §7.1 and §7.2 is the gap Phase Q must close. The
internal consistency work is done. The external validation work
is just beginning.

---

## 8. Open questions for reviewers

This section lists the questions the author most wants reviewers
to attack.

### 8.1 Is the substrate set necessary?

The model claims six substrates (Bytes, Names, Time, Coordination,
Range-Read, Key). Could fewer suffice?

- Could Coordination be folded into Time? (Probably not — A6
  atomic commit blob is a distinct mechanism from A5 Lamport
  clock.)
- Could Range-Read be eliminated? (Already demoted to Transport;
  could it be eliminated entirely?)
- Could Key be folded into Bytes? (Probably not — encryption keys
  are not bytes in the same sense.)

**Attack:** find a substrate that is redundant, or find a seventh
substrate the model silently depends on.

### 8.2 Is the Lens algebra sufficient for real workloads?

The model claims any workload can be implemented as a Lens. This
is a strong claim.

- Can a Lens implement a SQL optimizer? (Probably not — an
  optimizer is a query engine, not a codec.)
- Can a Lens implement a streaming engine? (Probably — streaming
  is append-mostly, which fits the kernel.)
- Can a Lens implement a vector database with ANN search?
  (Probably — the Physical Structure algebra covers indexes.)

**Attack:** find a workload that cannot be implemented as a Lens
without violating L1-L7.

### 8.3 Is the consistency model sufficient?

The model provides C0-C5 (blob immutability, ref eventual
propagation, single-ref atomicity, commit-blob atomicity,
within-collection snapshot isolation, no cross-collection
guarantee). No linearizability, no serializability, no
causal consistency across Collections.

- Is this sufficient for data lakehouse workloads? (Maybe —
  lakehouses are usually append-mostly.)
- Is this sufficient for OLTP? (No — OLTP needs transactions.)
- Is this sufficient for streaming? (Probably — streaming is
  append-mostly.)

**Attack:** find a workload that needs stronger consistency than
C0-C5 and cannot be served by layering a coordinator.

### 8.4 Is the kernel implementation honest?

The current kernel uses SQLite for the Names substrate. The model
says the Names substrate can be SQLite, FoundationDB, or a
directory of small files. The `ObjectStoreBackend` in
`experiments/` demonstrates the object-store-native variant, but
it is not the default.

- Does the SQLite backend hide problems that an object-store
  backend would expose?
- Are the 683 tests biased toward the SQLite backend's behavior?

**Attack:** re-run the test suite against the object-store
backend; find tests that pass on SQLite but fail on S3.

### 8.5 Is the formal model honest?

The TLA+ specification checks 6 invariants over 56 reachable
states in a finite model with 3 byte values, 4 hashes, 2 names.

- Is this state space large enough to catch real bugs?
- Are the 6 invariants the *right* invariants?
- Are there invariants that should hold but don't?

**Attack:** find an invariant that should hold in Pond but is not
in the TLA+ spec. Or find a state not covered by the 56 reachable
states that violates an invariant.

### 8.6 Is the comparison fair?

The capability matrix in §5.1 compares Pond favorably to peer
systems on several axes. Is this fair?

- Pond is unvalidated; peer systems are production-deployed.
  Comparing a research prototype to production systems may be
  unfair to the production systems.
- Pond's "pluggable semantics" claim is theoretical; no Lens ships
  that peer systems cannot replicate.

**Attack:** find a row in the capability matrix where the
comparison is unfair, or find a capability missing from the
matrix.

---

## 9. Related work

### 9.1 Content-addressed storage

- **Git** (2005): content-addressed tree-of-files. Pond
  generalizes to arbitrary bytes.
- **IPFS** (2015): content-addressed distributed filesystem.
  Pond is not distributed (no consensus); IPFS is.
- **Nix** (2003): purely functional package manager with
  content-addressed store. Pond's immutability axioms (A1, A2) are
  inspired by Nix.

### 9.2 Layered storage

- **FoundationDB Layers** (2010s): complex semantics built on a
  simple kv substrate. Pond's Lens algebra is analogous but on an
  immutable substrate.
- **Datomic** (2012): immutable facts with query layer. Pond's
  bytes-primary, state-derived distinction (§12 of formal
  algebras) is similar.

### 9.3 Lakehouse formats

- **Iceberg** (2017): table format with snapshot model. Pond's
  Manifest algebra is isomorphic but generalized.
- **Delta Lake** (2017): table format with transaction log. Pond's
  commit chain is similar but per-Collection, not per-table.
- **Hudi** (2016): table format with copy-on-write and
  merge-on-read. Pond's Lens algebra could host either model.

### 9.4 Object-store-native systems

- **WarpStream** (2023): Kafka-on-S3 without local state. Pond's
  object-store-native design (OSN definition) is inspired by
  WarpStream's approach.
- **LakeFS** (2019): Git-like versioning for object storage. Pond
  overlaps most closely with LakeFS; see §5.6.

### 9.5 Formal methods in storage

- **TLA+** specifications of distributed systems (Lamport et al.).
  Pond's TLA+ spec is in this tradition but much smaller.
- **Atomik** (Wilcox et al., 2015): formal model of FoundationDB.
  Pond's formal model is less rigorous than Atomik.

---

## 10. Conclusion

Pond is a hypothesis with strong internal consistency and zero
external validation. The internal consistency work (Phases K-P,
683 passing checks, TLA+ model, four engineering packages) is
done. The external validation work (Phase Q: benchmarks,
whitepaper, flagship, expert review) is just beginning.

This paper is not a claim that Pond is right. It is a precise
description of what Pond is, what it does, what it does not do,
and what has and has not been established. Reviewers are invited
to attack any of the open questions in §8.

If Pond's architecture survives external review, the next step is
the Phase Q.4 flagship: a DuckDB-based lightweight lakehouse built
on Pond, testing whether the Lens algebra covers real workloads.
If the flagship ships cleanly, Pond becomes a candidate substrate
for the "lightweight alternative to Spark/Flink/Databricks" the
author has long advocated.

If Pond's architecture does not survive external review, the
falsification is the contribution. Finding that a small-substrate
kernel is *not* sufficient — identifying which substrate is
missing, which law is wrong, which consistency level is too weak —
is a valuable result either way.

The architecture is frozen. The validation begins.

---

## Appendix A: Artifact inventory

| Artifact | Location | LOC |
|---|---|---|
| Kernel (FROZEN) | `pond-core/pond_minimal.py` | ~140 |
| Lens SDK | `pond-sdk/` | ~3000 |
| Feature Store | `pond-feature-store/` | ~1500 |
| Arrow Lens | `pond-arrow/` | ~500 |
| Transport (reference) | `pond-transport/transport.py` | ~330 |
| Transport (production) | `pond-transport/transport_production.py` | ~400 |
| Schema Registry | `pond-schema/schema_registry.py` | ~430 |
| Replication Coordinator | `pond-replication/replication_coordinator.py` | ~430 |
| Hazard Simulator | `scripts/phase_l_hazard_simulator.py` | ~400 |
| Property Tests | `scripts/phase_l_property_tests.py` | ~1100 |
| Differential Tests (Git) | `scripts/phase_l_differential_git.py` | ~630 |
| Untested Laws Tests | `scripts/phase_n_untested_laws.py` | ~340 |
| Additional Hazards Tests | `scripts/phase_n_additional_hazards.py` | ~140 |
| Remaining Laws Tests | `scripts/phase_o_remaining_laws.py` | ~640 |
| Remaining Hazards Tests | `scripts/phase_o_remaining_hazards.py` | ~490 |
| Real Dolt/Iceberg Differentials | `scripts/phase_p_real_differentials.py` | ~570 |
| TLA+ Specification | `tla/PondKernel.tla` | ~160 |
| Formal Algebras (Parts I-IV) | `docs/POND_FORMAL_ALGEBRAS.md` | ~2400 |
| Mathematical Model | `docs/POND_MATHEMATICAL_MODEL.md` | ~630 |
| Second Red Team | `docs/POND_SECOND_RED_TEAM.md` | ~690 |
| Third Red Team | `docs/POND_THIRD_RED_TEAM.md` | ~700 |
| Phase L Report | `docs/POND_PHASE_L_REPORT.md` | ~430 |
| Phase N Report | `docs/POND_PHASE_N_REPORT.md` | ~210 |
| Phase O Report | `docs/POND_PHASE_O_REPORT.md` | ~280 |
| Phase P Report | `docs/POND_PHASE_P_REPORT.md` | ~250 |
| This whitepaper | `docs/POND_WHITEPAPER.md` | ~900 |

---

## Appendix B: How to attack this paper

If you are a reviewer, the most valuable attacks are:

1. **Find a substrate that is missing.** The model claims six
   substrates. If you find a seventh that the model silently
   depends on (e.g., a "network" substrate, a "logging" substrate),
   the model is incomplete.

2. **Find a law that is wrong.** The model has ~30 laws (R1-R5,
   M1-M4, G1-G6, MAN1-MAN4, RR1-RR4, etc.). If you find a law
   that does not hold in a scenario the model does not exclude,
   the law is wrong.

3. **Find a workload that breaks the Lens algebra.** The model
   claims any workload can be implemented as a Lens (L1-L7). If
   you find a workload that violates L1-L7, the claim is false.

4. **Find a benchmark where Pond is fundamentally slower.** The
   Phase Q.3 benchmark suite is in progress. If you can show Pond
   is fundamentally slower than peer systems on a workload that
   matters, the architecture is wrong.

5. **Find an invariant that should hold but doesn't.** The TLA+
   spec checks 6 invariants. If you find a seventh that should
   hold but doesn't, the spec is incomplete.

Any of these attacks, if successful, is a contribution. The goal
of this paper is to be falsified.

---

*Draft 1. Comments welcome. Push to GitHub after each revision.*
