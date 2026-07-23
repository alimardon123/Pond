# Rejected Designs

A log of architectural decisions that were considered and rejected, with
reasons. This document exists because six months from now, nobody will
remember why Tree was removed from the kernel or why we didn't use
Arrow as the OPEN format. Without this, the same ideas will be
re-proposed and re-rejected endlessly.

Each entry: the rejected design, when it was considered, why it was
rejected, and whether the rejection could be revisited.

---

## 1. Tree as a kernel primitive

**When:** v0.1-v0.2 (original pond.py and pond_kernel.py)

**What:** The kernel had a `Tree` type — a structured object with
`entries: dict[name -> hash]`. Trees were first-class kernel objects
with dedicated `write_tree` / `read_tree` methods.

**Why rejected:** The minimality experiment (v0.4) showed that Tree is
a pattern, not a primitive. A Tree is just a blob containing serialized
`{name -> hash}` mappings. All 8 Views work without kernel-level Tree
support — they build Trees as View patterns using `Write(json)`.

**Could be revisited:** Yes, if a future workload requires kernel-level
tree semantics that can't be expressed as a blob. No such workload has
been found in 14 tested (8 standard + 6 alien).

---

## 2. Commit as a kernel primitive

**When:** v0.1-v0.2

**What:** The kernel had a `Commit` type — a structured object with
`tree_hash`, `parent_hash`, `timestamp`, `message`. Commits were
first-class kernel objects.

**Why rejected:** Same as Tree. A Commit is just a blob containing
serialized metadata. The `parent_hash` field is a Lens convention, not
a kernel requirement. Multi-parent commits (CRDTs, merges) work
because the kernel doesn't enforce single-parent — it just stores bytes.

**Could be revisited:** Yes, if a future workload requires kernel-level
commit semantics. The OCI View doesn't use commits at all (manifests
are independent, no parent chain). 7 of 8 Views use commits, but none
require them to be kernel-level.

---

## 3. OPEN/SEALED lifecycle as kernel state

**When:** v0.1-v0.2

**What:** Objects had a lifecycle: OPEN (mutable, appendable) → SEALED
(immutable) → COMPACTED → ARCHIVED → GC. The kernel tracked object state.

**Why rejected:** The lifecycle is a Lens-level buffer optimization.
Views can buffer in memory and call `Write` when ready. The kernel
doesn't need to track "OPEN" vs "SEALED" — once bytes are Written,
they're immutable, period. Compaction, archival, and GC are Lens-level
policies.

**Could be revisited:** No, unless a workload requires kernel-enforced
time-bounded mutability (e.g., "this object is mutable for 1 hour, then
immutable"). No such workload found.

---

## 4. Arrow IPC as the OPEN format

**When:** v0.1 (original pond.py)

**What:** The OPEN object format was hardcoded as Arrow IPC. `Write`
took `pa.RecordBatch`; `Seal` converted Arrow IPC to Parquet.

**Why rejected:** Format leak. Arrow is one format; hardcoding it bakes
assumptions about columnar, tabular data into the kernel. The kernel
should be bytes-only. Views choose their own serialization.

**Could be revisited:** No. This was the key insight that split the
kernel from Views (v0.2). Reverting would re-couple the kernel to a
specific format.

---

## 5. Parquet as the SEALED format

**When:** v0.1

**What:** `Seal` hardcoded Arrow IPC → Parquet conversion. Sealed
objects were always Parquet.

**Why rejected:** Same as Arrow. Parquet is one format. The kernel
stores bytes; Views interpret them. SQLView uses Parquet, VectorView
uses raw floats, StreamView uses length-prefixed records, GitView uses
raw file bytes. Forcing Parquet would break 4 of 8 Views.

**Could be revisited:** No. The Anti-Iceberg test (Experiment 7)
showed the kernel has zero Parquet dependencies. Reverting would
re-couple the kernel to the data ecosystem.

---

## 6. SQL methods on the kernel class

**When:** v0.1

**What:** The kernel had a `.sql(query)` method that executed SQL via
DuckDB over sealed Parquet files.

**Why rejected:** SQL is a Lens concern. Putting `.sql()` on the kernel
class makes SQL privileged — it suggests the kernel "knows" SQL. The
kernel should be query-language-agnostic. SQL belongs in SQLView, not
on the kernel.

**Could be revisited:** No. This was part of the v0.2 kernel/View split.

---

## 7. Schema tracking in the kernel

**When:** v0.1

**What:** The kernel tracked `pa.Schema` (Arrow schema) per table.

**Why rejected:** Arrow type system leak. The kernel should not know
about schemas — schemas are Lens-level concepts (SQL tables have
schemas; OCI images don't; Git files don't). Views track their own
schemas if they need them.

**Could be revisited:** No. Schema is a Lens concern by the Universality
criterion of the Admission Rule.

---

## 8. Skip pointers in the kernel (for time travel)

**When:** Considered in v0.5 (after Finding 5a)

**What:** Add skip pointers to the commit chain: every Kth commit
stores a back-pointer to the commit K steps back. Gives O(log N) time
travel instead of O(N).

**Why rejected:** Fails the Universality criterion of the Admission
Rule. Only SQL and Git Views need time travel. Vector, Streaming,
Graph, ML, TimeSeries, OCI Views don't. Skip pointers should be a
Lens-level pattern (each View that needs time travel implements its own).

**Could be revisited:** Yes, if 3+ structurally different Views need
skip pointers. Currently only 2 do. If a third emerges (e.g., a
Feature Store View with point-in-time correctness), reconsider.

---

## 9. GC in the kernel

**When:** Considered in v0.5 (after Finding 6)

**What:** Add a kernel-level garbage collector that walks reachability
from all root References and sweeps unreferenced blobs.

**Why rejected:** Fails the Universality criterion. Different Views
have different GC policies:
- SQL: keep last N snapshots, GC the rest
- Git: keep all commits reachable from any branch/tag
- OCI: keep all manifests tagged in the last 30 days
- ML: keep all checkpoints unless explicitly deleted

A kernel-level GC would impose one policy. Lens-level GC lets each
View choose. The kernel provides the primitives (Write, Read,
Reference, list-blobs); Views implement reachability walks.

**Could be revisited:** Yes, if all Lenses converge on the same GC
policy. Currently they don't.

---

## 10. Raft replication in the kernel

**When:** Considered for v0.6 (post-destruction roadmap)

**What:** Add Raft-based replication to the kernel for multi-node
deployments.

**Why rejected (deferred):** Not rejected — deferred. The kernel
should support replication, but the question is whether Raft is the
right mechanism or whether it should be a Lens/infrastructure concern.

The destruction phase (Stage 3) found that content-addressing handles
idempotent writes and dedup, but does NOT handle: lost updates,
concurrent reference races, namespace coordination, transactional
visibility, causal consistency. These need a coordination layer.

Open question: is the coordination layer part of the kernel, or a
separate infrastructure component? The laws-vs-APIs experiment
(Experiment 3) suggests the kernel is laws, and coordination is one
realization. Raft might be a Lens/infrastructure concern, not a
kernel primitive.

**Could be revisited:** Yes — this is an active research question.

---

## 11. Flat tree (O(N²) metadata growth)

**When:** v0.1 (original pond.py)

**What:** Each commit's tree was a flat list of all blob references.
Every commit copied all prior blob references.

**Why rejected:** O(N²) metadata growth. At 5,000 seals, metadata was
7,969% of data. The hierarchical tree (Git model + delta chain) fixed
this — each commit adds one new leaf, compacts when 256 leaves accumulate.

**Could be revisited:** No. The hierarchical tree is strictly better.
The flat tree was a bug, not a design choice.

---

## 12. SQLite as the only root namespace backend

**When:** v0.1-v0.4

**What:** The root namespace was always SQLite. No abstraction over
the root store.

**Why rejected (partially):** SQLite works for single-node, ~100M
names. At 1B+ names (Stage 5 scale destruction), SQLite hits its
practical limit. The root store should be swappable (SQLite for dev,
FoundationDB/etcd for 1B+ scale).

**Current state:** The kernel treats the root store as an
implementation detail. Views can swap it. But the default
implementation is SQLite, which doesn't scale to 1B+ names.

**Could be revisited:** The abstraction is in place; only the default
implementation needs to change. Not a kernel change.

---

## 13. "Subsumes Spark/Flink/Kafka/Iceberg" marketing language

**When:** v3-v5 (early architecture iterations)

**What:** The README claimed Pond "subsumes" Spark, Flink, Kafka,
Iceberg, Delta, Trino, Materialize, etc.

**Why rejected:** Overclaim. Pond doesn't subsume these systems — it
provides a substrate on which similar functionality could be built as
Views. Subsuming implies feature parity, which is unproven. The
comparison also frames Pond as "better Iceberg," which is the wrong
comparison set (should be Git, Irmin, IPFS, LakeFS, FDB, Dolt).

**Could be revisited:** No. Define by what Pond IS, not what it replaces.

---

## 14. LOC budget as a kernel discipline

**When:** v0.3

**What:** Kernel components had LOC budgets (Coordinator <1000, Planner
<500, Storage <2000, IR <3000). Enforced as release blockers.

**Why rejected:** LOC is the wrong metric. 500 ugly lines may be worse
than 900 beautiful lines. Dependency budgets (what each component may
depend on) are architecturally enforceable; LOC is not.

**Could be revisited:** No. Dependency budgets replaced LOC budgets in
v0.3 and are strictly better.

---

## 15. "Architecture frozen" as a permanent state

**When:** v0.4-v0.5

**What:** The README declared the architecture "frozen" and stated no
more architectural changes would be made.

**Why rejected (softened):** Overclaim. The architecture is "frozen
unless the destruction phase finds a real issue." The destruction
phase (v0.6) and Identity Destruction II (v0.7) are explicitly
designed to find issues. "Frozen" was too strong; "stable but
attackable" is more honest.

**Could be revisited:** The wording was softened. The architecture is
stable but not permanently frozen — it's stable until falsified.

---

## How to use this document

When proposing a feature:
1. Check if it's in this document. If so, read why it was rejected.
2. If the rejection reason still holds, don't re-propose.
3. If the rejection reason no longer holds (new workload, new evidence),
   re-propose with explicit reference to this document and the changed
   conditions.

This prevents endless re-litigation of settled decisions while keeping
the door open for genuinely new evidence.
