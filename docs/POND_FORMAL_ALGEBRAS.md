# Pond Formal Algebras

> Phase A — Formalize. No implementation. Only models.
> Every algebra must answer: "Is this the inevitable consequence
> of the model, or merely one implementation?"

---

## Table of Contents

1. [Reference Algebra](#1-reference-algebra)
2. [Merge Algebra](#2-merge-algebra)
3. [Garbage Collection Model](#3-garbage-collection-model)
4. [RTT Calculus](#4-rtt-calculus)
5. [Object Store Native Specification](#5-object-store-native-specification)
6. [Physical Structure Taxonomy](#6-physical-structure-taxonomy)
7. [Workspace Algebra](#7-workspace-algebra)
8. [History as a Mathematical Object](#8-history-as-a-mathematical-object)

---

## 1. Reference Algebra

### 1.1. The insight

References are much richer than "name → hash." They are the **only
mutable state** in the system, and they serve many roles:

| Role | Example | Lifetime | Mutability |
|---|---|---|---|
| HEAD | `analytics/orders` → latest commit | Permanent | Updated on every commit |
| Branch | `analytics/orders__branch__dev` → commit | Permanent | Updated on branch commits |
| Snapshot pointer | `analytics/orders__snapshot` → snapshot commit | Permanent | Updated on snapshot commits |
| Tag | `analytics/orders__tag__v1.0` → commit | Permanent | Immutable (once set) |
| Workspace | `analytics/orders__ws__session_42` → staging state | Ephemeral | Updated on every stage |
| Lock | `analytics/orders__lock__writer` → timestamp | Ephemeral | Acquired/released |
| Lease | `analytics/orders__lease__reader_7` → expiry time | Ephemeral | Auto-expires |
| Materialization | `analytics/orders__mat__by_region` → index tree | Permanent | Updated on rebuild |
| Collection meta | `analytics/orders__meta` → metadata blob | Permanent | Updated on meta change |
| Pack | `analytics/orders__pack` → pack file blob | Permanent | Updated on repack |
| Replica | `analytics/orders__replica__node_3` → commit | Permanent | Updated by replication |

**All of these are `Ref(name, hash)`.** The kernel doesn't distinguish
them. The naming convention is a Lens/Application-level concern.

### 1.2. Reference types (formal)

```
Reference = (Name, Hash, Properties)

Name ∈ String
Hash ∈ {0,1}^256 (SHA-256)
Properties = {
    lifetime: permanent | ephemeral,
    mutability: mutable | immutable_once_set | append_only,
    scope: kernel | collection | global,
}
```

### 1.3. Reference operations

```
set(name, hash)         — create or update a reference
get(name) → hash | ∅    — resolve a reference
list(prefix) → [name]   — list references matching a prefix
delete(name)            — remove a reference (tombstone)
compare_and_swap(name, expected, new) → bool  — atomic update
```

### 1.4. Reference laws

**R1 (Atomicity).** `set(name, hash)` is atomic. After the operation,
`get(name)` returns `hash` for all readers.

**R2 (Last-writer-wins).** If two `set(name, h₁)` and `set(name, h₂)`
race, one wins. There is no merge at the reference level.

**R3 (Compare-and-swap).** `compare_and_swap(name, expected, new)`
succeeds iff `get(name) == expected` at the time of the call. This
enables optimistic concurrency control.

**R4 (Tombstone).** `delete(name)` is expressed as `set(name, TOMBSTONE_HASH)`.
A tombstoned reference returns `∅` from `get()`. (Per RFC-0008.)

**R5 (Prefix listing).** `list(prefix)` returns all names starting
with `prefix`. On object stores, this maps to `LIST` operations.

### 1.5. Reference cost on object stores

| Operation | S3 equivalent | RTT cost |
|---|---|---|
| `set(name, hash)` | PUT object | 1 PUT |
| `get(name)` | GET object (or HEAD) | 1 GET/HEAD |
| `list(prefix)` | LIST objects v2 | 1 LIST (expensive) |
| `delete(name)` | PUT object (tombstone) | 1 PUT |
| `compare_and_swap` | GET + conditional PUT | 2 RTTs (no native CAS on S3) |

**Insight:** `compare_and_swap` requires 2 RTTs on S3 (no native CAS).
FoundationDB provides native CAS. This is a backend property, not a
model property. The model defines CAS; the backend decides how to
implement it.

### 1.6. References as a graph

References form a **directed graph**:

```
                HEAD
                 |
              commit_hash
              /        \
        parent          second_parent (merge)
          |                |
       commit            commit
          |                |
       ...              ...
```

But references also form a **naming graph** (namespace hierarchy):

```
analytics/
  orders          (HEAD)
  orders__snapshot
  orders__branch__dev
  orders__meta
  orders__pack
  orders__mat__by_region
  customers        (HEAD)
  customers__snapshot
```

These are two different graphs over the same namespace. The kernel
only sees flat names. The hierarchy is a naming convention.

---

## 2. Merge Algebra

### 2.1. The three-layer model

Merge is not a single operation. It is a **pipeline of three layers**:

```
Layer 1: Kernel (topology)
  - Records parents (first + second)
  - Provides commit graph
  - Does NOT define merge semantics

Layer 2: Lens (semantics)
  - Defines how to merge two states
  - May use 3-way, CRDT, domain-specific, etc.
  - Different Lenses can have different merge semantics

Layer 3: Application (policy)
  - Defines conflict resolution policy
  - May auto-resolve, prompt user, or abort
  - Example: "merged branch wins" vs "prompt user" vs "abort on conflict"
```

### 2.2. Kernel merge (topology only)

The kernel's only merge responsibility is recording topology:

```
merge_commit = {
    parent: HEAD,           # first parent (merged INTO)
    second_parent: branch,  # second parent (merged FROM)
    snapshot: merged_state, # the merged state (always a snapshot)
}
```

The kernel does NOT decide what `merged_state` contains. That is the
Lens's job.

### 2.3. Lens merge (semantics)

A Lens defines `merge(state_A, state_B, ancestor?) → state_merged`:

**Union merge (current default):**
```
merge(A, B) = A ∪ B  (B wins on conflict)
```

**3-way merge:**
```
merge(A, B, C) where C = common_ancestor(A, B)
  = apply(diff(C, A), apply(diff(C, B), C))
  conflicts = keys changed in both A and B since C
```

**CRDT merge:**
```
merge(A, B) = A ⊔ B  (least upper bound in the CRDT lattice)
  Always succeeds, no conflicts
```

**Timestamp merge:**
```
merge(A, B) = for each key: pick the version with the latest timestamp
  No conflicts; deterministic
```

### 2.4. Merge cost model

| Strategy | Read cost | Write cost | Conflicts? |
|---|---|---|---|
| Union | O(\|A\| + \|B\|) | O(\|merged\|) | No (B wins) |
| 3-way | O(\|A\| + \|B\| + \|C\|) | O(\|changed\|) | Yes (detected) |
| CRDT | O(\|A\| + \|B\|) | O(\|merged\|) | No (impossible) |
| Timestamp | O(\|A\| + \|B\|) | O(\|changed\|) | No (deterministic) |

On object stores, the read cost dominates (each state read is
O(N/chunk) GETs). **Diff-based merge** (only reading changed chunks)
is the key optimization:

```
diff-based merge:
  1. Find common ancestor C (O(log N) via skip pointers)
  2. Compute diff(A, C) — only changed chunks (O(changed_chunks) GETs)
  3. Compute diff(B, C) — only changed chunks (O(changed_chunks) GETs)
  4. Apply both diffs to C's snapshot
  5. Write merged snapshot
```

This reduces merge from O(|A| + |B|) to O(|changed_A| + |changed_B|).

### 2.5. Merge laws

**M1 (Commutativity of topology).** The kernel records both parents.
The order of `parent` and `second_parent` is conventional (first =
merged INTO, second = merged FROM), but the topology is the same
regardless of order.

**M2 (Associativity of merge commits).** Merging A←B then merging
(A←B)←C produces a commit graph that records both merges. The
topology is preserved.

**M3 (Lens determines semantics).** The kernel does not decide what
the merged state contains. The Lens defines `merge(A, B)`. Different
Lenses can have different merge semantics on the same byte graph.

**M4 (Merge is a snapshot).** A merge commit always contains a
snapshot (full Prolly tree root). This is because merge must produce
a consistent state that doesn't require replaying two parent chains.

---

## 3. Garbage Collection Model

### 3.1. The problem

Immutable storage accumulates orphaned objects. Blobs become
unreachable when:
- A Reference is updated (old commit becomes orphaned)
- A branch is deleted
- A snapshot is superseded
- A Physical Structure is rebuilt (old index orphaned)

### 3.2. GC is NOT a kernel concept

The kernel does not perform GC. The kernel's axioms (A1: immutability)
mean blobs are never deleted. GC is a **maintenance operation** that
reclaims space by removing unreachable blobs.

### 3.3. Reachability

A blob is **reachable** if there exists a path from any Reference to it:

```
reachable(blob) = ∃ ref. path(ref, blob)

path(ref, blob):
  ref → hash → read(hash) → if contains blob's hash → reachable
  (transitively: tree → entries → blob hashes → their content → ...)
```

### 3.4. GC algorithm

```
GC(kernel):
  1. MARK: walk all References, follow all hashes transitively
  2. SWEEP: delete all blobs not marked

  This is a tracing GC, like Git's `git gc`.
```

### 3.5. GC cost

| Phase | Cost | Object store RTTs |
|---|---|---|
| MARK | O(R × D) where R = refs, D = average depth | O(R) LIST + O(blobs) GET |
| SWEEP | O(total_blobs - reachable_blobs) | O(orphaned) DELETE |

**Insight:** MARK is expensive on object stores (must read every
reachable blob to find transitive references). Two optimizations:

1. **Manifest-based GC:** a manifest blob lists all reachable blob
   hashes. GC reads the manifest (1 GET) instead of walking the
   graph. The manifest is a Physical Structure: `f(all_refs) → manifest`.

2. **Epoch-based GC:** track epochs (time ranges). Blobs older than
   the oldest active epoch can be deleted. No graph walk needed.
   Approximate (may keep some orphaned blobs) but cheap.

### 3.6. GC laws

**G1 (Safety).** GC never deletes a reachable blob.

**G2 (Liveness).** Eventually, all unreachable blobs are collected
(liveness depends on GC being run periodically).

**G3 (Idempotency).** Running GC twice has the same effect as once.

**G4 (Non-blocking).** GC does not block reads or writes. (The kernel
continues to operate during GC.)

**G5 (Tombstone interaction).** Tombstoned references (deleted names)
make their target blobs unreachable. GC collects them. The tombstone
itself is a blob (`TOMBSTONE_HASH`'s marker blob) that is always
reachable (it's the target of the tombstoned reference).

---

## 4. RTT Calculus

### 4.1. Operation cost vectors

Every operation has a cost vector:

```
Cost = (GET, PUT, LIST, HEAD, RANGE, bytes_transferred, parallelizable)
```

Where:
- `GET` = number of object reads
- `PUT` = number of object writes
- `LIST` = number of list operations (expensive on S3)
- `HEAD` = number of metadata reads
- `RANGE` = number of range reads (partial object reads)
- `bytes_transferred` = total bytes
- `parallelizable` = can the GETs be parallelized?

### 4.2. Cost table

| Operation | GET | PUT | LIST | HEAD | RANGE | Parallel | S3 ms |
|---|---|---|---|---|---|---|---|
| Point lookup | 2 | 0 | 0 | 1 | 0 | No | 50 |
| Batch lookup (pack) | 1 | 0 | 0 | 1 | 0 | No | 30 |
| Streaming commit | 0 | 2 | 0 | 1 | 0 | No | 40 |
| Snapshot commit | 0 | 2+chunks | 0 | 1 | 0 | No | 40+ |
| Branch | 0 | 1 | 0 | 1 | 0 | No | 40 |
| Checkout | 1 | 1 | 0 | 2 | 0 | No | 70 |
| Merge (diff-based) | O(diff) | 2 | 0 | 3 | 0 | Partial | 100+ |
| Scan (pack) | 1 | 0 | 0 | 1 | 0 | No | 30 |
| Scan (no pack) | O(N) | 0 | 0 | 1 | 0 | Yes | N×20 |
| History(N) | N | 0 | 0 | 1 | 0 | No | N×20 |
| Restart | 0 | 0 | 0 | 1 | 0 | No | 10 |

### 4.3. RTT theorems (target)

**T1 (Lookup ≤ 3).** Point lookup requires at most:
```
1 HEAD (resolve HEAD reference)
1 GET (read commit → get snapshot root, if embedded in reference: 0)
1 GET (read tree leaf → get blob hash)
1 GET (read data blob)
= 3 GETs + 1 HEAD = 4 RTTs

With snapshot root embedded in HEAD reference:
1 HEAD (resolve → get snapshot_root directly)
1 GET (read tree leaf)
1 GET (read data blob)
= 2 GETs + 1 HEAD = 3 RTTs ✓
```

**T2 (Scan ≤ 5).** Full scan requires at most:
```
1 HEAD (resolve snapshot reference)
1 GET (read snapshot commit → get tree root)
1 GET (read tree internal nodes — if tree fits in 1 object)
1 GET (read pack file — all data blobs in one object)
= 3 GETs + 1 HEAD = 4 RTTs ✓
```

**T3 (Commit ≤ 3).** Streaming commit requires at most:
```
0 GETs (no reads needed for delta commit)
1 PUT (write delta blob)
1 PUT (update HEAD reference)
1 HEAD (resolve parent, for delta content)
= 2 PUTs + 1 HEAD = 3 RTTs ✓
```

**T4 (Branch ≤ 2).** Branch requires:
```
1 HEAD (resolve current HEAD)
1 PUT (create branch reference)
= 1 HEAD + 1 PUT = 2 RTTs ✓
```

### 4.4. Latency estimation

```
latency = Σ(GET_i × GET_latency) + Σ(PUT_i × PUT_latency) + ...

S3:     GET=20ms, PUT=30ms, LIST=100ms, HEAD=10ms, RANGE=15ms
Azure:  GET=15ms, PUT=25ms, LIST=80ms,  HEAD=8ms,  RANGE=12ms
R2:     GET=12ms, PUT=20ms, LIST=60ms,  HEAD=6ms,  RANGE=10ms
Local:  GET=0.1ms, PUT=0.1ms, LIST=0.5ms, HEAD=0.05ms, RANGE=0.1ms
```

---

## 5. Object Store Native Specification

### 5.1. Definition

A system is **Object Store Native** if it satisfies:

**OSN1 (Append-only writes).** All writes create new immutable
objects. No in-place updates, no appends to existing objects.

**OSN2 (No rename).** Object names are immutable once written.
"Moving" an object = copy + delete (or just create a new reference).

**OSN3 (No directory assumptions).** The system does not rely on
directory listing, directory atomicity, or hierarchical namespace
operations. Prefix listing is the only namespace operation.

**OSN4 (Bounded RTT).** Every operation has a bounded number of
object store round trips, independent of data size or history depth.

**OSN5 (Eventual consistency tolerant).** The system works correctly
under eventual consistency (S3's read-after-write consistency for
new objects; eventual consistency for overwrites of existing keys —
though Pond uses content-addressed blobs, so overwrites don't happen).

**OSN6 (Resumable).** Long operations can be resumed after interruption.
No operation requires holding state across RTTs (each RTT is independent).

**OSN7 (No local metadata dependence).** The system does not require
a local metadata database for correctness. Local caches are performance
optimizations, not correctness requirements. (The kernel's root
namespace can be stored as objects, not as a local SQLite file.)

**OSN8 (Range-read friendly).** The system can read partial objects
(range reads) for efficient access to large objects (pack files, tree
nodes).

### 5.2. Pond's compliance

| Property | Compliant? | Notes |
|---|---|---|
| OSN1 (Append-only) | ✓ | Blobs are immutable (A1) |
| OSN2 (No rename) | ✓ | References are set, not renamed |
| OSN3 (No directories) | ✓ | Names are flat strings; prefix is convention |
| OSN4 (Bounded RTT) | Partial | Lookup=3, Scan=4, Branch=2. Merge=unbounded (needs diff-based). |
| OSN5 (Eventual consistency) | ✓ | Content-addressed blobs are never overwritten |
| OSN6 (Resumable) | Partial | Commits are atomic. Scans could be resumed (future work). |
| OSN7 (No local metadata) | ✗ | Current kernel uses SQLite for root namespace. Needs object-store backend. |
| OSN8 (Range reads) | Partial | Pack files support range reads. Tree nodes don't yet. |

**Gap:** OSN7 is the biggest gap. The current SQLite root namespace
is a local metadata dependency. An object-store-native root namespace
(each reference is a separate object) would close this gap.

---

## 6. Physical Structure Taxonomy

### 6.1. Classification

```
Physical Structures (f(snapshot) → artifact, deterministic, rebuildable)
├── Search Structures
│   ├── Secondary indexes (Prolly tree: key → blob_hash)
│   ├── Bloom filters (key membership test)
│   ├── Trie indexes (prefix search)
│   └── Vector indexes (ANN: HNSW, IVF)
│
├── Statistics
│   ├── Histograms (value distribution)
│   ├── Sketches (HLL, count-min — deterministic if seed is fixed)
│   ├── Zone maps (min/max per chunk)
│   └── Column statistics (count, null_count, distinct_count)
│
├── Layout Structures
│   ├── Pack files (multiple blobs in one object)
│   ├── Chunk manifests (blob → physical location mapping)
│   └── Sort orders (sorted by a key for range queries)
│
├── Derived Data
│   ├── Materialized views (precomputed query results)
│   ├── Aggregates (SUM, COUNT, AVG per group)
│   └── Feature vectors (computed from raw data)
│
└── Execution Structures
    ├── Query plans (compiled execution strategies)
    └── Cached results (NOT a Physical Structure — see below)

Cache (separate category — depends on access patterns, not just snapshot)
├── Block cache (recently read chunks)
├── Result cache (recently computed query results)
└── Metadata cache (recently resolved references)
```

### 6.2. Why cache is separate

Cache violates P1 (Determinism): the cache content depends on **what
was accessed**, not just on the snapshot. Two identical snapshots
with different access patterns produce different cache contents.

Cache is an **access-pattern optimization**, not a Physical Structure.
It is:
- Not deterministic (depends on access history)
- Not rebuildable from the snapshot alone
- Not safe to share across workloads (different access patterns)

### 6.3. Laws per category

| Category | P1 (Deterministic) | P2 (Rebuildable) | P3 (Independent) | P4 (Composable) |
|---|---|---|---|---|
| Search | ✓ | ✓ | ✓ | ✓ |
| Statistics | ✓ (if seed fixed) | ✓ | ✓ | ✓ |
| Layout | ✓ | ✓ | ✓ | ✓ |
| Derived Data | ✓ | ✓ | ✓ | ✓ |
| Execution | ✓ | ✓ | ✓ | ✓ |
| **Cache** | **✗** | **✗** | **✓** | **N/A** |

---

## 7. Workspace Algebra

### 7.1. The problem

Currently, each Lens instance has its own staging area. This means:
- Cross-Lens atomic writes are impossible
- Two Lenses editing the same Collection can't share a transaction
- There is no rollback (once staged, changes can't be selectively undone)

### 7.2. Workspace definition

A **Workspace** is a staging area that is independent of any Lens:

```
Workspace = (Collection, StagedChanges, Savepoints)

StagedChanges = {add: dict[key, hash], del: set[key]}
Savepoints = [StagedChanges]  # for rollback
```

### 7.3. Workspace operations

```
begin(collection) → Workspace
stage(ws, key, hash) → ()           # stage a write
stage_delete(ws, key) → ()          # stage a deletion
savepoint(ws) → savepoint_id        # create a rollback point
rollback_to(ws, savepoint_id) → ()  # rollback to a savepoint
commit(ws, message) → commit_hash   # commit all staged changes
abort(ws) → ()                      discard all staged changes
```

### 7.4. Workspace laws

**W1 (Isolation).** Changes in a Workspace are not visible to other
Workspaces or readers until `commit`.

**W2 (Atomicity).** `commit(ws)` either commits all staged changes
or none. There is no partial commit.

**W3 (Savepoint rollback).** `rollback_to(ws, sp)` discards all
changes staged after `sp` but keeps changes staged before `sp`.

**W4 (Lens independence).** A Workspace is not bound to a Lens. Any
Lens can stage changes to the same Workspace. The Lens provides
encode/decode; the Workspace provides staging/commit.

**W5 (Workspace is ephemeral).** A Workspace lives in memory (or
in a temporary reference). It is not part of the commit history
until committed.

### 7.5. How this changes the hierarchy

```
Kernel (Bytes, History, Names)
    ↓
Workspace (staging, transactions, savepoints)
    ↓
Lens (interprets bytes, encodes/decodes)
    ↓
Physical Structures (accelerates access)
    ↓
Applications
```

The Lens becomes **pure**: it only encodes/decodes. It doesn't own
staging. The Workspace owns staging. This separation makes the Lens
simpler and enables cross-Lens transactions.

---

## 8. History as a Mathematical Object

### 8.1. The question

Is a linked list (or DAG) of commits the right structure for history?

### 8.2. What history represents

History represents **the sequence of state transitions** that produced
the current state. Formally:

```
History = (S₀, σ₁, S₁, σ₂, S₂, ..., σₙ, Sₙ)

where Sᵢ is a state and σᵢ is the operation that transformed Sᵢ₋₁ to Sᵢ
```

### 8.3. Current representation

```
Commitₙ → parent → Commitₙ₋₁ → parent → ... → Commit₀
```

This is a **linked list** (or DAG with merge commits). Each commit
stores either a full snapshot (Sᵢ) or a delta (σᵢ).

### 8.4. Alternative representations

**Option A: Prolly tree of commits**
- Commits are stored in a Prolly tree keyed by commit hash
- O(log N) access to any commit
- But: loses the parent→child ordering (must be stored separately)

**Option B: Skip pointers (Git commit-graph)**
- Each commit stores skip pointers to ancestors at exponentially
  increasing distances
- O(log N) traversal to any ancestor
- Simple to implement; doesn't change the commit format much

**Option C: Event log (not commits)**
- History is an append-only log of events (operations), not commits
- Events are: `put(key, hash)`, `delete(key)`, `snapshot(tree_root)`
- The current state is derived by replaying the event log
- Like an event sourcing system
- O(N) replay, but can be short-circuited by snapshots

**Option D: Segmented history**
- History is divided into segments (epochs)
- Each epoch has a summary (snapshot + commit range)
- O(epochs) to find the right epoch, then O(within_epoch) for detail
- Like Git's packfile + commit-graph combination

### 8.5. Analysis

| Option | Access | Traversal | Complexity | Novel? |
|---|---|---|---|---|
| Linked list (current) | O(N) | Linear | Simple | No |
| Prolly tree of commits | O(log N) | Tree walk | Medium | Somewhat |
| Skip pointers | O(log N) | Skip walk | Simple | No (Git does this) |
| Event log | O(N) replay | Sequential | Simple | No (event sourcing) |
| Segmented history | O(epochs + within) | Hierarchical | Medium | Somewhat |

**Recommendation:** Skip pointers (Option B) are the pragmatic answer.
They're simple, proven (Git commit-graph), and give O(log N) history
access without changing the commit model. The commit format adds
`skip_parent` fields at exponentially increasing distances.

### 8.6. The deeper question

**Is history itself a Physical Structure?**

If history = `f(all_commits) → history_graph`, then:
- History is deterministic (given the same commits, same graph)
- History is rebuildable (from the commit blobs)
- History is independent (computing it doesn't modify commits)

**Yes, history is a Physical Structure.** The commit graph is a
derived structure — it can be computed from the commit blobs. The
`parent` and `second_parent` fields in commits are the source data;
the graph traversal is the function.

This means: **the history graph can be cached, precomputed, and
stored as a Physical Structure** (like Git's commit-graph file).
The kernel doesn't need to maintain the graph — it just stores
commits. The graph is derived.

---

## Summary

| Algebra | Status | Key Insight |
|---|---|---|
| **Reference** | Formalized | References are the only mutable state. All roles (HEAD, branch, snapshot, tag, workspace, lock) are just `Ref(name, hash)` with different naming conventions. |
| **Merge** | Formalized | Three layers: kernel (topology), Lens (semantics), Application (policy). Merge cost can be reduced from O(\|A\|+\|B\|) to O(\|changed\|) via diff-based merge. |
| **GC** | Formalized | Tracing GC (mark + sweep). Expensive on object stores. Manifest-based GC (1 GET) is a Physical Structure optimization. |
| **RTT** | Formalized | Every operation has a cost vector (GET, PUT, LIST, HEAD, RANGE). Theorems T1-T4 define target RTTs. Embedding snapshot root in HEAD reference achieves T1 (lookup ≤ 3). |
| **Object Store Native** | Defined | 8 properties (OSN1-OSN8). Pond is compliant on 6, partial on 2 (OSN4 merge, OSN7 local metadata). |
| **Physical Structure Taxonomy** | Classified | 5 categories (Search, Statistics, Layout, Derived, Execution) + Cache (separate, not a Physical Structure). |
| **Workspace** | Formalized | Staging independent of Lens. W1-W5 laws. Enables cross-Lens transactions and savepoint rollback. |
| **History** | Analyzed | History is a Physical Structure (derivable from commit blobs). Skip pointers (Option B) are the pragmatic O(log N) answer. |
