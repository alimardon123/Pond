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
| OSN7 (No local metadata) | ✓ (ObjectStoreNativeKernel) / ✗ (PondMinimal) | `ObjectStoreNativeKernel` stores refs as content-addressed blobs — no SQLite. `PondMinimal` (legacy) still uses SQLite. New code should use `ObjectStoreNativeKernel`. |
| OSN8 (Range reads) | Partial | Pack files support range reads. Tree nodes don't yet. |

**Gap (closed):** OSN7 was the biggest gap. The legacy `PondMinimal` kernel
uses SQLite for the root namespace. The new `ObjectStoreNativeKernel`
(`pond-core/object_store_native_kernel.py`) closes this gap — refs are
stored as content-addressed blobs in the object store (root pointer →
root ref blob → name→hash dict). New code should use `ObjectStoreNativeKernel`;
`PondMinimal` remains for backward compatibility and local-disk testing.

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

---

# Part II — Post-Red-Team Algebras

> The Second Red Team Review (`POND_SECOND_RED_TEAM.md`) found 13
> attacks: 5 hidden primitives, 2 false laws, 2 collapses, 4
> under-specifications. The following six algebras are the model's
> response. They are *additive in clarity* but *subtractive in
> surface area*: every new algebra either closes a hidden-primitive
> attack or collapses two prior algebras into one.

## 9. Substrate Algebra (closes A1, A2, A12, A13)

### 9.1. The correction

The kernel is **not** three primitives. It is **six substrates**,
each with its own axioms and operations. The "three operations"
story is the user-facing API; the "six substrates" story is the
model.

```
┌─────────────────────────────────────────────────────────────┐
│ Substrate   │ Axioms                                  │ Ops  │
├─────────────────────────────────────────────────────────────┤
│ Bytes       │ A1 Immutability, A2 Content-addressing   │      │
│             │                                         │ Write(b)→h            │
│             │                                         │ Read(h)→b             │
│             │                                         │ ReadRange(h,off,len)  │
├─────────────────────────────────────────────────────────────┤
│ Names       │ A3 Last-writer-wins, A4 Referential     │ Ref(n,h), get(n),     │
│             │   integrity                             │ list(p), del(n),      │
│             │                                         │ CAS(n,exp,new)        │
├─────────────────────────────────────────────────────────────┤
│ Time        │ A5 Monotonic logical clock              │ now()→t,              │
│             │                                         │ compare(t₁,t₂)→<,=,> │
├─────────────────────────────────────────────────────────────┤
│ Coordination│ A6 Atomic commit blob,                  │ commit_blob(writes)  │
│ (optional)  │ A7 Cross-Collection coordinator is      │ →h                   │
│             │   out-of-model                          │                       │
├─────────────────────────────────────────────────────────────┤
│ Range-Read  │ A8 Range reads are first-class; backend │ (folded into Bytes)  │
│             │   may decompose to full-read+slice      │                       │
└─────────────────────────────────────────────────────────────┘
```

### 9.2. New axioms

**A5 (Monotonic logical clock).** There exists a function `now()`
returning a timestamp `t`, and a total order `compare(t₁, t₂)`.
The clock is monotonic within a single process: for any two
operations `o₁` before `o₂` in process order, `now(o₁) ≤ now(o₂)`.
Across processes, the clock is only *causally* consistent: if
`o₁ → o₂` (happens-before) then `now(o₁) < now(o₂)`.

This is Lamport's clock. It is the weakest clock that supports
timestamp-merge (§2.3) and freshness checks. Wall-clock time is
explicitly *not* required; logical clocks suffice.

**A6 (Atomic commit blob).** A "commit blob" is a single blob
that lists a set of `(name, hash)` writes. Updating a single Ref
to point at a commit blob is atomic (by A3). Readers that follow
the commit blob observe either all writes or none.

This is the kernel's only atomicity mechanism for multi-name
writes. It works only when all writes are reachable from a single
root reference (e.g., HEAD of one Collection). Cross-Collection
atomicity is not provided.

**A7 (Coordinator is out-of-model).** Cross-Collection atomic
writes, distributed transactions, and linearizable reads require
a coordinator substrate (2PC, Raft, Paxos). The model does not
specify one. Applications requiring these must layer a coordinator
on top of the kernel.

**A8 (Range reads are first-class).** `ReadRange(hash, offset,
length) → bytes` is a kernel operation. Its semantics: return
exactly the bytes `[offset, offset+length)` of the blob identified
by `hash`. The backend may implement this as a true range read
(S3 `Range` header), as a full read with in-memory slicing (local
disk small blob), or as a partial read with caching. The operation
is *visible in the cost model*: a Range Read costs 1 RANGE RTT
on S3, regardless of how the backend implements it.

### 9.3. Laws

**S1 (Substrate independence).** Each substrate can be replaced
independently. The bytes substrate can be local disk, S3, or
IPFS. The names substrate can be SQLite, FoundationDB, or a
directory of small files. The time substrate can be a Lamport
clock, an NTP-synchronized clock, or a TrueTime oracle. The
choice is a backend property.

**S2 (Substrate coupling).** The only substrate couplings are:
- Names substrate references bytes substrate (A4: referential
  integrity).
- Time substrate timestamps appear in commit blobs (which are
  bytes).

No other substrate references another. This is what makes the
model composable: backends can vary per substrate.

---

## 10. Manifest Algebra (closes A7, M6)

### 10.1. The problem

Reachability (§3.3) requires walking transitive blob references.
On object stores, walking = reading = RTTs. A 1M-blob Collection
would require 1M GETs to mark reachable. This is unacceptable.

The fix is a **Manifest**: a single blob (or small set of blobs)
that lists every blob hash in a pack. Reachability then reduces
to "is the hash in any reachable manifest?" — manifest reads, not
blob reads.

But manifests introduce a *new* concept: **physical reachability**
differs from **logical reachability**. The model must formalize
both.

### 10.2. Definitions

```
Manifest = (Pack, HashList)

Pack    = blob containing multiple objects, content-addressed
HashList = [hash₁, hash₂, ..., hashₙ]  -- hashes of objects in the pack
```

A Manifest is itself a blob, content-addressed. It is stored via
`Write(manifest_bytes) → manifest_hash`. A Reference may point
to a manifest.

### 10.3. Two reachability definitions

**Logical reachability (LR).**

```
LR(blob) = ∃ ref. path(ref → hash → blob)

where path traverses blob contents to find embedded hashes.
```

This is the "true" reachability — the blob is referenced by
something that is referenced by ... a Ref.

**Physical reachability (PR).**

```
PR(blob) = ∃ (ref, manifest). ref → manifest_hash → manifest
                                 ∧ blob ∈ manifest.HashList
                                 ∧ manifest.Pack contains blob's bytes
```

This is the "manifest-based" reachability — the blob is in a
pack whose manifest is reachable.

### 10.4. Equivalence theorem

**MAN1 (LR ⟺ PR when manifests are complete).** If every blob is
in some pack, and every pack has a manifest, and every manifest
is reachable from some Ref, then `LR(blob) ⟺ PR(blob)`.

**Proof sketch.**
- (⟹) Suppose `LR(blob)`. Then there is a path `ref → ... → blob`.
  Every node on the path is a blob; every blob is in some pack;
  every pack has a manifest; every manifest is reachable.
  Therefore `blob` is in some pack whose manifest is reachable.
  So `PR(blob)`.
- (⟦) Suppose `PR(blob)`. Then `blob` is in a pack whose manifest
  is reachable. The pack contains the bytes of `blob`. So `blob`
  is reachable. So `LR(blob)`.

**Corollary.** Under the conditions of MAN1, GC can use PR (cheap:
manifest reads only) instead of LR (expensive: blob walks).

### 10.5. Manifest laws

**MAN2 (Manifest is a Physical Structure).** A manifest is a
function of the pack it indexes: `f(pack) → manifest`. If lost,
it can be rebuilt by re-reading the pack. (P1: rebuildable.)

**MAN3 (Manifest may be stale).** A manifest lists the hashes in
the pack *at the time the manifest was written*. If the pack is
immutable (it is, by A1), the manifest is never stale. But if the
*pack reference* changes (a new pack replaces an old one), the old
manifest is orphaned. GC must mark both the old pack and the old
manifest.

**MAN4 (Manifest composition).** A "root manifest" can list
multiple pack manifests: `RootManifest = [pack_manifest_hash₁,
..., pack_manifest_hashₖ]`. Reachability via root manifest:
1 RTT to read root, k RTTs to read pack manifests (parallelizable),
then in-memory hash lookup. Total: 1 + k RTTs for full
reachability check.

### 10.6. Cost model

| Operation | RTTs | Without manifest |
|---|---|---|
| Check reachability of 1 blob | 1 GET (root manifest) + 1 GET (pack manifest) | O(depth) GETs |
| Full GC mark | 1 + k RTTs (k = #packs) | O(blobs) GETs |
| Full GC sweep | O(orphaned_blobs) DELETEs | same |

For a Collection with 1M blobs in 1000 packs of 1000 blobs each:
- Without manifest: 1M GETs to mark.
- With manifest: 1001 GETs to mark. **1000× speedup.**

### 10.7. Closing A7

The Manifest algebra breaks the circularity in §3.3 (GC claims to
operate on the kernel but actually operates on a manifest-augmented
kernel). The model now has two GC algebras:

- **GC-LR** (§3): logical reachability, for backends without packs.
- **GC-PR** (§10): physical reachability, for backends with packs.

Both are valid. The backend chooses. Equivalence holds when
manifests are complete (MAN1).

---

## 11. Range Read Algebra (closes A13, M1)

### 11.1. Why Range Read is a separate operation

`Read(hash)→bytes` returns the entire blob. For pack files (GBs),
this is wrong. For tree nodes (KBs), this is fine. The kernel
needs *both* operations, with different cost models.

### 11.2. Definition

```
ReadRange : hash × offset × length → bytes

where:
  hash   ∈ {0,1}^256
  offset ∈ ℕ  (byte offset, 0-indexed)
  length ∈ ℕ  (byte length, ≥ 1)
```

Returns exactly `bytes[offset : offset+length]`. If
`offset+length > |blob|`, returns up to end of blob.

### 11.3. Laws

**RR1 (Equivalence with Read).** `Read(h) = ReadRange(h, 0, |b|)`
where `b = Read(h)`. Range read with full extent is identical to
full read.

**RR2 (Composability).** `ReadRange(h, off, len) = ReadRange(h,
off, k) || ReadRange(h, off+k, len-k)` for any `0 < k < len`.
Range reads compose by concatenation. (This is what makes
streaming scans work: read 4KB at a time.)

**RR3 (Cost is per-range, not per-byte).** On S3, each Range Read
costs 1 RTT (10-30ms) regardless of length. The total RTT for a
scan is `ceil(scan_bytes / chunk_size)`. Smaller chunks → more
RTTs. This is a fundamental tension: small chunks enable
fine-grained caching but increase RTT count.

**RR4 (Backend may decompose).** The backend is free to implement
`ReadRange(h, off, len)` as `Read(h)` + in-memory slice when the
blob is small. This is invisible to the caller but visible in the
cost model: the call counts as 1 GET (full read), not 1 RANGE.

### 11.4. Cost model

| Operation | S3 RTT | S3 ms | Notes |
|---|---|---|---|
| `Read(h)` (small blob, <1MB) | 1 GET | 20 | Full read |
| `Read(h)` (large blob, >1MB) | 1 GET | 20 + 5/MB | Bandwidth-limited |
| `ReadRange(h, off, len)` (small range, <1MB) | 1 RANGE | 15 | Cheaper than GET for large blobs |
| `ReadRange(h, off, len)` (large range, >1MB) | 1 RANGE | 15 + 5/MB | Same per-byte cost as GET |

**Insight.** Range reads are *strictly cheaper* than full reads
when the caller needs less than the full blob. They are *equal*
when the caller needs the full blob. They are *never worse*.

### 11.5. Update to RTT Calculus (§4)

The RTT cost vector gains a sixth component:

```
Cost = (GET, PUT, LIST, HEAD, RANGE, bytes_transferred, parallelizable)
```

Theorems T1-T4 still hold (RANGE is interchangeable with GET in
the bounds). **T5 (Dollar bound)** is added:

**T5 (Dollar bound).** For any operation `op`:

```
cost(op) ≤ α·GETs + β·PUTs + γ·LISTs + δ·HEADs + ε·RANGEs
           + ζ·bytes_transferred

S3 (2026 pricing, USD):
  α = 0.0004 / 1000  (per GET)
  β = 0.005  / 1000  (per PUT)
  γ = 0.005  / 1000  (per LIST — 5× GET!)
  δ = 0.0004 / 1000  (per HEAD)
  ε = 0.0004 / 1000  (per RANGE — same as GET)
  ζ = 0.09 / GB       (egress to internet; 0 to S3-intra-region)
```

LISTs are 5× more expensive than GETs. The model must minimize
LISTs, not just RTTs.

### 11.6. Closing A13

Range Read is now a first-class kernel operation (§9.1 Bytes
substrate). The model no longer pretends that "read" is a single
concept.

---

## 12. State vs Bytes (closes A1)

### 12.1. The question

Should the primary substrate be "bytes" or "state"? Some systems
(Datomic, Nix) treat *state* as primary and bytes as one
serialization. Pond treats *bytes* as primary and state as one
interpretation. The red team asked: is this the right choice?

### 12.2. Definitions

- **Bytes substrate.** Primitive: `Write(b)→h`, `Read(h)→b`,
  `ReadRange(h,off,len)→b'`. Axioms: A1, A2.
- **State substrate.** Primitive: `Put(state)→id`,
  `Get(id)→state`. Axioms: state is immutable; state is
  content-addressed.

### 12.3. Analysis

The two are *not* equivalent. State requires a canonical
serialization, which requires a codec, which requires the kernel
to know about codecs. This violates L5 (kernel independence).

Concretely: if the substrate is "state," then `Put({...})` must
serialize `{...}` to bytes before hashing. The serialization
choice (JSON? msgpack? canonical CBOR?) is a kernel decision.
Two implementations of the kernel with different serializations
produce different hashes for the same state. Interop is broken.

If the substrate is "bytes," then `Write(b)→h` is codec-free.
The kernel hashes bytes; the application chooses the bytes. Two
applications can produce the same hash for the same logical state
*iff* they agree on the codec — and that agreement is
out-of-band (an application convention, not a kernel law).

**Verdict.** Bytes is primary. State is a Lens-level interpretation.
The kernel is bytes-only.

### 12.4. State algebra (for completeness)

A "state view" can be defined as a derived algebra:

```
StateView = (Lens, Snapshot)
  where Lens provides E, D (encode/decode)
  and   Snapshot = D(Read(h)) for some h

Put(state) = Write(E(state))     -- encode, then write bytes
Get(id)    = D(Read(id))         -- read bytes, then decode
```

State is *bytes + codec*. The codec is application-level. The
kernel never sees state.

### 12.5. Laws

**ST1 (State is derived).** State is a function of (bytes, codec).
Two different codecs produce two different states from the same
bytes. There is no "state" without a codec.

**ST2 (State hash depends on codec).** `hash(state) = hash(E(state))`.
The hash is over the *encoded* bytes, not over the abstract state.
Two different codecs produce two different hashes for the same
state.

**ST3 (State is never the kernel's concern).** The kernel hashes
bytes. The application hashes state (via its chosen codec). The
two layers do not communicate about state.

### 12.6. Closing A1

The substrate is bytes, not state. The "three primitives" claim
is rhetorical; the honest count is "six substrates, three
operations on the bytes substrate, two operations on the names
substrate, etc." State is a Layer-2 (Lens) concept.

---

## 13. GC with Packs and Manifests (closes A7, M6)

### 13.1. The two-phase structure

GC now has two phases, each with its own algebra:

```
Phase 1: MARK    (which blobs are reachable?)
Phase 2: SWEEP   (delete unreachable blobs)
```

Phase 1 has *two variants*:
- **MARK-LR**: logical reachability (walk blobs). O(blobs) GETs.
- **MARK-PR**: physical reachability (walk manifests). O(manifests) GETs.

Phase 2 is the same in both variants.

### 13.2. MARK-PR algorithm

```
GC-MARK-PR(kernel):
  reachable = ∅
  for each ref in list(all_refs):
    h = get(ref)
    if is_manifest(h):
      manifest = Read(h)
      for each pack_manifest_hash in manifest:
        pack_manifest = Read(pack_manifest_hash)
        reachable ∪= pack_manifest.HashList
        reachable ∪= {pack_manifest_hash}  # the manifest itself
    else:
      reachable ∪= {h}
      # if h is a tree, recurse with LR (no manifest for trees)
      # OR maintain a separate tree-manifest
  return reachable
```

Cost: 1 LIST (all refs) + 1 GET per ref + 1 GET per pack manifest.
For R refs and P packs: `1 LIST + R + P GETs`.

Compare to MARK-LR: 1 LIST + R GETs + B GETs (B = reachable blobs).
For R=1000, P=100, B=1M: MARK-PR = 1101 GETs; MARK-LR = 1,001,000
GETs. **1000× speedup.**

### 13.3. Sweep with packs

Sweep has two sub-cases:

**Sweep standalone blobs.** For each blob `b` not in `reachable`:
- DELETE the object on the backend (S3 DELETE).

**Sweep packs.** For each pack `p` whose manifest is not in
`reachable`:
- DELETE the pack object.
- DELETE the pack manifest object.

Packs *cannot* be partially swept. A pack is all-or-nothing: if
any blob in the pack is reachable, the entire pack is kept. This
is the cost of physical co-location.

**Insight.** Packs trade space for time. A pack keeps some
unreachable blobs (space cost) to enable manifest-based GC (time
savings). The breakeven is when the unreachable fraction is less
than the GC speedup ratio. For 1000× speedup, packs are worth it
even if 99% of the pack is unreachable.

### 13.4. Compaction

To reclaim space from packs with high unreachable fraction, use
**compaction**:

```
Compact(pack):
  1. Read pack manifest, identify reachable blobs
  2. Read each reachable blob from the pack (range reads)
  3. Write a new pack with only reachable blobs
  4. Write a new manifest for the new pack
  5. Ref-update: point the pack reference to the new manifest
  6. (GC later collects the old pack and old manifest)
```

Compaction is a maintenance operation, not a kernel operation. It
is triggered by policy (e.g., "compact when unreachable fraction
exceeds 25%").

### 13.5. Laws

**GC-P1 (Pack atomicity).** A pack is written once and never
modified. Updates create new packs; old packs become orphaned and
are swept.

**GC-P2 (Manifest integrity).** A manifest's HashList must match
the pack's contents exactly. If verification fails, the manifest is
corrupt and must be rebuilt (MAN2: rebuildable).

**GC-P3 (Compaction is idempotent).** Compacting an already-compact
pack (no unreachable blobs) produces an identical pack.

**GC-P4 (GC is conservative).** If the GC cannot determine
reachability (e.g., a manifest is missing), it treats the blob as
reachable. Never delete a possibly-reachable blob (G1: safety).

### 13.6. Closing A7

GC now operates over two layers:
- **Logical layer**: refs → blobs (MARK-LR).
- **Physical layer**: refs → manifests → packs → blobs (MARK-PR).

The two are equivalent under MAN1. Backends choose.

---

## 14. Physical Structure Dependency Graph (closes A5, A10)

### 14.1. The collapse

Per A5, the Physical Structure "algebra" was a tautology. We
collapse it to a single definition and one theorem, then redirect
the formal energy to the *dependency graph* — which structures
depend on which sources.

### 14.2. Definition (collapsed)

> **Definition.** A Physical Structure is an artifact `A` paired
> with a recompute function `f` such that `f(source) = A`.
>
> **Theorem P1 (Rebuildability).** Every Physical Structure can be
> lost without data loss, because `f` can recompute it.

All other claimed properties (determinism, independence,
composability) follow from this definition plus kernel A1.

### 14.3. The dependency graph

The interesting question is *what* `source` can be. There are
three candidate sources, and Physical Structures partition by
which they use:

```
Source types:
  S_snapshot  : a snapshot (Prolly tree root)
  S_commit    : a commit (blob in the commit graph)
  S_commitset : a set of commits (the entire commit graph or a subset)
```

| Physical Structure | Source | Why |
|---|---|---|
| Secondary index | `S_snapshot` | Index is over a state |
| Bloom filter | `S_snapshot` | Filter is over keys in a state |
| Zone map | `S_snapshot` | Stats are over chunks in a state |
| Histogram | `S_snapshot` | Distribution is over values in a state |
| Pack file | `S_snapshot` | Pack contains blobs of a state |
| Manifest | `S_snapshot` (or pack) | Manifest lists pack contents |
| Materialized view | `S_snapshot` | View is a query over a state |
| Feature vector | `S_snapshot` | Features computed from a state |
| Search index | `S_snapshot` | Index is over tokens in a state |
| **History graph** | `S_commitset` | Graph is over commits, not state |
| **Commit-graph** (Git style) | `S_commitset` | Skip pointers over commit set |
| **Branch list** | `S_commitset` | Branches are refs over commits |
| **Tag list** | `S_commitset` | Tags are refs over commits |

### 14.4. Dependency rules

**D1 (Sources are immutable).** Every source (snapshot, commit,
commitset) is immutable by A1. Therefore `f(source)` is a pure
function of an immutable input.

**D2 (Structures can depend on structures).** A materialized view
might depend on a secondary index: `view = f(snapshot, index)`.
Since `index = g(snapshot)`, by composition `view = f(snapshot,
g(snapshot)) = (f ∘ g)(snapshot)`. Still pure. (P4 composability,
preserved.)

**D3 (Structures do NOT depend on access patterns).** If a
structure depends on access patterns (e.g., cache, learned index
with online training, compaction schedule), it is **not a Physical
Structure**. It belongs to the Cache category (§6.2).

**D4 (Commitset-sourced structures survive snapshot loss).** A
history graph can be rebuilt from commits even if all snapshots
are lost (snapshots are themselves commit-sourced). The reverse
is false: snapshots cannot rebuild history (snapshots don't know
their ancestors).

**D5 (Snapshot-sourced structures survive commit loss only if the
snapshot is preserved).** If a commit is lost but its snapshot
blob survives, snapshot-sourced structures survive. If both are
lost, they are unrecoverable.

### 14.5. Dependency graph (visual)

```
                ┌─────────────┐
                │ Commitset   │ ← immutable
                │  (commits)  │
                └──────┬──────┘
                       │
            ┌──────────┼──────────┐
            │          │          │
            ▼          ▼          ▼
     ┌──────────┐ ┌─────────┐ ┌────────┐
     │ History  │ │ Branch  │ │  Tag   │
     │  graph   │ │  list   │ │  list  │
     └──────────┘ └─────────┘ └────────┘
            │
            ▼
       ┌─────────┐
       │ Commit  │ ← immutable
       └────┬────┘
            │
            ▼
       ┌─────────┐
       │Snapshot │ ← derived from commit
       │(Prolly) │
       └────┬────┘
            │
   ┌────────┼────────┬──────────┬──────────┐
   │        │        │          │          │
   ▼        ▼        ▼          ▼          ▼
┌─────┐ ┌──────┐ ┌──────┐ ┌─────────┐ ┌────────┐
│Index│ │Bloom │ │Zone  │ │Material-│ │  Pack  │
│     │ │filter│ │ map  │ │  ized   │ │ (blob) │
│     │ │      │ │      │ │  view   │ │        │
└─────┘ └──────┘ └──────┘ └─────────┘ └───┬────┘
                                          │
                                          ▼
                                     ┌──────────┐
                                     │ Manifest │
                                     │  (pack   │
                                     │  index)  │
                                     └──────────┘

Cache (off-graph; depends on access patterns, not on sources)
```

### 14.6. Closing A5 and A10

- **A5 (tautology)**: the algebra is collapsed to one definition
  + one theorem. The formal energy goes into the dependency graph.
- **A10 (history source)**: history is sourced from `S_commitset`,
  not from `S_snapshot`. The dependency graph makes this explicit.

---

## 15. Concurrency and Consistency Algebra (closes A2, A8, A12)

### 15.1. What the model guarantees

Given the six substrates (§9), and given that no coordinator
substrate is in-model (A7), the model guarantees the following
*and nothing more*:

| Property | Guaranteed? | Reason |
|---|---|---|
| Single-name atomicity | Yes | A3 (last-writer-wins on a single Ref) |
| Multi-name atomicity (within one Collection, via commit blob) | Yes | A6 (commit blob + single HEAD ref) |
| Multi-name atomicity (across Collections) | **No** | A7 (coordinator out-of-model) |
| Read-after-write on a single name | Yes (eventually) | OSN: object-store eventual consistency; content-addressed blobs are immutable so the only question is "has the Ref update propagated" |
| Read-after-write across names | **No** | Two Ref updates can interleave |
| Causal consistency | **No** | No vector clocks in-model; only A5 (Lamport) |
| Snapshot isolation | Within one Collection (via HEAD) | HEAD points at one commit; reads from HEAD are consistent |
| Linearizability | **No** | No coordinator |
| Serializable transactions | **No** | No coordinator |

### 15.2. Consistency levels (formalized)

**C0 (Blob immutability).** Once `Write(b) = h`, all subsequent
`Read(h)` return `b`. Always. Everywhere. (A1.)

**C1 (Ref eventual propagation).** After `Ref(name, h)`, eventually
all readers observe `get(name) = h`. Until then, some readers may
observe the old value. (Object-store eventual consistency.)

**C2 (Single-Ref atomicity).** A single `Ref(name, h)` is atomic:
either the old value or the new value is observed, never a mix.
(A3.)

**C3 (Commit-blob atomicity).** Updating HEAD to point at a commit
blob is atomic (C2). All writes inside the commit blob become
observable simultaneously. (A6.)

**C4 (Within-Collection snapshot isolation).** A reader that
resolves HEAD to commit `c` observes a consistent state derived
from `c`. Concurrent commits to the same Collection may change
HEAD, but the reader's view does not change mid-read.

**C5 (Cross-Collection: no guarantee).** A reader that reads
Collection A then Collection B may observe A at time `t_A` and B
at time `t_B < t_A`. There is no cross-Collection consistency.

### 15.3. Concurrency primitives

**Concurrency primitive 1: Optimistic concurrency via CAS.**

```
1. read get(name) → expected
2. compute new value
3. CAS(name, expected, new) → ok | conflict
4. if conflict: retry from step 1
```

This is the only concurrency primitive in-model. It works for
single-Ref updates. Multi-Ref updates require the commit-blob
trick (A6).

**Concurrency primitive 2: Commit-blob atomic batch.**

```
1. write all blobs (Write is concurrency-safe by A2)
2. write commit_blob listing all (name, hash) writes
3. CAS(HEAD, expected_head, commit_blob_hash) → ok | conflict
4. if conflict: another writer committed first; retry
```

This works for multi-name writes within one Collection. The CAS
on HEAD serializes commits.

**Concurrency primitive 3: Branches.**

A branch is a separate HEAD ref. Writers to different branches
do not conflict. Merging (§2) reconciles them.

### 15.4. Concurrency laws

**CC1 (CAS is the only atomic multi-step primitive).** All
in-model concurrency is built on CAS. There is no 2PC, no Raft,
no Paxos.

**CC2 (CAS requires backend support).** On backends without CAS
(plain S3 without conditional writes), the model degrades to
last-writer-wins with post-hoc conflict detection. This is the
honest statement; R3 (CAS) is conditional on backend support.

**CC3 (Cross-Collection atomicity is application responsibility).**
The application may layer a coordinator (2PC, Raft) on top of the
kernel. The model does not specify the coordinator's protocol.

**CC4 (Time is Lamport, not wall-clock).** Timestamps in commits
are Lamport timestamps (A5). They are monotonic within a process
and causally consistent across processes. They are NOT
wall-clock-comparable across processes. Merge policies that use
"latest timestamp wins" are well-defined only within a Lamport
order.

### 15.5. Closing A2, A8, A12

- **A2 (R1/R3 require metadata store with transactional semantics)**:
  the model now admits (CC2) that CAS is conditional on backend.
  R1 (atomicity of single Ref) holds; R3 (CAS) holds only on
  CAS-capable backends. On S3-without-conditional-writes, the
  model degrades to LWW + post-hoc detection.
- **A8 (W2 atomicity is a distributed transaction claim)**: W2 is
  restricted to within-Collection (via A6 commit blob). W4
  (cross-Lens) is restricted to within-Collection. Cross-Collection
  atomicity is out-of-model (CC3).
- **A12 (Time is a hidden primitive)**: Time is now a substrate
  (§9.1) with axiom A5 (Lamport). Wall-clock comparisons across
  processes are not supported (CC4).

---

## 16. Summary of Part II

| Algebra | Closes | Net effect |
|---|---|---|
| **Substrate** (§9) | A1, A2, A12, A13 | Promotes 5 substrates; demotes "3 primitives" to API |
| **Manifest** (§10) | A7, M6 | Adds physical reachability; breaks GC circularity |
| **Range Read** (§11) | A13, M1 | Adds `ReadRange` as kernel op; adds dollar-cost theorem |
| **State vs Bytes** (§12) | A1 | Settles: bytes primary, state derived |
| **GC with Packs** (§13) | A7, M6 | Two-phase GC; MARK-LR vs MARK-PR; compaction |
| **Physical Structure Dep Graph** (§14) | A5, A10 | Collapses PS algebra to def+theorem; adds dep graph |
| **Concurrency & Consistency** (§15) | A2, A8, A12 | Five consistency levels (C0-C5); CAS is only primitive |

**Net change to model surface area:**

- Substrates: 3 → 5 (added Time, Coordination; Range-Read folded
  into Bytes)
- Kernel operations: 3 → 4 (added `ReadRange`)
- Formal algebras: 8 → 14 (added 6 in Part II; collapsed 2)
- Axioms: 4 → 8 (added A5, A6, A7, A8)
- Laws: ~25 → ~30 (added 5 in Part II; reformed 2; withdrew 1)
- Theorems: 4 → 5 (added T5 dollar bound)
- Physical Structure properties: 4 (P1-P4) → 1 (rebuildability)
- OSN properties: 8 → 1 definition + 7 derived

The model is **smaller in concept count** (despite more algebras)
because the new algebras replace hand-wavy claims with formal
axioms. Every concept now has an axiom; every axiom has a law;
every law can be tested.

---

## 17. Open Questions Closed and Remaining

### Closed by Part II

1. ~~Should "State" replace "Bytes" as the primary primitive?~~
   **Closed (§12).** Bytes is primary. State is derived.

2. ~~Is Manifest a missing algebra?~~ **Closed (§10).** Yes; formalized.

3. ~~Is Range Read a missing algebra?~~ **Closed (§11).** Yes; promoted
   to kernel operation.

4. ~~How does GC work with packs?~~ **Closed (§13).** Two-phase GC
   with MARK-LR and MARK-PR.

5. ~~What's the Physical Structure dependency graph?~~ **Closed (§14).**
   Three source types; dep graph formalized.

6. ~~What does the model guarantee under concurrency?~~ **Closed (§15).**
   Five levels (C0-C5); CAS is the only primitive.

### Remaining open

7. **Replication.** The model has no replication algebra. Replication
   is "copy Refs and blobs to another backend." But consistency
   across replicas is unspecified. (Likely answer: per-replica
   last-writer-wins; cross-replica convergence via Epidemic/BiMehl
   or Raft-on-coordinator-substrate. Not yet formalized.)

8. **Compression.** Compressed blobs break `ReadRange` (offset is
   into compressed bytes, not logical bytes). Either: (a) compression
   is a Lens-level concern (the Lens compresses before Write), or
   (b) the kernel has a "compressed blob" type. Decision deferred.

9. **Encryption.** Same as compression: Lens-level or kernel-level?
   Decision deferred.

10. **Schema evolution.** How does a Lens change its codec without
    breaking old blobs? (Current answer: it can't; new codec =
    new Lens = new name prefix. This is restrictive. Alternative:
    codec-versioning in the name prefix, e.g., `sql_v2/`. Not yet
    formalized.)

These four are deferred to Part III, pending a third red team
round focused on operations (replication, encryption-at-rest,
schema evolution, multi-region) rather than on the model itself.

---

# Part III — Post-Third-Red-Team Algebras (Operations)

> The Third Red Team Review (`POND_THIRD_RED_TEAM.md`) found 13
> attacks: 5 hidden primitives, 3 false laws, 4 operational
> hazards, 1 collapse. The four operational questions deferred
> from Part II §17 are answered below by three new algebras
> (Replication, Transport, Schema Evolution) and amendments to
> three existing algebras (Range Read, GC, Physical Structure
> Dependency Graph).
>
> After Part III, the model has **0 open questions**. Phase K
> (model falsification) is complete.

## 16. Replication Algebra (closes B1, B5, B7, B11)

### 16.1. The correction

Replication is **not** a copy operation. It is a *convergence
contract* between replicas. The model picks the simplest
contract that is consistent with A7 (coordinator out-of-model):
**single-writer per Ref**.

### 16.2. Topology

```
┌─────────────────────┐
│  Primary Region     │
│  (single writer)    │
│                     │
│  All Ref writes     │
│  go here.           │
└──────────┬──────────┘
           │ async replication
           │ (commit blob + blobs)
           ▼
┌─────────────────────┐
│  Secondary Region   │
│  (read-only)        │
│                     │
│  Serves reads from  │
│  last-replicated    │
│  commit.            │
└─────────────────────┘
```

The primary is the single writer for each Ref. Secondaries
replicate the primary's commit stream. There is no
multi-writer convergence protocol in-model.

### 16.3. The convergence contract

**REP1 (Single writer per Ref).** For each Ref `name`, there is
exactly one primary at any time. All `Ref(name, ...)` writes go
to the primary. The primary's order is the canonical order.

**REP2 (Secondary reads are stale).** A secondary may serve reads
that reflect a commit `c` such that `c ≤ primary_HEAD`. The
staleness bound is backend-specific (S3 cross-region replication
latency, typically seconds to minutes).

**REP3 (Replication unit is the commit blob).** Replication copies
commit blobs + the blobs they reference. A commit blob is
self-contained: it lists all `(name, hash)` writes atomically
(per A6). Replicating a commit blob replicates all its writes
atomically.

**REP4 (Blob replication must precede commit replication).** The
primary writes all blobs first, then writes the commit blob. A
secondary that observes the commit blob can be sure all referenced
blobs are already replicated. This is the **happens-before** edge
from blobs to commit blob.

### 16.4. Tombstone barrier

**The problem (B5):** The primary may compact (Part II §13.4),
orphaning old blobs. If the primary deletes the old blobs before
the secondary has observed the new commit blob, the secondary's
attempt to read the old commit's blobs fails.

**The fix:**

**G6 (Tombstone barrier).** GC must not delete a blob until all
secondaries have observed a commit blob that does not reference
it. Equivalently: a blob is "safe to delete" only after
`deletion_grace_period` has elapsed since the orphaning commit.

```
GC safety check:
  for each blob b marked as orphaned at commit c_orphan:
    deletion_allowed_time(b) = c_orphan.timestamp + deletion_grace_period
    if now() < deletion_allowed_time(b):
      skip b  # barrier: secondary may still need it
```

The `deletion_grace_period` is a kernel-configurable parameter.
Typical values: 24 hours (single-region with cross-region
replication); 7 days (multi-region with cold storage); 0 (no
replication, immediate GC).

### 16.5. Failover contract

**The problem:** When the primary fails, a secondary must be
promoted. What is the consistency impact?

**REP5 (Failover loses in-flight writes).** If the primary fails
before a commit blob is replicated, that commit is lost. The new
primary starts from the last replicated commit. This is
last-replicated-wins, not last-written-wins.

**REP6 (Failover requires explicit promotion).** Promotion is an
operational action, not a kernel operation. The kernel does not
detect primary failure; the application (or a coordinator
substrate per A7) does.

### 16.6. Replication laws

**REP7 (Convergence is eventual).** If the primary stops
accepting writes and the system is left idle, all secondaries
eventually converge to the primary's final state. Bounded by
replication lag.

**REP8 (No multi-writer convergence).** If two regions both
attempt to write the same Ref, the model does not define a
merge. Application-level conflict resolution is required. (This
is the explicit deferral: distributed consensus is out-of-model
per A7.)

**REP9 (Replication is one-directional).** Primary → secondary.
Secondary → primary writes are not supported in-model. (Two
primaries each owning disjoint Ref sets is possible — this is
sharding, not multi-writer.)

### 16.7. Cost model

| Operation | Primary cost | Secondary cost |
|---|---|---|
| Write (commit blob) | 1 PUT (commit blob) + N PUTs (data blobs) + 1 CAS (HEAD) | 0 (async) |
| Read | 1 HEAD + 1 GET (commit) + tree GETs + blob GET | same as primary (local replica) |
| Replication lag | — | O(commit_blob_size + referenced_blob_sizes) / bandwidth |
| Failover | 0 (new primary promoted) | reads serve from last-replicated commit |

### 16.8. Closing B1, B5, B7, B11

- **B1 (replication is not copy):** the Replication Algebra
  formalizes the convergence contract (REP1-REP9).
- **B5 (compaction interaction):** G6 (tombstone barrier) delays
  deletion until secondaries ack.
- **B7 (multi-region):** confirmed out-of-model (REP9). Multi-region
  is an application-level topology choice; the model describes
  the convergence contract, not the region layout.
- **B11 (Lamport clocks insufficient):** REP1 sidesteps the
  problem by requiring single-writer per Ref. Lamport clocks
  (A5) are sufficient within a single writer's stream.

---

## 17. Transport Algebra (closes B2, B3, B6, B8, B10, B13)

### 17.1. The collapse (B13)

Compression, encryption, and checksumming are *not* Lens-level
concerns. They are *transport-layer* concerns, sitting between
the kernel (raw bytes) and the Lens (interpreted state). All
three are folded into one algebra: the **Transport Algebra**.

```
┌──────────────────────────────────────────┐
│ Application                              │
├──────────────────────────────────────────┤
│ Lens (encode/decode; schema-aware)       │
├──────────────────────────────────────────┤
│ Transport (compress → encrypt → checksum)│  ← new layer
├──────────────────────────────────────────┤
│ Kernel (Write, Read, ReadRange, Ref)     │
└──────────────────────────────────────────┘
```

The Lens sees plaintext, uncompressed bytes. The kernel stores
encrypted, compressed bytes. The Transport Layer translates.

### 17.2. Layer order (B10)

**A10 (Compress before encrypt).** The transport pipeline on
write is:

```
write(b):
  compressed = compress(b, dict)
  encrypted  = encrypt(compressed, key, nonce)
  checksum   = AEAD_tag(encrypted)  # GCM modes tag inline
  block_index = build_block_index(encrypted)
  kernel.Write(block_index || encrypted)
```

On read:

```
read(h):
  raw = kernel.Read(h)
  block_index, encrypted = split(raw)
  compressed = decrypt(encrypted, key)
  b = decompress(compressed, dict)
  return b
```

Encrypting after compression preserves the compression ratio
(compressed bytes are high-entropy, encryption preserves entropy).
Encrypting before compression would defeat compression.

### 17.3. Block index for range reads (B2, B3)

A transport-encoded blob cannot be range-read at arbitrary byte
offsets. The fix is a **block index** — a sidecar manifest that
maps logical byte offsets to physical byte offsets, one entry per
compression/encryption block.

```
BlockIndex = [(logical_offset, physical_offset, length, nonce)]
```

The block index is stored at the *start* of the blob. A range
read on a transport-encoded blob is:

```
ReadRange(h, logical_off, logical_len):
  raw = kernel.ReadRange(h, 0, header_size)  # read block index
  block_index = parse(raw)
  blocks_to_read = select_blocks(block_index, logical_off, logical_len)
  result = b""
  for block in blocks_to_read:
    raw_block = kernel.ReadRange(h, block.physical_offset, block.length)
    decrypted = decrypt(raw_block, key, block.nonce)
    decompressed = decompress(decrypted, dict)
    result += slice_to_logical_range(decompressed, block, logical_off, logical_len)
  return result
```

Cost: 1 small range read (block index) + K range reads (one per
block overlapping the logical range). For 4KB blocks and a 1MB
range, K ≈ 256. Total: 257 range reads vs 1 full read. Wins when
the range is much smaller than the blob.

### 17.4. Key management (B3)

**Envelope encryption** (AWS KMS / GCP KMS / Vault style):

- **Master key** lives in a KMS. Never touches the kernel.
- **Data Encryption Key (DEK)** is generated per blob (or per
  Collection, or per Lens — policy choice).
- DEK is encrypted by the master key; the encrypted DEK is
  stored alongside the blob.
- The kernel stores: `block_index || encrypted_DEK || encrypted_blob`.
- On read: application calls KMS to decrypt the DEK, then uses
  the DEK to decrypt the blob.

**Key substrate promotion:**

The kernel gains a **Key substrate** (sixth substrate, optional).
Operations:

```
wrap(DEK, master_key_id) → wrapped_DEK
unwrap(wrapped_DEK, master_key_id) → DEK
```

The kernel calls the KMS (via the Key substrate) to unwrap DEKs.
The kernel never holds the master key.

### 17.5. Dedup under encryption (B3 caveat)

**TR1 (Dedup is broken under encryption).** Two identical
plaintexts, encrypted with different nonces, produce different
ciphertexts, different hashes, no dedup.

This is *accepted*, not fixed. Two reasons:

1. **Security requires non-deterministic encryption.** Same
   plaintext → same ciphertext would leak information (frequency
   analysis, equality testing).
2. **Content-addressing is on ciphertext, not plaintext.** The
   kernel hashes the encrypted bytes. Two writes of the same
   plaintext produce two different blobs.

**Mitigation:** if dedup is critical, encrypt at the Collection
level (one DEK per Collection) and accept per-Collection dedup
instead of global dedup. Or use deterministic encryption (SIV
mode) for non-sensitive data — but this is a Lens-level choice,
not a kernel law.

### 17.6. Compression dictionaries (B6)

**Per-snapshot dictionary (pure):**

```
f(snapshot) = compress(snapshot, dict(snapshot))
```

The dictionary is a function of the snapshot. Pure. But:
- Dictionary training is expensive (seconds to minutes).
- Cross-snapshot compression is lost (each snapshot has its own
  dictionary; common patterns across snapshots are not exploited).

**Shared dictionary (impure):**

```
f(snapshot, dict) = compress(snapshot, dict)
```

The dictionary is external state. Two kernels with different
dictionaries produce different hashes for the same logical
state. Dedup across dictionaries is broken.

**TR2 (Dictionary is a sidecar).** A shared dictionary is stored
as a blob, content-addressed, referenced by the Collection's
metadata. The Transport Layer looks up the dictionary by hash
before decompressing. The dictionary is *not* part of the
snapshot — it is a Transport-layer artifact.

```
Collection metadata:
  ...
  transport_dict_hash: <hash of dict blob>
```

Compression is then:

```
write(b, dict_hash):
  dict = kernel.Read(dict_hash)  # read the dictionary blob
  compressed = compress(b, dict)
  ...
```

The dictionary is content-addressed (immutable, dedup'd). The
*choice* of dictionary is external state (a Collection
configuration). Two Collections can share a dictionary (same
hash) or not (different hashes).

### 17.7. Transport laws

**TR3 (Transport is below Lens, above Kernel).** The Lens calls
`kernel.Write(transport.encode(b))`. The Lens never sees
compressed or encrypted bytes. The kernel never sees plaintext.

**TR4 (Transport is optional per Collection).** A Collection may
have no transport layer (raw bytes), compression only, encryption
only, or both. The choice is in the Collection's metadata. The
kernel does not enforce transport; it stores what it's given.

**TR5 (Transport is per-blob, not per-byte).** The transport
pipeline runs once per `Write`, producing one encoded blob.
Streaming compression (compress across multiple Writes) is
not in-model; it would require a Session substrate.

**TR6 (Block index is a Physical Structure).** The block index
is `f(transport_blob) → block_index`. It can be lost and rebuilt
by re-reading the blob. (Per Part II §14.)

### 17.8. Closing B2, B3, B6, B8, B10, B13

- **B2 (compression breaks ReadRange):** the block index (§17.3)
  restores range-read capability.
- **B3 (encryption key management):** the Key substrate (§17.4)
  promotes key management to first-class. Dedup caveat accepted
  (TR1).
- **B6 (compression dictionaries):** TR2 makes the dictionary a
  sidecar, content-addressed.
- **B8 (encryption + Cross-Lens):** TR3 places encryption below
  Lens; Cross-Lens queries operate on plaintext (above Transport).
- **B10 (compress/encrypt order):** A10 mandates compress-before-
  encrypt.
- **B13 (collapse):** compression + encryption + checksumming
  are one algebra (Transport), not three.

---

## 18. Schema Evolution Algebra (closes B4, B9, B12)

### 18.1. The correction

The "new codec = new name prefix" answer (Part II §17.10) is
unworkable for any real dataset. Schemas evolve; the model must
admit this.

### 18.2. Schema versioning

A schema version is encoded in either:

**Option 1: Key prefix.**

```
feature/v1/orders/...
feature/v2/orders/...
```

The Lens's `D(key, bytes)` parses the version from the key prefix
and dispatches to the appropriate decoder.

**Option 2: Blob header.**

```
Bytes: [4-byte schema_id] [payload bytes]
```

The Lens's `D(key, bytes)` reads the first 4 bytes as a schema_id,
looks up the schema, and decodes.

Both options are valid. The model permits either; the choice is a
Lens-level convention.

### 18.3. Schema Registry substrate (B9)

**Option A: Schemas in code.** The Lens knows all schemas it
supports, linked into the application binary. Schema evolution
requires code deployment.

**Option B: Schemas as data (Schema Registry).** Schemas are
stored as blobs, content-addressed. A Schema Registry is a
special Ref namespace (`__schema/{name}/{version}` → schema
blob hash). The Lens fetches schemas by hash on first decode,
caches them.

**The model picks Option B** as the default, because:

- Schemas are immutable (content-addressed fits).
- Schemas can be shared across Lenses (a SQL Lens and a Feature
  Store Lens can share an Arrow schema).
- Schemas are themselves Physical Structures (rebuildable from
  the blobs they describe — a schema is `f(blob_samples) → schema`,
  though in practice schemas are authored, not derived).

The Schema Registry is **not a new substrate**. It is a
convention over the existing Names substrate (a special Ref
prefix). No new axiom is needed.

### 18.4. Compatibility contract

**SE1 (Backward compatibility — new code reads old data).** A
new version of the Lens must be able to decode blobs written by
all prior versions. New fields must have defaults; removed fields
are ignored.

**SE2 (Forward compatibility — old code reads new data).** An
old version of the Lens must be able to decode blobs written by
newer versions, skipping unknown fields. This requires
self-describing formats (Avro, Protobuf with `preserve_unknown_fields`,
JSON) or a schema-evolution-aware format.

**SE3 (Writer schema is recorded).** Each blob records its
writer schema version (in the key prefix or blob header, per
§18.2). The reader schema is the latest version the Lens
supports. The Lens resolves differences per SE1/SE2.

**SE4 (Compatibility is a Lens responsibility).** The kernel
does not enforce compatibility. A Lens that breaks backward
compatibility will produce read errors on old blobs. This is
the Lens author's responsibility, documented in the Lens's
specification.

### 18.5. Schema as a source type (B12)

The Physical Structure dependency graph (Part II §14) gains a
fourth source type:

```
Source types (amended):
  S_snapshot  : a snapshot (Prolly tree root)
  S_commit    : a commit (blob in the commit graph)
  S_commitset : a set of commits (the entire commit graph)
  S_schema    : a schema (a blob in the Schema Registry)  ← new
```

Structures that depend on schema source from `(S_snapshot, S_schema)`:

| Physical Structure | Source |
|---|---|
| Secondary index (over typed column) | `(S_snapshot, S_schema)` |
| Materialized view (with projected fields) | `(S_snapshot, S_schema)` |
| Feature vector (with feature definitions) | `(S_snapshot, S_schema)` |

If the schema changes, these structures must be rebuilt. The
dependency graph now records this:

**D6 (Schema-dependent structures are invalidated by schema
change).** If `S_schema` evolves from `v1` to `v2`, all
Physical Structures sourced from `(S_snapshot, v1)` are
orphaned. They must be rebuilt against `(S_snapshot, v2)`.

### 18.6. Migration

**Compaction with schema migration:**

```
Migrate(collection, v_old, v_new):
  for each blob b in collection (with schema v_old):
    decoded = Lens.D(b, v_old)
    reencoded = Lens.E(decoded, v_new)
    kernel.Write(reencoded) → h_new
    # update tree to point to h_new
  # commit
```

This is expensive (full rewrite) but rare (schemas evolve slowly).
The model does not optimize for it; the application schedules
migrations during low-traffic periods.

### 18.7. Schema laws

**SE5 (Schema is content-addressed).** A schema is a blob;
`Write(schema_bytes) → schema_hash`. The hash is referenced by
the Schema Registry.

**SE6 (Schemas are immutable).** A schema version, once written,
never changes. Evolution creates new versions, not edits to old
ones.

**SE7 (Schema Registry is a Naming convention).** The Schema
Registry uses the Names substrate (Refs with prefix
`__schema/`). No new substrate, no new axiom.

**SE8 (Kernel is schema-unaware).** The kernel does not know
which schema a blob uses. The schema_id is in the key prefix or
blob header, both of which are Lens-interpreted, not
kernel-interpreted.

### 18.8. Closing B4, B9, B12

- **B4 (new codec = new prefix is unworkable):** SE1-SE4
  formalize schema evolution with backward/forward compatibility.
- **B9 (Schema Registry substrate):** SE7 places the Schema
  Registry on the existing Names substrate; no new substrate.
- **B12 (Schema as source type):** D6 adds `S_schema` to the
  Physical Structure dependency graph.

---

## 19. Amendments to Existing Algebras

### 19.1. Range Read Algebra (§11) — RR2 amended (B2, N7)

**Original RR2 (Part II §11.3):**

> `ReadRange(h, off, len) = ReadRange(h, off, k) || ReadRange(h,
> off+k, len-k)` for any `0 < k < len`.

**Amended RR2':**

> `ReadRange(h, off, len)` composes by byte concatenation *for
> raw blobs*. For transport-encoded blobs (Part III §17), range
> reads compose via the transport block index: each block is
> range-read independently, decoded, and concatenated at the
> logical byte level.

### 19.2. GC Algebra (§3, §13) — G6 added (B5, N5)

**G6 (Tombstone barrier).** GC must not delete a blob until all
replicas have observed a commit blob that does not reference it.
The barrier is implemented as a `deletion_grace_period` (per
Part III §16.4).

### 19.3. Physical Structure Dependency Graph (§14) — D6 added (B12, N6)

**D6 (Schema-dependent structures are invalidated by schema
change).** The source type `S_schema` is added (Part III §18.5).
Structures sourced from `(S_snapshot, S_schema)` are orphaned
when the schema evolves.

### 19.4. New axioms

**A9 (Single-writer per Ref).** For each Ref `name`, there is at
most one writer at any time. Multi-writer convergence is
out-of-model (per A7). (Closes B11.)

**A10 (Compress before encrypt).** The transport pipeline on
write is: compress → encrypt → checksum. The reverse on read.
(Closes B10.)

---

## 20. Summary of Part III

| Algebra | Closes | Net effect |
|---|---|---|
| **Replication** (§16) | B1, B5, B7, B11 | Single-writer per Ref; tombstone barrier; failover loses in-flight writes |
| **Transport** (§17) | B2, B3, B6, B8, B10, B13 | Collapses compression + encryption + checksumming into one layer; block index for range reads; envelope encryption; dictionary as sidecar |
| **Schema Evolution** (§18) | B4, B9, B12 | Schema versions in key/blob; Schema Registry on Names substrate; backward/forward compat; `S_schema` source type |

**Amendments to existing algebras:**

| Amendment | Closes | Net effect |
|---|---|---|
| RR2 → RR2' (§11) | B2 | Range read composition is transport-aware |
| G6 added (§3, §13) | B5 | Tombstone barrier delays GC for replication |
| D6 added (§14) | B12 | `S_schema` source type; schema-dependent structures invalidated by schema change |
| A9, A10 added (§9) | B10, B11 | Single-writer per Ref; compress before encrypt |

**Net change to model surface area (cumulative, Parts I + II + III):**

| Metric | Phase K.1 (start) | Phase K.3 (after Part II) | Phase K.4 (after Part III) |
|---|---|---|---|
| Substrates | 3 (rhetorical) | 5 (honest) | 6 (added Key; Schema Registry on Names) |
| Operations | 3 | 4 (added ReadRange) | 4 |
| Axioms | 4 (A1-A4) | 8 (A1-A8) | 10 (A1-A10) |
| Formal algebras | 8 | 14 | 17 (added Replication, Transport, Schema Evolution) |
| Open questions | 8 | 4 | **0** |

---

## 21. Open Questions: All Closed

### Closed by Part I (Phase K.1)

- Reference algebra
- Merge algebra (3-layer)
- GC model
- RTT calculus
- Object Store Native
- Physical Structure taxonomy
- Workspace algebra
- History as mathematical object

### Closed by Part II (Phase K.3)

- Substrate count (5 honest)
- Manifest algebra
- Range Read algebra
- State vs Bytes
- GC with packs
- Physical Structure dependency graph
- Concurrency and consistency

### Closed by Part III (Phase K.4)

- ~~Replication~~ (§16)
- ~~Compression~~ (§17, Transport)
- ~~Encryption~~ (§17, Transport)
- ~~Schema evolution~~ (§18)

**The model has 0 open questions.** Phase K (model falsification)
is complete. Every concept has an axiom; every axiom has a law;
every law can be tested.

### Remaining engineering questions (not model questions)

These are *implementation choices*, not model gaps:

- Which compression codec? (zstd default; LZ4 for speed; LZ4HC
  for cold storage)
- Which KMS? (AWS KMS, GCP KMS, Vault, local keyfile — all
  equivalent at the model level)
- Which schema format? (Avro, Protobuf, JSON Schema, Arrow
  IPC — Lens's choice)
- Which replication topology? (single-region, primary-secondary,
  sharded primary — application's choice)
- What `deletion_grace_period`? (operational parameter, tuned
  per deployment)

The model is silent on all of these. The model is *complete*
without them. Phase K.4 is the end of model falsification.

### Phase L (next, not yet defined)

Phase L shifts from *model falsification* to *model
verification*: prove the laws hold under the operational hazards
the red teams identified. Concretely:

1. **Property tests** for every law (L1-L7, M1-M4, R1-R5, G1-G6,
   T1-T5, OSN, P1, W1-W5, MAN1-MAN4, RR1-RR4, ST1-ST3, S1-S2,
   D1-D6, C0-C5, CC1-CC4, REP1-REP9, TR1-TR6, SE1-SE8, A1-A10).
2. **Simulator** for object-store hazards (eventual consistency,
   partial writes, list-after-put, replica lag, tombstone races).
3. **Differential tests** against Git (for commit-graph semantics),
   Dolt (for SQL-on-content-addressed), Iceberg (for manifest
   semantics), FDB (for transaction semantics).

Phase L produces no new algebras. It produces tests, simulators,
and proofs. The model is frozen; the verification begins.

---

# Part IV — Post-Phase-L Refinements (Proofs)

> Phase L (`POND_PHASE_L_REPORT.md`) verified 539 checks across
> property tests, hazard simulations, and differential tests.
> Three findings emerged that the model did not anticipate:
>
> 1. The kernel's API is *smaller* than the model requires
>    (`ReadRange` is a model primitive but not a kernel method).
> 2. The CAS law (R3) is unverifiable on the current kernel
>    (`reference()` is unconditional LWW, no CAS parameter).
> 3. The Transport Layer (TR1-TR6) is entirely conceptual — no
>    implementation exists.
>
> Part IV resolves findings (1) and (2) by *demoting* the model
> claims to match the kernel's honest API. Finding (3) is resolved
> by building a reference Transport Layer (Phase N.3,
> `services/transport/`).
>
> The principle: when the model and the kernel disagree, the
> kernel wins. The model is a description of what the kernel
> guarantees; if the model claims more than the kernel provides,
> the model is wrong, not the kernel.

## 22. ReadRange Demotion (closes Phase L §3.1)

### 22.1. The finding

Phase L §3.1 found: the model specifies `ReadRange` as a kernel
operation (A8, §11), but the frozen kernel (`pond_minimal.py`)
does not implement `ReadRange` — it implements only `Read`. Range
reads are emulated in tests by `Read + slice`.

### 22.2. Two possible resolutions

**Option A: Grow the kernel.** Add `read_range(h, off, len)` to
`PondMinimal`. Contradicts the FROZEN policy.

**Option B: Demote `ReadRange` in the model.** Reclassify
`ReadRange` as a *transport-layer optimization* of `Read`, not a
kernel primitive. The kernel API stays at `Read`; the backend
(or Transport Layer) decides how to implement range reads.

### 22.3. The demotion (Option B)

`ReadRange` is removed from the Bytes substrate's operation list
(§9.1). The Bytes substrate has **two** operations: `Write` and
`Read`. `ReadRange` is recategorized as a Transport-layer
concern (see §17 Transport Algebra).

**Revised Bytes substrate (§9.1):**

```
Bytes substrate:
  Axioms: A1 Immutability, A2 Content-addressing
  Operations:
    Write(b) → h
    Read(h) → b
  (Range reads are a Transport-layer optimization, not a kernel operation.)
```

**Revised A8:**

> **A8' (Range reads are transport-layer).** Range reads are
> implemented at the Transport Layer (§17), not the Kernel. The
> Kernel exposes only `Read(h) → b` (full blob read). The
> Transport Layer may decompose `Read` into `ReadRange` calls
> on the backend (e.g., S3 `Range` header) for efficiency, but
> this decomposition is invisible to the Kernel API.

**Revised substrate count:** 6 substrates, **3 operations**
(`Write`, `Read`, `Ref`). The user-facing API remains three
operations; the model underneath admits six substrates.

**Consequence for RR1-RR4 (§11):** the Range Read Algebra (§11)
is recategorized as a **Transport-layer algebra**, not a
Kernel-layer algebra. RR1-RR4 still hold; they describe what the
Transport Layer guarantees, not what the Kernel guarantees. The
algebra moves from §11 (Kernel) to §17 (Transport), but the laws
themselves are unchanged.

### 22.4. Why demotion is correct

1. **The kernel's job is bytes-in, bytes-out.** How the backend
   delivers those bytes (full read, range read, cached read) is
   a backend concern.
2. **Adding `read_range` to the kernel violates L5 (kernel never
   decodes).** `ReadRange` requires the kernel to know about
   byte offsets, which is a transport concern, not a kernel
   concern.
3. **The Transport Layer (§17) already formalizes block indexes
   and range reads.** Moving `ReadRange` to the Transport Layer
   is consistent with the existing model.
4. **The frozen-kernel policy is preserved.** No new kernel
   method; no new axiom; no new law.

---

## 23. CAS Demotion (closes Phase L §3.2)

### 23.1. The finding

Phase L §3.2 found: the model's R3 (CAS) is unverifiable on the
current kernel because `reference()` is unconditional LWW with no
`expected` parameter. The optimistic-loop pattern (read expected,
compute new, conditional update) cannot be tested behaviorally
because the kernel doesn't expose the conditional update.

### 23.2. Two possible resolutions

**Option A: Grow the kernel.** Add `cas_reference(name, expected,
new) → bool` to `PondMinimal`. Contradicts the FROZEN policy.

**Option B: Demote R3 in the model.** Reclassify CAS as
*conditional on backend support*, with the kernel's default
behavior being LWW. The kernel does not expose CAS; applications
requiring CAS use the optimistic-loop pattern (read expected,
compute new, write new, detect conflict post-hoc).

### 23.3. The demotion (Option B)

R3 is rewritten to make the conditional explicit:

**Revised R3 (CAS is backend-conditional):**

> **R3' (CAS is backend-conditional).** Some backends provide
> `compare_and_swap(name, expected, new) → bool` as an atomic
> primitive. The legacy `PondMinimal` kernel (SQLite) does not
> expose this via the kernel API; it exposes only `Ref(name, h)`
> (unconditional LWW). The `ObjectStoreNativeKernel` also uses LWW
> for ref updates (no CAS). Applications requiring CAS implement the
> optimistic-loop pattern: read expected, compute new, write new,
> detect conflict by re-reading. This pattern is correct under
> single-writer-per-Ref (A9); under multi-writer, it requires
> post-hoc conflict detection.
>
> Formally: `CAS(name, expected, new)` is a *derived* operation,
> not a *primitive* operation. The kernel primitives are `Write`,
> `Read`, `Ref`. CAS is built on top.

### 23.4. Why demotion is correct

1. **The kernel's only mutation is `Ref(name, h)`.** Adding
   `cas_reference` adds a second mutation primitive, violating
   A3 (Ref is the only mutation).
2. **CAS is a backend property, not a kernel law.** CC2 already
   says "CAS requires backend support." R3' makes this explicit
   at the Reference Algebra level, not just the Concurrency
   Algebra level.
3. **The optimistic-loop pattern is well-known.** It is how Git,
   S3-without-conditional-writes, and many other systems handle
   concurrency. The model shouldn't pretend to provide CAS when
   it really provides LWW + optimistic-loop.
4. **The frozen-kernel policy is preserved.** No new kernel
   method; the existing `Ref` primitive suffices.

### 23.5. Consequence for CC2

CC2 (CAS requires backend support) is unchanged. R3' and CC2 are
now consistent: both state that CAS is conditional, not
unconditional.

---

## 24. Summary of Part IV

| Change | Closes | Net effect |
|---|---|---|
| §22 ReadRange demotion | Phase L §3.1 | Bytes substrate: 3 ops → 2 ops. A8 → A8'. Range Read Algebra moves from Kernel (§11) to Transport (§17). |
| §23 CAS demotion | Phase L §3.2 | R3 → R3'. CAS is derived, not primitive. Kernel API unchanged. |

**Cumulative model surface area (Parts I + II + III + IV):**

| Metric | Phase K.4 (after Part III) | Phase N.1 (after Part IV) |
|---|---|---|
| Substrates | 6 | **6** (unchanged) |
| Operations | 4 (Write, Read, ReadRange, Ref) | **3** (Write, Read, Ref — ReadRange demoted to Transport) |
| Axioms | 10 (A1-A10) | **10** (A8 → A8', count unchanged) |
| Formal algebras | 17 | **17** (unchanged; Range Read moved from Kernel-layer to Transport-layer) |
| Open questions | 0 | **0** |

**The kernel is now smaller than the model claimed.** The model
claimed 4 operations; the kernel has 3. Phase N.1 corrects the
model to match the kernel. The user-facing API is `Write`, `Read`,
`Ref` — three operations on six substrates.

This is the final form of the model. No further demotions are
possible without removing a substrate (which would break the
model). The model is minimal.
