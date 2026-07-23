# RFC-0003: Kernel Specification

## Status

**Accepted** — the kernel is frozen. Changes require a new RFC that
satisfies the kernel change criteria (see RFC index).

## Abstract

This RFC is the authoritative specification of the Pond kernel. It
supersedes all earlier descriptions (including the prototype README
and the v1.0 PDF RFC). The kernel is defined by 3 primitives, 5
storage laws, 7 composition laws, and a lower-bound proof.

---

## 1. The Kernel

The kernel is a 3-primitive immutable object runtime.

### Primitives

```
Write(data: bytes) → hash: string
Read(hash_or_name: string) → bytes
Reference(name: string, hash: string) → ()
```

No other operations exist in the kernel. Tree, Commit, Tag, Branch,
OPEN/SEALED, lifecycle, GC, indexes, and all other concepts are
Lens-level patterns built from these 3 primitives.

### State

```
State = (O, N)
  O : Hash ⇀ Bytes  (immutable object store, content-addressed)
  N : Name → Hash   (mutable namespace, the only mutable state)
```

---

## 2. Storage Laws (immutable)

### Law 1: Objects are immutable
Once `Write(data)` returns `h`, the bytes at `h` never change.
`h = H(data)` where H is SHA-256. Immutability is verifiable by re-hashing.

### Law 2: Objects are addressable
Every object has a stable identifier (its hash). The identifier is
derivable (compute H(bytes)) and verifiable (check H(bytes) = h).

### Law 3: Names are mutable
Names map to hashes. The mapping is mutable: names can be created
and updated. The namespace is the only mutable state.

### Law 4: References never mutate objects
`Reference(name, hash)` updates the name→hash mapping. It does not
modify any object. Objects are only created (Write) and read (Read).

### Law 5: Objects are backend-independent
The laws make no assumption about storage backend. Any backend that
can "store bytes by key" and "fetch bytes by key" satisfies the laws.
Verified on: filesystem, in-memory, SQLite, Redis (simulated), S3 (real, via moto).

---

## 3. Composition Laws (algebraic)

### Composition Law 1: Reference chains
The kernel provides one level of indirection (name → hash → bytes).
Deeper indirection is a Lens concern (walk embedded hashes in blobs).

### Composition Law 2: Reference moves
Overwriting a name orphans the old hash. The kernel does not track
reference history. Views build history via the Commit pattern.

### Composition Law 3: GC reachability
The kernel does not GC. Orphaned objects accumulate. GC is a Lens
concern (PondGC implements reachability walk + sweep).

### Composition Law 4: Backend substitution
Same operation sequence on different backends produces the same state.
Verified: S3 vs filesystem produce identical hashes and namespace.

### Composition Law 5: Snapshot composition
A snapshot is a hash. Reading at a hash is a consistent snapshot.
Snapshots are free (O(1) — just record the hash).

### Composition Law 6: Branching composition
A branch is `Reference(branch_name, commit_hash)`. O(1). Branches
share all objects reachable from the commit (copy-on-write for free).

### Composition Law 7: Cross-View isolation
The kernel has no isolation. Views use naming conventions (capability
tokens) or separate kernel instances for multi-tenancy.

---

## 4. Algebraic Properties (proven)

1. **Idempotence of Write:** `Write(Write(S, d), d) = Write(S, d)`
2. **Commutativity of Write (different data):** order doesn't matter
3. **Determinism of Write:** hash depends only on data, not on state
4. **Referential transparency of Read (by hash):** `Read(S, h) = O(h)` regardless of N
5. **Non-commutativity of Reference (same name):** last-writer-wins
6. **Idempotence of Reference (same value):** re-referencing same hash is a no-op
7. **Commutativity of Reference (different names):** order doesn't matter
8. **Write and Reference commute (in practice):** Views always Write-then-Reference

Proofs in FORMAL_ALGEBRA.md.

---

## 5. Lower-Bound Proof

**Theorem:** No immutable storage system can exist with fewer than 3 primitives.

**Proof:** All three possible 2-primitive merges fail:
- Merge Write+Read: loses Reference → not a database (IPFS without IPNS)
- Merge Write+Reference: cannot reference existing hash without re-writing; cannot create unnamed objects; conflates immutable/mutable
- Merge Read+Reference: conflates pure read with mutation; polymorphic signatures

QED: 3 is the minimum. Proof in FORMAL_ALGEBRA.md section 3.

---

## 6. Concurrency Contract

### Guaranteed
- Reference is atomic (no partial updates, no corruption)
- Reads are consistent (content-addressing gives stable snapshots)
- Crash recovery (system recovers to last completed Reference)
- Last-writer-wins on concurrent Reference to same name

### Not Guaranteed
- Multi-writer coordination (no CAS, no transactions, no MVCC)
- Multi-process concurrent writes (SQLite locking; use FDB/etcd)
- Lost update detection (writers can't detect overwrites)
- Ordering guarantees (no FIFO or causal ordering)

### View Implications
- Single-writer Views: no concurrency handling needed
- Multi-writer Views: use branches (CRDT) or external coordination
- Multi-process Views: use FDB/etcd backend or separate instances
- Crash-safe Views: kernel guarantees consistency; Views handle orphans via GC

---

## 7. Kernel Change Criteria

The kernel is frozen. A change requires a new RFC that demonstrates ALL THREE:

1. **A workload that cannot be expressed** with Write + Read + Reference
2. **The workload passes the 5-criterion Admission Rule:**
   - Universal (3+ structurally different Views need it)
   - Impossible outside the kernel (Views can't implement it)
   - Immutable (no mutable state beyond name→hash)
   - Storage-independent (no format/workload knowledge)
   - Decades-stable (could Linux keep this for 30 years?)
3. **The lower-bound proof is incorrect** (a 2-primitive system IS sufficient)

Until all three are demonstrated, the kernel stays at 3 primitives.

---

## 8. What the Kernel Does NOT Do (see NON_GOALS.md)

- SQL optimization, query planning, IR
- Distributed consensus (Raft, Paxos)
- Vector search (HNSW, IVF)
- Transactions (ACID, MVCC, 2PC)
- Schema management
- Caching
- Indexing
- Scheduling
- Authorization
- Compression
- Replication
- Streaming (watermarks, exactly-once)
- Garbage collection (Lens-level utility)
- Time-travel acceleration (Lens-level skip pointers)
- Merge (Lens-level conflict resolution)
- Working tree / staging area
- Network protocol

All of the above are Lens-level or infrastructure-level concerns.
