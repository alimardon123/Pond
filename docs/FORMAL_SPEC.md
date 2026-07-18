# Pond Kernel — Formal Specification

This document specifies the kernel as **laws** (invariants) plus the
**current API realization** with formal preconditions and postconditions.

The laws are the enduring architecture. The API is one point-in-time
realization. Future APIs may differ as long as they satisfy the laws.

---

## The Laws (architectural invariants)

### Law 1: Objects are immutable
For all bytes `b`, if `Write(b)` returns hash `h`, then the bytes at `h`
never change for the lifetime of the system. The hash `h` is a pure
function of `b` (content-addressing): `h = H(b)` where `H` is a
cryptographic hash function.

**Corollary:** Writing the same bytes twice produces the same hash.
Dedup is free.

**Corollary:** Immutability is verifiable: anyone with the bytes can
recompute `H(b)` and check it matches `h`.

### Law 2: Objects are addressable
Every object has a stable identifier (its hash). The identifier is:
- **Derivable:** anyone with the bytes can compute the hash.
- **Verifiable:** anyone with the bytes can check the hash matches.
- **Global:** the same hash refers to the same bytes on every node.

### Law 3: Names are mutable
Names (strings) can be mapped to object hashes. The mapping is mutable:
names can be created, updated, and (possibly) deleted. The namespace
is the only mutable state in the system.

**Corollary:** Names are NOT content-addressed. Two different names
can point to the same hash. The same name can point to different
hashes over time.

### Law 4: References never mutate objects
Updating a name to point to a different hash does not modify any
object. Objects are only created (via Write) and read (via Read).
The namespace mutation is a separate operation that doesn't touch
object bytes.

**Corollary:** Concurrent reads of an object are safe (the bytes
won't change mid-read). No locks needed for reads.

### Law 5: Objects are backend-independent
The laws make no assumption about where objects are stored. Any
backend that can "store bytes by key" and "fetch bytes by key"
satisfies the laws. The kernel does not require: rename, append,
seek, directories, or filesystem semantics.

---

## The Current API Realization

The current API is one realization of the 5 laws. It exposes three
operations.

### Operation 1: Write

```
Write(data: bytes) -> hash: string
```

**Preconditions:**
- `data` is a finite byte sequence.

**Postconditions:**
- Returns a 64-character hex string `hash` such that `H(data) = hash`.
- The bytes `data` are now durably stored at `hash`.
- Subsequent `Read(hash)` returns `data`.
- If `Write(data)` is called again with the same `data`, returns the
  same `hash` (dedup; no new storage consumed).
- The bytes at `hash` never change (Law 1).

**Laws satisfied:** 1 (immutability), 2 (addressability), 5 (backend-independent).

**Laws not exercised:** 3 (names), 4 (references).

### Operation 2: Read

```
Read(hash_or_name: string) -> bytes
```

**Preconditions:**
- `hash_or_name` is either:
  - A 64-character hex string that was previously returned by `Write`, OR
  - A name that is currently bound in the root namespace.

**Postconditions:**
- If `hash_or_name` is a hash: returns the bytes stored at that hash.
- If `hash_or_name` is a name: resolves the name to its current hash
  via the root namespace, then returns the bytes at that hash.
- The returned bytes match `H(returned_bytes) = hash` (verifiable).
- The returned bytes are immutable (Law 1); concurrent reads are safe (Law 4).

**Error cases:**
- `NOT_FOUND`: the hash doesn't exist OR the name isn't bound.
- `INTEGRITY_ERROR`: the bytes at the hash don't match `H(bytes)`.
  (Indicates corruption; should never happen if Law 1 holds.)

**Laws satisfied:** 1, 2, 4 (reads don't mutate), 5.

### Operation 3: Reference

```
Reference(name: string, hash: string) -> ()
```

**Preconditions:**
- `name` is a non-empty string.
- `hash` was previously returned by `Write` (the hash exists).

**Postconditions:**
- The root namespace maps `name` to `hash`.
- Subsequent `Read(name)` returns the bytes at `hash`.
- If `name` was previously bound to a different hash, the old binding
  is replaced (last-writer-wins).
- No object bytes are modified (Law 4).

**Error cases:**
- `HASH_NOT_FOUND`: `hash` doesn't exist in the object store.

**Laws satisfied:** 3 (names mutable), 4 (references don't mutate objects).

**Laws not exercised:** 1, 2, 5.

---

## Invariants (properties that always hold)

### Invariant 1: One-copy
Only sealed blobs (the bytes stored by Write) are canonical. The root
namespace is the only mutable state. All other representations (caches,
indexes, materialized views) are derived and rebuildable.

### Invariant 2: Reachability
An object is "reachable" if it's referenced by a name in the root
namespace, directly or transitively (via Trees, Commits, etc. that
are themselves reachable). Unreachable objects are orphans and may
be garbage-collected (GC is a View concern, not a kernel guarantee).

### Invariant 3: Deterministic apply
Given the same sequence of (Write, Reference) operations applied in
the same order, any two implementations produce the same state.
Non-determinism (timestamps, randomness) must be resolved BEFORE
calling Write (at the View/adapter layer), not inside the kernel.

### Invariant 4: Snapshot consistency
A Read at a hash always returns the same bytes (Law 1). A Read at a
name returns the bytes at the hash the name currently resolves to.
Once a Read resolves a name to a hash, the read is immune to
concurrent Reference updates.

### Invariant 5: Content-addressed integrity
For any hash `h` returned by Write, the bytes at `h` satisfy
`H(bytes) = h`. Any bit-rot or corruption is detectable by re-hashing.

---

## What the laws do NOT guarantee (honest gaps)

The laws are necessary but not sufficient for a production system.
The following are NOT guaranteed by the kernel and must be provided
by Views or infrastructure:

1. **Multi-writer coordination.** The kernel has one root namespace.
   Multiple writers racing on `Reference(name, hash)` have
   last-writer-wins semantics. For multi-writer ACID, Views need
   their own coordination (Raft, MVCC, OCC).

2. **Causal consistency.** The kernel doesn't track causal history.
   If writer A writes blob X then references name "foo" to X, and
   writer B reads "foo" and gets X, writer B has no way to know
   whether X was written before or after some other operation.
   Causal consistency is a View/infrastructure concern.

3. **Transactional visibility.** The kernel has no transactions.
   A View that writes blob X, then writes blob Y, then references
   "foo" to Y — readers might see X but not Y (if they read between
   the writes). Transactions are a View concern.

4. **Cross-region linearizability.** The kernel is single-region.
   Cross-region replication (async, read-your-writes) is an
   infrastructure concern, not a kernel law.

5. **Garbage collection.** Orphaned objects (Write without Reference,
   or Reference overwritten) accumulate. GC is a View concern.

6. **Time travel performance.** Walking the commit parent chain is
   O(N). Skip pointers (for O(log N)) are a View-level pattern.

These gaps are NOT kernel bugs. They are explicitly out of scope for
the kernel. Views and infrastructure provide them as needed.

---

## Open questions (to be attacked)

- Is Law 3 (names mutable) actually required? Could names be immutable
  with versioning handled another way? (Identity Destruction II, Exp 4)

- Is Law 1 (immutability) binary? Could there be tiered immutability
  (e.g., "mutable for 1 hour, then immutable")? (Identity Destruction II, Exp 8)

- Are there laws I'm missing? Laws that should be added?

- Are there laws I'm over-claiming? Laws that don't actually hold under
  adversarial conditions?

- Is the current API (Write/Read/Reference) the best realization of
  the laws, or would SetRoot (IPFS/IPNS model) be better?
  (Identity Destruction II, Exp 1)

These questions are not settled. The laws are a hypothesis, not a proof.
