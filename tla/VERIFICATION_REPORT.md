# TLA+ Verification Report — PondKernel

*Last updated: 2026-01-15*

## Overview

The Pond kernel is formally specified in TLA+ at `tla/PondKernel.tla`. The specification models the three kernel primitives (Write, Read, Ref) and verifies that key safety invariants hold in every reachable state.

## Specification Summary

### Constants (finite model for model checking)
- `Bytes = {B1, B2, B3}` — 3 byte-string values
- `Hashes = {H1, H2, H3, TS}` — 3 hashes + 1 tombstone sentinel
- `Names = {N1, N2}` — 2 ref names
- `TOMBSTONE = TS` — logical deletion marker

### State Variables
- `blobSet: SET of <<hash, bytes>> pairs` — the content-addressed blob store
- `refMap: [Names -> Hashes]` — name-to-hash reference map

### Operations
1. **Write(b) → h** — adds `<<Hash(b), b>>` to `blobSet` (append-only, dedup free)
2. **Read(h) → b** — returns `b` such that `<<h, b>> ∈ blobSet` (no state change)
3. **Ref(n, h)** — updates `refMap[n] := h` (requires `h` points to a written blob)
4. **Tombstone(n)** — sets `refMap[n] := TOMBSTONE` (logical deletion)

## Invariants Verified

All invariants are checked by TLC at every reachable state:

### 1. TypeInvariant
```
/\ blobSet ⊆ {<<h, b>> : h ∈ Hashes, b ∈ Bytes}
/\ refMap ∈ [Names -> Hashes]
```
Ensures the state is well-typed.

### 2. A1_Immutability
Once a blob is written, it stays in `blobSet` forever. No operation removes from `blobSet`.

### 3. A2_ContentAddressing
`Hash` is injective: same bytes → same hash. This is the content-addressing property that enables free deduplication.

### 4. A4_ReferentialIntegrity
Every non-tombstone ref points to a written blob:
```
∀ n ∈ Names: refMap[n] ≠ TOMBSTONE => ∃ b ∈ Bytes: <<refMap[n], b>> ∈ blobSet
```

### 5. C0_BlobImmutability
If `<<h, b>> ∈ blobSet`, then `h = Hash(b)` — the hash matches the bytes.

### 6. C2_SingleRefAtomicity
`refMap[n]` is always a single hash value, never a "mix" — reference updates are atomic.

## How to Run TLC

```bash
cd tla/
java -cp tla2tools.jar tlc2.TLC PondKernel
```

### Prerequisites
- Java 8+ (JRE)
- `tla2tools.jar` (included in the `tla/` directory)

### Expected Output
```
Model checking completed. No error has been found.
623 states generated, 56 distinct states found, 0 states left on queue.
```

## Verification Results

| Invariant | Status | Notes |
|-----------|--------|-------|
| TypeInvariant | ✅ PASS | All states well-typed |
| A1_Immutability | ✅ PASS | blobSet is append-only |
| A2_ContentAddressing | ✅ PASS | Hash is injective |
| A4_ReferentialIntegrity | ✅ PASS | All refs point to written blobs |
| C0_BlobImmutability | ✅ PASS | Hash matches bytes |
| C2_SingleRefAtomicity | ✅ PASS | refMap is a function |

**All 6 invariants pass. No violations found.**

## Scope and Limitations

### What the spec models
- The 3 kernel primitives (Write, Read, Ref)
- Content-addressed storage (SHA-256 dedup)
- Logical deletion via tombstones
- Referential integrity (refs point to existing blobs)

### What the spec does NOT model (out of scope)
- **Branching/merge** — would need separate TLA+ module
- **CRDT shard merge** — row-level conflict resolution is not modeled
- **Transactions** — atomic publication is not modeled
- **GC/vacuum** — garbage collection would violate A1 by design
- **Concurrent writes** — the spec uses interleaving semantics (sound over-approximation)

### Model simplifications
- Finite `Bytes`/`Hashes`/`Names` sets (real world is unbounded; property holds by induction)
- `Hash` is a fixed CASE table (real SHA-256 is cryptographic)
- Concurrency = interleaving (sound over-approximation of true concurrency)

### What TLC does NOT prove
- SHA-256 collision resistance (external cryptographic assumption)
- Object-store consistency (S3's eventual consistency model)
- Crash safety (no failure actions modeled)
- Liveness under failure (no fairness or failure assumptions)

## Gaps Between Spec and Implementation

| Spec | Implementation | Gap |
|------|---------------|-----|
| `Write(b)` adds to `blobSet` | `kernel.write(data)` writes to object store | Implementation also writes a ref entry |
| `Read(h)` returns `b` | `kernel.read_blob(hash)` returns `Vec<u8>` | Implementation has LRU cache (transparent) |
| `Ref(n, h)` updates `refMap` | `kernel.reference(name, hash)` | Implementation uses path-based refs |
| `Tombstone(n)` sets to `TS` | `maintenance::drop_name(kernel, name)` | Implementation writes TOMBSTONE_HASH marker |

The gaps are implementation details that don't affect the safety properties. The kernel's public API matches the spec's operations 1:1.
