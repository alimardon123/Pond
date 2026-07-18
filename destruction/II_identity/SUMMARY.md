# Identity Destruction II — Summary

> The first destruction phase tested the kernel against designed workloads
> and found no new issues. That was a red flag. Identity Destruction II
> attacks the foundational assumptions the first phase didn't question.

## 8 experiments, honest verdicts

| # | Question | Verdict |
|---|---|---|
| 1 | Is Reference primitive? | **INCONCLUSIVE** — leaning toward "not in current form" |
| 2 | Is name→hash the right namespace model? | **SUPPORTED** — flat is minimal; richer models are View concerns |
| 3 | Is the kernel an API or laws? | **SUPPORTED** — laws endure, APIs evolve |
| 4 | Can names disappear? | **SUPPORTED** — deletion is a View concern |
| 5 | Can references be CRDTs? | **SUPPORTED** — LWW is already a CRDT; richer semantics are View concerns |
| 6 | Can namespaces overlap/compose/conflict? | **SUPPORTED** — overlap is fine; collisions are LWW |
| 7 | Is hash primitive? | **SUPPORTED** — content-addressing is primitive; SHA-256 isn't |
| 8 | Is immutability binary? | **SUPPORTED** — tiered models violate laws or are View concerns |

**6 SUPPORTED, 1 INCONCLUSIVE, 1 NEW FINDING.** No falsifications.

## Real architectural decisions surfaced

### 1. Reference(name, hash) vs SetRoot(hash) — open question

Reference might not be primitive in its current form. The kernel could
shrink to `SetRoot(hash)` (IPFS/IPNS model) and let Views build namespace
structure on top. Trade-off:

- **Current (Reference):** pragmatic, simple Views, shared namespace.
- **Smaller (SetRoot):** minimal kernel, complex Views, per-View namespace models.

Decision: keep Reference for now (pragmatic), but document SetRoot as the
fallback if shared namespace becomes a bottleneck. This is a real
architectural decision, not a clear falsification.

### 2. Multi-hash admission — candidate for v0.8

The kernel hardcodes SHA-256. Multi-hash (IPFS CID style) would:
- Future-proof against SHA-256 breaks (quantum, cryptanalysis)
- Support multiple hash algorithms simultaneously
- Pass the Admission Rule (Universal, Impossible-outside-kernel, Immutable, Storage-independent, Decades-stable)

This is a real architectural improvement, not just an optimization.
Candidate for v0.8 kernel admission.

### 3. Laws over APIs — specification shift

The architecture should be specified as **laws** (invariants), not APIs.
The current API (Write/Read/Reference) is one realization. Future APIs
(SetRoot, transactional, CRDT-based) could satisfy the same laws.

5 laws specified:
1. Objects are immutable
2. Objects are addressable
3. Names are mutable
4. References never mutate objects
5. Objects are backend-independent

See `docs/FORMAL_SPEC.md` for the full specification.

## What was NOT falsified

Despite attacking:
- Reference's primitiveness
- The namespace model
- The hash model
- The immutability model
- Name deletion
- CRDT references
- Namespace composition

...the 3-primitive kernel (Write/Read/Reference) survived. No fourth
primitive was needed. No existing primitive was falsified.

## Honest caveat

These experiments are still mostly analytical. The real test is
implementing a workload that breaks the kernel — e.g., a CRDT View that
needs multi-writer namespace semantics the kernel can't provide, or a
multi-region View that needs causal consistency.

The next phase: **adversarial workload implementation**. Build Views
specifically designed to break the kernel's assumptions. If they
require kernel changes, that's the finding. If they don't, the kernel
is strengthened.

## Comparison to Identity Destruction I

| Phase | Experiments | Supported | Falsified | Inconclusive | New findings |
|---|---|---|---|---|---|
| I (8 experiments) | 8 | 6 | 0 | 0 | 2 confirmed primitives (immutability, Reference) |
| II (8 experiments) | 8 | 6 | 0 | 1 | 1 (multi-hash candidate) |
| **Total** | **16** | **12** | **0** | **1** | **3** |

The architecture has now survived 16 adversarial identity experiments
plus 41 destruction experiments (57 total). The 3-primitive kernel
remains an empirical hypothesis, now supported by:
- 8 standard workloads (SQL, Vector, Streaming, Git, Graph, ML, TimeSeries, OCI)
- 6 alien workloads (Minecraft, Blender, CAD, Genome, PACS, Photoshop)
- 16 identity experiments
- 41 destruction experiments
- 6 storage backends (FS, memory, SQLite, Redis, S3, FDB)

**Hypothesis status:** Still empirical, not proof. The next phase
(adversarial workload implementation) is the real test.
