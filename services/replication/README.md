# services/replication/

The **Replication Coordinator** — single-writer per Ref + 2PC for
cross-Collection atomicity.

## What it is

A reference implementation of the Replication Algebra (§16 of
`POND_FORMAL_ALGEBRAS.md`) plus the A7 escape hatch (a coordinator
that applications layer on top of the kernel for multi-writer
convergence and cross-Collection atomicity).

Per A7: *"Cross-Collection atomic writes, distributed transactions,
and linearizable reads require a coordinator substrate (2PC, Raft,
Paxos). The model does not specify one. Applications requiring these
must layer a coordinator on top of the kernel."*

## Two coordinators

| Class | Implements | Purpose |
|---|---|---|
| `PrimarySecondaryCoordinator` | REP1–REP9 | Single-writer per Ref; tombstone barrier (G6); failover loses in-flight writes (REP5); explicit promotion (REP6). |
| `TwoPhaseCommitCoordinator` | A7 escape hatch | Cross-Collection atomicity via 2PC, using only kernel primitives (no kernel changes). |

## Replication algebra highlights (REP1–REP9)

- REP1 — single writer per Ref
- REP2 — secondary reads may be stale
- REP3 — replication unit is the commit blob
- REP5 — failover loses in-flight writes (accepted trade-off)
- REP6 — failover requires explicit promotion
- G6 — tombstone barrier (deletions propagate before new writes)

The 2PC coordinator is NOT a kernel extension. It is a library that
uses the kernel's three primitives (`Write`, `Read`, `Ref`) plus the
kernel's LWW (last-writer-wins) semantics to implement distributed
transactions. The model says cross-Collection atomicity is the
application's responsibility — this module shows one way to discharge
it.

## Files

| File | Purpose |
|---|---|
| `replication_coordinator.py` | `PrimarySecondaryCoordinator`, `TwoPhaseCommitCoordinator` |
| `__init__.py` | Package exports |

## Architecture

Depends only on `bindings/python/core` (per `REPO_ORGANIZATION.md` §7). Each
coordinator is a library, not a kernel feature.

## Dependencies

- `bindings/python/core/` (kernel)
- Python stdlib only
