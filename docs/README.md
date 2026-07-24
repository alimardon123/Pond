# Pond Documentation

> Start here. The rest is in `archive/` for historical reference.

## Essential reading (in order)

1. **[`../README.md`](../README.md)** — 5-minute intro.
2. **[`POND_WHITEPAPER.md`](POND_WHITEPAPER.md)** — the contribution.
   20-page rigorous description with formal comparison to Git,
   Iceberg, Dolt, FDB, LakeFS. Read this if you read nothing else.
3. **[`WHERE_POND_FAILS.md`](WHERE_POND_FAILS.md)** — honest scope +
   Lens roadmap. 8 workloads where Pond struggles today, each mapped
   to the Lens that closes the gap.

## If you want to build on Pond

4. **[`LENS_GUIDE.md`](LENS_GUIDE.md)** — how to write a Lens.
   Merges the former Lens Author's Guide, Interpretation Contract,
   and Interop Spec into one document.
5. **[`GETTING_STARTED.md`](GETTING_STARTED.md)** — 5-minute tutorial
   with the Lakehouse Lens (CREATE TABLE, INSERT, SELECT, time travel,
   branching, schema evolution).
6. **[`NON_GOALS.md`](NON_GOALS.md)** — what Pond deliberately doesn't do.

## If you want to verify the claims

7. **[`POND_PHASE_Q_BENCHMARKS.md`](POND_PHASE_Q_BENCHMARKS.md)** —
   head-to-head benchmarks vs Git, Dolt, Iceberg.
8. **[`POND_FORMAL_ALGEBRAS.md`](POND_FORMAL_ALGEBRAS.md)** — the
   formal model. 17 algebras, 10 axioms, ~30 laws.
9. **[`../tla/PondKernel.tla`](../tla/PondKernel.tla)** — TLA+
   specification. 6 invariants checked across 56 reachable states.

## If you want the history

10. **[`POSTMORTEM_PROLLY_TREE_BUG.md`](POSTMORTEM_PROLLY_TREE_BUG.md)** —
    postmortem of the critical Prolly tree encoding bug.
11. **[`archive/`](archive/)** — historical docs (Phase L/N/O/P reports,
    red team reviews, original storage model paper, RFCs, etc.).

## Active docs (9 total)

| Doc | Purpose |
|---|---|
| `README.md` | This index |
| `POND_WHITEPAPER.md` | The contribution (20 pages) |
| `WHERE_POND_FAILS.md` | Honest scope + Lens roadmap |
| `LENS_GUIDE.md` | How to write a Lens |
| `GETTING_STARTED.md` | 5-minute tutorial |
| `NON_GOALS.md` | What Pond doesn't do |
| `POND_FORMAL_ALGEBRAS.md` | 17 algebras, 10 axioms |
| `POND_PHASE_Q_BENCHMARKS.md` | Head-to-head benchmarks |
| `POSTMORTEM_PROLLY_TREE_BUG.md` | Prolly tree bug postmortem |

## Archive

The `archive/` subdirectory contains 18+ historical documents:
- Phase L/N/O/P/Q reports (superseded by whitepaper)
- Second and Third Red Team reviews (findings folded into formal algebras)
- Original storage model paper (superseded by whitepaper)
- Original mathematical model (superseded by formal algebras)
- 3 lens docs (merged into LENS_GUIDE.md)
- DELETE_90_PERCENT.md (recommendation executed)
- RFCs (13 design docs, decisions folded into formal algebras)

These are kept for historical reference. They are not needed to
understand Pond.
