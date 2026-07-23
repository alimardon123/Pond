# Pond Documentation

> Start here. The rest is in `docs/archive/` for historical reference.

## Essential reading (in order)

1. **[`../README.md`](../README.md)** — 5-minute intro.
2. **[`POND_WHITEPAPER.md`](POND_WHITEPAPER.md)** — the contribution.
   20-page rigorous description with formal comparison to Git,
   Iceberg, Dolt, FDB, LakeFS. Read this if you read nothing else.
3. **[`WHERE_POND_FAILS.md`](WHERE_POND_FAILS.md)** — the honest scope.
   8 workloads where Pond is the wrong tool. 5 workloads where Pond
   excels. Read this before adopting.

## If you want to verify the claims

4. **[`POND_PHASE_Q_BENCHMARKS.md`](POND_PHASE_Q_BENCHMARKS.md)** —
   head-to-head benchmarks vs Git, Dolt, Iceberg.
5. **[`POND_FORMAL_ALGEBRAS.md`](POND_FORMAL_ALGEBRAS.md)** — the
   formal model. 17 algebras, 10 axioms, ~30 laws.
6. **[`../tla/PondKernel.tla`](../tla/PondKernel.tla)** — TLA+
   specification. 6 invariants checked across 56 reachable states.

## If you want to build on Pond

7. **[`../lenses/lakehouse/lakehouse.py`](../lenses/lakehouse/lakehouse.py)** —
   DuckDB lakehouse on Pond. The flagship.
8. **[`../pond-labs/feature_store_lens.py`](../pond-labs/feature_store_lens.py)** —
   versioned ML feature store with point-in-time joins.
9. **[`../pond-labs/interop_demo.py`](../pond-labs/interop_demo.py)** —
   bidirectional interop between Feature Store and Lakehouse Lenses.
   The killer demo.
10. **[`../pond-labs/loc_benchmark.py`](../pond-labs/loc_benchmark.py)** —
    LOC saved: 81% reduction vs building from scratch.
11. **[`LENS_AUTHORS_GUIDE.md`](LENS_AUTHORS_GUIDE.md)** — how to
    write a new Lens.

## If you are an external reviewer

12. **[`POND_PHASE_Q_REVIEW_PACKET.md`](POND_PHASE_Q_REVIEW_PACKET.md)** —
    review packet with 15 specific questions.
13. **[`DELETE_90_PERCENT.md`](DELETE_90_PERCENT.md)** — the
    simplification exercise (why this directory is so much smaller
    than it used to be).

## Archive

The `archive/` subdirectory contains historical documents:
- Phase L/N/O/P reports (superseded by whitepaper)
- Second and Third Red Team reviews (findings folded into formal algebras)
- Original storage model paper (superseded by whitepaper)
- Original mathematical model (superseded by formal algebras Parts I-IV)
- Peer comparison, Liquid Clustering comparison, Rejected Designs
  (folded into whitepaper)
- Feature Store use case (superseded by `pond-labs/feature_store_lens.py`)

These are kept for historical reference. They are not needed to
understand Pond.
