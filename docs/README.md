# Pond Documentation

> Start here. The rest is in `archive/` for historical reference.

## Essential reading (in order)

1. **[`../README.md`](../README.md)** — 5-minute intro.
2. **[`UNIFIED_STORAGE_DESIGN.md`](UNIFIED_STORAGE_DESIGN.md)** — the
   current architecture. ONE format (PND2), ONE write path, ONE read
   path, ANY workload. This is the canonical reference for the unified
   storage layer.
3. **[`HONEST_COMPETITOR_COMPARISON.md`](HONEST_COMPETITOR_COMPARISON.md)** —
   honest assessment of where Pond wins and loses vs Iceberg, FAISS,
   Redis, Kafka. Read this before making any performance claim.
4. **[`COLLECTION_MANIFEST_DESIGN.md`](COLLECTION_MANIFEST_DESIGN.md)** —
   the index layer. ONE manifest blob per commit with inline stats +
   chunk hashes. PB-scale delegation to the hierarchical stats tree.
5. **[`ROUND_TRIP_AUDIT.md`](ROUND_TRIP_AUDIT.md)** — honest cold-read
   round-trip accounting (no SQLite, no caching hidden).
6. **[`POND_WHITEPAPER.md`](POND_WHITEPAPER.md)** — the contribution.
   20-page rigorous description with formal comparison to Git,
   Iceberg, Dolt, FDB, LakeFS.
7. **[`WHERE_POND_FAILS.md`](WHERE_POND_FAILS.md)** — honest scope +
   Lens roadmap.

## If you want to build on Pond

8. **[`LENS_GUIDE.md`](LENS_GUIDE.md)** — how to write a Lens.
9. **[`GETTING_STARTED.md`](GETTING_STARTED.md)** — 5-minute tutorial.
10. **[`NON_GOALS.md`](NON_GOALS.md)** — what Pond deliberately doesn't do.
11. **[`BINARY_ENCODING_FORMAT.md`](BINARY_ENCODING_FORMAT.md)** — PND1
    column encoding spec (used inside PND2 blobs).

## If you want to verify the claims

12. **[`POND_FORMAL_ALGEBRAS.md`](POND_FORMAL_ALGEBRAS.md)** — formal model.
13. **[`POSTMORTEM_PROLLY_TREE_BUG.md`](POSTMORTEM_PROLLY_TREE_BUG.md)** —
    postmortem of the critical Prolly tree encoding bug.
14. **[`GENERIC_DESIGN_VISION.md`](GENERIC_DESIGN_VISION.md)** — the
    "any app" promise.

## Architecture reviews (historical)

- **[`archive/ARCHITECTURE_REVIEW_2_UNIFIED_STORAGE.md`](archive/ARCHITECTURE_REVIEW_2_UNIFIED_STORAGE.md)** —
  proposed the manifest + stats tree (now implemented).
- **[`archive/ARCHITECTURE_REVIEW_3_COMPLETE.md`](archive/ARCHITECTURE_REVIEW_3_COMPLETE.md)** —
  5 critical findings (all addressed by 3 rounds of fixes).
- **[`archive/DESIGN_REVIEW_2026_07_26.md`](archive/DESIGN_REVIEW_2026_07_26.md)** —
  42 findings on pre-fix code (superseded by unified storage).
- **[`archive/WORKLOAD_ANALYSIS_PB_SCALE.md`](archive/WORKLOAD_ANALYSIS_PB_SCALE.md)** —
  proposed hierarchical stats tree (now implemented as `stats_tree.py`).

## Active docs (16 total)

| Doc | Purpose |
|---|---|
| `README.md` | This index |
| `UNIFIED_STORAGE_DESIGN.md` | ONE format, ONE write/read path (current architecture) |
| `HONEST_COMPETITOR_COMPARISON.md` | Where Pond wins/loses vs Iceberg, FAISS, Redis, Kafka |
| `COLLECTION_MANIFEST_DESIGN.md` | ONE index blob per commit |
| `ROUND_TRIP_AUDIT.md` | Honest cold-read round-trip accounting |
| `POND_WHITEPAPER.md` | The contribution (20 pages) |
| `WHERE_POND_FAILS.md` | Honest scope + Lens roadmap |
| `LENS_GUIDE.md` | How to write a Lens |
| `GETTING_STARTED.md` | 5-minute tutorial |
| `NON_GOALS.md` | What Pond doesn't do |
| `BINARY_ENCODING_FORMAT.md` | PND1 column encoding spec |
| `GENERIC_DESIGN_VISION.md` | The "any app" promise |
| `POND_FORMAL_ALGEBRAS.md` | 17 algebras, 10 axioms |
| `POND_PHASE_Q_BENCHMARKS.md` | Head-to-head benchmarks (legacy kernel) |
| `POSTMORTEM_PROLLY_TREE_BUG.md` | Prolly tree bug postmortem |
| `../DESIGN_GOALS.md` | Canonical entry point for the project |

## Archive

The `archive/` subdirectory contains 23+ historical documents:
- `ARCHITECTURE_REVIEW_EXTERNAL.md` (pre-Round-1 review, findings addressed)
- Architecture reviews #2, #3 (findings implemented)
- Design review 2026-07-26 (42 findings, all addressed)
- Workload analysis PB scale (hierarchical stats tree now implemented)
- Phase L/N/O/P/Q reports (superseded by whitepaper)
- Second and Third Red Team reviews (findings folded into formal algebras)
- Original storage model paper (superseded by whitepaper)
- Original mathematical model (superseded by formal algebras)
- 3 lens docs (merged into LENS_GUIDE.md)
- DELETE_90_PERCENT.md (recommendation executed)
- RFCs (13 design docs, decisions folded into formal algebras)

These are kept for historical reference. They are not needed to
understand Pond.
