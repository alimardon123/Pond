# Pond Documentation

> Start here. The rest is organized by purpose.

## Current Status

**[`STATUS.md`](STATUS.md)** — What's done, what's in progress, what's next.
The migration from Python to Rust is underway: Rust core is done, Python
lenses are still in production use.

---

## Essential reading (in order)

1. **[`../README.md`](../README.md)** — 5-minute intro + quick start.
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
12. **[`UNIVERSAL_STORAGE_ARROW_DESIGN.md`](UNIVERSAL_STORAGE_ARROW_DESIGN.md)** —
    design decision for Arrow integration at PB scale.

## If you want to verify the claims

13. **[`POND_FORMAL_ALGEBRAS.md`](POND_FORMAL_ALGEBRAS.md)** — formal model.
14. **[`POSTMORTEM_PROLLY_TREE_BUG.md`](POSTMORTEM_PROLLY_TREE_BUG.md)** —
    postmortem of the critical Prolly tree encoding bug.
15. **[`GENERIC_DESIGN_VISION.md`](GENERIC_DESIGN_VISION.md)** — the
    "any app" promise.
16. **[`POND_PHASE_Q_BENCHMARKS.md`](POND_PHASE_Q_BENCHMARKS.md)** —
    head-to-head benchmarks vs Git, Dolt, Iceberg.

## Architecture reviews (historical, in `archive/`)

- **[`archive/superseded/VETERAN_ARCHITECT_REVIEW_V2.md`](archive/superseded/VETERAN_ARCHITECT_REVIEW_V2.md)** —
  latest (V2) review. V1 is also in archive.
- **[`archive/ARCHITECTURE_REVIEW_2_UNIFIED_STORAGE.md`](archive/ARCHITECTURE_REVIEW_2_UNIFIED_STORAGE.md)** —
  proposed the manifest + stats tree (now implemented).
- **[`archive/ARCHITECTURE_REVIEW_3_COMPLETE.md`](archive/ARCHITECTURE_REVIEW_3_COMPLETE.md)** —
  5 critical findings (all addressed by 3 rounds of fixes).
- **[`archive/DESIGN_REVIEW_2026_07_26.md`](archive/DESIGN_REVIEW_2026_07_26.md)** —
  42 findings on pre-fix code (superseded by unified storage).

## Active docs

| Doc | Purpose |
|---|---|
| `STATUS.md` | Current migration status + next steps |
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
| `UNIVERSAL_STORAGE_ARROW_DESIGN.md` | Arrow integration design decision |
| `GENERIC_DESIGN_VISION.md` | The "any app" promise |
| `POND_FORMAL_ALGEBRAS.md` | 17 algebras, 10 axioms |
| `POND_PHASE_Q_BENCHMARKS.md` | Head-to-head benchmarks |
| `POSTMORTEM_PROLLY_TREE_BUG.md` | Prolly tree bug postmortem |
| `VETERAN_ARCHITECT_REVIEW_V2.md` | Latest architect review |

## Archive

The `archive/` subdirectory contains historical documents:

- **`archive/superseded/`** — docs that have been superseded by current code
  or newer docs (architecture redesign, repo reorganization plans, migration
  strategy, cross-language SDK design, veteran review process, etc.)
- **`archive/`** (root) — older architecture reviews, phase reports, RFCs,
  red team reviews, formal specs (all superseded by the active docs above)

These are kept for historical reference. They are not needed to understand
Pond's current state — read `STATUS.md` for that.
