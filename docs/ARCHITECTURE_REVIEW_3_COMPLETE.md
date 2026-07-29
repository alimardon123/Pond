# Complete Project Architecture Review (#3)

**Date:** 2026-07-29
**Reviewer:** Senior distributed systems architect (first-time, whole project)
**Verdict:** "50% of the way to its stated goal. The kernel concept is right. The encoding format is right. But the storage engine implementation is wrong (not actually a Prolly tree, read_all everywhere), the S3 story is missing, and the index architecture is fragmented."

## Top 5 Critical Issues (existential — fix FIRST)

### 1. `read_all()` is called by EVERY range/point/diff/merge operation
Every "O(log N)" claim is FALSE. `range_read`, `range_point_lookup`, `diff`, `merge`, and `_compute_full_state` (called by every snapshot commit) all call `read_all()` which recursively reads the ENTIRE tree.
**Fix:** Add `ProllyTree.range_scan(start_key, end_key)` that walks the tree using internal-node pointers. O(log N + K).

### 2. The "Prolly tree" is NOT actually a Prolly tree
`_rolling_hash_boundary()` is defined but NEVER CALLED. `ProllyTree.build()` uses fixed-size chunks. This breaks structural sharing for inserts and the O(d) diff claim.
**Fix:** Either implement real Prolly boundaries (BuzHash) or rename to `FixedChunkBTree` and update docs.

### 3. No real S3 backend; refs cannot live on S3
`kernel.py` is bound to local disk + SQLite. `s3_mock_backend.py` is a mock. The Ref primitive cannot work on S3 without a separate coordination service or S3 conditional writes.
**Fix:** Build `s3_backend.py` with conditional writes for refs, or admit local-disk-only.

### 4. StatsIndex and embedded_stats are DEAD CODE
StatsIndex (177 LOC, 0 lens references), embedded_stats (207 LOC, 0 lens references). Production still uses the old ZoneMapIndex (467 LOC). The "refactored" design exists only on paper.
**Fix:** Wire in embedded_stats, delete ZoneMapIndex, or delete StatsIndex.

### 5. COMPACTION_THRESHOLD bug (STILL unfixed from review #2)
`prolly_tree.py:73` says `= 1` but `:333` uses literal `16`. "Always snapshot" is false.

## Top 5 Strengths

1. **PND1 binary format spec** — frozen, well-designed, SIMD-ready in theory
2. **Encoding layer** (`encoding.py`) — Vortex-style predicate eval, numpy fast paths
3. **3-primitive kernel concept** — intellectually sound, TLA+ verified
4. **ColumnSource protocol** — clean format-agnostic data access
5. **Lens independence rule** — no lens-to-lens inheritance, each lens removable

## The Path Forward (prioritized)

1. **Fix `read_all()`** — Add `ProllyTree.range_scan()`. Replace every `read_all` call. ONE WEEK of work, unblocks PB scale.
2. **Build real S3 backend** — conditional writes for refs
3. **Implement Collection Manifest** — one blob per commit, all index roots
4. **Wire in embedded_stats** — replace separate zone-map blobs
5. **Fix COMPACTION_THRESHOLD** — one-line fix
6. **Delete StatsIndex or ZoneMapIndex** — pick one
7. **Rename ProllyTree or implement real Prolly boundaries**
8. **Build real ANN index for VectorLens**
9. **Split lakehouse_lens.py** (1802 LOC → 4 files)
10. **Write one production S3 benchmark**
