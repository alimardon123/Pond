# pond-lab/

> Experiments, not architecture. No more theory. Just tests that
> validate (or falsify) the hypothesis that one immutable substrate
> can support many specialized systems that interoperate without ETL.

## Tracks

| Track | Question | Status |
|---|---|---|
| 1 | Can Lenses interoperate bidirectionally? | **10/10 pass** (Lakehouse ↔ FeatureStore) |
| 2 | Can one Physical Structure accelerate multiple Lenses? | **18/18 pass** (stats, bloom, zone maps) |
| 3 | Can each Lens approach its natural opponent? | **Done** (Lakehouse vs DuckDB+Parquet, FS vs Feast) |
| 4 | How efficient is Pond on object stores (GET/PUT/LIST/HEAD/RTT)? | **Done** (7 experiments; packing = 204x reduction) |
| 5 | Can Lenses compose without ETL (CSV → Lakehouse → Feature → Vector → Search)? | **15/15 pass** (ETL-free chain) |
| 6 | Real-world case studies (clinical data lake, ML platform, etc.) | **25/25 pass** (2 case studies) |
| 7 | Is the abstraction symmetric? (reverse: Vector → Lakehouse → Feature → Search → Git → Vector) | **24/24 pass** (symmetric) |
| 8 | Storage Independence certification (same bytes, different engines) | **23/23 pass** (Level 3 of Compatibility Suite) |
| 9 | Production-quality Lakehouse Lens (caching, invalidation, multi-table) | **20/20 pass** (2.2x speedup with cache) |
| 10 | Storage optimization at scale (packed Parquet, up to 500K records) | **10/10 pass** (996x fewer GETs at 500K) |
| 11 | Head-to-head vs Iceberg at scale (100K + 500K, multi-workload) | **Done** (Pond wins 4/7 ops at 500K) |

## Track 1: Bidirectional Lens Compatibility Matrix

**File:** `track1_compat_matrix.py`

**The formal contract:** every Lens must pass the same compatibility
suite against every other Lens. This is the test that makes interop
a guarantee, not a demo.

For every pair of Lenses (A, B), 6 tests:
1. **Bidirectional:** A writes → B reads; B writes → A reads
2. **Branch-safe:** branch on A doesn't affect B's HEAD
3. **Merge-safe:** merge produces valid state for both
4. **Schema-safe:** schema evolution on A visible to B
5. **Time-travel-safe:** any commit readable by any Lens
6. **Index-compatible:** indexes remain valid or rebuild

**Currently tested:**
- Lakehouse ↔ FeatureStore: **10/10 pass** (all 6 badges)

**Future pairs (when Lenses ship):**
- Lakehouse ↔ SQL
- Lakehouse ↔ Git
- Lakehouse ↔ Vector
- FeatureStore ↔ SQL
- FeatureStore ↔ Vector
- SQL ↔ Git
- etc.

**CI badges:**
```
✓ Bidirectional
✓ Branch-safe
✓ Merge-safe
✓ Schema-safe
✓ Time-travel-safe
✓ Index-compatible
```

## Running

```bash
python pond-lab/track1_compat_matrix.py
```
