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
| 4 | How efficient is Pond on object stores (GET/PUT/LIST/HEAD/RTT)? | Pending |
| 5 | Can Lenses compose without ETL (CSV → Lakehouse → Feature → Vector → Search)? | Pending |
| 6 | Real-world case studies (clinical data lake, ML platform, etc.) | Pending |

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
