# pond-labs/

Experiments and demonstrations.

This is where new ideas are tested. Code here is **not** production —
it's proof-of-concept that validates (or falsifies) the Lens algebra
on real workloads.

## What's here

| File | Purpose | Tests |
|---|---|---|
| `feature_store_lens.py` | Versioned ML feature store with point-in-time joins | 10/10 pass |
| `interop_demo.py` | Bidirectional Lens interop (Feature Store ↔ Lakehouse) | 12/12 pass |
| `loc_benchmark.py` | LOC saved: 81% reduction vs building from scratch | both implementations pass |

## feature_store_lens.py

A versioned ML feature store on Pond — the "Feast on Pond" demo.

Features:
- Versioned feature definitions (schema evolution: add/rename features)
- Point-in-time joins (prevents label leakage in ML training)
- Online + offline serving (same data, different access patterns)
- Branching for feature experimentation
- Time travel for reproducible training sets
- Cross-Lens interop with the Lakehouse Lens

## interop_demo.py

**The killer demo.** Proves the Lens algebra is real.

Two independently-developed Lenses (Feature Store + Lakehouse) share
data, branches, time travel, and schema evolution — with zero ETL,
zero sync, zero coordination code.

6 phases, 12 checks:
1. Feature Store writes → Lakehouse reads (via SQL)
2. Lakehouse branches → Feature Store sees it
3. Time travel across Lenses (same commit hash, same data)
4. Schema evolution propagates (Parquet-native)
5. Shared commit history
6. Cross-Lens workflow (FS trains → LH analyzes → LH branches → FS merges)

## loc_benchmark.py

The benchmark that matters: how much code to build a mini lakehouse?

- From scratch (DuckDB + Parquet + manual snapshot management): **120 LOC**
- On Pond (using LakehouseLens): **23 LOC**
- **81% reduction.**

Both implementations pass the same 9-test functional workflow.

## Running

```bash
python pond-labs/feature_store_lens.py    # 10 tests
python pond-labs/interop_demo.py          # 12 tests
python pond-labs/loc_benchmark.py         # LOC comparison
```

## Dependencies

- `pond-core/` (kernel)
- `lenses/lakehouse/` (for interop demo and LOC benchmark)
- `pyarrow`, `duckdb`
