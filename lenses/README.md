# lenses/

Layer 3: Lens implementations.

A **Lens** is an interpretation layer over immutable bytes — it defines
`encode` (domain object → bytes) and `decode` (bytes → domain object).
The kernel stores bytes; the Lens interprets them.

## What's here

| Lens | Purpose | Status |
|---|---|---|
| `lakehouse/` | DuckDB-based lakehouse (CREATE TABLE, INSERT, SELECT, time travel, branching, merge, schema evolution) | **Flagship** — 10 tests pass |
| `vector/` | Vector DB with ANN search | Working — uses mock kernel for testing |

## The flagship: `lakehouse/`

`lenses/lakehouse/lakehouse.py` is the flagship demonstration that the
Lens algebra covers a real workload. It provides:

- `LakehouseLens` — tabular semantics on Pond via Parquet
- `PondLakehouse` — full lakehouse = Pond kernel + LakehouseLens + DuckDB

Features (all tested):
- CREATE TABLE, INSERT, SELECT (WHERE, ORDER BY, GROUP BY, JOIN, aggregation)
- Time travel (query at old commit hash)
- Branching (dev/test branches on tables)
- Merge (2-parent merge commit)
- Schema evolution (Parquet-native; add column, old rows get NULL)

Benchmark vs native DuckDB+Parquet (10K rows):
- create: 15% overhead
- COUNT(*): 260% overhead (re-registering tables each query; production would cache)
- filter + scan: 127% overhead

## The killer demo: cross-Lens interop

The lakehouse Lens interoperates with the Feature Store Lens
(`pond-labs/feature_store_lens.py`) without coordination — they share
the kernel's refs and bytes. See `pond-labs/interop_demo.py` (12/12 pass).

## Adding a new Lens

1. Read `docs/LENS_GUIDE.md` (the Lens author's contract).
2. Subclass `Lens` from `pond-sdk/lens_sdk.py`.
3. Implement `encode` and `decode`.
4. Use `ProllyViewBase` for versioning, branching, time travel.
5. Add tests.
6. Verify the 7 design goals (`DESIGN_GOALS.md` §3) are served.

## Dependencies

- `pond-core/` (kernel)
- `pond-sdk/` (Lens base class, ProllyViewBase)
- `lakehouse/`: `duckdb`, `pyarrow`
- `vector/`: stdlib only (uses mock kernel)
