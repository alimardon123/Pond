# lenses/lakehouse/

The **LakehouseLens** — the app-facing tabular lens for Pond.

## What it is

A lightweight alternative to Spark/Iceberg/Databricks built on the
Pond kernel + DuckDB. The flagship application that tests whether
Pond's Lens algebra covers real workloads.

Two storage paths coexist inside one lens:

1. **Whole-table Parquet I/O** (default OLAP fast path):
   `create_table` / `insert` / `read_table` — one Parquet blob per
   commit, queried by DuckDB with full vectorized pushdown.
2. **Range read/write over the ProllyTreeIndex** (operational path):
   row groups keyed by primary key. O(log N) point lookups,
   O(log N + K) range scans, independent row-group updates.

## Capabilities

- `CREATE TABLE`, `INSERT`, `SELECT` (WHERE, ORDER BY, GROUP BY, JOIN)
- Time travel (query at old commit hash)
- Branching (dev/test branches on tables)
- Merge (2-parent merge commit)
- Schema evolution (Parquet-native; add column → old rows get NULL)
- Range read / range write over ProllyTreeIndex
- Predicate pushdown (zone maps skip row groups)
- Projection pushdown (DuckDB reads only needed columns)

## Files

| File | Purpose |
|---|---|
| `lakehouse_lens.py` | `LakehouseLens`, `PondLakehouse` (kernel + lens + DuckDB) |
| `__init__.py` | Package exports |

## Architecture

`LakehouseLens` extends `PondLens` (from `pond-sdk/base_lens.py`).
It owns its own read/write API and its own storage code — per
`REPO_ORGANIZATION.md` §4, production lenses do NOT inherit from each
other. It is the flagship cross-Lens interop partner: any collection
it writes is readable by the Feature Store Lens (see
`pond-labs/demos/interop_demo.py`).

## Dependencies

- `pond-core/` (kernel)
- `pond-sdk/` (base lens, ProllyTreeIndex, collection metadata)
- `duckdb`, `pyarrow`
