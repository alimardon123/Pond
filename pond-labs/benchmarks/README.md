# pond-labs/benchmarks/

Performance benchmarks. **NOT production.**

## What it is

Benchmarks that measure whether Pond's abstraction is worth its
overhead. Per `REPO_ORGANIZATION.md` §2.5, code here is experimental.
Each script is standalone — run it directly with `python`.

## Currently

| File | Benchmark | Question |
|---|---|---|
| `pruning_benchmark.py` | Pruning effectiveness (Vortex-style) | How many data blobs does zone-map pruning skip at various selectivities, on both Lakehouse (Parquet) and KV (JSON)? |
| `sql_pushdown_benchmark.py` | SQL pushdown end-to-end | How much faster is `query(sql, use_pruning=True)` vs `use_pruning=False)` on 100K rows with `>`, `IN`, `BETWEEN`? |
| `overhead_audit.py` | Overhead audit | What is the cost of zone maps for every workload type (OLTP write, OLAP write, streaming, point lookup, full scan, object-store round trips)? |
| `loc_benchmark.py` | LOC comparison | How much code does it take to build a mini lakehouse from scratch vs on Pond? (Answer: 120 LOC raw vs 23 LOC on Pond = 81% reduction.) |

## Running

```bash
python pond-labs/benchmarks/pruning_benchmark.py
python pond-labs/benchmarks/sql_pushdown_benchmark.py
python pond-labs/benchmarks/overhead_audit.py
python pond-labs/benchmarks/loc_benchmark.py
```

`loc_benchmark.py` is also wired into `tests/test_all.py` so the LOC
regression runs in CI.

## Architecture

Benchmarks depend on `bindings/python/core`, `bindings/python/sdk`, and `lenses/`. They do
NOT depend on each other — each script is independent.

Per the design goals, raw ms-per-op numbers favor in-process systems;
the **LOC saved** benchmark is the one that matters, because it
favors the right abstraction. Use the others to keep overhead honest.

## Dependencies

- `bindings/python/core/`, `bindings/python/sdk/`, `lenses/lakehouse/`, `lenses/keyvalue/`
- `pyarrow`, `duckdb`
