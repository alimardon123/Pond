# pond-labs/lenses/

Lab lens prototypes in development. **NOT production.**

## What it is

Lens prototypes that are not yet ready for `lenses/`. Per
`REPO_ORGANIZATION.md` §2.5, code here is experimental: it may break,
it may be deleted, and lab lenses are flat (no per-lens subdirectory,
unlike production lenses).

When a lab lens is approved for production, it is moved to
`lenses/{lens_name}/` and the promotion is documented in `worklog.md`
(see `REPO_ORGANIZATION.md` §6).

## Currently

| File | Lens | Status |
|---|---|---|
| `feature_store_lens.py` | `FeatureStoreLens` | Versioned ML feature store (the "Feast on Pond" demo). 10/10 self-tests pass. Cross-lens interop with `LakehouseLens` proven in `pond-labs/demos/interop_demo.py`. |

## FeatureStoreLens

A versioned ML feature store built on the Pond kernel. Tests whether
the Lens algebra covers the feature-store workload.

Features:
- Versioned feature definitions (schema evolution: add/rename features)
- Point-in-time joins (prevents label leakage in ML training)
- Online + offline serving (same data, different access patterns)
- Branching for feature experimentation
- Time travel for reproducible training sets
- Cross-Lens interop with the DuckDB Lakehouse Lens

Storage model mirrors the Lakehouse Lens (Parquet in Pond blobs), so
any feature collection is queryable by DuckDB without ETL.

## Promotion note

Before promotion to `lenses/feature_store/`, the lab lens must
satisfy `REPO_ORGANIZATION.md` §4 — no lens-to-lens inheritance.
The lab prototype may inherit from a production lens during
prototyping; before promotion it must extend `PondLens` directly and
own its storage code.

## Dependencies

- `bindings/python/core/` (kernel)
- `bindings/python/sdk/` (base lens, ProllyTreeIndex, collection metadata)
- `pyarrow`, `duckdb` (Parquet I/O, SQL)
