# tests/integration/

Integration tests — multi-lens sharing, cross-lens architecture,
lazy query, and pruning.

## What it is

Tests that exercise multiple components together. Per
`REPO_ORGANIZATION.md` §2.6, this directory reflects test **purpose**
(integration across components), not the code under test.

## Currently

| File | Tests |
|---|---|
| `test_shared_lenses.py` | Multiple KeyValueLens subclasses sharing the same Prolly tree (one write → all lenses see it). |
| `test_lens_architecture.py` | Multi-lens architecture proof (SQL / Git / Notebook coexisting on one kernel). |
| `test_lens_query.py` | Elegant cross-lens reading via `LensQuery` (`.where().select().collect()`). |
| `test_pruning.py` | Vortex-style pruning on ProllyTreeIndex — zone maps skip data blobs without decoding them; works for both KV (JSON) and tabular (Parquet). |
| `test_lakehouse_pruning.py` | Lakehouse-specific pruning — row-group skipping via `CollectionMetadata` + DuckDB predicate pushdown. |
| `test_kv_pruning_and_projection.py` | KV pruning + projection — predicate pruning AND column projection on the KV lens. |

## Running

```bash
python tests/integration/test_shared_lenses.py
python tests/integration/test_lens_architecture.py
python tests/integration/test_lens_query.py
python tests/integration/test_pruning.py
python tests/integration/test_lakehouse_pruning.py
python tests/integration/test_kv_pruning_and_projection.py

# Or via the CI entry point:
pytest tests/test_all.py -v
```

`test_pruning`, `test_lakehouse_pruning`, and
`test_kv_pruning_and_projection` are wired into `tests/test_all.py`
directly; the others run via the architecture-law and lab-track
scripts they overlap with.

## Architecture

Integration tests are the safety net for cross-component changes. If
a change to `bindings/python/sdk/` or a lens breaks sharing, query, or pruning,
these tests catch it before the architecture-law scripts do. They
depend on `bindings/python/core`, `bindings/python/sdk`, and `lenses/`.

## Dependencies

- `bindings/python/core/`, `bindings/python/sdk/`, `lenses/lakehouse/`, `lenses/keyvalue/`
- `pyarrow`, `duckdb` (for lakehouse / pruning tests)
