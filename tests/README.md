# tests/

All Pond tests, organized by purpose.

## What it is

Per `REPO_ORGANIZATION.md` §2.6, every test lives in `tests/` or a
subdirectory. Test files are NOT scattered inside `pond-sdk/` or
`lenses/` — those directories contain only production code. The
single entry point is `tests/test_all.py`.

Subdirectories reflect test **purpose**, not the code being tested.

## Layout

```
tests/
├── test_all.py          # Single pytest entry point (runs everything)
├── architecture/        # Architecture law tests (executable specification)
│   └── architecture_laws.py
├── lens_algebra/        # RFC-0007 Lens algebra property tests
│   ├── lens_laws.py
│   └── run_lens_laws_ci.py
└── integration/         # Integration tests (multi-lens, cross-lens)
    ├── test_shared_lenses.py
    ├── test_lens_architecture.py
    ├── test_lens_query.py
    ├── test_pruning.py
    ├── test_lakehouse_pruning.py
    └── test_kv_pruning_and_projection.py
```

## Subdirectory purpose

| Subdir | Purpose |
|---|---|
| `architecture/` | The executable specification of Pond — 18 laws that must always hold. |
| `lens_algebra/` | RFC-0007 Lens algebra property tests — 6 laws any Lens must satisfy. |
| `integration/` | Multi-lens sharing, cross-lens architecture, lazy query, pruning. |

## Running

```bash
# Run everything (the canonical CI command)
pytest tests/test_all.py -v
# or:
python -m pytest tests/test_all.py -v

# Run one subdirectory's laws directly
python tests/architecture/architecture_laws.py
python tests/lens_algebra/lens_laws.py
python tests/integration/test_pruning.py
```

`test_all.py` shells out to the underlying scripts (and also to
scripts in `scripts/`, lab tracks, and lens self-tests), so failures
are reported with the offending script's tail output.

## Dependencies

- `pond-core/`, `pond-sdk/`, `lenses/`, `pond-labs/`
- `pytest` (only needed for the top-level entry point)
- `pyarrow`, `duckdb` (for lakehouse / integration tests)
