# tests/architecture/

Architecture law tests — Pond's executable specification.

## What it is

NOT unit tests. NOT benchmarks. **Laws** — properties that must
ALWAYS hold. If any law fails, the architecture itself is violated.

This is Pond's executable specification. Every contributor must keep
them green. Every change must be validated against them.

## The 18 laws

**12 KeyValueLens laws** (laws 1–12):

1. **Identity** — once a blob hash exists, its contents never change.
2. **Branch checkout** preserves blobs.
3. **Lens purity** — a Lens may interpret bytes; it may never modify bytes during reading.
4. **Derived rebuild** produces identical hashes.
5. **History replay** equals snapshot.
6. **Scale correctness** — at 10K+, count must equal the number written.
7. **Index rebuild** at scale succeeds without decode errors.
8. **Determinism** — same writes, same ordering, same hashes.
9. **Scale** (regression for the Prolly tree build bug).
10. **Index** (regression for the Prolly tree build bug).
11. **Branch no duplication** — branch creation never duplicates blobs.
12. **Merge true DAG** — merge changes references, not blob contents.

**6 LakehouseLens laws** (laws 13–18):

13. Lakehouse data survives restart.
14. Lakehouse branch isolation.
15. Lakehouse merge true DAG.
16. Lakehouse time travel.
17. Lakehouse range ops.
18. Lakehouse ProllyTreeIndex storage.

## Files

| File | Purpose |
|---|---|
| `architecture_laws.py` | 18 executable laws. Each `law_N_*` function is one law. |

## Running

```bash
python tests/architecture/architecture_laws.py     # standalone
pytest tests/test_all.py::test_architecture_laws   # via CI entry point
```

## Architecture

These laws encode architectural truths, not implementation details.
Adding a feature that breaks a law is a red flag — either the feature
is wrong, or the law is wrong (in which case the spec must change
first). Per `REPO_ORGANIZATION.md` §2.6, this directory reflects test
purpose, not the code under test.

## Dependencies

- `pond-core/`, `pond-sdk/`, `lenses/keyvalue/`, `lenses/lakehouse/`
