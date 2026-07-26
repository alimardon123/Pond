# tests/lens_algebra/

RFC-0007 Lens algebra property tests.

## What it is

A property-test harness that verifies any Pond Lens satisfies the six
laws of the Lens algebra **V = (Σ, A, E, D, M)** defined in RFC-0007.

The harness is **Lens-agnostic**. A Lens author provides a small
`LensContract` adapter that maps the Lens's API to the harness's
expectations, then runs `check_all`. This makes the laws reusable
across `KeyValueLens`, `LakehouseLens`, `VectorLens`, and any future
Lens without modifying the harness.

## The 6 laws

1. **Round-trip** — `D(E(s)) = s`. Decode is the inverse of encode.
2. **Purity** — operations are deterministic functions of state.
3. **Encoding preservation** — every reachable state is persistable.
4. **Materialization determinism** — materializations are pure
   functions of state.
5. **Composition** — `V1 + V2` is itself a Lens (verified structurally).
6. **Kernel independence** — the kernel never inspects blob contents.

## Files

| File | Purpose |
|---|---|
| `lens_laws.py` | `LensLaws`, `LensContract`, `LawReport` — the harness and the 6 laws. |
| `run_lens_laws_ci.py` | CI runner for Lens contracts. Runs the harness against built-in lenses. |

## Usage

```python
from lens_laws import LensLaws, LensContract

laws = LensLaws(kernel)
report = laws.check_all(my_contract)
assert report.all_passed
```

## Running

```bash
python tests/lens_algebra/run_lens_laws_ci.py     # CI runner
pytest tests/test_all.py::test_lens_algebra       # via CI entry point
```

## Architecture

Per `REPO_ORGANIZATION.md` §2.6, this directory reflects test
purpose (the RFC-0007 algebra), not the code under test. The harness
is the metric **E1** from RFC-0009 (Lens algebra law violations:
hard constraint, target 0).

## Dependencies

- `pond-core/`, `pond-sdk/`
