# pond-labs/demos/

Demonstration scripts. **NOT production.**

## What it is

End-to-end demos that prove (or falsify) the Lens algebra on real
workloads. Each demo is the kind of artifact that answers: *"What can
people build with Pond that they currently cannot build simply?"*

Per `REPO_ORGANIZATION.md` §2.5, code here is experimental — it may
break or be replaced. It is not imported by production code.

## Currently

| File | Demo | Status |
|---|---|---|
| `interop_demo.py` | Feature Store ↔ Lakehouse bidirectional interop | **12/12 checks pass.** The killer demo. |

## interop_demo.py

Two independently-developed Lenses (Feature Store + Lakehouse) share
data, branches, time travel, and schema evolution — with zero ETL,
zero sync, zero coordination code. Neither Lens knows about the other;
they share only the kernel's refs, blobs, and commit graph.

6 phases, 12 checks:

1. Feature Store writes → Lakehouse reads (via SQL)
2. Lakehouse branches → Feature Store sees it
3. Time travel across Lenses (same commit hash, same data)
4. Schema evolution propagates (Parquet-native)
5. Shared commit history
6. Cross-Lens workflow (FS trains → LH analyzes → LH branches → FS merges)

## Running

```bash
python pond-labs/demos/interop_demo.py
```

## Architecture

The demo proves Pond's central claim: that one immutable substrate
can support many specialized systems that interoperate without ETL.
If the demo stops passing, the architecture is broken — file an issue
before touching `pond-sdk/` or `lenses/`.

## Dependencies

- `pond-core/` (kernel)
- `pond-sdk/` (base lens, ProllyTreeIndex)
- `lenses/lakehouse/` (LakehouseLens)
- `pond-labs/lenses/` (FeatureStoreLens)
- `pyarrow`, `duckdb`
