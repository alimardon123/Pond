# Pond Compatibility Suite

> Like LLVM: the contracts are the innovation, not the number of
> optimizations. Every Lens must pass the same certification.
> Every Physical Structure must pass the same certification.

## Philosophy

Pond is not famous because it has many Lenses. Pond is valuable if
every Lens obeys the same contracts. The contracts are the innovation.

This suite defines those contracts. Every Lens, every Physical
Structure, and (eventually) every execution engine must pass.

## Certification levels

### Level 1: Lens Compatibility (REQUIRED for every Lens)

Every Lens must pass:

| Contract | What it tests |
|---|---|
| Read compatibility | Data written by Lens A is readable by Lens B |
| Write compatibility | Data written by Lens B is readable by Lens A |
| Branch compatibility | Branch on A doesn't affect B's HEAD |
| Merge compatibility | Merge on A produces valid state for B |
| Time travel compatibility | Any commit readable by any Lens |
| Schema compatibility | Schema evolution on A is visible to B |
| Index compatibility | Physical Structures remain valid across Lenses |
| Storage compatibility | Same bytes, same refs, same commit DAG |

### Level 2: Physical Structure Compatibility (REQUIRED for every PS)

Every Physical Structure must pass:

| Contract | What it tests |
|---|---|
| Build | Structure can be built from any Lens's data |
| Query | Any Lens can query the structure |
| Cross-Lens | Structure built by Lens A is usable by Lens B |
| Rebuildability | Structure can be lost and rebuilt from snapshot |
| Delete | Deleting one structure doesn't affect others |
| Coexistence | Multiple structure types coexist in same kernel |

### Level 3: Storage Independence (FUTURE — for execution engines)

| Contract | What it tests |
|---|---|
| Engine swap | Switch DuckDB → Polars → Spark without rewriting storage |
| No engine ownership | No execution engine writes to kernel directly |
| Same bytes | All engines see the same immutable bytes |

## Current status

| Certification | Status |
|---|---|
| Level 1 (Lakehouse ↔ FeatureStore) | ✅ 10/10 pass (Track 1) |
| Level 2 (BloomFilter, Statistics, ZoneMap) | ✅ 18/18 pass (Track 2) |
| Level 3 (Storage Independence) | ✅ 23/23 pass (Track 8) |

## Running

```bash
# Level 1: Lens compatibility
python pond-lab/track1_compat_matrix.py

# Level 2: Physical Structure compatibility
python pond-lab/track2_index_portability.py
```

## Adding a new Lens to the suite

When a new Lens is added, it MUST pass Level 1 against at least one
existing Lens. The test matrix grows:

```
         Lakehouse  FeatureStore  NewLens  ...
Lakehouse     ✓          ✓           ?
FeatureStore  ✓          ✓           ?
NewLens       ?          ?           ✓
```

Every `?` must be resolved before the Lens is considered compatible.
