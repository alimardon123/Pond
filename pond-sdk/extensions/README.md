# extensions/

> Pluggable modules that sit between the Lens SDK and the kernel.
> **OPTIONAL.** The base Lens works without any extensions loaded.

## Architecture

```
Kernel (Write, Read, Ref) — FROZEN, ~140 LOC
    ↓
Lens SDK (Lens, ProllyLensBase, IndexedLens) — core, no extensions
    ↓
extensions/                    ← YOU ARE HERE
├── semantic/                  — Semantic model adapters (Ossie, Cube, dbt)
└── physical_structures/       — Acceleration structures (Bloom, Stats, ZoneMap)
    ↓
Applications
```

## Design principles

- **3.1 Simple:** The core Lens SDK has zero extension dependencies. Extensions load only when imported.
- **3.4 Scalable:** New extensions are added by implementing an abstract interface. No existing code changes.
- **3.7 Functional:** Extensions make Pond functional for specific use cases without baking any single standard into the core.

## What's here

### semantic/

Pluggable adapters for different semantic model standards. Each adapter
implements `SemanticModelAdapter` (from `semantic/base.py`) and translates
between Pond's internal metric/dimension/relationship storage and an
external semantic model standard.

Available: **Ossie** (Apache Ossie open semantic interchange spec)
Future: Cube.js, dbt metrics, custom

Usage:
```python
from extensions.semantic.ossie import SemanticLens, OssieAdapter

# Default (Ossie adapter)
semantic = SemanticLens(kernel, "semantic")

# Custom adapter (swap Ossie for Cube or your own)
from extensions.semantic.base import SemanticModelAdapter
class MyAdapter(SemanticModelAdapter): ...
semantic = SemanticLens(kernel, "semantic", adapter=MyAdapter())
```

### physical_structures/

Acceleration structures — each is `f(snapshot) → artifact` (deterministic,
rebuildable, per the Physical Structure algebra §14). All implement the
same interface from `physical_structures/base.py`.

Available types:

| Type | Class | Use case | Query complexity |
|---|---|---|---|
| Bloom filter | `BloomFilter` | "Is X in the set?" | O(1) (may have false positives) |
| Statistics | `Statistics` | "Can I skip this column range?" | O(1) (can_prune) |
| Zone map | `ZoneMap` | "Which chunks might contain [min, max]?" | O(chunks) |

The Prolly tree index (`IndexedLens` in `pond-sdk/auto_index.py`) is also
a Physical Structure, but it's built into the SDK core because every Lens
uses it — like LLVM's default optimization passes. Future index types
(HNSW, Trie, etc.) would be added as extensions here.

Usage:
```python
from extensions.physical_structures import BloomFilter, Statistics, ZoneMap

# Build (any Lens can build)
BloomFilter.build(kernel, "users", ["user_1", "user_2", "user_3"])
Statistics.build(kernel, "users", table_data)
ZoneMap.build(kernel, "events", {"chunk_0": [1,2,3], "chunk_1": [4,5,6]})

# Query (any Lens can query — Track 2 proved cross-Lense sharing)
BloomFilter.query(kernel, "users", "user_2")  # → True
Statistics.can_prune(stats, "age", 999)       # → True (skip)
ZoneMap.query(kernel, "events", 5, 7)         # → ["chunk_1"]
```

## Extension registry

Extensions register themselves on import. Discover available extensions:
```python
from extensions import list_extensions, load_extension
print(list_extensions())  # → ["semantic_ossie"]
```

## Adding a new extension

1. Create a new file or subfolder under `extensions/`.
2. Implement the abstract interface (`SemanticModelAdapter` or `PhysicalStructure`).
3. Call `register_extension()` at module level.
4. No existing code needs to change.
