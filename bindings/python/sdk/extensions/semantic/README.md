# extensions/semantic/

Pluggable semantic model adapters.

## Purpose

Semantic model adapters translate between Pond's internal
metric/dimension/relationship storage and external semantic model
standards. The core Lens SDK is NOT coupled to any specific format.

Different deployments can use different semantic standards:
- **Ossie** (Apache Ossie open semantic interchange spec)
- **Cube** (Cube.js — future)
- **dbt** (dbt metrics — future)
- **Custom** (implement `SemanticModelAdapter`)

## Files

| File | Exports | Purpose |
|---|---|---|
| `base.py` | `SemanticModelAdapter` | Abstract interface. Every adapter implements: `export_model()`, `import_model()`, `validate_model()`. |
| `ossie.py` | `SemanticLens`, `OssieAdapter` | Concrete Ossie implementation. `SemanticLens` takes an `adapter` parameter — swap Ossie for any adapter. |

## Usage

```python
# Default (Ossie adapter)
from extensions.semantic.ossie import SemanticLens
semantic = SemanticLens(kernel, "semantic")

# Custom adapter
from extensions.semantic.base import SemanticModelAdapter
from extensions.semantic.ossie import SemanticLens

class CubeAdapter(SemanticModelAdapter):
    def export_model(self, lens) -> dict: ...
    def import_model(self, lens, model: dict) -> None: ...

semantic = SemanticLens(kernel, "semantic", adapter=CubeAdapter())
```

## Architecture

The `SemanticLens` stores metrics, dimensions, and relationships as
kernel blobs (under `_semantic/` prefix). The adapter ONLY translates
between this internal format and the external standard. The adapter
does NOT own data — it translates.

This means:
- The same Lens can export to Ossie, Cube, and dbt simultaneously
- Switching standards is changing the adapter, not the data
- The kernel never knows which semantic standard is in use
