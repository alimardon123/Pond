# extensions/semantic/

Semantic Layer adapters for Pond.

> **STATUS**: This directory contains the LEGACY Python adapter interface
> (`SemanticModelAdapter`, `SemanticLens`). The new primary API is
> `SemanticLayer`, implemented in Rust/PyO3 and exposed via the `pond`
> module: `m = s.layer('sales', adapters=['ossie'])`. See
> `bindings/python/pyo3/src/lib.rs` for the implementation.

## Purpose

A **Semantic Layer** is a coherent set of metrics/dimensions/relationships/
datasets over Pond collections, exposed to external systems (BI tools,
query engines, AI agents) via one or more adapters.

### Why "Layer" (not "Model")

The word "model" collides with ML models, which Pond may host in the future.
"Semantic Layer" is the industry-standard term (dbt Semantic Layer, Cube
Semantic Layer, Looker LookML). Each layer is one coherent set of definitions
for a domain (`sales`, `product`, `finance`). You can have many layers; each
can be exposed via multiple adapters simultaneously.

## New API (recommended) — via the `pond` PyO3 module

```python
import pond
s = pond.Storage('/path/to/.pond')

# Create a layer with multiple adapters + reflection
m = s.layer('sales', adapters=['ossie', 'cube'], enable_reflection=True)

# Batch operations — add multiple items at once
m.add_datasets(['orders', 'users', 'products'])
m.add_metrics({'revenue': 'SUM(orders.amount)', 'count': 'COUNT(orders.id)'})
m.add_dimensions({
    'country': ('users', 'country', 'string'),
    'order_date': ('orders', 'created_at', 'datetime'),
})
m.add_relationships({
    'user_orders': ('users', 'orders', 'users.id = orders.user_id'),
})

# Inspect
m.info()           # → full spec dict
m.datasets()       # → ['orders', 'users', 'products']
m.metrics()        # → ['revenue', 'count']
m.adapters()       # → ['ossie', 'cube']

# Independent adapter management (multi-adapter)
m.add_adapter('dbt')         # add another adapter
m.remove_adapter('cube')     # → True

# Reflection management
m.enable_reflection()
m.disable_reflection()

# Export (OPTIONAL — auto-exposure is the default)
m.export('ossie')  # → dict in Ossie format
m.export()         # → uses first adapter in the list

# List all layers
s.layers()  # → ['sales']
```

## Design principles

1. **Optional adapter** — defaults to `['ossie']` if `adapters=None`
2. **Multiple adapters** — one layer can be exposed via Ossie + Cube + dbt
   simultaneously. External systems query the same layer through whichever
   adapter they speak.
3. **Independent adapter management** — `add_adapter` / `remove_adapter`
   don't touch the spec (datasets/metrics/dimensions/relationships).
4. **Auto-exposure** — no explicit export step. When you register an
   adapter, the layer is queryable via that adapter's protocol. Adapters
   read the layer's spec directly from storage.
5. **Batch operations** — add multiple datasets/metrics/dimensions/
   relationships in one call. Idempotent.
6. **Reflection** — optional, off by default. When enabled, the layer is
   registered for query acceleration (Dremio-style).
7. **Storage-independent** — works on local FS / S3 / anything Pond supports.

## Storage layout

```
semantic_layers/{name}/_meta            → {name, adapters, enable_reflection}
semantic_layers/{name}/datasets/{ds}    → {name, source}
semantic_layers/{name}/metrics/{name}   → {name, expression, description, format}
semantic_layers/{name}/dimensions/{name} → {name, dataset, field, data_type}
semantic_layers/{name}/relationships/{name} → {name, from, to, condition}
```

## Legacy API (deprecated — kept for backward compat)

```python
# Default (Ossie adapter) — LEGACY
from extensions.semantic.ossie import SemanticLens
semantic = SemanticLens(kernel, "semantic")

# Custom adapter — LEGACY
from extensions.semantic.base import SemanticModelAdapter
from extensions.semantic.ossie import SemanticLens

class CubeAdapter(SemanticModelAdapter):
    def export_model(self, lens) -> dict: ...
    def import_model(self, lens, model: dict) -> None: ...

semantic = SemanticLens(kernel, "semantic", adapter=CubeAdapter())
```

The legacy API is single-adapter (one adapter per SemanticLens). New code
should use the multi-adapter `SemanticLayer` via `s.layer()` instead.

## Files

| File | Exports | Purpose |
|---|---|---|
| `base.py` | `SemanticModelAdapter` | LEGACY abstract interface. Adapters implement `export_model()`, `import_model()`, `validate_model()`. |
| `ossie.py` | `SemanticLens`, `OssieAdapter` | LEGACY concrete Ossie adapter + SemanticLens wrapper. Single-adapter only. |

The new `SemanticLayer` class lives in `bindings/python/pyo3/src/lib.rs`
(Rust/PyO3). Adapters (e.g., Ossie) live in `extensions/semantic/*/rust/`.

## Architecture

The `SemanticLayer` stores the spec (datasets/metrics/dimensions/relationships)
as kernel blobs under `semantic_layers/{name}/...`. Adapters translate
between this canonical spec and external formats on demand. The adapter does
NOT own data — it translates.

This means:
- The same layer can be exposed via Ossie, Cube, dbt simultaneously.
- Adding/removing an adapter doesn't change the spec.
- The kernel never knows which semantic standard is in use.
- External systems query the layer via whichever adapter they speak — no
  explicit export step.
