# extensions/semantic/

Semantic model adapters — translate between Pond's internal definitions
and external semantic model formats.

## Structure

```
extensions/semantic/
├── base/rust/       # SemanticModelAdapter trait + SemanticDefinitions
├── ossie/rust/      # OssieAdapter — Apache Ossie format
└── README.md        # This file
```

## Base

| File | Purpose |
|---|---|
| `base/rust/src/lib.rs` | `SemanticModelAdapter` trait, `SemanticDefinitions`, `Metric`, `Dimension`, `Relationship` |

The base crate defines:
- `SemanticModelAdapter` trait: `export_model`, `import_model`, `validate_model`, `name`
- `SemanticDefinitions`: internal representation (metrics, dimensions, relationships)
- JSON serialization/deserialization

## Ossie Adapter

| File | Purpose |
|---|---|
| `ossie/rust/src/lib.rs` | `OssieAdapter` — translates between Pond definitions and Ossie format |

The Ossie format:
```json
{
  "name": "model_name",
  "metrics": [{"name": "revenue", "expression": {"dialects": {"ANSI_SQL": "SUM(amount)"}}}],
  "dimensions": [{"name": "country", "type": "string"}],
  "relationships": [{"name": "user_orders", "from": {"dataset": "users"}, "to": {"dataset": "orders"}}]
}
```

## Adding New Adapters

To add a new semantic standard (e.g., Cube.js):
1. Create `extensions/semantic/cube/rust/` with a new crate
2. Implement `SemanticModelAdapter` trait
3. Add to this README

Each adapter is independent — adding a new one doesn't modify existing code.

## Design Principles

- **Simple**: One trait, one struct, one adapter per format
- **Independent**: Adapters don't depend on each other
- **Generic**: Works with any collection via SemanticDefinitions
- **Orthogonal**: Semantic models are orthogonal to storage — data can live in any lens
