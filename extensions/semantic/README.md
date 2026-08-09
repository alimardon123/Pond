# extensions/semantic/

Semantic model adapters — translate between Pond's internal definitions
and external semantic model formats.

## Status

**Basic trait + definitions ported.** The `SemanticModelAdapter` trait
and `SemanticDefinitions` struct are implemented. Concrete adapters
(Cube, dbt, Malloy) are future work.

## What's here

| File | Purpose |
|---|---|
| `rust/src/lib.rs` | `SemanticModelAdapter` trait, `SemanticDefinitions`, `Metric`, `Dimension`, `Relationship` |

## Architecture

```
SemanticModelAdapter (trait)
  ├── export_model(definitions) → JSON
  ├── import_model(JSON) → definitions
  └── validate_model(JSON) → bool

SemanticDefinitions (internal representation)
  ├── metrics: Vec<Metric>
  ├── dimensions: Vec<Dimension>
  └── relationships: Vec<Relationship>
```

Future adapters:
- `CubeAdapter` — Cube.js semantic model format
- `DbtAdapter` — dbt metrics format
- `MalloyAdapter` — Malloy model format

Note: "Ossie" was a placeholder name in the Python code, not a real
semantic model spec. It has been replaced with this clean trait.
