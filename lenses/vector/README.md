# lenses/vector/

The **VectorLens** — the vector database lens for Pond.

## What it is

A vector database built on the KeyValueLens. Vectors are stored as
**packed binary** (`struct.pack`) rather than JSON, for efficiency.
The overridden `encode` / `decode` methods own the wire format:

```
+-------------------+-----------------------------+
| vec_len           | uint32  little-endian  (4B) |
| vector[0..N)      | N x float64 little-endian   |
| id_len            | uint32  little-endian  (4B) |
| id (utf-8)        | id_len bytes                |
| meta_len          | uint32  little-endian  (4B) |
| metadata (json)   | meta_len bytes              |
+-------------------+-----------------------------+
```

The `id` is stored inside the blob so the index extractor (which only
sees the decoded value, not the lens key) can still pull it out.

## Capabilities

- `insert(id, vector, metadata)`
- `search(query, k=5)` — k-nearest-neighbours (L2 / Euclidean, linear scan)
- `get(id)` / `delete(id)`
- `list_vectors()` / `count()`
- Branching, merge, history (inherited from KeyValueLens, re-exposed
  with domain-friendly names)

## Indexing

Uses `CollectionMetadata` (data-side) for indexing. An `"by_id"` index
is registered in eager mode so ID lookups go through the indexing
layer. Indexes belong to the collection, not the lens — any lens
reading the same collection sees them.

## Files

| File | Purpose |
|---|---|
| `vector_lens.py` | `VectorLens` |
| `test_vector.py` | Self-test |
| `__init__.py` | Package exports |

## Architecture

Extends `KeyValueLens` (which lives in `pond-sdk/`, not in `lenses/`),
so this is NOT lens-to-lens inheritance between production lenses — it
inherits SDK infrastructure, not another production lens. The packed
binary encoding is the only lens-specific logic.

## Dependencies

- `pond-core/` (kernel)
- `pond-sdk/` (KeyValueLens, CollectionMetadata)
- Python stdlib only
