# lenses/keyvalue/

The **KeyValueLens** — the app-facing key-value lens for Pond.

## What it is

Per-row key→blob storage over the ProllyTreeIndex. Each row is stored
as a single content-addressed blob, keyed by a user-supplied primary
key. O(log N) point lookups via the Prolly tree.

It is **collection-agnostic**: the lens does not bind to a collection
in `__init__`. You pass the collection name to each operation:

```python
lens = KeyValueLens(kernel)
lens.put("users", "user:1", {"name": "alice"})
lens.get("users", "user:1")
lens.commit("users", "msg")
```

This mirrors `LakehouseLens.create_table(name, ...)` and lets one lens
instance operate on any collection.

## Capabilities

- `put` / `get` / `delete` / `commit`
- Branching (`branch`, `checkout`, `merge`) — inherited from ProllyLensBase
- History (time travel via `history` and commit-hash reads)
- Zone maps (predicate pruning via `CollectionMetadata`)
- Pruning (skip data blobs without decoding)
- Lazy row query (`LensQuery` — `.where().select().collect()`)

## Files

| File | Purpose |
|---|---|
| `keyvalue_lens.py` | `KeyValueLens`, `KeylessLens` (UUIDv7 keys) |
| `__init__.py` | Package exports |

## Architecture

`KeyValueLens` extends `PondLens` (from `pond-sdk/base_lens.py`), the
thin shared-namespace base. It owns its read/write API; the base only
provides ref-namespace operations (`branch`, `list_collections`,
`set_definition`, `history`).

It is one of three peer app-facing lenses (KeyValueLens, LakehouseLens,
FeatureStoreLens). Per `REPO_ORGANIZATION.md` §4, production lenses do
NOT inherit from each other — each owns its storage code.

## Dependencies

- `pond-core/` (kernel)
- `pond-sdk/` (`base_lens`, `prolly_tree`, `binary_encoding`,
  `maintenance`, `row_query`, `collection_metadata`)
- Python stdlib only
