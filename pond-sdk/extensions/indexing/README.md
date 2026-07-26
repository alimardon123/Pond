# extensions/indexing/

Collection-level secondary indexes for Pond.

## What it is

Tools for building and querying secondary indexes on collections.
**Indexes belong to collections (data-side), not to lenses.** Any
lens reading a collection can use that collection's indexes — this is
the cross-lens sharing contract (Track 2 proved it works).

## Files

| File | Exports | Status |
|---|---|---|
| `collection_index.py` | `CollectionIndexer` | **Recommended.** Data-side, lens-independent. |
| `auto_index.py` | `AutoIndexMixin`, `AutoIndex`, `IndexedLens` (lazy) | **Deprecated.** Lens-mixin approach. Kept for backward compat. |

## CollectionIndexer (recommended)

Standalone collection-level indexer. Operates on `kernel + collection
name`. No lens dependency. Follows all design principles.

```python
from extensions.indexing import CollectionIndexer

idx = CollectionIndexer(kernel)
idx.build("users", index_name="by_email",
          key_fn=lambda rowid, row: row["email"])
hits = idx.query("users", "by_email", "alice@example.com")
```

GENERIC: works with any lens that provides a `scan_rows` callback
yielding `(rowid, row_dict)` pairs. For KV lenses, the default scan
reads the ProllyTreeIndex directly. For tabular lenses, the caller
provides `scan_rows` (e.g. from `LakehouseLens.iterate`).

## AutoIndexMixin (deprecated)

Legacy lens-mixin approach: `class MyLens(KeyValueLens, AutoIndexMixin): ...`.
Has a Principle 6 violation (imports from `lenses/keyvalue/`). New
code should use `CollectionIndexer`.

## Architecture

- Supported storage: ProllyTreeIndex (the universal storage backend)
- Supported lens types: ALL (KeyValueLens, LakehouseLens, FeatureStoreLens, …)
- Indexes are stored as kernel blobs under `{collection}__index__{name}`.
- Indexes are Physical Structures: deterministic, rebuildable, lossless
  on delete (RFC-0008 tombstones).

## Dependencies

- `pond-core/` (kernel)
- `pond-sdk/` (ProllyTreeIndex, KeyValueLens — lazy import)
