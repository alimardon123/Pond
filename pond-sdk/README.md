# pond-sdk

The Lens SDK — Layer 1 (state) and Layer 2 (access) on top of the kernel.

## What it is

The SDK provides the building blocks for writing Lenses:

- **PondLens** (`pond_lens.py`) — the SHARED NAMESPACE base for ALL
  Lenses. Provides only ref-namespace operations: `branch`,
  `list_collections`, `set_definition`, `get_definition`, `history`.
  No format awareness — each app-facing lens owns its own read/write API.
- **KeyValueLens** (`keyvalue_lens.py`) — the app-facing KEY-VALUE lens.
  Per-row key→blob storage over the ProllyTreeIndex. O(log N) point
  lookups, indexes (metadata only), branching, merge, history, lazy
  query API. Aliases: `Lens = KeyValueLens`, `View = KeyValueLens`
  (kept for backward compatibility).
- **KeylessLens** (`keyvalue_lens.py`) — KeyValueLens subclass that
  auto-generates UUID4 primary keys. Use for event logs, time-series,
  append-only streams.
- **ProllyLensBase** (`prolly_view.py`) — the universal storage backend
  for KV collections. ProllyTreeIndex (probabilistic Merkle tree) +
  tiered commits (delta + snapshot) + branching + merge + history.
- **IndexedLens** (`auto_index.py`) — KeyValueLens-style lens with
  eager/lazy auto-indexing (a Physical Structure for secondary indexes).
- **LensQuery** (`lens_query.py`) — lazy, composable query API
  (`.where()`, `.select()`, `.map()`, `.join()`, `.collect()`).
- **Collection** (`collection.py`) — a named collection of bytes with
  metadata and namespace support.
- **Maintenance** (`maintenance.py`) — tombstone helpers
  (RFC-0008: deletion as data).

## Files

| File | Purpose | LOC |
|---|---|---|
| `pond_lens.py` | PondLens — shared namespace base for all Lenses | 248 |
| `keyvalue_lens.py` | KeyValueLens, KeylessLens, CrossLens (+ Lens/View aliases) | 694 |
| `lens_sdk.py` | Backward-compat shim — re-exports from keyvalue_lens | 47 |
| `prolly_view.py` | ProllyLensBase (tiered commits, ProllyTreeIndex, branching, merge) | 764 |
| `auto_index.py` | IndexedLens (auto-indexing, lazy/eager/incremental) | 607 |
| `collection.py` | Collection (reference namespace + metadata) | 517 |
| `lens_laws.py` | RFC-0007 Lens algebra property tests | 591 |
| `architecture_laws.py` | 12 executable architecture laws | 557 |
| `binary_encoding.py` | Binary Prolly tree encoding (metadata optimization) | 323 |
| `test_shared_lenses.py` | Test: multiple KeyValueLens subclasses sharing same byte graph | 442 |
| `test_lens_architecture.py` | Test: multi-Lens architecture proof (SQL/Git/Notebook) | 449 |
| `lens_query.py` | LensQuery (lazy query API) | 288 |
| `test_lens_query.py` | Test: LensQuery | 327 |
| `maintenance.py` | Tombstone helpers (RFC-0008) | 315 |
| `run_lens_laws_ci.py` | CI runner for Lens contracts | 267 |

## Usage

```python
import sys; sys.path.insert(0, "pond-core"); sys.path.insert(0, "pond-sdk")
from pond_minimal import PondMinimal
from keyvalue_lens import KeyValueLens

class MyLens(KeyValueLens):
    def encode(self, data):
        return json.dumps(data).encode()
    def decode(self, bytes):
        return json.loads(bytes)

kernel = PondMinimal("/tmp/my-pond")
lens = MyLens(kernel, "my-collection")
lens.put("key1", {"hello": "world"})
assert lens.get("key1") == {"hello": "world"}
```

## Dependencies

- `pond-core/` (the kernel)
- Python stdlib only (no external packages)

## Running tests

```bash
python pond-sdk/architecture_laws.py    # 12 architecture laws
python pond-sdk/lens_laws.py            # RFC-0007 Lens algebra
python pond-sdk/test_shared_lenses.py   # multi-Lens sharing
python pond-sdk/test_lens_architecture.py
python pond-sdk/test_lens_query.py
```
