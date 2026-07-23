# pond-sdk

The Lens SDK — Layer 1 (state) and Layer 2 (access) on top of the kernel.

## What it is

The SDK provides the building blocks for writing Lenses:
- **ProllyViewBase** — the base class for all versioned Lenses. Provides
  tiered commits (delta + snapshot), Prolly tree storage, branching,
  merging, history, and time travel.
- **Lens / View** — the abstract base class. Defines `encode`/`decode`
  that subclasses implement. Alias: `Lens = View` (Lens is preferred;
  View kept for backward compatibility).
- **AutoIndex / IndexedView** — Physical Structure for secondary indexes.
  Indexes are Prolly trees mapping key→blob_hash. Metadata only; data
  blobs are never touched.
- **ViewQuery** — lazy, composable query API (`.where()`, `.select()`,
  `.map()`, `.join()`, `.collect()`).
- **Collection** — a named collection of bytes with metadata and
  namespace support.
- **Maintenance** — tombstone helpers (RFC-0008: deletion as data).

## Files

| File | Purpose | LOC |
|---|---|---|
| `lens_sdk.py` | Lens/View base class, CrossLens, SemanticLens, index management | 846 |
| `prolly_view.py` | ProllyViewBase (tiered commits, trees, branching, merge) | 761 |
| `auto_index.py` | IndexedView (auto-indexing, lazy/eager/incremental) | 604 |
| `collection.py` | Collection (reference namespace + metadata) | 517 |
| `lens_laws.py` | RFC-0007 Lens algebra property tests | 587 |
| `architecture_laws.py` | 10 executable architecture laws | 557 |
| `binary_encoding.py` | Binary Prolly tree encoding (metadata optimization) | 323 |
| `test_shared_lenses.py` | Test: multiple Lenses sharing same byte graph | 441 |
| `test_lens_architecture.py` | Test: multi-Lens architecture proof | 449 |
| `lens_query.py` | ViewQuery (lazy query API) | 285 |
| `test_lens_query.py` | Test: ViewQuery | 327 |
| `maintenance.py` | Tombstone helpers (RFC-0008) | 315 |
| `run_lens_laws_ci.py` | CI runner for Lens contracts | 267 |

## Usage

```python
import sys; sys.path.insert(0, "pond-core"); sys.path.insert(0, "pond-sdk")
from pond_minimal import PondMinimal
from lens_sdk import Lens

class MyLens(Lens):
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
python pond-sdk/architecture_laws.py    # 10 architecture laws
python pond-sdk/lens_laws.py            # RFC-0007 Lens algebra
python pond-sdk/test_shared_lenses.py   # multi-Lens sharing
python pond-sdk/test_lens_architecture.py
python pond-sdk/test_lens_query.py
```
