# bindings/python/sdk

The Pond Python SDK — storage infrastructure and extensions on top of
the kernel.

## What it is

The SDK provides the shared infrastructure for all Pond lenses:

- **PondStorage** (`pond_storage.py`) — the unified SDK entry point.
  Wraps the kernel and provides write/read/branch/merge/shard operations.
- **PondLens** (`base_lens.py`) — DEPRECATED. The shared namespace base
  for lenses. New lenses (including Rust ports) do NOT extend PondLens —
  they just own a UnifiedStorage directly.
- **UnifiedStorage** (`extensions/physical_structures/unified_storage.py`) —
  the production storage backend. PND2 format, CollectionManifest, CRDT
  shards, predicate pruning, projection pushdown, zstd compression.
- **LensQuery** (`row_query.py`) — lazy, composable row-level query builder.
- **UUIDv7** (`uuid7.py`) — time-ordered UUID for distributed row IDs.
- **HLC** (`hlc.py`) — Hybrid Logical Clock for clock-skew-safe LWW.
- **Maintenance** (`maintenance.py`) — tombstone helpers (RFC-0008).
- **PondConfig** (`pond_config.py`) — configuration.

## Extensions (bindings/python/sdk/extensions/)

Optional modules that extend Pond's capabilities:

- **physical_structures/** — UnifiedStorage, CollectionManifest, StatsTree,
  encoding (RLE/DICT/BITPACK/RAW), compression (zstd), PondPack.
- **indexing/** — CollectionIndexer (secondary indexes), IVFIndex (vector
  ANN), HNSWIndex (graph ANN).
- **maintenance/** — GarbageCollector + vacuum.
- **semantic/** — SemanticMixin + OssieAdapter (placeholder).

## Files

| File | Purpose |
|---|---|
| `pond_storage.py` | PondStorage — unified SDK entry point |
| `base_lens.py` | PondLens — DEPRECATED (vestigial base class) |
| `row_query.py` | LensQuery — lazy row-level query builder |
| `uuid7.py` | UUIDv7 time-ordered UUID generation |
| `hlc.py` | Hybrid Logical Clock for CRDT |
| `maintenance.py` | Tombstone helpers (drop_name, is_dropped, etc.) |
| `pond_config.py` | PondConfig — configuration |
| `extensions/` | physical_structures, indexing, maintenance, semantic |

## Dependencies

- `bindings/python/core/` (the Python reference kernel)
- `pond` Rust module (PyO3 — for Rust-accelerated PND2 decode/encode)
- Python stdlib + `zstandard` (for zstd compression)

## Architecture

```
bindings/python/core (kernel: Write, Read, Ref)
    ↓
bindings/python/sdk (pond_storage, extensions/)
    ↓
lenses/ (keyvalue, lakehouse, vector, streaming, oltp)
```

## Rust migration

The Rust core (`core/`) is the canonical implementation. Python code
calls Rust via PyO3 (`import pond`). The Python SDK is maintained for
bug fixes; new development happens in Rust.

See `docs/STATUS.md` for the migration status.
