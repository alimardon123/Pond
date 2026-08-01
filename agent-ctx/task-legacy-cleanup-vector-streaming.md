# Task: Clean up legacy ProllyLensBase code paths in vector_lens.py & streaming_lens.py

**Agent**: Z.ai Code (single agent — this was a focused refactor not split into subagents)
**Date**: 2025-01
**Files touched**:
  - `lenses/vector/vector_lens.py` (813 → 706 lines)
  - `lenses/streaming/streaming_lens.py` (642 → 589 lines)

## What changed (applies to BOTH files)

1. **Removed imports**: `from prolly_tree import ProllyLensBase` (and `ProllyTree`), `from binary_encoding import BinaryProllyTree` (vector only).
2. **Removed `self._bases` dict** from `__init__`.
3. **Removed `_get_base()` method** entirely.
4. **Added `_require_unified()` helper** that raises `RuntimeError` if `self._unified_storage is None`. Called at the start of every I/O method.
5. **Removed every `if self._unified_storage is not None:` check** — the unified path is now the ONLY path. If `self._unified_storage is None`, the method fails loudly via `_require_unified()` instead of silently falling back.
6. **Removed every `# Legacy path` branch** + comment block.
7. **`use_unified_storage=True`** kept in `__init__` signature but truly ignored — no `if` checks anywhere.
8. **Method signatures unchanged** — all public APIs preserved.
9. **`collection` is the consistent parameter name** for the first arg.
10. **Version-control methods** (`create_branch`, `checkout_branch`, `list_branches`, `merge_branch`, `get_history` for vector; `create_branch`, `merge_branch`, `get_history` for streaming) now delegate to `self._unified_storage.<method>(collection, ...)` instead of `self._get_base(collection).<method>(...)`.

## VectorLens-specific changes

- **`insert`** — removed legacy `kernel.write(encode(record))` + `_get_base().stage()` + immediate commit. Now strictly buffers in `_unified_buffer` and auto-commits at 10,000 entries.
- **`commit`** — removed the `if self._unified_storage is None: return ""` early-return.
- **`delete_vector`** — full rewrite. The legacy path used `_get_base().stage_delete()`. UnifiedStorage has no per-row delete, so the new impl reads all rows via `get_all()`, drops the matching id, and rewrites via `write()` (full overwrite, just like `KeyValueLens.commit()`'s tombstone path).
- **`get_vector`, `get_raw`, `list_vectors`, `count`, `get_all`** — removed legacy `# Legacy path` branches; `get_raw` now re-encodes the row via `encode()` (since unified storage doesn't keep per-row blobs).
- **`search`** — unchanged (already used `get_all()` + IVF auto-acceleration via `IVFIndex`).
- **`build_ann_index`, `ann_stats`** — unchanged (already use IVF, no legacy paths).
- **`build_vector_zone_maps`** — became a no-op returning 0. The legacy `ZoneMapIndex` infrastructure (collection_metadata, pruning, column_source) has been moved to `archive/legacy-extensions/` and is no longer importable from `physical_structures/`. Same pattern as `KeyValueLens.build_zone_maps()`.
- **`search_with_pruning`** — now delegates to `self.search()` (linear scan via unified read). Same rationale.
- **`find_by_id`** — was using `CollectionMetadata` secondary indexes (archived). Now delegates to `get_vector()`, which provides O(1) cold point lookup via the manifest.
- **IVF integration** preserved: `search()` still tries `IVFIndex.load()` first, falls back to linear scan.
- File-header + class docstrings updated to reflect UnifiedStorage (was: "ProllyTreeIndex").

## StreamingLens-specific changes

- **`write_stream`** — removed legacy `kernel.write(segment)` + `base.stage(seg_key, blob_hash)` + `base.commit()` block.
- **`read_stream`** — removed the entire legacy time-travel block that walked `_SEG_PREFIX` segment keys. `commit_hash` parameter is kept in the signature but ignored (with a docstring note explaining the unified path always reads from HEAD + shards; use `replay_from()` for offset-based time-travel on partitioned topics).
- **`append_stream`** — removed legacy `_get_base().read_all()` + walk `_SEG_PREFIX` keys block.
- **`stream_size`** — full rewrite. Was entirely legacy (no `if self._unified_storage is not None:` branch). Now uses `read_with_shards()` and sums bytes-typed column values (the `segment` column for streaming-native collections; any bytes-typed column for cross-lens reads).
- **`segment_count`** — removed `# Legacy path` branch.
- **`_read_segment_by_offset`** — removed legacy `base.lookup(seg_key)` block; now uses `read_with_shards()`.
- **Kafka-like features** (`create_topic`, `list_partitions`, `produce`, `produce_round_robin`, `get_latest_offset`, `consume`, `commit_offset`, `_get_offset`, `replay_from`, `_read_segment_by_offset`, `list_consumer_groups`, `get_consumer_group_offsets`) — preserved. Each had its `if self._unified_storage is not None:` guard replaced with `_require_unified()` + a direct call.
- **Version control** (`create_branch`, `merge_branch`, `get_history`) — delegates to UnifiedStorage.
- `_SEG_PREFIX = "seg/"` constant kept but marked as legacy documentation; no code paths use it anymore.
- File-header + class docstrings updated to reflect UnifiedStorage (was: "ProllyTreeIndex maps segment_number → blob_hash").

## Test results (all required tests pass)

```
python scripts/test_multi_workload.py        → 10/10 PASS
python scripts/test_vector_unified.py        →  4/4 PASS
python scripts/test_streaming.py             →  5/5 PASS
python scripts/test_ivf.py                   →  6/6 PASS
python tests/architecture/architecture_laws.py → 18/18 HOLD (Lakehouse laws SKIPped — pyarrow not installed)
```

Additional regression tests run for confidence:
```
python scripts/test_cross_lens_universal.py  →  7/7 PASS
python scripts/test_branch_shards.py         →  5/5 PASS
python scripts/test_concurrency.py           →  5/5 PASS
python scripts/test_range_scan_boundaries.py →  4/4 PASS
python scripts/test_keyvalue_unified.py      →  4/4 PASS
```

## Smoke-test verification of new code paths

The required test scripts don't directly exercise `delete_vector`, `get_raw`, `version control`, or `_require_unified` error paths. A separate smoke test verified:

- `delete_vector("v", "2")` removes the vector; subsequent `count()` returns N-1.
- `get_raw("v", "1")` returns the same binary wire format as `encode({id, vector, metadata})` (vec_len prefix etc.).
- `search_with_pruning(...)` delegates to `search()` and returns k results.
- `build_vector_zone_maps(...)` returns 0 (no-op).
- `find_by_id("v", "1")` returns the same record as `get_vector("v", "1")`.
- `create_branch` / `checkout_branch` / `list_branches` / `get_history` work via UnifiedStorage.
- Setting `vl._unified_storage = None` and calling `insert(...)` raises `RuntimeError` as required.
- Setting `sl._unified_storage = None` and calling `write_stream(...)` raises `RuntimeError` as required.

## Pre-existing issues NOT introduced by this cleanup

- `read_stream(collection, start_byte=50, end_byte=150)` on a 100-byte-segment stream returns 50 bytes instead of 100 (off-by-one in `UnifiedStorage.read`'s `start_key`/`end_key` range scan). Verified pre-existing by `git stash` + rerun on the original code. The required test `test_streaming_workload` uses boundary-aligned ranges (200, 400) so it doesn't hit this.
