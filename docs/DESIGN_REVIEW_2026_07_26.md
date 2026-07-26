# Pond Design Review — Post-Encoded-Pruning Audit

**Date:** 2026-07-26
**Scope:** All code as of commit `76a9fd0` (encoded-pruning).
**Method:** Two parallel sub-agent reviews against `DESIGN_GOALS.md §3` (the seven principles) and `REPO_ORGANIZATION.md` (folder rules, no lens-to-lens inheritance, extension principles).

---

## Executive summary

The architecture is **fundamentally sound**. The four-level pruning hierarchy
(row-group → column-chunk → encoded → row-level) is the right design, the
storage layer is execution-engine-independent (Principle 8 holds at the bytes
level), and no production lens inherits from another production lens
(REPO_ORGANIZATION.md §4 is respected in the code).

But the recent rapid feature additions (tasks `cc-pruning-scan`, `cc-storage`,
`encoded-pruning`) have introduced **technical debt** that, if not paid down,
will erode the principles:

- **Three write paths and three read paths in `LakehouseLens` duplicate ~90% of
  their bodies** — a violation of Simple (P1) and Beautiful (P6).
- **Two `ZoneMap` classes with the same name** in different files — confusion
  that will cause real bugs.
- **One silent data-loss bug** in `ColumnChunkZoneMap.prune_column_chunks`
  (returns `[]` instead of "all chunks" when a column has no stats).
- **The SQL façade `PondLakehouse.query()` never uses the new fast read paths**
  — the 3.37x speedup is invisible to SQL users.
- **`sys.path.insert` hacks repeated in 6+ method bodies** — works in the dev
  repo but will break under any real install.
- **Extensions hard-code PyArrow** despite docstrings claiming "format-agnostic"
  — a contract violation.

This document lists **42 findings** grouped by severity, then proposes a
prioritized fix plan.

---

## What is correct (do not change)

1. **No lens-to-lens inheritance.** `LakehouseLens(PondLens)`,
   `VectorLens(PondLens)`, `KeyValueLens(PondLens)`,
   `FeatureStoreLens(PondLens)`. `REPO_ORGANIZATION.md §4` is respected.
2. **`FeatureStoreLens` correctly de-inherited from `LakehouseLens`** and owns
   its own storage code (with intentional, documented duplication).
3. **Stored bytes are execution-engine-independent.** No Parquet blob,
   manifest blob, or zone-map blob references DuckDB. Principle 8 holds at the
   storage layer.
4. **`CollectionMetadata` is correctly data-side and lens-agnostic** — it uses
   `scan_fn`/`decode_fn` callbacks rather than importing any lens.
5. **`KeyValueLens`'s collection-agnostic API** (every method takes `name` as
   the first arg) is the right pattern; `LakehouseLens` follows it.
6. **`PondLens.history`** correctly walks the commit chain via type-byte
   dispatch and stops on undecodable commits rather than silently corrupting
   history.
7. **The four-level pruning hierarchy is the right design.** The benchmarks
   prove the wins: 9.37x I/O reduction with column-chunk storage, 3.37x
   speedup with encoded predicate eval.

---

## Findings

### CRITICAL (violates a principle or rule — fix first)

#### C1. Silent data loss in `ColumnChunkZoneMap.prune_column_chunks`

**File:** `pond-sdk/extensions/physical_structures/column_chunk_zone_map.py:180-182`

```python
if column not in self.column_chunks:
    # No stats for this column — can't prune, return all chunks
    return list(range(len(self.column_chunks.get(column, []))))
```

`self.column_chunks.get(column, [])` returns `[]` because the column isn't in
the dict. The comment says "return all chunks" but the code returns `[]`. The
caller treats `[]` as "no surviving chunks" and **silently drops the column**.

**Severity:** correctness bug, silent data loss.
**Fix:** return `None` (meaning "no stats, caller decides") and have callers
fall back to reading all chunks.

---

#### C2. `NameError` in `PruningReader.get_pruning_ratio`

**File:** `pond-sdk/extensions/physical_structures/pruning_reader.py:314`

```python
state = ProllyTree.read_all(self.kernel, tree_root)   # ProllyTree never imported
```

`ProllyTree` is not imported. The method will crash on first use. It is also
dead code (no callers).

**Fix:** delete the method.

---

#### C3. Two `ZoneMap` classes with the same name

**Files:**
- `pond-sdk/extensions/physical_structures/zone_map.py:37` — `class ZoneMap(PhysicalStructure)`: a JSON-stored per-chunk min/max Physical Structure.
- `pond-sdk/extensions/physical_structures/pruning.py:51` — `@dataclass class ZoneMap`: an in-memory row-group min/max container.

`__init__.py` exports the legacy `zone_map.py` version, but every real caller
imports `from pruning import ZoneMap`. The legacy version is dead while still
being the documented API.

**Severity:** confusion that will cause real bugs (which `ZoneMap` did the
caller mean?).
**Fix:** rename or delete the legacy `zone_map.py.ZoneMap`.

---

#### C4. Extensions hard-code PyArrow despite claiming "format-agnostic"

**Files:**
- `pruning.py:71-102` (`ZoneMap.build`)
- `column_chunk_zone_map.py:88-133` (`ColumnChunkZoneMap.build`)
- `column_chunk_storage.py:80-166` (`write_row_group_column_chunks`)
- `encoded_chunk_storage.py:76-169` (`write_row_group_encoded`)

All take a `table` parameter documented as "PyArrow Table" and call
`table.num_rows`, `table.column_names`, `column.slice(...)`,
`pa.Table.from_arrays(...)`, `chunk.to_pylist()`, `pc.min/pc.max/pc.is_null`.

The module docstrings claim format-agnostic. The code is not. A `KeyValueLens`
producing JSON cannot call these methods.

**Severity:** contract violation (P8 Storage-Independent, P4 Scalable).
**Fix:** take an explicit `column_source` callback (`Callable[[str], Iterable[Any]]` + row count) instead of a `table` object.

---

#### C5. `column_chunk_storage` error messages name `LakehouseLens`

**File:** `pond-sdk/extensions/physical_structures/column_chunk_storage.py:95-106`

```python
encode_fn: optional function(column_table) -> bytes for
    encoding each chunk. Defaults to Parquet encoding via
    LakehouseLens._encode_table (the caller is expected to
    pass this in).
...
raise ValueError("encode_fn is required (e.g., lens._encode_table)")
```

Extensions (Layer 2) must not know Layer-3 lenses exist. Same pattern at `:183`,
`:228`.

**Severity:** layering violation (P6 Beautiful).
**Fix:** rewrite messages without naming a lens; fix the docstring.

---

#### C6. `sys.path.insert` hacks in 6+ method bodies

**Files:**
- `zone_map_index.py:56-58`
- `pruning_reader.py:60-62`
- `column_chunk_storage.py:46-48`
- `encoded_chunk_storage.py:51-53`
- `lakehouse_lens.py:428, 498, 607, 991, 1129, 1354`

Each does:
```python
sys.path.insert(0, os.path.join(SCRIPT_DIR, "..", "..", "pond-sdk", "extensions", "physical_structures"))
from column_chunk_zone_map import ColumnChunkZoneMap
```

`sys.path.insert` is not idempotent — it grows the path on every call. Worse,
the lens's import behavior depends on call order. Will break under any real
install (`pip install pond-sdk`).

**Severity:** layering violation (P6), latent install bug.
**Fix:** make `pond-sdk` a proper package; use absolute imports.

---

#### C7. `ZoneMapIndex.commit_zone_maps` couples to `ProllyLensBase` private ref convention

**File:** `pond-sdk/extensions/physical_structures/zone_map_index.py:137`

```python
tree_root = self.kernel.resolve(f"collections/{collection}__zone_maps/HEAD")
```

If `ProllyLensBase` changes its HEAD-ref convention, this silently breaks.

**Fix:** add `ProllyLensBase.head_ref()` and have the extension call it.

---

#### C8. Double-encoding in `EncodedChunkStorage.write_row_group_encoded`

**File:** `pond-sdk/extensions/physical_structures/encoded_chunk_storage.py:120, 166-169, 184-189`

```python
# Line 120 (inside the chunk loop):
encoded_bytes, enc_meta = encode_column(values, hint=hint)   # ← enc_meta discarded
...
# Line 166 (outside the chunk loop):
cczm._encoding_meta = self._build_encoding_meta(table, chunk_size, encoding_hints)

# _build_encoding_meta (line 184-189) re-slices and re-encodes every chunk:
for start in range(0, n_rows, chunk_size):
    _, enc_meta = encode_column(values, hint=hint)   # ← second encode pass
```

The whole `_build_encoding_meta` pass is wasted work. The worklog admits
"encoded write is 3.95x slower" — half of that overhead is this double-encode.

**Fix:** collect `enc_meta` into a list during the main loop and pass it to
the sidecar setter directly.

---

#### C9. Three write paths + three read paths in `LakehouseLens` duplicate ~90% of their bodies

**File:** `lenses/lakehouse/lakehouse_lens.py:357-675, 875-1243`

`range_write` (357), `range_write_column_chunks` (456),
`range_write_encoded` (570) all repeat the same scaffold: import guard,
key-col check, sort, clear old zone maps, slice row groups, build zone map,
commit, notify indexers. The same is true for the three read methods.

**Severity:** violates Simple (P1) and Beautiful (P6).
**Fix:** extract `_range_write_generic(name, table, key_col, row_group_size,
write_one_rowgroup_fn, message, ...)` and `_read_with_pruning_generic(...)`.

---

#### C10. `PondLakehouse.query()` never uses the new fast read paths

**File:** `lenses/lakehouse/lakehouse_lens.py:1761-1799`

`_read_with_pushdown` only calls `self.lens.read_with_pruning(...)`. It never
invokes `read_with_column_chunk_pruning` or `read_with_encoded_pruning`. So
data written via `range_write_column_chunks` or `range_write_encoded` cannot
be queried through the SQL façade with the optimizations that justified
writing it that way.

The 3.37x speedup is invisible to SQL users.

**Severity:** violates Performant (P3) — optimization built but never surfaced.
**Fix:** `_read_with_pushdown` should call `read_with_encoded_pruning` (with
`read_with_column_chunk_pruning` and `read_with_pruning` as fallbacks based on
the collection's storage mode).

---

#### C11. `except Exception: pass` on zone-map build/commit silently produces partially-prunable collections

**File:** `lenses/lakehouse/lakehouse_lens.py:438, 448, 551, 563, 658, 670, 1367, 1378`

```python
try:
    zm = ZoneMap.build(group_table)
    ...
except Exception:
    pass  # zone map is best-effort
```

If `ZoneMap.build` raises, the zone map for that row group is silently
skipped. The collection becomes partially-prunable. The user gets no log,
no metric, no return value indicating "zone maps were partially built."

Same problem at `:1751, 1797` (SQL pushdown) and `:852-861` (read_columns
fallback).

**Fix:** catch specific exceptions; log a warning; expose a `meta.build_warnings`
list.

---

### MAJOR (code smell that hurts maintainability)

#### M1. `pruning_reader.scan` is a 135-line method doing four jobs

Reset stats, build per-column chunk-predicate lookup, iterate row groups +
compute surviving chunks + decode + slice + filter, track stats. The
column-chunk intersection block alone is 36 lines of dense logic.

**Fix:** extract `_compute_surviving_chunks(zm_dict) -> Optional[set[int]]`
and `_decode_and_slice(data_blob_hash, surviving_chunks) -> list`.

---

#### M2. `pruned_row_groups` stat is always 0

`pruning_reader.scan` increments `total_row_groups` and `data_blobs_read` but
never increments `pruned_row_groups`. `get_stats()`'s docstring promises
"pruned_row_groups: row groups skipped."

**Fix:** either compute it in `scan_with_pruning` or remove the field.

---

#### M3. `end_key` parameter is "documentation only"

`pruning_reader.scan` and `zone_map_index.scan_with_pruning` both accept
`end_key` but neither implements filtering. Callers will set it expecting
filtering; nothing happens.

**Fix:** either implement `end_key` filtering or remove it from both signatures.

---

#### M4–M11. Dead code (8 methods)

- `pruning_reader.scan_column_chunks` (lines 259-291) — dead, duplicates `scan()`'s logic.
- `pruning_reader.get_pruning_ratio` (lines 305-319) — dead + has C2 bug.
- `pruning.might_match` (line 247-249) — dead.
- `column_chunk_zone_map.get_surviving_chunks` (lines 220-234) — dead.
- `zone_map_index.rebuild_zone_maps` (lines 287-301) — dead; the "rebuildable from snapshot" property is *claimed* but no production code path rebuilds.
- `base.py._ref_name` (lines 48-51) — dead, always returns wrong value (uses class literal, not `cls`).
- `column_chunk_storage._manifest_blob_hash_default` (lines 75-78) — dead.
- `encoded_chunk_storage.has_encoded_storage`'s second clause (lines 280-283) — dead.

**Fix:** delete all eight.

---

#### M12. `read_column_chunks_encoded` decodes the whole chunk then slices — defeats the FastLanes win

**File:** `pond-sdk/extensions/physical_structures/encoded_chunk_storage.py:259-267`

```python
all_values = decode_column(blob_bytes)              # ← decode EVERYTHING
surviving_values = []
for s, e in surviving_ranges:
    surviving_values.extend(all_values[s:e])        # ← then slice
```

The whole point of "encoded predicate eval" is to skip the decode. Here we
still decode the full chunk to slice it. The 2.04x speedup vs column-chunk
comes from chunk-blob pruning, not from this method's encoded-eval path.

**Fix:** for Dict and RLE, build the surviving values directly from the
encoded form without going through `decode_column`.

---

#### M13. `Statistics.can_prune` stores min/max as `str(...)` then `float(...)`s back

`Statistics.build` does `"min": str(min(non_null))`. Then `can_prune` does
`float(col_stats["min"])`. The round-trip drops type info and breaks for
non-numeric values. Worse: the `except` falls back to string comparison,
which silently produces wrong results for numeric strings (`"9" > "10"` is
`True` in string comparison).

**Fix:** store min/max as-is (JSON supports numbers), or use a tagged format.

---

#### M14. `zone_map_index.scan_with_pruning` reads every zone map blob — O(N), not O(K)

The class docstring claims "O(log N) lookup and O(K) range scans (K = matching
row groups)." The implementation calls `base.read_all()` (materializes entire
zone-map ProllyTreeIndex into a dict), then iterates all keys, then calls
`kernel.read_blob(zm_blob_hash)` for each entry.

For a collection with 10k row groups and a predicate that prunes 99%, you
still do 10k small reads.

**Fix:** either be honest in the docstring, or walk the ProllyTree
level-by-level using min/max stats at internal nodes (a real B-tree-style
prune — the Vortex innovation).

---

#### M15. `__init__.py` does not export the new infrastructure classes

README lists `ZoneMapIndex`, `PruningReader`, `ColumnChunkZoneMap`,
`ColumnChunkStorage`, `EncodedChunkStorage` as part of the package, but
`__init__.py` only exports `PhysicalStructure`, `BloomFilter`, `Statistics`,
`ZoneMap` (the legacy one). All real callers do `from pruning import ...` via
`sys.path` hacks.

**Fix:** add the new classes to `__init__.py` and switch all callers to
`from extensions.physical_structures import ...`.

---

#### M16. Lens reaches into private extension internals in 6+ places

**File:** `lenses/lakehouse/lakehouse_lens.py:513, 516, 622, 624, 1248, 1331`

The lens calls `zm_index._get_base(name)` (a private method) to enumerate and
delete zone-map entries directly. Same pattern with `ProllyLensBase`'s
`_compute_full_state`, `_staged_add`, `_staged_del`, `_commit_index` at
`:1458-1475` (and duplicated in `feature_store_lens.py:504-521`).

If `ZoneMapIndex` or `ProllyLensBase` change internals, every lens breaks.

**Fix:** add public `ZoneMapIndex.clear_zone_maps(collection)` and
`ProllyLensBase.create_merge_commit(parent, second_parent, message)`.

---

#### M17. `_is_tabular`, `_scan_rows`, `_get_row`, `_indexed_collection` are dead code in `LakehouseLens`

**File:** `lenses/lakehouse/lakehouse_lens.py:1559-1614`

The comment says these are "for CollectionIndexer compatibility" and that
`CollectionIndexer` "sets `self._indexed_collection` when it registers an
index." But `CollectionIndexer.build_index` takes `scan_rows` as a parameter —
it does **not** introspect the lens. The protocol described in the comment was
never implemented.

`_indexed_collection` is never set, so `_scan_rows` and `_get_row` always
early-return without yielding anything.

**Fix:** delete all four (~55 LOC), or implement the protocol in
`CollectionIndexer`.

---

#### M18. `range_point_lookup` is O(N), not O(log N) as documented

**File:** `lenses/lakehouse/lakehouse_lens.py:770-782`

The docstring promises "O(log N) lookup." The implementation calls
`base.read_all()` (walks the entire tree) and then does a linear scan.

**Fix:** add `ProllyLensBase.successor(key)` and call it.

---

#### M19. `_cached_tables` is an unbounded dict with no eviction

**File:** `lenses/lakehouse/lakehouse_lens.py:175, 289-296, 1372, 1476, 1529, 1972`

A long-running `PondLakehouse` that reads many commits (time-travel sweeps)
will accumulate every version of every table in memory.

**Fix:** use `functools.lru_cache` or a `maxsize`-bounded dict.

---

#### M20. `LakehouseLens.__init__` creates a DuckDB connection unconditionally

**File:** `lenses/lakehouse/lakehouse_lens.py:174`

```python
self.duckdb = duckdb.connect()
```

Combined with the hard-failing import at `:92-97`, **`LakehouseLens` cannot be
instantiated without DuckDB installed, even for write-only workloads**.

**Fix:** make `self.duckdb` a lazy property (as `FeatureStoreLens` already
does at `feature_store_lens.py:123-132`).

---

#### M21. `PondLakehouse` (SQL façade) lives in the same file as `LakehouseLens`

**File:** `lenses/lakehouse/lakehouse_lens.py:1660-1971`

The lens file is 2,246 lines because it contains both the lens and the SQL
façade. The lens should be importable without DuckDB.

**Fix:** split into `lakehouse_lens.py` (lens, PyArrow-only) and
`pond_lakehouse.py` (DuckDB façade).

---

#### M22. Hand-rolled regex SQL parser inside `PondLakehouse`

**File:** `lenses/lakehouse/lakehouse_lens.py:1802-1944`

~120 LOC of SQL text munging inside a file that should be a thin DuckDB
adapter. Explicitly disclaims support for joins, subqueries, OR,
BETWEEN-with-non-numeric, IN with floats.

**Fix:** move to a separate `sql_pushdown.py`, or use `sqlglot`.

---

#### M23. Magic number `chunk_size=1000` duplicated in 8 places

Lines 431, 459, 573, 879, 956, 1098, 1357, and the default-value positions
all use the literal `1000`. No `DEFAULT_CHUNK_SIZE` constant. If the optimal
chunk size changes, all 8 sites must be updated in lockstep — and a write/read
mismatch will silently corrupt pruning.

**Fix:** introduce `DEFAULT_CHUNK_SIZE = 1000` and store the chunk size used
at write time inside the zone-map blob so the read path can verify it matches.

---

#### M24. Magic type byte `3` for "binary commit" duplicated 3× without a named constant

**Files:** `lakehouse_lens.py:820, 1495`, `base_lens.py:216`

If `binary_encoding.py` ever changes the type byte, the lens silently falls
through to the JSON path.

**Fix:** export `BinaryProllyTree.COMMIT_TYPE_BYTE` from `binary_encoding.py`.

---

#### M25. `_self_test` and `_benchmark` live inside `lakehouse_lens.py`

**File:** `lenses/lakehouse/lakehouse_lens.py:1978-2246` (269 LOC)

`REPO_ORGANIZATION.md §2.6` says "Test files are NOT in `pond-sdk/` or
`lenses/` — those directories contain only production code." Same problem in
`feature_store_lens.py:579-738`.

**Fix:** move to `tests/integration/`.

---

#### M26. `lenses/vector/README.md` and docs claim `VectorLens` inherits from `KeyValueLens`

**Files:** `lenses/vector/README.md:31, 51-53`, `REPO_ORGANIZATION.md:43`,
`base_lens.py:9`

The code reads `class VectorLens(PondLens):` — direct inheritance. The README
is stale. Same for the claim that `KeyValueLens` lives in `pond-sdk/` — it
lives in `lenses/keyvalue/`.

**Fix:** update the docs.

---

#### M27. Empty results return `pa.table({})` losing schema

**Files:** `lakehouse_lens.py:731, 947`

`pa.table({})` is 0-row, 0-column. Callers cannot distinguish "table is
empty" from "table doesn't exist" from "predicate pruned everything."

**Fix:** preserve schema: `pa.table({col: [] for col in expected_columns})`.

---

### MINOR (style)

#### m1. Dead imports across 8 files

- `pruning.py:47` — `Callable, Union`
- `encoding.py:61-62` — `dataclass, field, Iterable, Union`
- `zone_map_index.py:54` — `dataclass, field`
- `column_chunk_zone_map.py:39` — `List, Dict`
- `column_chunk_storage.py:44` — `Iterator`
- `pruning_reader.py:55, 58` — `json`, `Iterator`, `Union`

**Fix:** delete unused imports.

---

#### m2. Magic numbers without constants

- `chunk_size = 1000` (M23 above)
- `encoding.py:104, 116, 127, 130, 134` — DICT threshold, BITPACK range, RLE heuristics
- `bloom_filter.py:55-56` — false positive rate, capacity

**Fix:** module-level constants at top of each file.

---

#### m3. `encoding.encode_bitpack` is misleadingly named

The worklog admits "current implementation stores offset values; real bitpack
uses raw bits." The function returns `packed = offset_vals` (a JSON list of
full-width ints), not packed bits. The "bitwidth" is computed but unused.

**Fix:** either rename to `SmallRangeMinMax` (honest) or actually bitpack.

---

#### m4. `Lens = KeyValueLens` aliases should emit `DeprecationWarning`

**File:** `lenses/keyvalue/keyvalue_lens.py:698-701`

**Fix:** wrap with `warnings.warn(..., DeprecationWarning)`.

---

#### m5. Commit-message formatting copy-pasted across three write paths

**Fix:** extract `_format_commit_message(prefix, n_rows, n_groups, **extras)`.

---

## Prioritized fix plan

The fixes are grouped into phases that can be done independently. Each phase
leaves the test suite green.

### Phase A — correctness fixes (1 day)

1. **C1** — `prune_column_chunks` returns `None` for missing stats; callers
   handle it.
2. **C2** — delete `get_pruning_ratio`.
3. **C11** — replace `except Exception: pass` with specific exceptions +
   warnings.
4. **M13** — `Statistics.can_prune` stores min/max as native JSON types.
5. **M27** — empty results preserve schema.

### Phase B — delete dead code (0.5 day)

6. **M4–M11** — delete 8 dead methods.
7. **M17** — delete `_is_tabular`, `_scan_rows`, `_get_row`,
   `_indexed_collection` (or implement the protocol — YAGNI says delete).
8. **m1** — delete unused imports.

### Phase C — make extensions truly format-agnostic (1 day)

9. **C4** — refactor `ZoneMap.build`, `ColumnChunkZoneMap.build`,
   `ColumnChunkStorage.write_row_group_*`, `EncodedChunkStorage.write_row_group_encoded`
   to take `column_source` callbacks instead of PyArrow tables.
10. **C5** — remove `LakehouseLens` references from extension error messages.
11. **C8** — eliminate the double-encode in `EncodedChunkStorage`.
12. **M12** — `read_column_chunks_encoded` decodes only surviving ranges
    directly from the encoded form (no `decode_column` roundtrip).

### Phase D — surface the fast paths to SQL users (0.5 day)

13. **C10** — `_read_with_pushdown` calls `read_with_encoded_pruning` with
    fallbacks based on storage mode.
14. **CRITICAL-4** — add tests for SQL queries against column-chunk and
    encoded storage modes.

### Phase E — extract shared write/read scaffolds (1 day)

15. **C9** — extract `_range_write_generic` and `_read_with_pruning_generic`
    in `LakehouseLens`. The three write methods and three read methods become
    ~10 lines each.
16. **M1** — split `pruning_reader.scan` into `_compute_surviving_chunks` +
    `_decode_and_slice`.
17. **M23, M24** — introduce `DEFAULT_CHUNK_SIZE` and
    `BinaryProllyTree.COMMIT_TYPE_BYTE` constants.

### Phase F — clean up imports (0.5 day)

18. **C6** — move `sys.path.insert` to module-level guarded imports; add
    `HAVE_PRUNING` flag.
19. **M15** — export new infrastructure classes from `__init__.py`.
20. **C7** — add `ProllyLensBase.head_ref()`; remove private-ref string
    from `zone_map_index.py`.

### Phase G — split `lakehouse_lens.py` (1 day)

21. **M21** — split into `lakehouse_lens.py` (lens, PyArrow-only) +
    `pond_lakehouse.py` (DuckDB façade).
22. **M22** — extract SQL parser to `sql_pushdown.py`.
23. **M25** — move `_self_test`/`_benchmark` to `tests/integration/`.
24. **M20** — make `self.duckdb` a lazy property.

### Phase H — public APIs for lens-internal coupling (0.5 day)

25. **M16** — add `ZoneMapIndex.clear_zone_maps(collection)` and
    `ProllyLensBase.create_merge_commit(...)`.
26. **M18** — add `ProllyLensBase.successor(key)`; fix `range_point_lookup`.
27. **M19** — bound `_cached_tables` with LRU.

### Phase I — docs (0.25 day)

28. **C3** — rename or delete the legacy `zone_map.py.ZoneMap`.
29. **M26** — update `lenses/vector/README.md`, `REPO_ORGANIZATION.md`,
    `base_lens.py` docstring.
30. **M14** — fix `zone_map_index.scan_with_pruning` docstring (or implement
    true O(K) pruning).

---

## Total estimated effort

- **Phase A+B:** 1.5 days — fixes correctness bugs and removes dead code.
- **Phase C+D:** 1.5 days — makes extensions honest about format-agnosticism
  and surfaces the fast paths to SQL users.
- **Phase E+F:** 1.5 days — pays down the duplication debt and cleans up
  imports.
- **Phase G+H+I:** 1.75 days — splits the lens file and adds public APIs.

**Total: ~6 days of refactoring** to bring the codebase back into alignment
with the seven principles.

The alternative is to keep adding features on top of the debt and pay
compound interest later.
