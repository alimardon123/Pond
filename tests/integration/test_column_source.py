#!/usr/bin/env python3
"""
Test: ColumnSource — format-agnostic pruning (C4 fix verification).

Verifies that the pruning infrastructure (ZoneMap, ColumnChunkZoneMap)
works with NON-PyArrow data sources. This is the design review C4 fix:
extensions claimed to be "format-agnostic" but hard-coded PyArrow. Now
they accept any ColumnSource implementation.

Tests:
  1. ListColumnSource (list of dicts) produces correct zone maps
  2. ListColumnSource produces correct column-chunk zone maps
  3. Results match what PyArrowColumnSource would produce for the same data
  4. as_column_source auto-wraps PyArrow tables (backward compat)
  5. as_column_source passes through existing ColumnSource instances
  6. compute_list_stats handles nulls, empty lists, mixed types

Run:
    python tests/integration/test_column_source.py
"""

from __future__ import annotations

import os
import sys
import tempfile
import shutil

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, os.path.join(REPO, "bindings/python/core"))
sys.path.insert(0, os.path.join(REPO, "bindings/python/sdk"))
sys.path.insert(0, os.path.join(REPO, "bindings/python/sdk", "extensions", "physical_structures"))

from column_source import (
    ColumnSource, PyArrowColumnSource, ListColumnSource,
    as_column_source, compute_list_stats,
)
from pruning import ZoneMap
from column_chunk_zone_map import ColumnChunkZoneMap

try:
    import pyarrow as pa
    HAVE_PYARROW = True
except ImportError:
    HAVE_PYARROW = False


def test_list_column_source_basic():
    """ListColumnSource produces correct zone maps (no PyArrow needed)."""
    print("=" * 60)
    print("ListColumnSource: basic zone map build (no PyArrow)")
    print("=" * 60)

    rows = [
        {"id": 1, "name": "alice", "age": 30},
        {"id": 2, "name": "bob", "age": 25},
        {"id": 3, "name": "carol", "age": 35},
        {"id": 4, "name": "dave", "age": None},  # null age
    ]
    source = ListColumnSource(rows)

    assert source.column_names() == ["id", "name", "age"]
    assert source.num_rows() == 4
    print(f"  [OK] column_names={source.column_names()}, num_rows={source.num_rows()}")

    zm = ZoneMap.build(source)
    assert zm.row_count == 4
    assert zm.min["id"] == 1
    assert zm.max["id"] == 4
    assert zm.min["age"] == 25
    assert zm.max["age"] == 35
    assert zm.null_count["age"] == 1
    assert zm.null_count["id"] == 0
    print(f"  [OK] ZoneMap: id=[{zm.min['id']},{zm.max['id']}], "
          f"age=[{zm.min['age']},{zm.max['age']}], "
          f"age nulls={zm.null_count['age']}")


def test_list_column_source_chunked():
    """ListColumnSource produces correct column-chunk zone maps."""
    print("\n" + "=" * 60)
    print("ListColumnSource: column-chunk zone map build")
    print("=" * 60)

    # 2500 rows, chunk_size=1000 → 3 chunks (1000, 1000, 500)
    rows = [{"id": i, "age": i % 100} for i in range(2500)]
    source = ListColumnSource(rows)

    cczm = ColumnChunkZoneMap.build(source, "rg/2499", chunk_size=1000)

    assert "id" in cczm.column_chunks
    assert "age" in cczm.column_chunks
    assert len(cczm.column_chunks["id"]) == 3
    assert len(cczm.column_chunks["age"]) == 3

    # Chunk 0: ids 0-999, ages 0-99
    chunk0 = cczm.column_chunks["id"][0]
    assert chunk0.min == 0
    assert chunk0.max == 999
    assert chunk0.row_count == 1000
    assert chunk0.null_count == 0

    # Chunk 2: ids 2000-2499 (only 500 rows)
    chunk2 = cczm.column_chunks["id"][2]
    assert chunk2.min == 2000
    assert chunk2.max == 2499
    assert chunk2.row_count == 500

    print(f"  [OK] 3 chunks: id chunk0=[{chunk0.min},{chunk0.max}], "
          f"chunk2=[{chunk2.min},{chunk2.max}] (row_count={chunk2.row_count})")


def test_matches_pyarrow():
    """ListColumnSource and PyArrowColumnSource produce the same zone maps."""
    if not HAVE_PYARROW:
        print("\n[SKIP] PyArrow not installed — skipping match test")
        return

    print("\n" + "=" * 60)
    print("ListColumnSource vs PyArrowColumnSource: same results")
    print("=" * 60)

    rows = [
        {"id": 1, "name": "alice", "age": 30},
        {"id": 2, "name": "bob", "age": 25},
        {"id": 3, "name": "carol", "age": 35},
        {"id": 4, "name": "dave", "age": None},
    ]
    table = pa.table({
        "id": [1, 2, 3, 4],
        "name": ["alice", "bob", "carol", "dave"],
        "age": [30, 25, 35, None],
    })

    zm_list = ZoneMap.build(ListColumnSource(rows))
    zm_pa = ZoneMap.build(table)

    assert zm_list.row_count == zm_pa.row_count
    assert zm_list.min["id"] == zm_pa.min["id"]
    assert zm_list.max["id"] == zm_pa.max["id"]
    assert zm_list.min["age"] == zm_pa.min["age"]
    assert zm_list.max["age"] == zm_pa.max["age"]
    assert zm_list.null_count["age"] == zm_pa.null_count["age"]
    print(f"  [OK] Both sources produce identical zone maps")


def test_as_column_source_auto_wrap():
    """as_column_source auto-wraps PyArrow tables (backward compat)."""
    print("\n" + "=" * 60)
    print("as_column_source: auto-wrap PyArrow (backward compat)")
    print("=" * 60)

    # A ColumnSource passes through
    list_source = ListColumnSource([{"a": 1}])
    assert as_column_source(list_source) is list_source
    print(f"  [OK] ColumnSource passes through unchanged")

    # A non-ColumnSource raises TypeError
    try:
        as_column_source("not a table")
        assert False, "Should have raised TypeError"
    except TypeError:
        print(f"  [OK] Non-table, non-ColumnSource raises TypeError")

    if HAVE_PYARROW:
        # A PyArrow Table is auto-wrapped
        table = pa.table({"a": [1, 2, 3]})
        source = as_column_source(table)
        assert isinstance(source, PyArrowColumnSource)
        assert source.num_rows() == 3
        assert source.column_names() == ["a"]
        print(f"  [OK] PyArrow Table auto-wrapped in PyArrowColumnSource")


def test_compute_list_stats():
    """compute_list_stats handles nulls, empty lists, mixed types."""
    print("\n" + "=" * 60)
    print("compute_list_stats: edge cases")
    print("=" * 60)

    # Normal
    mn, mx, nc = compute_list_stats([3, 1, 2, None, 5])
    assert mn == 1 and mx == 5 and nc == 1
    print(f"  [OK] [3,1,2,None,5] → min={mn}, max={mx}, nulls={nc}")

    # All null
    mn, mx, nc = compute_list_stats([None, None])
    assert mn is None and mx is None and nc == 2
    print(f"  [OK] [None,None] → min=None, max=None, nulls={nc}")

    # Empty
    mn, mx, nc = compute_list_stats([])
    assert mn is None and mx is None and nc == 0
    print(f"  [OK] [] → min=None, max=None, nulls={nc}")

    # Strings
    mn, mx, nc = compute_list_stats(["c", "a", "b"])
    assert mn == "a" and mx == "c" and nc == 0
    print(f"  [OK] ['c','a','b'] → min='{mn}', max='{mx}', nulls={nc}")

    # Mixed types (unorderable) → can't compute min/max
    mn, mx, nc = compute_list_stats([1, "a", 2])
    assert mn is None and mx is None and nc == 0
    print(f"  [OK] [1,'a',2] → min=None (mixed types unorderable), nulls={nc}")


def test_format_agnostic_end_to_end():
    """End-to-end: build zone maps from list-of-dicts, store in kernel,
    read back, and verify pruning works."""
    print("\n" + "=" * 60)
    print("End-to-end: list-of-dicts → zone maps → pruning")
    print("=" * 60)

    from kernel import PondMinimal
    from zone_map_index import ZoneMapIndex
    from pruning import PruningPredicate, ColumnPredicate
    from pruning_reader import PruningReader

    tmpdir = tempfile.mkdtemp(prefix="pond_csource_e2e_")
    try:
        kernel = PondMinimal(tmpdir)
        zm_index = ZoneMapIndex(kernel)

        # Simulate a KeyValueLens-style collection: 3 "row groups" of
        # list-of-dicts data. Each row group has ages in a different range.
        row_groups = [
            ([{"id": i, "age": 20 + i} for i in range(10)], "rg/9"),
            ([{"id": 10 + i, "age": 30 + i} for i in range(10)], "rg/19"),
            ([{"id": 20 + i, "age": 40 + i} for i in range(10)], "rg/29"),
        ]

        # Build zone maps from list-of-dicts (NO PyArrow)
        for rows, rg_key in row_groups:
            source = ListColumnSource(rows)
            zm = ZoneMap.build(source)
            # Compute a fake data blob hash (just write the rows as JSON)
            import json
            data_blob_hash = kernel.write(json.dumps(rows).encode())
            zm_index.add_zone_map("events", rg_key, zm, data_blob_hash)
        zm_index.commit_zone_maps("events")
        print(f"  [OK] Built 3 zone maps from list-of-dicts (no PyArrow)")

        # Pruning predicate: age >= 40
        # Row group 0 (ages 20-29): max=29 < 40 → PRUNED
        # Row group 1 (ages 30-39): max=39 < 40 → PRUNED
        # Row group 2 (ages 40-49): max=49 >= 40 → SURVIVES
        predicate = PruningPredicate([
            ColumnPredicate(column="age", op=">=", value=40)
        ])
        reader = PruningReader(kernel, zm_index, "events", predicate)

        blobs_read = list(reader.scan_blob_hashes())
        assert len(blobs_read) == 1, f"Expected 1 surviving blob, got {len(blobs_read)}"
        print(f"  [OK] Predicate age >= 40: pruned 2/3 row groups, read 1 blob")

        kernel.close()
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == "__main__":
    test_list_column_source_basic()
    test_list_column_source_chunked()
    test_matches_pyarrow()
    test_as_column_source_auto_wrap()
    test_compute_list_stats()
    test_format_agnostic_end_to_end()
    print("\n" + "=" * 60)
    print("ALL COLUMN_SOURCE TESTS PASSED")
    print("=" * 60)
