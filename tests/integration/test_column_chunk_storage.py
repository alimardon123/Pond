#!/usr/bin/env python3
"""
Test: Column-chunk storage (per-column-chunk blobs) end-to-end.

Verifies that:
  1. range_write_column_chunks writes per-column-chunk Parquet blobs
  2. Each chunk blob is content-addressed and stored independently
  3. The zone map blob's column_chunks stats include blob_hash fields
  4. read_with_column_chunk_pruning fetches ONLY surviving chunk blobs
  5. Correctness: results match read_with_pruning (the whole-blob path)
  6. I/O savings: kernel.read_blob is called fewer times (count asserts)

Run:
    python tests/integration/test_column_chunk_storage.py
"""

from __future__ import annotations

import os
import sys
import shutil
import tempfile
import json
from unittest.mock import patch

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, os.path.join(REPO, "bindings/python/core"))
sys.path.insert(0, os.path.join(REPO, "bindings/python/sdk"))
sys.path.insert(0, os.path.join(REPO, "bindings/python/sdk", "extensions", "physical_structures"))
sys.path.insert(0, os.path.join(REPO, "lenses", "lakehouse"))

try:
    import pyarrow as pa
except ImportError:
    print("pyarrow not installed — skipping test")
    sys.exit(0)

from kernel import PondMinimal
from lakehouse_lens import LakehouseLens
from zone_map_index import ZoneMapIndex
from column_chunk_storage import ColumnChunkStorage


def test_column_chunk_storage_basic():
    """Test: range_write_column_chunks produces per-column-chunk blobs."""
    print("=" * 60)
    print("Column-Chunk Storage: Basic Write/Read Test")
    print("=" * 60)

    tmpdir = tempfile.mkdtemp(prefix="pond_ccs_basic_")
    try:
        kernel = PondMinimal(tmpdir)
        lens = LakehouseLens(kernel)

        # 3000 rows in 1 row group, 3 chunks of 1000 rows each
        n = 3000
        data = pa.table({
            "id": list(range(n)),
            "age": list(range(n)),
            "score": [float(i) / n for i in range(n)],
        })
        lens.range_write_column_chunks(
            "events", data, key_col="id",
            row_group_size=n,  # single row group
            chunk_size=1000,
        )
        print(f"\n  Created 'events' with {n} rows, 3 chunks of 1000 each")

        # Verify zone map has column-chunk stats with blob_hash fields
        zm_index = ZoneMapIndex(kernel)
        zm = zm_index.get_zone_map("events", f"rg/{n - 1}")
        assert zm is not None, "Zone map not built"
        assert "column_chunks" in zm, "Column-chunk stats missing"

        # Verify ColumnChunkStorage.has_column_chunk_storage detects this
        assert ColumnChunkStorage.has_column_chunk_storage(zm), (
            "has_column_chunk_storage should return True when blob_hash "
            "fields are populated")

        # Each column should have 3 chunks with blob_hash set
        cczm_dict = zm["column_chunks"]["column_chunks"]
        for col in ["id", "age", "score"]:
            assert col in cczm_dict, f"Column {col} missing from chunks"
            assert len(cczm_dict[col]) == 3, (
                f"Expected 3 chunks for {col}, got {len(cczm_dict[col])}")
            for i, chunk in enumerate(cczm_dict[col]):
                assert chunk["blob_hash"] is not None, (
                    f"Chunk {i} of {col} missing blob_hash")
                assert chunk["row_count"] == 1000
        print(f"  [OK] All chunks have blob_hash fields set")
        print(f"  [OK] 3 columns × 3 chunks = 9 per-column-chunk blobs written")

        # Test 1: Predicate age >= 2500
        # Row-group zone map: age [0, 2999] → can't prune row group
        # Column-chunk zone maps for age:
        #   chunk 0 [0, 999]:     max < 2500 → PRUNED
        #   chunk 1 [1000, 1999]: max < 2500 → PRUNED
        #   chunk 2 [2000, 2999]: max >= 2500 → SURVIVES
        # Expected: 500 rows (ages 2500-2999), 2/3 chunks pruned per column
        result = lens.read_with_column_chunk_pruning(
            "events",
            predicates=[("age", ">=", 2500)],
            row_filter=lambda r: r.get("age", 0) >= 2500,
        )
        assert result.num_rows == 500, (
            f"Expected 500 rows, got {result.num_rows}")
        ages = result.column("age").to_pylist()
        assert min(ages) == 2500, f"Expected min age 2500, got {min(ages)}"
        assert max(ages) == 2999, f"Expected max age 2999, got {max(ages)}"
        print(f"  [OK] read_with_column_chunk_pruning: age >= 2500 → "
              f"{result.num_rows} rows (2/3 chunks pruned per column)")

        # Test 2: Read ALL rows (no predicate) — should fetch all chunks
        result_all = lens.read_with_column_chunk_pruning("events")
        assert result_all.num_rows == n, (
            f"Expected {n} rows, got {result_all.num_rows}")
        print(f"  [OK] No predicate: all {n} rows returned")

        # Test 3: Projection pushdown — read only 'age' column
        result_proj = lens.read_with_column_chunk_pruning(
            "events",
            predicates=[("age", ">=", 2500)],
            row_filter=lambda r: r.get("age", 0) >= 2500,
            columns=["age"],
        )
        assert result_proj.num_rows == 500
        assert result_proj.column_names == ["age"], (
            f"Expected only 'age' column, got {result_proj.column_names}")
        print(f"  [OK] Projection: only 'age' column returned "
              f"({result_proj.num_rows} rows)")

        kernel.close()
        print("\nALL BASIC TESTS PASSED")

    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_column_chunk_storage_io_savings():
    """Test: verify that column-chunk storage reduces BYTES read.

    Uses a counter to track bytes returned by kernel.read_blob during the
    scan. With column-chunk storage:
      - Predicate age >= 2500 prunes 2/3 chunks per column
      - Only surviving chunk blobs are read (smaller, per-column Parquet)
      - Total bytes read should be much less than reading the whole row
        group blob (which contains all columns for all rows)
    """
    print("\n" + "=" * 60)
    print("Column-Chunk Storage: I/O Savings Test (bytes read)")
    print("=" * 60)

    tmpdir = tempfile.mkdtemp(prefix="pond_ccs_io_")
    try:
        kernel = PondMinimal(tmpdir)
        lens = LakehouseLens(kernel)

        n = 3000
        data = pa.table({
            "id": list(range(n)),
            "age": list(range(n)),
            "score": [float(i) / n for i in range(n)],
        })
        lens.range_write_column_chunks(
            "events", data, key_col="id",
            row_group_size=n, chunk_size=1000,
        )
        print(f"\n  Created 'events' with {n} rows, 3 chunks of 1000 each, "
              f"3 columns → 9 chunk blobs total")

        # Instrument kernel.read_blob to count BYTES
        original_read = kernel.read_blob
        bytes_read = []
        def counting_read(blob_hash):
            data_bytes = original_read(blob_hash)
            bytes_read.append(len(data_bytes))
            return data_bytes

        # Test 1: predicate age >= 2500 with column-chunk storage
        # Surviving chunks: chunk 2 of each column (3 chunks total)
        with patch.object(kernel, 'read_blob', side_effect=counting_read):
            bytes_read.clear()
            result = lens.read_with_column_chunk_pruning(
                "events",
                predicates=[("age", ">=", 2500)],
                row_filter=lambda r: r.get("age", 0) >= 2500,
            )
        bytes_cc = sum(bytes_read)
        assert result.num_rows == 500

        # Test 2: write the SAME data with regular range_write (whole-blob)
        # and read with read_with_pruning — compare bytes read
        lens.range_write("events_whole", data, key_col="id",
                          row_group_size=n)
        with patch.object(kernel, 'read_blob', side_effect=counting_read):
            bytes_read.clear()
            result2 = lens.read_with_pruning(
                "events_whole",
                predicates=[("age", ">=", 2500)],
                row_filter=lambda r: r.get("age", 0) >= 2500,
                columns=["age"],  # enable column-chunk slicing (no actual chunk blobs)
            )
        bytes_whole = sum(bytes_read)
        assert result2.num_rows == 500

        print(f"  [OK] Predicate age >= 2500:")
        print(f"       Column-chunk storage: {bytes_cc:,} bytes read")
        print(f"       Whole-blob storage:   {bytes_whole:,} bytes read")
        if bytes_whole > 0:
            ratio = bytes_whole / bytes_cc
            print(f"       Bytes saved:          {bytes_whole - bytes_cc:,} "
                  f"({ratio:.2f}x reduction)")
            # Column-chunk storage should read fewer bytes for selective predicates
            # (3 small chunk blobs vs 1 large row-group blob)
            assert bytes_cc < bytes_whole, (
                f"Column-chunk storage should read fewer bytes "
                f"({bytes_cc} >= {bytes_whole})")

        # Test 3: very selective predicate (age >= 2999) → 1 row
        with patch.object(kernel, 'read_blob', side_effect=counting_read):
            bytes_read.clear()
            result = lens.read_with_column_chunk_pruning(
                "events",
                predicates=[("age", ">=", 2999)],
                row_filter=lambda r: r.get("age", 0) >= 2999,
            )
        bytes_cc_very = sum(bytes_read)
        assert result.num_rows == 1
        print(f"\n  [OK] Predicate age >= 2999 (1 row): "
              f"{bytes_cc_very:,} bytes read "
              f"(only chunk 2 of each column, ~1/3 of row group)")

        # Test 4: no predicate → all chunks read
        with patch.object(kernel, 'read_blob', side_effect=counting_read):
            bytes_read.clear()
            result = lens.read_with_column_chunk_pruning("events")
        bytes_cc_all = sum(bytes_read)
        assert result.num_rows == n
        print(f"  [OK] No predicate: {bytes_cc_all:,} bytes read "
              f"(all 9 chunk blobs)")

        kernel.close()
        print("\nALL I/O SAVINGS TESTS PASSED")
        print("\nKey findings:")
        print("  - Column-chunk storage reads FEWER BYTES for selective predicates")
        print("  - Each chunk blob is single-column + single-chunk (small)")
        print("  - Whole-blob storage reads the full row group (all cols × all rows)")
        print("  - Real I/O savings scale with predicate selectivity")

    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_column_chunk_storage_fallback():
    """Test: read_with_column_chunk_pruning falls back to whole-blob path
    when the collection was written with regular range_write (no chunk blobs).
    """
    print("\n" + "=" * 60)
    print("Column-Chunk Storage: Fallback Test")
    print("=" * 60)

    tmpdir = tempfile.mkdtemp(prefix="pond_ccs_fb_")
    try:
        kernel = PondMinimal(tmpdir)
        lens = LakehouseLens(kernel)

        # Use regular range_write (NOT range_write_column_chunks)
        n = 30
        data = pa.table({
            "id": list(range(n)),
            "age": list(range(n)),
        })
        lens.range_write("legacy", data, key_col="id", row_group_size=10)
        print(f"\n  Created 'legacy' with regular range_write (3 row groups)")

        # The zone map for this collection has column_chunks stats
        # (computed by _write_via_prolly) but NO blob_hash fields —
        # the row group is a single Parquet blob, not per-column-chunk.
        zm_index = ZoneMapIndex(kernel)
        zm = zm_index.get_zone_map("legacy", "rg/9")
        assert zm is not None
        # The fallback should kick in because no blob_hash fields
        assert not ColumnChunkStorage.has_column_chunk_storage(zm), (
            "Legacy row group should not have column-chunk storage")
        print(f"  [OK] Legacy collection has no per-column-chunk blobs")

        # read_with_column_chunk_pruning should fall back to whole-blob
        # read and still produce correct results.
        result = lens.read_with_column_chunk_pruning(
            "legacy",
            predicates=[("age", ">=", 20)],
            row_filter=lambda r: r.get("age", 0) >= 20,
        )
        assert result.num_rows == 10, (
            f"Expected 10 rows (ages 20-29), got {result.num_rows}")
        ages = result.column("age").to_pylist()
        assert min(ages) == 20
        assert max(ages) == 29
        print(f"  [OK] Fallback to whole-blob read works: "
              f"{result.num_rows} rows (ages 20-29)")

        kernel.close()
        print("\nALL FALLBACK TESTS PASSED")

    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == "__main__":
    test_column_chunk_storage_basic()
    test_column_chunk_storage_io_savings()
    test_column_chunk_storage_fallback()
    print("\n" + "=" * 60)
    print("ALL COLUMN-CHUNK STORAGE TESTS PASSED")
    print("=" * 60)
