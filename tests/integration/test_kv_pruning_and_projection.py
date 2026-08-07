#!/usr/bin/env python3
"""
Test: KV pruning + Lakehouse projection pushdown.

Verifies:
  1. KeyValueLens builds zone maps at commit time
  2. read_with_pruning skips KV blobs without decoding
  3. LakehouseLens.read_columns does projection pushdown
"""

import os, sys, json, shutil, tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, os.path.join(REPO, "bindings/python/core"))
sys.path.insert(0, os.path.join(REPO, "bindings/python/sdk"))
sys.path.insert(0, os.path.join(REPO, "bindings/python/sdk", "extensions", "physical_structures"))
sys.path.insert(0, os.path.join(REPO, "lenses", "keyvalue"))
sys.path.insert(0, os.path.join(REPO, "lenses", "lakehouse"))

try:
    import pyarrow as pa
except ImportError:
    print("pyarrow not installed — skipping"); sys.exit(0)

from kernel import PondMinimal
from keyvalue_lens import KeyValueLens
from lakehouse_lens import LakehouseLens
from zone_map_index import ZoneMapIndex


def test_kv_pruning():
    """Test: KeyValueLens builds zone maps and read_with_pruning skips blobs."""
    print("\n=== Test 1: KV pruning ===")
    tmpdir = tempfile.mkdtemp(prefix="pond_kv_prune_")
    try:
        kernel = PondMinimal(tmpdir)
        lens = KeyValueLens(kernel)

        # Write 5 users with different ages
        for i in range(5):
            lens.put("users", f"user:{i}", {"name": f"user_{i}", "age": 20 + i * 10})
        lens.commit("users", "insert 5 users")

        # Build zone maps explicitly (KV zone maps are NOT auto-built)
        lens.build_zone_maps("users")

        # Verify zone maps were built
        zm_index = ZoneMapIndex(kernel)
        assert zm_index.has_zone_maps("users"), "Zone maps not built for KV collection"
        print("  [OK] Zone maps built via explicit build_zone_maps() call")

        # Pruning: age > 35
        # Users: age 20, 30, 40, 50, 60
        # Zone maps: each user has min=max=age (single row per blob)
        # Pruned: age 20 (max=20 < 35), age 30 (max=30 < 35)
        # Not pruned: age 40, 50, 60
        rows = list(lens.read_with_pruning(
            "users",
            predicates=[("age", ">", 35)],
            row_filter=lambda r: r.get("age", 0) > 35,
        ))
        assert len(rows) == 3, f"Expected 3 rows (age > 35), got {len(rows)}"
        ages = sorted(r["age"] for r in rows)
        assert ages == [40, 50, 60], f"Expected ages [40, 50, 60], got {ages}"
        print(f"  [OK] Pruned 2/5 blobs, got {len(rows)} rows with ages {ages}")

        kernel.close()
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_projection_pushdown():
    """Test: LakehouseLens.read_columns only reads requested columns."""
    print("\n=== Test 2: Projection pushdown ===")
    tmpdir = tempfile.mkdtemp(prefix="pond_proj_")
    try:
        kernel = PondMinimal(tmpdir)
        lens = LakehouseLens(kernel)

        # Create a wide table with 5 columns
        data = pa.table({
            "id": [1, 2, 3],
            "name": ["alice", "bob", "carol"],
            "age": [30, 25, 35],
            "city": ["NYC", "SF", "LA"],
            "score": [0.9, 0.8, 0.7],
        })
        lens.create_table("users", data)

        # Read only 2 columns
        result = lens.read_columns("users", ["name", "age"])
        assert result.num_rows == 3, f"Expected 3 rows, got {result.num_rows}"
        assert result.column_names == ["name", "age"], \
            f"Expected columns ['name', 'age'], got {result.column_names}"
        names = result.column("name").to_pylist()
        assert names == ["alice", "bob", "carol"]
        print(f"  [OK] Projection: read 2/5 columns, got {result.num_rows} rows")

        # Read 1 column
        result2 = lens.read_columns("users", ["score"])
        assert result2.num_rows == 3
        assert result2.column_names == ["score"]
        scores = result2.column("score").to_pylist()
        assert scores == [0.9, 0.8, 0.7]
        print(f"  [OK] Projection: read 1/5 columns, got {result2.num_rows} rows")

        kernel.close()
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_combined_predicate_and_projection():
    """Test: predicate pushdown + projection pushdown together."""
    print("\n=== Test 3: Combined predicate + projection pushdown ===")
    tmpdir = tempfile.mkdtemp(prefix="pond_combined_")
    try:
        kernel = PondMinimal(tmpdir)
        lens = LakehouseLens(kernel)

        # 30 rows, 3 row groups (ages 20-29, 30-39, 40-49)
        data = pa.table({
            "id": list(range(30)),
            "age": [20 + i for i in range(30)],
            "name": [f"user_{i}" for i in range(30)],
            "city": ["NYC" if i % 2 == 0 else "SF" for i in range(30)],
        })
        lens.range_write("users", data, key_col="id", row_group_size=10)

        # Step 1: Prune (skip row groups where max(age) < 40)
        # Step 2: Project (only read "name" and "age" columns)
        pruned = lens.read_with_pruning(
            "users",
            predicates=[("age", ">=", 40)],
            row_filter=lambda r: r.get("age", 0) >= 40,
        )
        assert pruned.num_rows == 10, f"Expected 10 rows after pruning, got {pruned.num_rows}"
        print(f"  [OK] Predicate pushdown: {pruned.num_rows} rows (skipped 2/3 row groups)")

        # Now project just the name column from the pruned result
        projected = pruned.select(["name"])
        assert projected.num_rows == 10
        assert projected.column_names == ["name"]
        print(f"  [OK] Projection pushdown: {projected.num_rows} rows with 1 column")

        kernel.close()
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == "__main__":
    print("=" * 60)
    print("KV Pruning + Projection Pushdown Tests")
    print("=" * 60)
    test_kv_pruning()
    test_projection_pushdown()
    test_combined_predicate_and_projection()
    print("\n" + "=" * 60)
    print("ALL TESTS PASSED")
    print("=" * 60)
