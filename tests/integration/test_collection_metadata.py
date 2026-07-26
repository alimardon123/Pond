#!/usr/bin/env python3
"""
Test: Collection integration — unified data-side metadata management.

Verifies the unified Collection class (namespace + labels + metadata):
  1. Create collection with labels and namespace
  2. Build zone maps via Collection
  3. Read with pruning via Collection
  4. Build index via Collection
  5. Lookup via Collection
  6. Compact zone maps via Collection
  7. Collection persists across restart
"""

import os, sys, json, shutil, tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, os.path.join(REPO, "pond-core"))
sys.path.insert(0, os.path.join(REPO, "pond-sdk"))
sys.path.insert(0, os.path.join(REPO, "pond-sdk", "extensions", "physical_structures"))
sys.path.insert(0, os.path.join(REPO, "lenses", "keyvalue"))

from kernel import PondMinimal
from collection import Collection
from keyvalue_lens import KeyValueLens


def test_collection_with_metadata():
    """Test: Collection unifies namespace + labels + metadata (zone maps, indexes)."""
    print("=" * 60)
    print("Collection Integration Test")
    print("=" * 60)

    tmpdir = tempfile.mkdtemp(prefix="pond_coll_meta_")
    try:
        kernel = PondMinimal(tmpdir)
        lens = KeyValueLens(kernel)

        # 1. Create collection with labels
        vol = Collection.create(kernel, "analytics/users",
                                labels=["sql", "production"],
                                created_by="KeyValueLens",
                                description="User table")
        print("\n  [OK] Created collection 'analytics/users' with labels")
        assert vol.labels == ["sql", "production"]
        assert vol.namespace == "analytics"
        assert vol.basename == "users"

        # 2. Write data via lens
        for i in range(10):
            lens.put("analytics/users", f"u{i}", {"name": f"user_{i}", "age": 20 + i * 5})
        lens.commit("analytics/users", "insert 10 users")
        print("  [OK] Wrote 10 users via KeyValueLens")

        # 3. Build zone maps via Collection
        def scan_fn():
            for key in lens.keys("analytics/users"):
                raw = lens.get_raw("analytics/users", key)
                if raw:
                    yield (f"kv/{key}", raw, 1)

        vol.build_zone_maps(scan_fn)
        assert vol.has_zone_maps(), "Zone maps not built"
        print("  [OK] Built zone maps via Collection.build_zone_maps()")

        # 4. Read with pruning via Collection
        rows = list(vol.read_with_pruning(
            predicates=[("age", ">=", 45)],
            decode_fn=lambda b: json.loads(b),
            row_filter=lambda r: r.get("age", 0) >= 45,
        ))
        assert len(rows) == 5, f"Expected 5 rows (age >= 45), got {len(rows)}"
        ages = sorted(r["age"] for r in rows)
        assert ages == [45, 50, 55, 60, 65], f"Expected ages [45,50,55,60,65], got {ages}"
        print(f"  [OK] Pruning via Collection: {len(rows)} rows with age >= 45 (ages {ages})")

        # 5. Build index via Collection
        vol.build_index("by_name",
                        extractor=lambda r: r.get("name", ""),
                        scan_fn=lambda: ((k, lens.get("analytics/users", k))
                                         for k in lens.keys("analytics/users")))
        assert "by_name" in vol.list_indexes()
        print("  [OK] Built index 'by_name' via Collection.build_index()")

        # 6. Lookup via Collection
        rowid = vol.lookup_index("by_name", "user_5")
        assert rowid is not None, "Index lookup returned None"
        row = lens.get("analytics/users", rowid)
        assert row["name"] == "user_5"
        assert row["age"] == 45
        print(f"  [OK] Index lookup via Collection: user_5 → age {row['age']}")

        # 7. Drop index via Collection
        assert vol.drop_index("by_name") is True
        assert "by_name" not in vol.list_indexes()
        print("  [OK] Dropped index 'by_name' via Collection.drop_index()")

        # 8. Compact zone maps via Collection
        removed = vol.compact_zone_maps()
        print(f"  [OK] Compacted zone maps: {removed} stale entries removed")

        kernel.close()

        # 9. Collection persists across restart
        kernel2 = PondMinimal(tmpdir)
        collections = Collection.list(kernel2)
        assert len(collections) >= 1
        names = [c["name"] for c in collections]
        assert "analytics/users" in names
        vol2 = Collection(kernel2, "analytics/users")
        assert vol2.labels == ["sql", "production"]
        assert vol2.description == "User table"
        print("  [OK] Collection metadata persists across restart")
        kernel2.close()

        print("\nALL COLLECTION INTEGRATION TESTS PASSED")

    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == "__main__":
    test_collection_with_metadata()
