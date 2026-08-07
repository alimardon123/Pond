#!/usr/bin/env python3
"""
Test: EAGER/LAZY index modes + incremental refresh via commit-diff.

Verifies:
  1. EAGER mode: index auto-refreshes on commit (via notify_write)
  2. LAZY mode: index refreshes on lookup when stale
  3. refresh_index_incremental: O(changed) via ProllyTree commit-diff
  4. is_index_stale: detects stale indexes
"""

import os, sys, json, shutil, tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, os.path.join(REPO, "bindings/python/core"))
sys.path.insert(0, os.path.join(REPO, "bindings/python/sdk"))
sys.path.insert(0, os.path.join(REPO, "bindings/python/sdk", "extensions", "physical_structures"))
sys.path.insert(0, os.path.join(REPO, "bindings/python/sdk", "extensions", "indexing"))
sys.path.insert(0, os.path.join(REPO, "lenses", "keyvalue"))

from kernel import PondMinimal
from keyvalue_lens import KeyValueLens
from collection_metadata import CollectionMetadata


def test_eager_mode():
    """EAGER: index auto-refreshes on every commit."""
    print("\n=== Test 1: EAGER mode (auto-refresh on commit) ===")
    tmpdir = tempfile.mkdtemp(prefix="pond_eager_")
    try:
        kernel = PondMinimal(tmpdir)
        lens = KeyValueLens(kernel)
        meta = CollectionMetadata(kernel)

        # Register EAGER index and attach to lens
        meta.register_eager_index("users", "by_name",
            extractor=lambda r: r.get("name", ""),
            scan_fn=lambda: ((k, lens.get("users", k)) for k in lens.keys("users")))
        lens.attach_indexer(meta)

        # Write data — commit auto-notifies indexer
        lens.put("users", "u1", {"name": "alice", "age": 30})
        lens.commit("users", "insert alice")

        # Lookup should find alice (index auto-refreshed by EAGER mode)
        rowid = meta.lookup_index("users", "by_name", "alice")
        assert rowid is not None, "EAGER: lookup returned None after commit"
        row = lens.get("users", rowid)
        assert row["name"] == "alice"
        print("  [OK] EAGER: index auto-refreshed on commit, lookup works")

        # Add more data
        lens.put("users", "u2", {"name": "bob", "age": 25})
        lens.commit("users", "insert bob")

        rowid2 = meta.lookup_index("users", "by_name", "bob")
        assert rowid2 is not None, "EAGER: lookup returned None for bob"
        print("  [OK] EAGER: second commit auto-refreshed, bob found")

        kernel.close()
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_lazy_mode():
    """LAZY: index refreshes on lookup when stale."""
    print("\n=== Test 2: LAZY mode (refresh on lookup when stale) ===")
    tmpdir = tempfile.mkdtemp(prefix="pond_lazy_")
    try:
        kernel = PondMinimal(tmpdir)
        lens = KeyValueLens(kernel)
        meta = CollectionMetadata(kernel)

        # Write initial data
        lens.put("users", "u1", {"name": "alice", "age": 30})
        lens.commit("users", "insert alice")

        # Build index initially
        meta.build_index("users", "by_name",
            extractor=lambda r: r.get("name", ""),
            scan_fn=lambda: ((k, lens.get("users", k)) for k in lens.keys("users")))

        # Register LAZY index with staleness_budget=2 and attach to lens
        meta.register_lazy_index("users", "by_name",
            extractor=lambda r: r.get("name", ""),
            scan_fn=lambda: ((k, lens.get("users", k)) for k in lens.keys("users")),
            staleness_budget=2)
        lens.attach_indexer(meta)

        # Add data (1 commit — within budget, no refresh yet)
        lens.put("users", "u2", {"name": "bob", "age": 25})
        lens.commit("users", "insert bob")

        # Lookup alice (should work — index has alice from initial build)
        rowid = meta.lookup_index("users", "by_name", "alice")
        assert rowid is not None, "LAZY: alice not found"
        print("  [OK] LAZY: alice found (within staleness budget)")

        # Add more data (3rd commit — exceeds budget=2, next lookup triggers refresh)
        lens.put("users", "u3", {"name": "carol", "age": 35})
        lens.commit("users", "insert carol")
        lens.put("users", "u4", {"name": "dave", "age": 40})
        lens.commit("users", "insert dave")

        # Lookup carol — should trigger refresh because staleness > budget
        rowid3 = meta.lookup_index("users", "by_name", "carol")
        assert rowid3 is not None, "LAZY: carol not found after refresh"
        print("  [OK] LAZY: carol found (index refreshed on lookup after exceeding budget)")

        kernel.close()
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_incremental_refresh():
    """refresh_index_incremental: O(changed) via commit-diff."""
    print("\n=== Test 3: refresh_index_incremental (O(changed) via commit-diff) ===")
    tmpdir = tempfile.mkdtemp(prefix="pond_inc_")
    try:
        kernel = PondMinimal(tmpdir)
        lens = KeyValueLens(kernel)
        meta = CollectionMetadata(kernel)

        # Write 10 users
        for i in range(10):
            lens.put("users", f"u{i}", {"name": f"user_{i}", "age": 20 + i})
        old_commit = lens.commit("users", "insert 10 users")

        # Build index at old_commit
        meta.build_index("users", "by_age",
            extractor=lambda r: str(r.get("age", 0)),
            scan_fn=lambda: ((k, lens.get("users", k)) for k in lens.keys("users")))

        # Verify index works
        rowid = meta.lookup_index("users", "by_age", "25")
        assert rowid is not None, "Incremental: initial index lookup failed"
        print("  [OK] Initial index built (10 users)")

        # Modify 2 users (add 1, modify 1, delete 1)
        lens.put("users", "u10", {"name": "user_10", "age": 50})  # add
        lens.put("users", "u0", {"name": "user_0_v2", "age": 99})  # modify
        lens.delete("users", "u1")  # delete
        new_commit = lens.commit("users", "modify 3 users")

        # Incremental refresh using commit-diff
        meta.refresh_index_incremental("users", "by_age",
            extractor=lambda r: str(r.get("age", 0)),
            old_commit=old_commit,
            new_commit=new_commit,
            decode_fn=lambda b: json.loads(b))

        # Verify: new user (age 50) should be in index
        rowid_new = meta.lookup_index("users", "by_age", "50")
        assert rowid_new is not None, "Incremental: new user (age 50) not found"
        print("  [OK] Incremental: new user (age 50) found after refresh")

        # Verify: modified user (age 99) should be in index
        rowid_mod = meta.lookup_index("users", "by_age", "99")
        assert rowid_mod is not None, "Incremental: modified user (age 99) not found"
        print("  [OK] Incremental: modified user (age 99) found after refresh")

        # Verify: deleted user (age 21) should NOT be in index
        rowid_del = meta.lookup_index("users", "by_age", "21")
        assert rowid_del is None, "Incremental: deleted user (age 21) should not be in index"
        print("  [OK] Incremental: deleted user (age 21) removed from index")

        kernel.close()
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_is_index_stale():
    """is_index_stale: detects stale indexes."""
    print("\n=== Test 4: is_index_stale ===")
    tmpdir = tempfile.mkdtemp(prefix="pond_stale_")
    try:
        kernel = PondMinimal(tmpdir)
        lens = KeyValueLens(kernel)
        meta = CollectionMetadata(kernel)

        lens.put("users", "u1", {"name": "alice", "age": 30})
        lens.commit("users", "insert alice")

        extractor = lambda r: r.get("name", "")
        scan_fn = lambda: ((k, lens.get("users", k)) for k in lens.keys("users"))

        # No index → stale
        assert meta.is_index_stale("users", "by_name", scan_fn, extractor) is True
        print("  [OK] No index → stale=True")

        # Build index
        meta.build_index("users", "by_name", extractor=extractor, scan_fn=scan_fn)

        # Index matches data → not stale
        assert meta.is_index_stale("users", "by_name", scan_fn, extractor) is False
        print("  [OK] Index matches data → stale=False")

        # Add data (index now stale)
        lens.put("users", "u2", {"name": "bob", "age": 25})
        lens.commit("users", "insert bob")

        assert meta.is_index_stale("users", "by_name", scan_fn, extractor) is True
        print("  [OK] Data changed → stale=True")

        kernel.close()
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == "__main__":
    print("=" * 60)
    print("Index Modes + Incremental Refresh Tests")
    print("=" * 60)
    test_eager_mode()
    test_lazy_mode()
    test_incremental_refresh()
    test_is_index_stale()
    print("\n" + "=" * 60)
    print("ALL TESTS PASSED")
    print("=" * 60)
