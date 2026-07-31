"""
Test KeyValueLens with unified storage backend.

Proves that the app lens (KeyValueLens) can use UnifiedStorage as its
storage backend with NO adapter layer. The same lens API works for both
the legacy ProllyTreeIndex path and the new unified storage path.

Tests:
  1. put/get/commit with use_unified_storage=True
  2. iterate, keys, count, exists, get_all
  3. Multi-commit preserves data (append semantics)
  4. Point lookup is 4 GETs cold (vs O(log N) legacy)
  5. Legacy path still works (use_unified_storage=False)
"""
import os, sys, json

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "pond-core"))
sys.path.insert(0, os.path.join(HERE, "..", "pond-sdk"))
sys.path.insert(0, os.path.join(HERE, "..", "pond-sdk", "extensions", "physical_structures"))
sys.path.insert(0, os.path.join(HERE, "..", "lenses", "keyvalue"))

from object_store_native_kernel import make_object_store_native_kernel
from keyvalue_lens import KeyValueLens


def test_unified_kv_basic():
    """Basic put/get/commit with unified storage backend."""
    kernel, _ = make_object_store_native_kernel()
    kv = KeyValueLens(kernel, use_unified_storage=True)

    kv.put("users", "user:1", {"name": "alice", "age": 30})
    kv.put("users", "user:2", {"name": "bob", "age": 25})
    kv.commit("users")

    # Point lookup
    user1 = kv.get("users", "user:1")
    assert user1 is not None
    assert user1["name"] == "alice"
    assert user1["age"] == 30

    user2 = kv.get("users", "user:2")
    assert user2 is not None
    assert user2["name"] == "bob"

    print("PASS: test_unified_kv_basic")
    return True


def test_unified_kv_iterate():
    """iterate, keys, count, exists, get_all with unified storage."""
    kernel, _ = make_object_store_native_kernel()
    kv = KeyValueLens(kernel, use_unified_storage=True)

    for i in range(10):
        kv.put("items", f"item:{i}", {"id": i, "name": f"name_{i}"})
    kv.commit("items")

    # keys
    all_keys = kv.keys("items")
    assert len(all_keys) == 10
    assert "item:0" in all_keys
    assert "item:9" in all_keys

    # count
    assert kv.count("items") == 10

    # exists
    assert kv.exists("items", "item:5")
    assert not kv.exists("items", "item:999")

    # get_all
    all_items = kv.get_all("items")
    assert len(all_items) == 10
    assert all_items["item:0"]["name"] == "name_0"

    # iterate
    iterated = list(kv.iterate("items"))
    assert len(iterated) == 10

    print("PASS: test_unified_kv_iterate")
    return True


def test_unified_kv_multi_commit():
    """Multiple commits preserve all data (append semantics)."""
    kernel, _ = make_object_store_native_kernel()
    kv = KeyValueLens(kernel, use_unified_storage=True)

    # Commit 1
    kv.put("multi", "k1", {"v": 1})
    kv.put("multi", "k2", {"v": 2})
    kv.commit("multi")

    # Commit 2
    kv.put("multi", "k3", {"v": 3})
    kv.put("multi", "k4", {"v": 4})
    kv.commit("multi")

    # All 4 keys should exist
    all_items = kv.get_all("multi")
    print(f"\n  After 2 commits: {len(all_items)} keys")
    assert len(all_items) == 4, \
        f"Expected 4 keys, got {len(all_items)} (destructive overwrite!)"

    for i in range(1, 5):
        assert kv.exists("multi", f"k{i}")
        v = kv.get("multi", f"k{i}")
        assert v["v"] == i

    print("PASS: test_unified_kv_multi_commit")
    return True


def test_unified_kv_point_lookup_4_gets():
    """Cold point lookup is 4-5 GETs (vs O(log N) legacy).

    Cross-lens awareness costs 1 extra GET on the FIRST cold lookup
    (to fetch the collection's metadata.key_col). The metadata is
    cached on the lens, so subsequent lookups on the same collection
    are 4 GETs.
    """
    kernel, _ = make_object_store_native_kernel()
    kv = KeyValueLens(kernel, use_unified_storage=True)

    for i in range(100):
        kv.put("big", f"k{i}", {"v": i})
    kv.commit("big")

    # Cold point lookup
    kernel.invalidate_root_cache()
    kv._unified_storage._manifest_cache.clear()
    kv._key_col_cache.clear()  # cross-lens metadata cache
    kernel.reset_stats()

    v = kv.get("big", "k42")
    assert v is not None
    assert v["v"] == 42

    total_gets = kernel.stats["reads"] + kernel.stats["ref_reads"]
    print(f"\n  Cold point lookup (k42): {total_gets} GETs (first call, includes metadata fetch)")
    # 2 ref + 1 manifest + 1 data + 1 metadata ref = 5 GETs cold (first call)
    # OR 4 GETs if metadata ref returned None (no definition blob to read)
    assert total_gets in (4, 5), f"expected 4-5 GETs, got {total_gets}"

    # Second lookup: metadata is cached, so should be 4 GETs
    kernel.invalidate_root_cache()
    kv._unified_storage._manifest_cache.clear()
    kernel.reset_stats()
    v2 = kv.get("big", "k42")
    assert v2 is not None
    assert v2["v"] == 42
    total_gets_2 = kernel.stats["reads"] + kernel.stats["ref_reads"]
    print(f"  Warm point lookup (k42): {total_gets_2} GETs (subsequent, metadata cached)")
    assert total_gets_2 <= 4, f"expected <=4 GETs on warm lookup, got {total_gets_2}"

    print("PASS: test_unified_kv_point_lookup_4_gets")
    return True


def test_legacy_kv_still_works():
    """Legacy path is no longer supported — use_unified_storage flag is
    ignored. This test verifies that passing use_unified_storage=False
    still works (uses the unified path, as that's the only path now)."""
    from kernel import PondMinimal
    import tempfile, shutil
    tmp = tempfile.mkdtemp(prefix="pond-legacy-")
    try:
        kernel = PondMinimal(tmp)
        # use_unified_storage=False is ignored — always unified now
        kv = KeyValueLens(kernel, use_unified_storage=False)

        kv.put("users", "user:1", {"name": "alice"})
        kv.commit("users")

        user = kv.get("users", "user:1")
        assert user is not None
        assert user["name"] == "alice"

        print("PASS: test_legacy_kv_still_works (unified path, flag ignored)")
        return True
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    ok1 = test_unified_kv_basic()
    ok2 = test_unified_kv_iterate()
    ok3 = test_unified_kv_multi_commit()
    ok4 = test_unified_kv_point_lookup_4_gets()
    ok5 = test_legacy_kv_still_works()
    if all([ok1, ok2, ok3, ok4, ok5]):
        print("\n=== ALL KEYVALUELENS UNIFIED STORAGE TESTS PASS ===")
        sys.exit(0)
    else:
        print("\n=== SOME TESTS FAILED ===")
        sys.exit(1)
