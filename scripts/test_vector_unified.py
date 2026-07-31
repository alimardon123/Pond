"""Test VectorLens with unified storage backend."""
import os, sys, math

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "pond-core"))
sys.path.insert(0, os.path.join(HERE, "..", "pond-sdk"))
sys.path.insert(0, os.path.join(HERE, "..", "pond-sdk", "extensions", "physical_structures"))
sys.path.insert(0, os.path.join(HERE, "..", "lenses", "vector"))

from object_store_native_kernel import make_object_store_native_kernel
from vector_lens import VectorLens


def test_unified_vector_basic():
    """Basic insert/search/get_vector with unified storage backend."""
    kernel, _ = make_object_store_native_kernel()
    vl = VectorLens(kernel, n_dimensions=3, use_unified_storage=True)

    # Insert vectors
    vl.insert("vecs", "1", [0.1, 0.1, 0.1], {"label": "a"})
    vl.insert("vecs", "2", [0.2, 0.1, 0.0], {"label": "b"})
    vl.insert("vecs", "3", [10.0, 10.0, 10.0], {"label": "c"})
    vl.commit("vecs")

    # Point lookup
    vec = vl.get_vector("vecs", "1")
    assert vec is not None
    assert vec["vector"] == [0.1, 0.1, 0.1]
    assert vec["metadata"]["label"] == "a"

    # Search
    results = vl.search("vecs", [0.0, 0.0, 0.0], k=2)
    assert len(results) == 2
    ids = [r["id"] for r in results]
    assert "1" in ids and "2" in ids  # closest to origin

    # list_vectors, count
    all_ids = vl.list_vectors("vecs")
    assert len(all_ids) == 3
    assert vl.count("vecs") == 3

    print("PASS: test_unified_vector_basic")
    return True


def test_unified_vector_point_lookup_4_gets():
    """Cold point lookup is 4-5 GETs.

    Cross-lens awareness costs 1 extra GET on the FIRST cold lookup
    (to fetch the collection's metadata.key_col). Subsequent lookups
    on the same collection are 4 GETs (metadata cached).
    """
    kernel, _ = make_object_store_native_kernel()
    vl = VectorLens(kernel, n_dimensions=3, use_unified_storage=True)

    for i in range(100):
        vl.insert("vecs", str(i), [float(i), 0.0, 0.0])
    vl.commit("vecs")

    # Cold point lookup
    kernel.invalidate_root_cache()
    vl._unified_storage._manifest_cache.clear()
    kernel.reset_stats()

    vec = vl.get_vector("vecs", "42")
    assert vec is not None
    assert vec["vector"] == [42.0, 0.0, 0.0]

    total_gets = kernel.stats["reads"] + kernel.stats["ref_reads"]
    print(f"\n  Cold point lookup (id=42): {total_gets} GETs (first call, includes metadata fetch)")
    assert total_gets in (4, 5), f"expected 4-5 GETs, got {total_gets}"

    # Second lookup: metadata cached
    kernel.invalidate_root_cache()
    vl._unified_storage._manifest_cache.clear()
    kernel.reset_stats()
    vec2 = vl.get_vector("vecs", "42")
    assert vec2 is not None
    total_gets_2 = kernel.stats["reads"] + kernel.stats["ref_reads"]
    print(f"  Warm point lookup (id=42): {total_gets_2} GETs (subsequent, metadata cached)")
    assert total_gets_2 <= 4, f"expected <=4 GETs on warm lookup, got {total_gets_2}"

    print("PASS: test_unified_vector_point_lookup_4_gets")
    return True


def test_unified_vector_multi_commit():
    """Multiple commits preserve all vectors."""
    kernel, _ = make_object_store_native_kernel()
    vl = VectorLens(kernel, n_dimensions=2, use_unified_storage=True)

    vl.insert("vecs", "1", [1.0, 2.0])
    vl.insert("vecs", "2", [3.0, 4.0])
    vl.commit("vecs")

    vl.insert("vecs", "3", [5.0, 6.0])
    vl.insert("vecs", "4", [7.0, 8.0])
    vl.commit("vecs")

    all_vecs = vl.get_all("vecs")
    print(f"\n  After 2 commits: {len(all_vecs)} vectors")
    assert len(all_vecs) == 4, f"Expected 4, got {len(all_vecs)}"

    print("PASS: test_unified_vector_multi_commit")
    return True


def test_legacy_vector_still_works():
    """Legacy path (use_unified_storage=False) still works."""
    from kernel import PondMinimal
    import tempfile, shutil
    tmp = tempfile.mkdtemp(prefix="pond-legacy-")
    try:
        kernel = PondMinimal(tmp)
        vl = VectorLens(kernel, use_unified_storage=False)

        vl.insert("vecs", "1", [1.0, 2.0], {"label": "a"})
        vec = vl.get_vector("vecs", "1")
        assert vec is not None
        assert vec["vector"] == [1.0, 2.0]

        print("PASS: test_legacy_vector_still_works")
        return True
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    ok1 = test_unified_vector_basic()
    ok2 = test_unified_vector_point_lookup_4_gets()
    ok3 = test_unified_vector_multi_commit()
    ok4 = test_legacy_vector_still_works()
    if all([ok1, ok2, ok3, ok4]):
        print("\n=== ALL VECTORLENS UNIFIED STORAGE TESTS PASS ===")
        sys.exit(0)
    else:
        print("\n=== SOME TESTS FAILED ===")
        sys.exit(1)
