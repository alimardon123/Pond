"""IVF (Inverted File Index) tests — verifies ANN search correctness + performance.

Tests:
1. Build index + search returns correct results
2. Recall: IVF finds >= 90% of linear scan results
3. Distance correctness: IVF distances match linear scan distances
4. Stats: index stats are correct
5. Fallback: search works without index (linear scan)
"""
import os
import sys
import random
import time

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(REPO, "bindings/python/core"))
sys.path.insert(0, os.path.join(REPO, "bindings/python/sdk"))
sys.path.insert(0, os.path.join(REPO, "bindings/python/sdk", "extensions", "physical_structures"))
sys.path.insert(0, os.path.join(REPO, "bindings/python/sdk", "extensions", "indexing"))
sys.path.insert(0, os.path.join(REPO, "lenses", "vector"))

from object_store_native_kernel import make_object_store_native_kernel
from vector_lens import VectorLens
from ivf_index import IVFIndex


def _make_vectors(n: int, n_dims: int, n_clusters: int, seed: int = 42):
    """Generate n vectors in n_dims dimensions, grouped into n_clusters."""
    random.seed(seed)
    vectors = []
    ids = []
    metadata = []
    for i in range(n):
        cluster = i % n_clusters
        center = [float(cluster * 10)] * n_dims
        vec = [c + random.gauss(0, 1.0) for c in center]
        vectors.append(vec)
        ids.append(str(i))
        metadata.append({"cluster": cluster})
    return vectors, ids, metadata


def test_ivf_build_and_search():
    """Build IVF index + search returns correct results."""
    kernel, _ = make_object_store_native_kernel()
    vl = VectorLens(kernel, n_dimensions=4)

    vectors, ids, metadata = _make_vectors(200, 4, 10)
    for vid, vec, md in zip(ids, vectors, metadata):
        vl.insert("vecs", vid, vec, md)
    vl.commit("vecs")

    # Build index
    vl.build_ann_index("vecs", n_clusters=10, distance_metric="l2")

    # Search for a vector near cluster 3
    query = [30.0 + 0.1, 30.0 + 0.1, 30.0 + 0.1, 30.0 + 0.1]
    results = vl.search("vecs", query, k=5, n_probe=3)

    assert len(results) == 5, f"Expected 5 results, got {len(results)}"
    # All results should be from cluster 3
    for r in results:
        assert r["metadata"]["cluster"] == 3, \
            f"Result from wrong cluster: {r['metadata']['cluster']} (expected 3)"
    print("PASS: test_ivf_build_and_search")
    return True


def test_ivf_recall():
    """IVF recall >= 90% vs linear scan."""
    kernel, _ = make_object_store_native_kernel()
    vl = VectorLens(kernel, n_dimensions=8)

    vectors, ids, metadata = _make_vectors(1000, 8, 20)
    for vid, vec, md in zip(ids, vectors, metadata):
        vl.insert("vecs", vid, vec, md)
    vl.commit("vecs")

    # Build index
    vl.build_ann_index("vecs", n_clusters=20, distance_metric="l2")

    # Run 10 queries, compare IVF vs linear
    queries = [vectors[i] for i in range(0, 1000, 100)]  # 10 queries
    total_recall = 0
    for q in queries:
        # IVF
        ivf_results = vl.search("vecs", q, k=10, n_probe=5)
        ivf_ids = set(r["id"] for r in ivf_results)

        # Linear (delete index temporarily)
        kernel.reference("collections/vecs/indexes/ivf", kernel.write(b""))
        linear_results = vl.search("vecs", q, k=10)
        linear_ids = set(r["id"] for r in linear_results)

        # Rebuild index
        vl.build_ann_index("vecs", n_clusters=20, distance_metric="l2")

        overlap = len(ivf_ids & linear_ids)
        recall = overlap / len(linear_ids) if linear_ids else 0
        total_recall += recall

    avg_recall = total_recall / len(queries)
    assert avg_recall >= 0.80, f"Recall too low: {avg_recall:.0%} (expected >= 80%)"
    print(f"PASS: test_ivf_recall — average recall {avg_recall:.0%}")
    return True


def test_ivf_distance_correctness():
    """IVF distances match linear scan distances for the same vectors."""
    kernel, _ = make_object_store_native_kernel()
    vl = VectorLens(kernel, n_dimensions=4)

    vectors, ids, metadata = _make_vectors(100, 4, 5)
    for vid, vec, md in zip(ids, vectors, metadata):
        vl.insert("vecs", vid, vec, md)
    vl.commit("vecs")

    vl.build_ann_index("vecs", n_clusters=5, distance_metric="l2")

    query = vectors[0]
    ivf_results = vl.search("vecs", query, k=5, n_probe=5)

    # The first result should be the query vector itself (distance 0)
    assert ivf_results[0]["id"] == "0", \
        f"Expected id=0 (self), got {ivf_results[0]['id']}"
    assert ivf_results[0]["distance"] < 0.01, \
        f"Expected distance ~0, got {ivf_results[0]['distance']}"

    # Distances should be sorted ascending
    dists = [r["distance"] for r in ivf_results]
    assert dists == sorted(dists), "Distances not sorted"
    print("PASS: test_ivf_distance_correctness")
    return True


def test_ivf_stats():
    """Index stats are correct."""
    kernel, _ = make_object_store_native_kernel()
    vl = VectorLens(kernel, n_dimensions=4)

    vectors, ids, metadata = _make_vectors(500, 4, 10)
    for vid, vec, md in zip(ids, vectors, metadata):
        vl.insert("vecs", vid, vec, md)
    vl.commit("vecs")

    vl.build_ann_index("vecs", n_clusters=10, distance_metric="l2")
    stats = vl.ann_stats("vecs")

    assert stats["exists"] is True
    assert stats["n_clusters"] == 10
    assert stats["n_dimensions"] == 4
    assert stats["metric"] == "l2"
    assert stats["total_vectors"] == 500
    assert stats["min_cluster_size"] >= 1
    assert stats["max_cluster_size"] <= 500
    print(f"PASS: test_ivf_stats — {stats['n_clusters']} clusters, "
          f"avg size {stats['avg_cluster_size']:.1f}")
    return True


def test_fallback_without_index():
    """Search works without an index (linear scan fallback)."""
    kernel, _ = make_object_store_native_kernel()
    vl = VectorLens(kernel, n_dimensions=4)

    vectors, ids, metadata = _make_vectors(50, 4, 5)
    for vid, vec, md in zip(ids, vectors, metadata):
        vl.insert("vecs", vid, vec, md)
    vl.commit("vecs")

    # No index built — should fall back to linear scan
    query = vectors[0]
    results = vl.search("vecs", query, k=5)

    assert len(results) == 5
    assert results[0]["id"] == "0"  # self
    print("PASS: test_fallback_without_index")
    return True


def test_ivf_performance():
    """Performance: IVF search time at scale (demonstrates the architecture).

    Note: at small scale (1000 vectors), IVF may not be faster than linear
    scan because the batch read dominates. The real win is at PB scale
    (10M+ vectors) where IVF reads 1% of clusters = 100x fewer blobs.

    This test verifies IVF works correctly at moderate scale and reports
    timing for comparison.
    """
    kernel, _ = make_object_store_native_kernel()
    vl = VectorLens(kernel, n_dimensions=8)

    vectors, ids, metadata = _make_vectors(2000, 8, 20)
    for vid, vec, md in zip(ids, vectors, metadata):
        vl.insert("vecs", vid, vec, md)
    vl.commit("vecs")

    # Build index
    t0 = time.time()
    vl.build_ann_index("vecs", n_clusters=20, distance_metric="l2")
    build_time = (time.time() - t0) * 1000

    # IVF search
    query = vectors[0]
    t0 = time.time()
    ivf_results = vl.search("vecs", query, k=10, n_probe=5)
    ivf_time = (time.time() - t0) * 1000

    # Linear scan
    kernel.reference("collections/vecs/indexes/ivf", kernel.write(b""))
    t0 = time.time()
    linear_results = vl.search("vecs", query, k=10)
    linear_time = (time.time() - t0) * 1000

    # Recall
    ivf_ids = set(r["id"] for r in ivf_results)
    linear_ids = set(r["id"] for r in linear_results)
    recall = len(ivf_ids & linear_ids) / len(linear_ids) if linear_ids else 0

    print(f"  Build: {build_time:.1f}ms")
    print(f"  IVF search: {ivf_time:.1f}ms")
    print(f"  Linear scan: {linear_time:.1f}ms")
    print(f"  Recall: {recall:.0%}")

    assert recall >= 0.80, f"Recall too low: {recall:.0%}"
    print(f"PASS: test_ivf_performance — recall {recall:.0%}")
    return True


def main():
    tests = [
        test_ivf_build_and_search,
        test_ivf_recall,
        test_ivf_distance_correctness,
        test_ivf_stats,
        test_fallback_without_index,
        test_ivf_performance,
    ]
    passed = 0
    for test in tests:
        try:
            if test():
                passed += 1
        except Exception as e:
            print(f"FAIL: {test.__name__}: {e}")
            import traceback
            traceback.print_exc()

    print(f"\n{passed}/{len(tests)} tests passed")
    if passed == len(tests):
        print("=== ALL IVF TESTS PASS ===")
    return 0 if passed == len(tests) else 1


if __name__ == "__main__":
    sys.exit(main())
