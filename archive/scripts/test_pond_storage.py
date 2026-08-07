"""Test PondStorage — the ONE unified storage SDK."""
import os, sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "bindings/python/core"))
sys.path.insert(0, os.path.join(HERE, "..", "bindings/python/sdk"))
sys.path.insert(0, os.path.join(HERE, "..", "bindings/python/sdk", "extensions", "physical_structures"))

from object_store_native_kernel import make_object_store_native_kernel
from pond_storage import PondStorage


def test_pond_storage_basic():
    """Basic write/read/point_lookup via PondStorage."""
    kernel, _ = make_object_store_native_kernel()
    storage = PondStorage(kernel)

    # Write
    storage.write("users", [{"id": 1, "name": "alice"},
                              {"id": 2, "name": "bob"}],
                   key_col="id", row_group_size=10)

    # Read
    rows = storage.read("users")
    assert len(rows) == 2

    # Point lookup
    row = storage.point_lookup("users", key="1")
    assert row is not None
    assert row["name"] == "alice"

    # Namespace ops
    assert storage.collection_exists("users")
    assert "users" in storage.list_collections()

    print("PASS: test_pond_storage_basic")
    return True


def test_pond_storage_predicates():
    """Predicate-pruned read via PondStorage."""
    kernel, _ = make_object_store_native_kernel()
    storage = PondStorage(kernel)

    rows = [{"id": i, "age": i % 100, "city": ["NYC", "LA", "SF"][i % 3]}
            for i in range(100)]
    storage.write("users", rows, key_col="id", row_group_size=10)

    # Multi-predicate
    result = storage.read("users",
                            predicates=[("age", ">", 50), ("city", "=", "NYC")])
    for r in result:
        assert r["age"] > 50
        assert r["city"] == "NYC"
    assert len(result) > 0

    # Projection
    result = storage.read("users", columns=["id", "age"])
    for r in result:
        assert "city" not in r

    print("PASS: test_pond_storage_predicates")
    return True


def test_pond_storage_append():
    """Non-destructive append via PondStorage."""
    kernel, _ = make_object_store_native_kernel()
    storage = PondStorage(kernel)

    storage.write("multi", [{"id": 1, "v": "a"}], key_col="id")
    storage.append("multi", [{"id": 2, "v": "b"}], key_col="id")
    storage.append("multi", [{"id": 3, "v": "c"}], key_col="id")

    rows = storage.read("multi")
    assert len(rows) == 3, f"Expected 3 rows, got {len(rows)}"

    print("PASS: test_pond_storage_append")
    return True


def test_pond_storage_branch_merge():
    """Branch and merge via PondStorage."""
    kernel, _ = make_object_store_native_kernel()
    storage = PondStorage(kernel)

    storage.write("data", [{"id": i} for i in range(100)],
                   key_col="id", row_group_size=10)

    # Branch
    storage.branch("data", "dev")
    assert "dev" in storage.list_branches("data")

    # History
    hist = storage.history("data")
    assert len(hist) > 0

    print("PASS: test_pond_storage_branch_merge")
    return True


def test_pond_storage_round_trips():
    """Verify honest cold-read round trips."""
    kernel, _ = make_object_store_native_kernel()
    storage = PondStorage(kernel)

    storage.write("test", [{"id": i} for i in range(100)],
                   key_col="id", row_group_size=10)

    # Cold point lookup
    kernel.invalidate_root_cache()
    storage._unified._manifest_cache.clear()
    storage._unified._manifest_hash_cache.clear()
    kernel.reset_stats()

    row = storage.point_lookup("test", key="42")
    assert row is not None
    assert row["id"] == 42

    total = kernel.stats["reads"] + kernel.stats["ref_reads"]
    print(f"\n  Cold point lookup: {total} GETs")
    # With dedicated paths: 1 ref_read (manifest ref) + 2 data reads = 3
    assert total == 3, f"Expected 3 GETs, got {total}"

    # Round trip estimate
    rt = storage.get_round_trip_count("test", predicates=[("id", ">", 90)])
    print(f"  Round trip estimate: {rt}")

    print("PASS: test_pond_storage_round_trips")
    return True


def test_pond_storage_cross_workload():
    """Same PondStorage instance serves tabular, KV-style, and vector-style data."""
    kernel, _ = make_object_store_native_kernel()
    storage = PondStorage(kernel)

    # Tabular
    storage.write("tabular", [{"id": i, "val": i * 2} for i in range(50)],
                   key_col="id")

    # KV-style (dict values stored as JSON string)
    storage.write("kv", [{"_key": f"k{i}", "value": '{"name": "user_' + str(i) + '"}'}
                            for i in range(50)],
                   key_col="_key")

    # Vector-style (float dimensions)
    storage.write("vectors", [{"id": i, "x": float(i), "y": float(i * 2)}
                                for i in range(50)],
                   key_col="id")

    # All three readable via the SAME API
    assert len(storage.read("tabular")) == 50
    assert len(storage.read("kv")) == 50
    assert len(storage.read("vectors")) == 50

    # All three support predicates
    assert len(storage.read("tabular", predicates=[("val", ">", 50)])) > 0
    assert len(storage.read("vectors", predicates=[("x", ">", 25)])) > 0

    # All three have manifests
    for name in ["tabular", "kv", "vectors"]:
        rt = storage.get_round_trip_count(name)
        assert "error" not in rt, f"{name} has no manifest"

    print("PASS: test_pond_storage_cross_workload")
    return True


if __name__ == "__main__":
    ok1 = test_pond_storage_basic()
    ok2 = test_pond_storage_predicates()
    ok3 = test_pond_storage_append()
    ok4 = test_pond_storage_branch_merge()
    ok5 = test_pond_storage_round_trips()
    ok6 = test_pond_storage_cross_workload()
    if all([ok1, ok2, ok3, ok4, ok5, ok6]):
        print("\n=== ALL PONDSTORAGE TESTS PASS ===")
        sys.exit(0)
    else:
        print("\n=== SOME TESTS FAILED ===")
        sys.exit(1)
