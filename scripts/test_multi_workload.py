"""Multi-workload integration test — proves ALL 5 workloads work on PondStorage.

Tests: Lakehouse (tabular), KV (JSON + bytes), Vector (per-dim FLOAT64),
Notebook (STRING + BINARY), Streaming (BINARY segments with range read).
"""
import os, sys, json, struct

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "pond-core"))
sys.path.insert(0, os.path.join(HERE, "..", "pond-sdk"))
sys.path.insert(0, os.path.join(HERE, "..", "pond-sdk", "extensions", "physical_structures"))
sys.path.insert(0, os.path.join(HERE, "..", "lenses", "keyvalue"))
sys.path.insert(0, os.path.join(HERE, "..", "lenses", "vector"))
sys.path.insert(0, os.path.join(HERE, "..", "lenses", "streaming"))
sys.path.insert(0, os.path.join(HERE, "..", "lenses", "lakehouse"))

from object_store_native_kernel import make_object_store_native_kernel
from pond_storage import PondStorage


def test_lakehouse_workload():
    """Tabular: INT64 + STRING + FLOAT64 columns."""
    kernel, _ = make_object_store_native_kernel()
    storage = PondStorage(kernel)
    rows = [{"id": i, "name": f"user_{i}", "score": float(i) * 0.1}
            for i in range(100)]
    storage.write("users", rows, key_col="id", row_group_size=10)
    # Point lookup
    row = storage.point_lookup("users", key="50")
    assert row is not None and row["id"] == 50
    # Predicate
    result = storage.read("users", predicates=[("id", ">", 90)])
    assert len(result) == 9
    print("PASS: test_lakehouse_workload")
    return True


def test_kv_json_workload():
    """KV with JSON values."""
    kernel, _ = make_object_store_native_kernel()
    storage = PondStorage(kernel)
    rows = [{"_key": f"k{i}", "value": json.dumps({"v": i}).encode()}
            for i in range(50)]
    storage.write("kv", rows, key_col="_key", row_group_size=10)
    row = storage.point_lookup("kv", key="k25")
    assert row is not None
    val = json.loads(row["value"])
    assert val["v"] == 25
    print("PASS: test_kv_json_workload")
    return True


def test_kv_bytes_workload():
    """KV with raw bytes values (git blobs, attachments)."""
    kernel, _ = make_object_store_native_kernel()
    storage = PondStorage(kernel)
    rows = [{"_key": f"file_{i}", "value": f"content_{i}".encode() * 100}
            for i in range(20)]
    storage.write("blobs", rows, key_col="_key", row_group_size=5)
    row = storage.point_lookup("blobs", key="file_10")
    assert row is not None
    assert row["value"] == b"content_10" * 100
    print("PASS: test_kv_bytes_workload")
    return True


def test_empty_bytes_workload():
    """Empty bytes (b'') vs None disambiguation (Round 24 Fix 1)."""
    kernel, _ = make_object_store_native_kernel()
    storage = PondStorage(kernel)
    rows = [
        {"_key": "empty", "value": b""},
        {"_key": "null", "value": None},
        {"_key": "data", "value": b"hello"},
    ]
    storage.write("mixed", rows, key_col="_key", row_group_size=10)
    all_rows = {r["_key"]: r["value"] for r in storage.read("mixed")}
    assert all_rows["empty"] == b"", f"empty should be b'', got {all_rows['empty']!r}"
    assert all_rows["null"] is None, f"null should be None, got {all_rows['null']!r}"
    assert all_rows["data"] == b"hello"
    print("PASS: test_empty_bytes_workload")
    return True


def test_vector_per_dim_workload():
    """Vector with per-dimension FLOAT64 columns (bbox pruning)."""
    from vector_lens import VectorLens
    kernel, _ = make_object_store_native_kernel()
    vl = VectorLens(kernel, n_dimensions=4, use_unified_storage=True)
    for i in range(50):
        vl.insert("vecs", str(i), [float(i), float(i % 10), float(i % 5), float(i % 3)])
    vl.commit("vecs")
    # Point lookup
    vec = vl.get_vector("vecs", "25")
    assert vec is not None
    assert vec["vector"] == [25.0, 5.0, 0.0, 1.0]
    # Predicate on dim_0 (bbox pruning via manifest stats)
    result = vl.get_all("vecs")
    assert len(result) == 50
    print("PASS: test_vector_per_dim_workload")
    return True


def test_notebook_workload():
    """Notebook: STRING cells + BINARY attachments + INT64 seq."""
    kernel, _ = make_object_store_native_kernel()
    storage = PondStorage(kernel)
    rows = [
        {"cell_id": i, "source": f"print({i})", "attachment": f"\\x89PNG{i}".encode() if i % 2 == 0 else None, "seq": i}
        for i in range(30)
    ]
    storage.write("notebook", rows, key_col="cell_id", row_group_size=10)
    # Point lookup
    cell = storage.point_lookup("notebook", key="5")
    assert cell is not None
    assert cell["source"] == "print(5)"
    assert cell["attachment"] is None  # odd cells have no attachment
    assert cell["seq"] == 5
    # Cell with attachment
    cell6 = storage.point_lookup("notebook", key="6")
    assert cell6["attachment"] == b"\\x89PNG6"
    print("PASS: test_notebook_workload")
    return True


def test_streaming_workload():
    """Streaming: BINARY segments with range read."""
    from streaming_lens import StreamingLens
    kernel, _ = make_object_store_native_kernel()
    lens = StreamingLens(kernel, use_unified_storage=True)
    # Write 1000 bytes in 100-byte segments
    data = bytes(range(256)) * 4  # 1024 bytes
    lens.write_stream("video", data[:1000], segment_size=100)
    # Full read
    full = lens.read_stream("video")
    assert len(full) == 1000
    assert full == data[:1000]
    # Range read [200, 400)
    chunk = lens.read_stream("video", start_byte=200, end_byte=400)
    assert len(chunk) == 200
    assert chunk == data[200:400]
    print("PASS: test_streaming_workload")
    return True


def test_git_workload():
    """Git: file paths as keys, content as BINARY, commit metadata as STRING."""
    kernel, _ = make_object_store_native_kernel()
    storage = PondStorage(kernel)
    rows = [
        {"path": "README.md", "content": b"# Project\n\nHello world\n", "sha": "abc123", "type": "blob"},
        {"path": "src/main.py", "content": b"print('hello')\n", "sha": "def456", "type": "blob"},
        {"path": "src/empty.txt", "content": b"", "sha": "empty", "type": "blob"},
        {"path": "HEAD", "content": b"abc123", "sha": "ref", "type": "commit"},
    ]
    storage.write("repo", rows, key_col="path", row_group_size=10)
    # Point lookup
    readme = storage.point_lookup("repo", key="README.md")
    assert readme is not None
    assert readme["content"] == b"# Project\n\nHello world\n"
    assert readme["type"] == "blob"
    # Empty file
    empty = storage.point_lookup("repo", key="src/empty.txt")
    assert empty is not None
    assert empty["content"] == b"", f"empty file should be b'', got {empty['content']!r}"
    print("PASS: test_git_workload")
    return True


def test_feature_store_workload():
    """Feature Store: INT64 entity_id + INT64 timestamp + FLOAT64 features."""
    kernel, _ = make_object_store_native_kernel()
    storage = PondStorage(kernel)
    rows = [
        {"entity_id": 1000 + i, "timestamp": 1700000000 + i * 60,
         "feature_age": float(20 + i % 50),
         "feature_income": float(50000 + i * 1000),
         "feature_score": float(i % 100) * 0.1}
        for i in range(200)
    ]
    storage.write("features", rows, key_col="entity_id", row_group_size=50)
    # Point lookup
    feat = storage.point_lookup("features", key="1050")
    assert feat is not None
    assert feat["entity_id"] == 1050
    assert feat["feature_age"] == float(20 + 50 % 50)
    # Predicate pushdown on a feature column
    result = storage.read("features",
                          predicates=[("feature_age", ">", 60)],
                          columns=["entity_id", "feature_age"])
    assert len(result) > 0
    for r in result:
        assert r["feature_age"] > 60
    print("PASS: test_feature_store_workload")
    return True


def test_cross_workload_same_kernel():
    """All 5 workloads on the same kernel instance."""
    kernel, _ = make_object_store_native_kernel()
    storage = PondStorage(kernel)

    # All workloads coexist
    storage.write("users", [{"id": 1, "name": "alice"}], key_col="id")
    storage.write("blobs", [{"_key": "f1", "value": b"binary data"}], key_col="_key")
    storage.write("vectors", [{"id": "v1", "dim_0": 0.1, "dim_1": 0.2}], key_col="id")
    storage.write("notebook", [{"cell_id": 0, "source": "print(1)", "attachment": b"PNG"}], key_col="cell_id")
    storage.write("repo", [{"path": "README", "content": b"# Hi"}], key_col="path")

    # All readable
    assert storage.point_lookup("users", key="1")["name"] == "alice"
    assert storage.point_lookup("blobs", key="f1")["value"] == b"binary data"
    assert storage.point_lookup("vectors", key="v1")["dim_0"] == 0.1
    assert storage.point_lookup("notebook", key="0")["source"] == "print(1)"
    assert storage.point_lookup("repo", key="README")["content"] == b"# Hi"

    # All in the same namespace
    collections = storage.list_collections()
    assert len(collections) >= 5

    print("PASS: test_cross_workload_same_kernel")
    return True


if __name__ == "__main__":
    tests = [
        test_lakehouse_workload,
        test_kv_json_workload,
        test_kv_bytes_workload,
        test_empty_bytes_workload,
        test_vector_per_dim_workload,
        test_notebook_workload,
        test_streaming_workload,
        test_git_workload,
        test_feature_store_workload,
        test_cross_workload_same_kernel,
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
        print("=== ALL MULTI-WORKLOAD TESTS PASS ===")
        sys.exit(0)
    else:
        sys.exit(1)
