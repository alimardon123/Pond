"""OLTP Lens tests — memtable + batch flush + cold concurrent multi-app.

Tests:
1. Basic put/get/delete (memtable)
2. Flush to object storage (data visible to cold readers)
3. Cold concurrent multi-app writes (multiple OLTPLens instances)
4. Read-your-writes (memtable consistency)
5. Performance: memtable vs direct shard write
"""
import sys, os, time, threading

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(REPO, "bindings/python/core"))
sys.path.insert(0, os.path.join(REPO, "bindings/python/sdk"))
sys.path.insert(0, os.path.join(REPO, "bindings/python/sdk", "extensions", "physical_structures"))
sys.path.insert(0, os.path.join(REPO, "lenses", "oltp"))

from object_store_native_kernel import make_object_store_native_kernel
from pond_storage import PondStorage
from oltp_lens import OLTPLens


def _setup(collection="kv"):
    kernel, _ = make_object_store_native_kernel()
    storage = PondStorage(kernel)
    storage.write(collection, [{"_key": "init", "value": b""}], key_col="_key",
                   row_group_size=100)
    return kernel, storage


def test_basic_put_get():
    """Basic put/get works in-memory."""
    kernel, storage = _setup()
    ottp = OLTPLens(storage, "kv", flush_threshold=100)
    ottp.put("user:1", {"name": "alice", "age": 30})
    ottp.put("user:2", {"name": "bob", "age": 25})
    assert ottp.get("user:1")["name"] == "alice"
    assert ottp.get("user:2")["age"] == 25
    assert ottp.pending_count() == 2
    print("PASS: test_basic_put_get")
    return True


def test_delete():
    """Delete works as tombstone in memtable."""
    kernel, storage = _setup()
    ottp = OLTPLens(storage, "kv", flush_threshold=100)
    ottp.put("user:1", {"name": "alice"})
    ottp.delete("user:1")
    assert ottp.get("user:1") is None
    print("PASS: test_delete")
    return True


def test_flush_visible_to_cold_reader():
    """After flush, data is visible to a new connection (cold read)."""
    kernel, storage = _setup()
    ottp = OLTPLens(storage, "kv", flush_threshold=100)
    ottp.put("user:1", {"name": "alice"})
    ottp.put("user:2", {"name": "bob"})
    ottp.flush()

    # New connection (no memtable)
    storage2 = PondStorage(kernel)
    rows = storage2.read_with_shards("kv")
    keys = [r.get("_key") for r in rows if r.get("_key") and r.get("_key") != "init"]
    assert "user:1" in keys
    assert "user:2" in keys
    print(f"PASS: test_flush_visible_to_cold_reader — {len(keys)} keys visible")
    return True


def test_cold_concurrent_multi_app():
    """Multiple apps writing concurrently — CRDT handles conflicts."""
    kernel, storage = _setup()

    results = []
    errors = []

    def app_writer(app_id, n_writes):
        try:
            local_storage = PondStorage(kernel)
            ottp = OLTPLens(local_storage, "kv", flush_threshold=50)
            for i in range(n_writes):
                ottp.put(f"app{app_id}:key{i}", {"v": i, "app": app_id})
            ottp.flush()  # flush remaining
            results.append(app_id)
        except Exception as e:
            errors.append((app_id, str(e)[:60]))

    # 5 concurrent apps, each writing 100 keys
    threads = [threading.Thread(target=app_writer, args=(w, 100)) for w in range(5)]
    t0 = time.time()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    t1 = time.time()

    assert len(errors) == 0, f"Errors: {errors[:3]}"
    assert len(results) == 5

    # Cold reader sees ALL data
    reader = PondStorage(kernel)
    rows = reader.read_with_shards("kv")
    keys = [r.get("_key") for r in rows if r.get("_key") and r.get("_key") != "init"]
    assert len(keys) == 500, f"Expected 500 keys, got {len(keys)}"

    print(f"PASS: test_cold_concurrent_multi_app — "
          f"5 apps × 100 writes = 500 keys in {(t1-t0)*1000:.0f}ms, 0 data loss")
    return True


def test_read_your_writes():
    """Writes to memtable are immediately visible to reads."""
    kernel, storage = _setup()
    ottp = OLTPLens(storage, "kv", flush_threshold=10000)  # high threshold = no auto-flush
    ottp.put("user:1", {"name": "alice"})
    assert ottp.get("user:1")["name"] == "alice"  # read-your-writes
    ottp.put("user:1", {"name": "alice_v2"})  # update
    assert ottp.get("user:1")["name"] == "alice_v2"
    ottp.delete("user:1")
    assert ottp.get("user:1") is None
    print("PASS: test_read_your_writes")
    return True


def test_performance_memtable_vs_direct():
    """Benchmark: memtable amortizes S3 latency across N writes."""
    kernel, storage = _setup()
    N = 500

    # Direct shard writes (1 S3 PUT per write)
    kernel2, storage2 = _setup("kv_direct")
    t0 = time.time()
    for i in range(N):
        storage2.append_shard("kv_direct", [{"_key": f"k{i}", "value": f"v{i}".encode()}],
                               key_col="_key", row_group_size=100)
    t1 = time.time()
    direct_ms = (t1 - t0) * 1000

    # Memtable writes (sub-µs each, 1 flush at end)
    ottp = OLTPLens(storage, "kv", flush_threshold=10000)
    t0 = time.time()
    for i in range(N):
        ottp.put(f"k{i}", f"v{i}")
    ottp.flush()
    t1 = time.time()
    memtable_ms = (t1 - t0) * 1000

    # Verify data integrity
    reader = PondStorage(kernel)
    rows = reader.read_with_shards("kv")
    keys = [r.get("_key") for r in rows if r.get("_key") and r.get("_key") != "init"]
    assert len(keys) == N, f"Expected {N} keys, got {len(keys)}"

    speedup = direct_ms / max(memtable_ms, 0.1)
    print(f"PASS: test_performance_memtable_vs_direct — "
          f"direct: {direct_ms:.0f}ms ({direct_ms/N:.2f}ms/write), "
          f"memtable: {memtable_ms:.0f}ms ({memtable_ms/N:.3f}ms/write), "
          f"{speedup:.0f}x speedup")
    return True


def test_compact():
    """Compact merges all shards into HEAD."""
    kernel, storage = _setup()
    ottp = OLTPLens(storage, "kv", flush_threshold=10)
    for i in range(50):
        ottp.put(f"k{i}", {"v": i})
    ottp.flush()

    assert storage.shard_count("kv") > 0
    ottp.compact()
    assert storage.shard_count("kv") == 0

    # Data still readable
    rows = storage.read_with_shards("kv")
    keys = [r.get("_key") for r in rows if r.get("_key") and r.get("_key") != "init"]
    assert len(keys) == 50
    print(f"PASS: test_compact — {len(keys)} keys after compaction")
    return True


def main():
    tests = [
        test_basic_put_get,
        test_delete,
        test_flush_visible_to_cold_reader,
        test_cold_concurrent_multi_app,
        test_read_your_writes,
        test_performance_memtable_vs_direct,
        test_compact,
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
        print("=== ALL OLTP TESTS PASS ===")
    return 0 if passed == len(tests) else 1


if __name__ == "__main__":
    sys.exit(main())
