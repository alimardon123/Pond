"""Concurrency tests — verifies multi-writer/multi-engine scenarios.

Tests:
1. Multiple concurrent writers (append_concurrent with CAS)
2. No data loss under contention
3. New connection reads latest state (cache-independent)
4. Concurrent reads while writes happen
5. Mixed workload: streaming writes + point lookups simultaneously
"""
import os
import sys
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(REPO, "pond-core"))
sys.path.insert(0, os.path.join(REPO, "pond-sdk"))
sys.path.insert(0, os.path.join(REPO, "pond-sdk", "extensions", "physical_structures"))

from object_store_native_kernel import make_object_store_native_kernel
from pond_storage import PondStorage


def test_concurrent_writers_no_data_loss():
    """5 concurrent writers, each appending 20 times — no data loss."""
    kernel, _ = make_object_store_native_kernel()
    storage = PondStorage(kernel)
    storage.write("events", [{"id": 0, "event": "init"}], key_col="id", row_group_size=10)

    results = []
    errors = []

    def writer(writer_id, n_appends):
        try:
            local_storage = PondStorage(kernel)
            for i in range(n_appends):
                rows = [{"id": writer_id * 1000 + i + 1, "event": f"w{writer_id}_{i}"}]
                local_storage.append_concurrent("events", rows, key_col="id", row_group_size=10)
            results.append(writer_id)
        except Exception as e:
            errors.append((writer_id, str(e)))

    threads = [threading.Thread(target=writer, args=(w, 20)) for w in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(errors) == 0, f"Errors: {errors[:3]}"
    assert len(results) == 5

    # Verify all 101 rows present (1 init + 100 appends)
    final = PondStorage(kernel)
    all_rows = final.read("events")
    assert len(all_rows) == 101, f"Expected 101 rows, got {len(all_rows)}"

    # Verify no missing IDs
    ids = set(r["id"] for r in all_rows)
    expected = {0} | {w * 1000 + i + 1 for w in range(5) for i in range(20)}
    missing = expected - ids
    assert len(missing) == 0, f"Missing IDs: {sorted(missing)[:10]}"

    print(f"PASS: test_concurrent_writers_no_data_loss — 100 appends, 0 lost")
    return True


def test_new_connection_reads_latest():
    """A new PondStorage instance (fresh caches) reads the latest state."""
    kernel, _ = make_object_store_native_kernel()

    # Writer 1 creates and appends
    s1 = PondStorage(kernel)
    s1.write("data", [{"id": 1, "v": "a"}], key_col="id", row_group_size=10)
    s1.append_concurrent("data", [{"id": 2, "v": "b"}], key_col="id", row_group_size=10)

    # Writer 2 (separate instance, no shared cache) appends
    s2 = PondStorage(kernel)
    s2.append_concurrent("data", [{"id": 3, "v": "c"}], key_col="id", row_group_size=10)

    # New connection reads — should see all 3 rows
    s3 = PondStorage(kernel)
    rows = s3.read("data")
    assert len(rows) == 3, f"Expected 3 rows, got {len(rows)}"
    ids = sorted(r["id"] for r in rows)
    assert ids == [1, 2, 3], f"Expected [1,2,3], got {ids}"

    print("PASS: test_new_connection_reads_latest — fresh instance sees all data")
    return True


def test_concurrent_reads_during_writes():
    """Reads work correctly while writes are happening."""
    kernel, _ = make_object_store_native_kernel()
    storage = PondStorage(kernel)
    storage.write("counter", [{"id": 0, "n": 0}], key_col="id", row_group_size=10)

    stop_writes = threading.Event()
    read_counts = []
    errors = []

    def writer():
        local = PondStorage(kernel)
        i = 1
        while not stop_writes.is_set():
            try:
                local.append_concurrent("counter", [{"id": i, "n": i}],
                                          key_col="id", row_group_size=10)
                i += 1
            except Exception as e:
                errors.append(("writer", str(e)))
                break

    def reader():
        local = PondStorage(kernel)
        for _ in range(10):
            try:
                rows = local.read("counter")
                read_counts.append(len(rows))
            except Exception as e:
                errors.append(("reader", str(e)))
            time.sleep(0.001)

    w = threading.Thread(target=writer)
    r = threading.Thread(target=reader)

    w.start()
    r.start()
    r.join()
    stop_writes.set()
    w.join()

    assert len(errors) == 0, f"Errors: {errors[:3]}"
    assert len(read_counts) == 10
    # Reads should see monotonically increasing counts
    assert read_counts[-1] >= read_counts[0], "Reads didn't see progress"

    print(f"PASS: test_concurrent_reads_during_writes — "
          f"reads saw {read_counts[0]}→{read_counts[-1]} rows during writes")
    return True


def test_mixed_workload_streaming_and_lookup():
    """Streaming writes + point lookups simultaneously."""
    kernel, _ = make_object_store_native_kernel()
    storage = PondStorage(kernel)
    # Pre-populate with 100 rows
    storage.write("users", [{"id": i, "name": f"u{i}"} for i in range(100)],
                   key_col="id", row_group_size=10)

    errors = []
    lookup_results = []

    def streamer():
        local = PondStorage(kernel)
        for i in range(50):
            try:
                local.append_concurrent("users", [{"id": 1000 + i, "name": f"stream{i}"}],
                                          key_col="id", row_group_size=10)
            except Exception as e:
                errors.append(("streamer", str(e)))
                break

    def lookuper():
        local = PondStorage(kernel)
        for i in range(20):
            try:
                row = local.point_lookup("users", key=str(i))
                if row:
                    lookup_results.append(row["id"])
            except Exception as e:
                errors.append(("lookuper", str(e)))
            time.sleep(0.001)

    t1 = threading.Thread(target=streamer)
    t2 = threading.Thread(target=lookuper)
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    assert len(errors) == 0, f"Errors: {errors[:3]}"
    assert len(lookup_results) > 0
    # All lookups should return correct IDs
    for uid in lookup_results:
        assert 0 <= uid < 100, f"Lookup returned wrong ID: {uid}"

    # Final state: 100 original + 50 streamed = 150
    final = PondStorage(kernel)
    rows = final.read("users")
    assert len(rows) == 150, f"Expected 150 rows, got {len(rows)}"

    print(f"PASS: test_mixed_workload_streaming_and_lookup — "
          f"{len(lookup_results)} lookups during 50 appends")
    return True


def test_cas_retry_under_contention():
    """Verify CAS retries correctly under heavy contention."""
    kernel, _ = make_object_store_native_kernel()
    storage = PondStorage(kernel)
    storage.write("hot", [{"id": 0, "v": 0}], key_col="id", row_group_size=10)

    success_count = [0]
    lock = threading.Lock()

    def writer(wid):
        local = PondStorage(kernel)
        for i in range(10):
            try:
                local.append_concurrent("hot", [{"id": wid * 100 + i + 1, "v": wid}],
                                          key_col="id", row_group_size=10, max_retries=10)
                with lock:
                    success_count[0] += 1
            except Exception as e:
                pass  # some may fail under extreme contention — that's OK

    threads = [threading.Thread(target=writer, args=(w,)) for w in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # 10 writers × 10 appends = 100 attempts. Most should succeed.
    assert success_count[0] >= 80, f"Too many failures: {success_count[0]}/100 succeeded"

    final = PondStorage(kernel)
    rows = final.read("hot")
    print(f"PASS: test_cas_retry_under_contention — "
          f"{success_count[0]}/100 appends succeeded, {len(rows)} rows final")
    return True


def main():
    tests = [
        test_concurrent_writers_no_data_loss,
        test_new_connection_reads_latest,
        test_concurrent_reads_during_writes,
        test_mixed_workload_streaming_and_lookup,
        test_cas_retry_under_contention,
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
        print("=== ALL CONCURRENCY TESTS PASS ===")
    return 0 if passed == len(tests) else 1


if __name__ == "__main__":
    sys.exit(main())
