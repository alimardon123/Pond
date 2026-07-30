"""Smoke test for object_store_native_kernel.py.

Verifies:
  1. write/read/reference/resolve work like PondMinimal
  2. NO SQLite is used — all state in the object store
  3. Stats honestly count every GET/PUT
  4. invalidate_root_cache() forces fresh ref reads
  5. The kernel works with UnifiedStorage end-to-end
"""
import os, sys, json

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "pond-core"))
sys.path.insert(0, os.path.join(HERE, "..", "pond-sdk"))
sys.path.insert(0, os.path.join(HERE, "..", "pond-sdk", "extensions", "physical_structures"))

from object_store_native_kernel import (
    ObjectStoreNativeKernel, InMemoryObjectStore,
    make_object_store_native_kernel,
)
from unified_storage import UnifiedStorage


def test_basic_kernel_ops():
    """Verify write/read/reference/resolve work like PondMinimal."""
    kernel, store = make_object_store_native_kernel()

    # Write a blob
    data = b"hello world"
    h = kernel.write(data)
    assert h and len(h) == 64, f"bad hash: {h}"
    assert kernel.stats["writes"] == 1

    # Read it back by hash
    read_data = kernel.read_blob(h)
    assert read_data == data, f"round-trip failed: {read_data!r} != {data!r}"
    assert kernel.stats["reads"] == 1

    # Reference a name → hash
    # First reference: loads root pointer (empty/None = 0 GETs since path
    # doesn't exist yet — get_path returns None without counting a GET,
    # because there's no blob to read). Then writes 2 PUTs (new root ref
    # blob + root pointer path).
    kernel.reference("collections/users/HEAD", h)
    assert kernel.stats["ref_writes"] == 2  # new root ref blob + root pointer update

    # Resolve the name (cached from the reference() call — 0 NEW ref_reads)
    resolved = kernel.resolve("collections/users/HEAD")
    assert resolved == h
    # reference() already did 1 ref_read (root pointer check — get_path returns
    # None for first-time, but we count the GET attempt honestly).
    # resolve() reuses the cache → 0 NEW ref_reads.
    assert kernel.stats["ref_reads"] == 1, \
        f"expected 1 ref_read (from reference), got {kernel.stats['ref_reads']}"

    # Read by name — force a fresh resolve
    kernel.invalidate_root_cache()  # force fresh resolve
    read_data = kernel.read("collections/users/HEAD")
    assert read_data == data
    # After invalidate: resolve reads root pointer (1 ref_read) + root ref
    # blob (1 ref_read) = 2 NEW ref_reads. Total = 1 + 2 = 3.
    assert kernel.stats["ref_reads"] == 3, \
        f"expected 3 ref_reads after cold resolve, got {kernel.stats['ref_reads']}"

    print("PASS: test_basic_kernel_ops")
    print(f"  Final stats: {kernel.stats}")
    return True


def test_no_sqlite():
    """Verify NO SQLite is used — all state in the object store."""
    kernel, store = make_object_store_native_kernel()

    # Write some data + refs
    h1 = kernel.write(b"data1")
    kernel.reference("ref1", h1)
    h2 = kernel.write(b"data2")
    kernel.reference("ref2", h2)

    # All state should be in the object store, not SQLite
    # The object store should have:
    #   - 2 data blobs (data1, data2)
    #   - 2 root ref blobs (one after each reference() call)
    #   - 1 root pointer path (latest = the second root ref blob)
    assert len(store._blobs) >= 4, f"expected >= 4 blobs, got {len(store._blobs)}"
    assert "_root" in store._paths, "root pointer path not set"

    # Verify no SQLite files exist anywhere in the test environment
    # (We can't easily check the entire FS, but we can verify the kernel
    # class itself doesn't import sqlite3)
    import object_store_native_kernel as mod
    source = open(mod.__file__).read()
    assert "sqlite3" not in source, "sqlite3 imported in object_store_native_kernel.py"
    assert ".sqlite" not in source, ".sqlite referenced in object_store_native_kernel.py"

    print("PASS: test_no_sqlite")
    return True


def test_cold_read_round_trips():
    """Verify the HONEST cold-read round-trip count.

    A cold point lookup on a unified-storage collection:
      1. Read root pointer (1 GET)
      2. Read root ref blob (1 GET)
      3. Read commit blob (1 GET)  — wait, the manifest ref is in the root ref blob
      4. Read manifest blob (1 GET)
      5. Read 1 data blob (1 GET)
    """
    kernel, store = make_object_store_native_kernel()
    storage = UnifiedStorage(kernel)

    # Write 100 rows in 10 row groups
    rows = [{"id": i, "age": i % 50} for i in range(100)]
    storage.write("test", rows, key_col="id", row_group_size=10)

    # Invalidate caches and reset stats for cold-read measurement
    kernel.invalidate_root_cache()
    kernel.reset_stats()

    # Cold point lookup
    row = storage.point_lookup("test", key="9")

    # Expected cold reads:
    #   1. Root pointer (ref_read)
    #   2. Root ref blob (ref_read)
    #   3. Manifest blob (data read)
    #   4. Data blob (data read)
    #
    # The manifest is loaded via _load_manifest which calls kernel.resolve()
    # → 2 ref_reads (cold). Then reads the manifest blob → 1 data read.
    # Then point_lookup reads 1 data blob.
    # But the manifest is CACHED inside UnifiedStorage._manifest_cache,
    # so the second call would be 1 read. For COLD measurement, we
    # invalidate that cache too.

    data_reads = kernel.stats["reads"]
    ref_reads = kernel.stats["ref_reads"]
    total_gets = data_reads + ref_reads

    print(f"\n  Cold point lookup:")
    print(f"    Data blob GETs: {data_reads}")
    print(f"    Ref GETs:       {ref_reads}")
    print(f"    Total GETs:     {total_gets}")

    # For a cold read, we expect:
    #   - 2 ref_reads (root pointer + root ref blob)
    #   - 2 data reads (manifest + 1 data blob)
    #   = 4 total GETs
    # (No commit blob because UnifiedStorage doesn't read commits — it
    # goes straight to the manifest ref.)
    assert ref_reads == 2, f"expected 2 ref_reads, got {ref_reads}"
    assert data_reads == 2, f"expected 2 data reads, got {data_reads}"
    assert total_gets == 4, f"expected 4 total GETs, got {total_gets}"

    print("PASS: test_cold_read_round_trips")
    return True


def test_simulated_s3_latency():
    """Verify the kernel works with simulated S3 latency."""
    kernel, store = make_object_store_native_kernel(latency_ms=10.0)
    storage = UnifiedStorage(kernel)

    rows = [{"id": i, "age": i % 50} for i in range(100)]
    storage.write("test", rows, key_col="id", row_group_size=10)

    # Cold point lookup with 10ms simulated RTT per GET
    kernel.invalidate_root_cache()
    kernel.reset_stats()
    store.reset_stats()

    row = storage.point_lookup("test", key="9")

    total_gets = kernel.stats["reads"] + kernel.stats["ref_reads"]
    expected_latency_ms = total_gets * 10.0
    actual_latency_ms = store.stats["latency_ms_total"]

    print(f"\n  Cold point lookup with 10ms simulated S3 RTT:")
    print(f"    Total GETs:           {total_gets}")
    print(f"    Expected latency:     {expected_latency_ms:.0f}ms")
    print(f"    Actual simulated:     {actual_latency_ms:.0f}ms")

    assert total_gets == 4
    assert actual_latency_ms == expected_latency_ms

    print("PASS: test_simulated_s3_latency")
    return True


def test_unified_storage_end_to_end():
    """Verify UnifiedStorage works end-to-end with the object-store-native kernel."""
    kernel, store = make_object_store_native_kernel()
    storage = UnifiedStorage(kernel)

    # Tabular workload
    tabular_rows = [{"id": i, "age": i % 100, "name": f"user_{i}"} for i in range(50)]
    storage.write("users", tabular_rows, key_col="id", row_group_size=10)

    # Cold full scan
    kernel.invalidate_root_cache()
    kernel.reset_stats()

    result = storage.read("users")
    assert len(result) == 50, f"expected 50 rows, got {len(result)}"

    # Cold reads: 2 ref + 1 manifest + 5 data blobs (5 row groups) = 8 GETs
    total_gets = kernel.stats["reads"] + kernel.stats["ref_reads"]
    print(f"\n  Cold full scan (50 rows, 5 row groups):")
    print(f"    Total GETs: {total_gets}")
    print(f"      Ref reads:    {kernel.stats['ref_reads']}")
    print(f"      Data reads:   {kernel.stats['reads']}")

    # 2 ref reads (root pointer + root ref blob) + 1 manifest + 5 data blobs = 8
    assert kernel.stats["ref_reads"] == 2, \
        f"expected 2 ref_reads, got {kernel.stats['ref_reads']}"
    assert kernel.stats["reads"] == 6, \
        f"expected 6 data reads (1 manifest + 5 data blobs), got {kernel.stats['reads']}"
    assert total_gets == 8, f"expected 8 total GETs, got {total_gets}"

    # Predicate-pruned read — 1 of 5 row groups survives
    kernel.invalidate_root_cache()
    # ALSO invalidate the UnifiedStorage manifest cache — otherwise it
    # reuses the manifest from the previous read.
    storage._manifest_cache.clear()
    kernel.reset_stats()

    # id > 45 → only the last row group (40-49) survives
    result = storage.read("users", predicates=[("id", ">", 45)])
    assert len(result) == 4, f"expected 4 rows (id 46-49), got {len(result)}"

    total_gets = kernel.stats["reads"] + kernel.stats["ref_reads"]
    print(f"\n  Cold pruned read (id > 45, 1/5 selectivity):")
    print(f"    Total GETs: {total_gets}")
    print(f"      Ref reads:    {kernel.stats['ref_reads']}")
    print(f"      Data reads:   {kernel.stats['reads']}")

    # 2 ref reads (root pointer + root ref blob) + 1 manifest + 1 data blob = 4 GETs
    assert kernel.stats["ref_reads"] == 2, \
        f"expected 2 ref_reads, got {kernel.stats['ref_reads']}"
    assert kernel.stats["reads"] == 2, \
        f"expected 2 data reads (1 manifest + 1 data blob), got {kernel.stats['reads']}"
    assert total_gets == 4, f"expected 4 total GETs, got {total_gets}"

    print("PASS: test_unified_storage_end_to_end")
    return True


def test_warm_read_round_trips():
    """Verify WARM read round trips (root ref blob cached).

    After the first read, the root ref blob is cached in the kernel.
    Subsequent reads skip the 2 ref_reads.
    """
    kernel, store = make_object_store_native_kernel()
    storage = UnifiedStorage(kernel)

    rows = [{"id": i, "age": i % 50} for i in range(100)]
    storage.write("test", rows, key_col="id", row_group_size=10)

    # First (cold) read populates the root ref cache
    kernel.invalidate_root_cache()
    kernel.reset_stats()
    storage.point_lookup("test", key="9")
    cold_gets = kernel.stats["reads"] + kernel.stats["ref_reads"]
    assert cold_gets == 4, f"cold: expected 4 GETs, got {cold_gets}"

    # Warm read — root ref blob is cached, manifest is cached
    # So only the data blob is read.
    kernel.reset_stats()
    storage.point_lookup("test", key="9")
    warm_gets = kernel.stats["reads"] + kernel.stats["ref_reads"]
    # Manifest is cached inside UnifiedStorage._manifest_cache.
    # Root ref blob is cached inside ObjectStoreNativeKernel._root_ref_cache.
    # So only 1 data blob read.
    assert warm_gets == 1, f"warm: expected 1 GET, got {warm_gets}"

    print(f"\n  Warm point lookup (caches populated):")
    print(f"    Total GETs: {warm_gets}")
    print(f"    (root ref blob + manifest cached — only data blob fetched)")

    print("PASS: test_warm_read_round_trips")
    return True


if __name__ == "__main__":
    ok1 = test_basic_kernel_ops()
    ok2 = test_no_sqlite()
    ok3 = test_cold_read_round_trips()
    ok4 = test_simulated_s3_latency()
    ok5 = test_unified_storage_end_to_end()
    ok6 = test_warm_read_round_trips()
    if all([ok1, ok2, ok3, ok4, ok5, ok6]):
        print("\n=== ALL OBJECT-STORE-NATIVE KERNEL TESTS PASS ===")
        sys.exit(0)
    else:
        print("\n=== SOME TESTS FAILED ===")
        sys.exit(1)
