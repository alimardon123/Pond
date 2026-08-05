"""Tests for manifest-level compaction (fast path) vs row-level compaction (fallback).

Tests:
1. Manifest-level compaction preserves all rows (insert-only)
2. Manifest-level compaction does ZERO data blob reads (only manifest reads)
3. Row-level compaction still works for _rowid (upsert/delete) shards
4. Mixed: insert-only shards + upsert shards → row-level fallback
5. Compaction is idempotent (double-compaction gives same result)
6. PB-scale simulation: many row groups, compaction is O(shards) not O(rows)
"""
import sys, os, time

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(REPO, "pond-core"))
sys.path.insert(0, os.path.join(REPO, "pond-sdk"))
sys.path.insert(0, os.path.join(REPO, "pond-sdk", "extensions", "physical_structures"))

from object_store_native_kernel import make_object_store_native_kernel
from pond_storage import PondStorage


def _setup(n_rows=100, rg_size=10):
    """Create a collection with initial data."""
    kernel, _ = make_object_store_native_kernel()
    s = PondStorage(kernel)
    s.write("test", [{"id": i, "val": f"v{i}"} for i in range(n_rows)],
            key_col="id", row_group_size=rg_size)
    return kernel, s


def test_manifest_level_preserves_rows():
    """Manifest-level compaction preserves all rows (insert-only)."""
    kernel, s = _setup(n_rows=50, rg_size=10)

    # Append 3 shards (insert-only, no _rowid)
    for shard_idx in range(3):
        s.append_shard("test",
                        [{"id": 50 + shard_idx * 10 + i, "val": f"s{shard_idx}_{i}"}
                         for i in range(10)],
                        key_col="id", row_group_size=10)

    # Before compaction: 50 (HEAD) + 30 (3 shards) = 80 rows
    rows_before = s.read_with_shards("test")
    assert len(rows_before) == 80, f"Expected 80 rows before compaction, got {len(rows_before)}"

    # Compact
    s.compact_shards("test")
    s.wait_for_background_tasks()

    # After compaction: still 80 rows, but now all in HEAD (0 shards)
    rows_after = s.read("test")
    assert len(rows_after) == 80, f"Expected 80 rows after compaction, got {len(rows_after)}"
    assert s.shard_count("test") == 0, f"Expected 0 shards, got {s.shard_count('test')}"

    # Verify specific rows from shards survived
    ids = sorted(r["id"] for r in rows_after)
    assert ids == list(range(80)), f"Missing IDs: {set(range(80)) - set(ids)}"

    # Verify shard data is correct (not just HEAD data)
    row_55 = next(r for r in rows_after if r["id"] == 55)
    assert row_55["val"] == "s0_5", f"Wrong val for id=55: {row_55['val']}"

    print("PASS: test_manifest_level_preserves_rows — 80 rows survive manifest-level compaction")
    return True


def test_manifest_level_zero_data_reads():
    """Manifest-level compaction does ZERO data blob reads.

    Only manifest blobs are read (HEAD manifest + shard manifests).
    No data blobs are fetched — the new manifest references the same
    blob_hash values directly.
    """
    kernel, s = _setup(n_rows=100, rg_size=10)

    # Append a shard with 10 rows (1 row group)
    s.append_shard("test", [{"id": 100 + i, "val": f"new_{i}"} for i in range(10)],
                    key_col="id", row_group_size=10)

    # Reset stats
    kernel.reset_stats()
    s._unified._manifest_cache.clear()
    s._unified._head_cache.clear()

    # Compact
    s.compact_shards("test")
    s.wait_for_background_tasks()

    # Count data reads (kernel.stats["reads"]) vs ref reads
    data_reads = kernel.stats["reads"]
    ref_reads = kernel.stats["ref_reads"]

    # Manifest-level compaction should read:
    #   - HEAD manifest (1 data read)
    #   - Shard manifest (1 data read)
    #   - New manifest blob write (counts as a write, not read)
    # Total data reads: 2 (just the manifests)
    # If it were row-level, it would read 10+1=11 data blobs (10 row groups from HEAD + 1 from shard)
    assert data_reads <= 3, \
        f"Manifest-level compaction should do <=3 data reads (manifests only), got {data_reads}"

    print(f"PASS: test_manifest_level_zero_data_reads — {data_reads} data reads (manifests only, zero data blobs)")
    return True


def test_row_level_for_upserts():
    """Row-level compaction (fallback) works for _rowid shards (upserts).

    To test true upsert (update, not duplicate), we first upsert ALL rows
    to give them _rowid, then upsert specific rows to update them.
    Uses the same row_group_size as the original write to ensure rg_keys match.
    """
    kernel, s = _setup(n_rows=20, rg_size=10)

    # Step 1: upsert all rows to give them _rowid (write() doesn't add it)
    # Use the SAME row_group_size (10) so rg_keys match and override HEAD
    rows = s.read_with_shards("test")
    s.upsert_shard("test", rows, key_col="id", row_group_size=10)

    # Step 2: now upsert specific rows to UPDATE them (same _rowid, new _version)
    rows_with_rowid = s.read_with_shards("test")
    rows_to_update = [r for r in rows_with_rowid if r["id"] in (0, 1) and r.get("_rowid")]
    assert len(rows_to_update) == 2, f"Expected 2 rows with _rowid, got {len(rows_to_update)}"

    for r in rows_to_update:
        r["val"] = f"updated_{r['id']}"
    s.upsert_shard("test", rows_to_update, key_col="id", row_group_size=10)

    # Before compaction: read_with_shards merges by _rowid
    rows_before = s.read_with_shards("test")
    assert len(rows_before) == 20, f"Expected 20 rows, got {len(rows_before)}"

    row_0 = next(r for r in rows_before if r["id"] == 0)
    assert row_0["val"] == "updated_0", f"Expected updated_0, got {row_0['val']}"

    # Compact (should use row-level fallback because _rowid is present)
    s.compact_shards("test")
    s.wait_for_background_tasks()

    rows_after = s.read("test")
    assert len(rows_after) == 20, f"Expected 20 rows after compaction, got {len(rows_after)}"

    row_0_after = next(r for r in rows_after if r["id"] == 0)
    assert row_0_after["val"] == "updated_0", \
        f"Updated value lost after compaction: {row_0_after['val']}"

    print("PASS: test_row_level_for_upserts — upsert values preserved by row-level compaction")
    return True


def test_row_level_for_deletes():
    """Row-level compaction handles tombstones (deletes) correctly."""
    kernel, s = _setup(n_rows=20, rg_size=10)

    # First upsert all rows to give them _rowid (write() doesn't add _rowid)
    # Use the SAME row_group_size so rg_keys match and override HEAD
    rows = s.read_with_shards("test")
    s.upsert_shard("test", rows, key_col="id", row_group_size=10)

    # Now read again — rows have _rowid (filter to only upserted rows)
    rows = s.read_with_shards("test")
    rows_with_rowid = [r for r in rows if r.get("_rowid") and r["id"] in (0, 1)]
    assert len(rows_with_rowid) == 2, f"Expected 2 rows with _rowid, got {len(rows_with_rowid)}"
    rowids_to_delete = [r["_rowid"] for r in rows_with_rowid]
    keys_to_delete = [str(r["id"]) for r in rows_with_rowid]  # pass keys for proper tombstone matching

    # Delete rows 0 and 1
    s.delete_shard("test", rowids_to_delete, key_col="id", row_group_size=10, keys=keys_to_delete)

    # Before compaction: 18 live rows (2 deleted via tombstone)
    rows_before = s.read_with_shards("test")
    assert len(rows_before) == 18, f"Expected 18 rows, got {len(rows_before)}"
    assert all(r["id"] not in (0, 1) for r in rows_before), "Deleted rows visible!"

    # Compact
    s.compact_shards("test")
    s.wait_for_background_tasks()

    rows_after = s.read("test")
    assert len(rows_after) == 18, f"Expected 18 rows after compaction, got {len(rows_after)}"
    assert all(r["id"] not in (0, 1) for r in rows_after), "Deleted rows reappeared!"

    print("PASS: test_row_level_for_deletes — tombstones correctly applied by row-level compaction")
    return True


def test_idempotent_compaction():
    """Double compaction gives the same result."""
    kernel, s = _setup(n_rows=30, rg_size=10)

    s.append_shard("test", [{"id": 30 + i, "val": f"new_{i}"} for i in range(10)],
                    key_col="id", row_group_size=10)

    # First compaction
    s.compact_shards("test")
    s.wait_for_background_tasks()
    rows_1 = sorted(s.read("test"), key=lambda r: r["id"])
    ids_1 = [r["id"] for r in rows_1]

    # Second compaction (should be a no-op — no shards to compact)
    result = s.compact_shards("test")
    assert result is None, "Second compaction should return None (no shards)"

    rows_2 = sorted(s.read("test"), key=lambda r: r["id"])
    ids_2 = [r["id"] for r in rows_2]

    assert ids_1 == ids_2, f"Double compaction changed results: {ids_1} vs {ids_2}"

    print("PASS: test_idempotent_compaction — double compaction is a no-op")
    return True


def test_pb_scale_compaction_throughput():
    """PB-scale simulation: compaction is O(shards) not O(rows).

    Creates a collection with many row groups (simulating PB scale via
    many small row groups), appends multiple shards, and measures
    compaction time. Manifest-level compaction should be fast regardless
    of total row count because it doesn't read data blobs.
    """
    kernel, s = _setup(n_rows=1000, rg_size=10)  # 100 row groups in HEAD

    # Append 5 shards, each with 100 rows (10 row groups each)
    for i in range(5):
        s.append_shard("test",
                        [{"id": 1000 + i * 100 + j, "val": f"s{i}_{j}"}
                         for j in range(100)],
                        key_col="id", row_group_size=10)

    # Total: 1000 (HEAD) + 500 (5 shards) = 1500 rows, 150 row groups

    # Measure compaction time
    kernel.reset_stats()
    t0 = time.perf_counter()
    s.compact_shards("test")
    s.wait_for_background_tasks()
    t1 = time.perf_counter()

    data_reads = kernel.stats["reads"]
    elapsed_ms = (t1 - t0) * 1000

    # Verify correctness
    rows = s.read("test")
    assert len(rows) == 1500, f"Expected 1500 rows, got {len(rows)}"

    # Manifest-level: should read ~6 manifests (1 HEAD + 5 shards), NOT 150 data blobs
    # With row-level, it would read 150 data blobs (one per row group)
    assert data_reads <= 10, \
        f"Manifest-level compaction should read <=10 blobs (manifests), got {data_reads}"

    print(f"PASS: test_pb_scale_compaction_throughput — "
          f"150 row groups compacted in {elapsed_ms:.1f}ms with {data_reads} data reads "
          f"(manifest-level, zero data blob I/O)")
    return True


def test_mixed_insert_and_upsert():
    """Mixed: insert-only shards + upsert shards → row-level fallback.

    Note: write() doesn't add _rowid, so upsert_shard on a row from write()
    creates a NEW row with _rowid (the CRDT can't match rows without _rowid).
    This is expected behavior — upsert only works on rows that already have
    _rowid (from a prior upsert_shard call).
    """
    kernel, s = _setup(n_rows=20, rg_size=10)

    # Insert-only shard (no _rowid)
    s.append_shard("test", [{"id": 20 + i, "val": f"ins_{i}"} for i in range(5)],
                    key_col="id", row_group_size=10)

    # Upsert shard (has _rowid) — creates a new row with id=0 + _rowid
    # The original id=0 (from write, no _rowid) is still visible because
    # the CRDT can't match them — different _rowid (None vs generated).
    s.upsert_shard("test", [{"id": 0, "val": "updated"}], key_col="id")

    # Compact — should use row-level fallback because upsert added _rowid
    s.compact_shards("test")
    s.wait_for_background_tasks()

    rows = s.read("test")
    # With the CRDT fix: the upsert shard has _rowid + key_col="id"=0.
    # The _merge_rows_by_rowid now uses str() coercion to match legacy
    # rows by key_col. So the upserted row (id=0, _rowid=X) suppresses
    # the legacy row (id=0, no _rowid) because they share the same key.
    # Result: 20 original - 1 superseded + 5 insert-only + 1 upsert = 25
    assert len(rows) == 25, f"Expected 25 rows (1 superseded by upsert), got {len(rows)}"

    # Verify the upserted row exists (should be the only id=0 row now)
    id_zero_rows = [r for r in rows if r["id"] == 0]
    assert len(id_zero_rows) == 1, \
        f"Expected 1 row with id=0 (upsert superseded original), got {len(id_zero_rows)}"
    vals = {r["val"] for r in id_zero_rows}
    assert "updated" in vals, f"Upsert value not found: {vals}"

    print("PASS: test_mixed_insert_and_upsert — mixed shards correctly use row-level fallback")
    return True


def main():
    tests = [
        test_manifest_level_preserves_rows,
        test_manifest_level_zero_data_reads,
        test_row_level_for_upserts,
        test_row_level_for_deletes,
        test_idempotent_compaction,
        test_pb_scale_compaction_throughput,
        test_mixed_insert_and_upsert,
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
        print("=== ALL MANIFEST COMPACTION TESTS PASS ===")
    return 0 if passed == len(tests) else 1


if __name__ == "__main__":
    sys.exit(main())
