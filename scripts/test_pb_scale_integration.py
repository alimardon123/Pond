"""
PB-scale integration test — proves the stats tree is wired in and provides
O(log N) reads at >25K row groups.

Builds a 30,000-row-group collection (above the FLAT_MANIFEST_MAX_ROW_GROUPS
threshold), then verifies:
  1. The manifest HAS a stats_tree_root (not None)
  2. The manifest blob is SMALL (~200 bytes, not ~5MB)
  3. point_lookup is O(log N) — only a few tree node GETs, not 30K
  4. scan_with_pruning with a predicate prunes correctly via the tree
"""
import os, sys, tempfile, shutil

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "pond-core"))
sys.path.insert(0, os.path.join(HERE, "..", "pond-sdk"))
sys.path.insert(0, os.path.join(HERE, "..", "pond-sdk", "extensions", "physical_structures"))

from object_store_native_kernel import make_object_store_native_kernel
from unified_storage import UnifiedStorage
from collection_manifest import CollectionManifest
from stats_tree import FLAT_MANIFEST_MAX_ROW_GROUPS, TARGET_LEAF_ENTRIES


def test_pb_scale_manifest_uses_stats_tree():
    """Verify that >25K row groups triggers stats tree creation."""
    kernel, store = make_object_store_native_kernel()
    storage = UnifiedStorage(kernel)

    # Build 30,000 row groups (above the 25K threshold).
    # Each row group has 10 rows, so 300K total rows.
    # Use a simple schema to keep memory usage reasonable.
    n_groups = 30_000
    rows_per_group = 10
    n_rows = n_groups * rows_per_group

    print(f"\nBuilding {n_groups:,} row groups ({n_rows:,} rows)...")
    print(f"  (FLAT_MANIFEST_MAX_ROW_GROUPS = {FLAT_MANIFEST_MAX_ROW_GROUPS:,})")

    # Build rows in chunks to avoid OOM
    rows = []
    for i in range(n_rows):
        rows.append({"id": i, "val": i % 1000})

    # Write via UnifiedStorage
    storage.write("pb_test", rows, key_col="id", row_group_size=rows_per_group)

    # Load the manifest
    manifest = storage._load_manifest("pb_test")
    assert manifest is not None, "manifest not built"

    print(f"\nManifest stats:")
    print(f"  stats_tree_root: {manifest.stats_tree_root[:16] if manifest.stats_tree_root else None}...")
    print(f"  n_row_groups (inline): {len(manifest.row_groups)}")

    # Verify stats_tree_root is set (PB-scale path active)
    assert manifest.stats_tree_root is not None, \
        "stats_tree_root should be set for >25K row groups"

    # Verify the manifest blob is SMALL (just schema + stats_tree_root)
    manifest_hash = kernel.resolve("collections/pb_test/manifest")
    manifest_blob = kernel.read_blob(manifest_hash)
    print(f"  manifest blob size: {len(manifest_blob):,} bytes")
    # Should be < 1KB (schema + sort order + 32-byte stats_tree_root hash)
    # NOT the ~5MB it would be if all 30K row groups were inline
    assert len(manifest_blob) < 5_000, \
        f"manifest blob should be < 5KB at PB scale, got {len(manifest_blob):,}"

    print("PASS: test_pb_scale_manifest_uses_stats_tree")
    return True


def test_pb_scale_point_lookup_is_o_log_n():
    """Verify point_lookup at PB scale is O(log N), not O(N).

    With 30K row groups and TARGET_LEAF_ENTRIES=64:
      - Tree depth = ceil(log_64(30000)) = 3 levels
      - Cold point lookup = 3 tree node GETs + 1 data blob GET = 4 GETs
      - (Plus 2 ref GETs for the object-store-native kernel = 6 total cold)

    Without the stats tree, point_lookup would be O(N) = 30K iterations
    in memory, plus 30K data blob reads if it naively fetched them.
    """
    # Patch the threshold BEFORE importing UnifiedStorage (so the lazy
    # import inside _build_manifest picks up the patched value)
    import stats_tree as _st
    original_threshold = _st.FLAT_MANIFEST_MAX_ROW_GROUPS
    _st.FLAT_MANIFEST_MAX_ROW_GROUPS = 1_000  # trigger at 1K groups

    kernel, store = make_object_store_native_kernel()
    storage = UnifiedStorage(kernel)

    n_groups = 5_000  # above the lowered threshold of 1K
    rows_per_group = 10
    n_rows = n_groups * rows_per_group

    print(f"\nBuilding {n_groups:,} row groups ({n_rows:,} rows)...")
    print(f"  (threshold lowered to 1,000 for faster test)")

    rows = [{"id": i, "val": i % 100} for i in range(n_rows)]
    storage.write("pb_lookup", rows, key_col="id", row_group_size=rows_per_group)

    manifest = storage._load_manifest("pb_lookup")
    assert manifest.stats_tree_root is not None, \
        "stats_tree_root should be set (threshold lowered to 1K)"

    # Cold point lookup — count GETs
    kernel.invalidate_root_cache()
    storage._manifest_cache.clear()
    kernel.reset_stats()

    # Use key="9" — should resolve to first row group
    row = storage.point_lookup("pb_lookup", key="9")
    assert row is not None

    data_gets = kernel.stats["reads"]
    ref_gets = kernel.stats["ref_reads"]
    total_gets = data_gets + ref_gets

    print(f"\n  Cold point lookup at {n_groups:,} row groups:")
    print(f"    Data GETs: {data_gets}")
    print(f"    Ref GETs:  {ref_gets}")
    print(f"    Total:     {total_gets}")

    # The KEY assertion: total GETs should be SMALL (O(log N)),
    # not O(N). For 5000 row groups, we expect < 15 GETs.
    assert total_gets < 15, \
        f"point_lookup should be O(log N) < 15 GETs, got {total_gets}"
    # And specifically NOT 5000+ (which would indicate linear scan)
    assert total_gets < n_groups, \
        f"point_lookup must not be O(N) — got {total_gets} GETs for {n_groups} row groups"

    print(f"  ✓ O(log N) confirmed: {total_gets} GETs for {n_groups:,} row groups")

    # Restore the threshold
    _st.FLAT_MANIFEST_MAX_ROW_GROUPS = original_threshold

    print("PASS: test_pb_scale_point_lookup_is_o_log_n")
    return True


def test_pb_scale_pruned_read():
    """Verify predicate-pruned read at PB scale uses the stats tree."""
    import stats_tree as _st
    original_threshold = _st.FLAT_MANIFEST_MAX_ROW_GROUPS
    _st.FLAT_MANIFEST_MAX_ROW_GROUPS = 1_000

    kernel, store = make_object_store_native_kernel()
    storage = UnifiedStorage(kernel)

    n_groups = 2_000
    rows_per_group = 10
    n_rows = n_groups * rows_per_group

    print(f"\nBuilding {n_groups:,} row groups ({n_rows:,} rows)...")

    # Each row group has val range [rg_idx*10, rg_idx*10+9]
    # Predicate val > 19000 → only the last ~100 row groups survive
    rows = [{"id": i, "val": i} for i in range(n_rows)]
    storage.write("pb_pruned", rows, key_col="id", row_group_size=rows_per_group)

    manifest = storage._load_manifest("pb_pruned")
    assert manifest.stats_tree_root is not None

    # Cold pruned read — count GETs
    kernel.invalidate_root_cache()
    storage._manifest_cache.clear()
    kernel.reset_stats()

    # val > 19500 → only ~50 row groups (of 2000) survive
    result = storage.read("pb_pruned", predicates=[("val", ">", 19500)])

    data_gets = kernel.stats["reads"]
    ref_gets = kernel.stats["ref_reads"]
    total_gets = data_gets + ref_gets

    print(f"\n  Cold pruned read (val > 19500, ~2.5% selectivity):")
    print(f"    Data GETs: {data_gets}")
    print(f"    Ref GETs:  {ref_gets}")
    print(f"    Total:     {total_gets}")
    print(f"    Rows returned: {len(result)}")

    # Should fetch:
    #   2 ref + 1 manifest + ~2 tree nodes (internal + leaf for surviving range)
    #   + ~50 surviving data blobs
    # = ~55 GETs
    # Without stats tree: would be 2000+ GETs (scan all row groups)
    assert total_gets < 100, \
        f"pruned read should fetch < 100 blobs, got {total_gets}"
    assert total_gets < n_groups, \
        f"pruned read must not scan all {n_groups} row groups — got {total_gets} GETs"

    print(f"  ✓ Stats tree pruned correctly: {total_gets} GETs (vs {n_groups} without tree)")

    # Verify the result is correct
    assert len(result) > 0
    assert all(r["val"] > 19500 for r in result), \
        "all returned rows should match the predicate"

    # Restore threshold
    _st.FLAT_MANIFEST_MAX_ROW_GROUPS = original_threshold

    print("PASS: test_pb_scale_pruned_read")
    return True


if __name__ == "__main__":
    ok1 = test_pb_scale_manifest_uses_stats_tree()
    ok2 = test_pb_scale_point_lookup_is_o_log_n()
    ok3 = test_pb_scale_pruned_read()
    if all([ok1, ok2, ok3]):
        print("\n=== ALL PB-SCALE INTEGRATION TESTS PASS ===")
        sys.exit(0)
    else:
        print("\n=== SOME TESTS FAILED ===")
        sys.exit(1)
