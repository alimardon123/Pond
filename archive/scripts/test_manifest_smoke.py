"""Smoke test for collection_manifest.py — verify encode/decode round-trip."""
import os, sys, tempfile

# Make bindings/python/core and physical_structures importable
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "bindings/python/core"))
sys.path.insert(0, os.path.join(HERE, "..", "bindings/python/sdk"))
sys.path.insert(0, os.path.join(HERE, "..", "bindings/python/sdk", "extensions", "physical_structures"))
# Legacy extensions (pruning.ZoneMap) were moved to archive during the
# ProllyTree cleanup. Keep them importable for the zone-map bridge test.
sys.path.insert(0, os.path.join(HERE, "..", "archive", "legacy-extensions"))

from kernel import PondMinimal
from collection_manifest import (
    CollectionManifest, RowGroupEntry, ColumnStatsEntry, ColumnChunkEntry,
    STORAGE_WHOLE_BLOB, STORAGE_COLUMN_CHUNKS, STORAGE_ENCODED,
    VALUE_TYPE_INT64, VALUE_TYPE_FLOAT64, VALUE_TYPE_STRING,
    build_manifest_from_zone_map,
)
from pruning import ZoneMap


def make_kernel():
    tmp = tempfile.mkdtemp(prefix="pond-manifest-test-")
    return PondMinimal(tmp), tmp


def test_round_trip_basic():
    """Encode a manifest, decode it, verify all fields survived."""
    kernel, tmp = make_kernel()
    try:
        m = CollectionManifest(kernel)
        m.set_schema(
            columns=[("id", VALUE_TYPE_INT64),
                     ("name", VALUE_TYPE_STRING),
                     ("score", VALUE_TYPE_FLOAT64)],
            key_col="id",
            row_group_size=1000,
            chunk_size=100,
        )

        # Row group 1
        rg1 = RowGroupEntry(
            key="rg/999",
            blob_hash="a" * 64,
            n_rows=1000,
            storage_mode=STORAGE_WHOLE_BLOB,
        )
        rg1.columns = [
            ColumnStatsEntry("id", VALUE_TYPE_INT64, 0, 999, 0),
            ColumnStatsEntry("name", VALUE_TYPE_STRING, "alice", "zoe", 0),
            ColumnStatsEntry("score", VALUE_TYPE_FLOAT64, 0.5, 99.5, 5),
        ]
        m.add_row_group(rg1)

        # Row group 2 with chunks
        rg2 = RowGroupEntry(
            key="rg/1999",
            blob_hash="b" * 64,
            n_rows=1000,
            storage_mode=STORAGE_COLUMN_CHUNKS,
        )
        rg2.columns = [
            ColumnStatsEntry("id", VALUE_TYPE_INT64, 1000, 1999, 0, [
                ColumnChunkEntry(blob_hash="c" * 64, min=1000, max=1099, null_count=0),
                ColumnChunkEntry(blob_hash="d" * 64, min=1100, max=1199, null_count=0),
            ]),
            ColumnStatsEntry("name", VALUE_TYPE_STRING, "alice", "zoe", 0, [
                ColumnChunkEntry(blob_hash="e" * 64, min="alice", max="mike", null_count=0),
                ColumnChunkEntry(blob_hash="f" * 64, min="nina", max="zoe", null_count=0),
            ]),
            ColumnStatsEntry("score", VALUE_TYPE_FLOAT64, 0.5, 99.5, 5, [
                ColumnChunkEntry(blob_hash="1" * 64, min=0.5, max=49.5, null_count=2),
                ColumnChunkEntry(blob_hash="2" * 64, min=50.0, max=99.5, null_count=3),
            ]),
        ]
        m.add_row_group(rg2)

        # Commit + reload
        h = m.commit()
        loaded = CollectionManifest.load(kernel, h)

        assert loaded.column_names == ["id", "name", "score"], \
            f"column_names mismatch: {loaded.column_names}"
        assert loaded.key_col == "id"
        assert loaded.row_group_size == 1000
        assert loaded.chunk_size == 100
        assert len(loaded.row_groups) == 2

        rg1_back = loaded.row_groups[0]
        assert rg1_back.key == "rg/999"
        assert rg1_back.blob_hash == "a" * 64
        assert rg1_back.n_rows == 1000
        assert rg1_back.storage_mode == STORAGE_WHOLE_BLOB

        # Check column stats
        id_col = rg1_back.get_column("id")
        assert id_col is not None
        assert id_col.min == 0
        assert id_col.max == 999
        assert id_col.null_count == 0
        assert id_col.value_type == VALUE_TYPE_INT64

        score_col = rg1_back.get_column("score")
        assert score_col.min == 0.5
        assert score_col.max == 99.5
        assert score_col.null_count == 5

        # Check chunk-level stats on rg2
        rg2_back = loaded.row_groups[1]
        id_col2 = rg2_back.get_column("id")
        assert len(id_col2.chunks) == 2
        assert id_col2.chunks[0].min == 1000
        assert id_col2.chunks[0].max == 1099
        assert id_col2.chunks[1].blob_hash == "d" * 64

        name_col2 = rg2_back.get_column("name")
        assert name_col2.chunks[0].min == "alice"
        assert name_col2.chunks[0].max == "mike"

        print("PASS: test_round_trip_basic")
        return True
    finally:
        import shutil
        shutil.rmtree(tmp, ignore_errors=True)


def test_pruning():
    """Verify manifest-based pruning works as expected."""
    kernel, tmp = make_kernel()
    try:
        m = CollectionManifest(kernel)
        m.set_schema(
            columns=[("age", VALUE_TYPE_INT64), ("region", VALUE_TYPE_STRING)],
            key_col="age",
            row_group_size=100,
            chunk_size=10,
        )

        # 3 row groups: ages 0-99, 100-199, 200-299
        for i, (lo, hi) in enumerate([(0, 99), (100, 199), (200, 299)]):
            rg = RowGroupEntry(
                key=f"rg/{hi}",
                blob_hash=f"{i:064d}",
                n_rows=100,
                storage_mode=STORAGE_WHOLE_BLOB,
            )
            rg.columns = [
                ColumnStatsEntry("age", VALUE_TYPE_INT64, lo, hi, 0),
                ColumnStatsEntry("region", VALUE_TYPE_STRING, "ASIA", "US", 0),
            ]
            m.add_row_group(rg)

        h = m.commit()
        loaded = CollectionManifest.load(kernel, h)

        # Predicate age > 150 should skip rg/99 (max=99) and yield rg/199, rg/299
        surviving = list(loaded.scan_with_pruning([("age", ">", 150)]))
        assert len(surviving) == 2, f"expected 2 surviving, got {len(surviving)}"
        assert surviving[0].key == "rg/199"
        assert surviving[1].key == "rg/299"

        # Predicate age = 50 should match only rg/99
        surviving = list(loaded.scan_with_pruning([("age", "=", 50)]))
        assert len(surviving) == 1
        assert surviving[0].key == "rg/99"

        # Predicate age > 500 should prune all
        surviving = list(loaded.scan_with_pruning([("age", ">", 500)]))
        assert len(surviving) == 0

        # No predicate = yield all
        surviving = list(loaded.scan_with_pruning())
        assert len(surviving) == 3

        print("PASS: test_pruning")
        return True
    finally:
        import shutil
        shutil.rmtree(tmp, ignore_errors=True)


def test_build_from_zone_map():
    """Verify the bridge function works with existing ZoneMap."""
    kernel, tmp = make_kernel()
    try:
        zm = ZoneMap(row_count=100)
        zm.min = {"age": 0, "region": "ASIA"}
        zm.max = {"age": 99, "region": "US"}
        zm.null_count = {"age": 0, "region": 0}

        rg = build_manifest_from_zone_map(
            kernel=kernel,
            row_group_key="rg/99",
            data_blob_hash="a" * 64,
            n_rows=100,
            zone_map=zm,
            storage_mode=STORAGE_WHOLE_BLOB,
        )

        assert rg.key == "rg/99"
        assert rg.blob_hash == "a" * 64
        assert rg.n_rows == 100
        assert len(rg.columns) == 2

        age_col = rg.get_column("age")
        assert age_col.min == 0
        assert age_col.max == 99
        assert age_col.value_type == VALUE_TYPE_INT64

        region_col = rg.get_column("region")
        assert region_col.min == "ASIA"
        assert region_col.max == "US"
        assert region_col.value_type == VALUE_TYPE_STRING

        # Wrap in a manifest and commit
        m = CollectionManifest(kernel)
        m.set_schema(
            columns=[("age", VALUE_TYPE_INT64), ("region", VALUE_TYPE_STRING)],
            key_col="age", row_group_size=100, chunk_size=10,
        )
        m.add_row_group(rg)
        h = m.commit()

        # Reload and verify
        loaded = CollectionManifest.load(kernel, h)
        assert len(loaded.row_groups) == 1
        assert loaded.row_groups[0].get_column("age").min == 0

        print("PASS: test_build_from_zone_map")
        return True
    finally:
        import shutil
        shutil.rmtree(tmp, ignore_errors=True)


def test_size_estimate():
    """Print the manifest size for a realistic workload."""
    kernel, tmp = make_kernel()
    try:
        m = CollectionManifest(kernel)
        m.set_schema(
            columns=[("id", VALUE_TYPE_INT64),
                     ("age", VALUE_TYPE_INT64),
                     ("region", VALUE_TYPE_STRING),
                     ("score", VALUE_TYPE_FLOAT64),
                     ("name", VALUE_TYPE_STRING)],
            key_col="id", row_group_size=10_000, chunk_size=1000,
        )

        # 100 row groups, 5 columns, 10 chunks per column
        for rg_idx in range(100):
            lo = rg_idx * 10_000
            hi = lo + 9999
            rg = RowGroupEntry(
                key=f"rg/{hi}",
                blob_hash=f"{rg_idx:064d}",
                n_rows=10_000,
                storage_mode=STORAGE_COLUMN_CHUNKS,
            )
            for col_name, vtype in [("id", VALUE_TYPE_INT64),
                                      ("age", VALUE_TYPE_INT64),
                                      ("region", VALUE_TYPE_STRING),
                                      ("score", VALUE_TYPE_FLOAT64),
                                      ("name", VALUE_TYPE_STRING)]:
                col = ColumnStatsEntry(col_name, vtype, lo, hi, 0)
                for chunk_idx in range(10):
                    chunk_lo = lo + chunk_idx * 1000
                    chunk_hi = chunk_lo + 999
                    if vtype == VALUE_TYPE_STRING:
                        chunk_lo = f"v{chunk_lo}"
                        chunk_hi = f"v{chunk_hi}"
                    col.chunks.append(ColumnChunkEntry(
                        blob_hash=f"{rg_idx:032d}{chunk_idx:032d}",
                        min=chunk_lo, max=chunk_hi, null_count=0,
                        encoding=0,
                    ))
                rg.columns.append(col)
            m.add_row_group(rg)

        h = m.commit()
        import os
        blob_path = kernel._blob_path(h)
        manifest_size = os.path.getsize(blob_path)

        print(f"\nManifest size for 100 row groups × 5 cols × 10 chunks: "
              f"{manifest_size:,} bytes ({manifest_size/1024:.1f} KB)")
        print(f"  Per row group: {manifest_size/100:.0f} bytes")
        print(f"  → ONE S3 fetch, well under 1 MB")

        assert manifest_size < 1_000_000, "manifest should be < 1MB for this workload"
        return True
    finally:
        import shutil
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    ok1 = test_round_trip_basic()
    ok2 = test_pruning()
    ok3 = test_build_from_zone_map()
    ok4 = test_size_estimate()
    if all([ok1, ok2, ok3, ok4]):
        print("\n=== ALL MANIFEST TESTS PASS ===")
        sys.exit(0)
    else:
        print("\n=== SOME TESTS FAILED ===")
        sys.exit(1)
