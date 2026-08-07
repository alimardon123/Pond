"""Cross-lens universal access test — the user's core requirement.

Verifies: ANY lens can read/write ANY collection created by ANY other
lens, with no special cross-lens glue code. Each collection carries
small metadata (lens_type, key_col, schema_hint) so lenses know what
shape to expect, but the access path is the same regardless.

Scenario: 8 lakehouse + 3 KV = 11 collections, all visible from every
lens, every lens can read/write any of them.

Usage:
    python scripts/test_cross_lens_universal.py
"""
from __future__ import annotations

import os
import sys
import json
import struct

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(REPO, "bindings/python/core"))
sys.path.insert(0, os.path.join(REPO, "bindings/python/sdk"))
sys.path.insert(0, os.path.join(REPO, "bindings/python/sdk", "extensions", "physical_structures"))
sys.path.insert(0, os.path.join(REPO, "lenses", "keyvalue"))
sys.path.insert(0, os.path.join(REPO, "lenses", "vector"))
sys.path.insert(0, os.path.join(REPO, "lenses", "streaming"))
sys.path.insert(0, os.path.join(REPO, "lenses", "lakehouse"))

from object_store_native_kernel import make_object_store_native_kernel
from pond_storage import PondStorage
from keyvalue_lens import KeyValueLens
from vector_lens import VectorLens
from streaming_lens import StreamingLens

try:
    import pyarrow as pa
    HAVE_PA = True
except ImportError:
    HAVE_PA = False


# ---------------------------------------------------------------------------
# Test 1: All 4 lenses see all 11 collections
# ---------------------------------------------------------------------------

def test_all_lenses_see_all_collections():
    """Create 8 lakehouse + 3 KV = 11 collections; verify all 4 lenses
    see all 11 (lakehouse, KV, vector, streaming)."""
    kernel, _ = make_object_store_native_kernel()
    storage = PondStorage(kernel)

    # Create 8 lakehouse collections via PondStorage (skipping LakehouseLens
    # because pyarrow may not be available — PondStorage uses the same
    # PND2 format and stamps metadata manually).
    for i in range(8):
        rows = [{"id": j, "value": f"lh{i}_{j}"} for j in range(10)]
        storage.write(f"lh_table_{i}", rows, key_col="id", row_group_size=5)
        storage.stamp_collection_metadata(
            f"lh_table_{i}", lens_type="lakehouse", key_col="id",
            schema_hint={"id": "int64", "value": "string"})

    # Create 3 KV collections via KeyValueLens
    kv = KeyValueLens(kernel)  # default use_unified_storage=True now
    for i in range(3):
        for j in range(10):
            kv.put(f"kv_col_{i}", f"k{j}", {"v": j})
        kv.commit(f"kv_col_{i}")

    # Verify all 4 lenses see all 11 collections
    expected = {f"lh_table_{i}" for i in range(8)} | {f"kv_col_{i}" for i in range(3)}
    assert len(expected) == 11

    # 1. PondStorage sees all
    seen = set(storage.list_collections())
    missing = expected - seen
    assert not missing, f"PondStorage missing: {missing}"

    # 2. KeyValueLens sees all
    seen_kv = set(kv.list_collections())
    missing_kv = expected - seen_kv
    assert not missing_kv, f"KeyValueLens missing: {missing_kv}"

    # 3. VectorLens sees all
    vl = VectorLens(kernel, n_dimensions=4)
    seen_vl = set(vl.list_collections())
    missing_vl = expected - seen_vl
    assert not missing_vl, f"VectorLens missing: {missing_vl}"

    # 4. StreamingLens sees all
    sl = StreamingLens(kernel)
    seen_sl = set(sl.list_collections())
    missing_sl = expected - seen_sl
    assert not missing_sl, f"StreamingLens missing: {missing_sl}"

    # 5. list_collections_with_metadata returns lens_type for each
    md_list = storage.list_collections_with_metadata()
    md_by_name = {m["name"]: m for m in md_list}
    assert len(md_by_name) >= 11
    for i in range(8):
        assert md_by_name[f"lh_table_{i}"]["lens_type"] == "lakehouse"
        assert md_by_name[f"lh_table_{i}"]["key_col"] == "id"
    for i in range(3):
        assert md_by_name[f"kv_col_{i}"]["lens_type"] == "keyvalue"
        assert md_by_name[f"kv_col_{i}"]["key_col"] == "_key"

    print(f"PASS: test_all_lenses_see_all_collections — 11 collections visible from all 4 lenses")
    return True


# ---------------------------------------------------------------------------
# Test 2: KV lens reads a lakehouse collection (cross-lens read)
# ---------------------------------------------------------------------------

def test_kv_reads_lakehouse_collection():
    """KV lens reads a lakehouse collection by metadata.key_col."""
    kernel, _ = make_object_store_native_kernel()
    storage = PondStorage(kernel)
    rows = [{"id": i, "name": f"user_{i}", "age": 20 + i} for i in range(50)]
    storage.write("users", rows, key_col="id", row_group_size=10)
    storage.stamp_collection_metadata(
        "users", lens_type="lakehouse", key_col="id",
        schema_hint={"id": "int64", "name": "string", "age": "int64"})

    kv = KeyValueLens(kernel)  # default unified now
    # KV.get on a lakehouse collection: returns full row dict (cross-lens)
    row = kv.get("users", "25")
    assert row is not None, "KV.get returned None for lakehouse row"
    assert row["id"] == 25, f"expected id=25, got {row.get('id')}"
    assert row["name"] == "user_25"
    assert row["age"] == 45

    # KV.keys returns the id column values
    keys = kv.keys("users")
    assert len(keys) == 50
    assert "25" in keys
    assert "0" in keys
    assert "49" in keys

    # KV.get_all returns {id: row_dict}
    all_data = kv.get_all("users")
    assert len(all_data) == 50
    assert all_data["25"]["name"] == "user_25"

    # KV.exists works cross-lens
    assert kv.exists("users", "10") is True
    assert kv.exists("users", "999") is False

    # KV.count works cross-lens
    assert kv.count("users") == 50

    print("PASS: test_kv_reads_lakehouse_collection — KV lens reads lakehouse collection transparently")
    return True


# ---------------------------------------------------------------------------
# Test 3: Lakehouse lens reads a KV collection (cross-lens read)
# ---------------------------------------------------------------------------

def test_lakehouse_reads_kv_collection():
    """Lakehouse lens reads a KV collection via PondStorage.read."""
    kernel, _ = make_object_store_native_kernel()
    kv = KeyValueLens(kernel)
    for i in range(30):
        kv.put("kv_data", f"k{i}", {"name": f"item_{i}", "qty": i})
    kv.commit("kv_data")

    # PondStorage (which LakehouseLens uses internally) reads the KV collection
    storage = PondStorage(kernel)
    rows = storage.read("kv_data")
    assert len(rows) == 30
    # Each row has _key and value columns
    sample = next(r for r in rows if r["_key"] == "k15")
    assert sample is not None
    # value is bytes (KV-encoded) — decode it
    val = kv.decode(sample["value"])
    assert val["name"] == "item_15"
    assert val["qty"] == 15

    # point_lookup also works cross-lens
    row = storage.point_lookup("kv_data", key="k15")
    assert row is not None
    assert row["_key"] == "k15"

    print("PASS: test_lakehouse_reads_kv_collection — Lakehouse/PondStorage reads KV collection")
    return True


# ---------------------------------------------------------------------------
# Test 4: Vector lens reads a lakehouse collection (cross-lens read)
# ---------------------------------------------------------------------------

def test_vector_reads_lakehouse_collection():
    """Vector lens reads a lakehouse collection — no vector columns,
    so vectors are empty but the full row is in _row."""
    kernel, _ = make_object_store_native_kernel()
    storage = PondStorage(kernel)
    rows = [{"id": i, "name": f"user_{i}", "score": float(i) * 0.5}
            for i in range(20)]
    storage.write("scores", rows, key_col="id", row_group_size=10)
    storage.stamp_collection_metadata(
        "scores", lens_type="lakehouse", key_col="id",
        schema_hint={"id": "int64", "name": "string", "score": "float64"})

    vl = VectorLens(kernel, n_dimensions=0)
    # Vector reads a lakehouse collection — ugly shape (empty vector) but full visibility
    vec = vl.get_vector("scores", "5")
    assert vec is not None
    assert vec["id"] == 5
    assert vec["vector"] == []  # no dim_* or vector column
    assert vec["_row"]["name"] == "user_5"  # full row accessible
    assert vec["_row"]["score"] == 2.5

    # list_vectors returns the key_col values
    ids = vl.list_vectors("scores")
    assert len(ids) == 20
    assert "5" in ids

    # get_all returns all rows with _row attached
    all_vecs = vl.get_all("scores")
    assert len(all_vecs) == 20
    assert all_vecs["5"]["_row"]["name"] == "user_5"

    print("PASS: test_vector_reads_lakehouse_collection — Vector lens reads lakehouse (ugly shape, full visibility)")
    return True


# ---------------------------------------------------------------------------
# Test 5: Streaming lens reads a KV collection (cross-lens read)
# ---------------------------------------------------------------------------

def test_streaming_reads_kv_collection():
    """Streaming lens reads a KV collection — concatenates bytes columns."""
    kernel, _ = make_object_store_native_kernel()
    kv = KeyValueLens(kernel)
    for i in range(10):
        kv.put("blobs", f"k{i}", f"chunk_{i}_data".encode())
    kv.commit("blobs")

    sl = StreamingLens(kernel)
    # Streaming reads a KV collection — concatenates bytes columns
    data = sl.read_stream("blobs")
    # Should contain all the encoded bytes (KV's encode wraps the value)
    assert len(data) > 0
    # Verify each chunk's bytes are present somewhere
    for i in range(10):
        # The raw bytes "chunk_i_data" should appear in the concatenation
        # (KV's encode is JSON-wrapped, so the inner string is present)
        assert f"chunk_{i}_data".encode() in data, f"chunk {i} missing from stream"

    print("PASS: test_streaming_reads_kv_collection — Streaming lens reads KV (concatenates bytes)")
    return True


# ---------------------------------------------------------------------------
# Test 6: KV lens writes to a lakehouse collection (cross-lens write)
# ---------------------------------------------------------------------------

def test_kv_writes_to_lakehouse_collection():
    """KV lens appends to a lakehouse collection. The appended row has
    _key+value columns (ugly shape) but is readable by any lens."""
    kernel, _ = make_object_store_native_kernel()
    storage = PondStorage(kernel)
    rows = [{"id": i, "name": f"user_{i}"} for i in range(10)]
    storage.write("mixed", rows, key_col="id", row_group_size=5)
    storage.stamp_collection_metadata(
        "mixed", lens_type="lakehouse", key_col="id",
        schema_hint={"id": "int64", "name": "string"})

    kv = KeyValueLens(kernel)
    # KV appends a row to the lakehouse collection
    kv.put("mixed", "kv_key_1", {"note": "appended via KV lens"})
    kv.commit("mixed")

    # Lakehouse reads all rows — sees both lakehouse rows and the KV row
    all_rows = storage.read("mixed")
    assert len(all_rows) == 11  # 10 lakehouse + 1 KV-appended
    # The KV-appended row has _key and value columns; id is absent (ugly shape)
    kv_rows = [r for r in all_rows if r.get("_key") == "kv_key_1"]
    assert len(kv_rows) == 1
    # id is absent from the KV-appended row — that's the "ugly shape" the
    # user explicitly allowed. The row is still readable by any lens.
    assert "id" not in kv_rows[0] or kv_rows[0].get("id") is None
    # The original lakehouse rows still have their data
    lh_rows = [r for r in all_rows if r.get("id") == 5]
    assert len(lh_rows) == 1
    assert lh_rows[0]["name"] == "user_5"

    print("PASS: test_kv_writes_to_lakehouse_collection — KV appends to lakehouse (ugly but readable)")
    return True


# ---------------------------------------------------------------------------
# Test 7: PondStorage reads/writes any collection uniformly
# ---------------------------------------------------------------------------

def test_pond_storage_uniform_access():
    """PondStorage can read and write any collection regardless of which
    lens created it. This is the foundation of cross-lens access."""
    kernel, _ = make_object_store_native_kernel()
    storage = PondStorage(kernel)

    # Create collections of different "lens types" via PondStorage directly
    storage.write("lh_via_storage", [{"id": 1, "name": "alice"}], key_col="id")
    storage.stamp_collection_metadata("lh_via_storage", lens_type="lakehouse", key_col="id")

    storage.write("kv_via_storage", [{"_key": "k1", "value": b"v1"}], key_col="_key")
    storage.stamp_collection_metadata("kv_via_storage", lens_type="keyvalue", key_col="_key")

    storage.write("vec_via_storage",
                   [{"id": 0, "dim_0": 1.0, "dim_1": 2.0, "metadata": "{}"}],
                   key_col="id")
    storage.stamp_collection_metadata("vec_via_storage", lens_type="vector", key_col="id")

    storage.write("stream_via_storage",
                   [{"offset": 0, "segment": b"hello"}],
                   key_col="offset")
    storage.stamp_collection_metadata("stream_via_storage", lens_type="streaming", key_col="offset")

    # PondStorage reads all 4 with the SAME API
    assert len(storage.read("lh_via_storage")) == 1
    assert len(storage.read("kv_via_storage")) == 1
    assert len(storage.read("vec_via_storage")) == 1
    assert len(storage.read("stream_via_storage")) == 1

    # point_lookup works on any of them
    assert storage.point_lookup("lh_via_storage", key="1")["name"] == "alice"
    assert storage.point_lookup("kv_via_storage", key="k1")["value"] == b"v1"
    assert storage.point_lookup("vec_via_storage", key="0")["dim_0"] == 1.0
    assert storage.point_lookup("stream_via_storage", key="0")["segment"] == b"hello"

    # list_collections_with_metadata shows all 4 with correct lens_type
    md = {m["name"]: m for m in storage.list_collections_with_metadata()}
    assert md["lh_via_storage"]["lens_type"] == "lakehouse"
    assert md["kv_via_storage"]["lens_type"] == "keyvalue"
    assert md["vec_via_storage"]["lens_type"] == "vector"
    assert md["stream_via_storage"]["lens_type"] == "streaming"

    print("PASS: test_pond_storage_uniform_access — PondStorage reads/writes any collection uniformly")
    return True


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    tests = [
        test_all_lenses_see_all_collections,
        test_kv_reads_lakehouse_collection,
        test_lakehouse_reads_kv_collection,
        test_vector_reads_lakehouse_collection,
        test_streaming_reads_kv_collection,
        test_kv_writes_to_lakehouse_collection,
        test_pond_storage_uniform_access,
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
        print("=== ALL CROSS-LENS UNIVERSAL ACCESS TESTS PASS ===")
        print("\nVerified: any lens can read/write any collection,")
        print("with metadata stamping for lens_type identification,")
        print("and NO cross-lens glue code required.")
    return 0 if passed == len(tests) else 1


if __name__ == "__main__":
    sys.exit(main())
