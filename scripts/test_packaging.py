"""Test that the pond package is properly installable and importable.

Run: python scripts/test_packaging.py
Or:  pytest scripts/test_packaging.py
"""
import os, sys, tempfile, shutil


def test_pond_core_imports():
    """Test pond.core imports work."""
    from pond.core import (
        make_kernel, ObjectStoreNativeKernel, LocalFSObjectStore,
        S3ObjectStore, InMemoryObjectStore, PondMinimal, hash_bytes,
    )
    assert make_kernel is not None
    assert ObjectStoreNativeKernel is not None
    assert LocalFSObjectStore is not None
    print("PASS: pond.core imports work")


def test_pond_sdk_imports():
    """Test pond.sdk imports work."""
    from pond.sdk import PondStorage, PondLens, PondConfig, HLC, uuidv7
    assert PondStorage is not None
    assert PondLens is not None
    print("PASS: pond.sdk imports work")


def test_pond_extensions_imports():
    """Test pond.sdk.extensions imports work."""
    from pond.sdk.extensions import UnifiedStorage, CollectionManifest
    assert UnifiedStorage is not None
    assert CollectionManifest is not None
    print("PASS: pond.sdk.extensions imports work")


def test_lens_imports():
    """Test lens imports work via pond.lenses."""
    import pond.lenses.keyvalue
    from keyvalue_lens import KeyValueLens
    assert KeyValueLens is not None
    print("PASS: lens imports work via pond.lenses")


def test_end_to_end():
    """Test end-to-end write/read via the pond package."""
    from pond.core import make_kernel
    from pond.sdk import PondStorage

    tmpdir = tempfile.mkdtemp(prefix="pond_pkg_test_")
    try:
        kernel = make_kernel(f"file://{tmpdir}")
        s = PondStorage(kernel)

        # Write
        s.write("users", [{"id": i, "name": f"u{i}"} for i in range(10)],
                key_col="id", row_group_size=5)

        # Read
        rows = s.read("users")
        assert len(rows) == 10, f"Expected 10 rows, got {len(rows)}"

        # Point lookup
        row = s.point_lookup("users", key="5")
        assert row is not None
        assert row["name"] == "u5"

        # Branch + merge
        s.branch("users", "dev")
        s.checkout("users", "dev")
        s.append("users", [{"id": 100, "name": "dev"}], key_col="id")
        s.merge("users", "dev")
        rows = s.read("users")
        assert len(rows) == 11, f"Expected 11 rows after merge, got {len(rows)}"

        print("PASS: end-to-end write/read/branch/merge via pond package")
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_make_kernel_url_schemes():
    """Test make_kernel with different URL schemes."""
    from pond.core import make_kernel, ObjectStoreNativeKernel

    # file:// scheme
    tmpdir = tempfile.mkdtemp(prefix="pond_url_test_")
    try:
        kernel = make_kernel(f"file://{tmpdir}")
        assert isinstance(kernel, ObjectStoreNativeKernel)
        assert hasattr(kernel, 'store')
        print("PASS: make_kernel('file://...') works")
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def main():
    print("=" * 60)
    print("  Pond Packaging Test")
    print("=" * 60)

    tests = [
        test_pond_core_imports,
        test_pond_sdk_imports,
        test_pond_extensions_imports,
        test_lens_imports,
        test_end_to_end,
        test_make_kernel_url_schemes,
    ]

    passed = 0
    for test in tests:
        try:
            test()
            passed += 1
        except Exception as e:
            print(f"FAIL: {test.__name__}: {e}")
            import traceback
            traceback.print_exc()

    print(f"\n{passed}/{len(tests)} tests passed")
    if passed == len(tests):
        print("=== ALL PACKAGING TESTS PASS ===")
    return 0 if passed == len(tests) else 1


if __name__ == "__main__":
    sys.exit(main())
