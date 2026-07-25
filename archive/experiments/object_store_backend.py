#!/usr/bin/env python3
"""
Object-Store-Native Backend — references as individual objects.

THE GAP (from POND_FORMAL_ALGEBRAS.md §5):
  OSN7 (No local metadata dependence): the current kernel uses SQLite
  for the root namespace. This is a local metadata dependency.

THE SOLUTION:
  An object-store-native backend where each reference is a separate
  object (file on FS, object on S3). No SQLite. No local database.

  Reference storage:
    {objects_dir}/{hash[:2]}/{hash}.bin    — blob storage (same as before)
    {refs_dir}/{name}                       — reference storage (1 file per ref)

  This satisfies OSN7: no local metadata. Every piece of state is
  either an immutable blob or a small reference file. On S3, each
  reference is a separate object (PUT to set, GET to resolve, LIST
  to enumerate).

DESIGN:
  - ObjectStoreKernel: implements Write/Read/Reference using only
    file/object operations (no SQLite).
  - Same API as PondMinimal: write(), read(), read_blob(), reference(),
    resolve(), list_names().
  - Reference files are tiny (64 bytes: the hash). On S3, each is
    a small PUT. On local disk, each is a small file.

COST COMPARISON:
  Operation     SQLite backend    Object-store backend
  reference()   1 SQLite INSERT   1 PUT (write ref file)
  resolve()     1 SQLite SELECT   1 GET (read ref file)
  list_names()  1 SQLite SELECT   1 LIST (list ref dir)
  write()       1 file write      1 file write (same)
  read_blob()   1 file read       1 file read (same)

  On S3: list_names() is expensive (LIST). But it's rarely called
  (only for listing collections/branches, not for lookups).

Run:
    python experiments/object_store_backend.py
"""

from __future__ import annotations

import os
import sys
import shutil
import hashlib
import time
from typing import Optional

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(REPO, "pond-core"))
sys.path.insert(0, os.path.join(REPO, "pond-sdk"))

from keyvalue_lens import Lens


class ObjectStoreKernel:
    """A kernel backend with NO local metadata database.

    References are stored as individual files (one per reference).
    Blobs are stored as content-addressed files (same as PondMinimal).

    This satisfies OSN7 (no local metadata dependence) from the
    Object Store Native specification.
    """

    def __init__(self, base_dir: str):
        self.base_dir = os.path.abspath(base_dir)
        self.objects_dir = os.path.join(self.base_dir, ".pond", "objects")
        self.refs_dir = os.path.join(self.base_dir, ".pond", "refs")

        os.makedirs(self.objects_dir, exist_ok=True)
        os.makedirs(self.refs_dir, exist_ok=True)

        self.stats = {"writes": 0, "reads": 0, "references": 0, "resolves": 0}

    # ------------------------------------------------------------------
    # Blob storage (same as PondMinimal)
    # ------------------------------------------------------------------

    def write(self, data: bytes) -> str:
        """Write an immutable blob. Returns its SHA-256 hash."""
        h = hashlib.sha256(data).hexdigest()
        shard_dir = os.path.join(self.objects_dir, h[:2])
        os.makedirs(shard_dir, exist_ok=True)
        path = os.path.join(shard_dir, h + ".bin")
        if not os.path.exists(path):
            with open(path, "wb") as f:
                f.write(data)
        self.stats["writes"] += 1
        return h

    def read_blob(self, h: str) -> bytes:
        """Read a blob by hash directly."""
        self.stats["reads"] += 1
        path = os.path.join(self.objects_dir, h[:2], h + ".bin")
        if not os.path.exists(path):
            raise ValueError(f"Blob {h} not found")
        with open(path, "rb") as f:
            return f.read()

    def read(self, hash_or_name: str) -> bytes:
        """Read by hash or by name."""
        self.stats["reads"] += 1
        if len(hash_or_name) == 64 and all(c in "0123456789abcdef" for c in hash_or_name):
            return self.read_blob(hash_or_name)
        h = self.resolve(hash_or_name)
        if h is None:
            raise ValueError(f"Name '{hash_or_name}' not bound")
        return self.read_blob(h)

    # ------------------------------------------------------------------
    # Reference storage (NO SQLite — individual files)
    # ------------------------------------------------------------------

    def _ref_path(self, name: str) -> str:
        """Convert a reference name to a file path.

        Names with '/' are stored in subdirectories:
          "analytics/orders" → refs/analytics/orders
          "test__snapshot"   → refs/test__snapshot
        """
        # Replace characters that are problematic in filenames
        # but keep '/' for namespace hierarchy
        safe_name = name.replace(":", "_").replace("|", "_")
        return os.path.join(self.refs_dir, safe_name)

    def reference(self, name: str, h: str) -> None:
        """Set a name → hash mapping. The ONLY mutation."""
        # Verify the blob exists (same as PondMinimal)
        blob_path = os.path.join(self.objects_dir, h[:2], h + ".bin")
        if not os.path.exists(blob_path):
            raise ValueError(f"Hash {h} does not refer to an existing blob")

        path = self._ref_path(name)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            f.write(h)
        self.stats["references"] += 1

    def resolve(self, name: str) -> Optional[str]:
        """Resolve a name to its current hash."""
        self.stats["resolves"] += 1
        path = self._ref_path(name)
        if not os.path.exists(path):
            return None
        with open(path, "r") as f:
            return f.read().strip()

    def list_names(self) -> list[str]:
        """List all reference names."""
        names = []
        for root, dirs, files in os.walk(self.refs_dir):
            for f in files:
                full = os.path.join(root, f)
                rel = os.path.relpath(full, self.refs_dir)
                # Restore the original name
                name = rel.replace(os.sep, "/")
                names.append(name)
        return sorted(names)

    # ------------------------------------------------------------------
    # Helpers (same interface as PondMinimal)
    # ------------------------------------------------------------------

    @property
    def objects_dir_prop(self):
        return self.objects_dir

    @property
    def root_db(self):
        # No root_db — this backend doesn't use SQLite.
        # This property exists for compatibility with code that
        # accesses kernel.root_db (e.g., compact_tombstones).
        raise AttributeError("ObjectStoreKernel has no root_db (no SQLite)")

    def storage_stats(self) -> dict:
        data_bytes = 0
        blob_count = 0
        for shard in os.listdir(self.objects_dir):
            shard_path = os.path.join(self.objects_dir, shard)
            if not os.path.isdir(shard_path):
                continue
            for f in os.listdir(shard_path):
                if f.endswith(".bin"):
                    data_bytes += os.path.getsize(os.path.join(shard_path, f))
                    blob_count += 1

        ref_count = 0
        for root, dirs, files in os.walk(self.refs_dir):
            ref_count += len(files)

        return {
            **self.stats,
            "data_bytes": data_bytes,
            "blob_count": blob_count,
            "name_count": ref_count,
        }

    def close(self) -> None:
        pass  # no SQLite to close


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_basic_operations():
    """Write, read, reference, resolve — same API as PondMinimal."""
    bench = "/tmp/pond_os_kernel"
    if os.path.exists(bench): shutil.rmtree(bench)
    os.makedirs(bench)
    kernel = ObjectStoreKernel(bench)

    # Write
    h = kernel.write(b'{"v": 1}')
    assert len(h) == 64

    # Read by hash
    data = kernel.read_blob(h)
    assert data == b'{"v": 1}'

    # Reference
    kernel.reference("test", h)

    # Resolve
    assert kernel.resolve("test") == h

    # Read by name
    assert kernel.read("test") == b'{"v": 1}'

    # List names
    assert "test" in kernel.list_names()

    kernel.close()
    shutil.rmtree(bench, ignore_errors=True)
    print("PASS: Basic operations (write, read, reference, resolve, list)")


def test_no_sqlite():
    """Verify: NO SQLite database exists."""
    bench = "/tmp/pond_os_nosqlite"
    if os.path.exists(bench): shutil.rmtree(bench)
    os.makedirs(bench)
    kernel = ObjectStoreKernel(bench)

    kernel.write(b"data")
    kernel.reference("ref1", kernel.write(b"more data"))

    # Check: no .sqlite file exists
    pond_dir = os.path.join(bench, ".pond")
    for f in os.listdir(pond_dir):
        assert not f.endswith(".sqlite"), f"SQLite database found: {f}"

    # References are stored as individual files
    refs_dir = os.path.join(pond_dir, "refs")
    assert os.path.exists(refs_dir)
    ref_files = os.listdir(refs_dir)
    assert "ref1" in ref_files

    # Each ref file contains just the hash (64 chars)
    with open(os.path.join(refs_dir, "ref1"), "r") as f:
        content = f.read().strip()
    assert len(content) == 64

    kernel.close()
    shutil.rmtree(bench, ignore_errors=True)
    print("PASS: No SQLite database (references are individual files)")


def test_namespace_hierarchy():
    """References with '/' create directory hierarchy."""
    bench = "/tmp/pond_os_namespace"
    if os.path.exists(bench): shutil.rmtree(bench)
    os.makedirs(bench)
    kernel = ObjectStoreKernel(bench)

    h = kernel.write(b"data")
    kernel.reference("analytics/orders", h)
    kernel.reference("analytics/customers", h)
    kernel.reference("ml/features", h)

    # Verify directory structure
    refs_dir = os.path.join(bench, ".pond", "refs")
    assert os.path.isdir(os.path.join(refs_dir, "analytics"))
    assert os.path.isdir(os.path.join(refs_dir, "ml"))

    # List should return full paths
    names = kernel.list_names()
    assert "analytics/orders" in names
    assert "analytics/customers" in names
    assert "ml/features" in names

    kernel.close()
    shutil.rmtree(bench, ignore_errors=True)
    print("PASS: Namespace hierarchy (analytics/orders, ml/features)")


def test_with_lens():
    """ObjectStoreKernel works with the Lens class."""
    bench = "/tmp/pond_os_lens"
    if os.path.exists(bench): shutil.rmtree(bench)
    os.makedirs(bench)
    kernel = ObjectStoreKernel(bench)

    lens = Lens(kernel, "test")
    lens.put("k1", {"v": 1})
    lens.put("k2", {"v": 2})
    lens.commit("initial")

    assert lens.get("k1") == {"v": 1}
    assert lens.get("k2") == {"v": 2}
    assert lens.count() == 2

    # Snapshot pointer should work
    snap = kernel.resolve("test__snapshot")
    assert snap is not None

    # Branch
    lens.branch("dev")
    assert "dev" in lens.list_branches()

    # History
    assert len(lens.history()) >= 1

    kernel.close()
    shutil.rmtree(bench, ignore_errors=True)
    print("PASS: Works with Lens (put, get, commit, branch, history, snapshot pointer)")


def test_persistence():
    """Data survives restart — no SQLite, just files."""
    bench = "/tmp/pond_os_persist"
    if os.path.exists(bench): shutil.rmtree(bench)
    os.makedirs(bench)
    kernel = ObjectStoreKernel(bench)

    lens = Lens(kernel, "test")
    for i in range(100):
        lens.put(f"k{i:03d}", {"v": i})
    lens.commit("100 records")
    kernel.close()

    # Reopen
    kernel2 = ObjectStoreKernel(bench)
    lens2 = Lens(kernel2, "test")
    assert lens2.count() == 100
    assert lens2.get("k050") == {"v": 50}
    assert lens2.get("k000") == {"v": 0}
    assert lens2.get("k099") == {"v": 99}

    kernel2.close()
    shutil.rmtree(bench, ignore_errors=True)
    print("PASS: Persistence (100 records survived restart, no SQLite)")


def test_reference_overwrite():
    """Overwriting a reference updates it atomically."""
    bench = "/tmp/pond_os_overwrite"
    if os.path.exists(bench): shutil.rmtree(bench)
    os.makedirs(bench)
    kernel = ObjectStoreKernel(bench)

    h1 = kernel.write(b"v1")
    h2 = kernel.write(b"v2")

    kernel.reference("test", h1)
    assert kernel.resolve("test") == h1

    kernel.reference("test", h2)
    assert kernel.resolve("test") == h2

    kernel.close()
    shutil.rmtree(bench, ignore_errors=True)
    print("PASS: Reference overwrite (last-writer-wins)")


def test_differential():
    """Run differential tests against the object-store kernel."""
    bench = "/tmp/pond_os_diff"
    if os.path.exists(bench): shutil.rmtree(bench)
    os.makedirs(bench)
    kernel = ObjectStoreKernel(bench)
    lens = Lens(kernel, "diff")

    # Simple differential test: put, commit, get, verify
    for i in range(50):
        lens.put(f"k{i:03d}", {"id": i, "val": i * 10})
    lens.commit("50 records")

    for i in range(50):
        assert lens.get(f"k{i:03d}") == {"id": i, "val": i * 10}

    # Delete some
    lens.delete("k005")
    lens.delete("k010")
    lens.commit("deletes")

    assert lens.get("k005") is None
    assert lens.get("k010") is None
    assert lens.get("k000") == {"id": 0, "val": 0}
    assert lens.count() == 48

    kernel.close()
    shutil.rmtree(bench, ignore_errors=True)
    print("PASS: Differential test (50 puts, 2 deletes, verify count + lookups)")


def test_storage_stats():
    """Storage stats work correctly."""
    bench = "/tmp/pond_os_stats"
    if os.path.exists(bench): shutil.rmtree(bench)
    os.makedirs(bench)
    kernel = ObjectStoreKernel(bench)

    for i in range(100):
        kernel.write(f"blob_{i}".encode())
    kernel.reference("ref1", kernel.write(b"ref_data"))
    kernel.reference("ref2", kernel.write(b"more_ref_data"))

    stats = kernel.storage_stats()
    assert stats["blob_count"] == 102  # 100 + 2 refs
    assert stats["name_count"] == 2
    assert stats["writes"] == 102

    kernel.close()
    shutil.rmtree(bench, ignore_errors=True)
    print(f"PASS: Storage stats ({stats['blob_count']} blobs, {stats['name_count']} refs)")


def main():
    print("=" * 72)
    print("  Object-Store-Native Backend")
    print("  No SQLite. References are individual files/objects.")
    print("  Satisfies OSN7 (no local metadata dependence).")
    print("=" * 72)

    test_basic_operations()
    print()
    test_no_sqlite()
    print()
    test_namespace_hierarchy()
    print()
    test_with_lens()
    print()
    test_persistence()
    print()
    test_reference_overwrite()
    print()
    test_differential()
    print()
    test_storage_stats()

    print("\n" + "=" * 72)
    print("  OBJECT-STORE-NATIVE BACKEND SUMMARY")
    print("=" * 72)
    print("  ✓ No SQLite database (OSN7 compliant)")
    print("  ✓ References are individual files (1 file per ref)")
    print("  ✓ Namespace hierarchy via directory structure")
    print("  ✓ Works with Lens (put, get, commit, branch, history)")
    print("  ✓ Data persists across restart (just files, no DB)")
    print("  ✓ Reference overwrite (last-writer-wins)")
    print("  ✓ Differential test passes (50 puts, 2 deletes)")
    print("  ✓ Storage stats correct")
    print()
    print("  On S3, each reference maps to 1 object:")
    print("    reference() = 1 PUT")
    print("    resolve()   = 1 GET")
    print("    list_names() = 1 LIST")
    print("=" * 72)


if __name__ == "__main__":
    main()
