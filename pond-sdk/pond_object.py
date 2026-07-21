"""
PondObject — a named collection of bytes in the kernel with metadata and
namespace support.

A PondObject is the "object" in Pond — like a table in a database, a repo
in Git, or a notebook in Jupyter. It lives in a namespace path
(e.g., "analytics/orders" or "ml/features/user_stats") and has a type
that identifies which Lens family created it.

Architecture:
  Kernel (Bytes, History, Names)
      ↓
  Namespace (path structure: analytics/orders, ml/features)
      ↓
  PondObject (named object + type metadata)
      ↓
  Physical Structures (indexes, stats — accelerate access)
      ↓
  Lens (interprets bytes, never owns them)

Why "PondObject" instead of "Dataset":
  - "Dataset" implies tabular data (ML datasets, Spark datasets).
  - "PondObject" is format-agnostic — a Docker PondObject can hold any bytes.
  - A Pond PondObject can hold SQL rows, Git trees, notebook cells, or
    raw binary attachments. "PondObject" communicates that flexibility.
  - "PondObject" also has a storage-engine connotation (disk PondObject,
    logical PondObject) that fits Pond's storage-substrate identity.

Namespace:
  PondObjects live in a hierarchical namespace, like a filesystem:
    analytics/orders          ← a SQL PondObject
    analytics/customers       ← another SQL PondObject
    ml/features/user_stats    ← a feature store PondObject
    repo/main                 ← a Git PondObject
    notebooks/analysis        ← a notebook PondObject

  The namespace is just the path structure of the kernel Name.
  No new kernel primitives — just a naming convention.

Materialized views:
  A materialized view is a PondObject whose data is derived from another
  PondObject. It's just a PondObject with optional `source` metadata pointing
  to the parent PondObject. No separate API — just pass `source` when
  creating the PondObject:

    PondObject.create(kernel, "analytics/orders_by_region",
                  type="sql", source="analytics/orders")

  This records lineage (orders_by_region ← orders) without any
  special machinery. The materialized view has its own commit DAG;
  it's a separate PondObject that happens to track its source.

Usage:
    # Create a PondObject
    vol = PondObject.create(kernel, "analytics/orders", type="sql",
                         description="Customer orders table")

    # Use a Lens to read/write it
    lens = Lens(kernel, "analytics/orders")
    lens.put("order:1", {"amount": 100})
    lens.commit("insert order 1")

    # List all PondObjects
    PondObjects = PondObject.list(kernel)
    # [{"name": "analytics/orders", "type": "sql", "description": "..."}]

    # List PondObjects in a namespace
    analytics_PondObjects = PondObject.list(kernel, prefix="analytics/")
    # [{"name": "analytics/orders", ...}, {"name": "analytics/customers", ...}]

    # List namespaces
    namespaces = PondObject.list_namespaces(kernel)
    # ["analytics", "ml/features", "notebooks", "repo"]

    # Create a materialized view (just a PondObject with source)
    mv = PondObject.create(kernel, "analytics/orders_by_region",
                       type="sql", source="analytics/orders",
                       description="Orders aggregated by region")
"""

from __future__ import annotations

import os
import sys
import json
import time
from typing import Optional, Any

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(REPO, "pond-core"))
sys.path.insert(0, HERE)

from pond_minimal import PondMinimal


# ---------------------------------------------------------------------------
# PondObject — named object + type metadata + namespace
# ---------------------------------------------------------------------------

class PondObject:
    """A named collection of bytes in the kernel with type metadata.

    A PondObject is the "object" — a table, a repo, a notebook, a feature
    store. The Lens is how you read/write it. Multiple Lenses can
    share a PondObject.

    PondObjects live in a hierarchical namespace (e.g., "analytics/orders").
    The metadata is ONE small JSON blob per PondObject, stored as a kernel
    reference (Name: "{name}__meta"). NOT per record.
    """

    def __init__(self, kernel: PondMinimal, name: str):
        """Open an existing PondObject. Raises ValueError if it doesn't exist."""
        self.kernel = kernel
        self.name = name
        meta = self._read_meta()
        if meta is None:
            raise ValueError(
                f"PondObject '{name}' does not exist. "
                f"Use PondObject.create() to create it."
            )
        self._meta = meta

    # ------------------------------------------------------------------
    # Creation
    # ------------------------------------------------------------------

    @classmethod
    def create(cls, kernel: PondMinimal, name: str,
               type: str = "generic",
               description: str = "",
               source: Optional[str] = None,
               **extra) -> "PondObject":
        """Create a new PondObject with metadata.

        Args:
            kernel: the Pond kernel.
            name: the PondObject name (may include namespace path,
                e.g., "analytics/orders").
            type: the lens type that created this PondObject
                (e.g., "sql", "git", "feature_store", "notebook",
                "arrow", "streaming", "vector", "semantic").
            description: human-readable description.
            source: optional source PondObject name (for materialized views).
                If provided, this PondObject is a materialized view derived
                from the source PondObject. The view has its own commit DAG;
                it's a separate PondObject that tracks its lineage.
            **extra: additional metadata fields.

        Returns:
            A PondObject instance.
        """
        meta_ref = f"{name}__meta"
        if kernel.resolve(meta_ref) is not None:
            raise ValueError(
                f"PondObject '{name}' already exists. "
                f"Use PondObject(kernel, '{name}') to open it."
            )

        meta = {
            "name": name,
            "type": type,
            "description": description,
            "created_at": time.time(),
            "source": source,  # None for base PondObjects; parent name for views
            **extra,
        }
        meta_bytes = json.dumps(meta, sort_keys=True).encode()
        meta_hash = kernel.write(meta_bytes)
        kernel.reference(meta_ref, meta_hash)

        return cls(kernel, name)

    # ------------------------------------------------------------------
    # Metadata access
    # ------------------------------------------------------------------

    def _read_meta(self) -> Optional[dict]:
        h = self.kernel.resolve(f"{self.name}__meta")
        if h is None:
            return None
        return json.loads(self.kernel.read_blob(h))

    @property
    def type(self) -> str:
        """The lens type that created this PondObject (e.g., 'sql', 'git')."""
        return self._meta.get("type", "generic")

    @property
    def description(self) -> str:
        return self._meta.get("description", "")

    @property
    def source(self) -> Optional[str]:
        """The source PondObject name (if this is a materialized view).
        None for base PondObjects."""
        return self._meta.get("source")

    @property
    def is_materialized(self) -> bool:
        """True if this is a materialized view (has a source)."""
        return self._meta.get("source") is not None

    @property
    def created_at(self) -> float:
        return self._meta.get("created_at", 0)

    def meta(self) -> dict:
        return dict(self._meta)

    def update_meta(self, **kwargs) -> None:
        self._meta.update(kwargs)
        meta_bytes = json.dumps(self._meta, sort_keys=True).encode()
        meta_hash = self.kernel.write(meta_bytes)
        self.kernel.reference(f"{self.name}__meta", meta_hash)

    # ------------------------------------------------------------------
    # Namespace
    # ------------------------------------------------------------------

    @property
    def namespace(self) -> str:
        """The namespace path (everything before the last /).
        For "analytics/orders" → "analytics".
        For "orders" → "" (root namespace)."""
        if "/" in self.name:
            return self.name.rsplit("/", 1)[0]
        return ""

    @property
    def basename(self) -> str:
        """The last component of the name.
        For "analytics/orders" → "orders".
        For "orders" → "orders"."""
        return self.name.rsplit("/", 1)[-1]

    # ------------------------------------------------------------------
    # Registry — list PondObjects and namespaces
    # ------------------------------------------------------------------

    @staticmethod
    def list(kernel: PondMinimal, prefix: Optional[str] = None) -> list[dict]:
        """List all PondObjects in the kernel, optionally filtered by namespace.

        Args:
            kernel: the Pond kernel.
            prefix: optional namespace prefix (e.g., "analytics/").
                Only PondObjects whose name starts with this prefix are listed.

        Returns:
            List of metadata dicts, sorted by name.
        """
        names = kernel.list_names()
        PondObjects = []
        for name in names:
            if not name.endswith("__meta"):
                continue
            vol_name = name[:-len("__meta")]
            if prefix and not vol_name.startswith(prefix):
                continue
            h = kernel.resolve(name)
            if h:
                meta = json.loads(kernel.read_blob(h))
                PondObjects.append(meta)
        return sorted(PondObjects, key=lambda m: m.get("name", ""))

    @staticmethod
    def list_by_type(kernel: PondMinimal, type: str) -> list[dict]:
        """List all PondObjects of a given type (e.g., 'sql', 'git')."""
        return [v for v in PondObject.list(kernel) if v.get("type") == type]

    @staticmethod
    def list_namespaces(kernel: PondMinimal) -> list[str]:
        """List all namespaces (unique namespace paths across all PondObjects).

        Example:
            >>> PondObject.list_namespaces(kernel)
            ["analytics", "ml/features", "notebooks", "repo"]
        """
        PondObjects = PondObject.list(kernel)
        namespaces = set()
        for vol in PondObjects:
            name = vol.get("name", "")
            if "/" in name:
                # Add all parent namespaces
                parts = name.split("/")[:-1]  # drop the basename
                for i in range(len(parts)):
                    namespaces.add("/".join(parts[:i + 1]))
        return sorted(namespaces)

    @staticmethod
    def list_views(kernel: PondMinimal, source: Optional[str] = None) -> list[dict]:
        """List all materialized views, optionally filtered by source PondObject.

        Args:
            kernel: the Pond kernel.
            source: optional source PondObject name. If provided, only views
                derived from this source are listed.
        """
        views = [v for v in PondObject.list(kernel) if v.get("source") is not None]
        if source:
            views = [v for v in views if v.get("source") == source]
        return views

    @staticmethod
    def list_base(kernel: PondMinimal) -> list[dict]:
        """List all base PondObjects (not materialized views)."""
        return [v for v in PondObject.list(kernel) if v.get("source") is None]

    @staticmethod
    def exists(kernel: PondMinimal, name: str) -> bool:
        return kernel.resolve(f"{name}__meta") is not None

    @staticmethod
    def open(kernel: PondMinimal, name: str) -> Optional["PondObject"]:
        try:
            return PondObject(kernel, name)
        except ValueError:
            return None


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_PondObject_create_and_list():
    import shutil
    bench = "/tmp/pond_PondObject_test"
    if os.path.exists(bench): shutil.rmtree(bench)
    os.makedirs(bench)
    kernel = PondMinimal(bench)

    PondObject.create(kernel, "analytics/orders", type="sql",
                   description="Customer orders table")
    PondObject.create(kernel, "analytics/customers", type="sql",
                   description="Customer profiles")
    PondObject.create(kernel, "ml/features/user_stats", type="feature_store",
                   description="User feature statistics")
    PondObject.create(kernel, "repo/main", type="git",
                   description="Source code repository")
    PondObject.create(kernel, "notebooks/analysis", type="notebook",
                   description="Analysis notebook")

    # List all
    PondObjects = PondObject.list(kernel)
    assert len(PondObjects) == 5
    names = [v["name"] for v in PondObjects]
    assert "analytics/orders" in names
    assert "ml/features/user_stats" in names

    # List by type
    sql_PondObjects = PondObject.list_by_type(kernel, "sql")
    assert len(sql_PondObjects) == 2

    # List by namespace prefix
    analytics = PondObject.list(kernel, prefix="analytics/")
    assert len(analytics) == 2
    assert all(v["name"].startswith("analytics/") for v in analytics)

    # List namespaces
    namespaces = PondObject.list_namespaces(kernel)
    assert "analytics" in namespaces
    assert "ml/features" in namespaces
    assert "repo" in namespaces
    assert "notebooks" in namespaces

    # Verify metadata
    vol = PondObject(kernel, "analytics/orders")
    assert vol.type == "sql"
    assert vol.description == "Customer orders table"
    assert vol.namespace == "analytics"
    assert vol.basename == "orders"
    assert vol.is_materialized is False
    assert vol.source is None

    kernel.close()
    shutil.rmtree(bench, ignore_errors=True)
    print("PASS: PondObject create, list, list_by_type, list_namespaces, namespace/basename")


def test_materialized_views():
    import shutil
    bench = "/tmp/pond_PondObject_views"
    if os.path.exists(bench): shutil.rmtree(bench)
    os.makedirs(bench)
    kernel = PondMinimal(bench)

    # Create a base PondObject
    PondObject.create(kernel, "analytics/orders", type="sql",
                   description="Orders table")

    # Create materialized views (just PondObjects with source=)
    PondObject.create(kernel, "analytics/orders_by_region", type="sql",
                   source="analytics/orders",
                   description="Orders aggregated by region")
    PondObject.create(kernel, "analytics/orders_index_customer", type="sql",
                   source="analytics/orders",
                   description="Index on customer_id")

    # List base PondObjects
    base = PondObject.list_base(kernel)
    assert len(base) == 1
    assert base[0]["name"] == "analytics/orders"

    # List materialized views
    views = PondObject.list_views(kernel)
    assert len(views) == 2

    # List views for a specific source
    order_views = PondObject.list_views(kernel, source="analytics/orders")
    assert len(order_views) == 2

    # Verify view metadata
    mv = PondObject(kernel, "analytics/orders_by_region")
    assert mv.is_materialized is True
    assert mv.source == "analytics/orders"

    kernel.close()
    shutil.rmtree(bench, ignore_errors=True)
    print("PASS: Materialized views (source metadata, list_views, list_base, lineage)")


def test_PondObject_with_lens():
    import shutil
    bench = "/tmp/pond_PondObject_lens"
    if os.path.exists(bench): shutil.rmtree(bench)
    os.makedirs(bench)
    kernel = PondMinimal(bench)

    PondObject.create(kernel, "analytics/users", type="sql",
                   description="User table")

    from lens_sdk import Lens
    lens = Lens(kernel, "analytics/users")
    lens.put("user:1", {"name": "Alice", "age": 30})
    lens.put("user:2", {"name": "Bob", "age": 25})
    lens.commit("insert 2 users")

    assert lens.get("user:1") == {"name": "Alice", "age": 30}
    assert lens.count() == 2

    vol = PondObject(kernel, "analytics/users")
    assert vol.type == "sql"
    assert vol.namespace == "analytics"
    assert vol.basename == "users"

    # Metadata is ONE small blob
    meta_h = kernel.resolve("analytics/users__meta")
    meta_bytes = kernel.read_blob(meta_h)
    assert len(meta_bytes) < 500

    kernel.close()
    shutil.rmtree(bench, ignore_errors=True)
    print("PASS: PondObject + Lens integration (namespace path, metadata is small)")


def test_PondObject_persists():
    import shutil
    bench = "/tmp/pond_PondObject_persist"
    if os.path.exists(bench): shutil.rmtree(bench)
    os.makedirs(bench)
    kernel = PondMinimal(bench)

    PondObject.create(kernel, "analytics/orders", type="sql",
                   description="Orders table")
    kernel.close()

    kernel2 = PondMinimal(bench)
    PondObjects = PondObject.list(kernel2)
    assert len(PondObjects) == 1
    assert PondObjects[0]["name"] == "analytics/orders"
    assert PondObjects[0]["type"] == "sql"

    vol = PondObject(kernel2, "analytics/orders")
    assert vol.description == "Orders table"
    assert vol.namespace == "analytics"

    kernel2.close()
    shutil.rmtree(bench, ignore_errors=True)
    print("PASS: PondObject metadata persists across restart (with namespace)")


def _run_all_tests():
    print("=== PondObject Layer Tests ===\n")
    test_PondObject_create_and_list()
    print()
    test_materialized_views()
    print()
    test_PondObject_with_lens()
    print()
    test_PondObject_persists()
    print("\n=== ALL TESTS PASSED ===")


if __name__ == "__main__":
    _run_all_tests()
