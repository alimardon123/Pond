"""
Collection — a named collection of bytes in the kernel with metadata and
namespace support.

A Collection is the "object" in Pond — like a table in a database, a repo
in Git, or a notebook in Jupyter. It lives in a namespace path
(e.g., "analytics/orders" or "ml/features/user_stats") and has a type
that identifies which Lens family created it.

Architecture:
  Kernel (Bytes, History, Names)
      ↓
  Namespace (path structure: analytics/orders, ml/features)
      ↓
  Collection (named object + type metadata)
      ↓
  Physical Structures (indexes, stats — accelerate access)
      ↓
  Lens (interprets bytes, never owns them)

Why "Collection" instead of "Dataset":
  - "Dataset" implies tabular data (ML datasets, Spark datasets).
  - "Collection" is format-agnostic — a Docker Collection can hold any bytes.
  - A Pond Collection can hold SQL rows, Git trees, notebook cells, or
    raw binary attachments. "Collection" communicates that flexibility.
  - "Collection" also has a storage-engine connotation (disk Collection,
    logical Collection) that fits Pond's storage-substrate identity.

Namespace:
  Collections live in a hierarchical namespace, like a filesystem:
    analytics/orders          ← a SQL Collection
    analytics/customers       ← another SQL Collection
    ml/features/user_stats    ← a feature store Collection
    repo/main                 ← a Git Collection
    notebooks/analysis        ← a notebook Collection

  The namespace is just the path structure of the kernel Name.
  No new kernel primitives — just a naming convention.

Materialized views:
  A materialized view is a Collection whose data is derived from another
  Collection. It's just a Collection with optional `source` metadata pointing
  to the parent Collection. No separate API — just pass `source` when
  creating the Collection:

    Collection.create(kernel, "analytics/orders_by_region",
                  labels=["sql"], created_by="SqlLens", source="analytics/orders")

  This records lineage (orders_by_region ← orders) without any
  special machinery. The materialized view has its own commit DAG;
  it's a separate Collection that happens to track its source.

Usage:
    # Create a Collection
    vol = Collection.create(kernel, "analytics/orders", labels=["sql"], created_by="SqlLens",
                         description="Customer orders table")

    # Use a Lens to read/write it
    lens = Lens(kernel, "analytics/orders")
    lens.put("order:1", {"amount": 100})
    lens.commit("insert order 1")

    # List all Collections
    Collections = Collection.list(kernel)
    # [{"name": "analytics/orders", "type": "sql", "description": "..."}]

    # List Collections in a namespace
    analytics_Collections = Collection.list(kernel, prefix="analytics/")
    # [{"name": "analytics/orders", ...}, {"name": "analytics/customers", ...}]

    # List namespaces
    namespaces = Collection.list_namespaces(kernel)
    # ["analytics", "ml/features", "notebooks", "repo"]

    # Create a materialized view (just a Collection with source)
    mv = Collection.create(kernel, "analytics/orders_by_region",
                       labels=["sql"], created_by="SqlLens", source="analytics/orders",
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
sys.path.insert(0, os.path.join(REPO, "lenses", "keyvalue"))

from kernel import PondMinimal


# ---------------------------------------------------------------------------
# Collection — named object + type metadata + namespace
# ---------------------------------------------------------------------------

class Collection:
    """A named collection of bytes in the kernel with type metadata.

    A Collection is the "object" — a table, a repo, a notebook, a feature
    store. The Lens is how you read/write it. Multiple Lenses can
    share a Collection.

    Collections live in a hierarchical namespace (e.g., "analytics/orders").
    The metadata is ONE small JSON blob per Collection, stored as a kernel
    reference (Name: "{name}__meta"). NOT per record.
    """

    def __init__(self, kernel: PondMinimal, name: str):
        """Open an existing Collection. Raises ValueError if it doesn't exist."""
        self.kernel = kernel
        self.name = name
        meta = self._read_meta()
        if meta is None:
            raise ValueError(
                f"Collection '{name}' does not exist. "
                f"Use Collection.create() to create it."
            )
        self._meta = meta

    # ------------------------------------------------------------------
    # Creation
    # ------------------------------------------------------------------

    @classmethod
    def create(cls, kernel: PondMinimal, name: str,
               labels: Optional[list[str]] = None,
               description: str = "",
               source: Optional[str] = None,
               created_by: str = "",
               **extra) -> "Collection":
        """Create a new Collection with metadata.

        Collections are NEUTRAL — they don't have a "type" that ties them
        to one Lens family. Instead, they have:
        - labels: neutral tags for organization (e.g., ["analytics", "production"])
        - created_by: provenance — which Lens created this (informational, not
          authoritative; any Lens can read/write any Collection)

        This preserves the architecture's key principle: Collections are
        interpreted by Lenses, not owned by them.

        Args:
            kernel: the Pond kernel.
            name: the Collection name (may include namespace path,
                e.g., "analytics/orders").
            labels: optional list of neutral tags for organization/filtering.
                e.g., ["analytics", "finance", "production"].
            description: human-readable description.
            source: optional source Collection name (for materialized views).
            created_by: optional — which Lens created this (provenance only).
            **extra: additional metadata fields.

        Returns:
            A Collection instance.
        """
        meta_ref = f"{name}__meta"
        if kernel.resolve(meta_ref) is not None:
            raise ValueError(
                f"Collection '{name}' already exists. "
                f"Use Collection(kernel, '{name}') to open it."
            )

        meta = {
            "name": name,
            "labels": labels or [],
            "description": description,
            "created_at": time.time(),
            "source": source,
            "created_by": created_by,
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
    def labels(self) -> list[str]:
        """Neutral tags for organization (e.g., ['analytics', 'production']).

        Collections are NOT typed — they don't belong to one Lens family.
        Any Lens can read/write any Collection. Labels are for filtering
        and organization, not for restricting access."""
        return self._meta.get("labels", [])

    @property
    def created_by(self) -> str:
        """Which Lens created this Collection (provenance only).

        Informational, not authoritative. Any Lens can read/write any
        Collection regardless of created_by."""
        return self._meta.get("created_by", "")

    @property
    def description(self) -> str:
        return self._meta.get("description", "")

    @property
    def source(self) -> Optional[str]:
        """The source Collection name (if this is a materialized lens).
        None for base Collections."""
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
    # Registry — list Collections and namespaces
    # ------------------------------------------------------------------

    @staticmethod
    def list(kernel: PondMinimal, prefix: Optional[str] = None) -> list[dict]:
        """List all Collections in the kernel, optionally filtered by namespace.

        Args:
            kernel: the Pond kernel.
            prefix: optional namespace prefix (e.g., "analytics/").
                Only Collections whose name starts with this prefix are listed.

        Returns:
            List of metadata dicts, sorted by name.
        """
        names = kernel.list_names()
        Collections = []
        for name in names:
            if not name.endswith("__meta"):
                continue
            vol_name = name[:-len("__meta")]
            if prefix and not vol_name.startswith(prefix):
                continue
            h = kernel.resolve(name)
            if h:
                meta = json.loads(kernel.read_blob(h))
                Collections.append(meta)
        return sorted(Collections, key=lambda m: m.get("name", ""))

    @staticmethod
    def list_by_label(kernel: PondMinimal, label: str) -> list[dict]:
        """List all Collections with a given label."""
        return [v for v in Collection.list(kernel) if label in v.get("labels", [])]

    @staticmethod
    def list_namespaces(kernel: PondMinimal) -> list[str]:
        """List all namespaces (unique namespace paths across all Collections).

        Example:
            >>> Collection.list_namespaces(kernel)
            ["analytics", "ml/features", "notebooks", "repo"]
        """
        Collections = Collection.list(kernel)
        namespaces = set()
        for vol in Collections:
            name = vol.get("name", "")
            if "/" in name:
                # Add all parent namespaces
                parts = name.split("/")[:-1]  # drop the basename
                for i in range(len(parts)):
                    namespaces.add("/".join(parts[:i + 1]))
        return sorted(namespaces)

    @staticmethod
    def list_views(kernel: PondMinimal, source: Optional[str] = None) -> list[dict]:
        """List all materialized views, optionally filtered by source Collection.

        Args:
            kernel: the Pond kernel.
            source: optional source Collection name. If provided, only views
                derived from this source are listed.
        """
        views = [v for v in Collection.list(kernel) if v.get("source") is not None]
        if source:
            views = [v for v in views if v.get("source") == source]
        return lenss

    @staticmethod
    def list_base(kernel: PondMinimal) -> list[dict]:
        """List all base Collections (not materialized views)."""
        return [v for v in Collection.list(kernel) if v.get("source") is None]

    @staticmethod
    def exists(kernel: PondMinimal, name: str) -> bool:
        return kernel.resolve(f"{name}__meta") is not None

    @staticmethod
    def open(kernel: PondMinimal, name: str) -> Optional["Collection"]:
        try:
            return Collection(kernel, name)
        except ValueError:
            return None


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_Collection_create_and_list():
    import shutil
    bench = "/tmp/pond_Collection_test"
    if os.path.exists(bench): shutil.rmtree(bench)
    os.makedirs(bench)
    kernel = PondMinimal(bench)

    Collection.create(kernel, "analytics/orders", labels=["sql"], created_by="SqlLens",
                   description="Customer orders table")
    Collection.create(kernel, "analytics/customers", labels=["sql"], created_by="SqlLens",
                   description="Customer profiles")
    Collection.create(kernel, "ml/features/user_stats", labels=["feature_store"], created_by="FeatureStoreLens",
                   description="User feature statistics")
    Collection.create(kernel, "repo/main", labels=["git"], created_by="GitLens",
                   description="Source code repository")
    Collection.create(kernel, "notebooks/analysis", labels=["notebook"], created_by="NotebookLens",
                   description="Analysis notebook")

    # List all
    Collections = Collection.list(kernel)
    assert len(Collections) == 5
    names = [v["name"] for v in Collections]
    assert "analytics/orders" in names
    assert "ml/features/user_stats" in names

    # List by type
    sql_Collections = Collection.list_by_label(kernel, "sql")
    assert len(sql_Collections) == 2

    # List by namespace prefix
    analytics = Collection.list(kernel, prefix="analytics/")
    assert len(analytics) == 2
    assert all(v["name"].startswith("analytics/") for v in analytics)

    # List namespaces
    namespaces = Collection.list_namespaces(kernel)
    assert "analytics" in namespaces
    assert "ml/features" in namespaces
    assert "repo" in namespaces
    assert "notebooks" in namespaces

    # Verify metadata
    vol = Collection(kernel, "analytics/orders")
    assert vol.labels == ["sql"]
    assert vol.description == "Customer orders table"
    assert vol.namespace == "analytics"
    assert vol.basename == "orders"
    assert vol.is_materialized is False
    assert vol.source is None

    kernel.close()
    shutil.rmtree(bench, ignore_errors=True)
    print("PASS: Collection create, list, list_by_label, list_namespaces, namespace/basename")


def test_materialized_views():
    import shutil
    bench = "/tmp/pond_Collection_views"
    if os.path.exists(bench): shutil.rmtree(bench)
    os.makedirs(bench)
    kernel = PondMinimal(bench)

    # Create a base Collection
    Collection.create(kernel, "analytics/orders", labels=["sql"], created_by="SqlLens",
                   description="Orders table")

    # Create materialized views (just Collections with source=)
    Collection.create(kernel, "analytics/orders_by_region", labels=["sql"], created_by="SqlLens",
                   source="analytics/orders",
                   description="Orders aggregated by region")
    Collection.create(kernel, "analytics/orders_index_customer", labels=["sql"], created_by="SqlLens",
                   source="analytics/orders",
                   description="Index on customer_id")

    # List base Collections
    base = Collection.list_base(kernel)
    assert len(base) == 1
    assert base[0]["name"] == "analytics/orders"

    # List materialized views
    views = Collection.list_views(kernel)
    assert len(views) == 2

    # List views for a specific source
    order_views = Collection.list_views(kernel, source="analytics/orders")
    assert len(order_views) == 2

    # Verify view metadata
    mv = Collection(kernel, "analytics/orders_by_region")
    assert mv.is_materialized is True
    assert mv.source == "analytics/orders"

    kernel.close()
    shutil.rmtree(bench, ignore_errors=True)
    print("PASS: Materialized views (source metadata, list_views, list_base, lineage)")


def test_Collection_with_lens():
    import shutil
    bench = "/tmp/pond_Collection_lens"
    if os.path.exists(bench): shutil.rmtree(bench)
    os.makedirs(bench)
    kernel = PondMinimal(bench)

    Collection.create(kernel, "analytics/users", labels=["sql"], created_by="SqlLens",
                   description="User table")

    from keyvalue_lens import KeyValueLens as Lens
    lens = Lens(kernel, "analytics/users")
    lens.put("user:1", {"name": "Alice", "age": 30})
    lens.put("user:2", {"name": "Bob", "age": 25})
    lens.commit("insert 2 users")

    assert lens.get("user:1") == {"name": "Alice", "age": 30}
    assert lens.count() == 2

    vol = Collection(kernel, "analytics/users")
    assert vol.labels == ["sql"]
    assert vol.namespace == "analytics"
    assert vol.basename == "users"

    # Metadata is ONE small blob
    meta_h = kernel.resolve("analytics/users__meta")
    meta_bytes = kernel.read_blob(meta_h)
    assert len(meta_bytes) < 500

    kernel.close()
    shutil.rmtree(bench, ignore_errors=True)
    print("PASS: Collection + Lens integration (namespace path, metadata is small)")


def test_Collection_persists():
    import shutil
    bench = "/tmp/pond_Collection_persist"
    if os.path.exists(bench): shutil.rmtree(bench)
    os.makedirs(bench)
    kernel = PondMinimal(bench)

    Collection.create(kernel, "analytics/orders", labels=["sql"], created_by="SqlLens",
                   description="Orders table")
    kernel.close()

    kernel2 = PondMinimal(bench)
    Collections = Collection.list(kernel2)
    assert len(Collections) == 1
    assert Collections[0]["name"] == "analytics/orders"
    assert "sql" in Collections[0].get("labels", [])

    vol = Collection(kernel2, "analytics/orders")
    assert vol.description == "Orders table"
    assert vol.namespace == "analytics"

    kernel2.close()
    shutil.rmtree(bench, ignore_errors=True)
    print("PASS: Collection metadata persists across restart (with namespace)")


def _run_all_tests():
    print("=== Collection Layer Tests ===\n")
    test_Collection_create_and_list()
    print()
    test_materialized_views()
    print()
    test_Collection_with_lens()
    print()
    test_Collection_persists()
    print("\n=== ALL TESTS PASSED ===")


if __name__ == "__main__":
    _run_all_tests()
