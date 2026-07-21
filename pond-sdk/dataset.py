"""
Dataset — the named object layer between the Kernel and the Lens.

A Dataset is a named collection of bytes in the kernel with:
  - A commit DAG (history, branches, snapshots)
  - A small metadata blob (type, source lens, description)
  - Zero or more physical structures (indexes, stats)

The Dataset is the "object" — like a table in a database, a repo in
Git, or a notebook in Jupyter. The Lens is how you read/write it.

Architecture:
  Kernel (Bytes, History, Names)
      ↓
  Dataset (named object + metadata)
      ↓
  Physical Structures (indexes, stats — accelerate access)
      ↓
  Lens (interprets bytes, never owns them)

Dataset metadata is ONE small blob per dataset — NOT per record. It's
stored as a kernel reference (Name), like the HEAD reference. The blob
bytes stay pure. This is NOT the TypedBlob envelope approach.

Metadata fields:
  - name: the dataset name (e.g., "orders")
  - type: the lens type that created it (e.g., "sql", "git", "feature_store")
  - source_lens: the lens class name (e.g., "SqlLens")
  - description: human-readable description
  - created_at: creation timestamp
  - is_materialized: True if this is a materialized view of another dataset
  - source_dataset: the source dataset name (if materialized)
  - materialization_type: "index", "aggregate", "transform", etc. (if materialized)

Usage:
    # Create a dataset with metadata
    ds = Dataset.create(kernel, "orders", type="sql", source_lens="SqlLens",
                         description="Customer orders table")

    # Use a Lens to read/write it
    lens = SqlLens(kernel, "orders")
    lens.put("order:1", {"amount": 100})
    lens.commit("insert order 1")

    # List all datasets
    datasets = Dataset.list(kernel)
    # [{"name": "orders", "type": "sql", "source_lens": "SqlLens", ...}]

    # Create a materialized view
    mv = Dataset.create_materialized(kernel, "orders_by_region",
        source_dataset="orders", materialization_type="aggregate",
        type="sql", source_lens="SqlLens",
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
# Dataset — named object + metadata
# ---------------------------------------------------------------------------

class Dataset:
    """A named object in the kernel with metadata.

    A Dataset is the "thing" — a table, a repo, a notebook, a feature
    store. The Lens is how you interact with it. Multiple Lenses can
    share a Dataset.

    The metadata is ONE small JSON blob per dataset, stored as a kernel
    reference (Name: "{name}__meta"). NOT per record. NOT per blob.
    The blob bytes stay pure.
    """

    def __init__(self, kernel: PondMinimal, name: str):
        """Open an existing dataset. Raises ValueError if it doesn't exist."""
        self.kernel = kernel
        self.name = name
        meta = self._read_meta()
        if meta is None:
            raise ValueError(
                f"Dataset '{name}' does not exist. "
                f"Use Dataset.create() to create it."
            )
        self._meta = meta

    # ------------------------------------------------------------------
    # Creation
    # ------------------------------------------------------------------

    @classmethod
    def create(cls, kernel: PondMinimal, name: str,
               type: str = "generic",
               source_lens: str = "",
               description: str = "",
               **extra) -> "Dataset":
        """Create a new dataset with metadata.

        Args:
            kernel: the Pond kernel.
            name: the dataset name (must be unique).
            type: the lens type that created this dataset
                (e.g., "sql", "git", "feature_store", "notebook",
                "arrow", "streaming", "vector", "semantic").
            source_lens: the lens class name (e.g., "SqlLens").
            description: human-readable description.
            **extra: additional metadata fields.

        Returns:
            A Dataset instance.
        """
        # Check if dataset already exists
        meta_ref = f"{name}__meta"
        if kernel.resolve(meta_ref) is not None:
            raise ValueError(
                f"Dataset '{name}' already exists. "
                f"Use Dataset(kernel, '{name}') to open it."
            )

        meta = {
            "name": name,
            "type": type,
            "source_lens": source_lens,
            "description": description,
            "created_at": time.time(),
            "is_materialized": False,
            "source_dataset": None,
            "materialization_type": None,
            **extra,
        }
        # Write the metadata as a single small blob
        meta_bytes = json.dumps(meta, sort_keys=True).encode()
        meta_hash = kernel.write(meta_bytes)
        kernel.reference(meta_ref, meta_hash)

        return cls(kernel, name)

    @classmethod
    def create_materialized(cls, kernel: PondMinimal, name: str,
                            source_dataset: str,
                            materialization_type: str = "transform",
                            type: str = "generic",
                            source_lens: str = "",
                            description: str = "",
                            **extra) -> "Dataset":
        """Create a materialized view of another dataset.

        A materialized view is a dataset whose data is derived from
        another dataset. Examples:
          - An index (materialization_type="index")
          - An aggregate (materialization_type="aggregate")
          - A transform (materialization_type="transform")
          - A join (materialization_type="join")

        The materialized view has its own commit DAG (it's a separate
        dataset), but its metadata records the source dataset.

        Args:
            kernel: the Pond kernel.
            name: the materialized view name.
            source_dataset: the source dataset name.
            materialization_type: "index", "aggregate", "transform", etc.
            type: the lens type.
            source_lens: the lens class name.
            description: human-readable description.
            **extra: additional metadata fields.
        """
        meta = {
            "name": name,
            "type": type,
            "source_lens": source_lens,
            "description": description,
            "created_at": time.time(),
            "is_materialized": True,
            "source_dataset": source_dataset,
            "materialization_type": materialization_type,
            **extra,
        }
        meta_bytes = json.dumps(meta, sort_keys=True).encode()
        meta_hash = kernel.write(meta_bytes)
        kernel.reference(f"{name}__meta", meta_hash)

        return cls(kernel, name)

    # ------------------------------------------------------------------
    # Metadata access
    # ------------------------------------------------------------------

    def _read_meta(self) -> Optional[dict]:
        """Read the dataset's metadata blob."""
        h = self.kernel.resolve(f"{self.name}__meta")
        if h is None:
            return None
        return json.loads(self.kernel.read_blob(h))

    @property
    def type(self) -> str:
        """The lens type that created this dataset (e.g., 'sql', 'git')."""
        return self._meta.get("type", "generic")

    @property
    def source_lens(self) -> str:
        """The lens class name that created this dataset."""
        return self._meta.get("source_lens", "")

    @property
    def description(self) -> str:
        """Human-readable description."""
        return self._meta.get("description", "")

    @property
    def is_materialized(self) -> bool:
        """True if this is a materialized view of another dataset."""
        return self._meta.get("is_materialized", False)

    @property
    def source_dataset(self) -> Optional[str]:
        """The source dataset name (if materialized)."""
        return self._meta.get("source_dataset")

    @property
    def materialization_type(self) -> Optional[str]:
        """The materialization type (if materialized): 'index', 'aggregate', etc."""
        return self._meta.get("materialization_type")

    @property
    def created_at(self) -> float:
        """Creation timestamp."""
        return self._meta.get("created_at", 0)

    def meta(self) -> dict:
        """Return the full metadata dict."""
        return dict(self._meta)

    def update_meta(self, **kwargs) -> None:
        """Update metadata fields. Only updates the provided fields."""
        self._meta.update(kwargs)
        meta_bytes = json.dumps(self._meta, sort_keys=True).encode()
        meta_hash = self.kernel.write(meta_bytes)
        self.kernel.reference(f"{self.name}__meta", meta_hash)

    # ------------------------------------------------------------------
    # Registry — list all datasets in the kernel
    # ------------------------------------------------------------------

    @staticmethod
    def list(kernel: PondMinimal) -> list[dict]:
        """List all datasets in the kernel with their metadata.

        Returns a list of metadata dicts, sorted by name. Each dict
        has at minimum: name, type, source_lens, description,
        is_materialized, source_dataset, materialization_type.

        Example:
            >>> Dataset.list(kernel)
            [
                {"name": "orders", "type": "sql", "source_lens": "SqlLens", ...},
                {"name": "orders_by_region", "type": "sql", "is_materialized": True,
                 "source_dataset": "orders", "materialization_type": "aggregate", ...},
                {"name": "repo", "type": "git", "source_lens": "GitLens", ...},
            ]
        """
        names = kernel.list_names()
        datasets = []
        for name in names:
            if name.endswith("__meta"):
                ds_name = name[:-len("__meta")]
                h = kernel.resolve(name)
                if h:
                    meta = json.loads(kernel.read_blob(h))
                    datasets.append(meta)
        return sorted(datasets, key=lambda m: m.get("name", ""))

    @staticmethod
    def list_by_type(kernel: PondMinimal, type: str) -> list[dict]:
        """List all datasets of a given type (e.g., 'sql', 'git')."""
        return [d for d in Dataset.list(kernel) if d.get("type") == type]

    @staticmethod
    def list_materialized(kernel: PondMinimal) -> list[dict]:
        """List all materialized views."""
        return [d for d in Dataset.list(kernel) if d.get("is_materialized")]

    @staticmethod
    def list_base(kernel: PondMinimal) -> list[dict]:
        """List all base datasets (not materialized)."""
        return [d for d in Dataset.list(kernel) if not d.get("is_materialized")]

    @staticmethod
    def exists(kernel: PondMinimal, name: str) -> bool:
        """Check if a dataset exists."""
        return kernel.resolve(f"{name}__meta") is not None

    @staticmethod
    def open(kernel: PondMinimal, name: str) -> Optional["Dataset"]:
        """Open a dataset. Returns None if it doesn't exist."""
        try:
            return Dataset(kernel, name)
        except ValueError:
            return None


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_dataset_create_and_list():
    """Create datasets, list them, verify metadata."""
    import shutil
    bench = "/tmp/pond_dataset_test"
    if os.path.exists(bench): shutil.rmtree(bench)
    os.makedirs(bench)
    kernel = PondMinimal(bench)

    # Create datasets of different types
    ds1 = Dataset.create(kernel, "orders", type="sql", source_lens="SqlLens",
                          description="Customer orders table")
    ds2 = Dataset.create(kernel, "repo", type="git", source_lens="GitLens",
                          description="Source code repository")
    ds3 = Dataset.create(kernel, "events", type="streaming", source_lens="StreamingLens",
                          description="Event log")
    ds4 = Dataset.create(kernel, "features", type="feature_store", source_lens="FeatureStoreLens",
                          description="ML feature store")

    # List all datasets
    datasets = Dataset.list(kernel)
    assert len(datasets) == 4
    names = [d["name"] for d in datasets]
    assert names == ["events", "features", "orders", "repo"]

    # List by type
    sql_datasets = Dataset.list_by_type(kernel, "sql")
    assert len(sql_datasets) == 1
    assert sql_datasets[0]["name"] == "orders"

    git_datasets = Dataset.list_by_type(kernel, "git")
    assert len(git_datasets) == 1
    assert git_datasets[0]["name"] == "repo"

    # Verify metadata
    ds = Dataset(kernel, "orders")
    assert ds.type == "sql"
    assert ds.source_lens == "SqlLens"
    assert ds.description == "Customer orders table"
    assert ds.is_materialized is False
    assert ds.source_dataset is None

    # exists() and open()
    assert Dataset.exists(kernel, "orders")
    assert not Dataset.exists(kernel, "nonexistent")
    assert Dataset.open(kernel, "orders") is not None
    assert Dataset.open(kernel, "nonexistent") is None

    kernel.close()
    shutil.rmtree(bench, ignore_errors=True)
    print("PASS: Dataset create, list, list_by_type, exists, open")


def test_materialized_views():
    """Create materialized views, list them, verify lineage."""
    import shutil
    bench = "/tmp/pond_materialized_test"
    if os.path.exists(bench): shutil.rmtree(bench)
    os.makedirs(bench)
    kernel = PondMinimal(bench)

    # Create a base dataset
    Dataset.create(kernel, "orders", type="sql", source_lens="SqlLens",
                    description="Customer orders table")

    # Create materialized views
    Dataset.create_materialized(kernel, "orders_by_region",
        source_dataset="orders", materialization_type="aggregate",
        type="sql", source_lens="SqlLens",
        description="Orders aggregated by region")

    Dataset.create_materialized(kernel, "orders_index_customer",
        source_dataset="orders", materialization_type="index",
        type="sql", source_lens="SqlLens",
        description="Index on customer_id")

    # List base datasets
    base = Dataset.list_base(kernel)
    assert len(base) == 1
    assert base[0]["name"] == "orders"

    # List materialized views
    mat = Dataset.list_materialized(kernel)
    assert len(mat) == 2
    mat_names = {d["name"] for d in mat}
    assert mat_names == {"orders_by_region", "orders_index_customer"}

    # Verify materialized view metadata
    mv = Dataset(kernel, "orders_by_region")
    assert mv.is_materialized is True
    assert mv.source_dataset == "orders"
    assert mv.materialization_type == "aggregate"

    # Lineage: materialized view → source dataset
    mv2 = Dataset(kernel, "orders_index_customer")
    assert mv2.source_dataset == "orders"
    assert mv2.materialization_type == "index"

    kernel.close()
    shutil.rmtree(bench, ignore_errors=True)
    print("PASS: Materialized views (create, list, list_base, list_materialized, lineage)")


def test_dataset_with_lens():
    """Dataset + Lens work together: create dataset, use lens to write/read."""
    import shutil
    bench = "/tmp/pond_dataset_lens_test"
    if os.path.exists(bench): shutil.rmtree(bench)
    os.makedirs(bench)
    kernel = PondMinimal(bench)

    # Create a dataset
    Dataset.create(kernel, "users", type="sql", source_lens="SqlLens",
                    description="User table")

    # Use a Lens to write to it
    from lens_sdk import Lens
    lens = Lens(kernel, "users")
    lens.put("user:1", {"name": "Alice", "age": 30})
    lens.put("user:2", {"name": "Bob", "age": 25})
    lens.commit("insert 2 users")

    # Read back via the Lens
    assert lens.get("user:1") == {"name": "Alice", "age": 30}
    assert lens.count() == 2

    # Dataset metadata is independent of the Lens data
    ds = Dataset(kernel, "users")
    assert ds.type == "sql"
    assert ds.description == "User table"

    # List shows the dataset with its type
    datasets = Dataset.list(kernel)
    assert len(datasets) == 1
    assert datasets[0]["name"] == "users"
    assert datasets[0]["type"] == "sql"

    # The metadata blob is ONE small blob — NOT per record
    meta_h = kernel.resolve("users__meta")
    meta_bytes = kernel.read_blob(meta_h)
    assert len(meta_bytes) < 500  # small, not per-record overhead

    kernel.close()
    shutil.rmtree(bench, ignore_errors=True)
    print("PASS: Dataset + Lens integration (create, write via lens, read, metadata)")


def test_dataset_persists_across_restart():
    """Dataset metadata survives process restart."""
    import shutil
    bench = "/tmp/pond_dataset_persist"
    if os.path.exists(bench): shutil.rmtree(bench)
    os.makedirs(bench)
    kernel = PondMinimal(bench)

    Dataset.create(kernel, "orders", type="sql", source_lens="SqlLens",
                    description="Orders table")
    kernel.close()

    # Reopen
    kernel2 = PondMinimal(bench)
    datasets = Dataset.list(kernel2)
    assert len(datasets) == 1
    assert datasets[0]["name"] == "orders"
    assert datasets[0]["type"] == "sql"

    ds = Dataset(kernel2, "orders")
    assert ds.description == "Orders table"

    kernel2.close()
    shutil.rmtree(bench, ignore_errors=True)
    print("PASS: Dataset metadata persists across restart")


def _run_all_tests():
    print("=== Dataset Layer Tests ===\n")
    test_dataset_create_and_list()
    print()
    test_materialized_views()
    print()
    test_dataset_with_lens()
    print()
    test_dataset_persists_across_restart()
    print("\n=== ALL TESTS PASSED ===")


if __name__ == "__main__":
    _run_all_tests()
