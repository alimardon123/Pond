"""
Shared Collection Lens — base class for all Lenses that store Parquet data.

All Lenses that store tabular data (Lakehouse, Feature Store, Vector, etc.)
extend this class and share the SAME ref namespace:

  collections/{name}/HEAD              — latest commit
  collections/{name}/branches/{branch} — branch commit
  collections/{name}/definition        — optional Lens-specific metadata

This means ANY Lens can read ANY collection's data through the public API:
  lh.read_table("user_features")   # Lakehouse reads FeatureStore's data
  fs.read_features("users")        # FeatureStore reads Lakehouse's data

No kernel bypass. No separate namespaces. No ETL.

Design principle 3.6 (Beautiful): one namespace, one responsibility per Lens.
Design principle 3.7 (Functional): any Lens can access any collection.
"""

from __future__ import annotations

import os
import sys
import json
import time
from typing import Optional, Any

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "pond-core"))
from pond_minimal import PondMinimal  # noqa: E402

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    raise ImportError("pyarrow required")


class CollectionLens:
    """Base class for all Lenses that store Parquet data in collections.

    Shared ref namespace:
      collections/{name}/HEAD
      collections/{name}/branches/{branch}
      collections/{name}/definition

    Subclasses (LakehouseLens, FeatureStoreLens, etc.) add domain-specific
    methods on top of this shared base. All subclasses can read each
    other's collections through the public API.
    """

    def __init__(self, kernel: PondMinimal):
        self.kernel = kernel

    # ------------------------------------------------------------------
    # Shared ref naming (ALL Lenses use this)
    # ------------------------------------------------------------------

    @staticmethod
    def _head_ref(name: str) -> str:
        return f"collections/{name}/HEAD"

    @staticmethod
    def _branch_ref(name: str, branch: str) -> str:
        return f"collections/{name}/branches/{branch}"

    @staticmethod
    def _definition_ref(name: str) -> str:
        return f"collections/{name}/definition"

    # ------------------------------------------------------------------
    # Core write (shared by all Lenses)
    # ------------------------------------------------------------------

    def _write_commit(self, name: str, parquet_hash: str,
                      parent: Optional[str] = None,
                      message: str = "",
                      extra: Optional[dict] = None) -> str:
        """Write a commit blob and update HEAD. Shared by all Lenses."""
        commit = {
            "parquet": parquet_hash,
            "parent": parent,
            "message": message,
            "timestamp": time.time(),
        }
        if extra:
            commit.update(extra)
        commit_bytes = json.dumps(commit, sort_keys=True).encode()
        commit_hash = self.kernel.write(commit_bytes)
        self.kernel.reference(self._head_ref(name), commit_hash)
        return commit_hash

    # ------------------------------------------------------------------
    # Core read (shared by all Lenses — ANY Lens can read ANY collection)
    # ------------------------------------------------------------------

    def read_collection(self, name: str,
                        commit_hash: Optional[str] = None) -> pa.Table:
        """Read a collection's data as a PyArrow Table.

        ANY Lens can call this on ANY collection, regardless of which
        Lens created it. This is the interop contract.

        Args:
            name: collection name
            commit_hash: optional commit hash for time travel (None = HEAD)
        """
        if commit_hash is None:
            commit_hash = self.kernel.resolve(self._head_ref(name))
            if commit_hash is None:
                raise KeyError(f"Collection '{name}' not found")
        commit = json.loads(self.kernel.read(commit_hash))
        parquet_bytes = self.kernel.read(commit["parquet"])
        reader = pa.BufferReader(parquet_bytes)
        return pq.read_table(reader)

    def read_collection_commit(self, name: str,
                               commit_hash: Optional[str] = None) -> dict:
        """Read a collection's commit metadata."""
        if commit_hash is None:
            commit_hash = self.kernel.resolve(self._head_ref(name))
            if commit_hash is None:
                raise KeyError(f"Collection '{name}' not found")
        return json.loads(self.kernel.read(commit_hash))

    def collection_exists(self, name: str) -> bool:
        """Check if a collection exists."""
        return self.kernel.resolve(self._head_ref(name)) is not None

    def list_collections(self) -> list[str]:
        """List all collections."""
        names = self.kernel.list_names()
        collections = set()
        for n in names:
            if n.startswith("collections/") and n.endswith("/HEAD"):
                coll = n[len("collections/"):-len("/HEAD")]
                collections.add(coll)
        return sorted(collections)

    # ------------------------------------------------------------------
    # Shared branching (ALL Lenses use this)
    # ------------------------------------------------------------------

    def branch(self, name: str, branch_name: str) -> str:
        """Create a branch. O(1) — just a ref copy."""
        head = self.kernel.resolve(self._head_ref(name))
        if head is None:
            raise KeyError(f"Collection '{name}' not found")
        self.kernel.reference(self._branch_ref(name, branch_name), head)
        return head

    def read_branch(self, name: str, branch_name: str) -> pa.Table:
        """Read a branch's data."""
        ref = self._branch_ref(name, branch_name)
        commit_hash = self.kernel.resolve(ref)
        if commit_hash is None:
            raise KeyError(f"Branch '{branch_name}' not found in '{name}'")
        return self.read_collection(name, commit_hash)

    def commit_to_branch(self, name: str, branch_name: str,
                         parquet_hash: str, message: str = "",
                         extra: Optional[dict] = None) -> str:
        """Write a commit to a branch (not HEAD)."""
        ref = self._branch_ref(name, branch_name)
        parent = self.kernel.resolve(ref)
        if parent is None:
            raise KeyError(f"Branch '{branch_name}' not found in '{name}'")
        commit = {
            "parquet": parquet_hash,
            "parent": parent,
            "message": message,
            "timestamp": time.time(),
        }
        if extra:
            commit.update(extra)
        commit_bytes = json.dumps(commit, sort_keys=True).encode()
        commit_hash = self.kernel.write(commit_bytes)
        self.kernel.reference(ref, commit_hash)
        return commit_hash

    def merge_branch(self, name: str, branch_name: str) -> str:
        """Union merge: append branch's parquet to HEAD's parquet.
        Creates a 2-parent merge commit."""
        head = self.kernel.resolve(self._head_ref(name))
        branch_ref = self._branch_ref(name, branch_name)
        branch_head = self.kernel.resolve(branch_ref)
        if branch_head is None:
            raise KeyError(f"Branch '{branch_name}' not found")

        # Read both states
        head_table = self.read_collection(name, head)
        branch_table = self.read_collection(name, branch_head)

        # Union merge (append — duplicates possible)
        try:
            merged = pa.concat_tables([head_table, branch_table], promote_options="default")
        except TypeError:
            merged = pa.concat_tables([head_table, branch_table])

        # Write merged Parquet
        sink = pa.BufferOutputStream()
        pq.write_table(merged, sink)
        merged_hash = self.kernel.write(sink.getvalue().to_pybytes())

        # Write merge commit with 2 parents
        commit = {
            "parquet": merged_hash,
            "parent": head,
            "second_parent": branch_head,
            "message": f"merge branch '{branch_name}'",
            "timestamp": time.time(),
        }
        commit_bytes = json.dumps(commit, sort_keys=True).encode()
        commit_hash = self.kernel.write(commit_bytes)
        self.kernel.reference(self._head_ref(name), commit_hash)
        return commit_hash

    # ------------------------------------------------------------------
    # Shared history (ALL Lenses use this)
    # ------------------------------------------------------------------

    def history(self, name: str) -> list[dict]:
        """Walk the commit chain for a collection."""
        head = self.kernel.resolve(self._head_ref(name))
        if head is None:
            return []
        history = []
        current = head
        while current:
            commit = json.loads(self.kernel.read(current))
            history.append({
                "hash": current,
                "message": commit.get("message", ""),
                "row_count": commit.get("row_count"),
                "timestamp": commit.get("timestamp"),
                "parent": commit.get("parent"),
                "second_parent": commit.get("second_parent"),
            })
            current = commit.get("parent")
        return history

    # ------------------------------------------------------------------
    # Shared encoding helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _encode_table(table: pa.Table) -> bytes:
        """Encode a PyArrow Table as Parquet bytes."""
        sink = pa.BufferOutputStream()
        pq.write_table(table, sink)
        return sink.getvalue().to_pybytes()

    @staticmethod
    def _decode_table(parquet_bytes: bytes) -> pa.Table:
        """Decode Parquet bytes into a PyArrow Table."""
        reader = pa.BufferReader(parquet_bytes)
        return pq.read_table(reader)

    # ------------------------------------------------------------------
    # Definition management (optional — Lens-specific metadata)
    # ------------------------------------------------------------------

    def _set_definition(self, name: str, definition: dict) -> str:
        """Store Lens-specific metadata for a collection."""
        defn_bytes = json.dumps(definition, sort_keys=True).encode()
        defn_hash = self.kernel.write(defn_bytes)
        self.kernel.reference(self._definition_ref(name), defn_hash)
        return defn_hash

    def _get_definition(self, name: str) -> Optional[dict]:
        """Read Lens-specific metadata for a collection."""
        h = self.kernel.resolve(self._definition_ref(name))
        if h is None:
            return None
        return json.loads(self.kernel.read(h))
