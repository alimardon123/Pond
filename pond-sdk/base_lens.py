"""
PondLens — the SHARED NAMESPACE base for all Lenses.

This is NOT a format-aware base class. Per the design goals:

  - ProllyTreeIndex (prolly_tree.py:ProllyLensBase) is the universal
    storage backend for key-value collections. It supports OLTP, OLAP,
    streaming, and point-lookup workloads.
  - App-facing lenses (KeyValueLens, LakehouseLens, VectorLens,
    FeatureStoreLens) inherit from PondLens and add their OWN
    read/write APIs. The base class does NOT decide what to write —
    each lens decides for itself.
  - LakehouseLens ADDS range reads/writes on top of the prolly tree
    as a lens-specific extension. Other lenses do not get them.

What this base provides:
  - Shared ref namespace:
      collections/{name}/HEAD
      collections/{name}/branches/{branch}
      collections/{name}/definition
  - Generic ref-level operations that work on ANY collection's refs,
    regardless of what is inside the blobs:
      - branch(name, branch_name)        — O(1) ref copy
      - list_collections()               — lists all collection names
      - collection_exists(name)
      - set_definition(name, definition) — optional lens-specific metadata
      - get_definition(name)
      - history(name)                    — walks the commit chain

What this base does NOT provide (deliberately):
  - read_collection(name)  — no universal read; each lens reads its own format
  - write_parquet(...)     — Lakehouse-specific, lives on LakehouseLens
  - put/get/commit(...)    — KV-specific, lives on Lens (via ProllyLensBase)
  - _detect_format(...)    — there is no format detection at this layer

History works for both binary commits (ProllyLensBase KV collections)
and JSON commits (Lakehouse/FeatureStore Parquet collections) because
the commit chain is just a parent-pointer walk — the encoding of each
commit blob does not matter at this layer.
"""

from __future__ import annotations

import os
import sys
import json
import time
from typing import Optional, Any

# Make pond-core importable
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "pond-core"))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from kernel import PondMinimal  # noqa: E402

# Binary commit decoding is needed by history() to walk KV commit chains.
# Imported lazily inside history() so this file stays importable even if
# binary_encoding is not yet on the path (e.g. during bootstrap).
try:
    from binary_encoding import BinaryProllyTree  # noqa: E402
except ImportError:
    BinaryProllyTree = None


class PondLens:
    """Shared namespace base for all Pond Lenses.

    This class is deliberately small. It only owns:
      1. The ref namespace conventions (collections/{name}/...).
      2. Generic operations that operate on REFS, not on blob contents.
      3. A history() walker that handles both binary and JSON commits.

    App-facing subclasses (Lens, LakehouseLens, FeatureStoreLens, ...)
    inherit from this class and add their OWN read/write APIs. The base
    class does not know whether a collection stores Parquet blobs, KV
    pairs, or anything else — it only knows about the commit chain
    (HEAD → commit → parent → ...).

    See DESIGN_GOALS.md §3 (the seven principles) and the worklog entry
    for this refactor.
    """

    def __init__(self, kernel: PondMinimal):
        self.kernel = kernel

    # ==================================================================
    # Ref namespace helpers (shared by ALL lenses)
    # ==================================================================

    @staticmethod
    def _head_ref(name: str) -> str:
        return f"collections/{name}/HEAD"

    @staticmethod
    def _branch_ref(name: str, branch: str) -> str:
        return f"collections/{name}/branches/{branch}"

    @staticmethod
    def _definition_ref(name: str) -> str:
        return f"collections/{name}/definition"

    # ==================================================================
    # Generic operations on the namespace (no format awareness)
    # ==================================================================

    def branch(self, name: str, branch_name: str) -> str:
        """Create a branch on ANY collection. O(1) — just a ref copy.

        Works for any collection regardless of whether its blobs are
        Parquet, KV, or something else, because branching only copies
        the HEAD ref to a new branch ref. The blobs are not touched.
        """
        head = self.kernel.resolve(self._head_ref(name))
        if head is None:
            raise KeyError(f"Collection '{name}' not found")
        self.kernel.reference(self._branch_ref(name, branch_name), head)
        return head

    def collection_exists(self, name: str) -> bool:
        """Check if a collection has a HEAD ref."""
        return self.kernel.resolve(self._head_ref(name)) is not None

    def list_collections(self) -> list[str]:
        """List ALL collections (any lens, any format).

        Collections are identified by the `collections/{name}/HEAD` ref
        pattern. This works for any lens because they all share the
        same namespace convention.
        """
        names = self.kernel.list_names()
        collections = set()
        for n in names:
            if n.startswith("collections/") and n.endswith("/HEAD"):
                coll = n[len("collections/"):-len("/HEAD")]
                collections.add(coll)
        return sorted(collections)

    def set_definition(self, name: str, definition: dict) -> str:
        """Store Lens-specific metadata for a collection (optional).

        This is the only "metadata" the base class knows about. The
        definition blob is a JSON dict stored at
        `collections/{name}/definition`. Each lens decides what to put
        in it (feature definitions, table schema, vector index config,
        etc.). The base class treats it as opaque JSON.
        """
        defn_bytes = json.dumps(definition, sort_keys=True).encode()
        defn_hash = self.kernel.write(defn_bytes)
        self.kernel.reference(self._definition_ref(name), defn_hash)
        return defn_hash

    def get_definition(self, name: str) -> Optional[dict]:
        """Read Lens-specific metadata for a collection."""
        h = self.kernel.resolve(self._definition_ref(name))
        if h is None:
            return None
        return json.loads(self.kernel.read(h))

    # ==================================================================
    # History — walks commit chain for ANY collection
    # ==================================================================

    def history(self, name: str, limit: int = 100) -> list[dict]:
        """Walk the commit chain for ANY collection.

        Works for both:
          - Binary commits (ProllyLensBase KV collections) — decoded
            via BinaryProllyTree.decode_commit
          - JSON commits (Lakehouse, FeatureStore Parquet collections)
            — decoded via json.loads

        Returns a unified list of dicts:
          {hash, message, parent, second_parent, timestamp, type, ...}

        The walk stops at the first commit that cannot be decoded
        (e.g. a tombstone or a foreign format) to avoid silent
        corruption.
        """
        head = self.kernel.resolve(self._head_ref(name))
        if head is None:
            return []

        history: list[dict] = []
        current: Optional[str] = head
        seen: set[str] = set()  # cycle guard

        while current and current not in seen and len(history) < limit:
            seen.add(current)
            raw = self.kernel.read_blob(current)
            entry = self._decode_commit_entry(current, raw)
            if entry is None:
                # Cannot decode — stop the walk to avoid silent corruption.
                history.append({
                    "hash": current,
                    "message": "(undecodable commit)",
                    "parent": None,
                    "second_parent": None,
                    "timestamp": None,
                    "type": "unknown",
                })
                break
            history.append(entry)
            current = entry.get("parent")

        return history

    @staticmethod
    def _decode_commit_entry(commit_hash: str, raw: bytes) -> Optional[dict]:
        """Decode a commit blob into a unified history entry.

        Tries binary first (ProllyLensBase KV commits start with a
        type byte of 3), then falls back to JSON (Lakehouse/
        FeatureStore commits). Returns None if neither decoder
        matches.
        """
        # Binary commit? Type byte 3 = commit (see binary_encoding.py).
        if BinaryProllyTree is not None and len(raw) > 0 and raw[0] == 3:
            try:
                commit = BinaryProllyTree.decode_commit(raw)
                return {
                    "hash": commit_hash,
                    "message": commit.get("message", ""),
                    "parent": commit.get("parent"),
                    "second_parent": commit.get("second_parent"),
                    "timestamp": commit.get("timestamp"),
                    "index": commit.get("index"),
                    "type": "snapshot" if commit.get("snapshot") else "delta",
                }
            except (ValueError, IndexError):
                pass

        # JSON commit? (Lakehouse, FeatureStore)
        try:
            commit = json.loads(raw)
            if isinstance(commit, dict):
                entry_type = "merge" if commit.get("second_parent") else "commit"
                return {
                    "hash": commit_hash,
                    "message": commit.get("message", ""),
                    "parent": commit.get("parent"),
                    "second_parent": commit.get("second_parent"),
                    "timestamp": commit.get("timestamp"),
                    "row_count": commit.get("row_count"),
                    "type": entry_type,
                }
        except (json.JSONDecodeError, UnicodeDecodeError):
            pass

        return None
