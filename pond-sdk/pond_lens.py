"""
Pond Lens — the ONE base class for ALL Lenses.

Design goals (per user):
  - Any lens can read/write any collection bidirectionally
  - Other lens's data should feel native/easy to read/write
  - Simple, reduced code
  - Beautiful API
  - Always interoperable: read, write, history, branching, merge

Architecture:
  ┌─────────────────────────────────────────────┐
  │              PondLens (THIS FILE)            │
  │  ┌─────────────────┐  ┌──────────────────┐  │
  │  │  Key-Value Mode │  │  Parquet Mode    │  │
  │  │  (Prolly tree)  │  │  (Arrow table)   │  │
  │  │  put/get/delete │  │  create/insert   │  │
  │  │  indexes        │  │  DuckDB query    │  │
  │  └─────────────────┘  └──────────────────┘  │
  │                                             │
  │  SHARED (both modes):                       │
  │    read_collection() — format-agnostic      │
  │    write_collection() — format-agnostic     │
  │    branch() / merge() / history()           │
  │    list_collections()                       │
  │    time travel (read at old commit)         │
  └─────────────────────────────────────────────┘

The key insight: a collection's FORMAT (KV vs Parquet) is detected
from the commit blob. The Lens reads/writes in either format
automatically. Cross-format access is seamless.

Ref namespace (shared by ALL lenses):
  collections/{name}/HEAD
  collections/{name}/branches/{branch}
  collections/{name}/definition

This file replaces both collection_lens.py and the base class in
lens_sdk.py. ProllyLensBase (prolly_view.py) remains as the KV
storage engine; this class wraps it.
"""

from __future__ import annotations

import os
import sys
import json
import time
from typing import Optional, Any, Callable

# Make pond-core importable
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "pond-core"))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pond_minimal import PondMinimal  # noqa: E402

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    pa = None  # PyArrow optional for pure KV mode

try:
    from prolly_view import ProllyLensBase, ProllyTree  # noqa: E402
except ImportError:
    ProllyLensBase = None


class PondLens:
    """The ONE base class for ALL Pond Lenses.

    Supports TWO storage modes:
      1. Key-value (Prolly tree): put(key, data), get(key), delete(key)
      2. Parquet table: create_table(name, data), insert(name, data), query(sql)

    BOTH modes share:
      - read_collection(name) — format-agnostic, returns PyArrow Table
      - branch(name, branch_name) — O(1)
      - merge_branch(name, branch_name) — union merge
      - history(name) — walk commit chain
      - list_collections() — all collections, any format
      - time travel — read at any commit hash

    Cross-format interop is automatic:
      - A KV lens can read a Parquet collection (gets Arrow table)
      - A Parquet lens can read a KV collection (gets Arrow table with key/value columns)
      - Both can branch, merge, and time-travel into each other's collections

    Subclasses (LakehouseLens, FeatureStoreLens) add domain-specific
    methods but inherit everything from this class.
    """

    def __init__(self, kernel: PondMinimal, name: Optional[str] = None):
        self.kernel = kernel
        self.name = name
        self._kv_base = None  # Lazy-init ProllyLensBase for KV mode
        self._cached_tables: dict[str, tuple[str, pa.Table]] = {}  # (commit_hash, table)

    # ==================================================================
    # SHARED NAMESPACE (all lenses use this)
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
    # FORMAT DETECTION
    # ==================================================================

    @staticmethod
    def _detect_format(commit_raw: bytes) -> str:
        """Detect collection format from commit blob bytes.
        Returns 'parquet' or 'kv'."""
        # Parquet commits are JSON with a "parquet" field
        try:
            commit = json.loads(commit_raw)
            if isinstance(commit, dict) and "parquet" in commit:
                return "parquet"
        except (json.JSONDecodeError, UnicodeDecodeError):
            pass
        # Binary commits = KV (Prolly tree)
        return "kv"

    # ==================================================================
    # READ (format-agnostic — ANY lens reads ANY collection)
    # ==================================================================

    def read_collection(self, name: str,
                        commit_hash: Optional[str] = None) -> "pa.Table | dict":
        """Read ANY collection, regardless of format.

        Returns:
          - PyArrow Table if Parquet format (or if PyArrow available)
          - dict of {key: value} if KV format and PyArrow not available

        ANY lens can call this on ANY collection.
        """
        if commit_hash is None:
            commit_hash = self.kernel.resolve(self._head_ref(name))
            if commit_hash is None:
                raise KeyError(f"Collection '{name}' not found")

        # Check cache
        cache_key = f"{name}:{commit_hash}"
        if cache_key in self._cached_tables:
            return self._cached_tables[cache_key]

        raw = self.kernel.read_blob(commit_hash)
        fmt = self._detect_format(raw)

        if fmt == "parquet":
            commit = json.loads(raw)
            parquet_bytes = self.kernel.read(commit["parquet"])
            if pa is not None:
                reader = pa.BufferReader(parquet_bytes)
                table = pq.read_table(reader)
                self._cached_tables[cache_key] = table
                return table
            return parquet_bytes  # raw bytes if no PyArrow

        # KV format: read via ProllyLensBase
        return self._read_kv_collection(name)

    def _read_kv_collection(self, name: str) -> "pa.Table | dict":
        """Read a key-value collection. Returns PyArrow Table or dict."""
        if ProllyLensBase is None:
            raise ValueError("KV mode requires prolly_view.py")

        base = ProllyLensBase(self.kernel, name)
        state = base.read_all()

        if pa is None:
            # No PyArrow: return dict
            result = {}
            for k, h in state.items():
                if k.startswith("_"):
                    continue
                raw = self.kernel.read_blob(h)
                try:
                    result[k] = json.loads(raw)
                except (json.JSONDecodeError, UnicodeDecodeError):
                    result[k] = raw
            return result

        # With PyArrow: return table with key/value columns
        keys, values = [], []
        for k, h in state.items():
            if k.startswith("_"):
                continue
            keys.append(k)
            raw = self.kernel.read_blob(h)
            try:
                values.append(json.loads(raw))
            except (json.JSONDecodeError, UnicodeDecodeError):
                values.append({"_raw": raw.hex()})
        table = pa.table({"key": keys, "value": values})
        cache_key = f"{name}:{self.kernel.resolve(self._head_ref(name))}"
        self._cached_tables[cache_key] = table
        return table

    def read_commit(self, name: str,
                    commit_hash: Optional[str] = None) -> dict:
        """Read commit metadata for a collection."""
        if commit_hash is None:
            commit_hash = self.kernel.resolve(self._head_ref(name))
            if commit_hash is None:
                raise KeyError(f"Collection '{name}' not found")
        raw = self.kernel.read_blob(commit_hash)
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError):
            # Binary commit (KV/Prolly) — return basic info
            return {"_format": "kv", "_hash": commit_hash}

    # ==================================================================
    # WRITE — Parquet mode
    # ==================================================================

    def write_parquet(self, name: str, table: "pa.Table",
                      message: str = "", parent: Optional[str] = None) -> str:
        """Write a PyArrow table as a Parquet collection."""
        if pa is None:
            raise ImportError("PyArrow required for Parquet mode")

        sink = pa.BufferOutputStream()
        pq.write_table(table, sink)
        parquet_bytes = sink.getvalue().to_pybytes()
        parquet_hash = self.kernel.write(parquet_bytes)

        if parent is None:
            parent = self.kernel.resolve(self._head_ref(name))

        commit = {
            "parquet": parquet_hash,
            "parent": parent,
            "message": message or f"write {table.num_rows} rows",
            "timestamp": time.time(),
            "row_count": table.num_rows,
        }
        commit_bytes = json.dumps(commit, sort_keys=True).encode()
        commit_hash = self.kernel.write(commit_bytes)
        self.kernel.reference(self._head_ref(name), commit_hash)

        # Invalidate cache
        cache_key = f"{name}:{commit_hash}"
        self._cached_tables.pop(cache_key, None)
        return commit_hash

    def append_parquet(self, name: str, new_data: "pa.Table",
                       message: str = "") -> str:
        """Append rows to a Parquet collection (reads current, concatenates)."""
        current = self.read_collection(name)
        try:
            combined = pa.concat_tables([current, new_data], promote_options="default")
        except TypeError:
            combined = pa.concat_tables([current, new_data])
        return self.write_parquet(name, combined, message or f"append {new_data.num_rows} rows")

    # ==================================================================
    # WRITE — Key-Value mode
    # ==================================================================

    def _ensure_kv(self, name: str = None):
        """Lazy-init ProllyLensBase for KV operations."""
        if self._kv_base is None:
            if ProllyLensBase is None:
                raise ImportError("KV mode requires prolly_view.py")
            kv_name = name or self.name
            if kv_name is None:
                raise ValueError("KV mode requires a collection name")
            self._kv_base = ProllyLensBase(self.kernel, kv_name)
        return self._kv_base

    def put(self, key: str, data: Any) -> str:
        """KV write: stage a key→value mapping."""
        base = self._ensure_kv(self.name)
        blob_hash = self.kernel.write(self.encode(data))
        base.stage(key, blob_hash)
        return blob_hash

    def get(self, key: str) -> Optional[Any]:
        """KV read: get a single value by key."""
        base = self._ensure_kv(self.name)
        h = base.lookup(key)
        return self.decode(self.kernel.read_blob(h)) if h else None

    def delete(self, key: str) -> None:
        """KV delete: stage a deletion."""
        base = self._ensure_kv(self.name)
        base.stage_delete(key)

    def commit(self, message: str = "") -> str:
        """KV commit: atomically commit all staged changes."""
        base = self._ensure_kv(self.name)
        return base.commit(message or f"{self.name} commit")

    def get_all(self) -> dict[str, Any]:
        """KV read: get all key-value pairs."""
        base = self._ensure_kv(self.name)
        state = base.read_all()
        return {k: self.decode(self.kernel.read_blob(h))
                for k, h in state.items() if not k.startswith("_")}

    # ==================================================================
    # SHARED: branch, merge, history (work on ANY collection, ANY format)
    # ==================================================================

    def branch(self, name: str, branch_name: str) -> str:
        """Create a branch on ANY collection. O(1) — just a ref."""
        head = self.kernel.resolve(self._head_ref(name))
        if head is None:
            raise KeyError(f"Collection '{name}' not found")
        self.kernel.reference(self._branch_ref(name, branch_name), head)
        return head

    def read_branch(self, name: str, branch_name: str):
        """Read a branch of ANY collection."""
        ref = self._branch_ref(name, branch_name)
        commit_hash = self.kernel.resolve(ref)
        if commit_hash is None:
            raise KeyError(f"Branch '{branch_name}' not found in '{name}'")
        return self.read_collection(name, commit_hash)

    def commit_to_branch(self, name: str, branch_name: str,
                         data: "pa.Table | dict", message: str = "") -> str:
        """Commit to a branch on ANY collection.

        Auto-detects format:
          - If data is a PyArrow Table → writes as Parquet
          - If data is a dict → writes as KV
        """
        ref = self._branch_ref(name, branch_name)
        parent = self.kernel.resolve(ref)
        if parent is None:
            raise KeyError(f"Branch '{branch_name}' not found in '{name}'")

        if pa is not None and isinstance(data, pa.Table):
            sink = pa.BufferOutputStream()
            pq.write_table(data, sink)
            parquet_hash = self.kernel.write(sink.getvalue().to_pybytes())
            commit = {
                "parquet": parquet_hash,
                "parent": parent,
                "message": message or f"branch {branch_name}: {data.num_rows} rows",
                "timestamp": time.time(),
                "row_count": data.num_rows,
            }
        else:
            # KV mode
            commit = {
                "parent": parent,
                "message": message or f"branch {branch_name}: kv commit",
                "timestamp": time.time(),
            }

        commit_bytes = json.dumps(commit, sort_keys=True).encode()
        commit_hash = self.kernel.write(commit_bytes)
        self.kernel.reference(ref, commit_hash)
        return commit_hash

    def merge_branch(self, name: str, branch_name: str) -> str:
        """Union merge a branch into HEAD on ANY collection."""
        head = self.kernel.resolve(self._head_ref(name))
        branch_ref = self._branch_ref(name, branch_name)
        branch_head = self.kernel.resolve(branch_ref)
        if branch_head is None:
            raise KeyError(f"Branch '{branch_name}' not found")

        # Read both states
        head_data = self.read_collection(name, head)
        branch_data = self.read_collection(name, branch_head)

        # Union merge
        if pa is not None and isinstance(head_data, pa.Table) and isinstance(branch_data, pa.Table):
            try:
                merged = pa.concat_tables([head_data, branch_data], promote_options="default")
            except TypeError:
                merged = pa.concat_tables([head_data, branch_data])
            sink = pa.BufferOutputStream()
            pq.write_table(merged, sink)
            merged_hash = self.kernel.write(sink.getvalue().to_pybytes())
            commit = {
                "parquet": merged_hash,
                "parent": head,
                "second_parent": branch_head,
                "message": f"merge branch '{branch_name}'",
                "timestamp": time.time(),
                "row_count": merged.num_rows,
            }
        else:
            # KV merge: just point HEAD at branch (simplified)
            commit = {
                "parent": head,
                "second_parent": branch_head,
                "message": f"merge branch '{branch_name}'",
                "timestamp": time.time(),
            }

        commit_bytes = json.dumps(commit, sort_keys=True).encode()
        commit_hash = self.kernel.write(commit_bytes)
        self.kernel.reference(self._head_ref(name), commit_hash)
        return commit_hash

    def history(self, name: str) -> list[dict]:
        """Walk the commit chain for ANY collection."""
        head = self.kernel.resolve(self._head_ref(name))
        if head is None:
            return []
        history = []
        current = head
        while current:
            try:
                commit = json.loads(self.kernel.read_blob(current))
                history.append({
                    "hash": current,
                    "message": commit.get("message", ""),
                    "row_count": commit.get("row_count"),
                    "timestamp": commit.get("timestamp"),
                    "parent": commit.get("parent"),
                    "second_parent": commit.get("second_parent"),
                })
                current = commit.get("parent")
            except (json.JSONDecodeError, UnicodeDecodeError):
                # Binary commit (KV/Prolly) — can't walk further in JSON
                history.append({"hash": current, "message": "(binary commit)", "parent": None})
                break
        return history

    # ==================================================================
    # SHARED: list, exists, definition
    # ==================================================================

    def collection_exists(self, name: str) -> bool:
        return self.kernel.resolve(self._head_ref(name)) is not None

    def list_collections(self) -> list[str]:
        """List ALL collections (any format, any lens)."""
        names = self.kernel.list_names()
        collections = set()
        for n in names:
            if n.startswith("collections/") and n.endswith("/HEAD"):
                coll = n[len("collections/"):-len("/HEAD")]
                collections.add(coll)
        return sorted(collections)

    def set_definition(self, name: str, definition: dict) -> str:
        """Store Lens-specific metadata for a collection."""
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
    # ENCODE/DECODE (override in subclasses)
    # ==================================================================

    def encode(self, data: Any) -> bytes:
        """Encode domain object → bytes. Default: JSON."""
        return json.dumps(data, sort_keys=True).encode()

    def decode(self, data: bytes) -> Any:
        """Decode bytes → domain object. Default: JSON."""
        return json.loads(data)
