"""
AutoIndexMixin — automatic indexing extension for KeyValueLens.

DESIGN: Auto-indexing is a CROSS-CUTTING EXTENSION, not a separate
lens class. It composes with KeyValueLens via mixin inheritance:

    class MyLens(KeyValueLens, AutoIndexMixin):
        ...

Or use the convenience class:

    class IndexedLens(KeyValueLens, AutoIndexMixin):
        pass

Indexes are derived structures (Prolly trees of key→blob_hash).
Data blobs are NEVER modified when indexes change — index operations
are metadata only.

Modes:
  - "eager": update index on every commit (slow writes, always-fresh reads)
  - "lazy": update index on read when stale (fast writes, eventually-fresh reads)
  - "background": update index in background thread (fast writes, periodic refresh)

Default: "lazy" with staleness_budget=5 (rebuild after 5 commits).

How it works:
  - When a lens registers an auto-index, it specifies a "staleness budget"
    (e.g., "index can be up to 5 commits stale")
  - On commit: EAGER indexes are updated (slow); LAZY indexes are NOT
    updated (O(1) write, fast)
  - On lookup: check if index is stale (commit count exceeded budget)
    - If fresh: use the index (O(log N) lookup)
    - If stale: rebuild index from current data, then use it
  - Optionally: a background thread can refresh indexes proactively

This gives:
  - Fast writes (O(1) for LAZY, O(N) for EAGER)
  - Fast reads when index is fresh (O(log N))
  - Correct reads when index is stale (rebuild + lookup = O(N) + O(log N))
  - User control: set staleness budget per index
"""

import json
import time
import sys
import os
import uuid
from typing import Optional, Any, Callable, Union

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "pond-core"))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from kernel import PondMinimal
from prolly_tree import ProllyLensBase, ProllyTree
from binary_encoding import BinaryProllyTree
from maintenance import drop_name, is_dropped, resolve_active, TOMBSTONE_HASH


# ---------------------------------------------------------------------------
# AutoIndex — configuration for one automatic index
# ---------------------------------------------------------------------------

class AutoIndex:
    """Configuration for an automatic index.

    The extractor may return either:
      - a single string (single-key index): one row -> one index entry
      - a list of strings (multi-key index): one row -> multiple index
        entries, one per returned key. Use this to index list-valued
        fields (e.g., tags, categories) or to allow the same row to
        appear under multiple lookup keys.
    """

    def __init__(self, name: str, extractor: Callable[[Any], Union[str, list[str]]],
                 mode: str = "lazy", staleness_budget: int = 5,
                 incremental: bool = True):
        self.name = name
        self.extractor = extractor
        self.mode = mode  # "eager", "lazy", "background"
        self.staleness_budget = staleness_budget  # max commits before rebuild
        self.last_built_at_commit = -1  # commit index when last built
        self.tree_root: Optional[str] = None  # cached tree root
        self.incremental = incremental  # use incremental updates vs full rebuild
        self.pending_additions: dict[str, str] = {}  # index_key -> blob_hash (not yet merged)
        self.pending_deletions: set[str] = set()  # index_keys to remove
        self._cached_entries: Optional[dict[str, str]] = None  # in-memory cache of index entries

    @staticmethod
    def extract_keys(extractor, data) -> list[str]:
        """Call the extractor and normalize the result to a list of keys.

        Handles three cases:
          - extractor returns a single str: wrap in a list.
          - extractor returns a list of str: use as-is.
          - extractor returns None or empty list: return [] (row not
            indexed).
        """
        result = extractor(data)
        if result is None:
            return []
        if isinstance(result, str):
            return [result]
        if isinstance(result, list):
            return [str(k) for k in result if k is not None]
        # Coerce other types (int, etc.) to string for safety
        return [str(result)]


# ---------------------------------------------------------------------------
# AutoIndexMixin — composable with ANY Pond lens (KV or tabular)
# ---------------------------------------------------------------------------

class AutoIndexMixin:
    """Mixin that adds automatic indexing to ANY Pond lens.

    EXTENSION METADATA:
      extension_type: "mixin"
      supported_lens_types: ["KeyValueLens", "KeylessLens", "SemanticLens", "LakehouseLens", "FeatureStoreLens"]
      supported_storage: ["ProllyTreeIndex"]
      not_supported: []

    TWO MODES OF OPERATION:

    1. KV-style (legacy, for KeyValueLens and subclasses):
       Uses self.base (ProllyLensBase) for staging + committing. Indexes
       map index_key → blob_hash. The lens's key IS the row identifier.

    2. Row-level (generic, for ANY lens including tabular):
       Uses _rowid (UUIDv7) as the universal row identifier. Indexes map
       index_key → _rowid. The lens must implement:
         - self._scan_rows() -> iterator of (rowid, row_dict)
         - self._get_row(rowid) -> Optional[row_dict]
       For KV lenses, _scan_rows iterates keys()+get() and rowid = key.
       For tabular lenses, _scan_rows iterates table rows and rowid = _rowid column.

    The _rowid approach is inspired by PostgreSQL's ctid and Apache Hudi's
    _hoodie_record_key. UUIDv7 (time-ordered) is used for distributed
    read/write support — no central ID allocator needed.

    Use by mixing with any lens:

        from keyvalue_lens import KeyValueLens
        from extensions.indexing.auto_index import AutoIndexMixin

        class MyIndexedLens(KeyValueLens, AutoIndexMixin):
            pass

        # Or with LakehouseLens:
        from lakehouse_lens import LakehouseLens
        class IndexedLakehouse(LakehouseLens, AutoIndexMixin):
            pass

    Adds:
      - register_index(name, extractor, mode, staleness_budget)
      - unregister_index(name)
      - find_by(index_name, index_key) -> Optional[Any]
      - find_all_by(index_name, index_key) -> list[Any]
      - list_auto_indexes() -> list[str]
      - get_index_staleness(index_name) -> int
      - refresh_all_indexes()

    The mixin OVERRIDES `put`, `delete`, and `commit` to track index
    changes (KV-style mode). For tabular lenses, use rebuild_index_after_write()
    after batch writes since the put/delete/commit override doesn't apply.
    """

    # Extension metadata (for introspection / tooling)
    extension_type = "mixin"
    supported_lens_types = ["KeyValueLens", "KeylessLens", "SemanticLens",
                            "LakehouseLens", "FeatureStoreLens"]
    supported_storage = ["ProllyTreeIndex"]
    not_supported = []

    # The hidden row identifier column name (for tabular lenses).
    # Like PostgreSQL's ctid — a system column that uniquely identifies each row.
    # UUIDv7 is used for time-ordered, distributed-friendly generation.
    ROWID_COLUMN = "_rowid"

    def _init_auto_index(self):
        """Call this from __init__ to initialize auto-index state.

        Subclasses call this after super().__init__():
            super().__init__(kernel, name)
            self._init_auto_index()
        """
        self._auto_indexes: dict[str, AutoIndex] = {}
        self._commit_count = 0

    # ------------------------------------------------------------------
    # Generic row-level interface (works with ANY lens)
    #
    # These methods provide a universal row-iteration interface that works
    # for both KV lenses (rowid = key) and tabular lenses (rowid = _rowid column).
    # Tabular lenses override these to scan Parquet row groups.
    # ------------------------------------------------------------------

    def _scan_rows(self):
        """Yield (rowid, row_dict) for every row in the collection.

        DEFAULT (KV-style): uses self.base.read_all() + self.get().
        Tabular lenses (LakehouseLens) override this to scan row groups.

        Yields:
            (rowid: str, row: dict) tuples
        """
        # KV-style: rowid = key, row = decoded value
        state = self.base.read_all()
        for key, blob_hash in state.items():
            if key.startswith("_"):
                continue
            row = self.decode(self.kernel.read_blob(blob_hash))
            yield key, row

    def _get_row(self, rowid: str) -> Optional[Any]:
        """Get a single row by its rowid.

        DEFAULT (KV-style): uses self.get(rowid).
        Tabular lenses override this to scan for the _rowid column.

        Returns:
            The row dict, or None if not found.
        """
        # KV-style: rowid = key
        return self.get(rowid)

    def _is_tabular(self) -> bool:
        """Check if this lens is tabular (stores row groups, not individual keys).

        Tabular lenses override this to return True. The mixin uses this to
        decide whether to use KV-style index tracking (put/delete/commit overrides)
        or batch-rebuild index tracking.
        """
        return False

    def rebuild_index_after_write(self, index_name: Optional[str] = None) -> None:
        """Rebuild index(es) after a batch write (for tabular lenses).

        Tabular lenses (LakehouseLens) do batch writes (create_table, insert)
        that bypass the put/delete/commit overrides. After such a write, call
        this method to rebuild the affected indexes.

        Args:
            index_name: specific index to rebuild, or None for all indexes.
        """
        if not hasattr(self, '_auto_indexes'):
            return
        if index_name:
            idx = self._auto_indexes.get(index_name)
            if idx:
                self._rebuild_index(idx)
        else:
            for idx in self._auto_indexes.values():
                self._rebuild_index(idx)

    # ------------------------------------------------------------------
    # Register/unregister indexes
    # ------------------------------------------------------------------

    def register_index(self, name: str, extractor: Callable[[Any], Union[str, list[str]]],
                       mode: str = "lazy", staleness_budget: int = 5,
                       collection: Optional[str] = None) -> None:
        """Register an automatic index.

        Args:
            name: the index name (appears in `f"{self.name}__index__{name}"`).
            extractor: function(decoded_data) -> str | list[str].
            mode: 'eager' (rebuild on commit), 'lazy' (rebuild on read
                when stale, default), 'background' (not yet implemented).
            staleness_budget: max commits before a lazy index is rebuilt.
            collection: for tabular lenses (LakehouseLens), the collection
                name to index. KV lenses ignore this (they're bound to one
                collection via __init__).
        """
        if not hasattr(self, '_auto_indexes'):
            self._init_auto_index()
        self._auto_indexes[name] = AutoIndex(name, extractor, mode, staleness_budget)
        # For tabular lenses, track which collection this index is for.
        if collection is not None:
            self._indexed_collection = collection

    def unregister_index(self, name: str) -> None:
        """Remove an auto-index (tombstone pattern per RFC-0008)."""
        if not hasattr(self, '_auto_indexes'):
            return
        self._auto_indexes.pop(name, None)
        ref_name = f"{self.name}__index__{name}"
        current = self.kernel.resolve(ref_name)
        if current and current != TOMBSTONE_HASH:
            drop_name(self.kernel, ref_name)

    def is_index_registered(self, name: str) -> bool:
        """True iff the index is registered AND not tombstoned."""
        if not hasattr(self, '_auto_indexes') or name not in self._auto_indexes:
            return False
        ref_name = f"{self.name}__index__{name}"
        if self.kernel.resolve(ref_name) == TOMBSTONE_HASH:
            return False
        return True

    def list_auto_indexes(self) -> list[str]:
        if not hasattr(self, '_auto_indexes'):
            return []
        return list(self._auto_indexes.keys())

    # ------------------------------------------------------------------
    # Write path overrides — track changes for incremental updates
    # ------------------------------------------------------------------

    def put(self, key: str, data: Any) -> str:
        """Override: stage data + track for incremental index updates.

        For KV lenses, the rowid = key. The index stores rowid as a blob
        (because ProllyTree values must be hex blob hashes).
        """
        if not hasattr(self, '_auto_indexes'):
            # No auto-index state — delegate to base lens put
            return super().put(key, data)
        blob_hash = self.kernel.write(self.encode(data))
        self.base.stage(key, blob_hash)
        # Track for incremental index updates.
        # Store rowid (= key for KV lenses) as a blob; use its hash.
        for idx in self._auto_indexes.values():
            if idx.incremental and idx.tree_root is not None:
                idx_keys = AutoIndex.extract_keys(idx.extractor, data)
                for idx_key in idx_keys:
                    full_key = f"_index/{idx.name}/{idx_key}"
                    rowid_blob_hash = self.kernel.write(str(key).encode())
                    idx.pending_additions[full_key] = rowid_blob_hash
        return blob_hash

    def delete(self, key: str) -> None:
        """Override: stage deletion + mark indexes for rebuild."""
        if not hasattr(self, '_auto_indexes'):
            return super().delete(key)
        self.base.stage_delete(key)
        # Deletions handled during incremental merge — can't compute old
        # index key without reading the old data first.

    def commit(self, message: str = "") -> str:
        """Override: commit + update EAGER indexes."""
        if not hasattr(self, '_auto_indexes'):
            return super().commit(message)
        commit_hash = super().commit(message or f"{self.name} commit")
        self._commit_count += 1

        # Only update EAGER indexes on commit
        for idx in self._auto_indexes.values():
            if idx.mode == "eager":
                if idx.incremental and idx.tree_root is not None:
                    self._incremental_update_index(idx)
                else:
                    self._rebuild_index(idx)
            # LAZY and BACKGROUND: skip (deferred to read time)

        return commit_hash

    # ------------------------------------------------------------------
    # Index-based lookups (the whole point of auto-indexing)
    # ------------------------------------------------------------------

    def find_by(self, index_name: str, index_key: str) -> Optional[Any]:
        """Look up data via an auto-index. Rebuilds or incrementally
        updates if stale. Returns None if not found or index is tombstoned.
        """
        if not hasattr(self, '_auto_indexes'):
            raise ValueError(f"Index '{index_name}' not registered (no auto-index state)")
        idx = self._auto_indexes.get(index_name)
        if not idx:
            raise ValueError(f"Index '{index_name}' not registered")

        # Tombstone check
        ref_name = f"{self.name}__index__{index_name}"
        if self.kernel.resolve(ref_name) == TOMBSTONE_HASH:
            return None

        # Build or refresh the index if needed
        if idx.tree_root is None:
            self._rebuild_index(idx)
        elif idx.mode == "lazy":
            staleness = self._commit_count - idx.last_built_at_commit
            if staleness > idx.staleness_budget:
                self._rebuild_index(idx)
            elif idx.incremental and (idx.pending_additions or idx.pending_deletions):
                self._incremental_update_index(idx)

        if idx.tree_root is None:
            return None

        full_key = f"_index/{index_name}/{index_key}"
        rowid_blob_hash = ProllyTree.lookup(self.kernel, idx.tree_root, full_key)
        if rowid_blob_hash:
            # The index stores blob_hash → rowid (UTF-8). Read the blob,
            # then use _get_row(rowid) to retrieve the actual data.
            rowid = self.kernel.read_blob(rowid_blob_hash).decode()
            return self._get_row(rowid)
        return None

    def find_all_by(self, index_name: str, index_key: str) -> list[Any]:
        """Find ALL entries matching an index key (not just the first)."""
        if not hasattr(self, '_auto_indexes'):
            raise ValueError(f"Index '{index_name}' not registered (no auto-index state)")
        idx = self._auto_indexes.get(index_name)
        if not idx:
            raise ValueError(f"Index '{index_name}' not registered")

        if idx.mode == "lazy":
            staleness = self._commit_count - idx.last_built_at_commit
            if staleness > idx.staleness_budget or idx.tree_root is None:
                self._rebuild_index(idx)

        if idx.tree_root is None:
            return []

        full_key = f"_index/{index_name}/{index_key}"
        rowid_blob_hash = ProllyTree.lookup(self.kernel, idx.tree_root, full_key)
        if rowid_blob_hash:
            rowid = self.kernel.read_blob(rowid_blob_hash).decode()
            row = self._get_row(rowid)
            if row is not None:
                return [row]
        return []

    # ------------------------------------------------------------------
    # Index rebuilding (metadata only — data never modified)
    # ------------------------------------------------------------------

    def _rebuild_index(self, idx: AutoIndex) -> str:
        """Rebuild an index from current data. Full O(N) scan.

        Uses the generic _scan_rows() interface so it works with ANY lens
        (KV or tabular). For KV lenses, _scan_rows iterates keys()+get().
        For tabular lenses, _scan_rows iterates row groups.

        The index maps index_key → blob_hash, where the blob contains the
        rowid (encoded as UTF-8). This is because ProllyTree values must
        be hex blob hashes. find_by() reads the blob to get the rowid,
        then calls _get_row(rowid) to retrieve the actual data.
        """
        index_entries = {}
        for rowid, row_data in self._scan_rows():
            idx_keys = AutoIndex.extract_keys(idx.extractor, row_data)
            for idx_key in idx_keys:
                # Store rowid as a blob, use its hash as the index value.
                # For KV lenses, rowid = key. For tabular lenses, rowid = _rowid.
                rowid_bytes = str(rowid).encode()
                rowid_blob_hash = self.kernel.write(rowid_bytes)
                index_entries[f"_index/{idx.name}/{idx_key}"] = rowid_blob_hash

        idx.tree_root = ProllyTree.build(self.kernel, index_entries)
        idx.last_built_at_commit = self._commit_count
        idx.pending_additions.clear()
        idx.pending_deletions.clear()
        idx._cached_entries = index_entries
        self.kernel.reference(f"{self.name}__index__{idx.name}", idx.tree_root)
        return idx.tree_root

    def _incremental_update_index(self, idx: AutoIndex) -> str:
        """Incrementally update an index by merging pending changes."""
        if not idx.tree_root:
            return self._rebuild_index(idx)

        if idx._cached_entries is not None:
            current_entries = dict(idx._cached_entries)
        else:
            current_entries = ProllyTree.read_all(self.kernel, idx.tree_root)

        current_entries.update(idx.pending_additions)
        for key in idx.pending_deletions:
            current_entries.pop(key, None)

        idx.tree_root = ProllyTree.build(self.kernel, current_entries)
        idx.last_built_at_commit = self._commit_count
        idx._cached_entries = current_entries
        idx.pending_additions.clear()
        idx.pending_deletions.clear()
        self.kernel.reference(f"{self.name}__index__{idx.name}", idx.tree_root)
        return idx.tree_root

    def refresh_all_indexes(self) -> None:
        """Force-refresh all indexes. Useful before heavy read workloads."""
        if not hasattr(self, '_auto_indexes'):
            return
        for idx in self._auto_indexes.values():
            self._rebuild_index(idx)

    def get_index_staleness(self, index_name: str) -> int:
        """How many commits since the index was last built."""
        if not hasattr(self, '_auto_indexes'):
            return -1
        idx = self._auto_indexes.get(index_name)
        if not idx:
            return -1
        return self._commit_count - idx.last_built_at_commit

    # ------------------------------------------------------------------
    # Version control overrides — invalidate indexes on branch/checkout/undo
    # ------------------------------------------------------------------

    def checkout(self, name: str) -> None:
        super().checkout(name)
        if hasattr(self, '_auto_indexes'):
            for idx in self._auto_indexes.values():
                idx.tree_root = None
                idx.last_built_at_commit = -1
                idx._cached_entries = None

    def undo(self, steps: int = 1) -> str:
        result = super().undo(steps)
        if hasattr(self, '_auto_indexes'):
            self._commit_count = max(0, self._commit_count - steps)
            for idx in self._auto_indexes.values():
                idx.tree_root = None
                idx.last_built_at_commit = -1
                idx._cached_entries = None
        return result


# ---------------------------------------------------------------------------
# IndexedLens — convenience class: KeyValueLens + AutoIndexMixin
# ---------------------------------------------------------------------------

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "lenses", "keyvalue"))
from keyvalue_lens import KeyValueLens


class IndexedLens(KeyValueLens, AutoIndexMixin):
    """A KeyValueLens with automatic indexing enabled.

    Convenience class — equivalent to:
        class MyLens(KeyValueLens, AutoIndexMixin): pass

    Indexes can be:
      - EAGER: updated on every commit (slow writes, always-fresh reads)
      - LAZY: updated on read when stale (fast writes, eventually-fresh reads)
      - BACKGROUND: updated periodically (fast writes, periodic refresh)

    For streaming/OLTP: use LAZY mode (default). Writes stay O(1).
    For OLAP: use EAGER mode. Indexes always fresh for fast scans.
    For mixed: use LAZY with low staleness_budget (e.g., 2-3 commits).

    Indexes are METADATA ONLY. Data blobs are never modified.

    Subclasses that want auto-indexing should extend this class OR mix
    KeyValueLens + AutoIndexMixin directly.
    """

    def __init__(self, kernel: PondMinimal, name: str):
        super().__init__(kernel, name)
        self._init_auto_index()


# Backward-compatible alias
IndexedView = IndexedLens  # backward-compatible alias
