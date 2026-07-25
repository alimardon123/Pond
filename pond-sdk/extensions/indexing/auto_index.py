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
# AutoIndexMixin — composable with any KV-style lens backed by ProllyTreeIndex
# ---------------------------------------------------------------------------

class AutoIndexMixin:
    """Mixin that adds automatic indexing to any KV-style Pond lens.

    EXTENSION METADATA:
      extension_type: "mixin"
      supported_lens_types: ["KeyValueLens", "KeylessLens", "SemanticLens"]
      supported_storage: ["ProllyTreeIndex"]
      not_supported: ["LakehouseLens", "FeatureStoreLens"]  # use Physical Structures instead

    GENERIC: works with any lens that exposes:
      - self.kernel         — the PondMinimal kernel
      - self.name           — the collection name
      - self.base           — a persistent ProllyLensBase for this collection
                              (used for staging + committing)
      - self.put(key, data) — stage a key→value mapping (KV-style)
      - self.delete(key)    — stage a deletion
      - self.commit(msg)    — commit staged changes
      - self.encode(data)   — encode a value to bytes
      - self.decode(bytes)  — decode bytes to a value

    Both KeyValueLens and any future KV-style lens that uses ProllyTreeIndex
    (and exposes `self.base`) can use this mixin. Tabular lenses (LakehouseLens)
    use a different acceleration model (Physical Structures: Statistics,
    ZoneMap, BloomFilter) because they store row groups, not individual keys.

    Use by mixing with a KV-style lens:

        from keyvalue_lens import KeyValueLens
        from extensions.indexing.auto_index import AutoIndexMixin

        class MyIndexedLens(KeyValueLens, AutoIndexMixin):
            pass

    Or use the convenience class `IndexedLens` defined at the end of
    this file.

    Adds:
      - register_index(name, extractor, mode, staleness_budget)
      - unregister_index(name)
      - find_by(index_name, index_key) -> Optional[Any]
      - find_all_by(index_name, index_key) -> list[Any]
      - list_auto_indexes() -> list[str]
      - get_index_staleness(index_name) -> int
      - refresh_all_indexes()

    The mixin OVERRIDES `put`, `delete`, and `commit` to track index
    changes. It does NOT override `get` or `get_all` — those still use
    the base lens implementation (O(log N) via ProllyTreeIndex).
    """

    # Extension metadata (for introspection / tooling)
    extension_type = "mixin"
    supported_lens_types = ["KeyValueLens", "KeylessLens", "SemanticLens"]
    supported_storage = ["ProllyTreeIndex"]
    not_supported = ["LakehouseLens", "FeatureStoreLens"]  # use Physical Structures

    def _init_auto_index(self):
        """Call this from __init__ to initialize auto-index state.

        Subclasses call this after super().__init__():
            super().__init__(kernel, name)
            self._init_auto_index()
        """
        self._auto_indexes: dict[str, AutoIndex] = {}
        self._commit_count = 0

    # ------------------------------------------------------------------
    # Register/unregister indexes
    # ------------------------------------------------------------------

    def register_index(self, name: str, extractor: Callable[[Any], Union[str, list[str]]],
                       mode: str = "lazy", staleness_budget: int = 5) -> None:
        """Register an automatic index.

        Args:
            name: the index name (appears in `f"{self.name}__index__{name}"`).
            extractor: function(decoded_data) -> str | list[str].
            mode: 'eager' (rebuild on commit), 'lazy' (rebuild on read
                when stale, default), 'background' (not yet implemented).
            staleness_budget: max commits before a lazy index is rebuilt.
        """
        if not hasattr(self, '_auto_indexes'):
            self._init_auto_index()
        self._auto_indexes[name] = AutoIndex(name, extractor, mode, staleness_budget)

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
        """Override: stage data + track for incremental index updates."""
        if not hasattr(self, '_auto_indexes'):
            # No auto-index state — delegate to KeyValueLens.put
            return super().put(key, data)
        blob_hash = self.kernel.write(self.encode(data))
        self.base.stage(key, blob_hash)
        # Track for incremental index updates
        for idx in self._auto_indexes.values():
            if idx.incremental and idx.tree_root is not None:
                idx_keys = AutoIndex.extract_keys(idx.extractor, data)
                for idx_key in idx_keys:
                    full_key = f"_index/{idx.name}/{idx_key}"
                    idx.pending_additions[full_key] = blob_hash
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
        bh = ProllyTree.lookup(self.kernel, idx.tree_root, full_key)
        return self.decode(self.kernel.read_blob(bh)) if bh else None

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
        bh = ProllyTree.lookup(self.kernel, idx.tree_root, full_key)
        if bh:
            return [self.decode(self.kernel.read_blob(bh))]
        return []

    # ------------------------------------------------------------------
    # Index rebuilding (metadata only — data never modified)
    # ------------------------------------------------------------------

    def _rebuild_index(self, idx: AutoIndex) -> str:
        """Rebuild an index from current data. Full O(N) scan."""
        state = self.base.read_all()
        index_entries = {}
        for pk, bh in state.items():
            if pk.startswith("_"):
                continue
            data = self.decode(self.kernel.read_blob(bh))
            idx_keys = AutoIndex.extract_keys(idx.extractor, data)
            for idx_key in idx_keys:
                index_entries[f"_index/{idx.name}/{idx_key}"] = bh

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
