"""
CollectionIndexer — collection-level indexing, independent of any lens.

DESIGN: Indexes belong to COLLECTIONS (data-side), not to LENSES. An index
is built on a collection and can be used by ANY lens that reads that
collection. The CollectionIndexer operates directly on the kernel +
collection name — it does not know or care what lens is calling it.

This is the correct architecture per the design principles:
  - Extensions are data-side (collection-level), not lens-side
  - Lenses are stateless read/write engines
  - Each layer is independent from the others

The index maps index_key → _rowid (stored as a blob). The _rowid is the
universal row identifier (UUIDv7 for tabular lenses, the key for KV lenses).
To retrieve the actual row, the caller uses the lens's get/lookup API.

Usage:
    from collection_index import CollectionIndexer

    indexer = CollectionIndexer(kernel)

    # Build an index on the "users" collection, indexing by "name" column
    indexer.build_index("users", "by_name",
                        extractor=lambda row: row.get("name", ""))

    # Look up by index — returns the _rowid
    rowid = indexer.lookup("users", "by_name", "alice")

    # Use ANY lens to retrieve the actual row by _rowid
    row = kv_lens.get("users", rowid)  # KeyValueLens
    # or: row = lh_lens.range_point_lookup("users", rowid)  # LakehouseLens

    # List indexes on a collection
    indexes = indexer.list_indexes("users")

    # Drop an index (tombstone pattern per RFC-0008)
    indexer.drop_index("users", "by_name")

The index is stored as a ProllyTree in the kernel, referenced by:
    collections/{collection}/indexes/{index_name}

The index tree maps: f"_index/{index_name}/{index_key}" → rowid_blob_hash
The rowid_blob_hash points to a blob containing the _rowid (UTF-8 string).
"""

from __future__ import annotations

import json
import os
import sys
from typing import Optional, Any, Callable, Union

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "pond-core"))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from kernel import PondMinimal
from prolly_tree import ProllyTree, ProllyLensBase
from maintenance import drop_name, is_dropped, resolve_active, TOMBSTONE_HASH
from uuid7 import uuidv7

# Import base interface — try relative first (package mode), then absolute (path mode)
try:
    from .base import CollectionIndexerInterface
except ImportError:
    from base import CollectionIndexerInterface


class CollectionIndexer(CollectionIndexerInterface):
    """Collection-level indexer. Operates on any collection via the kernel.

    Implements CollectionIndexerInterface (see base.py). This is the
    RECOMMENDED indexer — it is data-side (no lens dependency).

    This is NOT a lens mixin. It is a standalone tool that builds, queries,
    and manages indexes on collections. Any lens can use it — the lens
    provides the row data (via a scan callback), and the indexer handles
    index storage and lookup.

    The index maps index_key → _rowid. The _rowid is the universal row
    identifier (UUIDv7 for tabular lenses, the key for KV lenses).

    INDEX MODES:
      - "manual" (default): caller explicitly calls build_index/refresh_index.
        No automatic refresh. The index may become stale after writes.
      - "lazy": index is refreshed on lookup if stale (exceeds staleness_budget
        commits since last build). Caller must call register_lazy_index() to
        set up the mode, extractor, and scan_fn.
      - "eager": index is refreshed on every notify_write() call. The caller
        (typically the lens) calls notify_write() after each commit.
        Caller must call register_eager_index() to set up.

    The lazy/eager modes replace the old AutoIndexMixin's functionality,
    but are data-side (no lens dependency). The lens calls notify_write()
    or the indexer checks staleness on lookup — both work without coupling
    to the lens's internal staging state.
    """

    def __init__(self, kernel: PondMinimal):
        self.kernel = kernel
        # Registered indexes for lazy/eager modes
        # Key: (collection, index_name) → {extractor, scan_fn, mode, staleness_budget, last_commit_index}
        self._registered: dict[tuple[str, str], dict] = {}

    def register_lazy_index(self, collection: str, index_name: str,
                            extractor: Callable[[Any], Union[str, list[str]]],
                            scan_fn: Callable[[], Any],
                            staleness_budget: int = 5) -> None:
        """Register an index for LAZY auto-refresh.

        The index will be refreshed on lookup if the number of commits
        since the last build exceeds staleness_budget.

        Args:
            collection: collection name
            index_name: index name
            extractor: function(row_dict) → str | list[str]
            scan_fn: callback yielding (rowid, row_dict) pairs
            staleness_budget: max commits before refresh (default 5)
        """
        self._registered[(collection, index_name)] = {
            "extractor": extractor,
            "scan_fn": scan_fn,
            "mode": "lazy",
            "staleness_budget": staleness_budget,
            "last_commit_index": self._get_commit_index(collection),
        }

    def register_eager_index(self, collection: str, index_name: str,
                             extractor: Callable[[Any], Union[str, list[str]]],
                             scan_fn: Callable[[], Any]) -> None:
        """Register an index for EAGER auto-refresh.

        The index will be refreshed on every notify_write() call for
        this collection. This gives always-fresh reads but slower writes.

        Args:
            collection: collection name
            index_name: index name
            extractor: function(row_dict) → str | list[str]
            scan_fn: callback yielding (rowid, row_dict) pairs
        """
        self._registered[(collection, index_name)] = {
            "extractor": extractor,
            "scan_fn": scan_fn,
            "mode": "eager",
            "staleness_budget": 0,
            "last_commit_index": self._get_commit_index(collection),
        }

    def notify_write(self, collection: str) -> None:
        """Notify the indexer that a write (commit) has occurred on a collection.

        For EAGER indexes: refreshes the index immediately.
        For LAZY indexes: increments the staleness counter (refresh on next lookup).
        For MANUAL indexes: no-op.

        The lens should call this after each commit:
            lens.commit("users", "insert alice")
            indexer.notify_write("users")
        """
        current_commit = self._get_commit_index(collection)
        for (coll, idx_name), config in self._registered.items():
            if coll != collection:
                continue
            if config["mode"] == "eager":
                # Refresh immediately
                self.refresh_index(coll, idx_name,
                                   config["extractor"], config["scan_fn"])
                config["last_commit_index"] = current_commit
            # For lazy: just let staleness accumulate (checked on lookup)

    def _get_commit_index(self, collection: str) -> int:
        """Get the current commit count for a collection (from history)."""
        try:
            base = ProllyLensBase(self.kernel, collection)
            head = self.kernel.resolve(f"collections/{collection}/HEAD")
            if head is None:
                return 0
            from binary_encoding import BinaryProllyTree
            commit = BinaryProllyTree.decode_commit(self.kernel.read_blob(head))
            return commit.get("index", 0)
        except Exception:
            return 0

    # ------------------------------------------------------------------
    # Index ref naming
    # ------------------------------------------------------------------

    @staticmethod
    def _index_ref(collection: str, index_name: str) -> str:
        """The kernel reference name for an index on a collection."""
        return f"collections/{collection}/indexes/{index_name}"

    # ------------------------------------------------------------------
    # Build / rebuild / drop indexes
    # ------------------------------------------------------------------

    def build_index(self, collection: str, index_name: str,
                    extractor: Callable[[Any], Union[str, list[str]]],
                    scan_rows: Callable[[], Any] = None) -> str:
        """Build an index on a collection.

        Args:
            collection: the collection name
            index_name: name for this index (e.g., "by_name", "by_region")
            extractor: function(row_dict) -> str | list[str]. Extracts the
                index key(s) from each row.
            scan_rows: optional callback that yields (rowid, row_dict) pairs.
                If None, the indexer reads from the ProllyTreeIndex directly
                (KV-style collections only). For tabular collections, the
                caller must provide scan_rows (e.g., from LakehouseLens.iterate).

        Returns:
            The ProllyTree root hash of the index tree.
        """
        if scan_rows is None:
            scan_rows = self._default_scan_rows(collection)

        index_entries = {}
        for rowid, row_data in scan_rows():
            idx_keys = _extract_keys(extractor, row_data)
            for idx_key in idx_keys:
                # Store rowid as a blob; use its hash as the index value
                rowid_bytes = str(rowid).encode()
                rowid_blob_hash = self.kernel.write(rowid_bytes)
                index_entries[f"_index/{index_name}/{idx_key}"] = rowid_blob_hash

        tree_root = ProllyTree.build(self.kernel, index_entries)
        self.kernel.reference(self._index_ref(collection, index_name), tree_root)
        return tree_root

    def drop_index(self, collection: str, index_name: str) -> bool:
        """Drop an index (tombstone pattern per RFC-0008).

        Returns True if the index existed and was dropped, False otherwise.
        """
        ref = self._index_ref(collection, index_name)
        current = self.kernel.resolve(ref)
        if not current or current == TOMBSTONE_HASH:
            return False
        drop_name(self.kernel, ref)
        return True

    def rebuild_index(self, collection: str, index_name: str,
                      extractor: Callable[[Any], Union[str, list[str]]],
                      scan_rows: Callable[[], Any] = None) -> str:
        """Rebuild an index from current data. Same as build_index but
        overwrites the existing index (including tombstoned indexes)."""
        return self.build_index(collection, index_name, extractor, scan_rows)

    def refresh_index(self, collection: str, index_name: str,
                      extractor: Callable[[Any], Union[str, list[str]]],
                      scan_rows: Callable[[], Any] = None) -> str:
        """Refresh an index incrementally — only update changed entries.

        Instead of a full O(N) rebuild, this method:
          1. Reads the existing index tree (if any)
          2. Scans current data to get the new index entries
          3. Compares old vs new entries
          4. Only writes changed entries to a new ProllyTree

        If no existing index exists, falls back to full build_index().

        The ProllyTree's structural sharing means unchanged entries
        share the same tree nodes — only changed branches are rewritten.
        This makes refresh O(changed) instead of O(N) for the tree
        construction, though the scan is still O(N).

        For true O(changed) refresh (without scanning all data), the
        caller would need to track which rows changed since the last
        index build. That's a future optimization (requires commit-diff
        awareness).
        """
        if scan_rows is None:
            scan_rows = self._default_scan_rows(collection)

        # Check if an existing index exists
        ref = self._index_ref(collection, index_name)
        existing_root = resolve_active(self.kernel, ref)

        if existing_root is None or existing_root == TOMBSTONE_HASH:
            # No existing index — full build
            return self.build_index(collection, index_name, extractor, scan_rows)

        # Read existing index entries
        existing_entries = ProllyTree.read_all(self.kernel, existing_root)

        # Build new index entries
        new_entries = {}
        for rowid, row_data in scan_rows():
            idx_keys = _extract_keys(extractor, row_data)
            for idx_key in idx_keys:
                rowid_bytes = str(rowid).encode()
                rowid_blob_hash = self.kernel.write(rowid_bytes)
                full_key = f"_index/{index_name}/{idx_key}"
                new_entries[full_key] = rowid_blob_hash

        # If entries are identical, no refresh needed
        if existing_entries == new_entries:
            return existing_root

        # Build new tree (ProllyTree structural sharing handles unchanged nodes)
        tree_root = ProllyTree.build(self.kernel, new_entries)
        self.kernel.reference(ref, tree_root)
        return tree_root

    def is_index_stale(self, collection: str, index_name: str,
                       scan_rows: Callable[[], Any] = None,
                       extractor: Callable[[Any], Union[str, list[str]]] = None) -> bool:
        """Check if an index is stale (doesn't match current data).

        Compares the index entries against the current data entries.
        Returns True if the index needs refreshing, False if it's up-to-date.

        This is O(N) — it scans all data to compare. For a cheaper check,
        use commit-count comparison (if the collection has had commits
        since the index was built, it MIGHT be stale).
        """
        if scan_rows is None or extractor is None:
            return True  # can't check without scan_fn + extractor

        ref = self._index_ref(collection, index_name)
        existing_root = resolve_active(self.kernel, ref)
        if existing_root is None or existing_root == TOMBSTONE_HASH:
            return True  # no index → stale

        existing_entries = ProllyTree.read_all(self.kernel, existing_root)

        # Build current entries and compare
        new_keys = set()
        for rowid, row_data in scan_rows():
            idx_keys = _extract_keys(extractor, row_data)
            for idx_key in idx_keys:
                new_keys.add(f"_index/{index_name}/{idx_key}")

        existing_keys = set(existing_entries.keys())
        return new_keys != existing_keys

    # ------------------------------------------------------------------
    # Query indexes
    # ------------------------------------------------------------------

    def lookup(self, collection: str, index_name: str,
               index_key: str) -> Optional[str]:
        """Look up a single _rowid by index key.

        For LAZY indexes: checks staleness first and refreshes if needed.
        For EAGER/MANUAL indexes: just looks up (index is assumed fresh).

        Returns the _rowid string, or None if not found.
        """
        # Check if this is a registered lazy index that needs refresh
        key = (collection, index_name)
        if key in self._registered:
            config = self._registered[key]
            if config["mode"] == "lazy":
                current_commit = self._get_commit_index(collection)
                staleness = current_commit - config["last_commit_index"]
                if staleness > config["staleness_budget"]:
                    # Refresh before lookup
                    self.refresh_index(collection, index_name,
                                       config["extractor"], config["scan_fn"])
                    config["last_commit_index"] = current_commit

        ref = self._index_ref(collection, index_name)
        tree_root = resolve_active(self.kernel, ref)
        if not tree_root:
            return None

        full_key = f"_index/{index_name}/{index_key}"
        rowid_blob_hash = ProllyTree.lookup(self.kernel, tree_root, full_key)
        if rowid_blob_hash:
            return self.kernel.read_blob(rowid_blob_hash).decode()
        return None

    def lookup_all(self, collection: str, index_name: str,
                   index_key: str) -> list[str]:
        """Look up ALL _rowids matching an index key.

        Currently returns at most one (last writer wins). A future version
        will support multi-value indexes.
        """
        rowid = self.lookup(collection, index_name, index_key)
        return [rowid] if rowid else []

    # ------------------------------------------------------------------
    # List indexes
    # ------------------------------------------------------------------

    def list_indexes(self, collection: str) -> list[str]:
        """List all ACTIVE (non-tombstoned) indexes on a collection."""
        prefix = f"collections/{collection}/indexes/"
        return [n[len(prefix):] for n in self.kernel.list_names()
                if n.startswith(prefix) and not is_dropped(self.kernel, n)]

    def list_all_indexes(self, collection: str) -> list[str]:
        """List ALL indexes on a collection, including tombstoned ones."""
        prefix = f"collections/{collection}/indexes/"
        return [n[len(prefix):] for n in self.kernel.list_names()
                if n.startswith(prefix)]

    # ------------------------------------------------------------------
    # Default scan_rows for KV-style collections
    # ------------------------------------------------------------------

    def _default_scan_rows(self, collection: str):
        """Default scan_rows for KV-style collections (reads ProllyTreeIndex).

        Yields (rowid, row_dict) where rowid = key and row_dict = decoded value.
        For tabular collections (LakehouseLens), the caller must provide
        scan_rows (e.g., from LakehouseLens.iterate).
        """
        from prolly_tree import ProllyLensBase
        base = ProllyLensBase(self.kernel, collection)
        state = base.read_all()
        for key, blob_hash in state.items():
            if key.startswith("_"):
                continue
            try:
                row = json.loads(self.kernel.read_blob(blob_hash))
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue
            yield key, row


def _extract_keys(extractor, data) -> list[str]:
    """Call the extractor and normalize the result to a list of keys.

    Handles three cases:
      - extractor returns a single str: wrap in a list.
      - extractor returns a list of str: use as-is.
      - extractor returns None or empty list: return [] (row not indexed).
    """
    result = extractor(data)
    if result is None:
        return []
    if isinstance(result, str):
        return [result]
    if isinstance(result, list):
        return [str(k) for k in result if k is not None]
    return [str(result)]
