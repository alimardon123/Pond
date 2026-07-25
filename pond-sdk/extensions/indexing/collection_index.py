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
from prolly_tree import ProllyTree
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
    """

    def __init__(self, kernel: PondMinimal):
        self.kernel = kernel

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

    # ------------------------------------------------------------------
    # Query indexes
    # ------------------------------------------------------------------

    def lookup(self, collection: str, index_name: str,
               index_key: str) -> Optional[str]:
        """Look up a single _rowid by index key.

        Returns the _rowid string, or None if not found.
        The caller uses the lens to retrieve the actual row by _rowid.
        """
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
