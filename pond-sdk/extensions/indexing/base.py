"""
Abstract base for indexing extensions.

Defines the CollectionIndexerInterface — the contract that any
collection-level indexer must implement. CollectionIndexer (in
collection_index.py) is the concrete implementation.

An indexer manages secondary indexes on collections. Indexes are
data-side (belong to the collection, not to any lens). Any lens
reading a collection can use that collection's indexes.

The interface is:
  - build_index(collection, name, extractor, scan_rows) → str (tree root)
  - lookup(collection, name, key) → Optional[str] (rowid)
  - lookup_all(collection, name, key) → list[str] (rowids)
  - list_indexes(collection) → list[str]
  - drop_index(collection, name) → bool
  - rebuild_index(collection, name, extractor, scan_rows) → str

GENERIC: works with ANY lens. The lens provides a scan_rows callback
that yields (rowid, row_dict) pairs. For KV lenses, the default scan
reads the ProllyTreeIndex directly. For tabular lenses, the caller
provides scan_rows.
"""

from __future__ import annotations

from typing import Optional, Any, Callable, Union, Iterator
from abc import ABC, abstractmethod


class CollectionIndexerInterface(ABC):
    """Abstract interface for collection-level indexers.

    Any indexer must implement these methods. The indexer operates on
    a kernel + collection name — it does NOT know or care what lens
    is calling it. Indexes belong to collections (data-side), not lenses.
    """

    @abstractmethod
    def build_index(self, collection: str, index_name: str,
                    extractor: Callable[[Any], Union[str, list[str]]],
                    scan_rows: Callable[[], Iterator[tuple[str, Any]]] = None) -> str:
        """Build an index on a collection.

        Args:
            collection: collection name
            index_name: name for this index
            extractor: function(row_dict) → str | list[str]
            scan_rows: callback yielding (rowid, row_dict) pairs.
                If None, the indexer reads the ProllyTreeIndex directly.

        Returns:
            The ProllyTree root hash of the index tree.
        """
        ...

    @abstractmethod
    def lookup(self, collection: str, index_name: str,
               index_key: str) -> Optional[str]:
        """Look up a single _rowid by index key.

        Returns the _rowid string, or None if not found.
        """
        ...

    @abstractmethod
    def lookup_all(self, collection: str, index_name: str,
                   index_key: str) -> list[str]:
        """Look up ALL _rowids matching an index key."""
        ...

    @abstractmethod
    def list_indexes(self, collection: str) -> list[str]:
        """List all active (non-tombstoned) indexes on a collection."""
        ...

    @abstractmethod
    def drop_index(self, collection: str, index_name: str) -> bool:
        """Drop an index from a collection. Returns True if dropped."""
        ...

    @abstractmethod
    def rebuild_index(self, collection: str, index_name: str,
                      extractor: Callable[[Any], Union[str, list[str]]],
                      scan_rows: Callable[[], Iterator[tuple[str, Any]]] = None) -> str:
        """Rebuild an index from current data."""
        ...
