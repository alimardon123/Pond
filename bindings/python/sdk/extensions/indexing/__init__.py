"""
Indexing extensions — collection-level indexing for Pond.

Structure (follows the semantic/ pattern):
  - base.py: CollectionIndexerInterface — abstract interface
  - collection_index.py: CollectionIndexer — concrete implementation

Indexing is data-side: indexes belong to collections, not lenses. Any lens
reading a collection can use that collection's indexes. The indexer operates
on kernel + collection name — no lens dependency.

GENERIC: CollectionIndexer works with ANY lens. The lens provides a
scan_rows callback that yields (rowid, row_dict) pairs.

Supported storage: ProllyTreeIndex
Supported lens types: ALL (KeyValueLens, LakehouseLens, FeatureStoreLens, ...)

NOTE: AutoIndexMixin and IndexedLens have been REMOVED. They violated
Principle 6 (extensions must not depend on lenses). Use CollectionIndexer
or CollectionMetadata instead.
"""

from .base import CollectionIndexerInterface
from .collection_index import CollectionIndexer

__all__ = ["CollectionIndexerInterface", "CollectionIndexer"]
