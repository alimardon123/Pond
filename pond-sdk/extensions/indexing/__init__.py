"""
Indexing extensions — collection-level indexing for Pond.

Structure (follows the semantic/ pattern):
  - base.py: CollectionIndexerInterface — abstract interface
  - collection_index.py: CollectionIndexer — RECOMMENDED concrete implementation
  - auto_index.py: AutoIndexMixin + IndexedLens — DEPRECATED (lens-side, has
    Principle 6 violation). Kept for backward compat.

Indexing is data-side: indexes belong to collections, not lenses. Any lens
reading a collection can use that collection's indexes. The indexer operates
on kernel + collection name — no lens dependency.

GENERIC: CollectionIndexer works with ANY lens. The lens provides a
scan_rows callback that yields (rowid, row_dict) pairs.

Supported storage: ProllyTreeIndex
Supported lens types: ALL (KeyValueLens, LakehouseLens, FeatureStoreLens, ...)
"""

from .base import CollectionIndexerInterface
from .collection_index import CollectionIndexer
from .auto_index import AutoIndexMixin, AutoIndex

# IndexedLens is lazily created on first access (DEPRECATED — use CollectionIndexer)
def __getattr__(name):
    if name in ("IndexedLens", "IndexedView"):
        import warnings
        warnings.warn(
            "IndexedLens is deprecated. Use CollectionIndexer instead: "
            "from extensions.indexing.collection_index import CollectionIndexer",
            DeprecationWarning,
            stacklevel=2
        )
        from .auto_index import _get_indexed_lens
        return _get_indexed_lens()
    raise AttributeError(f"module 'extensions.indexing' has no attribute '{name}'")

__all__ = ["CollectionIndexerInterface", "CollectionIndexer", "AutoIndexMixin", "AutoIndex"]
