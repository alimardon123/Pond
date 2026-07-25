"""
Indexing extensions — collection-level indexing for Pond.

This subpackage contains tools for building and querying secondary indexes
on collections. Indexes are data-side (belong to the collection, not to
any lens). Any lens reading a collection can use that collection's indexes.

Modules:
  - collection_index.py: CollectionIndexer — standalone collection-level indexer
  - auto_index.py: AutoIndexMixin + IndexedLens — legacy lens-mixin approach
    (kept for backward compat; new code should use CollectionIndexer)

GENERIC: CollectionIndexer works with ANY lens. The lens provides a
scan_rows callback that yields (rowid, row_dict) pairs. For KV lenses,
the default scan reads the ProllyTreeIndex directly. For tabular lenses,
the caller provides scan_rows (e.g., from LakehouseLens.iterate).

Supported storage: ProllyTreeIndex (the universal storage backend)
Supported lens types: ALL (KeyValueLens, LakehouseLens, FeatureStoreLens, ...)
"""

from .collection_index import CollectionIndexer
from .auto_index import AutoIndexMixin, AutoIndex

# IndexedLens is lazily created on first access (avoids import path issues)
def __getattr__(name):
    if name in ("IndexedLens", "IndexedView"):
        from .auto_index import _get_indexed_lens
        return _get_indexed_lens()
    raise AttributeError(f"module 'extensions.indexing' has no attribute '{name}'")

__all__ = ["CollectionIndexer", "AutoIndexMixin", "AutoIndex", "IndexedLens"]
