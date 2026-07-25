"""
Indexing extensions — collection-level indexing for Pond.

This subpackage contains tools for building and querying secondary indexes
on collections. Indexes are data-side (belong to the collection, not to
any lens). Any lens reading a collection can use that collection's indexes.

Modules:
  - collection_index.py: CollectionIndexer — RECOMMENDED. Standalone
    collection-level indexer. Operates on kernel + collection name. No
    lens dependency. Follows all design principles.
  - auto_index.py: AutoIndexMixin + IndexedLens — DEPRECATED. Legacy
    lens-mixin approach. Kept for backward compat; has a Principle 6
    violation (imports from lenses/keyvalue/). New code should use
    CollectionIndexer.

GENERIC: CollectionIndexer works with ANY lens. The lens provides a
scan_rows callback that yields (rowid, row_dict) pairs. For KV lenses,
the default scan reads the ProllyTreeIndex directly. For tabular lenses,
the caller provides scan_rows (e.g., from LakehouseLens.iterate).

Supported storage: ProllyTreeIndex (the universal storage backend)
Supported lens types: ALL (KeyValueLens, LakehouseLens, FeatureStoreLens, ...)
"""

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

__all__ = ["CollectionIndexer", "AutoIndexMixin", "AutoIndex"]
