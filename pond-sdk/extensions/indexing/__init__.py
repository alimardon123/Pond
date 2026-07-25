"""
Indexing extensions — auto-indexing mixins for KV-style Pond lenses.

This subpackage contains mixins that add secondary index management
to lenses backed by ProllyTreeIndex. Indexes are derived structures
(Prolly trees of key→blob_hash) — metadata only, data blobs are never
modified.

Extensions in this category:
  - AutoIndexMixin: composable mixin for eager/lazy auto-indexing
  - IndexedLens: convenience class (KeyValueLens + AutoIndexMixin)

GENERIC: works with any lens that exposes:
  - self.kernel, self.name, self.base (persistent ProllyLensBase)
  - self.put/get/delete/commit (KV-style API)
  - self.encode/decode

Supported lens types: KeyValueLens and subclasses (KeylessLens, etc.)
Supported storage: ProllyTreeIndex
NOT supported: tabular lenses (LakehouseLens) — use Physical Structures
(Statistics, ZoneMap, BloomFilter) for tabular acceleration instead.
"""

from .auto_index import AutoIndexMixin, IndexedLens, AutoIndex

__all__ = ["AutoIndexMixin", "IndexedLens", "AutoIndex"]
