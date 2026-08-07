"""
Storage engine extensions — the PND2 format, CollectionManifest, and encodings.

This package contains the physical storage layer for Pond:
  - UnifiedStorage: ONE write/read path (PND2 format)
  - CollectionManifest: ONE index blob per commit
  - StatsTree: PB-scale hierarchical index
  - encoding: 4 binary encodings (RAW/RLE/DICT/BITPACK)
  - compression: transparent zstd/LZ4
  - column_source: format-agnostic data access
  - embedded_stats: value-type constants + ColumnStats

Legacy files (zone_map_index, pruning, pruning_reader, column_chunk_storage,
encoded_chunk_storage, stats_index, base, bloom_filter, statistics) have
been DELETED as superseded by UnifiedStorage + CollectionManifest.
"""

DEFAULT_CHUNK_SIZE = 1000
"""Default rows per column chunk. Kept for backward compat with code
that references this constant."""
