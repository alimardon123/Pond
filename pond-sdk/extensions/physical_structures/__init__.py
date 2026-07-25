"""
Physical Structure extensions — pluggable acceleration structures.

Physical Structures are `f(snapshot) → artifact` (deterministic,
rebuildable, per the Physical Structure algebra §14). Each type
accelerates a specific access pattern:

  - IndexStructure: O(log N) point lookup by non-primary key
  - BloomFilter: O(1) membership test (may have false positives)
  - Statistics: column min/max/null_count for pruning
  - ZoneMap: per-chunk min/max for range pruning

All Physical Structures:
  1. Are stored as kernel blobs (content-addressed, immutable)
  2. Are referenced by naming convention (__{type}/{collection})
  3. Can be shared across Lenses (Track 2 proved this)
  4. Can be lost and rebuilt from the snapshot (P1 rebuildability)
  5. Are OPTIONAL — the base Lens works without them

Usage:
    from extensions.physical_structures import BloomFilter, Statistics, ZoneMap

    # Build a bloom filter from a Lens's data
    bf_hash = BloomFilter.build(kernel, "users", user_ids)

    # Query it (any Lens can query, not just the one that built it)
    exists = BloomFilter.query(kernel, "users", "user_42")

    # Build statistics
    stats_hash = Statistics.build(kernel, "users", table_data)

    # Use statistics for pruning
    stats = Statistics.load(kernel, "users")
    can_skip = Statistics.can_prune(stats, "age", 999)

Available types:
  - BloomFilter: probabilistic membership test
  - Statistics: column-level min/max/null_count
  - ZoneMap: per-chunk min/max for range pruning
  - IndexStructure: Prolly tree index (wrapper around indexing.py)

Future types (implement PhysicalStructure):
  - HNSW: vector ANN index
  - Trie: prefix search index
  - Histogram: value distribution
  - Sketch: HLL, count-min
"""

from extensions.physical_structures.base import PhysicalStructure
from extensions.physical_structures.bloom_filter import BloomFilter
from extensions.physical_structures.statistics import Statistics
from extensions.physical_structures.zone_map import ZoneMap

__all__ = [
    "PhysicalStructure",
    "BloomFilter",
    "Statistics",
    "ZoneMap",
]
