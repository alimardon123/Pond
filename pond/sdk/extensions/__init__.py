"""Pond SDK Extensions — pluggable modules (physical_structures, indexing, maintenance, semantic)."""

import os, sys
_syspath = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))), "pond-sdk", "extensions")
if _syspath not in sys.path:
    sys.path.insert(0, _syspath)

# Re-export key classes
try:
    from unified_storage import UnifiedStorage, PND2
    from collection_manifest import CollectionManifest
    from stats_tree import StatsTreeReader
    _have_physical = True
except ImportError:
    _have_physical = False

try:
    from collection_index import CollectionIndexer
    from hnsw_index import HNSWIndex
    from ivf_index import IVFIndex
    _have_indexing = True
except ImportError:
    _have_indexing = False

try:
    from vacuum import GarbageCollector
    _have_maintenance = True
except ImportError:
    _have_maintenance = False
