"""
Pond SDK Extensions — pluggable modules that sit between the Lens and kernel.

Extensions are OPTIONAL. The base Lens works without any extensions loaded.
Extensions add domain-specific capabilities:

  - semantic: semantic model adapters (Ossie, Cube, dbt, custom)
  - physical_structures: acceleration structures (PND2, manifest, stats tree)
  - indexing: IVF, HNSW, CollectionIndexer
  - maintenance: GC/Vacuum

Architecture:
  Kernel (Write, Read, Ref) — FROZEN
      ↓
  Lens SDK (KeyValueLens, UnifiedStorage, CollectionIndexer) — core
      ↓
  Extensions (semantic, indexing, maintenance) — OPTIONAL, pluggable
      ↓
  Applications

Design principle (3.7 Functional): extensions make the Lens SDK functional
for specific use cases without baking any single standard into the core.

Design principle (3.1 Simple): the core stays small; extensions are
loaded only when needed.

Design principle (3.4 Scalable): extensions are independent. Adding a
new Physical Structure type or semantic adapter doesn't modify any
existing code.

Usage:
    # Semantic extensions
    from extensions.semantic.ossie import SemanticLens, OssieAdapter

    # Physical Structure extensions
    from extensions.physical_structures import BloomFilter, Statistics

    # Extension registry
    from extensions import list_extensions, load_extension
"""

import os
import sys

# Make pond-sdk importable for extension modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Extension registry — extensions register themselves here on import.
_registered = {}


def register_extension(name: str, module: str, classes: dict):
    """Register an extension. Called by extension modules on import."""
    _registered[name] = {"module": module, "classes": classes}


def get_extension(name: str):
    """Get a registered extension by name."""
    return _registered.get(name)


def list_extensions() -> list:
    """List all registered extensions."""
    return list(_registered.keys())


def load_extension(name: str):
    """Load an extension by name (imports the module)."""
    if name in _registered:
        return _registered[name]
    import importlib
    try:
        importlib.import_module(f"extensions.{name}")
        return _registered.get(name)
    except ImportError:
        return None
