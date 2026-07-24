"""
Pond SDK Extensions — pluggable modules that sit between the Lens and kernel.

Extensions are OPTIONAL. The base Lens (lens_sdk.Lens) works without any
extensions loaded. Extensions add domain-specific capabilities:

  - semantic_ossie: Apache Ossie semantic model adapter
  - (future) semantic_cube: Cube.js semantic model adapter
  - (future) semantic_dbt: dbt metric adapter
  - (future) physical_structures: bloom filters, zone maps, statistics
  - (future) packing: Manifest algebra packing Lens

Architecture:
  Kernel (Write, Read, Ref) — FROZEN
      ↓
  Lens SDK (Lens, ProllyLensBase, IndexedLens) — core, no extensions
      ↓
  Extensions (semantic adapters, physical structures) — OPTIONAL, pluggable
      ↓
  Applications

Design principle (3.7 Functional): extensions make the Lens SDK functional
for specific use cases without baking any single standard into the core.
Different deployments can use different semantic standards (Ossie, Cube,
dbt) by loading different extensions.
"""

# Extension registry — extensions register themselves here on import.
# This allows discovery: `from pond_sdk.extensions import registered_extensions`
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
    # Try to import the module
    import importlib
    try:
        importlib.import_module(f"extensions.{name}")
        return _registered.get(name)
    except ImportError:
        return None
