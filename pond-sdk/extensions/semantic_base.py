"""
Semantic Model Adapter — abstract interface for semantic model formats.

Pond supports multiple semantic model standards (Ossie, Cube, dbt, etc.)
via adapters. The Lens SDK core is NOT coupled to any specific format.

This module defines the abstract interface. Concrete adapters live in
separate modules:
  - semantic_ossie.py: Apache Ossie adapter
  - (future) semantic_cube.py: Cube.js adapter
  - (future) semantic_dbt.py: dbt metrics adapter

Usage:
    from extensions.semantic_base import SemanticModelAdapter

    class MyCustomAdapter(SemanticModelAdapter):
        def export_model(self, lens) -> dict: ...
        def import_model(self, lens, model: dict) -> None: ...
"""

from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from lens_sdk import Lens


class SemanticModelAdapter:
    """Abstract interface for semantic model formats.

    A semantic model adapter translates between Pond's internal
    metric/dimension/relationship storage and an external semantic
    model standard (Ossie, Cube, dbt, etc.).

    The adapter does NOT own data — it translates. The Lens stores
    the definitions; the adapter converts them to/from external formats.
    """

    def export_model(self, lens: "Lens") -> dict:
        """Export the Lens's semantic definitions in this adapter's format."""
        raise NotImplementedError

    def import_model(self, lens: "Lens", model: dict) -> None:
        """Import an external-format semantic model into the Lens."""
        raise NotImplementedError

    def validate_model(self, model: dict) -> bool:
        """Validate that a model dict conforms to this adapter's format."""
        raise NotImplementedError
