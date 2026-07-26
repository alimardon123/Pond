"""
Physical Structure — abstract base class for all acceleration structures.

A Physical Structure is `f(snapshot) → artifact`:
  - Deterministic: same snapshot → same artifact (P1)
  - Rebuildable: can be lost without data loss (P2)
  - Independent: computing it doesn't modify the snapshot (P3)
  - Shared: any Lens can build or query it (Track 2 proved this)

Each concrete type implements:
  - build(): create the structure from source data, store as kernel blob
  - load(): read the structure from the kernel
  - query(): use the structure to accelerate an operation
  - verify(): check the structure is valid (optional, for integrity)

All structures are stored as kernel blobs, referenced by naming convention:
  __{type_name}/{collection}

For example:
  __bloom/users          → bloom filter blob hash
  __stats/users          → statistics blob hash
  __zonemaps/users       → zone map blob hash
  __index/{collection}/{index_name} → index blob hash (already in indexing.py)

The naming convention is the contract. Any Lens can resolve these refs.
"""

from __future__ import annotations
import json
from typing import Any, Optional


class PhysicalStructure:
    """Abstract base class for all Physical Structures.

    Subclasses MUST override:
      - type_name: str (used in the naming convention)
      - build(kernel, collection, source_data, **kwargs) -> str (blob hash)
      - load(kernel, collection) -> Optional[dict] (the structure data)

    Subclasses MAY override:
      - query(kernel, collection, *args) -> Any
      - verify(kernel, collection) -> bool
    """

    type_name: str = "physical_structure"  # override in subclass

    @classmethod
    def ref_name(cls, collection: str) -> str:
        """The kernel reference name for this structure type + collection."""
        return f"__{cls.type_name}/{collection}"

    @staticmethod
    def build(kernel, collection: str, source_data: Any, **kwargs) -> str:
        """Build the structure from source data and store in kernel.

        Args:
            kernel: PondMinimal instance
            collection: collection name (used for the ref name)
            source_data: the data to build from (type depends on subclass)
            **kwargs: type-specific parameters

        Returns:
            The blob hash of the stored structure.
        """
        raise NotImplementedError

    @classmethod
    def load(cls, kernel, collection: str) -> Optional[dict]:
        """Load the structure from the kernel.

        Returns None if the structure doesn't exist for this collection.
        """
        h = kernel.resolve(cls.ref_name(collection))
        if h is None:
            return None
        raw = kernel.read_blob(h)
        return json.loads(raw)

    @classmethod
    def exists(cls, kernel, collection: str) -> bool:
        """Check if this structure exists for the collection (not tombstoned)."""
        from maintenance import is_dropped
        ref = cls.ref_name(collection)
        if kernel.resolve(ref) is None:
            return False
        if is_dropped(kernel, ref):
            return False
        return True

    @classmethod
    def delete(cls, kernel, collection: str) -> None:
        """Delete this structure (tombstone the ref).

        Uses the tombstone pattern from maintenance.py.
        """
        from maintenance import drop_name
        drop_name(kernel, cls.ref_name(collection))

    @staticmethod
    def query(kernel, collection: str, *args, **kwargs) -> Any:
        """Query the structure. Subclasses override with specific query logic."""
        raise NotImplementedError

    @classmethod
    def verify(cls, kernel, collection: str) -> bool:
        """Verify the structure is valid. Default: check it exists."""
        return cls.exists(kernel, collection)
