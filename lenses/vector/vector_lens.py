"""
VectorLens — production-ready vector database lens for Pond.

Extends PondLens directly (NOT KeyValueLens). Owns its ProllyTreeIndex
storage code. Per the design principles, production lenses must not
inherit from each other — each lens is independent and removable.

Vectors are stored as packed binary (struct.pack) — NOT JSON — for
efficiency. The encode/decode methods handle the custom wire format:

    +-------------------+-----------------------------+
    | Field             | Encoding                    |
    +-------------------+-----------------------------+
    | vec_len           | uint32  little-endian  (4B) |
    | vector[0..N)      | N x float64 little-endian   |
    | id_len            | uint32  little-endian  (4B) |
    | id (utf-8)        | id_len bytes                |
    | meta_len          | uint32  little-endian  (4B) |
    | metadata (json)   | meta_len bytes              |
    +-------------------+-----------------------------+

The id is included inside the blob so that index extractors can
pull it out from the decoded value.

Implements:
  - insert(id, vector, metadata) — insert a vector
  - search(query, k=5) — k-nearest-neighbours (L2 / Euclidean)
  - get_vector(id) — retrieve a vector by ID
  - delete_vector(id) — delete a vector by ID
  - list_vectors() — list all vector IDs
  - count() — count vectors
  - create_branch, checkout_branch, merge_branch, get_history

Uses CollectionMetadata for indexing (data-side, not lens-side).
Search is a linear scan over all vectors (suitable for small collections).
"""

from __future__ import annotations

import json
import math
import os
import struct
import sys
from typing import Optional, Any

# Make pond-core and pond-sdk importable
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(SCRIPT_DIR, "..", "..", "pond-core"))
sys.path.insert(0, os.path.join(SCRIPT_DIR, "..", "..", "pond-sdk"))

from kernel import PondMinimal
from base_lens import PondLens
from prolly_tree import ProllyLensBase, ProllyTree
from binary_encoding import BinaryProllyTree


class VectorLens(PondLens):
    """Production-ready vector database lens.

    Extends PondLens directly. Owns its ProllyTreeIndex storage code —
    per the design principles, production lenses must not inherit from
    each other. Each lens is independent and removable.

    Stores vectors as packed binary (struct.pack) for efficiency.
    Uses ProllyTreeIndex for storage and CollectionMetadata for indexing.

    COLLECTION-AGNOSTIC: Like all Pond lenses, VectorLens is a stateless
    read/write engine. Pass the collection name to each operation:

        lens = VectorLens(kernel)
        lens.insert("vectors", "v1", [1.0, 2.0], {"label": "a"})
        lens.search("vectors", [1.5, 1.5], k=2)
    """

    def __init__(self, kernel: PondMinimal):
        super().__init__(kernel)
        # Cache of ProllyLensBase instances per collection (for staging state)
        self._bases: dict[str, ProllyLensBase] = {}

    def _get_base(self, collection: str) -> ProllyLensBase:
        """Get or create the ProllyLensBase for a collection."""
        if collection not in self._bases:
            self._bases[collection] = ProllyLensBase(self.kernel, collection)
        return self._bases[collection]

    # ==================================================================
    # Binary serialization (custom format — NOT JSON)
    # ==================================================================

    @staticmethod
    def encode(data: Any) -> bytes:
        """Pack a vector record into compact binary."""
        vector = data["vector"]
        metadata = data.get("metadata", {})
        vid = str(data.get("id", ""))

        vec_len = len(vector)
        vec_bytes = struct.pack(f"<{vec_len}d", *vector) if vec_len else b""
        id_bytes = vid.encode("utf-8")
        meta_bytes = json.dumps(metadata).encode("utf-8")

        return (
            struct.pack("<I", vec_len) + vec_bytes
            + struct.pack("<I", len(id_bytes)) + id_bytes
            + struct.pack("<I", len(meta_bytes)) + meta_bytes
        )

    @staticmethod
    def decode(data: bytes) -> dict:
        """Unpack a binary record back into a dict."""
        offset = 0

        (vec_len,) = struct.unpack_from("<I", data, offset)
        offset += 4

        vector = list(struct.unpack_from(f"<{vec_len}d", data, offset)) if vec_len else []
        offset += 8 * vec_len

        (id_len,) = struct.unpack_from("<I", data, offset)
        offset += 4
        vid = data[offset:offset + id_len].decode("utf-8")
        offset += id_len

        (meta_len,) = struct.unpack_from("<I", data, offset)
        offset += 4
        metadata = json.loads(data[offset:offset + meta_len].decode("utf-8"))

        return {"id": vid, "vector": vector, "metadata": metadata}

    # ==================================================================
    # Write path — vector operations (own ProllyTreeIndex storage)
    # ==================================================================

    def insert(self, collection: str, id: str, vector: list[float],
               metadata: dict | None = None) -> str:
        """Insert (or replace) a vector. Returns the commit hash."""
        if metadata is None:
            metadata = {}
        record = {
            "id": str(id),
            "vector": [float(v) for v in vector],
            "metadata": metadata,
        }
        blob_hash = self.kernel.write(self.encode(record))
        self._get_base(collection).stage(str(id), blob_hash)
        return self._get_base(collection).commit(f"insert vector {id}")

    def delete_vector(self, collection: str, id: str) -> str:
        """Delete a vector by ID. Returns the commit hash."""
        self._get_base(collection).stage_delete(str(id))
        return self._get_base(collection).commit(f"delete vector {id}")

    # ==================================================================
    # Read path — vector operations
    # ==================================================================

    def get_vector(self, collection: str, id: str) -> Optional[dict]:
        """Retrieve a vector record by ID (returns None if absent)."""
        h = self._get_base(collection).lookup(str(id))
        return self.decode(self.kernel.read_blob(h)) if h else None

    def get_raw(self, collection: str, id: str) -> Optional[bytes]:
        """Read raw bytes by ID (no decode)."""
        h = self._get_base(collection).lookup(str(id))
        return self.kernel.read_blob(h) if h else None

    def list_vectors(self, collection: str) -> list[str]:
        """List all vector IDs."""
        return [k for k in self._get_base(collection).read_all()
                if not k.startswith("_")]

    def count(self, collection: str) -> int:
        """Return the number of stored vectors."""
        return sum(1 for k in self._get_base(collection).read_all()
                   if not k.startswith("_"))

    def get_all(self, collection: str) -> dict[str, dict]:
        """Read all vectors from the collection."""
        state = self._get_base(collection).read_all()
        return {k: self.decode(self.kernel.read_blob(h))
                for k, h in state.items() if not k.startswith("_")}

    # ==================================================================
    # Search — k-nearest-neighbours (L2 / Euclidean)
    # ==================================================================

    def search(self, collection: str, query: list[float], k: int = 5) -> list[dict]:
        """Return the k nearest vectors to query using L2 distance.

        Linear scan — fine for small collections. Each result dict has:
        id, distance, vector, metadata.
        """
        query = [float(v) for v in query]
        scored: list[tuple[float, str, dict]] = []

        for key, record in self.get_all(collection).items():
            vec = record["vector"]
            if len(vec) != len(query):
                continue  # dimension mismatch — skip
            dist = self._l2(query, vec)
            scored.append((dist, key, record))

        scored.sort(key=lambda t: t[0])
        return [
            {
                "id": key,
                "distance": dist,
                "vector": record["vector"],
                "metadata": record.get("metadata", {}),
            }
            for dist, key, record in scored[:k]
        ]

    # ==================================================================
    # Version control (delegated to ProllyLensBase)
    # ==================================================================

    def create_branch(self, collection: str, branch_name: str) -> str:
        return self._get_base(collection).branch(branch_name)

    def checkout_branch(self, collection: str, branch_name: str) -> None:
        self._get_base(collection).checkout(branch_name)

    def list_branches(self, collection: str) -> list[str]:
        return self._get_base(collection).list_branches()

    def merge_branch(self, collection: str, branch_name: str) -> str:
        return self._get_base(collection).merge(branch_name)

    def get_history(self, collection: str, limit: int = 20) -> list[dict]:
        return self._get_base(collection).history(limit)

    # ==================================================================
    # Index-backed lookup (uses CollectionMetadata — data-side)
    # ==================================================================

    def find_by_id(self, collection: str, id: str) -> Optional[dict]:
        """O(log N) lookup via CollectionMetadata index.

        Builds the index on first call if it doesn't exist.
        """
        from collection_metadata import CollectionMetadata
        meta = CollectionMetadata(self.kernel)

        # Build index if it doesn't exist
        if "by_id" not in meta.list_indexes(collection):
            meta.build_index(collection, "by_id",
                             extractor=lambda r: str(r.get("id", "")),
                             scan_fn=lambda: ((k, self.get_vector(collection, k))
                                              for k in self.list_vectors(collection)))

        rowid = meta.lookup_index(collection, "by_id", str(id))
        if rowid is None:
            return None
        return self.get_vector(collection, rowid)

    # ==================================================================
    # Helpers
    # ==================================================================

    @staticmethod
    def _l2(a: list[float], b: list[float]) -> float:
        """Euclidean (L2) distance."""
        return math.sqrt(sum((x - y) ** 2 for x, y in zip(a, b)))
