"""
Vector Database View — built on top of the Pond SDK specification.

Implements:
    insert(id, vector, metadata)   — insert a vector
    search(query, k=5)             — k-nearest-neighbours (L2 / Euclidean)
    get(id)                        — retrieve a vector by ID
    delete(id)                     — delete a vector by ID
    list_vectors()                 — list all vector IDs
    count()                        — count vectors

Design notes
------------
* Extends ``IndexedLens`` and registers an ``"by_id"`` index (eager mode)
  so that ID lookups go through the indexing layer as required.
* Vectors are stored as packed binary (``struct.pack``) — NOT JSON — for
  efficiency.  The overridden ``encode`` / ``decode`` methods handle the
  custom wire format:

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

  The ``id`` is included inside the blob so that the index extractor —
  which only receives the decoded *value*, not the view key — can still
  pull it out.  (The spec does not document the extractor call signature;
  see the validation report for details.)
* Search is a linear scan over all vectors (as allowed by the task).
* Branching and history are inherited from ``View`` and re-exposed with
  domain-friendly names.
"""

from __future__ import annotations

import json
import math
import struct
from typing import Any

from auto_index import IndexedLens          # Layer 2
from lens_sdk import CrossLens               # cross-view helpers (available)


class VectorLens(IndexedLens):
    """A simple vector database on Pond's IndexedLens."""

    # ---- lifecycle --------------------------------------------------

    def __init__(self, kernel, name: str):
        super().__init__(kernel, name)
        # Auto-index by ID (eager so reads are always fresh).
        # The extractor receives the decoded record and returns its "id".
        self.register_index(
            "by_id",
            self._id_extractor,
            mode="eager",
        )

    # ---- index helper -----------------------------------------------

    @staticmethod
    def _id_extractor(record: dict) -> str:
        """Extract the ID from a decoded record (for the by_id index)."""
        return record.get("id")

    # ---- binary serialization (override) ----------------------------

    def encode(self, data: Any) -> bytes:
        """Pack a vector record into compact binary (not JSON)."""
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

    def decode(self, data: bytes) -> dict:
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

    # ---- public vector API ------------------------------------------

    def insert(self, id: str, vector: list[float],
               metadata: dict | None = None) -> str:
        """Insert (or replace) a vector.  Returns the commit hash."""
        if metadata is None:
            metadata = {}
        record = {
            "id": str(id),
            "vector": [float(v) for v in vector],
            "metadata": metadata,
        }
        self.put(str(id), record)
        return self.commit(f"insert vector {id}")

    def search(self, query: list[float], k: int = 5) -> list[dict]:
        """
        Return the *k* nearest vectors to *query* using L2 distance.

        Linear scan — fine for small collections.
        Each result dict has: id, distance, vector, metadata.
        """
        query = [float(v) for v in query]
        scored: list[tuple[float, str, dict]] = []

        for key, record in self.get_all().items():
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

    def get(self, id: str) -> dict | None:
        """Retrieve a vector record by ID (returns None if absent)."""
        return super().get(str(id))

    def delete(self, id: str) -> str:
        """Delete a vector by ID.  Returns the commit hash."""
        super().delete(str(id))
        return self.commit(f"delete vector {id}")

    def list_vectors(self) -> list[str]:
        """List all vector IDs."""
        return self.keys()

    def count(self) -> int:
        """Return the number of stored vectors."""
        return super().count()

    # ---- branching / history (domain-friendly wrappers) -------------

    def create_branch(self, branch_name: str) -> str:
        return self.branch(branch_name)

    def checkout_branch(self, branch_name: str) -> None:
        self.checkout(branch_name)

    def merge_branch(self, branch_name: str) -> str:
        return self.merge(branch_name)

    def get_history(self, limit: int = 20) -> list[dict]:
        return self.history(limit)

    # ---- index-backed lookup (demonstrates IndexedLens usage) -------

    def find_by_id(self, id: str) -> dict | None:
        """O(log N) lookup via the registered by_id index."""
        return self.find_by("by_id", str(id))

    # ---- helpers ----------------------------------------------------

    @staticmethod
    def _l2(a: list[float], b: list[float]) -> float:
        """Euclidean (L2) distance."""
        return math.sqrt(sum((x - y) ** 2 for x, y in zip(a, b)))
