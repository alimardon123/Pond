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

    def __init__(self, kernel: PondMinimal, n_dimensions: int = 0,
                 use_unified_storage: bool = True):
        """Create a VectorLens.

        Args:
            kernel: the PondMinimal kernel instance
            n_dimensions: number of dimensions per vector (required for
                unified storage path — each dimension becomes a FLOAT64
                column in PND2). Ignored on legacy path.
            use_unified_storage: if True (DEFAULT), use UnifiedStorage
                (PND2 format) as the storage backend. This is the
                cross-lens default — any lens can read/write any
                collection through the same PND2 format. Setting
                this to False selects the legacy ProllyTreeIndex +
                per-vector binary blob path (kept for backward compat,
                but produces collections that other lenses cannot
                read through PND2).

                Unified storage gives:
                  - 4 GETs cold point lookup (vs O(log N))
                  - Bounding-box pruning via manifest stats
                  - Non-destructive append() semantics
                  - CROSS-LENS: any lens can read this collection.
                The lens API is IDENTICAL — only the storage backend changes.
        """
        super().__init__(kernel)
        self._bases: dict[str, ProllyLensBase] = {}
        self._attached_indexer = None
        self._n_dimensions = n_dimensions

        # Unified storage backend (default ON for cross-lens compatibility)
        self._unified_storage = None
        if use_unified_storage:
            try:
                from unified_storage import UnifiedStorage
                self._unified_storage = UnifiedStorage(kernel)
            except ImportError:
                pass
        # Buffer for uncommitted inserts: collection → list of (id, vector, metadata)
        self._unified_buffer: dict[str, list[tuple[str, list[float], dict]]] = {}
        # Cross-lens metadata cache: collection → key_col (so cold lookup
        # costs 1 GET, subsequent lookups are free).
        self._key_col_cache: dict[str, str] = {}

    def _resolve_key_col(self, collection: str) -> str:
        """Resolve the key column for a collection — cross-lens aware.

        Vector collections use "id" as the key column. But a VectorLens
        can read ANY collection (lakehouse, KV, streaming). For those,
        the key column comes from the collection's metadata.

        CACHED: first cold lookup pays 1 extra GET; subsequent lookups
        are free.
        """
        if collection in self._key_col_cache:
            return self._key_col_cache[collection]
        md = self.get_collection_metadata(collection)
        kc = md.get("key_col") or "id"
        self._key_col_cache[collection] = kc
        return kc

    def attach_indexer(self, indexer) -> None:
        """Attach a CollectionMetadata or CollectionIndexer for auto-notify.

        After attaching, every commit (insert, delete_vector) auto-notifies
        the indexer. EAGER indexes refresh immediately; LAZY indexes
        accumulate staleness.

        Usage:
            meta = CollectionMetadata(kernel)
            meta.register_eager_index('vectors', 'by_id', extractor, scan_fn)
            lens.attach_indexer(meta)
            lens.insert('vectors', 'v1', [1.0, 2.0])  # auto-refreshes
        """
        self._attached_indexer = indexer

    def _notify_indexers(self, collection: str) -> None:
        """Notify attached indexer after a commit. Best-effort."""
        if self._attached_indexer is not None:
            try:
                self._attached_indexer.notify_write(collection)
            except Exception:
                pass

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
        """Insert (or replace) a vector. Returns the commit hash.

        Unified storage path: buffers the insert, commits on explicit
        commit() call (or auto-commits if buffer is full).
        Legacy path: commits immediately per insert.
        """
        if metadata is None:
            metadata = {}

        # Unified storage path: buffer
        if self._unified_storage is not None:
            if collection not in self._unified_buffer:
                self._unified_buffer[collection] = []
            self._unified_buffer[collection].append(
                (str(id), [float(v) for v in vector], metadata))
            # Auto-commit if buffer is large enough
            if len(self._unified_buffer[collection]) >= 10000:
                return self.commit(collection)
            return ""  # not yet committed

        # Legacy path: commit immediately
        record = {
            "id": str(id),
            "vector": [float(v) for v in vector],
            "metadata": metadata,
        }
        blob_hash = self.kernel.write(self.encode(record))
        self._get_base(collection).stage(str(id), blob_hash)
        commit_hash = self._get_base(collection).commit(f"insert vector {id}")
        self._notify_indexers(collection)
        return commit_hash

    def commit(self, collection: str, message: str = "") -> str:
        """Commit buffered inserts (unified storage path only).

        Legacy path commits immediately on each insert, so this is a no-op.
        """
        if self._unified_storage is None:
            return ""  # legacy path already committed

        if collection not in self._unified_buffer:
            raise ValueError(f"No staged data for collection '{collection}'")

        buffer = self._unified_buffer[collection]
        # Use numeric ID for row group key (zero-padded for correct ordering)
        # Try to convert IDs to int for proper sorting; fall back to string
        try:
            buffer.sort(key=lambda x: int(x[0]))
        except (ValueError, TypeError):
            buffer.sort(key=lambda x: x[0])

        rows = []
        for vec_id, vector, metadata in buffer:
            # Fix (Round 24): convert numeric string IDs to int for correct
            # sorting and stats. _format_rg_key handles int correctly via
            # bias encoding; string "9" > "49" lexicographically but
            # int 9 < int 49 numerically.
            try:
                row_id = int(vec_id)
            except (ValueError, TypeError):
                row_id = vec_id
            row = {"id": row_id}
            # Fix (Round 24 Issue #3): store per-dimension FLOAT64 columns
            # when n_dimensions is set. This enables bbox pruning via
            # manifest stats (dim_0 min/max per row group).
            if self._n_dimensions and len(vector) == self._n_dimensions:
                for d in range(self._n_dimensions):
                    row[f"dim_{d}"] = float(vector[d])
            else:
                # Fallback: store as JSON string (ragged dims or no n_dimensions)
                row["vector"] = json.dumps(vector)
            row["metadata"] = json.dumps(metadata) if metadata else "{}"
            rows.append(row)

        # Decide: append (existing collection) or write (new)?
        existing_manifest = self._unified_storage._load_manifest(collection)
        if existing_manifest is None:
            # NEW collection — write() creates the first manifest.
            # Stamp cross-lens metadata so other lenses know this is a
            # vector collection with key_col="id" and per-dim columns.
            commit_hash = self._unified_storage.write(
                collection, rows, key_col="id",
                row_group_size=10_000,
                message=message or f"vector insert: {len(rows)} vectors")
            schema = {"id": "string", "metadata": "string"}
            if self._n_dimensions:
                for d in range(self._n_dimensions):
                    schema[f"dim_{d}"] = "float64"
            else:
                schema["vector"] = "string"
            self.stamp_collection_metadata(
                collection, lens_type="vector", key_col="id",
                schema_hint=schema,
                extra={"n_dimensions": self._n_dimensions})
        else:
            # EXISTING collection — append (preserves existing rows).
            commit_hash = self._unified_storage.append(
                collection, rows, key_col="id",
                row_group_size=10_000,
                message=message or f"vector insert: {len(rows)} vectors")
        del self._unified_buffer[collection]
        return commit_hash

    def delete_vector(self, collection: str, id: str) -> str:
        """Delete a vector by ID. Returns the commit hash."""
        self._get_base(collection).stage_delete(str(id))
        commit_hash = self._get_base(collection).commit(f"delete vector {id}")
        self._notify_indexers(collection)
        return commit_hash

    # ==================================================================
    # Read path — vector operations
    # ==================================================================

    def get_vector(self, collection: str, id: str) -> Optional[dict]:
        """Retrieve a vector record by ID (returns None if absent).

        Unified storage path: 4 GETs cold point lookup.
        Fix (Round 24 Issue #3): reads per-dim FLOAT64 columns when available.

        CROSS-LENS: if the collection was created by another lens, this
        reads the full row by metadata.key_col and returns it as-is.
        Vector-specific fields (vector, metadata) are best-effort: if
        the row has dim_*/vector columns, they're parsed; otherwise
        the row is returned with vector=[] and metadata={} (ugly shape
        but full visibility).
        """
        # Unified storage path
        if self._unified_storage is not None:
            # Resolve the key column from metadata (cross-lens aware, cached)
            key_col = self._resolve_key_col(collection)
            # Convert numeric string ID to int for lookup if key_col is "id"
            try:
                lookup_key = str(int(id)) if key_col == "id" else str(id)
            except (ValueError, TypeError):
                lookup_key = str(id)
            # Read the row (no column projection — get everything)
            row = self._unified_storage.point_lookup(collection, key=lookup_key)
            if row is None or row.get(key_col) is None:
                return None
            # Try to reassemble a vector from per-dim columns or vector column
            vector = []
            metadata = {}
            if f"dim_0" in row:
                # Per-dim columns
                d = 0
                while f"dim_{d}" in row:
                    vector.append(row[f"dim_{d}"])
                    d += 1
            elif "vector" in row and isinstance(row["vector"], str):
                try:
                    vector = json.loads(row["vector"])
                except (json.JSONDecodeError, TypeError):
                    vector = []
            if "metadata" in row and isinstance(row["metadata"], str):
                try:
                    metadata = json.loads(row["metadata"])
                except (json.JSONDecodeError, TypeError):
                    metadata = {}
            return {
                "id": row.get(key_col),
                "vector": vector,
                "metadata": metadata,
                "_row": row,  # full row for cross-lens visibility
            }

        # Legacy path
        h = self._get_base(collection).lookup(str(id))
        return self.decode(self.kernel.read_blob(h)) if h else None

    def get_raw(self, collection: str, id: str) -> Optional[bytes]:
        """Read raw bytes by ID (no decode)."""
        h = self._get_base(collection).lookup(str(id))
        return self.kernel.read_blob(h) if h else None

    def list_vectors(self, collection: str) -> list[str]:
        """List all vector IDs.

        CROSS-LENS: works on any collection — returns the values of the
        key_col column (from metadata), defaulting to "id".
        """
        # Unified storage path
        if self._unified_storage is not None:
            key_col = self._resolve_key_col(collection)
            rows = self._unified_storage.read(collection, columns=[key_col])
            return [str(r[key_col]) for r in rows if r.get(key_col) is not None]

        # Legacy path
        return [k for k in self._get_base(collection).read_all()
                if not k.startswith("_")]

    def count(self, collection: str) -> int:
        """Return the number of stored vectors."""
        # Unified storage path
        if self._unified_storage is not None:
            return len(self.list_vectors(collection))

        # Legacy path
        return sum(1 for k in self._get_base(collection).read_all()
                   if not k.startswith("_"))

    def get_all(self, collection: str) -> dict[str, dict]:
        """Read all vectors from the collection.

        CROSS-LENS: works on any collection. Reads all rows, tries to
        reassemble vectors from dim_* or vector columns, falls back to
        empty vectors for non-vector collections (ugly shape but full
        visibility — caller sees the full row in _row).
        """
        # Unified storage path
        if self._unified_storage is not None:
            key_col = self._resolve_key_col(collection)
            rows = self._unified_storage.read(collection)
            result = {}
            for r in rows:
                k = r.get(key_col)
                if k is None:
                    continue
                # Try to reassemble vector
                vector = []
                if f"dim_0" in r:
                    d = 0
                    while f"dim_{d}" in r:
                        vector.append(r[f"dim_{d}"])
                        d += 1
                elif "vector" in r and isinstance(r["vector"], str):
                    try:
                        vector = json.loads(r["vector"])
                    except (json.JSONDecodeError, TypeError):
                        vector = []
                metadata = {}
                if "metadata" in r and isinstance(r["metadata"], str):
                    try:
                        metadata = json.loads(r["metadata"])
                    except (json.JSONDecodeError, TypeError):
                        metadata = {}
                result[str(k)] = {
                    "id": k,
                    "vector": vector,
                    "metadata": metadata,
                    "_row": r,  # full row for cross-lens visibility
                }
            return result

        # Legacy path
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

    def build_vector_zone_maps(self, collection: str,
                                 chunk_size: int = 100) -> int:
        """Build per-dimension bounding-box zone maps for a vector collection.

        This enables search_with_pruning to skip vectors whose bounding
        box proves they can't be in the top-k results — WITHOUT reading
        or decoding the vector blob.

        The zone maps store min/max per dimension across all vectors in
        a "chunk" (a group of vectors). At search time, the lower bound
        on L2 distance from the query to the chunk's bounding box is
        computed. If that lower bound exceeds the k-th best distance
        found so far, the entire chunk is skipped.

        This is the vector equivalent of Vortex-style predicate pushdown:
        evaluate a conservative lower bound on the encoded/metadata form
        before touching the data bytes.

        GENERIC: uses the same ZoneMapIndex infrastructure as tabular
        lenses. The "columns" are vector dimensions (dim_0, dim_1, ...).
        Any format-agnostic ColumnSource could produce this data.

        Args:
            collection: vector collection name
            chunk_size: vectors per zone-map chunk (default 100)

        Returns:
            Number of zone map entries created.
        """
        sys.path.insert(0, os.path.join(SCRIPT_DIR, "..", "..", "pond-sdk",
                                          "extensions", "physical_structures"))
        from collection_metadata import CollectionMetadata
        from pruning import ZoneMap
        from column_source import ListColumnSource

        meta = CollectionMetadata(self.kernel)
        zm_index = meta.zm_index
        if zm_index is None:
            return 0

        # Clear old zone maps
        zm_index.clear_zone_maps(collection)

        # Read all vectors and group into chunks
        state = self._get_base(collection).read_all()
        all_keys = sorted(k for k in state.keys() if not k.startswith("_"))
        n = 0

        for start in range(0, len(all_keys), chunk_size):
            end = min(start + chunk_size, len(all_keys))
            chunk_keys = all_keys[start:end]

            # Decode all vectors in the chunk and build per-dimension min/max
            vectors = []
            for key in chunk_keys:
                record = self.decode(self.kernel.read_blob(state[key]))
                vectors.append(record["vector"])

            if not vectors:
                continue

            # Determine dimensionality
            dim = len(vectors[0])

            # Build min/max per dimension
            min_dims = [min(v[d] for v in vectors) for d in range(dim)]
            max_dims = [max(v[d] for v in vectors) for d in range(dim)]

            # Build a zone map with per-dimension stats
            # Column names: dim_0, dim_1, ...
            zm = ZoneMap(row_count=len(vectors))
            for d in range(dim):
                col = f"dim_{d}"
                zm.min[col] = min_dims[d]
                zm.max[col] = max_dims[d]
                zm.null_count[col] = 0

            # Store the zone map. The "blob_hash" points to the first
            # vector's blob in the chunk (for pruning reader compatibility).
            rg_key = f"rg/{chunk_keys[-1]}"
            zm_index.add_zone_map(collection, rg_key, zm, state[chunk_keys[0]])
            n += 1

        zm_index.commit_zone_maps(collection, f"vector zone maps for {collection}")
        return n

    def search_with_pruning(self, collection: str, query: list[float],
                              k: int = 5) -> list[dict]:
        """k-NN search with bounding-box pruning (Vortex-style for vectors).

        Uses per-dimension zone maps to skip chunks whose bounding box
        proves they can't contain a top-k vector. For each surviving
        chunk, decodes all vectors and computes exact L2 distance.

        The pruning lower bound: for each dimension d, the minimum
        contribution to L2 distance from the query to ANY point in the
        chunk's bounding box [min_d, max_d] is:
          0 if query[d] in [min_d, max_d]  (query is inside the box)
          (query[d] - max_d)^2 if query[d] > max_d
          (min_d - query[d])^2 if query[d] < min_d

        The sum across dimensions gives a lower bound on L2^2 distance.
        If this lower bound >= k-th best distance^2, skip the chunk.

        This is GENERIC: uses the same ZoneMapIndex + PruningPredicate
        infrastructure as tabular lenses. The "predicate" is a custom
        lower-bound check against per-dimension min/max.

        Args:
            collection: vector collection name
            query: query vector
            k: number of nearest neighbors to return

        Returns:
            List of {id, distance, vector, metadata} dicts, sorted by distance.
        """
        query = [float(v) for v in query]
        dim = len(query)

        sys.path.insert(0, os.path.join(SCRIPT_DIR, "..", "..", "pond-sdk",
                                          "extensions", "physical_structures"))
        from collection_metadata import CollectionMetadata

        meta = CollectionMetadata(self.kernel)
        zm_index = meta.zm_index

        if zm_index is None or not zm_index.has_zone_maps(collection):
            # No zone maps — fall back to linear scan
            return self.search(collection, query, k)

        # Phase 1: Walk zone maps, compute lower bound on L2^2 for each
        # chunk. Collect chunks that might contain top-k vectors.
        state = self._get_base(collection).read_all()
        all_keys = sorted(k for k in state.keys() if not k.startswith("_"))

        # Build a map from zone map keys to the chunk's vector keys
        # (zone maps are keyed by rg/{last_key_in_chunk})
        chunk_map: dict[str, list[str]] = {}
        chunk_size = 100  # must match build_vector_zone_maps
        for start in range(0, len(all_keys), chunk_size):
            end = min(start + chunk_size, len(all_keys))
            chunk_keys = all_keys[start:end]
            rg_key = f"rg/{chunk_keys[-1]}"
            chunk_map[rg_key] = chunk_keys

        # k-th best distance^2 (updated as we find better candidates)
        # Initialize to infinity so the first k vectors are always admitted
        best_k_dist_sq = [float('inf')] * k
        best_k_results: list[tuple[float, str, dict]] = []

        chunks_total = 0
        chunks_pruned = 0
        chunks_read = 0

        for rg_key, zm_dict in zm_index.iter_zone_maps(collection):
            chunks_total += 1
            chunk_keys = chunk_map.get(rg_key, [])
            if not chunk_keys:
                continue

            # Compute lower bound on L2^2 distance from query to the
            # chunk's bounding box
            lb_dist_sq = 0.0
            for d in range(dim):
                col = f"dim_{d}"
                mn = zm_dict.get("min", {}).get(col)
                mx = zm_dict.get("max", {}).get(col)
                if mn is None or mx is None:
                    continue  # no stats for this dimension — can't bound
                q = query[d]
                if q < mn:
                    lb_dist_sq += (mn - q) ** 2
                elif q > mx:
                    lb_dist_sq += (q - mx) ** 2
                # else: query is inside the box for this dimension → 0 contribution

            # If the lower bound >= k-th best distance^2, skip this chunk
            if lb_dist_sq >= best_k_dist_sq[-1]:
                chunks_pruned += 1
                continue

            # Chunk survived — decode all vectors and compute exact distance
            chunks_read += 1
            for key in chunk_keys:
                h = state.get(key)
                if not h:
                    continue
                record = self.decode(self.kernel.read_blob(h))
                vec = record["vector"]
                if len(vec) != dim:
                    continue
                dist = self._l2(query, vec)
                dist_sq = dist * dist

                # Insert into top-k if better than k-th best
                if dist_sq < best_k_dist_sq[-1]:
                    best_k_results.append((dist, key, record))
                    best_k_dist_sq.append(dist_sq)
                    # Keep only top-k
                    combined = list(zip(best_k_dist_sq, best_k_results))
                    combined.sort(key=lambda t: t[0])
                    combined = combined[:k]
                    best_k_dist_sq = [c[0] for c in combined]
                    best_k_results = [c[1] for c in combined]

        # Sort final results by distance
        best_k_results.sort(key=lambda t: t[0])

        print(f"  [pruning] chunks: {chunks_total} total, "
              f"{chunks_pruned} pruned, {chunks_read} read")

        return [
            {
                "id": key,
                "distance": dist,
                "vector": record["vector"],
                "metadata": record.get("metadata", {}),
            }
            for dist, key, record in best_k_results[:k]
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
