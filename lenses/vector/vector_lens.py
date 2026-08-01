"""
VectorLens — production-ready vector database lens for Pond.

Extends PondLens directly (NOT KeyValueLens). Owns its UnifiedStorage
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

STORAGE: There is exactly ONE storage path — the UnifiedStorage
backend (PND2 blobs + CollectionManifest + JSON commit blobs). The
legacy ProllyTreeIndex / ProllyLensBase path has been removed. If
UnifiedStorage is not available, all I/O methods raise RuntimeError.
Search is a linear scan over all vectors (suitable for small
collections); larger collections should build an IVF index via
build_ann_index() for 100-100,000x speedup.
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


class VectorLens(PondLens):
    """Production-ready vector database lens.

    Extends PondLens directly. Owns its UnifiedStorage storage code —
    per the design principles, production lenses must not inherit from
    each other. Each lens is independent and removable.

    Stores vectors as packed binary (struct.pack) for efficiency.
    Uses UnifiedStorage (PND2 blobs + CollectionManifest) for storage.
    Search auto-accelerates via IVF index when present.

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
            n_dimensions: number of dimensions per vector (each dimension
                becomes a FLOAT64 column in PND2 for bbox pruning).
            use_unified_storage: IGNORED (kept for backward compat).
                There is now only ONE storage path — the unified
                manifest-based architecture.
        """
        super().__init__(kernel)
        self._attached_indexer = None
        self._n_dimensions = n_dimensions

        # Unified storage backend (the ONLY storage path)
        self._unified_storage = None
        try:
            from unified_storage import UnifiedStorage
            self._unified_storage = UnifiedStorage(kernel)
        except ImportError:
            pass  # _require_unified() will raise RuntimeError on first I/O
        # Buffer for uncommitted inserts: collection → list of (id, vector, metadata)
        self._unified_buffer: dict[str, list[tuple[str, list[float], dict]]] = {}
        # Cross-lens metadata cache: collection → key_col
        self._key_col_cache: dict[str, str] = {}

    def _require_unified(self) -> None:
        """Raise RuntimeError if UnifiedStorage is not available.

        The legacy ProllyTreeIndex / ProllyLensBase path has been removed.
        UnifiedStorage is the ONLY storage path. If it is None (because
        the physical_structures extension is not importable), every I/O
        method must fail loudly rather than silently fall back.
        """
        if self._unified_storage is None:
            raise RuntimeError(
                "UnifiedStorage is not available — the legacy "
                "ProllyTreeIndex path has been removed. Install the "
                "physical_structures extension (pond-sdk/extensions/"
                "physical_structures) to enable Vector I/O."
            )

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
    # Write path — vector operations (UnifiedStorage)
    # ==================================================================

    def insert(self, collection: str, id: str, vector: list[float],
               metadata: dict | None = None) -> str:
        """Insert (or replace) a vector. Returns the commit hash.

        Buffers the insert; commits on explicit commit() call (or
        auto-commits when the buffer reaches 10,000 entries).
        """
        if metadata is None:
            metadata = {}
        self._require_unified()

        if collection not in self._unified_buffer:
            self._unified_buffer[collection] = []
        self._unified_buffer[collection].append(
            (str(id), [float(v) for v in vector], metadata))
        # Auto-commit if buffer is large enough
        if len(self._unified_buffer[collection]) >= 10000:
            return self.commit(collection)
        return ""  # not yet committed

    def commit(self, collection: str, message: str = "") -> str:
        """Commit buffered inserts via UnifiedStorage."""
        self._require_unified()

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
        self._notify_indexers(collection)
        return commit_hash

    def delete_vector(self, collection: str, id: str) -> str:
        """Delete a vector by ID. Returns the commit hash.

        UnifiedStorage has no per-row delete primitive, so this performs
        a full rewrite: read all existing rows, drop the one whose id
        matches, write the result back via write() (overwrite).
        """
        self._require_unified()

        # Read all existing vectors (preserving schema).
        all_records = self.get_all(collection)
        lookup_id = str(id)
        # Try numeric coercion for comparison (vector ids may be int or str).
        try:
            lookup_id_num = int(lookup_id)
        except (ValueError, TypeError):
            lookup_id_num = None

        kept = []
        for key, record in all_records.items():
            # Match on either string or numeric form of the id.
            if key == lookup_id:
                continue
            if lookup_id_num is not None:
                try:
                    if int(key) == lookup_id_num:
                        continue
                except (ValueError, TypeError):
                    pass
            kept.append(record)

        # Re-encode rows for write(). Use the same per-dim / vector
        # column strategy as commit().
        rows = []
        for record in kept:
            vec = record.get("vector", [])
            raw_id = record.get("id", "")
            try:
                row_id = int(raw_id)
            except (ValueError, TypeError):
                row_id = str(raw_id)
            row = {"id": row_id}
            if self._n_dimensions and len(vec) == self._n_dimensions:
                for d in range(self._n_dimensions):
                    row[f"dim_{d}"] = float(vec[d])
            else:
                row["vector"] = json.dumps(vec)
            row["metadata"] = json.dumps(record.get("metadata", {}))
            rows.append(row)

        # Sort rows by id (numeric if possible, else string).
        try:
            rows.sort(key=lambda r: int(r["id"]))
        except (ValueError, TypeError):
            rows.sort(key=lambda r: str(r["id"]))

        commit_hash = self._unified_storage.write(
            collection, rows, key_col="id",
            row_group_size=10_000,
            message=f"delete vector {id}")
        # Re-stamp metadata so other lenses still see this as a vector
        # collection (write() overwrites the manifest).
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
        self._notify_indexers(collection)
        return commit_hash

    # ==================================================================
    # Read path — vector operations
    # ==================================================================

    def get_vector(self, collection: str, id: str) -> Optional[dict]:
        """Retrieve a vector record by ID (returns None if absent).

        4-5 GETs cold point lookup via the manifest. Reads per-dim
        FLOAT64 columns when available.

        CROSS-LENS: if the collection was created by another lens, this
        reads the full row by metadata.key_col and returns it as-is.
        Vector-specific fields (vector, metadata) are best-effort: if
        the row has dim_*/vector columns, they're parsed; otherwise
        the row is returned with vector=[] and metadata={} (ugly shape
        but full visibility).
        """
        self._require_unified()

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

    def get_raw(self, collection: str, id: str) -> Optional[bytes]:
        """Read raw bytes by ID (no decode).

        Returns the vector record re-encoded via encode() (the custom
        binary wire format). This is the in-memory equivalent of the
        legacy per-row blob bytes.
        """
        record = self.get_vector(collection, id)
        if record is None:
            return None
        # Drop the cross-lens _row before re-encoding — encode() only
        # understands {id, vector, metadata}.
        clean = {k: record.get(k) for k in ("id", "vector", "metadata")}
        if clean.get("metadata") is None:
            clean["metadata"] = {}
        return self.encode(clean)

    def list_vectors(self, collection: str) -> list[str]:
        """List all vector IDs.

        CROSS-LENS: works on any collection — returns the values of the
        key_col column (from metadata), defaulting to "id".
        """
        self._require_unified()
        key_col = self._resolve_key_col(collection)
        rows = self._unified_storage.read(collection, columns=[key_col])
        return [str(r[key_col]) for r in rows if r.get(key_col) is not None]

    def count(self, collection: str) -> int:
        """Return the number of stored vectors."""
        self._require_unified()
        return len(self.list_vectors(collection))

    def get_all(self, collection: str) -> dict[str, dict]:
        """Read all vectors from the collection.

        CROSS-LENS: works on any collection. Reads all rows, tries to
        reassemble vectors from dim_* or vector columns, falls back to
        empty vectors for non-vector collections (ugly shape but full
        visibility — caller sees the full row in _row).
        """
        self._require_unified()
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

    # ==================================================================
    # Search — k-nearest-neighbours (L2 / Euclidean)
    # ==================================================================

    def search(self, collection: str, query: list[float], k: int = 5,
               n_probe: int = 10) -> list[dict]:
        """Return the k nearest vectors to query using L2 distance.

        AUTO-ACCELERATED: if an IVF index exists for the collection,
        uses approximate nearest neighbor search (100-100,000x faster
        than linear scan at scale). Falls back to linear scan if no
        index exists.

        Args:
            collection: collection name
            query: query vector
            k: number of nearest neighbors to return
            n_probe: number of IVF clusters to search (higher = more
                accurate, slower). Only used if an IVF index exists.

        Each result dict has: id, distance, vector, metadata.
        """
        # Try IVF index first (100-100,000x faster at scale)
        try:
            sys.path.insert(0, os.path.join(SCRIPT_DIR, "..", "..", "pond-sdk",
                                              "extensions", "indexing"))
            from ivf_index import IVFIndex
            ivf = IVFIndex(self.kernel)
            index = ivf.load(collection)
            if index is not None:
                return ivf.search(collection, query, k=k, n_probe=n_probe)
        except (ImportError, Exception):
            pass  # fall through to linear scan

        # Linear scan fallback (fine for small collections)
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

    def build_ann_index(self, collection: str, n_clusters: int = 100,
                         distance_metric: str = "l2") -> str:
        """Build an IVF (Inverted File) index for approximate nearest
        neighbor search.

        After building, search() automatically uses the index for
        100-100,000x speedup at scale.

        Args:
            collection: collection name
            n_clusters: number of clusters. Rule of thumb: sqrt(n_vectors).
                More clusters = faster search, less accuracy.
            distance_metric: "l2" or "cosine"

        Returns:
            The index root hash.
        """
        sys.path.insert(0, os.path.join(SCRIPT_DIR, "..", "..", "pond-sdk",
                                          "extensions", "indexing"))
        from ivf_index import IVFIndex
        ivf = IVFIndex(self.kernel)
        return ivf.build(collection, n_clusters=n_clusters,
                          n_dimensions=self._n_dimensions or None,
                          distance_metric=distance_metric)

    def ann_stats(self, collection: str) -> dict:
        """Return ANN index statistics (n_clusters, cluster sizes, etc.)."""
        try:
            sys.path.insert(0, os.path.join(SCRIPT_DIR, "..", "..", "pond-sdk",
                                              "extensions", "indexing"))
            from ivf_index import IVFIndex
            ivf = IVFIndex(self.kernel)
            return ivf.stats(collection)
        except (ImportError, Exception):
            return {"exists": False}

    def build_vector_zone_maps(self, collection: str,
                                 chunk_size: int = 100) -> int:
        """Build per-dimension bounding-box zone maps for a vector collection.

        DEPRECATED: Zone maps were a legacy pruning extension that sat
        on top of the ProllyTreeIndex backend (per-blob min/max stats).
        The unified storage architecture uses manifest-level inline
        stats for pruning instead (1 zone map per row group of 10K
        rows = negligible overhead, auto-built at write time).

        This method is kept for API compatibility but is a no-op in
        unified mode. The legacy pruning extension (collection_metadata,
        pruning, pruning_reader) has been moved to archive/.

        Args:
            collection: vector collection name (ignored)
            chunk_size: vectors per zone-map chunk (ignored)

        Returns:
            0 (no zone maps built — manifest stats already exist).
        """
        # No-op: zone maps are superseded by manifest-level stats.
        # Kept for API compatibility — callers that invoke this method
        # will not crash, but no zone maps are built.
        return 0

    def search_with_pruning(self, collection: str, query: list[float],
                              k: int = 5) -> list[dict]:
        """k-NN search with bounding-box pruning (Vortex-style for vectors).

        DEPRECATED: The legacy ZoneMapIndex + PruningPredicate
        infrastructure has been moved to archive/. UnifiedStorage's
        manifest already carries per-row-group stats (min/max per dim_*
        column), so pruning happens automatically inside read() when a
        predicate is supplied.

        For k-NN, the only available lower-bound check is per-row-group
        bbox pruning, which doesn't compose with the top-k heap that
        this method historically maintained. Instead, callers should:
          - For exact k-NN on small collections: use search() (linear
            scan over the unified read).
          - For approximate k-NN at scale: build_ann_index() and call
            search() — IVF automatically kicks in (100-100,000x
            speedup).

        This method is kept for API compatibility and delegates to
        search() (linear scan via the unified read path).

        Args:
            collection: vector collection name
            query: query vector
            k: number of nearest neighbors to return

        Returns:
            List of {id, distance, vector, metadata} dicts, sorted by distance.
        """
        return self.search(collection, query, k)

    # ==================================================================
    # Version control (delegated to UnifiedStorage)
    # ==================================================================

    def create_branch(self, collection: str, branch_name: str) -> str:
        """Create a branch — O(1) ref copy via UnifiedStorage."""
        self._require_unified()
        return self._unified_storage.branch(collection, branch_name)

    def checkout_branch(self, collection: str, branch_name: str) -> None:
        """Checkout a branch — point HEAD at the branch's commit."""
        self._require_unified()
        self._unified_storage.checkout(collection, branch_name)

    def list_branches(self, collection: str) -> list[str]:
        """List all branches for a collection."""
        self._require_unified()
        return self._unified_storage.list_branches(collection)

    def merge_branch(self, collection: str, branch_name: str) -> str:
        """Merge a branch into the collection's HEAD.

        Union merge with a 2-parent commit (git-like).
        """
        self._require_unified()
        return self._unified_storage.merge(collection, branch_name)

    def get_history(self, collection: str, limit: int = 20) -> list[dict]:
        """Walk the commit chain for the collection."""
        self._require_unified()
        return self._unified_storage.history(collection, limit)

    # ==================================================================
    # Index-backed lookup (delegates to UnifiedStorage point_lookup)
    # ==================================================================

    def find_by_id(self, collection: str, id: str) -> Optional[dict]:
        """O(1) cold point lookup via UnifiedStorage.

        Equivalent to get_vector() — kept for API compatibility with
        callers that historically used CollectionMetadata secondary
        indexes (which have been archived). The unified manifest now
        provides O(1) cold point lookups natively.
        """
        return self.get_vector(collection, id)

    # ==================================================================
    # Helpers
    # ==================================================================

    @staticmethod
    def _l2(a: list[float], b: list[float]) -> float:
        """Euclidean (L2) distance."""
        return math.sqrt(sum((x - y) ** 2 for x, y in zip(a, b)))
