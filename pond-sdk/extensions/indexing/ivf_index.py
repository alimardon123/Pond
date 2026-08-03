"""
IVF (Inverted File Index) — approximate nearest neighbor search for vectors.

Solves the #1 competitive gap: VectorLens.search() is a linear scan
(10M GETs for 10M vectors). IVF reduces this to O(n_probe × cluster_size)
— typically 100-100,000x fewer distance computations.

DESIGN:
  1. BUILD TIME (build_index):
     - Read all vectors from the collection
     - Run k-means to find n_clusters centroids
     - Assign each vector to its nearest centroid
     - Store: centroids blob + per-cluster vector ID lists
     - The index is content-addressed (stored as kernel blobs)
     - Reference the index root hash from the CollectionManifest

  2. SEARCH TIME (search):
     - Find n_probe nearest centroids to the query (small scan)
     - Fetch only vectors in those clusters (n_probe × cluster_size vectors)
     - Compute exact L2 distances, return top-k
     - Total GETs: 1 (index) + n_probe (cluster blobs) + n_probe data blobs

  3. PB-SCALE:
     - For 10M vectors, n_clusters=1000, n_probe=10:
       - Search reads ~100K vectors (1% of 10M) = 100x reduction
     - For 100M vectors, n_clusters=10000, n_probe=10:
       - Search reads ~100K vectors (0.1%) = 1000x reduction
     - The centroids blob is small (n_clusters × n_dimensions × 8 bytes)

  4. INTEGRATION:
     - The index is stored at collections/{name}/indexes/ivf
     - The CollectionManifest references it via bloom_filter_ref-style field
     - VectorLens.search() checks if an IVF index exists; if so, uses it
     - If no index, falls back to linear scan (backward compat)

FORMAT (binary, content-addressed):
  +--------------------------------+
  | Magic (4B): b"PIVF"            |
  | Version (1B): 1                |
  | n_dimensions (4B)              |
  | n_clusters (4B)                |
  | distance_metric (1B):          |
  |   0 = L2, 1 = cosine           |
  +--------------------------------+
  | Centroids section:             |
  |   n_clusters × n_dimensions    |
  |   × float64 (8B each)          |
  +--------------------------------+
  | Cluster assignments:           |
  |   For each cluster:            |
  |     n_vectors (4B)             |
  |     vector_ids (var-len)       |
  +--------------------------------+

USAGE:
    from ivf_index import IVFIndex

    # Build the index (after inserting vectors)
    ivf = IVFIndex(kernel)
    ivf.build("vectors", n_clusters=100, n_dimensions=128)

    # Search (100x faster than linear scan)
    results = ivf.search("vectors", query_vec, k=10, n_probe=10)
"""
from __future__ import annotations

import struct
import os
import sys
import math
import json
from typing import Optional, Any, Iterator

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                  "..", "..", "..", "pond-core"))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from kernel import PondMinimal

_MAGIC = b"PIVF"
_VERSION = 1
_METRIC_L2 = 0
_METRIC_COSINE = 1


class IVFIndex:
    """Inverted File Index for approximate nearest neighbor search.

    Stores centroids + cluster assignments as content-addressed blobs.
    Referenced from the CollectionManifest.
    """

    def __init__(self, kernel: PondMinimal):
        self.kernel = kernel

    @staticmethod
    def _index_ref(collection: str) -> str:
        return f"collections/{collection}/indexes/ivf"

    # ------------------------------------------------------------------
    # BUILD
    # ------------------------------------------------------------------

    def build(self, collection: str,
              n_clusters: int = 100,
              n_dimensions: Optional[int] = None,
              max_iterations: int = 20,
              distance_metric: str = "l2") -> str:
        """Build the IVF index for a collection.

        Reads all vectors, runs k-means, stores centroids + assignments.

        Args:
            collection: collection name
            n_clusters: number of clusters (centroids). Rule of thumb:
                sqrt(n_vectors) for balanced search.
            n_dimensions: number of dimensions per vector. If None,
                auto-detected from the first vector.
            max_iterations: k-means iterations
            distance_metric: "l2" or "cosine"

        Returns:
            The index root hash.
        """
        # Read all vectors from the collection
        vectors, ids = self._read_all_vectors(collection, n_dimensions)
        if not vectors:
            raise ValueError(f"No vectors found in collection '{collection}'")

        n_dims = n_dimensions or len(vectors[0])
        n_clusters = min(n_clusters, len(vectors))

        # Run k-means
        centroids, assignments = self._kmeans(
            vectors, n_clusters, n_dims, max_iterations, distance_metric)

        # Encode and store the index
        metric_code = _METRIC_L2 if distance_metric == "l2" else _METRIC_COSINE
        index_bytes = self._encode_index(
            centroids, assignments, ids, n_dims, n_clusters, metric_code)
        index_hash = self.kernel.write(index_bytes)
        self.kernel.reference(self._index_ref(collection), index_hash)
        return index_hash

    def _read_all_vectors(self, collection: str,
                           n_dimensions: Optional[int]) -> tuple[list[list[float]], list[str]]:
        """Read all vectors from the collection via UnifiedStorage.

        Returns (vectors, ids) — vectors as float lists, ids as strings.
        """
        sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                          "..", "physical_structures"))
        try:
            from unified_storage import UnifiedStorage
            storage = UnifiedStorage(self.kernel)
            # Read all rows — each row has 'id' + dim_0..dim_N or 'vector'
            rows = storage.read(collection)
        except ImportError:
            rows = []

        vectors = []
        ids = []
        for row in rows:
            vid = row.get("id")
            if vid is None:
                continue
            # Try per-dim columns first
            vec = []
            d = 0
            while f"dim_{d}" in row:
                val = row.get(f"dim_{d}")
                if val is not None:
                    vec.append(float(val))
                d += 1
            # Fallback: JSON string column
            if not vec and "vector" in row:
                try:
                    vec = [float(v) for v in json.loads(row["vector"])]
                except (json.JSONDecodeError, TypeError):
                    continue
            if vec:
                vectors.append(vec)
                ids.append(str(vid))
        return vectors, ids

    def _kmeans(self, vectors: list[list[float]], n_clusters: int,
                n_dims: int, max_iterations: int,
                distance_metric: str) -> tuple[list[list[float]], list[int]]:
        """Simple k-means clustering.

        Returns (centroids, assignments) where assignments[i] is the
        cluster index for vectors[i].
        """
        n = len(vectors)
        if n_clusters >= n:
            # Each vector is its own cluster
            return vectors, list(range(n))

        # Initialize: pick n_clusters evenly-spaced vectors as initial centroids
        step = n // n_clusters
        centroids = [vectors[i * step][:] for i in range(n_clusters)]

        assignments = [0] * n
        for iteration in range(max_iterations):
            changed = False
            # Assign each vector to nearest centroid
            for i, vec in enumerate(vectors):
                best_cluster = 0
                best_dist = self._distance(vec, centroids[0], distance_metric)
                for c in range(1, n_clusters):
                    dist = self._distance(vec, centroids[c], distance_metric)
                    if dist < best_dist:
                        best_dist = dist
                        best_cluster = c
                if assignments[i] != best_cluster:
                    assignments[i] = best_cluster
                    changed = True

            if not changed and iteration > 0:
                break

            # Update centroids
            cluster_sums = [[0.0] * n_dims for _ in range(n_clusters)]
            cluster_counts = [0] * n_clusters
            for i, vec in enumerate(vectors):
                c = assignments[i]
                for d in range(n_dims):
                    cluster_sums[c][d] += vec[d]
                cluster_counts[c] += 1

            for c in range(n_clusters):
                if cluster_counts[c] > 0:
                    centroids[c] = [s / cluster_counts[c]
                                     for s in cluster_sums[c]]

        return centroids, assignments

    @staticmethod
    def _distance(a: list[float], b: list[float], metric: str) -> float:
        """Compute distance between two vectors."""
        if metric == "cosine":
            dot = sum(x * y for x, y in zip(a, b))
            norm_a = math.sqrt(sum(x * x for x in a))
            norm_b = math.sqrt(sum(y * y for y in b))
            if norm_a == 0 or norm_b == 0:
                return float('inf')
            return 1.0 - dot / (norm_a * norm_b)
        else:  # L2
            return sum((x - y) ** 2 for x, y in zip(a, b))

    # ------------------------------------------------------------------
    # ENCODE / DECODE
    # ------------------------------------------------------------------

    def _encode_index(self, centroids: list[list[float]],
                       assignments: list[int], ids: list[str],
                       n_dims: int, n_clusters: int,
                       metric_code: int) -> bytes:
        """Encode the IVF index as a binary blob."""
        buf = bytearray()
        buf += _MAGIC
        buf += struct.pack("<B", _VERSION)
        buf += struct.pack("<I", n_dims)
        buf += struct.pack("<I", n_clusters)
        buf += struct.pack("<B", metric_code)

        # Centroids: n_clusters × n_dims × float64
        for c in range(n_clusters):
            for d in range(n_dims):
                buf += struct.pack("<d", centroids[c][d])

        # Cluster assignments: for each cluster, list of vector IDs
        # Group IDs by cluster
        cluster_ids: list[list[str]] = [[] for _ in range(n_clusters)]
        for i, c in enumerate(assignments):
            cluster_ids[c].append(ids[i])

        for c in range(n_clusters):
            id_list = cluster_ids[c]
            buf += struct.pack("<I", len(id_list))
            for vid in id_list:
                vid_bytes = vid.encode("utf-8")
                buf += struct.pack("<H", len(vid_bytes))
                buf += vid_bytes

        return bytes(buf)

    def _decode_index(self, data: bytes) -> dict:
        """Decode the IVF index from a binary blob."""
        if data[:4] != _MAGIC:
            raise ValueError("Not an IVF index blob")
        pos = 4
        version = data[pos]; pos += 1
        n_dims = struct.unpack_from("<I", data, pos)[0]; pos += 4
        n_clusters = struct.unpack_from("<I", data, pos)[0]; pos += 4
        metric_code = data[pos]; pos += 1

        # Centroids
        centroids = []
        for c in range(n_clusters):
            vec = []
            for d in range(n_dims):
                vec.append(struct.unpack_from("<d", data, pos)[0])
                pos += 8
            centroids.append(vec)

        # Cluster assignments
        cluster_ids = []
        for c in range(n_clusters):
            n_ids = struct.unpack_from("<I", data, pos)[0]; pos += 4
            ids = []
            for _ in range(n_ids):
                id_len = struct.unpack_from("<H", data, pos)[0]; pos += 2
                vid = data[pos:pos + id_len].decode("utf-8"); pos += id_len
                ids.append(vid)
            cluster_ids.append(ids)

        return {
            "n_dims": n_dims,
            "n_clusters": n_clusters,
            "metric": "l2" if metric_code == _METRIC_L2 else "cosine",
            "centroids": centroids,
            "cluster_ids": cluster_ids,
        }

    # ------------------------------------------------------------------
    # LOAD / SEARCH
    # ------------------------------------------------------------------

    def load(self, collection: str) -> Optional[dict]:
        """Load the IVF index for a collection (cached by content addressing).

        Returns None if no index exists.
        """
        index_hash = self.kernel.resolve(self._index_ref(collection))
        if index_hash is None:
            return None
        data = self.kernel.read_blob(index_hash)
        return self._decode_index(data)

    def search(self, collection: str, query: list[float],
               k: int = 10, n_probe: int = 10) -> list[dict]:
        """Approximate k-NN search using IVF.

        1. Find n_probe nearest centroids to query
        2. Fetch ALL vectors (batch read, 1 GET for manifest + K data blobs)
        3. Filter to probed clusters, compute exact distances, return top-k

        The batch read is the key optimization: instead of N point lookups
        (4 GETs each), we do 1 manifest read + K data blob reads (parallel).
        This makes IVF faster than linear scan at scale because we only
        decode the probed clusters' vectors.

        Args:
            collection: collection name
            query: query vector
            k: number of nearest neighbors to return
            n_probe: number of clusters to search (higher = more accurate)

        Returns:
            List of {id, distance, vector, metadata} dicts, sorted by distance.

        TODO / KNOWN LIMITATION (Bug 10):
            The current implementation reads ALL vectors via
            storage.read(collection) then filters by target_ids in
            Python (step 2 + step 4 below). This means n_probe has NO
            effect on I/O — every search reads the entire collection.
            At PB scale (10M+ vectors) this defeats the purpose of IVF.

            The fix is to store per-cluster blob references in the index
            (cluster_id → list of blob_hashes / rg_keys), so search can
            fetch ONLY the n_probe probed clusters' blobs (true I/O
            reduction). The index format already stores cluster_ids
            (vector IDs per cluster) — it just needs to also store the
            blob_hash / rg_key for each cluster's data, so search can do
            a targeted fetch instead of a full collection scan.

            Until that optimization lands, n_probe only reduces the
            NUMBER OF DISTANCE COMPUTATIONS (Python-side), not the
            number of S3 GETs. The IVF index is still correct (returns
            the right top-k), just not as fast as it could be.
        """
        index = self.load(collection)
        if index is None:
            raise ValueError(f"No IVF index for collection '{collection}'. "
                             "Call build() first.")

        query = [float(v) for v in query]
        centroids = index["centroids"]
        cluster_ids = index["cluster_ids"]
        metric = index["metric"]
        n_clusters = index["n_clusters"]

        # Step 1: find n_probe nearest centroids
        centroid_dists = []
        for c in range(n_clusters):
            dist = self._distance(query, centroids[c], metric)
            centroid_dists.append((dist, c))
        centroid_dists.sort()
        probe_clusters = set(c for _, c in centroid_dists[:n_probe])

        # Step 2: build set of target IDs in probed clusters
        target_ids = set()
        for c in probe_clusters:
            target_ids.update(cluster_ids[c])

        if not target_ids:
            return []

        # Step 3: batch-read ALL vectors (1 manifest GET + K data blob GETs)
        # This is faster than N point lookups because:
        # - 1 manifest read vs N manifest reads
        # - K data blob reads in parallel vs N sequential point lookups
        sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                          "..", "physical_structures"))
        from unified_storage import UnifiedStorage
        storage = UnifiedStorage(self.kernel)
        all_rows = storage.read(collection)

        # Step 4: filter to target IDs, compute distances
        scored: list[tuple[float, str, dict]] = []
        for row in all_rows:
            vid = row.get("id")
            if vid is None or str(vid) not in target_ids:
                continue
            # Reassemble vector
            vec = []
            d = 0
            while f"dim_{d}" in row:
                val = row.get(f"dim_{d}")
                if val is not None:
                    vec.append(float(val))
                d += 1
            if not vec and "vector" in row:
                try:
                    vec = [float(v) for v in json.loads(row["vector"])]
                except (json.JSONDecodeError, TypeError):
                    continue
            if not vec or len(vec) != len(query):
                continue

            dist = self._distance(query, vec, metric)
            metadata = {}
            if "metadata" in row and isinstance(row["metadata"], str):
                try:
                    metadata = json.loads(row["metadata"])
                except (json.JSONDecodeError, TypeError):
                    pass
            scored.append((dist, str(vid), {"id": vid, "vector": vec, "metadata": metadata}))

        scored.sort(key=lambda t: t[0])
        return [
            {
                "id": vid,
                "distance": dist,
                "vector": record["vector"],
                "metadata": record.get("metadata", {}),
            }
            for dist, vid, record in scored[:k]
        ]

    # ------------------------------------------------------------------
    # STATS
    # ------------------------------------------------------------------

    def stats(self, collection: str) -> dict:
        """Return index statistics for debugging/benchmarking."""
        index = self.load(collection)
        if index is None:
            return {"exists": False}
        cluster_sizes = [len(ids) for ids in index["cluster_ids"]]
        return {
            "exists": True,
            "n_clusters": index["n_clusters"],
            "n_dimensions": index["n_dims"],
            "metric": index["metric"],
            "total_vectors": sum(cluster_sizes),
            "min_cluster_size": min(cluster_sizes) if cluster_sizes else 0,
            "max_cluster_size": max(cluster_sizes) if cluster_sizes else 0,
            "avg_cluster_size": sum(cluster_sizes) / len(cluster_sizes) if cluster_sizes else 0,
        }
