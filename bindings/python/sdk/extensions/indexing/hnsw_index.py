"""
HNSW (Hierarchical Navigable Small World) — graph-based ANN for vectors.

Better than IVF for high-recall at low latency:
  - IVF: O(n_probe × cluster_size) — reads a fraction of all vectors
  - HNSW: O(log N) — walks a graph, only visits ~log N nodes

DESIGN:
  1. BUILD TIME (build):
     - Read all vectors from the collection
     - Build a multi-layer graph:
       - Layer 0: all vectors, each connected to M nearest neighbors
       - Layer 1: ~1/e fraction, connected to M neighbors
       - Layer L: ~1/e^L fraction
     - Store: adjacency lists per layer + vector IDs
     - Content-addressed (stored as kernel blobs)

  2. SEARCH TIME (search):
     - Start at the top layer, greedily walk to nearest node
     - Drop down a layer, repeat
     - At layer 0, explore neighbors with ef (search beam width)
     - Return top-k nearest
     - Total distance computations: ~M × log N + ef

  3. PB-SCALE:
     - For 10M vectors: HNSW visits ~500 nodes (M=16, ef=50)
     - vs IVF: visits ~100K nodes (n_probe=10, 10K per cluster)
     - 200x fewer distance computations

  4. INTEGRATION:
     - Stored at collections/{name}/indexes/hnsw
     - VectorLens.search() checks IVF first, then HNSW, then linear scan

FORMAT (binary, content-addressed):
  +--------------------------------+
  | Magic (4B): b"PHNS"            |
  | Version (1B): 1                |
  | n_dimensions (4B)              |
  | max_layer (4B)                 |
  | M (4B) — max connections       |
  | ef_construction (4B)           |
  | distance_metric (1B)           |
  +--------------------------------+
  | Entry point (4B) — node ID     |
  +--------------------------------+
  | For each layer (0..max_layer): |
  |   n_nodes (4B)                 |
  |   For each node:               |
  |     node_id (4B)               |
  |     n_neighbors (4B)           |
  |     neighbor_ids (4B each)     |
  +--------------------------------+
  | Vector ID → row mapping:       |
  |   n_vectors (4B)               |
  |   For each: vector_id (4B)     |
  |     row_key (var-len string)   |
  +--------------------------------+

USAGE:
    from hnsw_index import HNSWIndex

    hnsw = HNSWIndex(kernel)
    hnsw.build("vectors", M=16, ef_construction=200)
    results = hnsw.search("vectors", query, k=10, ef=50)
"""
from __future__ import annotations

import struct
import os
import sys
import math
import json
import random
import heapq
from typing import Optional, Any, Iterator

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                  "..", "..", "..", "bindings/python/core"))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

_MAGIC = b"PHNS"
_VERSION = 1
_METRIC_L2 = 0
_METRIC_COSINE = 1


class HNSWIndex:
    """Hierarchical Navigable Small World index for ANN search.

    Better than IVF for high-recall at low latency — O(log N) vs O(N/k).
    """

    def __init__(self, kernel):
        self.kernel = kernel

    @staticmethod
    def _index_ref(collection: str) -> str:
        return f"collections/{collection}/indexes/hnsw"

    # ------------------------------------------------------------------
    # BUILD
    # ------------------------------------------------------------------

    def build(self, collection: str,
              M: int = 16,
              ef_construction: int = 200,
              n_dimensions: Optional[int] = None,
              distance_metric: str = "l2") -> str:
        """Build the HNSW index for a collection.

        Args:
            collection: collection name
            M: max connections per node per layer (higher = better recall, more memory)
            ef_construction: search beam width during construction (higher = better quality)
            n_dimensions: auto-detected if None
            distance_metric: "l2" or "cosine"

        Returns:
            The index root hash.
        """
        vectors, ids = self._read_all_vectors(collection, n_dimensions)
        if not vectors:
            raise ValueError(f"No vectors found in collection '{collection}'")

        n_dims = n_dimensions or len(vectors[0])
        metric_code = _METRIC_L2 if distance_metric == "l2" else _METRIC_COSINE

        # Build the hierarchical graph
        graph, entry_point = self._build_graph(
            vectors, M, ef_construction, distance_metric)

        # P3 fix: Store graph as CHUNKED blobs — one per layer + a small header.
        # This avoids loading a 640MB blob for 10M vectors. Search only fetches
        # the layers it needs: top layers are tiny (few nodes), layer 0 is big
        # but only fetched once and cached.
        header, layer_blobs = self._encode_chunked(
            graph, entry_point, ids, n_dims, M, ef_construction, metric_code)

        # Write each layer as a separate blob
        layer_hashes = []
        for layer_data in layer_blobs:
            h = self.kernel.write(layer_data)
            layer_hashes.append(h)

        # Write header (small — just metadata + layer hash list)
        header["layer_hashes"] = layer_hashes
        header_bytes = json.dumps(header, sort_keys=True).encode()
        index_hash = self.kernel.write(header_bytes)
        self.kernel.reference(self._index_ref(collection), index_hash)
        return index_hash

    def _read_all_vectors(self, collection, n_dimensions):
        """Read all vectors from the collection via UnifiedStorage."""
        sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                          "..", "physical_structures"))
        try:
            from unified_storage import UnifiedStorage
            storage = UnifiedStorage(self.kernel)
            rows = storage.read(collection)
        except ImportError:
            rows = []

        vectors = []
        ids = []
        for row in rows:
            vid = row.get("id")
            if vid is None:
                continue
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
            if vec:
                vectors.append(vec)
                ids.append(str(vid))
        return vectors, ids

    def _build_graph(self, vectors, M, ef_construction, metric):
        """Build a multi-layer HNSW graph.

        Returns (graph, entry_point) where:
          - graph: list of layers, each layer is dict[node_idx → list[neighbor_idx]]
          - entry_point: index of the entry node at the top layer
        """
        n = len(vectors)
        if n == 0:
            return [], 0

        # Determine max layer for each node (geometric distribution)
        ml = 1.0 / math.log(M) if M > 1 else 1.0
        max_layers = [0] * n
        for i in range(n):
            level = int(-math.log(random.random() + 1e-10) * ml)
            max_layers[i] = level

        max_layer = max(max_layers) if max_layers else 0

        # Initialize graph: graph[layer] = {node_idx: [neighbor_idx, ...]}
        graph = [dict() for _ in range(max_layer + 1)]
        entry_point = 0

        # Insert nodes one by one
        for i in range(n):
            self._insert_node(i, vectors, graph, max_layers, M,
                              ef_construction, metric, entry_point)
            # Update entry point if this node has a higher top layer
            if max_layers[i] > max_layers[entry_point]:
                entry_point = i

        return graph, entry_point

    def _insert_node(self, node_idx, vectors, graph, max_layers,
                      M, ef_construction, metric, entry_point):
        """Insert a single node into the HNSW graph."""
        top_layer = max_layers[node_idx]
        ep = entry_point
        query = vectors[node_idx]

        # Phase 1: walk down from top layer to top_layer + 1 (greedy search)
        curr_max = max_layers[entry_point]
        for layer in range(curr_max, top_layer, -1):
            ep = self._greedy_search(vectors, graph, layer, ep, query, metric)

        # Phase 2: insert at layers top_layer down to 0
        for layer in range(min(top_layer, curr_max), -1, -1):
            # Find ef_construction nearest neighbors at this layer
            neighbors = self._search_layer(
                vectors, graph, layer, ep, query, ef_construction, metric)

            # Select M best neighbors (heuristic: keep diverse set)
            selected = self._select_neighbors_heuristic(
                vectors, node_idx, neighbors, M, metric)

            # Add bidirectional connections
            if layer < len(graph):
                graph[layer][node_idx] = [n for n in selected]
                for n in selected:
                    if n in graph[layer]:
                        graph[layer][n].append(node_idx)
                        # Prune neighbor list if too long
                        if len(graph[layer][n]) > M:
                            graph[layer][n] = self._select_neighbors_heuristic(
                                vectors, n, graph[layer][n], M, metric)
                    else:
                        graph[layer][n] = [node_idx]

            # Update entry point for next layer
            if neighbors:
                ep = min(neighbors, key=lambda n: self._distance(query, vectors[n], metric))

    def _greedy_search(self, vectors, graph, layer, entry, query, metric):
        """Greedy search at a layer — walk to the nearest neighbor."""
        if layer >= len(graph) or entry not in graph[layer]:
            return entry

        current = entry
        current_dist = self._distance(query, vectors[current], metric)
        improved = True

        while improved:
            improved = False
            for neighbor in graph[layer].get(current, []):
                d = self._distance(query, vectors[neighbor], metric)
                if d < current_dist:
                    current = neighbor
                    current_dist = d
                    improved = True

        return current

    def _search_layer(self, vectors, graph, layer, entry, query, ef, metric):
        """Beam search at a layer — find ef nearest neighbors."""
        if layer >= len(graph):
            return []

        visited = {entry}
        candidates = [(self._distance(query, vectors[entry], metric), entry)]
        results = [(self._distance(query, vectors[entry], metric), entry)]

        while candidates:
            _, curr = heapq.heappop(candidates)
            for neighbor in graph[layer].get(curr, []):
                if neighbor in visited:
                    continue
                visited.add(neighbor)
                d = self._distance(query, vectors[neighbor], metric)
                if len(results) < ef or d < results[-1][0]:
                    heapq.heappush(candidates, (d, neighbor))
                    heapq.heappush(results, (d, neighbor))
                    if len(results) > ef:
                        results.sort()
                        results = results[:ef]

        return [n for _, n in results]

    def _select_neighbors_heuristic(self, vectors, node_idx, candidates, M, metric):
        """Select M diverse neighbors (heuristic from HNSW paper)."""
        if len(candidates) <= M:
            return list(candidates)

        # Sort by distance to the node
        query = vectors[node_idx]
        scored = [(self._distance(query, vectors[c], metric), c) for c in candidates]
        scored.sort()

        selected = []
        for _, c in scored:
            if len(selected) >= M:
                break
            # Check if c is closer to any selected than to the node
            good = True
            for s in selected:
                d_cs = self._distance(vectors[c], vectors[s], metric)
                d_cn = self._distance(vectors[c], query, metric)
                if d_cs < d_cn:
                    good = False
                    break
            if good:
                selected.append(c)

        # If not enough, add closest
        if len(selected) < M:
            for _, c in scored:
                if c not in selected:
                    selected.append(c)
                    if len(selected) >= M:
                        break

        return selected

    @staticmethod
    def _distance(a, b, metric):
        if metric == "cosine":
            dot = sum(x * y for x, y in zip(a, b))
            na = math.sqrt(sum(x * x for x in a))
            nb = math.sqrt(sum(y * y for y in b))
            if na == 0 or nb == 0:
                return float('inf')
            return 1.0 - dot / (na * nb)
        return sum((x - y) ** 2 for x, y in zip(a, b))

    def _encode_chunked(self, graph, entry_point, ids, n_dims, M,
                         ef_construction, metric_code):
        """Encode graph as chunked blobs: header (JSON) + one binary blob per layer.

        Returns (header_dict, list_of_layer_bytes).
        """
        header = {
            "n_dims": n_dims,
            "max_layer": len(graph) - 1,
            "M": M,
            "ef_construction": ef_construction,
            "metric": "l2" if metric_code == _METRIC_L2 else "cosine",
            "entry_point": entry_point,
            "n_vectors": len(ids),
        }

        layer_blobs = []
        for layer in range(len(graph)):
            buf = bytearray()
            nodes = graph[layer]
            buf += struct.pack("<I", len(nodes))
            for node_idx, neighbors in nodes.items():
                buf += struct.pack("<I", node_idx)
                buf += struct.pack("<I", len(neighbors))
                for n in neighbors:
                    buf += struct.pack("<I", n)
            layer_blobs.append(bytes(buf))

        # Encode IDs as a separate small blob (included in header as JSON)
        header["ids"] = ids

        return header, layer_blobs

    # ------------------------------------------------------------------
    # ENCODE / DECODE (legacy single-blob, kept for backward compat)
    # ------------------------------------------------------------------

    def _encode_index(self, graph, entry_point, ids, n_dims, M,
                       ef_construction, metric_code):
        buf = bytearray()
        buf += _MAGIC
        buf += struct.pack("<B", _VERSION)
        buf += struct.pack("<I", n_dims)
        buf += struct.pack("<I", len(graph) - 1)  # max_layer
        buf += struct.pack("<I", M)
        buf += struct.pack("<I", ef_construction)
        buf += struct.pack("<B", metric_code)
        buf += struct.pack("<I", entry_point)

        for layer in range(len(graph)):
            nodes = graph[layer]
            buf += struct.pack("<I", len(nodes))
            for node_idx, neighbors in nodes.items():
                buf += struct.pack("<I", node_idx)
                buf += struct.pack("<I", len(neighbors))
                for n in neighbors:
                    buf += struct.pack("<I", n)

        buf += struct.pack("<I", len(ids))
        for vid in ids:
            vid_bytes = vid.encode("utf-8")
            buf += struct.pack("<H", len(vid_bytes))
            buf += vid_bytes

        return bytes(buf)

    def _decode_index(self, data):
        if data[:4] != _MAGIC:
            raise ValueError("Not an HNSW index blob")
        pos = 4
        version = data[pos]; pos += 1
        n_dims = struct.unpack_from("<I", data, pos)[0]; pos += 4
        max_layer = struct.unpack_from("<I", data, pos)[0]; pos += 4
        M = struct.unpack_from("<I", data, pos)[0]; pos += 4
        ef_construction = struct.unpack_from("<I", data, pos)[0]; pos += 4
        metric_code = data[pos]; pos += 1
        entry_point = struct.unpack_from("<I", data, pos)[0]; pos += 4

        graph = []
        for layer in range(max_layer + 1):
            n_nodes = struct.unpack_from("<I", data, pos)[0]; pos += 4
            nodes = {}
            for _ in range(n_nodes):
                node_idx = struct.unpack_from("<I", data, pos)[0]; pos += 4
                n_neighbors = struct.unpack_from("<I", data, pos)[0]; pos += 4
                neighbors = []
                for _ in range(n_neighbors):
                    neighbors.append(struct.unpack_from("<I", data, pos)[0])
                    pos += 4
                nodes[node_idx] = neighbors
            graph.append(nodes)

        n_vectors = struct.unpack_from("<I", data, pos)[0]; pos += 4
        ids = []
        for _ in range(n_vectors):
            vid_len = struct.unpack_from("<H", data, pos)[0]; pos += 2
            ids.append(data[pos:pos + vid_len].decode("utf-8"))
            pos += vid_len

        return {
            "n_dims": n_dims,
            "max_layer": max_layer,
            "M": M,
            "ef_construction": ef_construction,
            "metric": "l2" if metric_code == _METRIC_L2 else "cosine",
            "entry_point": entry_point,
            "graph": graph,
            "ids": ids,
        }

    # ------------------------------------------------------------------
    # LOAD / SEARCH
    # ------------------------------------------------------------------

    def load(self, collection):
        """Load the HNSW index header (small JSON blob).

        Returns the header dict with layer_hashes, or None if no index.
        Layer data is fetched lazily by search() — only the layers
        actually needed are read.
        """
        index_hash = self.kernel.resolve(self._index_ref(collection))
        if index_hash is None:
            return None
        data = self.kernel.read_blob(index_hash)

        # Try chunked format (JSON header with layer_hashes)
        try:
            header = json.loads(data)
            if "layer_hashes" in header:
                return header
        except (json.JSONDecodeError, UnicodeDecodeError):
            pass

        # Legacy single-blob format
        return self._decode_index(data)

    def _load_layer(self, layer_hash):
        """Load a single layer's adjacency list from its blob."""
        data = self.kernel.read_blob(layer_hash)
        pos = 0
        n_nodes = struct.unpack_from("<I", data, pos)[0]; pos += 4
        nodes = {}
        for _ in range(n_nodes):
            node_idx = struct.unpack_from("<I", data, pos)[0]; pos += 4
            n_neighbors = struct.unpack_from("<I", data, pos)[0]; pos += 4
            neighbors = []
            for _ in range(n_neighbors):
                neighbors.append(struct.unpack_from("<I", data, pos)[0])
                pos += 4
            nodes[node_idx] = neighbors
        return nodes

    def search(self, collection, query, k=10, ef=50):
        """HNSW search — O(log N) distance computations.

        P3 fix: uses chunked loading — fetches only the layers needed.
        Top layers are tiny (few nodes). Layer 0 is big but fetched once.

        1. Load header (1 GET — small JSON)
        2. Greedy walk top layers (each layer = 1 GET, tiny)
        3. Beam search layer 0 (1 GET — big, but only once)
        4. Return top-k nearest
        """
        index = self.load(collection)
        if index is None:
            raise ValueError(f"No HNSW index for '{collection}'")

        query = [float(v) for v in query]
        metric = index.get("metric", "l2")
        entry = index["entry_point"]
        ids = index.get("ids", [])

        # Check if chunked (has layer_hashes) or legacy (has graph)
        is_chunked = "layer_hashes" in index

        # Read all vectors (needed for distance computation)
        sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                          "..", "physical_structures"))
        from unified_storage import UnifiedStorage
        storage = UnifiedStorage(self.kernel)
        all_rows = storage.read_with_shards(collection)
        vectors = {}
        for row in all_rows:
            vid = row.get("id")
            if vid is None:
                continue
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
                except:
                    continue
            if vec:
                vectors[str(vid)] = vec

        # Build node_idx → vector mapping
        node_vectors = []
        for i, vid in enumerate(ids):
            node_vectors.append(vectors.get(vid, [0.0] * len(query)))

        if is_chunked:
            # Chunked: load layers lazily
            layer_hashes = index["layer_hashes"]
            max_layer = index["max_layer"]

            # Phase 1: greedy walk from top layer to layer 1
            curr = entry
            for layer in range(max_layer, 0, -1):
                graph_layer = self._load_layer(layer_hashes[layer])
                curr = self._greedy_search(node_vectors, [graph_layer], 0, curr, query, metric)

            # Phase 2: beam search at layer 0
            graph_layer0 = self._load_layer(layer_hashes[0])
            candidates = self._search_layer(node_vectors, [graph_layer0], 0, curr, query, max(ef, k), metric)
        else:
            # Legacy: graph is in memory
            graph = index["graph"]
            # Phase 1: greedy walk from top layer to layer 1
            curr = entry
            for layer in range(len(graph) - 1, 0, -1):
                curr = self._greedy_search(node_vectors, graph, layer, curr, query, metric)
            # Phase 2: beam search at layer 0
            candidates = self._search_layer(node_vectors, graph, 0, curr, query, max(ef, k), metric)

        # Sort by distance and return top-k
        scored = [(self._distance(query, node_vectors[n], metric), n) for n in candidates]
        scored.sort()

        results = []
        for dist, node_idx in scored[:k]:
            if node_idx < len(ids):
                vid = ids[node_idx]
                results.append({
                    "id": vid,
                    "distance": dist,
                    "vector": node_vectors[node_idx],
                    "metadata": {},
                })

        return results

    def stats(self, collection):
        index = self.load(collection)
        if index is None:
            return {"exists": False}
        total_edges = sum(len(neighbors) for layer in index["graph"]
                          for neighbors in layer.values())
        return {
            "exists": True,
            "n_vectors": len(index["ids"]),
            "n_dims": index["n_dims"],
            "max_layer": index["max_layer"],
            "M": index["M"],
            "ef_construction": index["ef_construction"],
            "metric": index["metric"],
            "total_edges": total_edges,
        }
