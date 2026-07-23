"""
More Views — testing whether the kernel is truly universal.

Each View must be implemented using ONLY the kernel's 4 syscalls
(Read/Write/Seal/Reference) + DAG patterns (Tree/Commit). If a Lens
needs a kernel change, that's a finding — it means the kernel leaked
or is missing a universal primitive.

Views in this file:

  GraphView      — nodes, edges, properties, adjacency traversal
  MLView         — model checkpoints, weights, training history, artifacts
  TimeSeriesView — compressed segments, retention, aggregation
  DocumentView   — markdown/PDF/Word versioning + content search
  OCIView        — Docker image layers, container registry

Each View is a thin adapter. The kernel stays bytes-only.

The test: do any of these require kernel modifications?
"""

from __future__ import annotations

import os
import io
import json
import time
import struct
import hashlib
from typing import Optional, Iterator, Any

from pond_kernel import PondKernel, Tree, Commit, hash_bytes


# ===========================================================================
# GraphView — nodes, edges, properties, adjacency
# ===========================================================================
#
# A graph is structurally different from SQL/streaming/Git:
#   - Nodes have properties (heterogeneous)
#   - Edges have direction and properties
#   - Traversal requires adjacency lookup (not scan)
#   - Updates are local (one node/edge at a time, not append-only)
#
# If the kernel can't support this, it's not universal.

class GraphView:
    """
    A graph database Lens. Stores nodes and edges as immutable blobs;
    builds adjacency index as Trees.

    Design:
      - Each "commit" snapshots the full graph (all nodes + edges)
      - Nodes are stored as JSON blobs (one blob per node, content-addressed)
      - Edges are stored as JSON blobs (one blob per edge)
      - An adjacency Tree maps node_id -> [edge hashes]
      - The root Tree references the adjacency Tree + node/edge blobs

    Traversal: walk the adjacency Tree to find a node's edges, then read
    the edge blobs to get targets. O(degree) per hop, not O(graph size).

    This works because Trees can reference other Trees (hierarchical),
    so the adjacency index scales to large graphs without kernel changes.
    """

    def __init__(self, kernel: PondKernel, graph_name: str):
        self.kernel = kernel
        self.graph_name = graph_name
        self._staged_nodes: dict[str, dict] = {}
        self._staged_edges: list[dict] = []

    def add_node(self, node_id: str, properties: dict) -> None:
        self._staged_nodes[node_id] = properties

    def add_edge(self, src: str, dst: str, properties: Optional[dict] = None) -> None:
        self._staged_edges.append({
            "src": src, "dst": dst,
            "properties": properties or {}
        })

    def commit(self, message: str = "") -> str:
        """Commit a graph snapshot. Tree contains:
          - nodes/<id> -> node_blob_hash (one entry per node)
          - edges/<i> -> edge_blob_hash (one entry per edge)
          - adjacency/<src_id> -> adj_blob_hash (one entry per source node)
        """
        if not self._staged_nodes and not self._staged_edges:
            raise ValueError("Nothing to commit")

        # Inherit parent's tree (graph snapshots, like Git commits)
        parent_hash = self.kernel._resolve_name(self.graph_name)
        tree_entries: dict[str, str] = {}
        if parent_hash is not None:
            parent_commit = self.kernel.read_commit(parent_hash)
            if parent_commit:
                parent_tree = self.kernel.read_tree(parent_commit.tree_hash)
                if parent_tree:
                    tree_entries = dict(parent_tree.entries)

        # Stage new/updated nodes
        for node_id, props in self._staged_nodes.items():
            node_bytes = json.dumps({
                "id": node_id, "properties": props
            }, sort_keys=True).encode()
            blob_hash = self.kernel.write_blob(node_bytes)
            tree_entries[f"nodes/{node_id}"] = blob_hash

        # Stage new edges
        for i, edge in enumerate(self._staged_edges):
            edge_bytes = json.dumps(edge, sort_keys=True).encode()
            blob_hash = self.kernel.write_blob(edge_bytes)
            # Use a counter for edge index
            existing_edge_idxs = [
                int(k.split("/")[-1]) for k in tree_entries
                if k.startswith("edges/") and k.split("/")[-1].isdigit()
            ]
            next_idx = (max(existing_edge_idxs) + 1) if existing_edge_idxs else 0
            tree_entries[f"edges/{next_idx}"] = blob_hash

        # Build adjacency index: src_id -> list of edge hashes
        # (In a real implementation, this would be incremental; here we rebuild)
        adjacency: dict[str, list[str]] = {}
        for name, bh in tree_entries.items():
            if name.startswith("edges/"):
                edge_data = json.loads(self.kernel.read_blob(bh))
                src = edge_data["src"]
                adjacency.setdefault(src, []).append(bh)

        for src, edge_hashes in adjacency.items():
            adj_bytes = json.dumps({"src": src, "edges": edge_hashes}).encode()
            adj_blob_hash = self.kernel.write_blob(adj_bytes)
            tree_entries[f"adjacency/{src}"] = adj_blob_hash

        tree = Tree(entries=tree_entries, tree_type="leaf")
        tree_hash = self.kernel.write_tree(tree)

        commit = Commit(
            tree_hash=tree_hash,
            parent_hash=parent_hash,
            timestamp=time.time(),
            message=message or f"graph commit {self.graph_name}",
            schema_hash=None,
        )
        commit_hash = self.kernel.write_commit(commit)
        self.kernel.reference(self.graph_name, commit_hash)

        self._staged_nodes = {}
        self._staged_edges = []
        return commit_hash

    def get_node(self, node_id: str) -> Optional[dict]:
        commit_hash = self.kernel._resolve_name(self.graph_name)
        if commit_hash is None:
            return None
        commit = self.kernel.read_commit(commit_hash)
        tree = self.kernel.read_tree(commit.tree_hash)
        key = f"nodes/{node_id}"
        if key not in tree.entries:
            return None
        return json.loads(self.kernel.read_blob(tree.entries[key]))

    def neighbors(self, node_id: str) -> list[dict]:
        """Traverse: get all edges from node_id, return edge dicts."""
        commit_hash = self.kernel._resolve_name(self.graph_name)
        if commit_hash is None:
            return []
        commit = self.kernel.read_commit(commit_hash)
        tree = self.kernel.read_tree(commit.tree_hash)
        key = f"adjacency/{node_id}"
        if key not in tree.entries:
            return []
        adj = json.loads(self.kernel.read_blob(tree.entries[key]))
        edges = []
        for edge_hash in adj["edges"]:
            edges.append(json.loads(self.kernel.read_blob(edge_hash)))
        return edges

    def traverse(self, start: str, max_depth: int = 2) -> list[str]:
        """BFS traversal. Returns visited node IDs."""
        visited = []
        seen = {start}
        frontier = [start]
        for _ in range(max_depth):
            next_frontier = []
            for node in frontier:
                visited.append(node)
                for edge in self.neighbors(node):
                    if edge["dst"] not in seen:
                        seen.add(edge["dst"])
                        next_frontier.append(edge["dst"])
            frontier = next_frontier
            if not frontier:
                break
        return visited


# ===========================================================================
# MLView — model checkpoints, weights, training history, artifacts
# ===========================================================================
#
# ML workloads are structurally different from SQL/streaming/Git:
#   - Artifacts are large binary blobs (model weights, often GB-scale)
#   - Each artifact has rich metadata (training step, loss, hyperparams)
#   - Lineage matters (which checkpoint produced which eval results)
#   - Versioning is per-artifact, not per-table
#
# If the kernel can't support this, it's not universal.

class MLView:
    """
    An ML artifact registry Lens. Stores:
      - model_weights/<name>/<step> -> raw weight bytes (large blob)
      - metadata/<name>/<step> -> JSON metadata (loss, hyperparams, lineage)
      - lineage/<name> -> JSON lineage tree (parent step, derived artifacts)

    The kernel stores the weight bytes opaquely; the Lens interprets them.
    Large blobs are deduplicated by content hash (good for ML, where the
    same checkpoint might be referenced by multiple experiments).
    """

    def __init__(self, kernel: PondKernel, registry_name: str):
        self.kernel = kernel
        self.registry_name = registry_name

    def log_checkpoint(self, model_name: str, step: int,
                       weights: bytes, metadata: dict) -> str:
        """Log a model checkpoint. Returns the commit hash."""
        # Write weights as a blob
        weights_hash = self.kernel.write_blob(weights)
        # Write metadata as a blob
        meta_bytes = json.dumps({
            "model": model_name, "step": step,
            "weights_hash": weights_hash,
            **metadata
        }, sort_keys=True).encode()
        meta_hash = self.kernel.write_blob(meta_bytes)

        # Inherit parent's tree
        parent_hash = self.kernel._resolve_name(self.registry_name)
        tree_entries: dict[str, str] = {}
        if parent_hash is not None:
            parent_commit = self.kernel.read_commit(parent_hash)
            if parent_commit:
                parent_tree = self.kernel.read_tree(parent_commit.tree_hash)
                if parent_tree:
                    tree_entries = dict(parent_tree.entries)

        # Add this checkpoint
        tree_entries[f"weights/{model_name}/{step:08d}"] = weights_hash
        tree_entries[f"metadata/{model_name}/{step:08d}"] = meta_hash

        tree = Tree(entries=tree_entries, tree_type="leaf")
        tree_hash = self.kernel.write_tree(tree)

        commit = Commit(
            tree_hash=tree_hash,
            parent_hash=parent_hash,
            timestamp=time.time(),
            message=f"checkpoint {model_name}@{step}",
            schema_hash=None,
        )
        commit_hash = self.kernel.write_commit(commit)
        self.kernel.reference(self.registry_name, commit_hash)
        return commit_hash

    def get_weights(self, model_name: str, step: int) -> bytes:
        commit_hash = self.kernel._resolve_name(self.registry_name)
        commit = self.kernel.read_commit(commit_hash)
        tree = self.kernel.read_tree(commit.tree_hash)
        key = f"weights/{model_name}/{step:08d}"
        if key not in tree.entries:
            raise ValueError(f"No checkpoint for {model_name}@{step}")
        return self.kernel.read_blob(tree.entries[key])

    def get_metadata(self, model_name: str, step: int) -> dict:
        commit_hash = self.kernel._resolve_name(self.registry_name)
        commit = self.kernel.read_commit(commit_hash)
        tree = self.kernel.read_tree(commit.tree_hash)
        key = f"metadata/{model_name}/{step:08d}"
        if key not in tree.entries:
            raise ValueError(f"No metadata for {model_name}@{step}")
        return json.loads(self.kernel.read_blob(tree.entries[key]))

    def history(self, model_name: str) -> list[dict]:
        """List all checkpoints for a model, sorted by step."""
        commit_hash = self.kernel._resolve_name(self.registry_name)
        commit = self.kernel.read_commit(commit_hash)
        tree = self.kernel.read_tree(commit.tree_hash)

        steps = []
        for name in tree.entries:
            if name.startswith(f"metadata/{model_name}/"):
                step_str = name.split("/")[-1]
                step = int(step_str)
                meta = json.loads(self.kernel.read_blob(tree.entries[name]))
                steps.append({"step": step, "metadata": meta})
        steps.sort(key=lambda x: x["step"])
        return steps


# ===========================================================================
# TimeSeriesView — compressed segments, retention, aggregation
# ===========================================================================
#
# Time-series is structurally different from SQL/streaming:
#   - Data is mostly append-only but per-series (many series, each appending)
#   - Compression matters (Gorilla/XOR encoding for floats)
#   - Retention is per-series (drop old data)
#   - Aggregation is common (downsample)
#
# If the kernel can't support per-series retention, it's not universal.

class TimeSeriesView:
    """
    A time-series Lens. Each series is a sequence of (timestamp, value) pairs.
    Segments are batches of points compressed as raw float bytes.

    Layout:
      - series/<name>/segment/<i> -> blob of (ts, value) pairs
      - series/<name>/meta -> JSON {count, min_ts, max_ts, retention_days}

    Retention: old segments are removed from new commits' Trees.
    (They're still on disk as blobs until GC, but no longer referenced.)
    """

    def __init__(self, kernel: PondKernel, db_name: str):
        self.kernel = kernel
        self.db_name = db_name

    def write_points(self, series_name: str,
                     points: list[tuple[int, float]]) -> str:
        """Write a batch of (timestamp_us, value) points to a series."""
        if not points:
            raise ValueError("No points to write")

        # Serialize: count + count*(8-byte ts + 4-byte float)
        buf = struct.pack("<I", len(points))
        for ts, val in points:
            buf += struct.pack("<Qf", ts, val)

        buf_hash = self.kernel.write_blob(buf)

        # Inherit parent's tree
        parent_hash = self.kernel._resolve_name(self.db_name)
        tree_entries: dict[str, str] = {}
        if parent_hash is not None:
            parent_commit = self.kernel.read_commit(parent_hash)
            if parent_commit:
                parent_tree = self.kernel.read_tree(parent_commit.tree_hash)
                if parent_tree:
                    tree_entries = dict(parent_tree.entries)

        # Find next segment index for this series
        prefix = f"series/{series_name}/segment/"
        existing_idxs = [
            int(k[len(prefix):]) for k in tree_entries
            if k.startswith(prefix) and k[len(prefix):].isdigit()
        ]
        next_idx = (max(existing_idxs) + 1) if existing_idxs else 0
        tree_entries[f"{prefix}{next_idx:08d}"] = buf_hash

        # Update series meta
        min_ts = min(p[0] for p in points)
        max_ts = max(p[0] for p in points)
        meta = {"count": len(points), "min_ts": min_ts, "max_ts": max_ts}
        meta_bytes = json.dumps(meta).encode()
        meta_hash = self.kernel.write_blob(meta_bytes)
        tree_entries[f"series/{series_name}/meta"] = meta_hash

        tree = Tree(entries=tree_entries, tree_type="leaf")
        tree_hash = self.kernel.write_tree(tree)
        commit = Commit(
            tree_hash=tree_hash, parent_hash=parent_hash,
            timestamp=time.time(), message=f"ts write {series_name}",
            schema_hash=None,
        )
        commit_hash = self.kernel.write_commit(commit)
        self.kernel.reference(self.db_name, commit_hash)
        return commit_hash

    def read_series(self, series_name: str,
                    from_ts: Optional[int] = None,
                    to_ts: Optional[int] = None) -> list[tuple[int, float]]:
        commit_hash = self.kernel._resolve_name(self.db_name)
        commit = self.kernel.read_commit(commit_hash)
        tree = self.kernel.read_tree(commit.tree_hash)

        prefix = f"series/{series_name}/segment/"
        points = []
        for name in sorted(tree.entries):
            if not name.startswith(prefix):
                continue
            data = self.kernel.read_blob(tree.entries[name])
            count = struct.unpack("<I", data[:4])[0]
            pos = 4
            for _ in range(count):
                ts, val = struct.unpack("<Qf", data[pos:pos+12])
                pos += 12
                if from_ts and ts < from_ts:
                    continue
                if to_ts and ts > to_ts:
                    continue
                points.append((ts, val))
        return points

    def apply_retention(self, series_name: str, retention_days: int) -> str:
        """Drop segments older than retention_days. Creates a new commit
        that simply doesn't reference old segments (they're orphaned on disk
        until GC)."""
        cutoff_us = int((time.time() - retention_days * 86400) * 1e6)

        parent_hash = self.kernel._resolve_name(self.db_name)
        if parent_hash is None:
            return ""
        parent_commit = self.kernel.read_commit(parent_hash)
        parent_tree = self.kernel.read_tree(parent_commit.tree_hash)

        # Filter out old segments
        prefix = f"series/{series_name}/segment/"
        new_entries = {}
        dropped = 0
        for name, h in parent_tree.entries.items():
            if name.startswith(prefix):
                data = self.kernel.read_blob(h)
                count = struct.unpack("<I", data[:4])[0]
                # Read first timestamp to check age
                first_ts = struct.unpack("<Q", data[4:12])[0]
                if first_ts < cutoff_us:
                    dropped += 1
                    continue  # drop this segment
            new_entries[name] = h

        tree = Tree(entries=new_entries, tree_type="leaf")
        tree_hash = self.kernel.write_tree(tree)
        commit = Commit(
            tree_hash=tree_hash, parent_hash=parent_hash,
            timestamp=time.time(),
            message=f"retention: dropped {dropped} segments from {series_name}",
            schema_hash=None,
        )
        commit_hash = self.kernel.write_commit(commit)
        self.kernel.reference(self.db_name, commit_hash)
        return commit_hash


# ===========================================================================
# OCIView — Docker image layers, container registry
# ===========================================================================
#
# OCI is structurally different from everything else:
#   - Images are trees of compressed tar layers (not rows, not events)
#   - Manifest references config + layers by digest
#   - Layers are shared across images (deduplication is critical)
#   - Push/pull semantics (not append-only log)
#
# If the kernel can support OCI, it can support anything.

class OCIView:
    """
    An OCI container registry Lens. Stores:
      - blobs/<sha256> -> raw layer/config bytes (content-addressed, dedup'd)
      - manifests/<image>/<tag> -> JSON manifest referencing config + layers

    The kernel's content-addressing IS OCI's digest model. Pushing the same
    layer to multiple images costs zero extra storage.
    """

    def __init__(self, kernel: PondKernel, registry_name: str):
        self.kernel = kernel
        self.registry_name = registry_name

    def push_layer(self, layer_bytes: bytes) -> str:
        """Push a layer. Returns its digest (sha256)."""
        # Kernel content-addresses by sha256, which IS the OCI digest format
        return self.kernel.write_blob(layer_bytes)

    def push_config(self, config: dict) -> str:
        """Push an image config. Returns its digest."""
        return self.kernel.write_blob(json.dumps(config, sort_keys=True).encode())

    def push_manifest(self, image: str, tag: str,
                      config_digest: str,
                      layer_digests: list[str]) -> str:
        """Push an image manifest referencing config + layers."""
        manifest = {
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "config": {
                "mediaType": "application/vnd.oci.image.config.v1+json",
                "digest": f"sha256:{config_digest}",
                "size": len(self.kernel.read_blob(config_digest)),
            },
            "layers": [
                {
                    "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
                    "digest": f"sha256:{d}",
                    "size": len(self.kernel.read_blob(d)),
                }
                for d in layer_digests
            ],
        }
        manifest_bytes = json.dumps(manifest, sort_keys=True).encode()
        manifest_hash = self.kernel.write_blob(manifest_bytes)

        # Inherit parent's tree
        parent_hash = self.kernel._resolve_name(self.registry_name)
        tree_entries: dict[str, str] = {}
        if parent_hash is not None:
            parent_commit = self.kernel.read_commit(parent_hash)
            if parent_commit:
                parent_tree = self.kernel.read_tree(parent_commit.tree_hash)
                if parent_tree:
                    tree_entries = dict(parent_tree.entries)

        tree_entries[f"manifests/{image}/{tag}"] = manifest_hash

        tree = Tree(entries=tree_entries, tree_type="leaf")
        tree_hash = self.kernel.write_tree(tree)
        commit = Commit(
            tree_hash=tree_hash, parent_hash=parent_hash,
            timestamp=time.time(),
            message=f"push {image}:{tag}",
            schema_hash=None,
        )
        commit_hash = self.kernel.write_commit(commit)
        self.kernel.reference(self.registry_name, commit_hash)
        return commit_hash

    def pull_manifest(self, image: str, tag: str) -> dict:
        commit_hash = self.kernel._resolve_name(self.registry_name)
        commit = self.kernel.read_commit(commit_hash)
        tree = self.kernel.read_tree(commit.tree_hash)
        key = f"manifests/{image}/{tag}"
        if key not in tree.entries:
            raise ValueError(f"Image {image}:{tag} not found")
        return json.loads(self.kernel.read_blob(tree.entries[key]))

    def pull_layer(self, digest: str) -> bytes:
        """Pull a layer by its sha256 digest (without the 'sha256:' prefix)."""
        return self.kernel.read_blob(digest)
