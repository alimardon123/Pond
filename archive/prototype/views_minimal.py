"""
Views on the MINIMAL kernel — no Tree/Commit helpers from the kernel.

Each View builds its own Tree/Commit/Tag patterns using only:
  - kernel.write(bytes) -> hash
  - kernel.read(hash_or_name) -> bytes
  - kernel.reference(name, hash)

Tree, Commit, Tag, Branch, OPEN/SEALED — all are Lens-level patterns
built from these 3 primitives. The kernel has zero knowledge of them.

If all 8 Views work on the minimal kernel, then Tree/Commit/OPEN-SEALED
were never primitive — they were patterns. That's the finding.
"""

from __future__ import annotations

import os
import io
import json
import time
import struct
from typing import Optional

import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq

from pond_minimal import PondMinimal, hash_bytes


# ===========================================================================
# Lens-level pattern helpers — built ONLY from Write/Read/Reference
# ===========================================================================
#
# These are NOT kernel primitives. They're conventions that Views use to
# organize blobs. Different Views could use different conventions.

def write_tree(kernel: PondMinimal, entries: dict[str, str]) -> str:
    """A Tree is just a blob containing serialized {name -> hash} mappings.
    This is a Lens-level pattern, not a kernel primitive."""
    data = json.dumps({"type": "tree", "entries": entries}, sort_keys=True).encode()
    return kernel.write(data)

def read_tree(kernel: PondMinimal, tree_hash: str) -> dict[str, str]:
    """Read a Tree blob and return its entries."""
    data = kernel.read_blob(tree_hash)
    obj = json.loads(data)
    return obj.get("entries", {})

def write_commit(kernel: PondMinimal, tree_hash: str,
                 parent_hash: Optional[str], message: str,
                 extra: Optional[dict] = None) -> str:
    """A Commit is just a blob containing serialized metadata.
    This is a Lens-level pattern, not a kernel primitive."""
    obj = {
        "type": "commit",
        "tree": tree_hash,
        "parent": parent_hash,
        "timestamp": time.time(),
        "message": message,
    }
    if extra:
        obj.update(extra)
    data = json.dumps(obj, sort_keys=True).encode()
    return kernel.write(data)

def read_commit(kernel: PondMinimal, commit_hash: str) -> dict:
    """Read a Commit blob and return its fields."""
    data = kernel.read_blob(commit_hash)
    return json.loads(data)

def make_tag(kernel: PondMinimal, name: str, commit_hash: str) -> None:
    """A Tag is just a Reference to a commit. Pure naming."""
    kernel.reference(name, commit_hash)

def make_branch(kernel: PondMinimal, name: str, commit_hash: str) -> None:
    """A Branch is just a Reference to a commit. Same as a Tag."""
    kernel.reference(name, commit_hash)


# ===========================================================================
# SQLLens — tabular data via Parquet
# ===========================================================================

class SQLLens:
    def __init__(self, kernel: PondMinimal, table_name: str):
        self.kernel = kernel
        self.table_name = table_name
        self._schema: Optional[pa.Schema] = None
        self._pending: list[pa.RecordBatch] = []

    def create(self, schema: pa.Schema) -> None:
        self._schema = schema

    def insert(self, batch: pa.RecordBatch) -> None:
        if self._schema is None:
            self._schema = batch.schema
        self._pending.append(batch)

    def commit(self, message: str = "") -> str:
        if not self._pending:
            raise ValueError("Nothing to commit")

        # Serialize batches as Parquet bytes (View decision, not kernel)
        table = pa.Table.from_batches(self._pending, self._schema)
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="zstd")
        parquet_bytes = buf.getvalue()
        blob_hash = self.kernel.write(parquet_bytes)  # Primitive 1: Write

        # Build Tree (View pattern, not kernel primitive)
        parent_hash = self.kernel.resolve(self.table_name)
        tree_entries = {}
        if parent_hash:
            parent_commit = read_commit(self.kernel, parent_hash)
            parent_tree = read_tree(self.kernel, parent_commit["tree"])
            tree_entries = dict(parent_tree)
        blob_counter = len(tree_entries)
        tree_entries[f"{self.table_name}/data/{blob_counter}"] = blob_hash
        tree_hash = write_tree(self.kernel, tree_entries)

        # Build Commit (View pattern, not kernel primitive)
        commit_hash = write_commit(self.kernel, tree_hash, parent_hash,
                                   message or f"sql commit {self.table_name}")

        # Update root namespace (Primitive 3: Reference)
        self.kernel.reference(self.table_name, commit_hash)
        self._pending = []
        return commit_hash

    def read(self) -> pa.Table:
        commit_hash = self.kernel.resolve(self.table_name)
        if not commit_hash:
            raise ValueError(f"Table '{self.table_name}' has no commits")
        commit = read_commit(self.kernel, commit_hash)
        tree = read_tree(self.kernel, commit["tree"])

        tables = []
        for name, bh in tree.items():
            if "/data/" in name:
                parquet_bytes = self.kernel.read_blob(bh)  # Primitive 2: Read
                t = pq.read_table(io.BytesIO(parquet_bytes))
                tables.append(t)
        if not tables:
            return pa.table({})
        return pa.concat_tables(tables)


# ===========================================================================
# VectorLens — embeddings via raw float bytes
# ===========================================================================

class VectorLens:
    def __init__(self, kernel: PondMinimal, collection_name: str, dim: int = 4):
        self.kernel = kernel
        self.collection_name = collection_name
        self.dim = dim
        self._pending: list[list[float]] = []

    def insert(self, vector: list[float]) -> None:
        if len(vector) != self.dim:
            raise ValueError(f"dim {len(vector)} != {self.dim}")
        self._pending.append(vector)

    def commit(self, message: str = "") -> str:
        if not self._pending:
            raise ValueError("Nothing to commit")
        buf = struct.pack("<II", self.dim, len(self._pending))
        for v in self._pending:
            buf += struct.pack(f"<{self.dim}f", *v)
        blob_hash = self.kernel.write(buf)

        parent_hash = self.kernel.resolve(self.collection_name)
        tree_entries = {}
        if parent_hash:
            parent_commit = read_commit(self.kernel, parent_hash)
            parent_tree = read_tree(self.kernel, parent_commit["tree"])
            tree_entries = dict(parent_tree)
        blob_counter = len(tree_entries)
        tree_entries[f"vector/{self.collection_name}/emb/{blob_counter}"] = blob_hash
        tree_hash = write_tree(self.kernel, tree_entries)
        commit_hash = write_commit(self.kernel, tree_hash, parent_hash,
                                   message or f"vector commit")
        self.kernel.reference(self.collection_name, commit_hash)
        self._pending = []
        return commit_hash

    def search(self, query: list[float], k: int = 3) -> list[tuple[float, int]]:
        import math
        commit_hash = self.kernel.resolve(self.collection_name)
        if not commit_hash:
            return []
        commit = read_commit(self.kernel, commit_hash)
        tree = read_tree(self.kernel, commit["tree"])

        all_vecs = []
        for name, bh in tree.items():
            if "/emb/" in name:
                data = self.kernel.read_blob(bh)
                dim, count = struct.unpack("<II", data[:8])
                floats = struct.unpack(f"<{count * dim}f", data[8:])
                for i in range(count):
                    all_vecs.append(floats[i * dim:(i + 1) * dim])

        dists = []
        for i, v in enumerate(all_vecs):
            d = math.sqrt(sum((a - b) ** 2 for a, b in zip(query, v)))
            dists.append((d, i))
        dists.sort()
        return dists[:k]


# ===========================================================================
# StreamView — append-only log
# ===========================================================================

class StreamView:
    def __init__(self, kernel: PondMinimal, topic_name: str):
        self.kernel = kernel
        self.topic_name = topic_name
        self._pending: list[bytes] = []

    def produce(self, record: bytes) -> None:
        self._pending.append(record)

    def commit(self, message: str = "") -> str:
        if not self._pending:
            raise ValueError("Nothing to commit")
        buf = struct.pack("<I", len(self._pending))
        for rec in self._pending:
            buf += struct.pack("<Q", len(rec)) + rec
        blob_hash = self.kernel.write(buf)

        parent_hash = self.kernel.resolve(self.topic_name)
        tree_entries = {}
        if parent_hash:
            parent_commit = read_commit(self.kernel, parent_hash)
            parent_tree = read_tree(self.kernel, parent_commit["tree"])
            tree_entries = dict(parent_tree)
        blob_counter = len(tree_entries)
        tree_entries[f"stream/{self.topic_name}/log/{blob_counter}"] = blob_hash
        tree_hash = write_tree(self.kernel, tree_entries)
        commit_hash = write_commit(self.kernel, tree_hash, parent_hash,
                                   message or f"stream commit")
        self.kernel.reference(self.topic_name, commit_hash)
        self._pending = []
        return commit_hash

    def consume(self) -> list[bytes]:
        commit_hash = self.kernel.resolve(self.topic_name)
        if not commit_hash:
            return []
        commit = read_commit(self.kernel, commit_hash)
        tree = read_tree(self.kernel, commit["tree"])

        records = []
        for name in sorted(tree):
            if "/log/" in name:
                data = self.kernel.read_blob(tree[name])
                count = struct.unpack("<I", data[:4])[0]
                pos = 4
                for _ in range(count):
                    rec_len = struct.unpack("<Q", data[pos:pos+8])[0]
                    pos += 8
                    records.append(data[pos:pos+rec_len])
                    pos += rec_len
        return records


# ===========================================================================
# GitLens — files + directories + commits
# ===========================================================================

class GitLens:
    def __init__(self, kernel: PondMinimal, repo_name: str):
        self.kernel = kernel
        self.repo_name = repo_name
        self._staged: dict[str, bytes] = {}

    def add(self, path: str, content: bytes) -> None:
        self._staged[path] = content

    def commit(self, message: str = "") -> str:
        if not self._staged:
            raise ValueError("Nothing to commit")

        parent_hash = self.kernel.resolve(self.repo_name)
        tree_entries = {}
        if parent_hash:
            parent_commit = read_commit(self.kernel, parent_hash)
            parent_tree = read_tree(self.kernel, parent_commit["tree"])
            tree_entries = dict(parent_tree)

        for path, content in self._staged.items():
            blob_hash = self.kernel.write(content)
            tree_entries[path] = blob_hash

        tree_hash = write_tree(self.kernel, tree_entries)
        commit_hash = write_commit(self.kernel, tree_hash, parent_hash, message)
        self.kernel.reference(self.repo_name, commit_hash)
        self._staged = {}
        return commit_hash

    def read_file(self, path: str) -> bytes:
        commit_hash = self.kernel.resolve(self.repo_name)
        commit = read_commit(self.kernel, commit_hash)
        tree = read_tree(self.kernel, commit["tree"])
        if path not in tree:
            raise ValueError(f"File '{path}' not in commit")
        return self.kernel.read_blob(tree[path])

    def log(self) -> list[dict]:
        history = []
        current = self.kernel.resolve(self.repo_name)
        visited = set()
        while current and current not in visited:
            visited.add(current)
            c = read_commit(self.kernel, current)
            history.append({"commit": current, "message": c["message"],
                            "parent": c["parent"]})
            current = c["parent"]
        return history


# ===========================================================================
# GraphView — nodes, edges, adjacency
# ===========================================================================

class GraphView:
    def __init__(self, kernel: PondMinimal, graph_name: str):
        self.kernel = kernel
        self.graph_name = graph_name
        self._nodes: dict[str, dict] = {}
        self._edges: list[dict] = []

    def add_node(self, node_id: str, properties: dict) -> None:
        self._nodes[node_id] = properties

    def add_edge(self, src: str, dst: str, properties: Optional[dict] = None) -> None:
        self._edges.append({"src": src, "dst": dst, "properties": properties or {}})

    def commit(self, message: str = "") -> str:
        if not self._nodes and not self._edges:
            raise ValueError("Nothing to commit")

        parent_hash = self.kernel.resolve(self.graph_name)
        tree_entries = {}
        if parent_hash:
            parent_commit = read_commit(self.kernel, parent_hash)
            parent_tree = read_tree(self.kernel, parent_commit["tree"])
            tree_entries = dict(parent_tree)

        for node_id, props in self._nodes.items():
            blob = json.dumps({"id": node_id, "properties": props}, sort_keys=True).encode()
            tree_entries[f"nodes/{node_id}"] = self.kernel.write(blob)

        existing_edge_idxs = [int(k.split("/")[-1]) for k in tree_entries
                              if k.startswith("edges/") and k.split("/")[-1].isdigit()]
        next_idx = (max(existing_edge_idxs) + 1) if existing_edge_idxs else 0
        for edge in self._edges:
            blob = json.dumps(edge, sort_keys=True).encode()
            tree_entries[f"edges/{next_idx:08d}"] = self.kernel.write(blob)
            next_idx += 1

        # Build adjacency index
        adjacency: dict[str, list[str]] = {}
        for name, bh in tree_entries.items():
            if name.startswith("edges/"):
                edge_data = json.loads(self.kernel.read_blob(bh))
                adjacency.setdefault(edge_data["src"], []).append(bh)
        for src, edge_hashes in adjacency.items():
            blob = json.dumps({"src": src, "edges": edge_hashes}).encode()
            tree_entries[f"adjacency/{src}"] = self.kernel.write(blob)

        tree_hash = write_tree(self.kernel, tree_entries)
        commit_hash = write_commit(self.kernel, tree_hash, parent_hash,
                                   message or "graph commit")
        self.kernel.reference(self.graph_name, commit_hash)
        self._nodes = {}
        self._edges = []
        return commit_hash

    def get_node(self, node_id: str) -> Optional[dict]:
        commit_hash = self.kernel.resolve(self.graph_name)
        commit = read_commit(self.kernel, commit_hash)
        tree = read_tree(self.kernel, commit["tree"])
        key = f"nodes/{node_id}"
        if key not in tree:
            return None
        return json.loads(self.kernel.read_blob(tree[key]))

    def neighbors(self, node_id: str) -> list[dict]:
        commit_hash = self.kernel.resolve(self.graph_name)
        commit = read_commit(self.kernel, commit_hash)
        tree = read_tree(self.kernel, commit["tree"])
        key = f"adjacency/{node_id}"
        if key not in tree:
            return []
        adj = json.loads(self.kernel.read_blob(tree[key]))
        return [json.loads(self.kernel.read_blob(eh)) for eh in adj["edges"]]


# ===========================================================================
# MLView — model checkpoints
# ===========================================================================

class MLView:
    def __init__(self, kernel: PondMinimal, registry_name: str):
        self.kernel = kernel
        self.registry_name = registry_name

    def log_checkpoint(self, model_name: str, step: int,
                       weights: bytes, metadata: dict) -> str:
        weights_hash = self.kernel.write(weights)
        meta = {"model": model_name, "step": step, "weights_hash": weights_hash, **metadata}
        meta_hash = self.kernel.write(json.dumps(meta, sort_keys=True).encode())

        parent_hash = self.kernel.resolve(self.registry_name)
        tree_entries = {}
        if parent_hash:
            parent_commit = read_commit(self.kernel, parent_hash)
            parent_tree = read_tree(self.kernel, parent_commit["tree"])
            tree_entries = dict(parent_tree)
        tree_entries[f"weights/{model_name}/{step:08d}"] = weights_hash
        tree_entries[f"metadata/{model_name}/{step:08d}"] = meta_hash
        tree_hash = write_tree(self.kernel, tree_entries)
        commit_hash = write_commit(self.kernel, tree_hash, parent_hash,
                                   f"checkpoint {model_name}@{step}")
        self.kernel.reference(self.registry_name, commit_hash)
        return commit_hash

    def get_weights(self, model_name: str, step: int) -> bytes:
        commit_hash = self.kernel.resolve(self.registry_name)
        commit = read_commit(self.kernel, commit_hash)
        tree = read_tree(self.kernel, commit["tree"])
        key = f"weights/{model_name}/{step:08d}"
        return self.kernel.read_blob(tree[key])

    def get_metadata(self, model_name: str, step: int) -> dict:
        commit_hash = self.kernel.resolve(self.registry_name)
        commit = read_commit(self.kernel, commit_hash)
        tree = read_tree(self.kernel, commit["tree"])
        key = f"metadata/{model_name}/{step:08d}"
        return json.loads(self.kernel.read_blob(tree[key]))


# ===========================================================================
# TimeSeriesView — segments + retention
# ===========================================================================

class TimeSeriesView:
    def __init__(self, kernel: PondMinimal, db_name: str):
        self.kernel = kernel
        self.db_name = db_name

    def write_points(self, series_name: str, points: list[tuple[int, float]]) -> str:
        buf = struct.pack("<I", len(points))
        for ts, val in points:
            buf += struct.pack("<Qf", ts, val)
        blob_hash = self.kernel.write(buf)

        parent_hash = self.kernel.resolve(self.db_name)
        tree_entries = {}
        if parent_hash:
            parent_commit = read_commit(self.kernel, parent_hash)
            parent_tree = read_tree(self.kernel, parent_commit["tree"])
            tree_entries = dict(parent_tree)
        prefix = f"series/{series_name}/segment/"
        existing = [int(k[len(prefix):]) for k in tree_entries
                    if k.startswith(prefix) and k[len(prefix):].isdigit()]
        next_idx = (max(existing) + 1) if existing else 0
        tree_entries[f"{prefix}{next_idx:08d}"] = blob_hash
        tree_hash = write_tree(self.kernel, tree_entries)
        commit_hash = write_commit(self.kernel, tree_hash, parent_hash,
                                   f"ts write {series_name}")
        self.kernel.reference(self.db_name, commit_hash)
        return commit_hash

    def read_series(self, series_name: str) -> list[tuple[int, float]]:
        commit_hash = self.kernel.resolve(self.db_name)
        commit = read_commit(self.kernel, commit_hash)
        tree = read_tree(self.kernel, commit["tree"])
        prefix = f"series/{series_name}/segment/"
        points = []
        for name in sorted(tree):
            if not name.startswith(prefix):
                continue
            data = self.kernel.read_blob(tree[name])
            count = struct.unpack("<I", data[:4])[0]
            pos = 4
            for _ in range(count):
                ts, val = struct.unpack("<Qf", data[pos:pos+12])
                points.append((ts, val))
                pos += 12
        return points


# ===========================================================================
# OCIView — container registry (uses Tree/Commit minimally)
# ===========================================================================

class OCIView:
    def __init__(self, kernel: PondMinimal, registry_name: str):
        self.kernel = kernel
        self.registry_name = registry_name

    def push_layer(self, layer_bytes: bytes) -> str:
        return self.kernel.write(layer_bytes)

    def push_config(self, config: dict) -> str:
        return self.kernel.write(json.dumps(config, sort_keys=True).encode())

    def push_manifest(self, image: str, tag: str,
                      config_digest: str, layer_digests: list[str]) -> str:
        manifest = {
            "schemaVersion": 2,
            "config": {"digest": f"sha256:{config_digest}",
                       "size": len(self.kernel.read_blob(config_digest))},
            "layers": [{"digest": f"sha256:{d}",
                        "size": len(self.kernel.read_blob(d))} for d in layer_digests],
        }
        manifest_hash = self.kernel.write(json.dumps(manifest, sort_keys=True).encode())

        parent_hash = self.kernel.resolve(self.registry_name)
        tree_entries = {}
        if parent_hash:
            parent_commit = read_commit(self.kernel, parent_hash)
            parent_tree = read_tree(self.kernel, parent_commit["tree"])
            tree_entries = dict(parent_tree)
        tree_entries[f"manifests/{image}/{tag}"] = manifest_hash
        tree_hash = write_tree(self.kernel, tree_entries)
        commit_hash = write_commit(self.kernel, tree_hash, parent_hash,
                                   f"push {image}:{tag}")
        self.kernel.reference(self.registry_name, commit_hash)
        return commit_hash

    def pull_manifest(self, image: str, tag: str) -> dict:
        commit_hash = self.kernel.resolve(self.registry_name)
        commit = read_commit(self.kernel, commit_hash)
        tree = read_tree(self.kernel, commit["tree"])
        key = f"manifests/{image}/{tag}"
        return json.loads(self.kernel.read_blob(tree[key]))

    def pull_layer(self, digest: str) -> bytes:
        return self.kernel.read_blob(digest)
