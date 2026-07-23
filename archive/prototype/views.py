"""
Views layer — built ON TOP of the kernel, using only the 4 syscalls.

Each View interprets the same immutable objects differently. The kernel
knows nothing about formats; the Lens knows everything.

Views implemented here:
  - SQLLens      (writes/reads Parquet bytes; tabular interpretation)
  - VectorLens   (writes/reads Arrow IPC bytes of float arrays; vector interpretation)
  - StreamView   (writes/reads Arrow IPC bytes; append-only log interpretation)
  - GitLens      (stores files + directories as blobs/trees; version control)

The point: the kernel is bytes-only. Each View is a thin adapter that
knows one format. Adding a new Lens (e.g., LanceView, IcebergView)
requires NO changes to the kernel.
"""

from __future__ import annotations

import os
import io
import json
import time
import struct
from typing import Optional, Iterator

import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq

from pond_kernel import PondKernel, Tree, Commit, hash_bytes


# ===========================================================================
# SQL View — tabular data via Parquet
# ===========================================================================

class SQLLens:
    """
    A SQL view of immutable objects. Writes Arrow RecordBatches, seals them
    as Parquet bytes. Reads Parquet bytes and returns Arrow Tables.

    The kernel stores the Parquet bytes; the Lens interprets them.
    """

    def __init__(self, kernel: PondKernel, table_name: str):
        self.kernel = kernel
        self.table_name = table_name
        self._schema: Optional[pa.Schema] = None
        self._open_handle: Optional[str] = None
        self._open_writer: Optional[ipc.RecordBatchStreamWriter] = None
        self._open_sink: Optional[io.BytesIO] = None

    def create(self, schema: pa.Schema) -> None:
        """Define the table schema. Creates an initial empty commit."""
        self._schema = schema
        # No data yet — just register the name with an empty commit
        # (in a real implementation, we'd write a schema-only blob)

    def insert(self, batch: pa.RecordBatch) -> None:
        """Append a batch to the OPEN object for this table."""
        if self._schema is None:
            self._schema = batch.schema
        if self._open_handle is None:
            self._open_handle = f"sql_{self.table_name}_{int(time.time())}"
            self._open_sink = io.BytesIO()
            self._open_writer = ipc.new_stream(self._open_sink, self._schema)

        # Write the batch as Arrow IPC into the in-memory buffer,
        # then flush the buffer to the kernel as a write fragment.
        # (In a real implementation, we'd write directly to the kernel.)
        self._open_writer.write_batch(batch)

    def commit(self, message: str = "") -> str:
        """Seal the OPEN object, build the DAG, return the new commit hash."""
        if self._open_handle is None or self._open_writer is None:
            raise ValueError("Nothing to commit")

        # Close the Arrow IPC stream, get bytes
        self._open_writer.close()
        arrow_bytes = self._open_sink.getvalue()

        # Convert Arrow IPC -> Parquet (this is a Lens decision, not kernel)
        reader = ipc.open_stream(pa.BufferReader(arrow_bytes))
        table = pa.Table.from_batches(reader, self._schema)
        parquet_buf = io.BytesIO()
        pq.write_table(table, parquet_buf, compression="zstd")
        parquet_bytes = parquet_buf.getvalue()

        # Write the Parquet bytes to the kernel as a blob (single-shot)
        blob_hash = self.kernel.write_blob(parquet_bytes)

        # Build the DAG: tree (with parent's entries + new blob), commit
        parent_hash = self.kernel._resolve_name(self.table_name)

        parent_subtrees = []
        parent_unsealed_leaves = []
        if parent_hash is not None:
            parent_commit = self.kernel.read_commit(parent_hash)
            if parent_commit:
                parent_root = self.kernel.read_tree(parent_commit.tree_hash)
                if parent_root:
                    for name, h in sorted(parent_root.entries.items()):
                        if name.startswith("subtree/"):
                            parent_subtrees.append((name, h))
                        elif name.startswith("leaf/"):
                            parent_unsealed_leaves.append((name, h))

        blob_counter = len(parent_subtrees) * 256 + len(parent_unsealed_leaves)
        blob_name = f"sql/{self.table_name}/data/{blob_counter}"
        new_leaf = Tree(entries={blob_name: blob_hash}, tree_type="leaf")
        new_leaf_hash = self.kernel.write_tree(new_leaf)
        parent_unsealed_leaves.append((f"leaf/{blob_counter:08d}", new_leaf_hash))

        if len(parent_unsealed_leaves) >= 256:
            compacted = {}
            for _, lh in parent_unsealed_leaves:
                leaf = self.kernel.read_tree(lh)
                if leaf:
                    compacted.update(leaf.entries)
            sealed_subtree = Tree(entries=compacted, tree_type="leaf")
            sealed_subtree_hash = self.kernel.write_tree(sealed_subtree)
            parent_subtrees.append(
                (f"subtree/{len(parent_subtrees):08d}", sealed_subtree_hash)
            )
            parent_unsealed_leaves = []

        root_entries = {}
        for n, h in parent_subtrees:
            root_entries[n] = h
        for n, h in parent_unsealed_leaves:
            root_entries[n] = h

        root_tree = Tree(entries=root_entries, tree_type="interior")
        root_tree_hash = self.kernel.write_tree(root_tree)

        commit = Commit(
            tree_hash=root_tree_hash,
            parent_hash=parent_hash,
            timestamp=time.time(),
            message=message or f"sql commit {self.table_name}",
            schema_hash=hash_bytes(str(self._schema).encode()),
        )
        commit_hash = self.kernel.write_commit(commit)
        self.kernel.reference(self.table_name, commit_hash)

        # Reset OPEN state
        self._open_handle = None
        self._open_writer = None
        self._open_sink = None

        return commit_hash

    def read(self) -> pa.Table:
        """Read all data from the current commit. Returns an Arrow Table."""
        commit_hash = self.kernel._resolve_name(self.table_name)
        if commit_hash is None:
            raise ValueError(f"Table '{self.table_name}' has no commits")

        commit = self.kernel.read_commit(commit_hash)
        blob_hashes = self.kernel.walk_tree_for_blobs(commit.tree_hash)

        tables = []
        for bh in blob_hashes:
            parquet_bytes = self.kernel.read_blob(bh)
            t = pq.read_table(io.BytesIO(parquet_bytes))
            tables.append(t)

        if not tables:
            return pa.table({})
        return pa.concat_tables(tables)


# ===========================================================================
# Vector View — embedding collections via raw float bytes
# ===========================================================================

class VectorLens:
    """
    A vector view. Stores embeddings as raw float32 bytes (no Parquet —
    vectors don't need columnar format). Each "insert" appends a vector;
    "search" scans all vectors and computes distances.

    Demonstrates: a Lens that uses a DIFFERENT format than SQLLens, but
    the same kernel. The kernel doesn't care that these bytes are floats.
    """

    def __init__(self, kernel: PondKernel, collection_name: str, dim: int = 4):
        self.kernel = kernel
        self.collection_name = collection_name
        self.dim = dim
        self._open_handle: Optional[str] = None
        self._pending_vectors: list[list[float]] = []

    def insert(self, vector: list[float], metadata: Optional[dict] = None) -> None:
        """Queue a vector for insertion. Will be sealed on commit()."""
        if len(vector) != self.dim:
            raise ValueError(f"Vector dim {len(vector)} != collection dim {self.dim}")
        self._pending_vectors.append(vector)

    def commit(self, message: str = "") -> str:
        """Seal pending vectors as a blob of raw float32 bytes."""
        if not self._pending_vectors:
            raise ValueError("Nothing to commit")

        # Serialize: 4-byte dim + 4-byte count + count*dim*4 bytes of floats
        import struct
        buf = struct.pack("<II", self.dim, len(self._pending_vectors))
        for v in self._pending_vectors:
            buf += struct.pack(f"<{self.dim}f", *v)

        # Write to kernel (single-shot blob)
        blob_hash = self.kernel.write_blob(buf)

        # Build the DAG (similar to SQLLens but with vector naming)
        parent_hash = self.kernel._resolve_name(self.collection_name)
        parent_subtrees = []
        parent_unsealed_leaves = []
        if parent_hash is not None:
            parent_commit = self.kernel.read_commit(parent_hash)
            if parent_commit:
                parent_root = self.kernel.read_tree(parent_commit.tree_hash)
                if parent_root:
                    for name, h in sorted(parent_root.entries.items()):
                        if name.startswith("subtree/"):
                            parent_subtrees.append((name, h))
                        elif name.startswith("leaf/"):
                            parent_unsealed_leaves.append((name, h))

        blob_counter = len(parent_subtrees) * 256 + len(parent_unsealed_leaves)
        blob_name = f"vector/{self.collection_name}/embeddings/{blob_counter}"
        new_leaf = Tree(entries={blob_name: blob_hash}, tree_type="leaf")
        new_leaf_hash = self.kernel.write_tree(new_leaf)
        parent_unsealed_leaves.append((f"leaf/{blob_counter:08d}", new_leaf_hash))

        root_entries = {}
        for n, h in parent_subtrees:
            root_entries[n] = h
        for n, h in parent_unsealed_leaves:
            root_entries[n] = h

        root_tree = Tree(entries=root_entries, tree_type="interior")
        root_tree_hash = self.kernel.write_tree(root_tree)

        commit = Commit(
            tree_hash=root_tree_hash,
            parent_hash=parent_hash,
            timestamp=time.time(),
            message=message or f"vector commit {self.collection_name}",
            schema_hash=hash_bytes(f"vector:{self.dim}".encode()),
        )
        commit_hash = self.kernel.write_commit(commit)
        self.kernel.reference(self.collection_name, commit_hash)

        self._pending_vectors = []
        return commit_hash

    def search(self, query: list[float], k: int = 3) -> list[tuple[float, int]]:
        """Find k nearest neighbors by L2 distance. Returns (distance, blob_idx)."""
        import struct
        import math

        commit_hash = self.kernel._resolve_name(self.collection_name)
        if commit_hash is None:
            return []

        commit = self.kernel.read_commit(commit_hash)
        blob_hashes = self.kernel.walk_tree_for_blobs(commit.tree_hash)

        all_vectors = []
        for bh in blob_hashes:
            data = self.kernel.read_blob(bh)
            dim, count = struct.unpack("<II", data[:8])
            floats = struct.unpack(f"<{count * dim}f", data[8:])
            for i in range(count):
                all_vectors.append(floats[i * dim:(i + 1) * dim])

        # Compute L2 distances
        distances = []
        for i, v in enumerate(all_vectors):
            d = sum((a - b) ** 2 for a, b in zip(query, v))
            distances.append((math.sqrt(d), i))

        distances.sort()
        return distances[:k]


# ===========================================================================
# Stream View — append-only log via length-prefixed records
# ===========================================================================

class StreamView:
    """
    A streaming log view. Each commit is a batch of records. Reading the
    log walks the commit DAG in order, returning records from each blob.

    Demonstrates: a Lens that interprets bytes as a sequence of records,
    not as a table or vectors. Same kernel, different interpretation.
    """

    def __init__(self, kernel: PondKernel, topic_name: str):
        self.kernel = kernel
        self.topic_name = topic_name
        self._pending_records: list[bytes] = []

    def produce(self, record: bytes) -> None:
        """Queue a record for the next commit."""
        self._pending_records.append(record)

    def commit(self, message: str = "") -> str:
        """Seal pending records as a blob of length-prefixed records."""
        if not self._pending_records:
            raise ValueError("Nothing to commit")

        # Serialize: 4-byte count + each record as length-prefixed bytes
        buf = struct.pack("<I", len(self._pending_records))
        for rec in self._pending_records:
            buf += struct.pack("<Q", len(rec)) + rec

        # Write to kernel (single-shot blob)
        blob_hash = self.kernel.write_blob(buf)
        parent_hash = self.kernel._resolve_name(self.topic_name)
        parent_subtrees = []
        parent_unsealed_leaves = []
        if parent_hash is not None:
            parent_commit = self.kernel.read_commit(parent_hash)
            if parent_commit:
                parent_root = self.kernel.read_tree(parent_commit.tree_hash)
                if parent_root:
                    for name, h in sorted(parent_root.entries.items()):
                        if name.startswith("subtree/"):
                            parent_subtrees.append((name, h))
                        elif name.startswith("leaf/"):
                            parent_unsealed_leaves.append((name, h))

        blob_counter = len(parent_subtrees) * 256 + len(parent_unsealed_leaves)
        blob_name = f"stream/{self.topic_name}/log/{blob_counter}"
        new_leaf = Tree(entries={blob_name: blob_hash}, tree_type="leaf")
        new_leaf_hash = self.kernel.write_tree(new_leaf)
        parent_unsealed_leaves.append((f"leaf/{blob_counter:08d}", new_leaf_hash))

        root_entries = {}
        for n, h in parent_subtrees:
            root_entries[n] = h
        for n, h in parent_unsealed_leaves:
            root_entries[n] = h

        root_tree = Tree(entries=root_entries, tree_type="interior")
        root_tree_hash = self.kernel.write_tree(root_tree)

        commit = Commit(
            tree_hash=root_tree_hash,
            parent_hash=parent_hash,
            timestamp=time.time(),
            message=message or f"stream commit {self.topic_name}",
            schema_hash=None,
        )
        commit_hash = self.kernel.write_commit(commit)
        self.kernel.reference(self.topic_name, commit_hash)

        self._pending_records = []
        return commit_hash

    def consume(self, from_offset: int = 0) -> list[bytes]:
        """Read all records from the log, starting at offset.
        Strategy: read the LATEST commit's tree (which contains all blobs
        via inheritance), walk it once, return records in order.
        Don't walk every commit in the DAG — that would duplicate blobs."""
        commit_hash = self.kernel._resolve_name(self.topic_name)
        if commit_hash is None:
            return []

        # Just read the latest commit — its tree contains ALL blobs
        commit = self.kernel.read_commit(commit_hash)
        if commit is None:
            return []

        blob_hashes = self.kernel.walk_tree_for_blobs(commit.tree_hash)

        # Each blob is a batch of records. Read them in order.
        # (Blob order is determined by leaf name, which includes an index.)
        # Sort blob hashes by their leaf name index for correct ordering.
        # For simplicity here, we just read in tree-walk order.
        records = []
        offset = 0
        for bh in blob_hashes:
            data = self.kernel.read_blob(bh)
            count = struct.unpack("<I", data[:4])[0]
            pos = 4
            for _ in range(count):
                rec_len = struct.unpack("<Q", data[pos:pos+8])[0]
                pos += 8
                if offset >= from_offset:
                    records.append(data[pos:pos+rec_len])
                pos += rec_len
                offset += 1

        return records


# ===========================================================================
# Git View — files + directories, version control
# ===========================================================================

class GitLens:
    """
    A Git-style view. Stores files as blobs, directories as trees, commits
    as commit objects, branches as named references.

    Demonstrates: a Lens that interprets objects as a filesystem, not as
    a database. Same kernel, different interpretation. This is the
    "implement Git on top of Pond" experiment.
    """

    def __init__(self, kernel: PondKernel, repo_name: str):
        self.kernel = kernel
        self.repo_name = repo_name
        self._staged: dict[str, bytes] = {}  # path -> file content

    def add(self, path: str, content: bytes) -> None:
        """Stage a file for the next commit."""
        self._staged[path] = content

    def commit(self, message: str = "") -> str:
        """Seal staged files and create a commit. Like `git commit`.
        The new commit's tree contains ALL files (inherited from parent +
        staged changes), not just the changed files. This is the Git model."""
        if not self._staged:
            raise ValueError("Nothing to commit")

        # Inherit parent's tree entries (Git semantics: tree at commit = ALL files)
        parent_hash = self.kernel._resolve_name(self.repo_name)
        tree_entries: dict[str, str] = {}
        if parent_hash is not None:
            parent_commit = self.kernel.read_commit(parent_hash)
            if parent_commit:
                parent_tree = self.kernel.read_tree(parent_commit.tree_hash)
                if parent_tree:
                    tree_entries = dict(parent_tree.entries)

        # Stage new/updated files (overwrites parent entries with same path)
        for path, content in self._staged.items():
            blob_hash = self.kernel.write_blob(content)
            tree_entries[path] = blob_hash

        tree = Tree(entries=tree_entries, tree_type="leaf")
        tree_hash = self.kernel.write_tree(tree)

        commit = Commit(
            tree_hash=tree_hash,
            parent_hash=parent_hash,
            timestamp=time.time(),
            message=message,
            schema_hash=None,
        )
        commit_hash = self.kernel.write_commit(commit)
        self.kernel.reference(self.repo_name, commit_hash)

        self._staged = {}
        return commit_hash

    def read_file(self, path: str) -> bytes:
        """Read a file from the current commit. Like `git show HEAD:path`."""
        commit_hash = self.kernel._resolve_name(self.repo_name)
        if commit_hash is None:
            raise ValueError(f"Repo '{self.repo_name}' has no commits")
        commit = self.kernel.read_commit(commit_hash)
        tree = self.kernel.read_tree(commit.tree_hash)
        if path not in tree.entries:
            raise ValueError(f"File '{path}' not in commit")
        return self.kernel.read_blob(tree.entries[path])

    def log(self) -> list[dict]:
        """Show commit history. Like `git log`."""
        return self.kernel.history(self.repo_name)

    def checkout(self, commit_hash: str) -> None:
        """Move HEAD to a past commit. Like `git checkout`."""
        self.kernel.reference(self.repo_name, commit_hash)
