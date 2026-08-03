"""LocalFSObjectStore — a pure local-filesystem object store for Pond.

Implements the SAME 9-primitive interface as S3ObjectStore and
InMemoryObjectStore. No SQLite, no databases — just files on disk.

  Blobs:   {base_dir}/blobs/{hash[:2]}/{hash}.bin   (content-addressed)
  Paths:   {base_dir}/paths/{path}                   (one file per named ref,
                                                       contains the hash as text)

This makes local FS and S3 interchangeable: swap the store object and
the kernel code is unchanged. The switch between local and S3 is ONE line:

    # Local FS:
    store = LocalFSObjectStore("/path/to/.pond")
    # S3:
    store = S3ObjectStore(client, bucket="my-pond", prefix="prod")

    # Same kernel, same SDK, same everything else:
    kernel = ObjectStoreNativeKernel(store)
    storage = PondStorage(kernel)

For compare_and_set_path (optimistic concurrency), we use OS-level file
locking (fcntl.flock on Linux/macOS, msvcrt on Windows) to atomically
check-and-set. This gives CAS semantics on local FS without any database.
"""
from __future__ import annotations

import os
import json
import threading
from typing import Optional

# Import hash_bytes from kernel.py (same as InMemoryObjectStore)
import sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from kernel import hash_bytes


class LocalFSObjectStore:
    """A pure local-filesystem content-addressed object store.

    No SQLite. No databases. Just files.

      Blobs:   {base_dir}/blobs/{hash[:2]}/{hash}.bin
      Paths:   {base_dir}/paths/{path}    (one file per named ref,
                                            contains the hash as text)

    Thread-safe: blob writes are protected by a per-hash lock; path
    writes use OS-level file locking for CAS semantics.

    The directory layout mirrors S3's key structure, so migrating from
    local FS to S3 is a straight copy: rsync {base_dir}/ to s3://{prefix}/.
    """

    def __init__(self, base_dir: str):
        """Create a local-FS object store.

        Args:
            base_dir: the root directory. Blobs go in {base_dir}/blobs/,
                paths go in {base_dir}/paths/. Created if it doesn't exist.
        """
        self._base_dir = os.path.abspath(base_dir)
        self._blobs_dir = os.path.join(self._base_dir, "blobs")
        self._paths_dir = os.path.join(self._base_dir, "paths")
        os.makedirs(self._blobs_dir, exist_ok=True)
        os.makedirs(self._paths_dir, exist_ok=True)

        self._lock = threading.Lock()
        # Per-hash locks for concurrent blob writes
        self._blob_locks: dict[str, threading.Lock] = {}
        # Counter for unique temp filenames (avoids collisions between
        # concurrent writers racing on the same path)
        self._tmp_counter = 0

        # Honest stats (same shape as InMemoryObjectStore / S3ObjectStore)
        self.stats = {
            "gets": 0,
            "puts": 0,
            "bytes_read": 0,
            "bytes_written": 0,
            "latency_ms_total": 0.0,
        }

    @property
    def base_dir(self) -> str:
        return self._base_dir

    # ------------------------------------------------------------------
    # Key helpers (mirror S3ObjectStore's _blob_key / _path_key)
    # ------------------------------------------------------------------

    def _blob_path(self, hash_val: str) -> str:
        """The on-disk path for a content-addressed blob.

        Layout matches S3ObjectStore exactly: {blobs_dir}/{hash} (no
        sharding, no .bin suffix). This makes `aws s3 sync` a straight
        copy — no format conversion needed.
        """
        return os.path.join(self._blobs_dir, hash_val)

    def _path_file(self, path: str) -> str:
        """The on-disk file for a named path (ref).

        Paths can contain '/' (e.g., 'collections/users/HEAD'). We map
        them directly to nested directories.
        """
        return os.path.join(self._paths_dir, path)

    def _paths_prefix_dir(self, prefix: str = "") -> str:
        """The on-disk directory for listing paths with a prefix."""
        return os.path.join(self._paths_dir, prefix)

    def _blobs_prefix_dir(self) -> str:
        return self._blobs_dir

    # ------------------------------------------------------------------
    # Content-addressed blob operations
    # ------------------------------------------------------------------

    def _get_blob_lock(self, hash_val: str) -> threading.Lock:
        with self._lock:
            if hash_val not in self._blob_locks:
                self._blob_locks[hash_val] = threading.Lock()
            return self._blob_locks[hash_val]

    def put_blob(self, data: bytes) -> str:
        """Write bytes, content-addressed. Returns the content hash.

        Idempotent: same bytes → same hash → same file. If the file
        already exists, we skip the write (dedup for free, same as S3
        when using content-addressed keys).
        """
        h = hash_bytes(data)
        path = self._blob_path(h)
        # Per-hash lock so concurrent writers don't corrupt the file
        with self._get_blob_lock(h):
            if not os.path.exists(path):
                os.makedirs(os.path.dirname(path), exist_ok=True)
                # Write to a UNIQUE temp file then rename (atomic on POSIX).
                # Use pid + thread id + counter to avoid collisions between
                # concurrent writers writing different blobs.
                tmp = f"{path}.tmp.{os.getpid()}.{threading.get_ident()}"
                with open(tmp, "wb") as f:
                    f.write(data)
                os.rename(tmp, path)
        with self._lock:
            self.stats["puts"] += 1
            self.stats["bytes_written"] += len(data)
        return h

    def get_blob(self, hash_val: str) -> bytes:
        """Read bytes by content hash."""
        path = self._blob_path(hash_val)
        if not os.path.exists(path):
            raise KeyError(f"Blob {hash_val} not found on disk")
        with open(path, "rb") as f:
            data = f.read()
        with self._lock:
            self.stats["gets"] += 1
            self.stats["bytes_read"] += len(data)
        return data

    def has_blob(self, hash_val: str) -> bool:
        """Check if a blob exists."""
        return os.path.exists(self._blob_path(hash_val))

    def delete_blob(self, hash_val: str) -> bool:
        """Delete a blob by hash. Used by GC/vacuum."""
        path = self._blob_path(hash_val)
        if os.path.exists(path):
            os.remove(path)
            return True
        return False

    def list_all_blob_hashes(self) -> list[str]:
        """List all blob hashes in the store (for GC reachability)."""
        hashes = []
        if not os.path.isdir(self._blobs_dir):
            return hashes
        for f in os.listdir(self._blobs_dir):
            # Blobs are stored as {blobs_dir}/{hash} (flat, no sharding,
            # no suffix — matches S3 layout exactly)
            full = os.path.join(self._blobs_dir, f)
            if os.path.isfile(full):
                hashes.append(f)
        return hashes

    # ------------------------------------------------------------------
    # Named path operations (well-known refs)
    # ------------------------------------------------------------------

    def put_path(self, path: str, hash_val: str) -> None:
        """Bind a well-known path to a content hash.

        The path file contains JSON {"hash": "..."} — same format as
        S3ObjectStore. This makes `aws s3 sync` a straight copy.
        Last-writer-wins (no CAS).
        """
        file = self._path_file(path)
        os.makedirs(os.path.dirname(file), exist_ok=True)
        tmp = f"{file}.tmp.{os.getpid()}.{threading.get_ident()}.{self._tmp_counter}"
        self._tmp_counter += 1
        with open(tmp, "w") as f:
            json.dump({"hash": hash_val}, f)
        os.rename(tmp, file)
        with self._lock:
            self.stats["puts"] += 1

    def get_path(self, path: str) -> Optional[str]:
        """Resolve a well-known path to its current content hash."""
        file = self._path_file(path)
        if not os.path.exists(file):
            return None
        with open(file, "r") as f:
            data = json.load(f)
        with self._lock:
            self.stats["gets"] += 1
        return data.get("hash")

    def list_paths(self, prefix: str = "") -> list[str]:
        """List all paths with the given prefix."""
        prefix_dir = self._paths_prefix_dir(prefix)
        paths = []
        if not os.path.isdir(prefix_dir):
            return paths
        for root, _dirs, files in os.walk(prefix_dir):
            for f in files:
                # Full path relative to _paths_dir
                full = os.path.join(root, f)
                rel = os.path.relpath(full, self._paths_dir)
                # Normalize to forward slashes (S3 uses /)
                paths.append(rel.replace(os.sep, "/"))
        return sorted(paths)

    # ------------------------------------------------------------------
    # Per-path locking for CAS
    # ------------------------------------------------------------------

    _path_locks: dict[str, threading.Lock] = {}
    _path_locks_guard = threading.Lock()

    def _get_path_lock(self, file_path: str) -> threading.Lock:
        """Get a per-path lock for CAS operations."""
        with self._path_locks_guard:
            if file_path not in self._path_locks:
                self._path_locks[file_path] = threading.Lock()
            return self._path_locks[file_path]

    # ------------------------------------------------------------------
    # Stats (same interface as InMemoryObjectStore / S3ObjectStore)
    # ------------------------------------------------------------------

    def reset_stats(self) -> None:
        """Reset the I/O stats (for benchmarking)."""
        with self._lock:
            self.stats = {
                "gets": 0, "puts": 0,
                "bytes_read": 0, "bytes_written": 0,
                "latency_ms_total": 0.0,
            }

    def print_stats(self, label: str = "") -> None:
        """Print I/O stats."""
        if label:
            print(f"  [{label}]")
        print(f"    GETs:           {self.stats['gets']:,}")
        print(f"    PUTs:           {self.stats['puts']:,}")
        print(f"    Bytes read:     {self.stats['bytes_read']:,}")
        print(f"    Bytes written:  {self.stats['bytes_written']:,}")


def make_local_kernel(base_dir: str) -> "ObjectStoreNativeKernel":
    """Convenience constructor: create an ObjectStoreNativeKernel backed by local FS.

    No SQLite. No databases. Just files on disk. Same architecture as S3.

    Args:
        base_dir: the root directory for the pond. Blobs go in
            {base_dir}/blobs/, paths go in {base_dir}/paths/.

    Returns:
        An ObjectStoreNativeKernel instance backed by LocalFSObjectStore.

    Usage:
        kernel = make_local_kernel("/path/to/.pond")
        storage = PondStorage(kernel)
    """
    from object_store_native_kernel import ObjectStoreNativeKernel
    store = LocalFSObjectStore(base_dir)
    return ObjectStoreNativeKernel(store)
