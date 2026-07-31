"""
ObjectStoreNativeKernel — refs stored as content-addressed blobs, no SQLite.

THE PROBLEM:
  The original PondMinimal kernel stores refs (HEAD, branches, manifest
  pointer) in SQLite. That works on local disk but is NOT object-store-native:
    - S3 has no SQLite. You can't `SELECT * FROM roots` on S3.
    - SQLite is a single-node, mutable-state crutch.
    - It hides the real cost of ref resolution from benchmarks.

THE SOLUTION:
  Store refs as content-addressed blobs in the SAME object store as data.
  The "current HEAD" is found via a TINY root pointer blob.

  Layout:
    - All blobs (data + refs + manifest) live in the object store, addressed
      by SHA-256 hash.
    - The "root pointer" is a tiny blob at a WELL-KNOWN path
      (`{base_dir}/_root`) containing the hash of the current root ref blob.
    - The root ref blob is a small JSON dict mapping name → hash. It's
      content-addressed: every ref update writes a NEW root ref blob and
      updates the root pointer.

REF UPDATE FLOW (e.g., updating HEAD):
  1. Read the current root ref blob (1 GET to object store, cached for the
     duration of one transaction)
  2. Mutate the dict in memory: root["collections/users/HEAD"] = new_hash
  3. Write the new root ref blob (1 PUT)
  4. Write the new root pointer (1 PUT to well-known path)

REF RESOLUTION FLOW (e.g., reading HEAD):
  1. Read the root pointer (1 GET to well-known path — usually tiny, ~80 bytes)
  2. Read the root ref blob (1 GET — usually tiny, ~1KB for 50 refs)
  3. Look up the name in the dict (in-memory, free)

This is the same pattern Git uses: HEAD → commit → tree. Or Iceberg:
metadata.json → manifest-list → manifest → data files. Every level is a
content-addressed blob in the object store.

NO SQLite. NO local state. NO cache (for benchmarking — production caches
are an SDK concern, not a kernel concern).

This file provides TWO classes:
  - ObjectStoreNativeKernel: the kernel (drop-in replacement for PondMinimal)
  - InMemoryObjectStore: an in-memory object store for testing (no disk I/O,
    so benchmarks measure pure kernel + storage logic, not filesystem)

ROUND-TRIP ACCOUNTING (cold, no cache):
  Every kernel.read_blob() call is 1 S3 GET. Every kernel.write() is 1 S3 PUT.
  The kernel's stats dict tracks these EXACTLY — no SQLite lookups hidden.

  Ref resolution adds 2 GETs to the FIRST read in a session (root pointer +
  root ref blob). Subsequent reads in the same session can reuse the cached
  root ref blob (the SDK does this, not the kernel).

  A cold point lookup on a unified-storage collection:
    1. Read root pointer (1 GET — ~80 bytes)
    2. Read root ref blob (1 GET — ~1KB)
    3. Resolve collections/{name}/HEAD → commit_hash (in-memory)
    4. Read commit blob (1 GET — ~200 bytes)
    5. Resolve collections/{name}/manifest → manifest_hash (in-memory)
    6. Read manifest blob (1 GET — ~165 bytes/row group)
    7. Read 1 data blob (1 GET — actual data)

  Total cold: 5 GETs (root pointer + root ref + commit + manifest + 1 data blob)
  Subsequent reads in same session: 3 GETs (commit + manifest + data blob)
  Manifest cached: 2 GETs (commit + data blob)
  Commit also cached: 1 GET (data blob)

  The irreducible cold minimum is 5 GETs. With SDK caching, it drops to 1-3.

WHY THIS MATTERS:
  The previous design used SQLite for refs. That made ref resolution look
  "free" in benchmarks — but on real S3, you can't use SQLite. The honest
  accounting is: every ref resolution is at least 1 GET to the object store.
  This kernel makes that cost explicit and measurable.
"""

from __future__ import annotations

import os
import sys
import json
import time
import hashlib
import threading
from typing import Optional, Any

# Add pond-core to path so we can import hash_bytes
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from kernel import hash_bytes  # noqa: E402


# ---------------------------------------------------------------------------
# InMemoryObjectStore — a minimal object store for testing
# ---------------------------------------------------------------------------

class InMemoryObjectStore:
    """An in-memory content-addressed object store.

    Simulates S3 semantics:
      - put(key, data): writes bytes, returns the content hash
      - get(key): reads bytes
      - exists(key): checks if a key exists
      - list_prefix(prefix): lists keys with a prefix (like S3 list-objects)

    For benchmarks, set latency_ms > 0 to simulate S3 network RTT.
    Every get() call adds latency_ms to the total — this is the cost
    the benchmark should report (NO caching, NO SQLite hiding it).

    NOT thread-safe for writes (single-writer model, same as S3 strong
    consistency after read-after-write). Thread-safe for reads.
    """

    def __init__(self, latency_ms: float = 0.0):
        """Create an in-memory object store.

        Args:
            latency_ms: simulated network RTT per GET (default 0 — pure
                in-memory; set to 50.0 to simulate S3)
        """
        # content_hash → bytes (the actual blobs)
        self._blobs: dict[str, bytes] = {}
        # well-known path → content_hash (for root pointer etc.)
        self._paths: dict[str, str] = {}
        self._latency_ms = latency_ms
        self._lock = threading.Lock()

        # Honest stats — NO caching hidden
        self.stats = {
            "gets": 0,
            "puts": 0,
            "bytes_read": 0,
            "bytes_written": 0,
            "latency_ms_total": 0.0,
        }

    def put_blob(self, data: bytes) -> str:
        """Write bytes, content-addressed. Returns the content hash.

        Same bytes → same hash → idempotent (dedup for free, same as S3
        when you use content-addressed keys).
        """
        h = hash_bytes(data)
        with self._lock:
            if h not in self._blobs:
                self._blobs[h] = data
                self.stats["puts"] += 1
                self.stats["bytes_written"] += len(data)
        return h

    def get_blob(self, hash_val: str) -> bytes:
        """Read bytes by content hash. 1 GET = 1 S3 round trip."""
        # Simulate network RTT (do this OUTSIDE the lock to allow parallel reads)
        if self._latency_ms > 0:
            time.sleep(self._latency_ms / 1000.0)
            self.stats["latency_ms_total"] += self._latency_ms

        with self._lock:
            if hash_val not in self._blobs:
                raise KeyError(f"Blob {hash_val} not found")
            data = self._blobs[hash_val]
            self.stats["gets"] += 1
            self.stats["bytes_read"] += len(data)
        return data

    def has_blob(self, hash_val: str) -> bool:
        """Check if a blob exists (no latency — S3 HEAD is cheap)."""
        with self._lock:
            return hash_val in self._blobs

    def delete_blob(self, hash_val: str) -> bool:
        """Delete a blob by hash. Returns True if deleted, False if not found.

        This is a MAINTENANCE operation (not a kernel primitive). Used by
        the GC/vacuum system to reclaim space from unreachable blobs.

        On S3, this maps to DELETE object. On local FS, unlink. The kernel
        (Write/Read/Reference) stays FROZEN — deletion is a storage-backend
        concern, not a kernel concern.
        """
        with self._lock:
            if hash_val in self._blobs:
                del self._blobs[hash_val]
                # Also remove from paths that point at it
                to_remove = [p for p, h in self._paths.items() if h == hash_val]
                for p in to_remove:
                    del self._paths[p]
                return True
            return False

    def list_all_blob_hashes(self) -> list[str]:
        """List all blob hashes in the store (for GC reachability analysis).

        On S3, this is a list-objects-v2 with no prefix. On local FS,
        it's a directory listing.
        """
        with self._lock:
            return list(self._blobs.keys())

    def put_path(self, path: str, hash_val: str) -> None:
        """Bind a well-known path to a content hash.

        This is the ONLY mutable operation. Used for the root pointer:
        path="_root" → hash of the current root ref blob.

        On S3, this is a small object PUT to a well-known key. Same
        semantics: last writer wins, read-after-write consistency.
        """
        with self._lock:
            self._paths[path] = hash_val
            self.stats["puts"] += 1

    def compare_and_set_path(self, path: str, expected_hash: Optional[str],
                              new_hash: str) -> bool:
        """Atomic compare-and-set for a path.

        If the path currently points to expected_hash (or doesn't exist
        if expected_hash is None), set it to new_hash and return True.
        Otherwise return False (another writer won the race).

        On S3, this maps to a conditional PUT with If-Match/If-None-Match
        headers. On the in-memory store, it's protected by the lock.

        This is the foundation for optimistic concurrency: multiple
        writers can attempt to update HEAD simultaneously; the loser
        retries by re-reading and re-applying.
        """
        if self._latency_ms > 0:
            time.sleep(self._latency_ms / 1000.0)
            self.stats["latency_ms_total"] += self._latency_ms

        with self._lock:
            current = self._paths.get(path)
            if current == expected_hash:
                self._paths[path] = new_hash
                self.stats["puts"] += 1
                return True
            self.stats["gets"] += 1
            return False

    def get_path(self, path: str) -> Optional[str]:
        """Resolve a well-known path to its current content hash."""
        if self._latency_ms > 0:
            time.sleep(self._latency_ms / 1000.0)
            self.stats["latency_ms_total"] += self._latency_ms

        with self._lock:
            if path not in self._paths:
                return None
            self.stats["gets"] += 1
        return self._paths[path]

    def list_paths(self, prefix: str = "") -> list[str]:
        """List all paths with the given prefix (like S3 list-objects-v2)."""
        with self._lock:
            return sorted(p for p in self._paths if p.startswith(prefix))

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
        if self._latency_ms > 0:
            print(f"    Simulated RTT:  {self.stats['latency_ms_total']:.0f}ms "
                  f"({self.stats['latency_ms_total']/1000:.2f}s)")


# ---------------------------------------------------------------------------
# ObjectStoreNativeKernel — refs as content-addressed blobs, no SQLite
# ---------------------------------------------------------------------------

# Well-known path for the root pointer
_ROOT_POINTER_PATH = "_root"


class ObjectStoreNativeKernel:
    """A Pond kernel that uses ONLY the object store for all state.

    NO SQLite. NO local disk. ALL state (refs + blobs) lives in the
    object store as content-addressed blobs.

    Drop-in replacement for PondMinimal:
      - write(data) → hash  (same)
      - read(hash_or_name) → bytes  (same — but name resolution goes
        through the object store, not SQLite)
      - reference(name, hash)  (same — but updates a content-addressed
        root ref blob, not a SQLite row)

    The kernel's `stats` dict tracks EVERY object-store operation:
      - "writes": PUTs of data blobs
      - "reads": GETs of data blobs (NOT including ref resolution)
      - "ref_reads": GETs for ref resolution (root pointer + root ref blob)
      - "ref_writes": PUTs for ref updates (new root ref blob + root pointer)

    For benchmarks: use `kernel.reset_stats()` and `kernel.print_stats()`
    to see the HONEST round-trip count, no caching, no SQLite hidden.
    """

    def __init__(self, object_store: InMemoryObjectStore,
                  root_pointer_path: str = _ROOT_POINTER_PATH):
        """Create an object-store-native kernel.

        Args:
            object_store: the InMemoryObjectStore (or any object store
                with put_blob/get_blob/put_path/get_path)
            root_pointer_path: well-known path for the root pointer
                (default "_root")
        """
        self.store = object_store
        self._root_pointer_path = root_pointer_path
        # Cache of the root ref blob — the SDK can cache this, the kernel
        # does NOT. For honest benchmarking, call invalidate_root_cache()
        # before each measurement.
        self._root_ref_cache: Optional[dict[str, str]] = None
        self._root_ref_hash: Optional[str] = None

        self.stats = {
            "writes": 0,
            "reads": 0,
            "references": 0,
            "ref_reads": 0,    # GETs for ref resolution
            "ref_writes": 0,   # PUTs for ref updates
        }

    # ------------------------------------------------------------------
    # Primitive 1: Write
    # ------------------------------------------------------------------

    def write(self, data: bytes) -> str:
        """Write bytes to the object store. Returns the content hash.

        Idempotent: same bytes → same hash → no duplicate write.
        """
        h = self.store.put_blob(data)
        self.stats["writes"] += 1
        return h

    # ------------------------------------------------------------------
    # Primitive 2: Read
    # ------------------------------------------------------------------

    def read_blob(self, h: str) -> bytes:
        """Read a blob by hash directly (no name resolution)."""
        self.stats["reads"] += 1
        return self.store.get_blob(h)

    def read(self, hash_or_name: str) -> bytes:
        """Read a blob. If given a 64-char hex hash, read directly.
        If given a name, resolve it via the root ref blob, then read.
        """
        if len(hash_or_name) == 64 and all(c in "0123456789abcdef" for c in hash_or_name):
            return self.read_blob(hash_or_name)

        h = self.resolve(hash_or_name)
        if h is None:
            raise ValueError(f"Name '{hash_or_name}' not bound in root ref")
        return self.read_blob(h)

    # ------------------------------------------------------------------
    # Primitive 3: Reference (name → hash)
    # ------------------------------------------------------------------

    def reference(self, name: str, h: str) -> None:
        """Bind a name to a hash. Updates the root ref blob.

        Flow:
          1. Read the current root ref blob (1 GET — cached if possible)
          2. Mutate: root[name] = h
          3. Write a new root ref blob (1 PUT)
          4. Update the root pointer to point to it (1 PUT to well-known path)

        Total: 1 GET + 2 PUTs per reference update.
        """
        # Verify the blob exists (defensive — same as PondMinimal)
        if not self.store.has_blob(h):
            raise ValueError(f"Hash {h} does not refer to an existing blob")

        # Read the current root ref blob
        root_ref = self._load_root_ref()

        # Mutate
        root_ref[name] = h

        # Write the new root ref blob
        new_root_bytes = json.dumps(root_ref, sort_keys=True).encode()
        new_root_hash = self.store.put_blob(new_root_bytes)
        self.stats["ref_writes"] += 1

        # Update the root pointer
        self.store.put_path(self._root_pointer_path, new_root_hash)
        self.stats["ref_writes"] += 1

        # Update the cache
        self._root_ref_cache = root_ref
        self._root_ref_hash = new_root_hash
        self.stats["references"] += 1

    def resolve(self, name: str) -> Optional[str]:
        """Resolve a name to its current hash.

        Flow:
          1. Read the root pointer (1 GET — well-known path)
          2. Read the root ref blob (1 GET — content-addressed)
          3. Look up name in the dict (in-memory, free)

        Total: 2 GETs per cold resolve. Subsequent resolves in the same
        kernel instance reuse the cached root ref blob (0 GETs).

        For HONEST cold benchmarking, call invalidate_root_cache() before
        each measurement.
        """
        root_ref = self._load_root_ref()
        return root_ref.get(name)

    def list_names(self) -> list[str]:
        """List all bound names."""
        root_ref = self._load_root_ref()
        return sorted(root_ref.keys())

    # ------------------------------------------------------------------
    # Concurrency: per-path compare-and-set (CAS)
    #
    # These methods bypass the root ref blob and use dedicated paths
    # per collection. This enables:
    #   1. Optimistic concurrency — multiple writers can attempt to
    #      update HEAD simultaneously; losers retry.
    #   2. No global serialization point — each collection's HEAD is
    #      an independent CAS target.
    #   3. Cache-independent correctness — a new connection reads the
    #      current HEAD via a single path GET, no cache needed.
    # ------------------------------------------------------------------

    def cas_path(self, path: str, expected_hash: Optional[str],
                  new_hash: str) -> bool:
        """Atomic compare-and-set for a dedicated path.

        If path currently points to expected_hash (or doesn't exist if
        expected_hash is None), set it to new_hash and return True.
        Otherwise return False.

        Use this for HEAD refs: collections/{name}/HEAD → commit_hash.
        Multiple writers can race to update HEAD; the CAS ensures only
        one wins, the others retry.

        On S3 this maps to a conditional PUT (If-Match/If-None-Match).
        """
        self.stats["ref_writes"] += 1
        return self.store.compare_and_set_path(path, expected_hash, new_hash)

    def get_path(self, path: str) -> Optional[str]:
        """Read a dedicated path (1 GET, no root ref needed).

        Use this to read HEAD refs directly without going through the
        root ref blob — enables cache-independent reads.
        """
        return self.store.get_path(path)

    def set_path(self, path: str, hash_val: str) -> None:
        """Set a dedicated path (last-writer-wins, no CAS).

        Use this for paths where CAS isn't needed (e.g., manifest refs
        that are only written by the HEAD owner).
        """
        self.store.put_path(path, hash_val)

    # ------------------------------------------------------------------
    # MAINTENANCE operations (NOT kernel primitives)
    #
    # These are used by the GC/vacuum system to reclaim space. The kernel
    # stays FROZEN at 3 primitives (Write/Read/Reference). Deletion is a
    # storage-backend concern, exposed here for maintenance tools.
    # ------------------------------------------------------------------

    def delete_blob(self, hash_val: str) -> bool:
        """Delete a blob by hash. MAINTENANCE operation (not a primitive)."""
        return self.store.delete_blob(hash_val)

    def list_all_blob_hashes(self) -> list[str]:
        """List all blob hashes in the store. Used by GC reachability analysis."""
        return self.store.list_all_blob_hashes()

    def _load_root_ref(self) -> dict[str, str]:
        """Load the root ref blob, using the cache if valid.

        For HONEST benchmarking, call invalidate_root_cache() before
        each measurement to force a fresh GET.
        """
        if self._root_ref_cache is not None:
            return self._root_ref_cache

        # Read the root pointer (1 GET)
        root_hash = self.store.get_path(self._root_pointer_path)
        self.stats["ref_reads"] += 1

        if root_hash is None:
            # First time — empty root ref
            self._root_ref_cache = {}
            self._root_ref_hash = None
            return {}

        # Read the root ref blob (1 GET)
        root_bytes = self.store.get_blob(root_hash)
        self.stats["ref_reads"] += 1

        root_ref = json.loads(root_bytes)
        self._root_ref_cache = root_ref
        self._root_ref_hash = root_hash
        return root_ref

    def invalidate_root_cache(self) -> None:
        """Force the next resolve() to re-read the root ref blob from the store.

        Use this for HONEST cold-read benchmarking. In production, the SDK
        would cache the root ref blob across reads (it's content-addressed,
        so the cache is always consistent — if the root pointer changes,
        the cache is stale, but the SDK can detect this by comparing
        root_pointer hash with the cached root_ref_hash).
        """
        self._root_ref_cache = None
        self._root_ref_hash = None

    # ------------------------------------------------------------------
    # Stats + helpers
    # ------------------------------------------------------------------

    def reset_stats(self) -> None:
        """Reset all stats (for benchmarking)."""
        self.stats = {
            "writes": 0, "reads": 0, "references": 0,
            "ref_reads": 0, "ref_writes": 0,
        }
        # Also reset the underlying store's stats
        self.store.reset_stats()

    @property
    def base_dir(self) -> str:
        """Compat with CollectionMetadata's _detect_object_store check."""
        return "object-store://in-memory"

    def print_stats(self, label: str = "") -> None:
        """Print honest I/O stats — no caching hidden."""
        if label:
            print(f"  [{label}]")
        print(f"    Data blob GETs:    {self.stats['reads']:,}")
        print(f"    Data blob PUTs:    {self.stats['writes']:,}")
        print(f"    Ref GETs:          {self.stats['ref_reads']:,}  (root pointer + root ref blob)")
        print(f"    Ref PUTs:          {self.stats['ref_writes']:,}  (new root ref + root pointer)")
        print(f"    Total GETs:        {self.stats['reads'] + self.stats['ref_reads']:,}")
        print(f"    Total PUTs:        {self.stats['writes'] + self.stats['ref_writes']:,}")
        if self.store._latency_ms > 0:
            total_rtts = self.stats['reads'] + self.stats['ref_reads']
            total_latency = total_rtts * self.store._latency_ms
            print(f"    Simulated RTT:     {total_latency:.0f}ms "
                  f"({total_latency/1000:.2f}s at {self.store._latency_ms}ms/GET)")


# ---------------------------------------------------------------------------
# Convenience: create a kernel + store pair for testing
# ---------------------------------------------------------------------------

def make_object_store_native_kernel(latency_ms: float = 0.0
                                      ) -> tuple[ObjectStoreNativeKernel, InMemoryObjectStore]:
    """Create an object-store-native kernel for testing.

    Args:
        latency_ms: simulated S3 RTT per GET (default 0 = pure in-memory;
            set to 50.0 for S3 simulation)

    Returns:
        Tuple of (kernel, object_store). Use the kernel as you would
        PondMinimal. Use the object_store to inspect raw stats.
    """
    store = InMemoryObjectStore(latency_ms=latency_ms)
    kernel = ObjectStoreNativeKernel(store)
    return kernel, store
