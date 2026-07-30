"""
Mock S3 backend for Pond kernel — simulates object storage with network latency.

OBJECT-STORE-NATIVE (no SQLite):
  This mock uses ObjectStoreNativeKernel under the hood — refs are stored
  as content-addressed blobs in an InMemoryObjectStore, NOT in SQLite.
  This makes the mock HONEST: every ref resolution is a real GET (counted
  in stats), just like real S3.

  The original S3MockKernel extended PondMinimal (SQLite) — which hid the
  cost of ref resolution from benchmarks. This version fixes that.

Pond's kernel uses content-addressed blobs. This mock simulates S3 by:
  1. Storing blobs in an InMemoryObjectStore (no disk I/O)
  2. Adding configurable network latency per GET (default 50ms)
  3. Tracking total bytes read and blob fetch count

This lets benchmarks show the REAL pruning benefit: on S3, each blob
fetch has ~50ms network RTT. Skipping 99% of blobs saves 99% of RTTs.

Usage:
    from s3_mock_backend import S3MockKernel

    kernel = S3MockKernel(latency_ms=50)
    # Use kernel exactly like PondMinimal — it's a drop-in replacement
    # with simulated S3 latency. NO SQLite — all state in the object store.
"""

from __future__ import annotations

import os
import sys
import time
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from object_store_native_kernel import (
    ObjectStoreNativeKernel, InMemoryObjectStore,
)


class S3MockKernel(ObjectStoreNativeKernel):
    """Mock S3 backend — ObjectStoreNativeKernel with simulated network latency.

    Drop-in replacement for PondMinimal. Every read_blob() call adds
    `latency_ms` of sleep to simulate S3 network RTT. This makes
    pruning benefits visible in local benchmarks.

    NO SQLite — all state (refs + blobs) lives in the InMemoryObjectStore
    as content-addressed blobs. Every ref resolution is a real GET,
    counted in stats.

    Tracks:
      - total_bytes_read: sum of all blob bytes read
      - total_blob_fetches: count of read_blob calls
      - total_latency_ms: sum of simulated latency
      - data_reads, data_writes, ref_reads, ref_writes (from parent)

    After a benchmark, print these stats to show the I/O savings:
      - Without pruning: 100 fetches × 50ms = 5s
      - With pruning: 1 fetch × 50ms = 0.05s (100x speedup)
    """

    def __init__(self, base_dir: str = "/tmp/pond-s3-mock",
                  latency_ms: float = 50.0):
        """Create an S3 mock kernel.

        Args:
            base_dir: ignored (kept for backward compat with PondMinimal).
                All state lives in the InMemoryObjectStore.
            latency_ms: simulated network RTT per blob read (default 50ms,
                typical S3 GET latency)
        """
        # Create an InMemoryObjectStore with the configured latency
        store = InMemoryObjectStore(latency_ms=latency_ms)
        super().__init__(store)
        self._latency_ms = latency_ms
        self._base_dir = base_dir  # for CollectionMetadata compat
        # Extend the parent's stats dict
        self.stats["total_bytes_read"] = 0
        self.stats["total_blob_fetches"] = 0
        self.stats["total_latency_ms"] = 0.0

    @property
    def base_dir(self) -> str:
        """Compat with CollectionMetadata's _detect_object_store check."""
        return "s3://mock"

    def read_blob(self, hash_val: str) -> bytes:
        """Read a blob with simulated S3 latency."""
        # The InMemoryObjectStore already adds latency, but we also track
        # stats here for backward compat with old benchmark code.
        data = super().read_blob(hash_val)
        self.stats["total_bytes_read"] += len(data)
        self.stats["total_blob_fetches"] += 1
        # Note: latency is already added by InMemoryObjectStore.get_blob
        # and counted in self.store.stats["latency_ms_total"]
        return data

    def reset_stats(self):
        """Reset I/O statistics."""
        super().reset_stats()
        self.stats["total_bytes_read"] = 0
        self.stats["total_blob_fetches"] = 0
        self.stats["total_latency_ms"] = 0.0

    def print_stats(self, label: str = ""):
        """Print I/O statistics — honest, no SQLite hidden."""
        if label:
            print(f"  [{label}]")
        print(f"    Blob fetches:     {self.stats['total_blob_fetches']:,}")
        print(f"    Bytes read:       {self.stats['total_bytes_read']:,}")
        print(f"    Ref GETs:         {self.stats['ref_reads']:,}")
        print(f"    Ref PUTs:         {self.stats['ref_writes']:,}")
        if self._latency_ms > 0:
            total_rtts = (self.stats['total_blob_fetches'] +
                          self.stats['ref_reads'])
            total_latency = total_rtts * self._latency_ms
            print(f"    Simulated latency:{total_latency:.0f}ms "
                  f"({total_latency/1000:.1f}s at {self._latency_ms}ms/GET)")

    def close(self) -> None:
        """Close the kernel (no-op for in-memory store).

        Provided for backward compat with PondMinimal.close() which
        closes the SQLite connection. The object-store-native kernel
        has no resources to close.
        """
        pass
