"""
Mock S3 backend for Pond kernel — simulates object storage with network latency.

Pond's kernel uses content-addressed blobs. This mock simulates S3 by:
  1. Storing blobs in memory (same as PondMinimal)
  2. Adding configurable network latency per read (default 50ms)
  3. Tracking total bytes read and blob fetch count

This lets benchmarks show the REAL pruning benefit: on S3, each blob
fetch has ~50ms network RTT. Skipping 99% of blobs saves 99% of RTTs.

Usage:
    from s3_mock_backend import S3MockKernel

    kernel = S3MockKernel(tmpdir, latency_ms=50)
    # Use kernel exactly like PondMinimal — it's a drop-in replacement
    # with simulated S3 latency.
"""

from __future__ import annotations

import os
import sys
import time
import hashlib
from typing import Optional

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                  "..", "..", "pond-core"))
from kernel import PondMinimal


class S3MockKernel(PondMinimal):
    """Mock S3 backend — PondMinimal with simulated network latency.

    Drop-in replacement for PondMinimal. Every read_blob() call adds
    `latency_ms` of sleep to simulate S3 network RTT. This makes
    pruning benefits visible in local benchmarks.

    Tracks:
      - total_bytes_read: sum of all blob bytes read
      - total_blob_fetches: count of read_blob calls
      - total_latency_ms: sum of simulated latency

    After a benchmark, print these stats to show the I/O savings:
      - Without pruning: 100 fetches × 50ms = 5s
      - With pruning: 1 fetch × 50ms = 0.05s (100x speedup)
    """

    def __init__(self, base_dir: str, latency_ms: float = 50.0):
        """Create an S3 mock kernel.

        Args:
            base_dir: filesystem path (used for local metadata storage)
            latency_ms: simulated network RTT per blob read (default 50ms,
                typical S3 GET latency)
        """
        super().__init__(base_dir)
        self._latency_ms = latency_ms
        # Extend the parent's stats dict (don't replace it — parent
        # uses self.stats["writes"], self.stats["reads"], etc.)
        self.stats["total_bytes_read"] = 0
        self.stats["total_blob_fetches"] = 0
        self.stats["total_latency_ms"] = 0.0

    def read_blob(self, hash_val: str) -> bytes:
        """Read a blob with simulated S3 latency."""
        # Simulate network RTT
        time.sleep(self._latency_ms / 1000.0)

        data = super().read_blob(hash_val)
        self.stats["total_bytes_read"] += len(data)
        self.stats["total_blob_fetches"] += 1
        self.stats["total_latency_ms"] += self._latency_ms
        return data

    def reset_stats(self):
        """Reset I/O statistics."""
        self.stats["total_bytes_read"] = 0
        self.stats["total_blob_fetches"] = 0
        self.stats["total_latency_ms"] = 0.0

    def print_stats(self, label: str = ""):
        """Print I/O statistics."""
        if label:
            print(f"  [{label}]")
        print(f"    Blob fetches:     {self.stats['total_blob_fetches']:,}")
        print(f"    Bytes read:       {self.stats['total_bytes_read']:,}")
        print(f"    Simulated latency:{self.stats['total_latency_ms']:.0f}ms "
              f"({self.stats['total_latency_ms']/1000:.1f}s)")
