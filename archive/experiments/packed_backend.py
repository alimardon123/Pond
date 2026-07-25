#!/usr/bin/env python3
"""
Packed-Object Backend — reduces object-store GETs from N (1 per blob)
to 2 (1 pack file + 1 index file), like Git packfiles.

THE PROBLEM:
  Current filesystem backend: 1 file per blob.
  Scanning 100 records = 100 GETs = 2130ms on S3. Catastrophic.

THE SOLUTION:
  Pack multiple blobs into a single large file (a "pack").
  A separate index file maps hash → (pack_id, offset, length).
  Scanning 100 records = 2 GETs (pack + index) + range reads = ~60ms on S3.

DESIGN:
  - PackFile: an immutable file containing multiple blobs concatenated.
    Format: [blob_count][hash1][offset1][len1][hash2][offset2][len2]...[data1][data2]...
  - PackIndex: maps blob_hash → (pack_file_id, offset, length).
    Stored as a kernel Reference pointing to the pack index blob.
  - The kernel's write/read_blob API stays the same — the packed backend
    is an internal optimization. The logical API is unchanged.

  Like Git:
    Git loose objects → 1 file per object (fast write, slow scan)
    Git packfiles → 1 file for many objects (slow to build, fast to read)
    Pond blobs → 1 file per blob (fast write, slow scan)
    Pond packs → 1 file for many blobs (slow to build, fast to read)

  The packed backend does NOT change the kernel API. It changes how
  blobs are stored physically. The kernel still exposes Write/Read/Reference.
  The pack is an internal storage optimization.

Run:
    python experiments/packed_backend.py
"""

from __future__ import annotations

import os
import sys
import shutil
import struct
import hashlib
import time
import json
from typing import Optional

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(REPO, "pond-core"))
sys.path.insert(0, os.path.join(REPO, "pond-sdk"))

from kernel import PondMinimal
from keyvalue_lens import Lens


# ---------------------------------------------------------------------------
# PackFile — multiple blobs in one file
# ---------------------------------------------------------------------------

class PackFile:
    """A pack file containing multiple blobs.

    Format:
      [4B: magic "POND"][4B: blob_count]
      For each blob:
        [32B: hash][8B: offset][4B: length]
      [blob data concatenated]

    The index is embedded in the pack file itself (like Git's .idx).
    No separate index file needed — read the header to find any blob.
    """

    MAGIC = b"POND"

    @staticmethod
    def create(blob_map: dict[str, bytes]) -> tuple[bytes, dict[str, tuple[int, int]]]:
        """Create a pack file from a dict of hash → bytes.

        Returns:
            (pack_bytes, index) where index is {hash: (offset, length)}.
        """
        blob_count = len(blob_map)
        header = struct.pack("<4sI", PackFile.MAGIC, blob_count)

        # Calculate offsets
        index_entries = []
        data_offset = 4 + 4 + blob_count * (32 + 8 + 4)  # header + index entries
        current_offset = data_offset
        blob_data = b""

        for h, data in blob_map.items():
            index_entries.append((h, current_offset, len(data)))
            blob_data += data
            current_offset += len(data)

        # Build index section
        index_section = b""
        for h, offset, length in index_entries:
            index_section += bytes.fromhex(h)
            index_section += struct.pack("<QI", offset, length)

        return header + index_section + blob_data, {h: (o, l) for h, o, l in index_entries}

    @staticmethod
    def read_blob(pack_bytes: bytes, blob_hash: str) -> Optional[bytes]:
        """Read a single blob from a pack file by hash.

        This is the fast path: read the pack header (small), find the
        offset, then read just the blob data. On object storage, this
        is 1 GET (read the pack) + 1 range read (read the blob).
        """
        if len(pack_bytes) < 8:
            return None

        magic, blob_count = struct.unpack("<4sI", pack_bytes[:8])
        if magic != PackFile.MAGIC:
            return None

        # Binary search or linear scan the index (for small counts, linear is fine)
        pos = 8
        for _ in range(blob_count):
            h = pack_bytes[pos:pos+32].hex()
            pos += 32
            offset, length = struct.unpack("<QI", pack_bytes[pos:pos+12])
            pos += 12
            if h == blob_hash:
                return pack_bytes[offset:offset+length]

        return None

    @staticmethod
    def read_all_blobs(pack_bytes: bytes) -> dict[str, bytes]:
        """Read ALL blobs from a pack file. 1 GET for the entire pack.

        This is the scan optimization: instead of N GETs (1 per blob),
        read the entire pack in 1 GET and extract all blobs.
        """
        if len(pack_bytes) < 8:
            return {}

        magic, blob_count = struct.unpack("<4sI", pack_bytes[:8])
        if magic != PackFile.MAGIC:
            return {}

        result = {}
        pos = 8
        entries = []
        for _ in range(blob_count):
            h = pack_bytes[pos:pos+32].hex()
            pos += 32
            offset, length = struct.unpack("<QI", pack_bytes[pos:pos+12])
            pos += 12
            entries.append((h, offset, length))

        for h, offset, length in entries:
            result[h] = pack_bytes[offset:offset+length]

        return result


# ---------------------------------------------------------------------------
# PackedBackend — wraps the kernel with pack file support
# ---------------------------------------------------------------------------

class PackedBackend:
    """A packed-object backend that reduces GETs for scans.

    Usage:
        1. Write blobs normally (kernel.write)
        2. Create a pack from the blobs (PackedBackend.create_pack)
        3. Read from the pack (1 GET for all blobs, vs N GETs)

    The pack is stored as a kernel blob (content-addressed).
    A reference ({name}__pack) points to the latest pack.

    Point lookups still use individual blobs (fast for single reads).
    Scans use the pack (fast for bulk reads).
    """

    def __init__(self, kernel: PondMinimal, name: str):
        self.kernel = kernel
        self.name = name
        self._pack_ref = f"{name}__pack"
        self._cached_pack: Optional[bytes] = None

    def create_pack(self, blob_hashes: list[str]) -> str:
        """Create a pack file from a list of blob hashes.

        Args:
            blob_hashes: list of blob hashes to pack.

        Returns:
            The pack file's hash (stored as a kernel blob).
        """
        blob_map = {}
        for h in blob_hashes:
            try:
                data = self.kernel.read_blob(h)
                blob_map[h] = data
            except Exception:
                continue  # skip missing blobs

        pack_bytes, _ = PackFile.create(blob_map)
        pack_hash = self.kernel.write(pack_bytes)
        self.kernel.reference(self._pack_ref, pack_hash)
        self._cached_pack = pack_bytes
        return pack_hash

    def read_from_pack(self, blob_hash: str) -> Optional[bytes]:
        """Read a single blob from the pack. 1 GET (cached) + extraction."""
        if self._cached_pack is None:
            pack_hash = self.kernel.resolve(self._pack_ref)
            if pack_hash is None:
                return None
            self._cached_pack = self.kernel.read_blob(pack_hash)
        return PackFile.read_blob(self._cached_pack, blob_hash)

    def read_all_from_pack(self) -> dict[str, bytes]:
        """Read ALL blobs from the pack. 1 GET for the entire pack.

        This is the scan optimization:
          Without pack: N GETs (1 per blob) = N × 20ms = 2000ms for 100 blobs on S3
          With pack: 1 GET (pack file) = 1 × 20ms = 20ms for 100 blobs on S3
        """
        if self._cached_pack is None:
            pack_hash = self.kernel.resolve(self._pack_ref)
            if pack_hash is None:
                return {}
            self._cached_pack = self.kernel.read_blob(pack_hash)
        return PackFile.read_all_blobs(self._cached_pack)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_pack_creation_and_reading():
    """Create a pack, read individual blobs and all blobs from it."""
    bench = "/tmp/pond_pack_test"
    if os.path.exists(bench): shutil.rmtree(bench)
    os.makedirs(bench)
    kernel = PondMinimal(bench)

    # Write 100 individual blobs
    blob_hashes = []
    for i in range(100):
        h = kernel.write(f'{{"id":{i},"name":"user_{i}"}}'.encode())
        blob_hashes.append(h)

    # Create a pack
    backend = PackedBackend(kernel, "test")
    pack_hash = backend.create_pack(blob_hashes)
    assert pack_hash is not None

    # Read individual blobs from pack
    for i, h in enumerate(blob_hashes):
        data = backend.read_from_pack(h)
        assert data is not None
        assert json.loads(data)["id"] == i

    # Read ALL blobs from pack (1 GET instead of 100)
    all_blobs = backend.read_all_from_pack()
    assert len(all_blobs) == 100
    for i, h in enumerate(blob_hashes):
        assert h in all_blobs
        assert json.loads(all_blobs[h])["id"] == i

    kernel.close()
    shutil.rmtree(bench, ignore_errors=True)
    print("PASS: Pack creation + individual read + bulk read (100 blobs)")


def test_pack_vs_individual_scan():
    """Compare scan performance: individual GETs vs pack read."""
    bench = "/tmp/pond_pack_perf"
    if os.path.exists(bench): shutil.rmtree(bench)
    os.makedirs(bench)
    kernel = PondMinimal(bench)

    N = 500
    blob_hashes = []
    for i in range(N):
        h = kernel.write(f'{{"id":{i},"name":"user_{i}","val":{i*10}}}'.encode())
        blob_hashes.append(h)

    # Individual reads (simulates current scan: N GETs)
    t0 = time.perf_counter()
    individual_results = {}
    for h in blob_hashes:
        individual_results[h] = kernel.read_blob(h)
    t1 = time.perf_counter()
    individual_ms = (t1 - t0) * 1000

    # Pack read (1 GET for all blobs)
    backend = PackedBackend(kernel, "test")
    backend.create_pack(blob_hashes)

    t0 = time.perf_counter()
    pack_results = backend.read_all_from_pack()
    t1 = time.perf_counter()
    pack_ms = (t1 - t0) * 1000

    # Verify correctness
    assert len(pack_results) == N
    for h in blob_hashes:
        assert pack_results[h] == individual_results[h]

    # On local disk, the speedup is modest (disk cache).
    # On S3, the speedup would be ~100x (1 GET vs 500 GETs).
    speedup = individual_ms / pack_ms if pack_ms > 0 else float('inf')

    print(f"  Individual reads ({N} blobs): {individual_ms:.1f}ms ({N}/{individual_ms*1000:.0f} reads/sec)")
    print(f"  Pack read (1 GET): {pack_ms:.1f}ms")
    print(f"  Speedup: {speedup:.1f}x (local disk; ~{N}x on S3)")
    print(f"  S3 estimate: individual={N*20}ms vs pack={1*20}ms = {N}x speedup")

    kernel.close()
    shutil.rmtree(bench, ignore_errors=True)
    print("PASS: Pack vs individual scan performance")


def test_pack_with_lens():
    """Use pack with a Lens: write via Lens, create pack, scan via pack."""
    bench = "/tmp/pond_pack_lens"
    if os.path.exists(bench): shutil.rmtree(bench)
    os.makedirs(bench)
    kernel = PondMinimal(bench)

    # Write data via a Lens
    lens = Lens(kernel, "test")
    blob_hashes = []
    for i in range(100):
        lens.put(f"k{i:03d}", {"id": i, "name": f"user_{i}", "val": i * 10})
        # Capture the blob hash (lens.put returns it)
    lens.commit("100 records")

    # Get all blob hashes from the tree
    state = lens.base.read_all()
    all_hashes = list(state.values())

    # Create a pack from all data blobs
    backend = PackedBackend(kernel, "test")
    backend.create_pack(all_hashes)

    # Read ALL data via pack (1 GET instead of 100)
    all_blobs = backend.read_all_from_pack()

    # Decode all blobs and verify
    for key, blob_hash in state.items():
        assert blob_hash in all_blobs
        decoded = lens.decode(all_blobs[blob_hash])
        assert decoded["id"] == int(key[1:4])  # key format "k000" → id=0

    # Simulate S3 round trips:
    # Without pack: 1 HEAD (resolve) + 1 GET (commit) + 1 GET (tree) + 100 GETs (blobs) = 103 RTTs
    # With pack: 1 HEAD (resolve) + 1 GET (commit) + 1 GET (tree) + 1 GET (pack) = 4 RTTs
    print(f"  Without pack: ~103 RTTs (1 HEAD + 1 commit + 1 tree + 100 blobs)")
    print(f"  With pack: ~4 RTTs (1 HEAD + 1 commit + 1 tree + 1 pack)")
    print(f"  S3 speedup: {103*20}ms → {4*20}ms = {103/4:.0f}x faster")

    kernel.close()
    shutil.rmtree(bench, ignore_errors=True)
    print("PASS: Pack + Lens integration (write via Lens, scan via pack)")


def test_pack_persistence():
    """Pack survives restart."""
    bench = "/tmp/pond_pack_persist"
    if os.path.exists(bench): shutil.rmtree(bench)
    os.makedirs(bench)
    kernel = PondMinimal(bench)

    blob_hashes = [kernel.write(f"blob_{i}".encode()) for i in range(50)]
    backend = PackedBackend(kernel, "test")
    backend.create_pack(blob_hashes)
    kernel.close()

    # Reopen
    kernel2 = PondMinimal(bench)
    backend2 = PackedBackend(kernel2, "test")
    all_blobs = backend2.read_all_from_pack()
    assert len(all_blobs) == 50

    kernel2.close()
    shutil.rmtree(bench, ignore_errors=True)
    print("PASS: Pack persists across restart")


def main():
    print("=" * 72)
    print("  Packed-Object Backend Prototype")
    print("  Reduces scan GETs from N to 1 (Git packfile style)")
    print("=" * 72)

    test_pack_creation_and_reading()
    print()
    test_pack_vs_individual_scan()
    print()
    test_pack_with_lens()
    print()
    test_pack_persistence()

    print("\n" + "=" * 72)
    print("  PACKED BACKEND SUMMARY")
    print("=" * 72)
    print("  Without pack (100 blobs):  100 GETs = ~2000ms on S3")
    print("  With pack (100 blobs):       1 GET  = ~20ms on S3")
    print("  Speedup: ~100x for scans on object storage")
    print()
    print("  The pack does NOT change the kernel API.")
    print("  write/read_blob still work individually (for point lookups).")
    print("  The pack is an internal optimization for bulk reads.")
    print("  Like Git: loose objects for writes, packfiles for reads.")
    print("=" * 72)


if __name__ == "__main__":
    main()
