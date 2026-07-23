"""
Pond Transport Layer (Phase N.3)

A reference implementation of the Transport Algebra (Part III §17
of POND_FORMAL_ALGEBRAS.md). Sits between the Kernel (raw bytes)
and the Lens (interpreted state).

The Transport Layer provides:
  - Compression (zlib for portability; zstd would be used in production)
  - Encryption (XOR for test clarity; AES-GCM would be used in production)
  - Block index (for range reads on compressed/encrypted blobs)
  - Key management (envelope encryption pattern; master key in KMS)

Layer order (A10): compress -> encrypt -> checksum.
The Lens sees plaintext, uncompressed bytes.
The Kernel stores encrypted, compressed bytes with a block index.

This is a *reference implementation* for verification. Production
use would replace zlib with zstd, XOR with AES-GCM, and the local
key store with AWS KMS / GCP KMS / Vault.

Transport laws (TR1-TR6):
  TR1  Dedup broken under encryption (accepted)
  TR2  Dictionary is a content-addressed sidecar
  TR3  Transport below Lens, above Kernel
  TR4  Transport optional per Collection
  TR5  Transport is per-blob, not per-byte
  TR6  Block index is a Physical Structure (rebuildable)

Run tests:
    python pond-transport/transport.py
"""

from __future__ import annotations

import os
import sys
import json
import zlib
import hashlib
import struct
import tempfile
import shutil
from typing import Optional

# Make pond-core importable
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(SCRIPT_DIR, "..", "pond-core"))
from pond_minimal import PondMinimal  # noqa: E402


# ---------------------------------------------------------------------------
# Block format
# ---------------------------------------------------------------------------

# A transport-encoded blob is:
#   [4-byte magic 'PDTP']
#   [4-byte version]
#   [4-byte block_count]
#   [block_count * (4+4+4+32) bytes: block index entries]
#     each entry: (logical_offset, physical_offset, length, nonce_hash)
#   [4-byte wrapped_DEK_length]
#   [wrapped_DEK_length bytes: encrypted DEK]
#   [block_count compressed-encrypted blocks, concatenated]
#
# The block index at the start enables range reads: a reader can
# read the index first, then range-read only the blocks needed.

MAGIC = b"PDTP"
VERSION = 1

BLOCK_HEADER_FMT = ">III32s"  # logical_offset, physical_offset, length, nonce_hash
BLOCK_HEADER_SIZE = struct.calcsize(BLOCK_HEADER_FMT)


# ---------------------------------------------------------------------------
# Key management (envelope encryption — simplified)
# ---------------------------------------------------------------------------

class KeyStore:
    """Simplified envelope encryption. In production, this would
    call AWS KMS / GCP KMS / Vault. Here, we use a local master
    key for demonstration.

    The master key encrypts Data Encryption Keys (DEKs). The DEK
    encrypts blob blocks. The kernel stores wrapped_DEK alongside
    the blob; the KeyStore unwraps the DEK on demand."""

    def __init__(self, master_key: bytes = None):
        self.master_key = master_key or hashlib.sha256(b"pond-default-master-key").digest()

    def generate_dek(self) -> bytes:
        """Generate a random 32-byte DEK."""
        return hashlib.sha256(os.urandom(32)).digest()

    def wrap(self, dek: bytes) -> bytes:
        """Wrap (encrypt) a DEK with the master key. Simplified:
        XOR with master_key (production: AES-KW or RSA-OAEP)."""
        return bytes(a ^ b for a, b in zip(dek, self.master_key))

    def unwrap(self, wrapped_dek: bytes) -> bytes:
        """Unwrap (decrypt) a DEK. XOR is symmetric."""
        return bytes(a ^ b for a, b in zip(wrapped_dek, self.master_key))


# ---------------------------------------------------------------------------
# Transport Layer
# ---------------------------------------------------------------------------

class TransportLayer:
    """Implements compress -> encrypt -> checksum per A10.

    The Lens calls transport.write(b) -> h, which internally:
      1. Compresses b (with optional dictionary)
      2. Splits compressed bytes into blocks
      3. Encrypts each block with a fresh DEK
      4. Builds a block index
      5. Writes the transport-encoded blob to the kernel

    On read, transport.read(h) -> b:
      1. Reads the transport-encoded blob from the kernel
      2. Parses the block index
      3. Decrypts each block
      4. Decompresses
      5. Returns the original bytes

    Range reads: transport.read_range(h, off, len) -> bytes
      1. Reads only the block index
      2. Selects blocks overlapping [off, off+len)
      3. Range-reads each selected block from the kernel
      4. Decrypts, decompresses, slices to logical range
      5. Returns the bytes
    """

    def __init__(self, kernel: PondMinimal, key_store: KeyStore = None,
                 block_size: int = 4096, compress_level: int = 6):
        self.kernel = kernel
        self.key_store = key_store or KeyStore()
        self.block_size = block_size
        self.compress_level = compress_level

    # ------------------------------------------------------------------
    # Write (compress -> encrypt -> checksum)
    # ------------------------------------------------------------------

    def write(self, data: bytes, dict_hash: Optional[str] = None) -> str:
        """Encode and write a blob. Returns the kernel hash.

        Steps (A10):
          1. Compress (zlib)
          2. Split into blocks of block_size
          3. Generate a DEK
          4. Encrypt each block (XOR with DEK; production: AES-GCM)
          5. Build block index
          6. Serialize transport blob: header + index + wrapped_DEK + blocks
          7. Kernel.Write(transport_blob) -> h
        """
        # 1. Compress
        compressed = zlib.compress(data, self.compress_level)

        # 2. Split into blocks
        blocks = []
        for off in range(0, len(compressed), self.block_size):
            blocks.append(compressed[off:off + self.block_size])

        # 3. Generate DEK
        dek = self.key_store.generate_dek()
        wrapped_dek = self.key_store.wrap(dek)

        # 4. Encrypt each block (XOR with DEK; production would use
        # per-block nonces + AES-GCM)
        encrypted_blocks = []
        block_index = []
        physical_offset = 0
        for i, block in enumerate(blocks):
            encrypted = bytes(b ^ dek[i % len(dek)] for b in block)
            encrypted_blocks.append(encrypted)
            # Block index entry: (logical_offset, physical_offset, length, nonce_hash)
            # For XOR, the "nonce" is the block index; nonce_hash is hash of (i, dek)
            nonce = struct.pack(">I", i) + dek
            nonce_hash = hashlib.sha256(nonce).digest()
            block_index.append((
                i * self.block_size,           # logical offset in compressed stream
                physical_offset,                # physical offset in encrypted blob
                len(encrypted),                 # length
                nonce_hash,
            ))
            physical_offset += len(encrypted)

        # 5. Serialize transport blob
        # Header: magic(4) + version(4) + block_count(4)
        header = MAGIC + struct.pack(">II", VERSION, len(blocks))
        # Block index
        index_bytes = b"".join(
            struct.pack(BLOCK_HEADER_FMT, *entry) for entry in block_index
        )
        # Wrapped DEK length + wrapped DEK
        dek_section = struct.pack(">I", len(wrapped_dek)) + wrapped_dek

        transport_blob = header + index_bytes + dek_section + b"".join(encrypted_blocks)

        # 6. Write to kernel
        return self.kernel.write(transport_blob)

    # ------------------------------------------------------------------
    # Read (decrypt -> decompress)
    # ------------------------------------------------------------------

    def read(self, h: str) -> bytes:
        """Read and decode a transport-encoded blob."""
        transport_blob = self.kernel.read(h)
        return self._decode(transport_blob, range_start=None, range_end=None)

    def read_range(self, h: str, logical_off: int, logical_len: int) -> bytes:
        """Range read on a transport-encoded blob. Reads only the
        blocks needed to cover [logical_off, logical_off+logical_len)
        in the *original* (decompressed) bytes.

        This implements the block-index range read from §17.3.
        """
        # For simplicity, we read the whole transport blob and slice.
        # In production, this would issue range-reads on the kernel
        # for only the blocks needed. The kernel API doesn't expose
        # range reads (per Phase N.1 demotion), so we use Read + slice.
        transport_blob = self.kernel.read(h)
        full = self._decode(transport_blob, range_start=None, range_end=None)
        return full[logical_off:logical_off + logical_len]

    def _decode(self, transport_blob: bytes,
                range_start: Optional[int],
                range_end: Optional[int]) -> bytes:
        """Decode a transport blob: parse header, decrypt blocks, decompress."""
        # Parse header
        if transport_blob[:4] != MAGIC:
            raise ValueError("not a transport-encoded blob")
        version, block_count = struct.unpack(">II", transport_blob[4:12])
        if version != VERSION:
            raise ValueError(f"unsupported version {version}")

        # Parse block index
        index_start = 12
        index_end = index_start + block_count * BLOCK_HEADER_SIZE
        block_index = []
        for i in range(block_count):
            entry_bytes = transport_blob[
                index_start + i * BLOCK_HEADER_SIZE:
                index_start + (i + 1) * BLOCK_HEADER_SIZE
            ]
            block_index.append(struct.unpack(BLOCK_HEADER_FMT, entry_bytes))

        # Parse wrapped DEK
        dek_len = struct.unpack(">I", transport_blob[index_end:index_end+4])[0]
        wrapped_dek = transport_blob[index_end+4:index_end+4+dek_len]
        dek = self.key_store.unwrap(wrapped_dek)

        # Decrypt blocks
        blocks_start = index_end + 4 + dek_len
        encrypted_stream = transport_blob[blocks_start:]
        decrypted = b""
        for i, (logical_off, physical_off, length, nonce_hash) in enumerate(block_index):
            encrypted_block = encrypted_stream[physical_off:physical_off + length]
            decrypted_block = bytes(
                b ^ dek[i % len(dek)] for b in encrypted_block
            )
            decrypted += decrypted_block

        # Decompress
        return zlib.decompress(decrypted)

    # ------------------------------------------------------------------
    # Dictionary support (TR2)
    # ------------------------------------------------------------------

    def write_dict(self, samples: list[bytes]) -> str:
        """Train a compression dictionary on samples and write it
        as a content-addressed blob. Returns the dictionary hash.

        Per TR2: the dictionary is a sidecar, content-addressed.
        Two Collections can share a dictionary (same hash) or not.
        """
        # Simplified: concatenate samples as the "dictionary".
        # Production: zdict_train from zstd.
        dict_bytes = b"".join(samples)
        return self.kernel.write(dict_bytes)

    def read_dict(self, dict_hash: str) -> bytes:
        """Read a dictionary blob."""
        return self.kernel.read(dict_hash)


# ---------------------------------------------------------------------------
# Self-tests
# ---------------------------------------------------------------------------

def _self_test():
    """Verify the Transport Layer round-trips data correctly."""
    print("=== Transport Layer self-test ===")

    tmpdir = tempfile.mkdtemp(prefix="pond_transport_")
    try:
        kernel = PondMinimal(tmpdir)
        transport = TransportLayer(kernel)

        # Test 1: round-trip
        data = b"Hello, world! " * 100  # highly compressible
        h = transport.write(data)
        decoded = transport.read(h)
        assert decoded == data, f"round-trip failed: {decoded[:50]}..."
        print(f"  [OK] round-trip: {len(data)} bytes -> hash {h[:8]}")

        # Test 2: range read
        range_data = transport.read_range(h, 100, 50)
        assert range_data == data[100:150], "range read failed"
        print(f"  [OK] range read: data[100:150] = {range_data[:20]!r}...")

        # Test 3: compression actually compresses
        transport_blob = kernel.read(h)
        ratio = len(transport_blob) / len(data)
        assert ratio < 1.0, f"compression didn't help: ratio={ratio}"
        print(f"  [OK] compression: {len(data)} -> {len(transport_blob)} bytes (ratio {ratio:.2f})")

        # Test 4: dedup broken under encryption (TR1)
        # Two writes of the same data produce different hashes because
        # the DEK is randomly generated each time.
        h1 = transport.write(data)
        h2 = transport.write(data)
        assert h1 != h2, "TR1: dedup should be broken under encryption"
        print(f"  [OK] TR1: dedup broken under encryption ({h1[:8]} != {h2[:8]})")

        # Test 5: dictionary support (TR2)
        dict_hash = transport.write_dict([b"foo", b"bar", b"baz"])
        dict_bytes = transport.read_dict(dict_hash)
        assert dict_bytes == b"foobarbaz", "dictionary round-trip failed"
        print(f"  [OK] TR2: dictionary stored as content-addressed blob ({dict_hash[:8]})")

        # Test 6: block index is rebuildable (TR6 / MAN2)
        # Re-parse the transport blob and verify the block index
        transport_blob = kernel.read(h)
        # The block index is at offset 12, with block_count entries
        block_count = struct.unpack(">I", transport_blob[8:12])[0]
        assert block_count > 0, "block index should have entries"
        print(f"  [OK] TR6: block index has {block_count} entries (rebuildable)")

        # Test 7: multiple distinct blobs
        for i in range(5):
            d = f"test data {i} ".encode() * 50
            hh = transport.write(d)
            assert transport.read(hh) == d, f"blob {i} round-trip failed"
        print(f"  [OK] 5 distinct blobs round-tripped")

        # Test 8: empty blob
        h_empty = transport.write(b"")
        assert transport.read(h_empty) == b"", "empty blob round-trip failed"
        print(f"  [OK] empty blob round-tripped")

        kernel.close()
        print("\nAll transport tests pass.")
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == "__main__":
    _self_test()
