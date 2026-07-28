"""
Compression layer for Pond encoded chunk blobs.

This is a LENS-LEVEL responsibility, not a kernel primitive. The kernel
stays FROZEN at 3 primitives (Write, Read, Ref). Compression is applied
as a post-encoding step: the encoding layer produces PND1 binary bytes,
then this layer compresses them before writing to the kernel.

DESIGN GOALS COMPLIANCE:
- Principle 1 (Simple): Kernel stays FROZEN. Compression is above the core.
- Principle 3 (Performant): Compression reduces I/O on object storage.
- Principle 8 (Storage-Independent): Compressed bytes are still PND1 —
  the format spec doesn't change. The compression wrapper is transparent
  to readers (they decompress before parsing PND1).

FORMAT:
  Compressed blobs have a 1-byte compression prefix before the PND1 data:
    0x00 = uncompressed (passthrough — no compression applied)
    0x01 = LZ4
    0x02 = Zstandard

  Readers check the first byte: if 0x00, read PND1 directly; otherwise
  decompress first, then parse PND1.

  This is TRANSPARENT to the encoding layer — encoders produce PND1 bytes,
  the compression layer wraps them. Decoders unwrap, then parse PND1.

GENERIC: works with ANY encoding (RAW, RLE, DICT, BITPACK) and ANY
workload (tabular, KV, vector, streaming, notebooks). The compression
layer doesn't know or care what's inside the PND1 bytes.
"""

from __future__ import annotations

import struct
from typing import Optional

# Compression type tags
COMPRESSION_NONE = 0x00
COMPRESSION_LZ4 = 0x01
COMPRESSION_ZSTD = 0x02

_COMPRESSOR_NAMES = {
    COMPRESSION_NONE: "none",
    COMPRESSION_LZ4: "lz4",
    COMPRESSION_ZSTD: "zstd",
}


def compress_blob(data: bytes, method: int = COMPRESSION_ZSTD) -> bytes:
    """Compress a PND1 blob with the specified method.

    Prepends a 1-byte compression tag. If compression makes the blob
    larger (small inputs), falls back to COMPRESSION_NONE.

    Args:
        data: the PND1 encoded bytes to compress
        method: COMPRESSION_ZSTD (default), COMPRESSION_LZ4, or COMPRESSION_NONE

    Returns:
        Compressed bytes with 1-byte prefix. Decompress with decompress_blob().
    """
    if method == COMPRESSION_NONE or len(data) < 64:
        # Too small to benefit from compression, or explicitly disabled
        return struct.pack("<B", COMPRESSION_NONE) + data

    try:
        if method == COMPRESSION_ZSTD:
            import zstandard as zstd
            compressed = zstd.compress(data)
        elif method == COMPRESSION_LZ4:
            import lz4.frame
            compressed = lz4.frame.compress(data)
        else:
            return struct.pack("<B", COMPRESSION_NONE) + data

        # Only use compressed version if it's actually smaller
        if len(compressed) + 1 < len(data):
            return struct.pack("<B", method) + compressed
        else:
            return struct.pack("<B", COMPRESSION_NONE) + data
    except ImportError:
        # Compression library not available — passthrough
        return struct.pack("<B", COMPRESSION_NONE) + data


def decompress_blob(data: bytes) -> bytes:
    """Decompress a blob that was compressed by compress_blob().

    Reads the 1-byte compression tag and decompresses accordingly.
    If tag is COMPRESSION_NONE, returns the data as-is (minus the tag byte).

    Args:
        data: compressed bytes with 1-byte prefix

    Returns:
        The original PND1 encoded bytes (decompressed).
    """
    if not data:
        return b""

    method = data[0]
    payload = data[1:]

    if method == COMPRESSION_NONE:
        return payload

    try:
        if method == COMPRESSION_ZSTD:
            import zstandard as zstd
            return zstd.decompress(payload)
        elif method == COMPRESSION_LZ4:
            import lz4.frame
            return lz4.frame.decompress(payload)
    except ImportError:
        pass

    # If we can't decompress, return the raw payload (best effort)
    return payload


def get_compression_name(data: bytes) -> str:
    """Get the compression method name from a compressed blob's prefix."""
    if not data:
        return "none"
    return _COMPRESSOR_NAMES.get(data[0], "unknown")


def get_compression_ratio(original_size: int, compressed_size: int) -> float:
    """Compute compression ratio (original / compressed)."""
    if compressed_size == 0:
        return 1.0
    return original_size / compressed_size
