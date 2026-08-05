"""PondPack — ONE blob containing commit + manifest.

THE PROBLEM:
  Every commit currently writes TWO blobs:
    1. A commit blob (JSON: parent, manifest_hash, message, timestamp, index)
    2. A manifest blob (PMAN: row groups + stats)
  And every cold read that needs both (merge, time-travel, branch read)
  does TWO GETs — one for the commit, one for the manifest.

THE SOLUTION:
  PondPack combines commit JSON + manifest bytes into ONE blob.
  The HEAD ref points to the pack hash. Reading the pack gives you
  both the commit metadata AND the manifest in ONE GET.

  This is a STORAGE-SIDE optimization (Layer 1, above the FROZEN kernel).
  It helps ALL workloads (KV, vector, streaming, lakehouse) because
  every workload uses the same commit + manifest infrastructure.
  It follows design principle 3.3 (Performant — optimizations live
  above the core) and 3.5 (Efficient — fewer round trips).

FORMAT:
  +----------------------------------+
  | Magic: "PNPK" (4 bytes)          |
  | Version: 1 (1 byte)              |
  | commit_json_len: 4 bytes (u32 LE)|
  | commit_json: variable (UTF-8)    |
  | manifest_len: 4 bytes (u32 LE)   |
  | manifest_bytes: variable (PMAN)  |
  +----------------------------------+

  The pack's hash = SHA-256(pack_bytes). Content-addressed, immutable.
  HEAD ref → pack_hash. manifest_ref → pack_hash (same blob).

BACKWARD COMPATIBILITY:
  Old collections have separate commit (JSON) + manifest (PMAN) blobs.
  The read path checks the magic bytes:
    "PNPK" → pack format, extract commit + manifest from one blob
    "{"    → old JSON commit, read manifest separately via manifest_ref
    "PMAN" → old standalone manifest

  New commits always use the pack format. Old commits are still readable.
  No migration needed — the format is self-describing.

ROUND TRIP SAVINGS:
  Cold point lookup: 3 GETs → 3 GETs (no change — already uses manifest_ref)
  Cold merge:        8 GETs → 4 GETs (halved — 2 packs instead of 4 blobs)
  Cold time-travel:  3 GETs → 2 GETs (pack replaces commit + manifest)
  Cold branch read:  3 GETs → 2 GETs (same as time-travel)
  Write:             4 PUTs → 3 PUTs (pack replaces commit + manifest blobs)
"""

from __future__ import annotations

import struct
import json
from typing import Optional, Any

_PACK_MAGIC = b"PNPK"
_PACK_VERSION = 1


def encode_pack(commit: dict, manifest_bytes: bytes) -> bytes:
    """Encode a commit dict + manifest bytes into ONE pack blob.

    Args:
        commit: the commit metadata dict (parent, second_parent, manifest_hash,
                message, timestamp, index). Serialized as JSON.
        manifest_bytes: the PMAN-encoded manifest bytes
                        (from CollectionManifest.encode()).

    Returns:
        The pack blob bytes. Content-addressed — SHA-256 of these bytes
        is the pack hash.
    """
    commit_json = json.dumps(commit, sort_keys=True).encode("utf-8")
    buf = bytearray()
    buf += _PACK_MAGIC
    buf += struct.pack("<B", _PACK_VERSION)
    buf += struct.pack("<I", len(commit_json))
    buf += commit_json
    buf += struct.pack("<I", len(manifest_bytes))
    buf += manifest_bytes
    return bytes(buf)


def is_pack(blob_bytes: bytes) -> bool:
    """Check if a blob is a PondPack blob (vs old JSON commit or PMAN manifest)."""
    return len(blob_bytes) >= 5 and blob_bytes[:4] == _PACK_MAGIC


def decode_pack(blob_bytes: bytes) -> tuple[dict, bytes]:
    """Decode a pack blob into (commit_dict, manifest_bytes).

    Args:
        blob_bytes: the pack blob bytes

    Returns:
        Tuple of (commit_dict, manifest_bytes).

    Raises:
        ValueError if the blob is not a valid pack.
    """
    if not is_pack(blob_bytes):
        raise ValueError(f"Not a PNPK blob (magic={blob_bytes[:4]!r})")
    version = blob_bytes[4]
    if version != _PACK_VERSION:
        raise ValueError(f"Unsupported PNPK version: {version}")
    pos = 5
    commit_json_len = struct.unpack("<I", blob_bytes[pos:pos+4])[0]
    pos += 4
    commit_json = blob_bytes[pos:pos+commit_json_len].decode("utf-8")
    pos += commit_json_len
    manifest_len = struct.unpack("<I", blob_bytes[pos:pos+4])[0]
    pos += 4
    manifest_bytes = blob_bytes[pos:pos+manifest_len]
    commit = json.loads(commit_json)
    return commit, manifest_bytes


def extract_commit(blob_bytes: bytes) -> Optional[dict]:
    """Extract the commit dict from a blob (pack or old JSON).

    Returns None if the blob is not a commit (e.g., it's a standalone manifest).
    """
    if is_pack(blob_bytes):
        commit, _ = decode_pack(blob_bytes)
        return commit
    # Old format: JSON commit
    if blob_bytes and blob_bytes[0:1] == b"{":
        try:
            return json.loads(blob_bytes)
        except (json.JSONDecodeError, UnicodeDecodeError):
            return None
    return None


def extract_manifest_bytes(blob_bytes: bytes) -> Optional[bytes]:
    """Extract the manifest bytes from a blob (pack or standalone PMAN).

    Returns None if the blob is not a manifest-containing blob.
    """
    if is_pack(blob_bytes):
        _, manifest_bytes = decode_pack(blob_bytes)
        return manifest_bytes
    # Old format: standalone PMAN manifest
    if len(blob_bytes) >= 4 and blob_bytes[:4] == b"PMAN":
        return blob_bytes
    return None
