"""
TypedBlob — DEPRECATED. NOT part of Pond's architecture.

⚠ STATUS: DEPRECATED. The falsification round (Task 26) proved that
context-based interpretation provides all 8 capabilities (universal
readability, bidirectional write/read, branch/merge/history, derived
structures, zero metadata, pure bytes, transform-later, kernel purity)
WITHOUT any blob-level envelope. See:
  - RFC-0012 (Accepted): context-based interpretation is the chosen approach
  - RFC-0013: the formal Lens Interpretation Contract
  - experiments/resolver_comparison/falsification_context.py: the proof
  - docs/LENS_INTERPRETATION_CONTRACT.md: the one-page contract

This module is kept as an experimental artifact for reference only.
Do NOT use it in production. It will be removed in a future cleanup.

The kernel stores pure bytes. The interpretation layer lives in CODE
(the resolver), not in DATA (the blob).
"""

from __future__ import annotations

import os
import sys
import struct
import json
from typing import Optional, Any, Callable, Union

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(REPO, "pond-core"))
sys.path.insert(0, HERE)

from kernel import PondMinimal
from keyvalue_lens import Lens, IndexedLens


# ---------------------------------------------------------------------------
# Codec IDs (registered globally)
# ---------------------------------------------------------------------------

# Codec IDs are 1-byte unsigned integers (0-255).
# 0 is reserved for "raw" (no encoding — payload IS the data).
# 1-99 are for built-in codecs.
# 100-255 are for user-defined codecs.
CODEC_RAW = 0
CODEC_JSON = 1
CODEC_GIT_TREE = 2
CODEC_NOTEBOOK = 3
CODEC_ARROW_IPC = 4
CODEC_CSV = 5


# ---------------------------------------------------------------------------
# CodecRegistry — maps codec_id → (encode, decode)
# ---------------------------------------------------------------------------

class CodecRegistry:
    """Registry of codecs for the TypedBlob envelope.

    Each codec has:
      - encode(data) -> bytes (the payload, without the envelope)
      - decode(payload: bytes) -> data

    The registry is global (module-level). Lenses register their
    codecs at import time. Any lens can read any blob because the
    registry knows how to decode all registered codecs.
    """

    _codecs: dict[int, tuple[Callable, Callable]] = {}
    _names: dict[str, int] = {}

    @classmethod
    def register(cls, codec_id: int, name: str,
                 encode: Callable[[Any], bytes],
                 decode: Callable[[bytes], Any]) -> None:
        """Register a codec."""
        cls._codecs[codec_id] = (encode, decode)
        cls._names[name] = codec_id

    @classmethod
    def get_codec(cls, codec_id: int) -> Optional[tuple[Callable, Callable]]:
        """Get the (encode, decode) pair for a codec_id. None if not registered."""
        return cls._codecs.get(codec_id)

    @classmethod
    def get_id(cls, name: str) -> Optional[int]:
        """Get the codec_id for a codec name. None if not registered."""
        return cls._names.get(name)

    @classmethod
    def decode_with_codec(cls, payload: bytes,
                           codec_id: int) -> tuple[bool, Any]:
        """Decode a payload using the registered codec.

        Returns (success, decoded_value):
          - If codec_id is registered: (True, decoded_value)
          - If codec_id is not registered: (False, payload) — caller
            gets the raw payload and can transform later.
        """
        codec = cls._codecs.get(codec_id)
        if codec is None:
            return (False, payload)
        try:
            return (True, codec[1](payload))
        except Exception:
            # Decode failed (wrong codec for this payload). Return
            # raw payload so the caller can transform later.
            return (False, payload)


def _git_tree_encode(d):
    """Encode a tree dict as Git tree format bytes."""
    lines = [f"100644 blob {h}\t{n}" for n, h in sorted(d.items())]
    return "\n".join(lines).encode()


def _git_tree_decode(b):
    """Decode Git tree format bytes back to a tree dict."""
    result = {}
    for line in b.decode().split("\n"):
        line = line.strip()
        if not line or "\t" not in line:
            continue
        meta, filename = line.split("\t", 1)
        parts = meta.split()
        if len(parts) >= 3:
            result[filename] = parts[2]
    return result


# Register built-in codecs at import time.
CodecRegistry.register(CODEC_RAW, "raw",
                        encode=lambda d: d if isinstance(d, bytes) else str(d).encode(),
                        decode=lambda b: b)
CodecRegistry.register(CODEC_JSON, "json",
                        encode=lambda d: json.dumps(d, sort_keys=True).encode(),
                        decode=lambda b: json.loads(b))
CodecRegistry.register(CODEC_GIT_TREE, "git_tree",
                        encode=_git_tree_encode,
                        decode=_git_tree_decode)
CodecRegistry.register(CODEC_NOTEBOOK, "notebook",
                        encode=lambda d: json.dumps(d, sort_keys=True).encode(),
                        decode=lambda b: json.loads(b))


def _csv_encode(d):
    return ",".join(f"{k}={v}" for k, v in sorted(d.items())).encode()


def _csv_decode(b):
    result = {}
    for line in b.decode().split(","):
        if "=" in line:
            k, v = line.split("=", 1)
            result[k] = v
    return result


CodecRegistry.register(CODEC_CSV, "csv",
                        encode=_csv_encode,
                        decode=_csv_decode)


# ---------------------------------------------------------------------------
# TypedBlob — the envelope
# ---------------------------------------------------------------------------

class TypedBlob:
    """A typed envelope around raw bytes.

    Envelope format:
      [1 byte: codec_id]
      [4 bytes: payload_len (uint32 LE)]
      [payload_len bytes: payload]

    The kernel stores this as raw bytes. The kernel does NOT interpret
    the envelope. The TypedBlob class provides encode/decode for the
    envelope.

    Overhead: 5 bytes per blob.
    """

    @staticmethod
    def encode(codec_id: int, data: Any) -> bytes:
        """Encode data with a codec, wrap in the typed envelope.

        Args:
            codec_id: the codec to use (from CodecRegistry).
            data: the data to encode.

        Returns:
            The envelope bytes (codec_id + payload_len + payload).
        """
        codec = CodecRegistry.get_codec(codec_id)
        if codec is None:
            raise ValueError(f"Unknown codec_id: {codec_id}")
        payload = codec[0](data)
        return struct.pack("<BI", codec_id, len(payload)) + payload

    @staticmethod
    def decode(envelope: bytes) -> tuple[int, bytes]:
        """Decode the envelope, returning (codec_id, payload_bytes).

        Does NOT decode the payload — just extracts it. The caller
        can then use CodecRegistry.decode_with_codec to decode, or
        use the raw payload directly.
        """
        if len(envelope) < 5:
            raise ValueError(f"Envelope too short: {len(envelope)} bytes")
        codec_id, payload_len = struct.unpack("<BI", envelope[:5])
        payload = envelope[5:5 + payload_len]
        return (codec_id, payload)

    @staticmethod
    def decode_value(envelope: bytes) -> tuple[int, bool, Any]:
        """Decode the envelope AND the payload.

        Returns (codec_id, success, decoded_value):
          - If the codec is registered and decode succeeds:
            (codec_id, True, decoded_value)
          - If the codec is not registered or decode fails:
            (codec_id, False, raw_payload_bytes)
        """
        codec_id, payload = TypedBlob.decode(envelope)
        success, value = CodecRegistry.decode_with_codec(payload, codec_id)
        return (codec_id, success, value)


# ---------------------------------------------------------------------------
# TypedLens — any lens can read any blob
# ---------------------------------------------------------------------------

class TypedLens(Lens):
    """A Lens that uses the TypedBlob envelope.

    Writing: encodes data via the lens's codec, wraps in the envelope.
    Reading: unwraps the envelope, decodes via the registered codec.
             If the codec doesn't match (or isn't registered), returns
             the raw payload bytes — so the caller can transform later.

    This means: ANY TypedLens can read ANY blob in the shared byte
    graph. It might not decode natively, but it always gets something
    (the raw payload).

    Bidirectionality:
      - Any lens can write (via its codec).
      - Any lens can read (native decode or raw payload).
      - Any lens can branch/checkout/merge (shared DAG).
    """

    def __init__(self, kernel: PondMinimal, name: str, codec_id: int):
        """Construct a TypedLens.

        Args:
            kernel: the Pond kernel.
            name: the lens name (shared byte graph).
            codec_id: the codec this lens uses for writing (from
                CodecRegistry). Reading always works for any codec.
        """
        super().__init__(kernel, name)
        self.codec_id = codec_id

    def encode(self, data: Any) -> bytes:
        """Encode data via this lens's codec, wrapped in the envelope."""
        return TypedBlob.encode(self.codec_id, data)

    def decode(self, data: bytes) -> Any:
        """Decode the envelope. If codec matches, decode natively.
        If not, return the raw payload (transform later).

        Returns:
            - If codec matches: the decoded value (dict, list, etc.)
            - If codec doesn't match: the raw payload bytes.
        """
        codec_id, success, value = TypedBlob.decode_value(data)
        if success:
            return value
        # Codec didn't match or decode failed. Return the raw payload
        # so the caller can transform it later.
        return value  # This is the raw payload bytes

    def get_typed(self, key: str) -> Optional[dict]:
        """Read a blob and return typed metadata + value.

        Returns a dict with:
          - "codec_id": the codec the blob was written with
          - "codec_name": the human-readable codec name (or "unknown")
          - "decoded": True if the codec matched and decoded, False otherwise
          - "value": the decoded value (if decoded) or raw payload bytes

        Returns None if the key doesn't exist.
        """
        raw = self.get_raw(key)
        if raw is None:
            return None
        codec_id, success, value = TypedBlob.decode_value(raw)
        codec_name = "unknown"
        for name, cid in CodecRegistry._names.items():
            if cid == codec_id:
                codec_name = name
                break
        return {
            "codec_id": codec_id,
            "codec_name": codec_name,
            "decoded": success,
            "value": value,
        }


# ---------------------------------------------------------------------------
# TypedIndex — cross-lens index
# ---------------------------------------------------------------------------

class TypedIndex(IndexedLens):
    """An index that works across lenses.

    The extractor receives DECODED payloads, regardless of which lens
    wrote the blob. The middle layer (TypedBlob) decodes based on the
    codec_id in the envelope.

    If a blob's codec isn't registered (or decode fails), the extractor
    receives the raw payload bytes. The extractor can handle this case
    (e.g., skip blobs it can't decode, or extract keys from raw bytes).

    This means: a SQL index can index Git blobs (if the Git codec is
    registered and the extractor knows what to extract). Cross-lens
    indexing works because the index doesn't belong to any single lens.
    """

    def __init__(self, kernel: PondMinimal, name: str,
                 extractor_codec_id: int):
        """Construct a TypedIndex.

        Args:
            kernel: the Pond kernel.
            name: the lens name (shared byte graph).
            extractor_codec_id: the codec the extractor expects. Blobs
                with this codec are decoded and passed to the extractor.
                Other blobs are passed as raw payload bytes.
        """
        super().__init__(kernel, name)
        self._extractor_codec_id = extractor_codec_id

    def encode(self, data: Any) -> bytes:
        return TypedBlob.encode(self._extractor_codec_id, data)

    def decode(self, data: bytes) -> Any:
        codec_id, success, value = TypedBlob.decode_value(data)
        if success:
            return value
        return value  # raw payload

    def _decode_for_index(self, raw: bytes) -> Any:
        """Decode a blob for the index extractor.

        Always returns something — either the decoded value (if codec
        matches) or the raw payload bytes. The extractor can handle both.
        """
        codec_id, success, value = TypedBlob.decode_value(raw)
        return value

    def build_cross_lens_index(self, index_name: str,
                                extractor: Callable[[Any], Union[str, list[str]]]) -> str:
        """Build an index across all blobs, regardless of which lens wrote them.

        The extractor receives the decoded payload (or raw bytes if the
        codec doesn't match). The extractor decides what to extract.

        Args:
            index_name: the index name.
            extractor: function(payload) -> str | list[str]. Receives
                the decoded value (if codec matched) or raw bytes.

        Returns:
            The index tree root hash.
        """
        state = self.base.read_all()
        index_entries = {}
        for pk, bh in state.items():
            if pk.startswith("_"):
                continue
            raw = self.kernel.read_blob(bh)
            payload = self._decode_for_index(raw)
            try:
                keys = extractor(payload)
                if isinstance(keys, str):
                    keys = [keys]
                elif keys is None:
                    continue
                for k in keys:
                    if k is not None:
                        index_entries[f"_index/{index_name}/{k}"] = bh
            except Exception:
                # Extractor failed for this blob (e.g., payload is raw
                # bytes and the extractor expected a dict). Skip it.
                continue

        from prolly_tree import ProllyTree
        tree_root = ProllyTree.build(self.kernel, index_entries)
        self.kernel.reference(f"{self.name}__index__{index_name}", tree_root)
        return tree_root

    def find_cross_lens(self, index_name: str, index_key: str) -> Optional[dict]:
        """Look up via a cross-lens index. Returns typed info about the blob."""
        from prolly_tree import ProllyTree
        from maintenance import resolve_active
        ref_name = f"{self.name}__index__{index_name}"
        tree_root = resolve_active(self.kernel, ref_name)
        if not tree_root:
            return None
        full_key = f"_index/{index_name}/{index_key}"
        bh = ProllyTree.lookup(self.kernel, tree_root, full_key)
        if not bh:
            return None
        raw = self.kernel.read_blob(bh)
        return self.get_typed(full_key.split("/")[-1]) if False else {
            "blob_hash": bh,
            "typed": TypedBlob.decode_value(raw),
        }
