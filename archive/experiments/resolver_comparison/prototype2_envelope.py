#!/usr/bin/env python3
"""
Prototype 2: Minimal Envelope (current TypedBlob approach)

Each blob is wrapped in a 5-byte envelope:
  [1 byte: codec_id][4 bytes: payload_len][payload]

The codec_id tells a global CodecRegistry which decoder to use.

This is the existing TypedBlob approach, kept for comparison.
"""

from __future__ import annotations
import os, sys, json, shutil, struct
from typing import Optional, Any, Callable

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, os.path.join(REPO, "pond-core"))
sys.path.insert(0, os.path.join(REPO, "pond-sdk"))

from pond_minimal import PondMinimal
from lens_sdk import Lens


# Codec IDs
CODEC_JSON = 1
CODEC_GIT_TREE = 2


class EnvelopeRegistry:
    _codecs = {}
    @classmethod
    def register(cls, cid, enc, dec): cls._codecs[cid] = (enc, dec)
    @classmethod
    def get(cls, cid): return cls._codecs.get(cid)

    @classmethod
    def encode(cls, cid, data):
        codec = cls._codecs[cid]
        payload = codec[0](data)
        return struct.pack("<BI", cid, len(payload)) + payload

    @classmethod
    def decode(cls, raw):
        cid, plen = struct.unpack("<BI", raw[:5])
        payload = raw[5:5+plen]
        codec = cls._codecs.get(cid)
        if codec:
            try: return (True, codec[1](payload))
            except: return (False, payload)
        return (False, payload)


# Register codecs
EnvelopeRegistry.register(CODEC_JSON,
    lambda d: json.dumps(d, sort_keys=True).encode(),
    lambda b: json.loads(b))
EnvelopeRegistry.register(CODEC_GIT_TREE,
    lambda d: "\n".join(f"100644 blob {h}\t{n}" for n,h in sorted(d.items())).encode(),
    lambda b: dict(
        (line.split("\t",1)[1].strip(), line.split()[2])
        for line in b.decode().split("\n") if "\t" in line and len(line.split()) >= 3
    ))


class EnvelopeLens(Lens):
    """A Lens that wraps blobs in a 5-byte envelope."""

    def __init__(self, kernel, name, codec_id):
        super().__init__(kernel, name)
        self._codec_id = codec_id

    def encode(self, data):
        return EnvelopeRegistry.encode(self._codec_id, data)

    def decode(self, raw):
        success, value = EnvelopeRegistry.decode(raw)
        return value if success else value  # decoded or raw


def test_envelope():
    bench = "/tmp/pond_envelope"
    if os.path.exists(bench): shutil.rmtree(bench)
    os.makedirs(bench)
    kernel = PondMinimal(bench)

    sql = EnvelopeLens(kernel, "workspace", CODEC_JSON)
    git = EnvelopeLens(kernel, "workspace", CODEC_GIT_TREE)

    sql.put("user:1", {"name": "Alice", "age": 30})
    sql.commit("SQL write")
    git.put("tree:main", {"README.md": "abc123"})
    git.commit("Git write")

    # Any lens reads any blob
    assert sql.get("user:1") == {"name": "Alice", "age": 30}
    assert git.get("user:1") == {"name": "Alice", "age": 30}  # Git reads SQL
    assert sql.get("tree:main") == {"README.md": "abc123"}    # SQL reads Git
    assert git.get("tree:main") == {"README.md": "abc123"}

    # The blobs have the envelope (5 bytes overhead)
    raw_sql = sql.get_raw("user:1")
    cid, plen = struct.unpack("<BI", raw_sql[:5])
    assert cid == CODEC_JSON  # envelope byte
    assert raw_sql[5:] == json.dumps({"name": "Alice", "age": 30}, sort_keys=True).encode()

    kernel.close()
    shutil.rmtree(bench, ignore_errors=True)
    print("PASS: Envelope approach")
    print("      - 5-byte envelope per blob (codec_id + payload_len)")
    print("      - Any lens reads any blob (registry decodes via codec_id)")
    print("      - Blobs carry type metadata (codec_id byte)")
    print("      - Kernel stores 'typed bytes' (envelope + payload)")


if __name__ == "__main__":
    test_envelope()
