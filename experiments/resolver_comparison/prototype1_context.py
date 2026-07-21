#!/usr/bin/env python3
"""
Prototype 1: Context-based Interpretation

NO metadata in blobs. NO envelope. The blob is pure bytes.

The interpretation comes from CONTEXT — specifically, the key prefix.
Like Git: Git knows whether it's requesting a blob, tree, commit, or
tag from the context (which command asked, which reference it
resolved). The object itself doesn't carry its type.

In this prototype:
  - Keys have a prefix: "sql/user:1", "git/tree:main", "nb/cell:1"
  - The Resolver looks at the prefix to determine which codec to use
  - The blob is pure payload (no envelope, no codec_id)
  - The kernel stores pure bytes

Trade-off: the key carries the "type" information. This is metadata,
but it's in the NAME (which the kernel already owns), not in the
BYTES. The kernel philosophy ("Bytes, History, Names") is preserved —
the Names now carry type info, but the Bytes stay pure.
"""

from __future__ import annotations
import os, sys, json, shutil
from typing import Optional, Any, Callable

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, os.path.join(REPO, "pond-core"))
sys.path.insert(0, os.path.join(REPO, "pond-sdk"))

from pond_minimal import PondMinimal
from view_sdk import Lens


# ---------------------------------------------------------------------------
# Codec functions (plain functions, no registry, no IDs)
# ---------------------------------------------------------------------------

def json_encode(d): return json.dumps(d, sort_keys=True).encode()
def json_decode(b): return json.loads(b)

def git_tree_encode(d):
    return "\n".join(f"100644 blob {h}\t{n}" for n, h in sorted(d.items())).encode()

def git_tree_decode(b):
    result = {}
    for line in b.decode().split("\n"):
        line = line.strip()
        if not line or "\t" not in line: continue
        meta, filename = line.split("\t", 1)
        parts = meta.split()
        if len(parts) >= 3:
            result[filename] = parts[2]
    return result


# ---------------------------------------------------------------------------
# Context-based Resolver: uses key prefix to determine codec
# ---------------------------------------------------------------------------

class ContextResolver:
    """Resolves bytes to objects using key-prefix context.

    The key prefix (e.g., "sql/", "git/", "nb/") determines which
    codec to use. The blob itself carries NO metadata.

    This is like Git: Git knows it's asking for a commit/tree/blob/tag
    from the context, not from the object itself.
    """

    def __init__(self):
        self._prefix_codecs: dict[str, tuple[Callable, Callable]] = {}

    def register(self, prefix: str, encode: Callable, decode: Callable):
        """Register a codec for a key prefix."""
        self._prefix_codecs[prefix] = (encode, decode)

    def encode_for_key(self, key: str, data: Any) -> bytes:
        """Encode data using the codec matching the key's prefix."""
        for prefix, (enc, _) in self._prefix_codecs.items():
            if key.startswith(prefix):
                return enc(data)
        # No prefix match — encode as raw bytes
        return data if isinstance(data, bytes) else str(data).encode()

    def decode_for_key(self, key: str, raw: bytes) -> Any:
        """Decode bytes using the codec matching the key's prefix.

        If no prefix matches, return raw bytes.
        """
        for prefix, (_, dec) in self._prefix_codecs.items():
            if key.startswith(prefix):
                try:
                    return dec(raw)
                except Exception:
                    return raw  # decode failed, return raw
        return raw  # no prefix match, return raw

    def get_codec_name_for_key(self, key: str) -> str:
        """Return the codec name for a key, or 'unknown'."""
        for prefix in self._prefix_codecs:
            if key.startswith(prefix):
                return prefix.rstrip("/")
        return "unknown"


# ---------------------------------------------------------------------------
# ContextLens: uses the resolver, but the blob is pure bytes
# ---------------------------------------------------------------------------

class ContextLens(Lens):
    """A Lens that uses context-based interpretation.

    The blob is pure bytes (no envelope). The resolver uses the key
    prefix to determine which codec to use.

    Any lens can read any blob because the resolver knows ALL prefix
    codecs. The key prefix tells the resolver what to do.
    """

    def __init__(self, kernel: PondMinimal, name: str,
                 resolver: ContextResolver, write_prefix: str):
        super().__init__(kernel, name)
        self._resolver = resolver
        self._write_prefix = write_prefix

    def encode(self, data: Any) -> bytes:
        """Encode using the resolver (context = write_prefix)."""
        # Use a dummy key with the write prefix to encode
        return self._resolver.encode_for_key(self._write_prefix + "dummy", data)

    def decode(self, data: bytes) -> Any:
        """Decode — but we need the KEY to know the context.

        Problem: Lens.decode() doesn't receive the key. We need to
        override get() to pass the key to the resolver.
        """
        # This is a fundamental issue: decode() doesn't know the key.
        # We'll override get() below to use the resolver directly.
        # If someone calls decode() directly, we can't decode (no context).
        return data  # raw bytes — caller should use get() instead

    def get(self, key: str) -> Optional[Any]:
        """Read and decode using the key as context."""
        h = self.base.lookup(key)
        if h is None:
            return None
        raw = self.kernel.read_blob(h)
        return self._resolver.decode_for_key(key, raw)

    def put(self, key: str, data: Any) -> str:
        """Write — the key must have the lens's prefix."""
        # Ensure the key has the write prefix (or add it)
        if not key.startswith(self._write_prefix):
            key = self._write_prefix + key
        raw = self._resolver.encode_for_key(key, data)
        blob_hash = self.kernel.write(raw)
        self.base.stage(key, blob_hash)
        return blob_hash


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------

def test_context_based():
    bench = "/tmp/pond_context_resolver"
    if os.path.exists(bench): shutil.rmtree(bench)
    os.makedirs(bench)
    kernel = PondMinimal(bench)

    # Set up the resolver with prefix → codec mappings
    resolver = ContextResolver()
    resolver.register("sql/", json_encode, json_decode)
    resolver.register("git/", git_tree_encode, git_tree_decode)
    resolver.register("nb/", json_encode, json_decode)  # notebook also uses JSON

    # Three lenses, same byte graph, different write prefixes
    sql = ContextLens(kernel, "workspace", resolver, "sql/")
    git = ContextLens(kernel, "workspace", resolver, "git/")
    notebook = ContextLens(kernel, "workspace", resolver, "nb/")

    # Each lens writes with its prefix
    sql.put("user:1", {"name": "Alice", "age": 30})  # key becomes "sql/user:1"
    sql.commit("SQL write")
    git.put("tree:main", {"README.md": "abc123"})  # key becomes "git/tree:main"
    git.commit("Git write")

    # Any lens can read any blob — the resolver uses the KEY prefix
    assert sql.get("sql/user:1") == {"name": "Alice", "age": 30}
    assert git.get("sql/user:1") == {"name": "Alice", "age": 30}  # Git reads SQL!
    assert sql.get("git/tree:main") == {"README.md": "abc123"}  # SQL reads Git!
    assert notebook.get("sql/user:1") == {"name": "Alice", "age": 30}
    assert notebook.get("git/tree:main") == {"README.md": "abc123"}

    # The blobs are PURE bytes (no envelope)
    raw_sql = sql.get_raw("sql/user:1")
    raw_git = git.get_raw("git/tree:main")
    assert raw_sql == json_encode({"name": "Alice", "age": 30})  # pure JSON
    assert b"100644 blob abc123" in raw_git  # pure Git tree format
    # NO envelope bytes at the start
    assert raw_sql[0:1] != b"\x01"  # not a codec_id byte

    # Verify: no metadata in kernel names
    names = kernel.list_names()
    assert not any("manifest" in n or "enable" in n for n in names)

    kernel.close()
    shutil.rmtree(bench, ignore_errors=True)
    print("PASS: Context-based interpretation")
    print("      - NO envelope, NO codec_id in blobs")
    print("      - Key prefix provides the context (like Git)")
    print("      - Any lens reads any blob (resolver uses key prefix)")
    print("      - Blobs are pure bytes (JSON, Git tree format)")
    print("      - Kernel stores pure bytes (no typed bytes)")


if __name__ == "__main__":
    test_context_based()
