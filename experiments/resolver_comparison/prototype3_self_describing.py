#!/usr/bin/env python3
"""
Prototype 3: Self-describing Payloads

NO envelope. NO key-prefix context. The payload format itself carries
enough information to be identified.

The Resolver SNIFFS the first few bytes to determine the format:
  - Starts with { or [  → JSON
  - Starts with "100644 blob" (or similar Git patterns) → Git tree
  - Starts with ARROW1 magic bytes → Arrow IPC
  - Otherwise → raw bytes

This is like Unix file(1): the file command sniffs magic bytes to
determine the format. The file doesn't carry a "type" header; the
format is self-identifying.

Trade-off: only works for formats that ARE self-describing. Custom
binary formats without magic bytes can't be sniffed. But JSON, Git,
Arrow, Parquet, CBOR — all are self-describing.
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


# ---------------------------------------------------------------------------
# Codec functions
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
# Self-describing Resolver: sniffs the payload to determine format
# ---------------------------------------------------------------------------

class SniffingResolver:
    """Resolves bytes to objects by sniffing the payload format.

    Like Unix `file(1): examines magic bytes / patterns to determine
    the format. NO envelope, NO key context. The payload is
    self-describing.

    Limitation: only works for formats with identifiable patterns.
    Raw bytes or custom formats without magic bytes can't be sniffed.
    """

    def __init__(self):
        self._sniffers: list[tuple[Callable[[bytes], bool], Callable, Callable]] = []

    def register(self, sniff: Callable[[bytes], bool],
                 encode: Callable, decode: Callable):
        """Register a codec with a sniffer function.

        Args:
            sniff: function(raw_bytes) -> bool. Returns True if the
                bytes match this format.
            encode: function(data) -> bytes.
            decode: function(bytes) -> data.
        """
        self._sniffers.append((sniff, encode, decode))

    def encode(self, data: Any, preferred_codec: int = 0) -> bytes:
        """Encode using the preferred codec."""
        if preferred_codec < len(self._sniffers):
            return self._sniffers[preferred_codec][1](data)
        return data if isinstance(data, bytes) else str(data).encode()

    def decode(self, raw: bytes) -> tuple[str, Any]:
        """Decode by sniffing the format.

        Returns (format_name, decoded_value). If no sniffer matches,
        returns ("raw", raw_bytes).
        """
        for i, (sniff, _, dec) in enumerate(self._sniffers):
            if sniff(raw):
                try:
                    name = f"codec_{i}"
                    return (name, dec(raw))
                except Exception:
                    pass  # sniffer matched but decode failed, try next
        return ("raw", raw)


# ---------------------------------------------------------------------------
# SniffingLens: uses the sniffing resolver, blobs are pure self-describing bytes
# ---------------------------------------------------------------------------

class SniffingLens(Lens):
    """A Lens that uses self-describing payloads + sniffing.

    The blob is pure payload (no envelope). The resolver sniffs the
    first few bytes to determine the format and decode.

    Any lens can read any blob because the resolver can sniff all
    registered formats.
    """

    def __init__(self, kernel: PondMinimal, name: str,
                 resolver: SniffingResolver, write_codec_idx: int = 0):
        super().__init__(kernel, name)
        self._resolver = resolver
        self._write_codec_idx = write_codec_idx

    def encode(self, data: Any) -> bytes:
        return self._resolver.encode(data, self._write_codec_idx)

    def decode(self, raw: bytes) -> Any:
        _, value = self._resolver.decode(raw)
        return value


# ---------------------------------------------------------------------------
# Sniffers
# ---------------------------------------------------------------------------

def is_json(raw: bytes) -> bool:
    """Sniff: does this look like JSON?"""
    if not raw: return False
    # JSON objects/arrays start with { or [
    # (after optional whitespace)
    stripped = raw.lstrip()
    return stripped[:1] in (b'{', b'[')

def is_git_tree(raw: bytes) -> bool:
    """Sniff: does this look like a Git tree object?"""
    if not raw: return False
    # Git tree format: "100644 blob <hash>\t<filename>"
    # or "040000 tree <hash>\t<dirname>"
    try:
        text = raw.decode()
        first_line = text.split("\n")[0]
        # Check for Git tree entry pattern
        parts = first_line.split()
        if len(parts) >= 2 and parts[0] in ("100644", "100755", "120000", "040000"):
            if parts[1] in ("blob", "tree") and "\t" in first_line:
                return True
    except: pass
    return False


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------

def test_self_describing():
    bench = "/tmp/pond_self_describing"
    if os.path.exists(bench): shutil.rmtree(bench)
    os.makedirs(bench)
    kernel = PondMinimal(bench)

    # Set up the sniffing resolver
    resolver = SniffingResolver()
    resolver.register(is_json, json_encode, json_decode)          # idx 0
    resolver.register(is_git_tree, git_tree_encode, git_tree_decode)  # idx 1

    # Two lenses, same byte graph, different write codecs
    sql = SniffingLens(kernel, "workspace", resolver, write_codec_idx=0)  # writes JSON
    git = SniffingLens(kernel, "workspace", resolver, write_codec_idx=1)  # writes Git trees

    sql.put("user:1", {"name": "Alice", "age": 30})
    sql.commit("SQL write")
    git.put("tree:main", {"README.md": "abc123"})
    git.commit("Git write")

    # Any lens reads any blob — the resolver sniffs the format
    assert sql.get("user:1") == {"name": "Alice", "age": 30}
    assert git.get("user:1") == {"name": "Alice", "age": 30}  # Git reads SQL!
    assert sql.get("tree:main") == {"README.md": "abc123"}    # SQL reads Git!
    assert git.get("tree:main") == {"README.md": "abc123"}

    # The blobs are PURE self-describing bytes (no envelope)
    raw_sql = sql.get_raw("user:1")
    raw_git = git.get_raw("tree:main")
    assert raw_sql == json_encode({"name": "Alice", "age": 30})  # pure JSON
    assert b"100644 blob abc123" in raw_git  # pure Git tree format
    # NO envelope bytes
    assert raw_sql[0:1] == b"{"  # JSON starts with {
    assert not raw_sql[0:1] == b"\x01"  # not a codec_id

    # Verify: no metadata in kernel
    names = kernel.list_names()
    assert not any("manifest" in n or "enable" in n for n in names)

    kernel.close()
    shutil.rmtree(bench, ignore_errors=True)
    print("PASS: Self-describing payloads")
    print("      - NO envelope, NO key context")
    print("      - Resolver sniffs magic bytes / patterns (like Unix file)")
    print("      - Any lens reads any blob (resolver sniffs format)")
    print("      - Blobs are pure self-describing bytes (JSON, Git tree)")
    print("      - Kernel stores pure bytes")
    print("      - Limitation: only works for self-describing formats")


if __name__ == "__main__":
    test_self_describing()
