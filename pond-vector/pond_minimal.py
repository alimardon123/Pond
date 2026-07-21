"""
Mock implementation of the Pond kernel (Layer 0), built ONLY from the spec.

This is a faithful interpretation of:
    kernel.write(data: bytes) -> str          # 64-char hex hash (SHA-256)
    kernel.read(hash_or_name: str) -> bytes   # read by hash or name
    kernel.reference(name: str, hash: str)    # mutable name -> hash
    kernel.resolve(name: str) -> str | None   # resolve name to hash
    kernel.list_names() -> list[str]          # list all names

Used for testing VectorView.  NOT the real SDK.
"""

import hashlib


class PondMinimal:
    """Content-addressed immutable object store (the kernel)."""

    def __init__(self):
        self._objects: dict[str, bytes] = {}   # hash -> bytes
        self._names: dict[str, str] = {}       # name -> hash

    # --- Layer 0 primitives ---

    def write(self, data: bytes) -> str:
        h = hashlib.sha256(data).hexdigest()
        # Immutability + dedup: same bytes -> same hash.
        self._objects[h] = data
        return h

    def read(self, hash_or_name: str) -> bytes:
        if hash_or_name in self._objects:
            return self._objects[hash_or_name]
        if hash_or_name in self._names:
            return self._objects[self._names[hash_or_name]]
        raise KeyError(f"Object not found: {hash_or_name}")

    def reference(self, name: str, hash: str) -> None:
        self._names[name] = hash

    def resolve(self, name: str) -> str | None:
        return self._names.get(name)

    def list_names(self) -> list[str]:
        return list(self._names.keys())
