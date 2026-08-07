"""
OLTPLens — fast key-value with in-memory memtable + batch flush to object storage.

Solves the KV competitive gap: direct shard writes are ~0.5-3ms per write
(S3 RTT). With a memtable, writes are sub-microsecond (in-memory) and
flush to object storage in batches — amortizing S3 latency across N writes.

DESIGN FOR COLD CONCURRENT MULTI-APP:
  Each app process has its own OLTPLens instance with its own memtable.
  Writes go to the in-memory memtable (sub-µs). When the memtable is full
  (or flush() is called explicitly), it flushes to object storage as a
  CRDT shard via upsert_shard() — which is concurrent-safe.

  Multiple apps flush independently — no coordination, no CAS, no locks.
  The CRDT merge (read_with_shards) handles conflicts deterministically.

  This is the SAME pattern as RocksDB's LSM-tree, but:
    - SST files → CRDT shards (concurrent-safe, no CAS)
    - Compaction → compact_shards (already built)
    - Multi-process → each flushes independently (CRDT handles conflicts)

USAGE:
    from pond_storage import PondStorage
    from oltp_lens import OLTPLens

    storage = PondStorage(kernel)
    storage.write("kv", [{"_key": "init", "value": b""}], key_col="_key")

    ottp = OLTPLens(storage, "kv", flush_threshold=1000)

    # Fast writes (sub-µs, in-memory)
    ottp.put("user:1", {"name": "alice", "age": 30})
    ottp.put("user:2", {"name": "bob", "age": 25})
    ottp.delete("user:2")

    # Reads check memtable first (0 GETs), then object storage
    user = ottp.get("user:1")

    # Flush to object storage (1 PUT, amortized across all writes)
    ottp.flush()

    # Cold read (new connection) — sees all flushed data
    rows = storage.read_with_shards("kv")
"""
from __future__ import annotations

import os, sys, time, json
from typing import Optional, Any, Dict
from collections import OrderedDict

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                  "..", "..", "..", "bindings/python/sdk"))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                  "..", "..", "..", "bindings/python/sdk", "extensions",
                                  "physical_structures"))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                  "..", "..", "..", "bindings/python/core"))

from base_lens import PondLens  # noqa: E402


class OLTPLens(PondLens):
    """Fast key-value lens with in-memory memtable + batch flush.

    Each app process creates its own OLTPLens instance. Writes go to
    the in-memory memtable (sub-µs). When full or flush() is called,
    it flushes to object storage as a CRDT shard.

    Multiple apps flush independently — CRDT handles conflicts.

    Extends PondLens to inherit branch/list_collections/set_definition/
    get_definition/history for free. Note: OLTPLens's __init__ takes
    `storage` (a PondStorage instance) rather than `kernel` directly,
    because it needs the full storage API for shard appends.
    """

    def __init__(self, storage, collection: str,
                 key_col: str = "_key",
                 flush_threshold: int = 1000,
                 flush_interval_s: float = 5.0,
                 value_col: str = "value"):
        # OLTPLens doesn't take kernel directly — it takes a PondStorage
        # which wraps a kernel. Extract the kernel for PondLens.__init__.
        kernel = getattr(storage, "kernel", None) or storage
        super().__init__(kernel)
        self.storage = storage
        self.collection = collection
        self.key_col = key_col
        self.value_col = value_col
        self.flush_threshold = flush_threshold
        self.flush_interval_s = flush_interval_s
        self._memtable: OrderedDict = OrderedDict()
        self._last_flush = time.time()
        self._rowid_cache: Dict[str, str] = {}

    def put(self, key: str, value: Any) -> None:
        """Put a key-value pair (sub-µs, in-memory). Auto-flushes when full."""
        if len(self._memtable) >= self.flush_threshold:
            self.flush()
        elif self.flush_interval_s > 0 and (time.time() - self._last_flush) > self.flush_interval_s:
            self.flush()
        entry = {self.key_col: key, self.value_col: self._encode_value(value), "_deleted": False}
        if key in self._rowid_cache:
            entry["_rowid"] = self._rowid_cache[key]
        self._memtable[key] = entry

    def delete(self, key: str) -> None:
        """Delete a key (tombstone, sub-µs)."""
        if len(self._memtable) >= self.flush_threshold:
            self.flush()
        entry = {self.key_col: key, self.value_col: b"", "_deleted": True}
        if key in self._rowid_cache:
            entry["_rowid"] = self._rowid_cache[key]
        self._memtable[key] = entry

    def get(self, key: str) -> Optional[Any]:
        """Get a value (memtable first, then object storage)."""
        if key in self._memtable:
            entry = self._memtable[key]
            if entry.get("_deleted"):
                return None
            return self._decode_value(entry.get(self.value_col, b""))
        # Fall back to object storage
        rows = self.storage.read_with_shards(self.collection)
        for r in rows:
            if str(r.get(self.key_col)) == str(key):
                if r.get("_rowid"):
                    self._rowid_cache[key] = r["_rowid"]
                if r.get("_deleted"):
                    return None
                return self._decode_value(r.get(self.value_col, b""))
        return None

    def exists(self, key: str) -> bool:
        return self.get(key) is not None

    def keys(self) -> list:
        """List all keys (memtable + object storage, merged)."""
        rows = self.storage.read_with_shards(self.collection)
        all_keys = set()
        for r in rows:
            k = r.get(self.key_col)
            if k is not None and not r.get("_deleted"):
                all_keys.add(str(k))
        for key, entry in self._memtable.items():
            if entry.get("_deleted"):
                all_keys.discard(key)
            else:
                all_keys.add(key)
        return sorted(all_keys)

    def count(self) -> int:
        return len(self.keys())

    def flush(self) -> Optional[str]:
        """Flush memtable to object storage as a CRDT shard.

        N writes went to memtable (sub-µs each). 1 flush = 1 shard PUT.
        Amortized: ~2-5ms / N per write.
        """
        if not self._memtable:
            return None
        rows = list(self._memtable.values())
        result = self.storage.upsert_shard(
            self.collection, rows, key_col=self.key_col,
            row_group_size=min(len(rows), 1000))
        self._memtable.clear()
        self._last_flush = time.time()
        return result

    def pending_count(self) -> int:
        return len(self._memtable)

    def compact(self) -> Optional[str]:
        """Flush memtable + compact all shards into HEAD."""
        self.flush()
        return self.storage.compact_shards(self.collection)

    def _encode_value(self, value: Any) -> bytes:
        if isinstance(value, bytes):
            return value
        if isinstance(value, str):
            return value.encode("utf-8")
        return json.dumps(value, sort_keys=True).encode("utf-8")

    def _decode_value(self, data: bytes) -> Any:
        if data is None:
            return None
        if not isinstance(data, bytes):
            return data
        try:
            return json.loads(data)
        except (json.JSONDecodeError, UnicodeDecodeError):
            try:
                return data.decode("utf-8")
            except UnicodeDecodeError:
                return data
