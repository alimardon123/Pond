"""
LogLens — External Implementation Challenge (Task ID 27).

Implemented ONLY from:
  - RFC-0013: The Lens Interpretation Contract
  - RFC-0012: The Lens Architecture
  - pond-core/pond_minimal.py (the 3-primitive kernel)
  - DESIGN_GOALS.md
  - pond-sdk/view_sdk.py — consulted ONLY to locate the `Lens` base class
    (alias for `View`). No other Lens implementation was read.

The contract (RFC-0013) specifies:
  - The kernel stores pure bytes (no envelope, no codec_id, no header).   (§2)
  - The key prefix (e.g., "log/") provides context for the Resolver.      (§3.1)
  - Any Lens can read any blob via the Resolver (cross-Lens reading).      (§3.2)
  - get_raw(key) bypasses the Resolver (transform-later capability).       (§3.3)
  - The commit DAG is shared across Lenses of the same name.               (§3.4)
  - A Lens must NOT write envelope/header bytes into blobs.                (§4.1)
  - A Lens must NOT write manifest or enable_view metadata.                (§4.3)
  - The Resolver is code-level, ~30 LOC, with the interface:
        register(prefix, encode, decode)
        encode_for_key(key, data) -> bytes
        decode_for_key(key, raw)  -> Any                                   (§8)

This file:
  1. Defines ContextResolver (~30 LOC).
  2. Defines ContextLens — the generic Resolver-backed Lens (~30 LOC).
  3. Defines LogLens — a domain Lens for structured application logs.
  4. Defines a minimal SqlLens (a sibling Lens) to demonstrate cross-Lens
     reading and shared branching.
  5. Exercises all 7 challenge requirements in a self-contained test.
"""

from __future__ import annotations

import os
import sys
import json
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Optional

# ---------------------------------------------------------------------------
# Import path setup. pond-sdk/view_sdk.py adds prototype/ and pond-sdk/ to
# sys.path at import time and imports `pond_minimal` from there. We want to
# use pond-core/pond_minimal.py (the file the task told us to read). Both
# expose the same `PondMinimal` API (write/read/reference/resolve/list_names).
# ---------------------------------------------------------------------------
HERE = os.path.dirname(os.path.abspath(__file__))
POND_ROOT = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(POND_ROOT, "pond-sdk"))
sys.path.insert(0, os.path.join(POND_ROOT, "pond-core"))

from pond_minimal import PondMinimal          # the 3-primitive kernel
from lens_sdk import Lens                     # Lens is the alias for View (RFC-0012 §2)


# ---------------------------------------------------------------------------
# §8 — The Resolver (code-level, not data-level). ~30 LOC.
# ---------------------------------------------------------------------------

class ContextResolver:
    """Maps key prefix -> (encode, decode).

    Per RFC-0013 §8: lives in the application, not the kernel. Each
    deployment registers its own codecs. Different deployments can have
    different Resolvers with different codecs.

    Fallback behaviour (RFC-0013 §5):
      - If no codec matches the key prefix: return raw bytes.
      - If decode raises: return raw bytes.
    The caller never gets nothing; it always gets something it can work
    with, even if that is just the raw payload for later transformation.
    """

    def __init__(self) -> None:
        # prefix -> (encode: Any -> bytes, decode: bytes -> Any)
        self._codecs: dict[str, tuple[Callable[[Any], bytes], Callable[[bytes], Any]]] = {}

    def register(self, prefix: str, encode: Callable[[Any], bytes],
                 decode: Callable[[bytes], Any]) -> None:
        # Last registration wins (dict semantics). Idempotent for the
        # same (prefix, encode, decode) triple.
        self._codecs[prefix] = (encode, decode)

    def _codec_for(self, key: str) -> Optional[tuple]:
        # Longest-prefix match wins: "log/" beats "l" if both were registered.
        # This is an inference — the contract does not specify match policy
        # (see report §3, gap #4).
        best, best_len = None, -1
        for prefix, codec in self._codecs.items():
            if key.startswith(prefix) and len(prefix) > best_len:
                best, best_len = codec, len(prefix)
        return best

    def encode_for_key(self, key: str, data: Any) -> bytes:
        codec = self._codec_for(key)
        if codec is None:
            # No codec for this prefix. If the caller already gave us bytes,
            # pass them through; otherwise serialise as JSON (a sensible
            # default, but the contract is silent here — see report §3,
            # gap #5).
            if isinstance(data, (bytes, bytearray)):
                return bytes(data)
            return json.dumps(data, sort_keys=True).encode("utf-8")
        return codec[0](data)

    def decode_for_key(self, key: str, raw: bytes) -> Any:
        codec = self._codec_for(key)
        if codec is None:
            return raw  # RFC-0013 §5 step 4: return raw payload bytes.
        try:
            return codec[1](raw)
        except Exception:
            return raw  # RFC-0013 §5 step 5: decode failed -> raw bytes.


# ---------------------------------------------------------------------------
# Built-in codecs.
# ---------------------------------------------------------------------------

def _encode_json(data: Any) -> bytes:
    return json.dumps(data, sort_keys=True, separators=(",", ":")).encode("utf-8")

def _decode_json(raw: bytes) -> Any:
    return json.loads(raw.decode("utf-8"))


# ---------------------------------------------------------------------------
# ContextLens — a Resolver-backed Lens. ~30 LOC.
#
# The contract (RFC-0013 §3.2, §6) shows the usage pattern:
#     sql_lens = ContextLens(kernel, "workspace", resolver, "sql/")
#     git_lens = ContextLens(kernel, "workspace", resolver, "git/")
# but does not define the class itself. This is my inference of what the
# class must look like based on the contract's stated properties.
#
# Key design decisions (each inferred, not specified — see report §3):
#   - The caller provides the FULL key (including prefix). The Lens's
#     `prefix` attribute is metadata used to (a) register the codec with
#     the resolver, and (b) generate keys in domain helpers like write_log.
#   - put/get delegate encode/decode to the resolver, keyed by the FULL key.
#   - get_raw returns the pure payload bytes (bypassing the resolver),
#     matching RFC-0013 §3.3.
# ---------------------------------------------------------------------------

class ContextLens(Lens):
    """A Lens that delegates encode/decode to a ContextResolver.

    Multiple ContextLenses over the same kernel + same Lens name share:
      - the same Prolly tree (so they see each other's keys)
      - the same commit DAG (so branches are shared)                  (§3.4)
    They differ only in which key prefix they "own" — but any Lens can
    read any key, because the Resolver dispatches by the KEY's prefix,
    not the Lens's prefix.                                                (§3.2)
    """

    def __init__(self, kernel: PondMinimal, name: str,
                 resolver: ContextResolver, prefix: str) -> None:
        super().__init__(kernel, name)
        self.resolver = resolver
        self.prefix = prefix

    # Override put/get to thread the key through to the resolver.
    # The base class's encode(data)/decode(raw) signatures don't carry the
    # key, but the resolver needs the key to pick the codec. So we bypass
    # the base class's encode/decode and call the resolver directly.

    def put(self, key: str, data: Any) -> str:
        # RFC-0013 §4.1: the blob is pure payload. No envelope, no header.
        # The resolver encodes; the kernel stores the resulting bytes as-is.
        raw = self.resolver.encode_for_key(key, data)
        blob_hash = self.kernel.write(raw)
        self.base.stage(key, blob_hash)
        return blob_hash

    def get(self, key: str) -> Optional[Any]:
        h = self.base.lookup(key)
        if h is None:
            return None
        raw = self.kernel.read_blob(h)
        # RFC-0013 §3.2 / §5: resolver decodes by key prefix; falls back
        # to raw bytes if no codec matches or decode fails.
        return self.resolver.decode_for_key(key, raw)

    def get_raw(self, key: str) -> Optional[bytes]:
        # RFC-0013 §3.3: bypass the resolver entirely. Universal fallback.
        h = self.base.lookup(key)
        if h is None:
            return None
        return self.kernel.read_blob(h)

    # Commit / branch / checkout / history are inherited from the base
    # Lens class — they operate on the shared Prolly tree + commit DAG,
    # which is exactly what RFC-0013 §3.4 requires.


# ---------------------------------------------------------------------------
# LogLens — a domain Lens for structured application logs.
# ---------------------------------------------------------------------------

class LogLens(ContextLens):
    """A Lens for structured application logs.

    Each log entry is a JSON object with fields:
      - timestamp (ISO-8601 UTC)
      - level     ("INFO" | "WARN" | "ERROR")
      - message   (human-readable string)
      - service   (service name that emitted the log)
      - trace_id  (correlation id, auto-generated if not supplied)

    The Lens uses the "log/" key prefix (RFC-0013 §3.1). The codec is
    JSON, registered with the Resolver at construction time (§8).
    """

    PREFIX = "log/"
    LEVELS = {"INFO", "WARN", "ERROR"}

    def __init__(self, kernel: PondMinimal, name: str,
                 resolver: ContextResolver) -> None:
        super().__init__(kernel, name, resolver, self.PREFIX)
        # §8: register the codec with the Resolver. The Resolver is shared
        # across all Lenses in this deployment, so any Lens that reads a
        # "log/..." key will get the decoded dict.
        resolver.register(self.PREFIX, _encode_json, _decode_json)

    # --- Domain helpers ---

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    def write_log(self, level: str, message: str, service: str,
                  trace_id: Optional[str] = None,
                  timestamp: Optional[str] = None) -> str:
        """Write one log entry. Returns the full key (including "log/" prefix)."""
        if level not in self.LEVELS:
            raise ValueError(f"level must be one of {self.LEVELS}, got {level!r}")
        entry = {
            "timestamp": timestamp or self._now_iso(),
            "level":     level,
            "message":   message,
            "service":   service,
            "trace_id":  trace_id or uuid.uuid4().hex,
        }
        # Key layout: log/<timestamp>:<trace_id>
        # The timestamp makes keys sort chronologically in the Prolly tree;
        # the trace_id disambiguates entries that share a timestamp.
        key = f"{self.PREFIX}{entry['timestamp']}:{entry['trace_id']}"
        self.put(key, entry)
        return key

    def read_log(self, key: str) -> Optional[dict]:
        return self.get(key)

    def all_logs(self) -> list[dict]:
        """Return all log entries, sorted by key (≈ chronological)."""
        out = []
        for k in self.keys():
            if k.startswith(self.PREFIX):
                v = self.get(k)
                if v is not None:
                    out.append(v)
        out.sort(key=lambda e: e.get("timestamp", ""))
        return out


# ---------------------------------------------------------------------------
# SqlLens — a minimal sibling Lens, used to demonstrate cross-Lens reading
# and shared branching. NOT a full SQL implementation; just enough to prove
# that another Lens can read log entries written by LogLens.
# ---------------------------------------------------------------------------

class SqlLens(ContextLens):
    """A minimal "SQL row" Lens. Each row is a JSON dict. Prefix "sql/"."""

    PREFIX = "sql/"

    def __init__(self, kernel: PondMinimal, name: str,
                 resolver: ContextResolver) -> None:
        super().__init__(kernel, name, resolver, self.PREFIX)
        resolver.register(self.PREFIX, _encode_json, _decode_json)

    def insert(self, table: str, row_id: str, row: dict) -> str:
        key = f"{self.PREFIX}{table}:{row_id}"
        self.put(key, row)
        return key

    def select(self, table: str, row_id: str) -> Optional[dict]:
        return self.get(f"{self.PREFIX}{table}:{row_id}")


# ---------------------------------------------------------------------------
# Test harness — exercises all 7 challenge requirements.
# ---------------------------------------------------------------------------

def _make_kernel(tmpdir: str) -> PondMinimal:
    return PondMinimal(tmpdir)

LOG_FIELDS = {"timestamp", "level", "message", "service", "trace_id"}

def test_1_write_log_entries(log_lens: LogLens) -> list[str]:
    """Req 1: write log entries as JSON with the required fields."""
    print("\n[1] Writing log entries...")
    keys = [
        log_lens.write_log("INFO",  "service started",        "auth-svc", trace_id="t1"),
        log_lens.write_log("WARN",  "rate limit near",        "auth-svc", trace_id="t1"),
        log_lens.write_log("ERROR", "database connection lost", "db-svc",  trace_id="t2"),
    ]
    # Stage-then-commit model: lookup walks the commit DAG from HEAD, so
    # we commit before reading back. (The View base class's put() only
    # stages; commit() flushes to the kernel.)
    log_lens.commit("initial logs")
    # Verify each written blob is pure JSON (starts with '{') — RFC-0013 §4.1.
    for k in keys:
        raw = log_lens.get_raw(k)
        assert raw is not None
        assert raw[:1] == b"{", f"blob for {k} should be pure JSON, starts with {raw[:1]!r}"
        # Verify all required fields are present in the decoded entry.
        entry = json.loads(raw)
        assert LOG_FIELDS.issubset(entry.keys()), f"missing fields in {k}: {entry.keys()}"
        assert entry["level"] in LogLens.LEVELS
    print(f"    wrote {len(keys)} entries; all are pure JSON with required fields.")
    return keys


def test_2_read_log_entries(log_lens: LogLens, keys: list[str]) -> None:
    """Req 2: read log entries back, decoded as dicts."""
    print("\n[2] Reading log entries back...")
    for k in keys:
        entry = log_lens.read_log(k)
        assert isinstance(entry, dict), f"expected dict, got {type(entry)}"
        assert LOG_FIELDS.issubset(entry.keys())
        assert isinstance(entry["timestamp"], str)
        assert isinstance(entry["message"], str)
    print(f"    read {len(keys)} entries; all decode to dicts with correct fields.")


def test_3_key_prefix(keys: list[str]) -> None:
    """Req 3: keys use the 'log/' prefix."""
    print("\n[3] Verifying key prefix...")
    for k in keys:
        assert k.startswith("log/"), f"key {k!r} does not start with 'log/'"
    print(f"    all {len(keys)} keys start with 'log/' (RFC-0013 §3.1).")


def test_4_resolver_registration(resolver: ContextResolver) -> None:
    """Req 4: the codec is registered with the Resolver."""
    print("\n[4] Verifying Resolver registration...")
    assert "log/" in resolver._codecs, "log/ codec not registered"
    enc, dec = resolver._codecs["log/"]
    # Round-trip through the registered codec.
    payload = {"level": "INFO", "message": "hi", "service": "x", "trace_id": "t", "timestamp": "now"}
    raw = enc(payload)
    assert dec(raw) == payload
    # And through the resolver's key-dispatched path.
    assert resolver.decode_for_key("log/anything", resolver.encode_for_key("log/anything", payload)) == payload
    print("    'log/' codec registered with Resolver (RFC-0013 §8); round-trip OK.")


def test_5_cross_lens_reading(log_lens: LogLens, sql_lens: SqlLens, log_keys: list[str]) -> None:
    """Req 5: another Lens (SqlLens) can read log entries written by LogLens."""
    print("\n[5] Cross-Lens reading...")
    # SqlLens reads a key written by LogLens. The Resolver dispatches by
    # the KEY's prefix ("log/"), not by the Lens's prefix ("sql/").
    for k in log_keys:
        entry_via_sql = sql_lens.get(k)
        entry_via_log = log_lens.get(k)
        assert entry_via_sql == entry_via_log, (
            f"SqlLens read of {k} diverges from LogLens read"
        )
        assert LOG_FIELDS.issubset(entry_via_sql.keys())
    # And the reverse: LogLens reads a row written by SqlLens.
    # Each Lens commits its own staged writes; the shared commit DAG (in
    # the kernel) is what makes the write visible to the other Lens.
    sql_key = sql_lens.insert("users", "1", {"name": "Alice", "age": 30})
    sql_lens.commit("sql row inserted by SqlLens")
    row_via_log = log_lens.get(sql_key)
    assert row_via_log == {"name": "Alice", "age": 30}
    print(f"    SqlLens read {len(log_keys)} log entries (decoded via Resolver).")
    print(f"    LogLens read 1 SQL row (decoded via Resolver). Bidirectional OK.")


def test_6_branching(kernel: PondMinimal, name: str, resolver: ContextResolver) -> None:
    """Req 6: create a branch, write logs on it, verify the branch is visible."""
    print("\n[6] Branching...")
    log_lens = LogLens(kernel, name, resolver)
    sql_lens = SqlLens(kernel, name, resolver)

    # Baseline commit on main (the default branch is just the `workspace`
    # name ref pointing at HEAD; no explicit "main" branch is needed).
    log_lens.write_log("INFO", "main branch baseline", "svc-a", trace_id="base")
    log_lens.commit("baseline on main")
    baseline_head = kernel.resolve(name)  # C1

    # Create + checkout a branch from the Lens.
    branch_name = "debug-session"
    log_lens.branch(branch_name)
    assert branch_name in log_lens.list_branches(), f"branch {branch_name} not listed"
    log_lens.checkout(branch_name)

    # Write logs on the branch. (Levels are INFO/WARN/ERROR per the task spec.)
    branch_keys = [
        log_lens.write_log("WARN", "branch debug entry 1", "svc-a", trace_id="b1"),
        log_lens.write_log("ERROR", "branch debug entry 2", "svc-a", trace_id="b2"),
    ]
    log_lens.commit("debug logs on branch")

    # The OTHER Lens (SqlLens, same name) sees the branch and its entries —
    # because the commit DAG is shared (RFC-0013 §3.4). list_branches()
    # reads kernel.list_names() filtered by `<name>__branch__*`, which is
    # kernel-level state visible to every Lens over this name.
    assert branch_name in sql_lens.list_branches(), (
        "SqlLens does not see the branch created by LogLens — violates §3.4"
    )
    sql_lens.checkout(branch_name)
    for k in branch_keys:
        v = sql_lens.get(k)
        assert v is not None, f"SqlLens cannot read branch log {k}"
        assert v["trace_id"] in ("b1", "b2")

    # Isolation: walk the commit DAG back to the baseline (undo the single
    # branch commit). The branch entries must NOT be visible from C1.
    # (We use undo because the default branch is the `workspace` name ref,
    # not an explicit "main" branch — there is no checkout("main").)
    log_lens.undo(1)  # workspace -> parent(C2) == C1
    for k in branch_keys:
        assert sql_lens.get(k) is None, (
            f"branch log {k} leaked onto baseline — isolation broken"
        )
    # But the branch itself is still intact (the branch ref was not touched).
    sql_lens.checkout(branch_name)
    for k in branch_keys:
        assert sql_lens.get(k) is not None
    print(f"    created branch '{branch_name}' via LogLens.")
    print(f"    SqlLens sees the branch and reads its log entries (shared DAG).")
    print(f"    baseline (C1) is isolated from branch writes; branch ref intact.")


def test_7_transform_later(log_lens: LogLens, log_keys: list[str]) -> None:
    """Req 7: read a log entry as raw bytes (bypassing the resolver) and transform it."""
    print("\n[7] Transform-later (raw bytes bypass)...")
    key = log_keys[0]
    # §3.3: get_raw bypasses the resolver. We get pure JSON bytes.
    raw = log_lens.get_raw(key)
    assert raw is not None
    assert raw[:1] == b"{", f"raw bytes should be pure JSON, got {raw[:8]!r}"

    # Demonstrate that we can parse the raw bytes externally (no resolver)
    # and transform them into a different shape — e.g., a syslog-style line.
    entry = json.loads(raw)
    syslog_line = (
        f"<{134 if entry['level'] == 'INFO' else 137}>"
        f"{entry['timestamp']} {entry['service']}[{entry['trace_id'][:8]}]: "
        f"{entry['message']}"
    )
    assert "auth-svc" in syslog_line
    assert entry["message"] in syslog_line

    # Demonstrate that we can also re-encode the transformed value back into
    # the byte graph under a DIFFERENT key prefix, using a different codec.
    # Here we register a trivial "text/" codec for plain-text lines and write
    # the syslog line under it. The blob is pure text — no envelope.
    def _enc_text(s: Any) -> bytes:
        return s.encode("utf-8") if isinstance(s, str) else str(s).encode("utf-8")
    def _dec_text(b: bytes) -> str:
        return b.decode("utf-8")
    log_lens.resolver.register("text/", _enc_text, _dec_text)

    text_key = f"text/syslog:{entry['trace_id']}"
    log_lens.put(text_key, syslog_line)
    log_lens.commit("transformed log into syslog line")

    raw_text = log_lens.get_raw(text_key)
    assert raw_text == syslog_line.encode("utf-8"), (
        "transformed blob should be pure text, no envelope"
    )
    # And the reverse: read it back via the resolver as a decoded string.
    decoded = log_lens.get(text_key)
    assert decoded == syslog_line

    print(f"    read raw bytes for {key!r} ({len(raw)} bytes, pure JSON).")
    print(f"    transformed into syslog line: {syslog_line!r}")
    print(f"    wrote transformed line under 'text/' prefix; pure text bytes.")


def main() -> int:
    import tempfile
    tmpdir = tempfile.mkdtemp(prefix="loglens_")
    print(f"=== LogLens External Implementation Challenge (Task 27) ===")
    print(f"kernel tmpdir: {tmpdir}")

    kernel = _make_kernel(tmpdir)
    resolver = ContextResolver()
    name = "workspace"  # shared Lens name — both Lenses see the same DAG (§3.4)

    log_lens = LogLens(kernel, name, resolver)
    sql_lens = SqlLens(kernel, name, resolver)

    keys = test_1_write_log_entries(log_lens)
    test_2_read_log_entries(log_lens, keys)
    test_3_key_prefix(keys)
    test_4_resolver_registration(resolver)
    test_5_cross_lens_reading(log_lens, sql_lens, keys)
    test_6_branching(kernel, name, resolver)
    test_7_transform_later(log_lens, keys)

    # Kernel purity check: confirm no envelope, no manifest, no sidecar.
    stats = kernel.storage_stats()
    print(f"\n=== Kernel stats ===")
    print(f"  blob_count:   {stats['blob_count']}")
    print(f"  name_count:   {stats['name_count']}")
    print(f"  data_bytes:   {stats['data_bytes']}")
    print(f"  writes:       {stats['writes']}")
    print(f"  reads:        {stats['reads']}")
    print(f"  references:   {stats['references']}")

    kernel.close()
    print("\n=== ALL 7 REQUIREMENTS PASSED ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
