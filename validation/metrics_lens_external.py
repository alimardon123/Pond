"""
MetricsLens — an independent implementation built from the Lens
Interpretation Contract (RFC-0013) alone.

This file was authored by a fresh agent who had never seen Pond before.
Allowed inputs:
  - RFC-0013 (the contract)
  - RFC-0012 (the architecture)
  - pond-core/pond_minimal.py (the ~140-LOC kernel)
  - POND.md (the one-page summary)
  - lens_sdk.py — consulted ONLY for the Lens class import path
    and `Lens(kernel, name)` constructor signature.

It was NOT allowed to read any existing Lens implementation, test,
or experiment file.

Design (per RFC-0013):
  - The kernel stores pure bytes (no envelope, no codec id).
  - Interpretation lives in CODE (a Resolver), not in DATA (the blob).
  - The Resolver dispatches by KEY PREFIX (e.g. "metrics/").
  - Multiple Lenses that share the same `name` share the same byte
    graph (same Prolly tree, same commit DAG, same branches).
  - Any Lens can read any blob via the Resolver; if the codec is
    unknown or decode fails, the caller gets raw bytes (transform-later).

Layers:
  ContextResolver    — prefix -> (encode, decode) registry. (~37 LOC)
  ContextLens        — Lens override that routes put/get by key prefix
                       through the resolver. (~30 LOC)
  MetricsLens        — registers the "metrics/" prefix with a JSON
                       codec and adds time-series + tag-filter helpers.
                       (~70 LOC)
  main()             — verification harness covering every task
                       requirement + contract fallback + kernel purity.

Metric data point schema (stored as JSON under key prefix "metrics/"):
  {
    "metric_name": str,        # e.g. "cpu_usage"
    "timestamp":   float|int,  # epoch seconds (or any comparable scalar)
    "value":       number,     # the measurement
    "tags":        dict,       # e.g. {"host": "h-1", "region": "us-east"}
    "unit":        str         # e.g. "percent", "bytes", "requests/s"
  }

Key format (chosen by this Lens author; the kernel does not interpret it):
  metrics/<metric_name>:<timestamp>:<short_uuid>

The short_uuid disambiguates multiple samples for the same metric at
the same timestamp. The colon-separated form is human-greppable.
"""

from __future__ import annotations

import os
import sys
import json
import time
import uuid
from typing import Any, Callable, Optional

# Make pond-sdk importable. We only import the Lens base class — we do
# NOT import or read any other Lens implementation.
_HERE = os.path.dirname(os.path.abspath(__file__))
_POND_SDK = os.path.normpath(os.path.join(_HERE, "..", "pond-sdk"))
_POND_CORE = os.path.normpath(os.path.join(_HERE, "..", "pond-core"))
for _p in (_POND_SDK, _POND_CORE):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from lens_sdk import Lens                       # noqa: E402
from pond_minimal import PondMinimal            # noqa: E402


# ===========================================================================
# ContextResolver — RFC-0013 §8
#
# A CODE-level (not DATA-level) registry that maps key prefixes to
# (encode, decode) pairs. Dispatches by longest-prefix-match. Falls
# back to raw bytes when no codec matches or when decode raises.
# ===========================================================================

class ContextResolver:
    """Prefix-dispatch codec resolver (RFC-0013 §8).

    Lives in application code, not in the kernel. Each deployment
    registers its own codecs. The kernel never sees this object.
    """

    def __init__(self) -> None:
        # Ordered list of (prefix, encode, decode). Longest prefix wins.
        self._codecs: list[tuple[str, Callable[[Any], bytes],
                                 Callable[[bytes], Any]]] = []

    def register(self, prefix: str,
                 encode: Callable[[Any], bytes],
                 decode: Callable[[bytes], Any]) -> None:
        # Longest-prefix-first ordering makes dispatch O(n_codecs) but
        # n_codecs is tiny (~5). Keeps the code path trivial to read.
        self._codecs.append((prefix, encode, decode))
        self._codecs.sort(key=lambda t: len(t[0]), reverse=True)

    def _codec_for(self, key: str):
        for prefix, enc, dec in self._codecs:
            if key.startswith(prefix):
                return enc, dec
        return None, None

    def encode_for_key(self, key: str, data: Any) -> bytes:
        enc, _ = self._codec_for(key)
        if enc is None:
            # No codec — let the caller decide. We fall back to JSON so
            # that put() still produces immutable bytes; the contract
            # only requires that get() falls back to raw bytes on read.
            return json.dumps(data, sort_keys=True,
                              default=str).encode("utf-8")
        return enc(data)

    def decode_for_key(self, key: str, raw: bytes) -> Any:
        _, dec = self._codec_for(key)
        if dec is None:
            return raw  # RFC-0013 §5 step 4: unknown codec -> raw bytes
        try:
            return dec(raw)
        except Exception:
            return raw  # RFC-0013 §5 step 5: decode failed -> raw bytes


# ===========================================================================
# ContextLens — the ~25-LOC Lens override (RFC-0013 §8)
#
# Overrides put/get/get_all so encode/decode dispatch by key prefix
# through the Resolver. Inherits everything else (branch/checkout/merge,
# history, commit, keys, count, get_raw) from the base Lens.
#
# All ContextLens instances that share the same `name` and `resolver`
# share the same byte graph and the same codec dispatch — this is what
# makes cross-Lens reading work (RFC-0013 §6).
# ===========================================================================

class ContextLens(Lens):
    """A Lens whose codec is chosen by key prefix via a ContextResolver."""

    def __init__(self, kernel: PondMinimal, name: str,
                 resolver: ContextResolver) -> None:
        super().__init__(kernel, name)
        self.resolver = resolver

    # --- Write path: route by key prefix ---
    def put(self, key: str, data: Any) -> str:           # type: ignore[override]
        blob_hash = self.kernel.write(self.resolver.encode_for_key(key, data))
        self.base.stage(key, blob_hash)
        return blob_hash

    # --- Read path: route by key prefix, fall back to raw bytes ---
    def get(self, key: str) -> Optional[Any]:            # type: ignore[override]
        h = self.base.lookup(key)
        if h is None:
            return None
        return self.resolver.decode_for_key(key, self.kernel.read_blob(h))

    def get_all(self) -> dict[str, Any]:                 # type: ignore[override]
        state = self.base.read_all()
        out: dict[str, Any] = {}
        for k, h in state.items():
            if k.startswith("_"):
                continue
            out[k] = self.resolver.decode_for_key(k, self.kernel.read_blob(h))
        return out


# ===========================================================================
# MetricsLens — the actual time-series Lens
# ===========================================================================

# Key prefix (RFC-0013 §3.1 — the prefix is part of the key, owned by
# the kernel as Names; the Lens author chooses the convention).
METRICS_PREFIX = "metrics/"


def _encode_json(data: Any) -> bytes:
    """Encode as compact, sort-keyed JSON. Pure payload — no envelope."""
    return json.dumps(data, sort_keys=True,
                      default=str).encode("utf-8")


def _decode_json(raw: bytes) -> Any:
    return json.loads(raw.decode("utf-8"))


class MetricsLens(ContextLens):
    """A Lens for time-series metric data points.

    Stores each data point as a pure-JSON blob under the "metrics/"
    key prefix. Inherits branching, checkout, history, get_raw, keys,
    and count from the base Lens (via ContextLens). Adds:

      put_metric(...)          — append a single data point
      get_metric(key)          — read a single data point
      query_time_range(...)    — points whose timestamp is in [start, end]
      filter_by_tags(...)      — points whose tags contain the query tags
      list_metric_names()      — distinct metric_name values present
    """

    # Required metric-point fields. Stored as-is in the JSON payload.
    REQUIRED_FIELDS = ("metric_name", "timestamp", "value", "tags", "unit")

    def __init__(self, kernel: PondMinimal, name: str,
                 resolver: Optional[ContextResolver] = None) -> None:
        # If the caller didn't supply a resolver, build one and register
        # the metrics/ codec. Either way, this Lens guarantees its codec
        # is registered (RFC-0013 §11 — "registers its codec with the
        # Resolver").
        owns_resolver = resolver is None
        if owns_resolver:
            resolver = ContextResolver()
        if not any(p == METRICS_PREFIX for p, _, _ in resolver._codecs):
            resolver.register(METRICS_PREFIX, _encode_json, _decode_json)
        super().__init__(kernel, name, resolver)

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def put_metric(self,
                   metric_name: str,
                   value: float,
                   timestamp: Optional[float] = None,
                   tags: Optional[dict] = None,
                   unit: str = "") -> str:
        """Append a metric data point. Returns the key."""
        if not isinstance(metric_name, str) or not metric_name:
            raise ValueError("metric_name must be a non-empty string")
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            raise ValueError("value must be a number")
        if timestamp is None:
            timestamp = time.time()
        if not isinstance(timestamp, (int, float)) or isinstance(timestamp, bool):
            raise ValueError("timestamp must be a number")
        if tags is None:
            tags = {}
        if not isinstance(tags, dict):
            raise ValueError("tags must be a dict")
        if not isinstance(unit, str):
            raise ValueError("unit must be a string")

        # Sanitize metric_name for use in the key (replace any "/").
        safe = metric_name.replace("/", "_")
        short = uuid.uuid4().hex[:8]
        key = f"{METRICS_PREFIX}{safe}:{timestamp}:{short}"

        point = {
            "metric_name": metric_name,
            "timestamp": timestamp,
            "value": value,
            "tags": dict(tags),   # defensive copy
            "unit": unit,
        }
        self.put(key, point)
        return key

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def get_metric(self, key: str) -> Optional[dict]:
        """Read a single metric data point (decoded)."""
        return self.get(key)

    def list_metric_names(self) -> list[str]:
        """Distinct metric_name values across all committed points."""
        names: set[str] = set()
        for row in self._iter_points():
            names.add(row["metric_name"])
        return sorted(names)

    def query_time_range(self,
                         start: float,
                         end: float,
                         metric_name: Optional[str] = None) -> list[dict]:
        """Return all metric points with start <= timestamp <= end.

        If metric_name is given, filter to that metric only.
        """
        if start > end:
            raise ValueError(f"start ({start}) > end ({end})")
        out: list[dict] = []
        for row in self._iter_points():
            ts = row["timestamp"]
            if ts < start or ts > end:
                continue
            if metric_name is not None and row["metric_name"] != metric_name:
                continue
            out.append(row)
        out.sort(key=lambda r: (r["timestamp"], r["metric_name"]))
        return out

    def filter_by_tags(self,
                       tags: dict,
                       metric_name: Optional[str] = None) -> list[dict]:
        """Return all metric points whose tags contain every (k, v) in
        `tags` (subset match). If metric_name is given, also filter by
        metric name.
        """
        if not isinstance(tags, dict):
            raise ValueError("tags must be a dict")
        query_items = set(tags.items())
        out: list[dict] = []
        for row in self._iter_points():
            row_items = set(row["tags"].items())
            if not query_items.issubset(row_items):
                continue
            if metric_name is not None and row["metric_name"] != metric_name:
                continue
            out.append(row)
        out.sort(key=lambda r: (r["timestamp"], r["metric_name"]))
        return out

    # ------------------------------------------------------------------
    # Internal helper — iterate decoded points, skipping non-metrics
    # (e.g. blobs written by another Lens under a different prefix).
    # ------------------------------------------------------------------

    def _iter_points(self):
        for key in self.keys():
            if not key.startswith(METRICS_PREFIX):
                continue
            row = self.get(key)
            if isinstance(row, dict) and "metric_name" in row:
                yield row


# ===========================================================================
# Verification harness — exercises every task requirement + the contract.
# ===========================================================================

def main() -> int:
    import tempfile
    tmp = tempfile.mkdtemp(prefix="metrics_lens_")
    kernel = PondMinimal(tmp)

    print("=" * 70)
    print("MetricsLens — independent implementation from RFC-0013")
    print("=" * 70)

    # ------------------------------------------------------------------
    # Shared resolver so cross-Lens reading works (RFC-0013 §6).
    # The MetricsLens registers "metrics/" automatically.
    # ------------------------------------------------------------------
    resolver = ContextResolver()
    metrics = MetricsLens(kernel, "workspace", resolver)

    # ---- Requirement 1 + 2: store metric data points as JSON under
    #      the "metrics/" key prefix. ----
    print("\n[R1/R2] Store metric data points as JSON under metrics/ prefix")
    base_ts = 1_700_000_000
    k1 = metrics.put_metric("cpu_usage",  42.5, base_ts + 0,
                            {"host": "h-1", "region": "us-east"}, "percent")
    k2 = metrics.put_metric("cpu_usage",  88.0, base_ts + 60,
                            {"host": "h-2", "region": "us-east"}, "percent")
    k3 = metrics.put_metric("cpu_usage",  99.9, base_ts + 120,
                            {"host": "h-1", "region": "us-west"}, "percent")
    k4 = metrics.put_metric("mem_used",   2048, base_ts + 0,
                            {"host": "h-1", "region": "us-east"}, "bytes")
    k5 = metrics.put_metric("req_rate",   1500, base_ts + 30,
                            {"host": "h-2", "region": "us-west",
                             "svc":  "checkout"}, "requests/s")
    c0 = metrics.commit("initial metrics batch")
    print(f"  wrote 5 points, commit={c0[:12]}…, count={metrics.count()}")

    # Verify the schema round-trips and the kernel sees PURE JSON.
    p1 = metrics.get_metric(k1)
    assert p1 is not None
    assert p1["metric_name"] == "cpu_usage"
    assert p1["value"] == 42.5
    assert p1["tags"] == {"host": "h-1", "region": "us-east"}
    assert p1["unit"] == "percent"
    assert set(p1.keys()) == set(metrics.REQUIRED_FIELDS), \
        f"unexpected fields: {set(p1.keys())}"
    print(f"  point schema OK: {p1}")

    # ---- Requirement 3: codec registered with a ContextResolver. ----
    print("\n[R3] Codec registered with ContextResolver")
    # The resolver knows "metrics/" -> JSON.
    found = [(p, e, d) for p, e, d in resolver._codecs
             if p == METRICS_PREFIX]
    assert found, "metrics/ codec not registered"
    enc, dec = found[0][1], found[0][2]
    sample = {"metric_name": "x", "timestamp": 1, "value": 2,
              "tags": {}, "unit": ""}
    raw = enc(sample)
    assert dec(raw) == sample
    # Encode/decode are pure (no envelope, no header).
    assert raw[:1] == b"{", f"blob should start with '{{', got {raw[:8]!r}"
    print(f"  metrics/ codec registered; sample payload head={raw[:24]!r}")

    # ---- Requirement 4: cross-Lens reading. ----
    print("\n[R4] Cross-Lens reading — a different Lens reads metrics data")
    # A second Lens under a different prefix but the same name + resolver.
    # It can read metrics/* keys via the shared resolver.
    observer = ContextLens(kernel, "workspace", resolver)
    # Register an unrelated codec for "observer/" just to show the
    # observer has its own encoding too — but it can still read metrics.
    resolver.register("observer/",
                      lambda d: _encode_json(d),
                      lambda b: _decode_json(b))
    # NOTE on contract clarity: the base Lens.put(key, data) returns the
    # BLOB HASH, not the key. The key is what the caller passed in and
    # is what must be shared across Lenses for cross-Lens reading. The
    # contract's example doesn't specify put's return value; this is a
    # small but real DX gap (recorded in the report).
    obs_key = "observer/note:1"
    bh = observer.put(obs_key, {"note": "watching"})
    assert isinstance(bh, str) and len(bh) == 64, \
        f"put should return a 64-char blob hash, got {bh!r}"
    observer.commit("observer note")

    cross = observer.get(k1)  # observer reads a metrics/ key
    assert cross == p1, f"cross-lens read mismatch: {cross} != {p1}"
    print(f"  observer.get({k1!r}) = {cross}")
    # And metrics Lens can read the observer's blob (both are JSON via
    # the shared resolver, so they interdecode — emergent overlap,
    # exactly as RFC-0012 §3 describes).
    back = metrics.get(obs_key)
    assert back == {"note": "watching"}, f"reverse cross-lens: {back}"
    print(f"  metrics.get({obs_key!r}) = {back}")

    # ---- Requirement 5: branching. ----
    print("\n[R5] Branching — create a branch, verify isolation")
    # The SDK doesn't auto-create a "main" branch — list_branches()
    # returns only named branches. We name our trunk "main" explicitly
    # so the experiment is unambiguous.
    main_branch = metrics.branch("main")
    print(f"  created 'main' branch ref: {main_branch[:12]}…")
    dev_branch = metrics.branch("dev")
    print(f"  created 'dev'  branch ref: {dev_branch[:12]}…")
    assert set(metrics.list_branches()) == {"main", "dev"}, \
        metrics.list_branches()

    # On 'dev', add a point. On 'main', it must be absent.
    metrics.checkout("dev")
    k_dev = metrics.put_metric("cpu_usage", 100.0,
                               base_ts + 180,
                               {"host": "h-3", "region": "eu"},
                               "percent")
    metrics.commit("dev: add a spike point")
    print(f"  wrote to dev: {k_dev}")
    dev_count = metrics.count()
    print(f"  count on dev branch = {dev_count}")

    metrics.checkout("main")
    main_count = metrics.count()
    print(f"  count on main branch = {main_count}")
    assert main_count < dev_count, \
        f"branch isolation failed: main={main_count} dev={dev_count}"
    assert metrics.get_metric(k_dev) is None, \
        "dev-only point leaked into main branch"
    print("  branch isolation OK (dev-only point absent from main)")

    # ---- Requirement 6: get_raw — raw bytes for transform-later. ----
    print("\n[R6] get_raw — pure payload bytes, transform-later")
    raw1 = metrics.get_raw(k1)
    assert isinstance(raw1, bytes)
    assert raw1[:1] == b"{", f"raw should start with '{{': {raw1[:8]!r}"
    # The caller can re-parse externally (transform-later, RFC-0013 §7).
    re_parsed = json.loads(raw1.decode("utf-8"))
    assert re_parsed == p1
    # Also: get_raw on a key whose codec the resolver doesn't know must
    # still return raw bytes (RFC-0013 §5 fallback). We write a blob
    # under an unknown prefix via the kernel directly to prove this.
    raw_blob = b"\x00\x01\x02NOT-JSON-NO-CODEC"
    bh = kernel.write(raw_blob)
    metrics.base.stage("unknown_prefix/thing", bh)
    metrics.commit("add an unknown-prefix blob")
    fb = metrics.get("unknown_prefix/thing")  # via resolver -> raw bytes
    assert fb == raw_blob, f"fallback expected raw bytes, got {fb!r}"
    fb_raw = metrics.get_raw("unknown_prefix/thing")
    assert fb_raw == raw_blob
    print(f"  get_raw OK: {raw1[:32]!r}…")
    print(f"  fallback OK (unknown prefix -> raw bytes): {fb!r}")

    # ---- Requirement 7: time-range query. ----
    print("\n[R7] Time-range query")
    # Back to main: 5 points at base_ts + {0, 0, 30, 60, 120}.
    # Query [base_ts + 0, base_ts + 60] should hit 4 of them.
    q = metrics.query_time_range(base_ts + 0, base_ts + 60)
    ts_in_range = sorted({base_ts + 0, base_ts + 30, base_ts + 60})
    got_ts = sorted({r["timestamp"] for r in q})
    assert got_ts == ts_in_range, f"time-range query: {got_ts}"
    print(f"  [base+0, base+60] -> {len(q)} points (expected 4)")
    # Filter by metric_name within the range.
    q_cpu = metrics.query_time_range(base_ts + 0, base_ts + 60,
                                     metric_name="cpu_usage")
    assert all(r["metric_name"] == "cpu_usage" for r in q_cpu)
    assert len(q_cpu) == 2, f"expected 2 cpu_usage points, got {len(q_cpu)}"
    print(f"  same range, metric_name='cpu_usage' -> {len(q_cpu)} points")
    # start > end must raise.
    try:
        metrics.query_time_range(100, 50)
        raise AssertionError("expected ValueError for start > end")
    except ValueError:
        print("  start > end correctly raises ValueError")

    # ---- Requirement 8: tag-based filtering. ----
    print("\n[R8] Tag-based filtering")
    east = metrics.filter_by_tags({"region": "us-east"})
    assert len(east) == 3, f"region=us-east -> {len(east)} (want 3)"
    assert all(r["tags"]["region"] == "us-east" for r in east)
    print(f"  tags={{'region':'us-east'}} -> {len(east)} points")
    h1_east = metrics.filter_by_tags({"host": "h-1", "region": "us-east"})
    assert len(h1_east) == 2, f"host=h-1,region=us-east -> {len(h1_east)}"
    print(f"  tags={{'host':'h-1','region':'us-east'}} -> {len(h1_east)} points")
    # Tag filter + metric_name combo.
    cpu_h1 = metrics.filter_by_tags({"host": "h-1"}, metric_name="cpu_usage")
    assert len(cpu_h1) == 2, f"cpu_usage host=h-1 -> {len(cpu_h1)}"
    print(f"  tags={{'host':'h-1'}} + metric_name='cpu_usage' -> {len(cpu_h1)} points")
    # No-match tag.
    none = metrics.filter_by_tags({"region": "ap-south"})
    assert none == [], f"non-matching tag -> {none}"
    print("  tags={'region':'ap-south'} -> 0 points (no false positives)")

    # ---- Contract §4/§9: kernel purity. ----
    print("\n[Contract §4/§9] Kernel purity — every blob is pure payload")
    stats = kernel.storage_stats()
    print(f"  kernel stats: blobs={stats['blob_count']}, "
          f"bytes={stats['data_bytes']}, "
          f"names={stats['name_count']}")
    # Walk every committed metrics/ key on main, verify the stored blob
    # has NO envelope (starts with '{' for JSON metrics, or is the raw
    # fallback blob for unknown_prefix).
    bad = []
    for key in metrics.keys():
        raw = metrics.get_raw(key)
        if key.startswith(METRICS_PREFIX):
            if raw[:1] != b"{":
                bad.append((key, raw[:8]))
        # unknown_prefix/thing is intentionally non-JSON — that's allowed
        # (the kernel doesn't know what format the bytes are in).
    assert not bad, f"impure metric blobs: {bad}"
    print(f"  all {sum(1 for k in metrics.keys() if k.startswith(METRICS_PREFIX))} "
          "metrics/ blobs are pure JSON (start with '{')")

    # ---- Bonus: list_metric_names ----
    print("\n[Bonus] list_metric_names()")
    names = metrics.list_metric_names()
    assert names == ["cpu_usage", "mem_used", "req_rate"], names
    print(f"  distinct metric names: {names}")

    # ---- Bonus: merge dev back into main (branching round-trip) ----
    print("\n[Bonus] Merge dev into main (branching round-trip)")
    pre = metrics.count()
    # NOTE on contract clarity: the base Lens.merge(name) takes only the
    # branch name — no message argument. The contract doesn't specify
    # merge's signature; discovered via the SDK.
    merged = metrics.merge("dev")
    post = metrics.count()
    assert post == pre + 1, f"merge count: pre={pre} post={post}"
    assert metrics.get_metric(k_dev) is not None, "dev point missing after merge"
    print(f"  pre={pre}, post={post} (merged commit {merged[:12]}…)")
    print(f"  dev-only point {k_dev} now visible on main after merge")

    # ------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("ALL REQUIREMENTS PASS")
    print("=" * 70)
    print("""
  R1  Store metric data points as JSON ........... PASS
  R2  Use key prefix "metrics/" .................. PASS
  R3  Register codec with ContextResolver ........ PASS
  R4  Cross-Lens reading ......................... PASS
  R5  Branching (create, verify isolation) ....... PASS
  R6  get_raw (transform-later fallback) ......... PASS
  R7  Time-range query [start, end] .............. PASS
  R8  Tag-based filtering .......................  PASS
  §5  Unknown-prefix fallback -> raw bytes ....... PASS
  §4/§9 Kernel purity (pure payload, no envelope). PASS
""")
    kernel.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
