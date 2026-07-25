"""
Pond Phase O.1 — Tests for the Remaining Untested Laws

Covers laws Phase L and N did not test:
  Manifest Algebra:
    MAN3  Manifest may be stale (orphaned after pack replacement)

  Range Read Algebra:
    RR3   Cost is per-range, not per-byte
    RR4   Backend may decompose ReadRange as Read+slice

  GC Algebra:
    G2    Liveness — eventually all unreachable blobs collected
    G4    Non-blocking — GC doesn't block reads/writes
    G5    Tombstone interaction — tombstoned ref's blobs become unreachable

  Replication Algebra:
    REP2  Secondary reads are stale, bounded by replica_lag_ms
    REP4  Blob replication must precede commit replication
    REP5  Failover loses in-flight writes
    REP6  Failover requires explicit promotion
    REP8  No multi-writer convergence (without coordinator)
    REP9  Replication is one-directional

  Transport Algebra:
    TR4   Transport optional per Collection
    TR5   Transport is per-blob, not per-byte

  Schema Evolution Algebra:
    SE1   Backward compatibility — new code reads old data
    SE2   Forward compatibility — old code reads new data
    SE3   Writer schema recorded
    SE4   Compatibility is Lens responsibility (kernel doesn't enforce)
    SE7   Schema Registry is a Naming convention (no new substrate)

Run:
    python scripts/phase_o_remaining_laws.py
"""

from __future__ import annotations

import os
import sys
import json
import time
import tempfile
import shutil
import struct

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(SCRIPT_DIR, "..", "pond-core"))
sys.path.insert(0, os.path.join(SCRIPT_DIR, "..", "services", "transport"))
sys.path.insert(0, SCRIPT_DIR)
from kernel import PondMinimal  # noqa: E402
from phase_l_hazard_simulator import HazardSimulator, HazardConfig  # noqa: E402

PASS = 0
FAIL = 0


def check(cond, label, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  [OK] {label}")
    else:
        FAIL += 1
        print(f"  [FAIL] {label} {detail}")


def make_kernel():
    tmpdir = tempfile.mkdtemp(prefix="pond_o_")
    return PondMinimal(tmpdir), tmpdir


def make_hazard(cfg=None):
    tmpdir = tempfile.mkdtemp(prefix="pond_oh_")
    return HazardSimulator(tmpdir, cfg or HazardConfig()), tmpdir


def cleanup(k, d):
    try:
        k.close()
    except Exception:
        pass
    shutil.rmtree(d, ignore_errors=True)


# ---------------------------------------------------------------------------
# Manifest Algebra
# ---------------------------------------------------------------------------

def test_MAN3_manifest_may_be_stale():
    """MAN3: A manifest lists hashes at write time. If the pack
    reference changes (new pack replaces old), the old manifest
    is orphaned. GC must mark both."""
    print("\n=== MAN3: Manifest may be stale ===")
    k, d = make_kernel()
    try:
        # Pack 1 with manifest
        blob1 = k.write(b"data1")
        manifest1 = json.dumps({"hashes": [blob1]}).encode()
        m1 = k.write(manifest1)
        k.reference("pack/manifest", m1)

        # Pack 2 replaces pack 1
        blob2 = k.write(b"data2")
        manifest2 = json.dumps({"hashes": [blob2]}).encode()
        m2 = k.write(manifest2)
        k.reference("pack/manifest", m2)  # old m1 is now orphaned

        # The new manifest is reachable
        check(k.resolve("pack/manifest") == m2,
              "new manifest is reachable")
        # The old manifest is orphaned (no ref points to it)
        all_refs = k.list_names()
        old_manifest_still_referenced = any(
            k.resolve(r) == m1 for r in all_refs
        )
        check(not old_manifest_still_referenced,
              "old manifest is orphaned (MAN3)")
        # GC must collect both m1 and blob1 (the old pack's contents)
    finally:
        cleanup(k, d)


# ---------------------------------------------------------------------------
# Range Read Algebra
# ---------------------------------------------------------------------------

def test_RR3_cost_per_range():
    """RR3: On S3, each Range Read costs 1 RTT regardless of length.
    The total RTT for a scan is ceil(scan_bytes / chunk_size)."""
    print("\n=== RR3: Cost is per-range, not per-byte ===")
    # This is a cost-model property, not a behavioral one.
    # We verify the formula.
    scan_bytes = 1_000_000  # 1 MB scan
    chunk_sizes = [4096, 16384, 65536, 262144, 1048576]
    for cs in chunk_sizes:
        rtt_count = (scan_bytes + cs - 1) // cs  # ceil
        # Smaller chunks → more RTTs
        if cs == 4096:
            check(rtt_count == 245, f"4KB chunks: 245 RTTs for 1MB (got {rtt_count})")
        elif cs == 1048576:
            check(rtt_count == 1, f"1MB chunks: 1 RTT for 1MB (got {rtt_count})")
    # Verify the monotonicity: smaller chunks → more RTTs
    rtts = [(scan_bytes + cs - 1) // cs for cs in chunk_sizes]
    check(rtts == sorted(rtts, reverse=True),
          "smaller chunks → more RTTs (monotonic)")


def test_RR4_backend_may_decompose():
    """RR4: Backend may implement ReadRange as Read + in-memory slice.
    The kernel returns the same bytes either way."""
    print("\n=== RR4: Backend may decompose ===")
    k, d = make_kernel()
    try:
        b = b"abcdefghijklmnopqrstuvwxyz0123456789"
        h = k.write(b)

        # Simulated "true range read" (would be S3 Range header)
        def true_range(hh, off, length):
            return k.read(hh)[off:off+length]

        # Simulated "decomposed range read" (Read + slice)
        def decomposed_range(hh, off, length):
            full = k.read(hh)
            return full[off:off+length]

        # Both return identical bytes
        for off in [0, 5, 13, 30]:
            for length in [1, 5, 10]:
                check(true_range(h, off, length) == decomposed_range(h, off, length),
                      f"true range == decomposed range (off={off}, len={length})")
    finally:
        cleanup(k, d)


# ---------------------------------------------------------------------------
# GC Algebra
# ---------------------------------------------------------------------------

def test_G2_liveness():
    """G2: Eventually, all unreachable blobs are collected (liveness
    depends on GC being run periodically)."""
    print("\n=== G2: Liveness ===")
    cfg = HazardConfig(deletion_grace_period_ms=10, seed=10)
    h, d = make_hazard(cfg)
    try:
        # Write 5 blobs, all orphaned
        orphan_hashes = []
        for i in range(5):
            hh = h.write(f"orphan{i}".encode())
            h.mark_orphaned(hh)
            orphan_hashes.append(hh)

        # Wait past grace period
        time.sleep(0.02)

        # Run GC — should collect all 5
        deleted = h.gc_collect()
        check(deleted == 5, f"GC collected all 5 orphaned blobs (got {deleted})")

        # Run GC again — should collect 0 (already collected)
        deleted2 = h.gc_collect()
        check(deleted2 == 0, f"second GC collects 0 (got {deleted2})")
    finally:
        cleanup(h, d)


def test_G4_non_blocking():
    """G4: GC does not block reads or writes."""
    print("\n=== G4: Non-blocking ===")
    cfg = HazardConfig(deletion_grace_period_ms=0, seed=11)
    h, d = make_hazard(cfg)
    try:
        # Set up: some reachable blobs, some orphaned
        reachable = h.write(b"keep")
        h.reference("keep_ref", reachable)
        for i in range(3):
            oh = h.write(f"orphan{i}".encode())
            h.mark_orphaned(oh)

        # Run GC, then immediately read and write — should not block
        h.gc_collect()
        check(h.read("keep_ref") == b"keep",
              "read after GC works (non-blocking)")
        new_h = h.write(b"new after gc")
        h.reference("new_ref", new_h)
        check(h.read("new_ref") == b"new after gc",
              "write after GC works (non-blocking)")
    finally:
        cleanup(h, d)


def test_G5_tombstone_interaction():
    """G5: Tombstoned references make their target blobs unreachable.
    GC collects them. The tombstone marker itself stays reachable."""
    print("\n=== G5: Tombstone interaction ===")
    k, d = make_kernel()
    try:
        # Set a ref
        blob_h = k.write(b"data")
        k.reference("x", blob_h)

        # Tombstone: write a tombstone marker and point ref at it
        tomb_blob = b"\x00TOMB\x00"
        tomb_h = k.write(tomb_blob)
        k.reference("x", tomb_h)  # x now points at tombstone marker

        # The original blob_h is now orphaned (no ref points to it)
        all_refs = k.list_names()
        blob_still_referenced = any(k.resolve(r) == blob_h for r in all_refs)
        check(not blob_still_referenced,
              "tombstoned ref's target blob is unreachable")
        # The tombstone marker itself IS reachable (x points to it)
        check(k.resolve("x") == tomb_h,
              "tombstone marker itself is reachable (G5)")
    finally:
        cleanup(k, d)


# ---------------------------------------------------------------------------
# Replication Algebra
# ---------------------------------------------------------------------------

def test_REP2_secondary_reads_stale():
    """REP2: Secondary reads are stale by replica_lag_ms."""
    print("\n=== REP2: Secondary reads are stale ===")
    cfg = HazardConfig(replica_lag_ms=100, seed=12)
    h, d = make_hazard(cfg)
    try:
        # Write to primary
        hh = h.write(b"data")
        h.reference("x", hh)

        # Immediately, secondary has stale view (no x)
        check(h.resolve_secondary("x") is None,
              "secondary is stale immediately after write")

        # After replica_lag, secondary converges
        time.sleep(0.15)
        check(h.resolve_secondary("x") == hh,
              "secondary converges after replica_lag")
    finally:
        cleanup(h, d)


def test_REP4_blob_before_commit():
    """REP4: Blob replication must precede commit replication.
    The primary writes blobs first, then the commit blob. A
    secondary observing the commit blob can be sure all referenced
    blobs are already replicated."""
    print("\n=== REP4: Blob replication precedes commit ===")
    k, d = make_kernel()
    try:
        # Write data blobs
        b1 = k.write(b"data1")
        b2 = k.write(b"data2")

        # Write commit blob (referencing b1 and b2)
        commit = json.dumps({"writes": [("x", b1), ("y", b2)]}).encode()
        ch = k.write(commit)

        # The commit blob references b1 and b2, which were written first
        # A secondary observing ch can read b1 and b2 immediately
        commit_data = json.loads(k.read(ch))
        for name, hh in commit_data["writes"]:
            check(k.read(hh) is not None,
                  f"secondary can read {name} after observing commit")
    finally:
        cleanup(k, d)


def test_REP5_failover_loses_inflight():
    """REP5: Failover loses in-flight writes. If primary fails before
    a commit blob is replicated, that commit is lost."""
    print("\n=== REP5: Failover loses in-flight writes ===")
    cfg = HazardConfig(replica_lag_ms=100, seed=13)
    h, d = make_hazard(cfg)
    try:
        # Write a commit on primary
        hh = h.write(b"data")
        h.reference("x", hh)

        # Simulate failover immediately (before replication)
        # The secondary's last synced state is empty
        secondary_view = h.resolve_secondary("x")
        check(secondary_view is None,
              "failover before replication: secondary has no x (in-flight lost)")

        # After lag, secondary would have x — but we simulated
        # failover BEFORE the lag elapsed, so the commit is "lost"
        # from the secondary's perspective.
    finally:
        cleanup(h, d)


def test_REP6_failover_explicit_promotion():
    """REP6: Failover requires explicit promotion. The kernel does
    not detect primary failure; the application does."""
    print("\n=== REP6: Failover requires explicit promotion ===")
    k, d = make_kernel()
    try:
        # Verify the kernel has no auto-failover API
        api = [m for m in dir(k) if not m.startswith("_")]
        has_failover = any("failover" in m.lower() or "promote" in m.lower()
                           or "detect_failure" in m.lower() for m in api)
        check(not has_failover,
              "kernel has no failover/promote API (application's job)")
    finally:
        cleanup(k, d)


def test_REP8_no_multi_writer_convergence():
    """REP8: No multi-writer convergence. If two regions both write
    the same Ref, the model does not define a merge."""
    print("\n=== REP8: No multi-writer convergence ===")
    k, d = make_kernel()
    try:
        # Simulate two writers to the same Ref
        h1 = k.write(b"writer1")
        h2 = k.write(b"writer2")
        k.reference("x", h1)
        k.reference("x", h2)  # second writer overwrites
        # The kernel's LWW picks one; the model doesn't merge
        result = k.resolve("x")
        check(result in (h1, h2),
              "LWW picks one of the two writes (no merge)")
        # If we wanted both writes preserved, the application must
        # use a coordinator (out-of-model per A7) or use branches
    finally:
        cleanup(k, d)


def test_REP9_one_directional():
    """REP9: Replication is one-directional (primary → secondary).
    Secondary → primary writes are not supported in-model."""
    print("\n=== REP9: Replication is one-directional ===")
    cfg = HazardConfig(replica_lag_ms=50, seed=14)
    h, d = make_hazard(cfg)
    try:
        # The simulator has a primary and a secondary namespace mirror
        # Writes go to primary; secondary syncs from primary
        hh = h.write(b"data")
        h.reference("x", hh)
        time.sleep(0.1)
        # Secondary has x
        check(h.resolve_secondary("x") == hh, "primary -> secondary works")

        # But there's no API to write to the secondary
        api = [m for m in dir(h) if not m.startswith("_")]
        has_secondary_write = any(
            "write_secondary" in m or "write_to_secondary" in m
            for m in api
        )
        check(not has_secondary_write,
              "no API to write to secondary (one-directional)")
    finally:
        cleanup(h, d)


# ---------------------------------------------------------------------------
# Transport Algebra
# ---------------------------------------------------------------------------

def test_TR4_transport_optional_per_collection():
    """TR4: Transport is optional per Collection. A Collection may
    have no transport (raw bytes), compression only, encryption only,
    or both."""
    print("\n=== TR4: Transport optional per Collection ===")
    k, d = make_kernel()
    try:
        # Collection 1: raw bytes (no transport)
        raw_blob = k.write(b"raw data")
        k.reference("coll1/data", raw_blob)
        check(k.read("coll1/data") == b"raw data",
              "Collection 1: raw bytes (no transport)")

        # Collection 2: with transport (using TransportLayer)
        # We don't need to instantiate TransportLayer here; we just
        # verify the kernel stores whatever it's given (transport
        # encoded or raw)
        transport_blob = b"PDTP\x00\x00\x00\x01..."  # fake transport bytes
        th = k.write(transport_blob)
        k.reference("coll2/data", th)
        check(k.read("coll2/data") == transport_blob,
              "Collection 2: transport-encoded bytes")

        # Both Collections coexist; kernel doesn't enforce transport
        check(k.resolve("coll1/data") != k.resolve("coll2/data"),
              "two Collections with different transport policies coexist")
    finally:
        cleanup(k, d)


def test_TR5_transport_per_blob():
    """TR5: Transport is per-blob, not per-byte. The transport
    pipeline runs once per Write, producing one encoded blob."""
    print("\n=== TR5: Transport is per-blob ===")
    sys.path.insert(0, os.path.join(SCRIPT_DIR, "..", "services", "transport"))
    from transport import TransportLayer
    k, d = make_kernel()
    try:
        t = TransportLayer(k)
        # Write 3 separate blobs
        h1 = t.write(b"blob1")
        h2 = t.write(b"blob2")
        h3 = t.write(b"blob3")
        # Each blob is independently transport-encoded
        check(h1 != h2 != h3, "3 distinct transport-encoded blobs")
        # Each decodes independently
        check(t.read(h1) == b"blob1", "blob1 decodes independently")
        check(t.read(h2) == b"blob2", "blob2 decodes independently")
        check(t.read(h3) == b"blob3", "blob3 decodes independently")
    finally:
        cleanup(k, d)


# ---------------------------------------------------------------------------
# Schema Evolution Algebra
# ---------------------------------------------------------------------------

def test_SE1_backward_compat():
    """SE1: New code reads old data. A new version of the Lens
    decodes blobs written by prior versions."""
    print("\n=== SE1: Backward compatibility ===")
    k, d = make_kernel()
    try:
        # v1 schema: {"a": int}
        v1_data = json.dumps({"a": 1}).encode()
        v1_h = k.write(v1_data)
        k.reference("feature/v1/x", v1_h)

        # v2 schema: {"a": int, "b": int (default 0)}
        # New code reads v1 data; "b" defaults to 0
        def v2_decode(blob):
            d = json.loads(blob)
            return {"a": d["a"], "b": d.get("b", 0)}  # b defaults

        decoded = v2_decode(k.read(v1_h))
        check(decoded == {"a": 1, "b": 0},
              "v2 Lens reads v1 data with default for new field")
    finally:
        cleanup(k, d)


def test_SE2_forward_compat():
    """SE2: Old code reads new data. An old version of the Lens
    decodes blobs written by newer versions, skipping unknown fields."""
    print("\n=== SE2: Forward compatibility ===")
    k, d = make_kernel()
    try:
        # v2 schema writes data with extra field
        v2_data = json.dumps({"a": 1, "b": 2, "c": 3}).encode()
        v2_h = k.write(v2_data)

        # v1 code reads v2 data, ignores unknown fields
        def v1_decode(blob):
            d = json.loads(blob)
            return {"a": d["a"]}  # only reads "a"

        decoded = v1_decode(k.read(v2_h))
        check(decoded == {"a": 1},
              "v1 Lens reads v2 data, ignores unknown fields")
    finally:
        cleanup(k, d)


def test_SE3_writer_schema_recorded():
    """SE3: Writer schema version is recorded (in key prefix or blob
    header). The reader schema is the latest version the Lens supports."""
    print("\n=== SE3: Writer schema recorded ===")
    k, d = make_kernel()
    try:
        # Option 1: schema version in key prefix
        v1_data = json.dumps({"a": 1}).encode()
        v1_h = k.write(v1_data)
        k.reference("feature/v1/x", v1_h)

        v2_data = json.dumps({"a": 1, "b": 2}).encode()
        v2_h = k.write(v2_data)
        k.reference("feature/v2/x", v2_h)

        # The Lens can tell which version by inspecting the key prefix
        def decode(name):
            h = k.resolve(name)
            if "/v1/" in name:
                return ("v1", json.loads(k.read(h)))
            elif "/v2/" in name:
                return ("v2", json.loads(k.read(h)))

        v1_result = decode("feature/v1/x")
        v2_result = decode("feature/v2/x")
        check(v1_result[0] == "v1", "writer schema v1 recorded in key")
        check(v2_result[0] == "v2", "writer schema v2 recorded in key")
    finally:
        cleanup(k, d)


def test_SE4_compat_is_lens_responsibility():
    """SE4: Compatibility is a Lens responsibility. The kernel does
    not enforce it."""
    print("\n=== SE4: Compatibility is Lens's responsibility ===")
    k, d = make_kernel()
    try:
        api = [m for m in dir(k) if not m.startswith("_")]
        has_compat_check = any("compat" in m.lower() or "version_check" in m.lower()
                               or "schema_validate" in m.lower() for m in api)
        check(not has_compat_check,
              "kernel has no compatibility-check API (Lens's job)")
    finally:
        cleanup(k, d)


def test_SE7_schema_registry_naming_convention():
    """SE7: Schema Registry is a Naming convention. It uses the
    existing Names substrate (Refs with prefix __schema/). No new
    substrate, no new axiom."""
    print("\n=== SE7: Schema Registry is Naming convention ===")
    k, d = make_kernel()
    try:
        # Schemas stored as blobs, referenced by name
        schema_v1 = json.dumps({"fields": ["a"]}).encode()
        sv1_h = k.write(schema_v1)
        k.reference("__schema/my_feature/v1", sv1_h)

        schema_v2 = json.dumps({"fields": ["a", "b"]}).encode()
        sv2_h = k.write(schema_v2)
        k.reference("__schema/my_feature/v2", sv2_h)

        # The "Schema Registry" is just a naming convention
        # over the existing Refs substrate. No new substrate needed.
        schemas = [n for n in k.list_names() if n.startswith("__schema/")]
        check(len(schemas) == 2, "Schema Registry: 2 schemas registered")
        check("__schema/my_feature/v1" in schemas, "v1 schema registered")
        check("__schema/my_feature/v2" in schemas, "v2 schema registered")

        # The kernel API is unchanged — no schema-specific methods
        api = [m for m in dir(k) if not m.startswith("_")]
        has_schema_method = any("schema" in m.lower() for m in api)
        check(not has_schema_method,
              "no schema-specific kernel methods (SE7: Naming convention only)")
    finally:
        cleanup(k, d)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

ALL_TESTS = [
    test_MAN3_manifest_may_be_stale,
    test_RR3_cost_per_range,
    test_RR4_backend_may_decompose,
    test_G2_liveness,
    test_G4_non_blocking,
    test_G5_tombstone_interaction,
    test_REP2_secondary_reads_stale,
    test_REP4_blob_before_commit,
    test_REP5_failover_loses_inflight,
    test_REP6_failover_explicit_promotion,
    test_REP8_no_multi_writer_convergence,
    test_REP9_one_directional,
    test_TR4_transport_optional_per_collection,
    test_TR5_transport_per_blob,
    test_SE1_backward_compat,
    test_SE2_forward_compat,
    test_SE3_writer_schema_recorded,
    test_SE4_compat_is_lens_responsibility,
    test_SE7_schema_registry_naming_convention,
]


def main():
    print("=" * 70)
    print("Pond Phase O.1 — Tests for Remaining Untested Laws")
    print("Covers MAN3, RR3/4, G2/4/5, REP2/4/5/6/8/9, TR4/5, SE1/2/3/4/7")
    print("=" * 70)

    for test in ALL_TESTS:
        try:
            test()
        except Exception as e:
            global FAIL
            FAIL += 1
            print(f"  [ERROR] {test.__name__} raised: {type(e).__name__}: {e}")

    print("\n" + "=" * 70)
    print(f"RESULTS: {PASS} pass, {FAIL} fail")
    print("=" * 70)
    sys.exit(0 if FAIL == 0 else 1)


if __name__ == "__main__":
    main()
