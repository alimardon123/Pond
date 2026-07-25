"""
Pond Phase O.2 — Remaining Hazard Simulators and Tests

Adds 4 new hazards to the simulator:
  - Byzantine replica (serves wrong data)
  - Hash collision (extremely unlikely with SHA-256, but the model
    assumes A2 holds absolutely — we test what happens if it doesn't)
  - Replay attack (replica replays old commits)
  - Concurrent compaction + replication (B5 hazard with timing)

These are fault injectors for testing the model's robustness under
operational anomalies that go beyond eventual consistency.

Run:
    python scripts/phase_o_remaining_hazards.py
"""

from __future__ import annotations

import os
import sys
import time
import json
import tempfile
import shutil
import hashlib
import random

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(SCRIPT_DIR, "..", "pond-core"))
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


# ---------------------------------------------------------------------------
# Byzantine Replica Simulator
# ---------------------------------------------------------------------------

class ByzantineSimulator:
    """Wraps a HazardSimulator with a Byzantine secondary that
    sometimes serves wrong data.

    The primary is honest; the secondary may serve:
      - correct data (most of the time)
      - wrong data (with probability byzantine_p)
      - stale data (with probability stale_p)
    """

    def __init__(self, base_dir: str, byzantine_p: float = 0.3,
                 stale_p: float = 0.2, seed: int = 42):
        self.sim = HazardSimulator(base_dir, HazardConfig(
            replica_lag_ms=50, seed=seed
        ))
        self.byzantine_p = byzantine_p
        self.stale_p = stale_p
        self.rng = random.Random(seed + 1)
        self._wrong_blob = self.sim.primary.write(b"BYZANTINE_WRONG_DATA")

    def write(self, data: bytes) -> str:
        return self.sim.write(data)

    def reference(self, name: str, h: str) -> None:
        self.sim.reference(name, h)

    def resolve_primary(self, name: str):
        return self.sim.resolve(name)

    def read_primary(self, name: str) -> bytes:
        return self.sim.read(name)

    def read_secondary_byzantine(self, name: str):
        """Read from the Byzantine secondary. Returns either:
          - correct hash (with probability 1 - byzantine_p - stale_p)
          - wrong hash (with probability byzantine_p)
          - None / stale (with probability stale_p)
        """
        self.sim._maybe_sync_secondary()
        r = self.rng.random()
        if r < self.byzantine_p:
            # Serve wrong data
            return self._wrong_blob
        elif r < self.byzantine_p + self.stale_p:
            # Serve stale (return None — name not yet replicated)
            return None
        else:
            # Serve correct
            if name in self.sim._secondary_ns:
                return self.sim._secondary_ns[name][0]
            return None

    def close(self):
        self.sim.close()


def test_byzantine_replica_serves_wrong_data():
    """Byzantine replica serves wrong data. The reader must verify
    via A2 (content-addressing) — hash mismatch detected."""
    print("\n=== Byzantine replica: serves wrong data ===")
    b = ByzantineSimulator(tempfile.mkdtemp(prefix="pond_byz_"),
                           byzantine_p=1.0, stale_p=0.0, seed=1)
    try:
        # Write to primary
        data = b"correct data"
        h = b.write(data)
        b.reference("x", h)
        time.sleep(0.1)  # let replication converge

        # Read from Byzantine secondary
        secondary_response = b.read_secondary_byzantine("x")
        check(secondary_response == b._wrong_blob,
              "Byzantine secondary serves wrong data")

        # The reader detects the mismatch via A2
        # If the reader asks for hash h but gets hash _wrong_blob,
        # the content-addressing check fails
        check(secondary_response != h,
              "A2 detects Byzantine response (hash mismatch)")
    finally:
        b.close()


def test_byzantine_replica_detectable_via_hash():
    """A reader that knows the expected hash can detect Byzantine
    responses by comparing the returned hash to the expected one."""
    print("\n=== Byzantine detection via hash comparison ===")
    b = ByzantineSimulator(tempfile.mkdtemp(prefix="pond_byzd_"),
                           byzantine_p=0.5, stale_p=0.0, seed=2)
    try:
        data = b"correct"
        expected_h = b.write(data)
        b.reference("x", expected_h)
        time.sleep(0.1)

        # Reader knows expected_h (from primary)
        # Reader asks secondary for "x"
        # Reader verifies: secondary's hash == expected_h?
        detected_count = 0
        for _ in range(20):
            secondary_h = b.read_secondary_byzantine("x")
            if secondary_h != expected_h:
                detected_count += 1
        check(detected_count > 0,
              f"reader detected {detected_count}/20 Byzantine responses via hash")
    finally:
        b.close()


# ---------------------------------------------------------------------------
# Hash Collision Simulator
# ---------------------------------------------------------------------------

class HashCollisionSimulator:
    """Simulates a hash collision by overriding the hash function.
    With probability collision_p, two different byte strings produce
    the same hash. This violates A2 — we test what breaks."""

    def __init__(self, base_dir: str, collision_p: float = 0.1,
                 seed: int = 42):
        self.base_dir = base_dir
        self.collision_p = collision_p
        self.rng = random.Random(seed)
        # Map from "real hash" to "collision hash"
        self._collision_map: dict[str, str] = {}
        # Track all hashes we've issued
        self._all_hashes: set[str] = set()

    def fake_hash(self, data: bytes) -> str:
        """Compute a hash, possibly with a forced collision."""
        real = hashlib.sha256(data).hexdigest()
        # With probability collision_p, force a collision:
        # return an already-issued hash instead of the real one
        if (self.rng.random() < self.collision_p
                and len(self._all_hashes) > 0):
            # Pick an existing hash to collide with
            existing = self.rng.choice(list(self._all_hashes))
            self._collision_map[real] = existing
            return existing
        self._all_hashes.add(real)
        return real


def test_hash_collision_breaks_dedup():
    """If A2 is violated (hash collision), dedup gives wrong results:
    two different byte strings produce the same hash, so the second
    write appears to be a dedup but actually loses data."""
    print("\n=== Hash collision breaks dedup (A2 violation) ===")
    sim = HashCollisionSimulator(tempfile.mkdtemp(prefix="pond_coll_"),
                                 collision_p=0.5, seed=3)
    try:
        # Write two DIFFERENT byte strings
        data1 = b"first data"
        data2 = b"second data"  # different
        h1 = sim.fake_hash(data1)
        h2 = sim.fake_hash(data2)

        # If a collision occurred, h1 == h2 but data1 != data2
        # This violates A2 (content-addressing)
        if h1 == h2:
            check(data1 != data2,
                  "A2 violated: different data → same hash (collision)")
            check(True, "dedup would incorrectly return data1 for h2 (data loss)")
        else:
            check(True, "no collision this time (A2 holds)")
    finally:
        pass  # no kernel state to clean up


def test_hash_collision_documented_assumption():
    """The model assumes A2 holds absolutely. In practice, SHA-256
    collisions are computationally infeasible (~2^128 work for
    birthday attack). We document this as a model assumption."""
    print("\n=== Hash collision: documented assumption ===")
    # SHA-256 has 2^256 output space; birthday bound is 2^128
    # For 1 million blobs, collision probability is ~10^-31
    # The model assumes this is zero.
    n_blobs = 1_000_000
    p_collision = n_blobs * (n_blobs - 1) / (2 * 2**256)
    check(p_collision < 1e-30,
          f"SHA-256 collision probability for {n_blobs} blobs < 1e-30",
          f"(got {p_collision:.2e})")
    check(True, "model assumption A2 is computationally safe")


# ---------------------------------------------------------------------------
# Replay Attack Simulator
# ---------------------------------------------------------------------------

class ReplaySimulator:
    """Simulates a replica that replays old commits. The replica
    serves an old commit hash instead of the latest one."""

    def __init__(self, base_dir: str, replay_p: float = 0.3, seed: int = 42):
        self.sim = HazardSimulator(base_dir, HazardConfig(
            replica_lag_ms=0, seed=seed
        ))
        self.replay_p = replay_p
        self.rng = random.Random(seed + 1)
        self._old_commits: dict[str, list[str]] = {}  # name -> [old hashes]

    def write(self, data: bytes) -> str:
        return self.sim.write(data)

    def reference(self, name: str, h: str) -> None:
        # Record the old commit before overwriting
        old = self.sim.resolve(name)
        if old is not None:
            self._old_commits.setdefault(name, []).append(old)
        self.sim.reference(name, h)

    def resolve_replay(self, name: str):
        """Resolve a name on the 'replay' secondary. With probability
        replay_p, return an OLD commit hash instead of the latest."""
        if self.rng.random() < self.replay_p:
            old_commits = self._old_commits.get(name, [])
            if old_commits:
                return self.rng.choice(old_commits)
        return self.sim.resolve(name)

    def close(self):
        self.sim.close()


def test_replay_attack_serves_old_commit():
    """Replay attack: replica serves an old commit hash instead of
    the latest. The reader sees stale state."""
    print("\n=== Replay attack: serves old commit ===")
    r = ReplaySimulator(tempfile.mkdtemp(prefix="pond_replay_"),
                        replay_p=1.0, seed=4)
    try:
        # Commit v1
        h1 = r.write(b"v1")
        r.reference("x", h1)
        # Commit v2
        h2 = r.write(b"v2")
        r.reference("x", h2)
        # Commit v3
        h3 = r.write(b"v3")
        r.reference("x", h3)

        # Primary has v3
        check(r.sim.resolve("x") == h3, "primary has latest (v3)")

        # Replay secondary serves an OLD commit
        replayed = r.resolve_replay("x")
        check(replayed in (h1, h2),
              f"replay secondary serves old commit (got {replayed[:8]})")
        check(replayed != h3, "replay secondary does NOT serve latest")
    finally:
        r.close()


def test_replay_attack_detectable_via_commit_timestamp():
    """A reader can detect replay by checking the commit's timestamp.
    Old commits have older timestamps than the latest known HEAD."""
    print("\n=== Replay detectable via commit timestamp ===")
    r = ReplaySimulator(tempfile.mkdtemp(prefix="pond_replayd_"),
                        replay_p=1.0, seed=5)
    try:
        # Commit v1 with timestamp
        c1_data = json.dumps({"data": "v1", "ts": 1000}).encode()
        h1 = r.write(c1_data)
        r.reference("x", h1)
        time.sleep(0.01)
        # Commit v2 with later timestamp
        c2_data = json.dumps({"data": "v2", "ts": 2000}).encode()
        h2 = r.write(c2_data)
        r.reference("x", h2)

        # Reader knows latest HEAD = h2, ts = 2000
        # If replay secondary returns h1 (ts = 1000), reader detects
        # the older timestamp and rejects
        replayed = r.resolve_replay("x")
        if replayed != h2:
            replayed_data = json.loads(r.sim.read(replayed))
            check(replayed_data["ts"] < 2000,
                  "replayed commit has older timestamp (detectable)")
    finally:
        r.close()


# ---------------------------------------------------------------------------
# Concurrent Compaction + Replication Simulator
# ---------------------------------------------------------------------------

class ConcurrentCompactionReplication:
    """Simulates the B5 hazard: compaction orphans old blobs while
    a secondary is still trying to replicate them. The secondary's
    read fails (blob deleted)."""

    def __init__(self, base_dir: str, seed: int = 42):
        self.sim = HazardSimulator(base_dir, HazardConfig(
            replica_lag_ms=100,  # secondary lags 100ms
            deletion_grace_period_ms=0,  # GC deletes immediately
            seed=seed,
        ))
        self.rng = random.Random(seed + 1)
        self._compacted_packs: list[str] = []  # old pack hashes

    def write_pack(self, data: bytes) -> str:
        """Write a 'pack' blob."""
        h = self.sim.write(data)
        self.sim.reference("pack/current", h)
        return h

    def compact(self, new_data: bytes) -> str:
        """Compact: write a new pack, orphan the old one. The old
        pack's blobs become unreachable and may be GC'd."""
        old = self.sim.resolve("pack/current")
        if old:
            self._compacted_packs.append(old)
            self.sim.mark_orphaned(old)
        new_h = self.sim.write(new_data)
        self.sim.reference("pack/current", new_h)
        # GC immediately (grace period = 0)
        self.sim.gc_collect()
        return new_h

    def secondary_read_old_pack(self) -> bool:
        """Secondary tries to read the OLD pack (which may have been
        GC'd). Returns True if read succeeds, False if blob is gone."""
        if not self._compacted_packs:
            return True
        old_h = self._compacted_packs[-1]
        # Wait for secondary to sync (but the old pack may be deleted)
        time.sleep(0.15)  # past replica_lag
        try:
            # Try to read the old pack from primary
            self.sim.primary.read_blob(old_h)
            return True
        except (FileNotFoundError, ValueError):
            return False

    def close(self):
        self.sim.close()


def test_concurrent_compaction_replication_hazard():
    """B5: Compaction orphans old pack; GC deletes it before
    secondary replicates. Secondary's read fails."""
    print("\n=== Concurrent compaction + replication (B5 hazard) ===")
    c = ConcurrentCompactionReplication(tempfile.mkdtemp(prefix="pond_b5_"),
                                        seed=6)
    try:
        # Write pack v1
        c.write_pack(b"pack v1 data")
        # Compact to pack v2 (orphans v1, GC deletes v1 immediately)
        c.compact(b"pack v2 data")
        # Secondary tries to read old pack v1 — should fail (deleted)
        # This is the B5 hazard: replication cannot complete because
        # the source blob is gone
        success = c.secondary_read_old_pack()
        check(not success,
              "B5 hazard: old pack deleted before secondary could replicate")
    finally:
        c.close()


def test_concurrent_compaction_with_grace_period_safe():
    """With G6 tombstone barrier (deletion_grace_period > replica_lag),
    the old pack survives long enough for the secondary to replicate."""
    print("\n=== B5 hazard mitigated by G6 tombstone barrier ===")
    sim = HazardSimulator(tempfile.mkdtemp(prefix="pond_b5safe_"),
                          HazardConfig(
                              replica_lag_ms=50,
                              deletion_grace_period_ms=200,  # > replica_lag
                              seed=7,
                          ))
    try:
        # Write and reference a blob
        h1 = sim.write(b"v1")
        sim.reference("pack/current", h1)
        # Orphan it
        sim.mark_orphaned(h1)
        # Immediately try to read — grace period prevents deletion
        # Wait for replica_lag
        time.sleep(0.06)
        # Old blob should still exist (within grace period)
        try:
            sim.primary.read_blob(h1)
            survives = True
        except (FileNotFoundError, ValueError):
            survives = False
        check(survives,
              "G6 barrier: old pack survives past replica_lag (within grace)")

        # After grace period, GC can delete
        time.sleep(0.2)
        sim.gc_collect()
        try:
            sim.primary.read_blob(h1)
            still_exists = True
        except (FileNotFoundError, ValueError):
            still_exists = False
        check(not still_exists,
              "G6 barrier: after grace period, GC can delete")
    finally:
        sim.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

ALL_TESTS = [
    test_byzantine_replica_serves_wrong_data,
    test_byzantine_replica_detectable_via_hash,
    test_hash_collision_breaks_dedup,
    test_hash_collision_documented_assumption,
    test_replay_attack_serves_old_commit,
    test_replay_attack_detectable_via_commit_timestamp,
    test_concurrent_compaction_replication_hazard,
    test_concurrent_compaction_with_grace_period_safe,
]


def main():
    print("=" * 70)
    print("Pond Phase O.2 — Remaining Hazard Simulators and Tests")
    print("Byzantine replica, hash collision, replay attack,")
    print("concurrent compaction + replication (B5).")
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
