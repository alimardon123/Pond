"""
Pond Object-Store Hazard Simulator (Phase L.1)

A wrapper around PondMinimal that injects the operational hazards
identified by the Second and Third Red Team Reviews. The wrapper
exposes the same API as PondMinimal (write, read, reference,
resolve, list_names) but behind the scenes:

  - read-after-write is eventually consistent (configurable lag)
  - list-after-put is eventually consistent (new refs may not
    appear in list_names() until the lag elapses)
  - writes can fail partially (multipart interrupted, commit blob
    written but data blobs not)
  - range reads can return partial data (network interruption)
  - replica lag: a "secondary" view of the namespace lags behind
    the primary by a configurable amount
  - tombstone races: GC may delete blobs that a slow reader is
    still trying to read
  - clock skew: time() returns a skewed clock per "region"

This is NOT a real distributed system. It is a deterministic
fault-injector for property tests. Every hazard is reproducible
by seeding the random generator.

Model laws to verify under hazard:
  - A1 (Immutability): Read(Write(b)) = b, even under partial
    failure (the simulator must not corrupt blobs)
  - A2 (Content-addressing): Write(b1) = Write(b2) ⟺ b1 = b2
  - A3 (Name mutability): Ref(name, h) is the only mutation
  - A4 (Referential integrity): Ref(name, h) requires h exists
  - A6 (Atomic commit blob): commit-blob atomicity
  - C1 (Ref eventual propagation): eventually, get(name) = h
  - C2 (Single-Ref atomicity): readers see old OR new, never mix
  - G6 (Tombstone barrier): GC respects deletion_grace_period
  - REP2 (Secondary reads are stale, but bounded)
"""

from __future__ import annotations

import os
import sys
import time
import json
import random
import shutil
import tempfile
import threading
from typing import Optional, Callable
from collections import defaultdict

# Make pond-core importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "pond-core"))
from pond_minimal import PondMinimal  # noqa: E402


class HazardConfig:
    """Configuration for the hazard simulator. All hazards are
    off by default; tests enable them explicitly."""

    def __init__(
        self,
        read_after_write_lag_ms: int = 0,
        list_after_put_lag_ms: int = 0,
        replica_lag_ms: int = 0,
        write_partial_failure_p: float = 0.0,
        read_partial_failure_p: float = 0.0,
        delete_race_p: float = 0.0,
        clock_skew_ms: int = 0,
        deletion_grace_period_ms: int = 0,
        seed: int = 42,
        enable_bookkeeping: bool = True,
        # Phase N.5: additional hazards
        partition_p: float = 0.0,
        disk_corruption_p: float = 0.0,
    ):
        # Eventual consistency
        self.read_after_write_lag_ms = read_after_write_lag_ms
        self.list_after_put_lag_ms = list_after_put_lag_ms
        self.replica_lag_ms = replica_lag_ms

        # Partial failure probabilities (0..1)
        self.write_partial_failure_p = write_partial_failure_p
        self.read_partial_failure_p = read_partial_failure_p
        self.delete_race_p = delete_race_p

        # Clock skew
        self.clock_skew_ms = clock_skew_ms

        # Deletion grace period (G6)
        self.deletion_grace_period_ms = deletion_grace_period_ms

        # Reproducibility
        self.seed = seed

        # Hazard bookkeeping
        self.enable_bookkeeping = enable_bookkeeping

        # Phase N.5: additional hazards
        self.partition_p = partition_p                 # network partition probability
        self.disk_corruption_p = disk_corruption_p     # silent disk corruption probability


class HazardSimulator:
    """
    A wrapper around PondMinimal that injects operational hazards.

    The wrapper maintains:
      - a primary PondMinimal instance (the "primary region")
      - an optional secondary namespace mirror (the "secondary region")
        that lags behind the primary by replica_lag_ms
      - a per-blob write timestamp log (for G6 tombstone barrier)
      - a per-name ref update log (for C1/C2 verification)
      - a deterministic RNG (seeded) for reproducible hazard injection

    The API matches PondMinimal exactly so property tests can run
    against either the clean kernel or the hazard kernel.
    """

    def __init__(self, base_dir: str, config: Optional[HazardConfig] = None):
        self.config = config or HazardConfig()
        self.rng = random.Random(self.config.seed)

        self.primary = PondMinimal(base_dir)

        # Secondary namespace mirror — a dict mapping name -> (hash, ts)
        # that lags behind the primary by replica_lag_ms
        self._secondary_ns: dict[str, tuple[str, float]] = {}
        self._secondary_last_sync_ms = 0

        # Per-blob write timestamps (for G6)
        self._blob_write_ts: dict[str, float] = {}
        # Per-blob orphan timestamps (when the blob became unreachable)
        self._blob_orphan_ts: dict[str, float] = {}

        # Per-name ref update log (for C1/C2 verification)
        # {name: [(ts, hash), ...]}
        self._ref_log: dict[str, list[tuple[float, str]]] = defaultdict(list)

        # Bookkeeping counters
        self.hazard_events: dict[str, int] = defaultdict(int)

        # Clock skew state
        self._clock_skew_offset_ms = self.rng.uniform(
            -self.config.clock_skew_ms, self.config.clock_skew_ms
        )

    # ------------------------------------------------------------------
    # Clock (with skew)
    # ------------------------------------------------------------------

    def now_ms(self) -> float:
        """Returns current time in ms, with per-region skew applied."""
        return (time.time() * 1000.0) + self._clock_skew_offset_ms

    # ------------------------------------------------------------------
    # Write (with partial-failure hazard)
    # ------------------------------------------------------------------

    def write(self, data: bytes) -> str:
        """Write bytes. May inject partial-failure hazard:
        simulate a multipart interrupted: the blob is written but
        the function raises. The next write of the same bytes
        succeeds (dedup)."""
        h = None
        try:
            h = self.primary.write(data)
        except Exception:
            self.hazard_events["write_failure"] += 1
            raise

        if h:
            self._blob_write_ts[h] = self.now_ms()
            # Inject partial-failure hazard: pretend the multipart
            # was interrupted AFTER the blob is on disk but BEFORE
            # the caller receives the hash. The next write of the
            # same bytes succeeds (dedup) and the caller eventually
            # gets the hash.
            if self.rng.random() < self.config.write_partial_failure_p:
                self.hazard_events["write_partial_failure"] += 1
                raise IOError(
                    "simulated multipart interrupted (blob is on disk; "
                    "retry will dedup and succeed)"
                )

        # Phase N.5: partition hazard — the primary is unreachable
        # for writes. Caller must retry.
        if self.rng.random() < self.config.partition_p:
            self.hazard_events["partition"] += 1
            raise ConnectionError(
                "simulated network partition: primary unreachable for write"
            )
        return h

    # ------------------------------------------------------------------
    # Read (with eventual-consistency hazard)
    # ------------------------------------------------------------------

    def read(self, hash_or_name: str) -> bytes:
        """Read by hash or name. May inject:
        - read-after-write lag: a recently-written blob may not be
          visible to read() until read_after_write_lag_ms elapses.
        - partial read failure: the blob is truncated."""
        # Resolve name to hash first (so we can apply lag consistently)
        if not (len(hash_or_name) == 64 and all(
                c in "0123456789abcdef" for c in hash_or_name)):
            h = self.resolve(hash_or_name)
            if h is None:
                raise ValueError(f"Name '{hash_or_name}' not bound")
        else:
            h = hash_or_name

        # Read-after-write lag
        if self.config.read_after_write_lag_ms > 0:
            age_ms = self.now_ms() - self._blob_write_ts.get(h, 0)
            if age_ms < self.config.read_after_write_lag_ms:
                self.hazard_events["read_after_write_lag"] += 1
                # The blob exists on disk but the read returns
                # "not found" (simulating eventual consistency)
                raise FileNotFoundError(
                    f"simulated read-after-write lag: blob {h[:8]} is "
                    f"{age_ms:.0f}ms old, lag is "
                    f"{self.config.read_after_write_lag_ms}ms"
                )

        # Partial read failure
        if self.rng.random() < self.config.read_partial_failure_p:
            self.hazard_events["read_partial_failure"] += 1
            data = self.primary.read_blob(h)
            # Truncate at a random point
            trunc = self.rng.randint(1, max(1, len(data) - 1))
            return data[:trunc]

        # Phase N.5: partition hazard — the primary is unreachable
        # for reads. Caller must retry or failover to secondary.
        if self.rng.random() < self.config.partition_p:
            self.hazard_events["partition"] += 1
            raise ConnectionError(
                "simulated network partition: primary unreachable for read"
            )

        # Phase N.5: disk corruption hazard — the blob on disk is
        # silently corrupted (one byte flipped). The kernel returns
        # corrupted bytes; the caller (or Lens) must verify against
        # the content-addressed hash and detect the mismatch.
        if self.rng.random() < self.config.disk_corruption_p:
            self.hazard_events["disk_corruption"] += 1
            data = bytearray(self.primary.read_blob(h))
            if len(data) > 0:
                # Flip one random byte
                idx = self.rng.randint(0, len(data) - 1)
                data[idx] ^= 0x01
            return bytes(data)

        return self.primary.read_blob(h)

    def read_blob(self, h: str) -> bytes:
        return self.read(h)

    # ------------------------------------------------------------------
    # Reference (with eventual-consistency on the namespace)
    # ------------------------------------------------------------------

    def reference(self, name: str, h: str) -> None:
        """Set name -> h. Records the update in the ref log for
        C1/C2 verification. The primary namespace is updated
        synchronously; the secondary namespace is updated after
        replica_lag_ms."""
        # A4: referential integrity — the blob must exist
        if h not in self._blob_write_ts:
            # The kernel itself will check; we just record
            pass

        self.primary.reference(name, h)

        ts = self.now_ms()
        self._ref_log[name].append((ts, h))

    def resolve(self, name: str) -> Optional[str]:
        """Resolve a name on the primary."""
        return self.primary.resolve(name)

    def resolve_secondary(self, name: str) -> Optional[str]:
        """Resolve a name on the secondary (may be stale)."""
        self._maybe_sync_secondary()
        if name in self._secondary_ns:
            return self._secondary_ns[name][0]
        return None

    def list_names(self) -> list[str]:
        """List all names. May be stale by list_after_put_lag_ms."""
        all_names = self.primary.list_names()
        if self.config.list_after_put_lag_ms == 0:
            return all_names

        # Filter out names that were updated less than lag_ms ago
        result = []
        for name in all_names:
            if name not in self._ref_log:
                result.append(name)
                continue
            last_ts = self._ref_log[name][-1][0]
            age_ms = self.now_ms() - last_ts
            if age_ms >= self.config.list_after_put_lag_ms:
                result.append(name)
            else:
                self.hazard_events["list_after_put_lag"] += 1
        return result

    # ------------------------------------------------------------------
    # GC (with tombstone barrier — G6)
    # ------------------------------------------------------------------

    def mark_orphaned(self, h: str) -> None:
        """Mark a blob as orphaned (no Ref points to it). The blob
        will be eligible for deletion after deletion_grace_period_ms."""
        self._blob_orphan_ts[h] = self.now_ms()

    def gc_collect(self) -> int:
        """Delete orphaned blobs whose grace period has elapsed.
        Returns the number of blobs deleted."""
        deleted = 0
        now = self.now_ms()
        for h, orphan_ts in list(self._blob_orphan_ts.items()):
            age_ms = now - orphan_ts
            if age_ms < self.config.deletion_grace_period_ms:
                # G6 tombstone barrier: respect grace period
                continue
            # Inject delete-race hazard: pretend a reader is mid-read
            if self.rng.random() < self.config.delete_race_p:
                self.hazard_events["delete_race"] += 1
                # Skip this deletion (the blob is preserved)
                continue
            # Actually delete
            path = self.primary._blob_path(h)
            if os.path.exists(path):
                os.remove(path)
                deleted += 1
            del self._blob_orphan_ts[h]
        return deleted

    # ------------------------------------------------------------------
    # Secondary sync (replica lag)
    # ------------------------------------------------------------------

    def _maybe_sync_secondary(self) -> None:
        """Sync the secondary namespace with the primary, up to
        replica_lag_ms ago."""
        now = self.now_ms()
        cutoff = now - self.config.replica_lag_ms
        if cutoff <= self._secondary_last_sync_ms:
            return
        # Apply all ref updates older than cutoff
        for name, log in self._ref_log.items():
            for ts, h in log:
                if ts <= cutoff:
                    self._secondary_ns[name] = (h, ts)
        self._secondary_last_sync_ms = cutoff

    # ------------------------------------------------------------------
    # Verification helpers (used by property tests)
    # ------------------------------------------------------------------

    def ref_history(self, name: str) -> list[tuple[float, str]]:
        """Return the full ref update history for a name."""
        return list(self._ref_log.get(name, []))

    def hazard_count(self, event: str) -> int:
        return self.hazard_events.get(event, 0)

    def reset_hazard_counts(self) -> None:
        self.hazard_events.clear()

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------

    def close(self) -> None:
        self.primary.close()

    def storage_stats(self) -> dict:
        return self.primary.storage_stats()


# ---------------------------------------------------------------------------
# Self-test
# ---------------------------------------------------------------------------

def _self_test():
    """Quick smoke test: verify the simulator wraps the kernel
    correctly with no hazards enabled."""
    tmpdir = tempfile.mkdtemp()
    try:
        sim = HazardSimulator(tmpdir)
        h1 = sim.write(b"hello")
        assert sim.read(h1) == b"hello"
        sim.reference("greeting", h1)
        assert sim.resolve("greeting") == h1
        assert sim.read("greeting") == b"hello"
        print("[OK] no-hazard baseline:", h1[:8])

        # Enable read-after-write lag
        sim2 = HazardSimulator(tmpdir + "_lag", HazardConfig(
            read_after_write_lag_ms=50,
        ))
        h2 = sim2.write(b"world")
        try:
            sim2.read(h2)
            print("[FAIL] read-after-write lag not injected")
        except FileNotFoundError:
            print("[OK] read-after-write lag injected")
        time.sleep(0.1)
        assert sim2.read(h2) == b"world"
        print("[OK] read-after-write lag cleared after sleep")
        sim2.close()

        sim.close()
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)
        shutil.rmtree(tmpdir + "_lag", ignore_errors=True)


if __name__ == "__main__":
    _self_test()
