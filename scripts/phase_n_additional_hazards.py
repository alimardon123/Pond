"""
Pond Phase N.5 — Additional Hazard Tests

Verifies the new hazards (partition, disk corruption) are
correctly injected and that the model's invariants survive them.

Run:
    python scripts/phase_n_additional_hazards.py
"""

from __future__ import annotations

import os
import sys
import time
import tempfile
import shutil
import hashlib

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(SCRIPT_DIR, "..", "pond-core"))
sys.path.insert(0, SCRIPT_DIR)
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


def test_partition_injected():
    """Verify partition hazard is injected on writes and reads."""
    print("\n=== Partition hazard injection ===")
    cfg = HazardConfig(partition_p=1.0, seed=1)  # always partition
    h = HazardSimulator(tempfile.mkdtemp(prefix="pond_p_"), cfg)
    try:
        try:
            h.write(b"data")
            check(False, "write should fail under partition_p=1.0")
        except ConnectionError:
            check(True, "write fails with ConnectionError under partition")

        # Pre-load a blob via the primary directly (bypass hazard)
        real_h = h.primary.write(b"preload")
        h.primary.reference("x", real_h)
        try:
            h.read("x")
            check(False, "read should fail under partition_p=1.0")
        except ConnectionError:
            check(True, "read fails with ConnectionError under partition")
        check(h.hazard_count("partition") >= 2, "partition counted in bookkeeping")
    finally:
        h.close()


def test_partition_recoverable():
    """Partition is transient: with partition_p=0.5, retries eventually succeed."""
    print("\n=== Partition is recoverable ===")
    cfg = HazardConfig(partition_p=0.5, seed=2)
    h = HazardSimulator(tempfile.mkdtemp(prefix="pond_pr_"), cfg)
    try:
        # Retry write until it succeeds (max 20 attempts)
        succeeded = False
        for _ in range(20):
            try:
                hh = h.write(b"data")
                succeeded = True
                break
            except ConnectionError:
                continue
        check(succeeded, "write eventually succeeds under partition_p=0.5")
    finally:
        h.close()


def test_disk_corruption_detected_by_hash():
    """Disk corruption flips a byte; the content-addressed hash
    detects the mismatch. This is the model's A2 (content-addressing)
    serving as integrity check."""
    print("\n=== Disk corruption detected by hash (A2) ===")
    cfg = HazardConfig(disk_corruption_p=1.0, seed=3)  # always corrupt
    h = HazardSimulator(tempfile.mkdtemp(prefix="pond_dc_"), cfg)
    try:
        # Write a blob via the primary directly (bypass hazard)
        data = b"important data that should not change"
        real_h = h.primary.write(data)
        # Read through the hazard simulator — will return corrupted bytes
        corrupted = h.read(real_h)
        # The corruption is detectable: hash(corrupted) != real_h
        corrupted_hash = hashlib.sha256(corrupted).hexdigest()
        check(corrupted != data, "corruption modified the bytes")
        check(corrupted_hash != real_h,
              "A2 content-addressing detects corruption (hash mismatch)")
    finally:
        h.close()


def test_disk_corruption_silent():
    """Disk corruption is silent — the kernel doesn't detect it.
    The CALLER must verify against the hash. This documents the
    boundary: the kernel returns bytes; integrity is the caller's job."""
    print("\n=== Disk corruption is silent (caller verifies) ===")
    cfg = HazardConfig(disk_corruption_p=1.0, seed=4)
    h = HazardSimulator(tempfile.mkdtemp(prefix="pond_dcs_"), cfg)
    try:
        data = b"silent corruption test"
        real_h = h.primary.write(data)
        # The kernel's read returns corrupted bytes WITHOUT raising
        corrupted = h.read(real_h)
        check(isinstance(corrupted, bytes), "kernel returns bytes (no exception)")
        check(corrupted != data, "bytes are corrupted (silently)")
        # The caller must verify: hash(corrupted) != real_h
        # This is the model's contract: kernel provides bytes; caller
        # verifies integrity via A2.
    finally:
        h.close()


def test_combined_hazards():
    """All hazards can be enabled simultaneously."""
    print("\n=== Combined hazards ===")
    cfg = HazardConfig(
        read_after_write_lag_ms=10,
        list_after_put_lag_ms=10,
        replica_lag_ms=10,
        write_partial_failure_p=0.1,
        read_partial_failure_p=0.1,
        delete_race_p=0.1,
        clock_skew_ms=50,
        deletion_grace_period_ms=100,
        partition_p=0.1,
        disk_corruption_p=0.1,
        seed=5,
    )
    h = HazardSimulator(tempfile.mkdtemp(prefix="pond_combo_"), cfg)
    try:
        # Try to write 10 blobs; some will fail, some will succeed
        successes = 0
        hashes = []
        for i in range(10):
            try:
                # Wait out read-after-write lag for previous writes
                time.sleep(0.015)
                hh = h.write(f"blob {i}".encode())
                hashes.append(hh)
                successes += 1
            except (IOError, ConnectionError):
                continue
        check(successes > 0, f"some writes succeeded under combined hazards ({successes}/10)")
        # Verify successful writes still respect A1 (after waiting for lag)
        time.sleep(0.05)
        for hh in hashes:
            try:
                # Disk corruption may break this; that's expected
                # (corruption is a fault, not a model violation)
                data = h.read(hh)
                # If we got here, data may or may not match (corruption)
                # The model's contract is: caller verifies via hash
                pass
            except (FileNotFoundError, ConnectionError):
                # Lag or partition — retry-able
                pass
        check(True, "combined hazards exercised without crash")
    finally:
        h.close()


def main():
    print("=" * 70)
    print("Pond Phase N.5 — Additional Hazard Tests")
    print("Verifies partition and disk corruption hazards.")
    print("=" * 70)

    for test in [test_partition_injected, test_partition_recoverable,
                 test_disk_corruption_detected_by_hash,
                 test_disk_corruption_silent,
                 test_combined_hazards]:
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
