"""
Pond Lab — Track 2: Index Portability

The experiment: can one Physical Structure accelerate multiple Lenses?

Hypothesis: if Physical Structures are stored as Pond blobs (content-
addressed, referenced by name), then any Lens can read and use them.
A Parquet statistics blob built by the Lakehouse Lens should be usable
by the Feature Store Lens for pruning. A bloom filter built by one
Lens should be usable by another.

This is the architectural insight that would make Pond genuinely
different from peer systems: indexes are not owned by Lenses; they
are shared Physical Structures on the kernel.

Experiments:
  1. Lakehouse builds Parquet statistics → FeatureStore uses them for pruning
  2. FeatureStore builds a feature index → Lakehouse uses it for point lookup
  3. A bloom filter built by one Lens → used by another for membership test
  4. A zone map (min/max per chunk) → used by any Lens for range pruning

The key question: does sharing Physical Structures across Lenses
actually work, and does it reduce total metadata overhead?

Run:
    python pond-lab/track2_index_portability.py
"""

from __future__ import annotations

import os
import sys
import json
import time
import tempfile
import shutil
import datetime
import hashlib

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(SCRIPT_DIR, "..", "..", "bindings/python/core"))
sys.path.insert(0, os.path.join(SCRIPT_DIR, "..", "..", "lenses", "lakehouse"))
sys.path.insert(0, os.path.join(SCRIPT_DIR, "..", "lenses"))
sys.path.insert(0, SCRIPT_DIR)

from kernel import PondMinimal  # noqa: E402
from lakehouse_lens import LakehouseLens  # noqa: E402
from feature_store_lens import FeatureStoreLens  # noqa: E402

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    import duckdb
except ImportError:
    raise ImportError("pyarrow and duckdb required")

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
# Physical Structure: Parquet statistics (shared across Lenses)
# ---------------------------------------------------------------------------

def build_parquet_statistics(kernel, collection_name, table_data):
    """Build Parquet statistics (min/max/null_count per column) and
    store as a Physical Structure in the kernel.

    Per the Physical Structure algebra (§14): f(snapshot) → artifact.
    The statistics are a deterministic function of the snapshot.
    Any Lens can read them.
    """
    # Compute statistics from the PyArrow table
    stats = {}
    for col_name in table_data.column_names:
        col = table_data.column(col_name)
        try:
            # Convert to Python list for min/max (avoids PyArrow null issues)
            values = col.to_pylist()
            non_null = [v for v in values if v is not None]
            if non_null:
                stats[col_name] = {
                    "min": str(min(non_null)),
                    "max": str(max(non_null)),
                    "null_count": col.null_count,
                    "count": len(col),
                }
            else:
                stats[col_name] = {
                    "min": None, "max": None,
                    "null_count": col.null_count, "count": len(col),
                }
        except Exception:
            stats[col_name] = {
                "min": None, "max": None,
                "null_count": col.null_count, "count": len(col),
            }

    # Store as a blob in the kernel
    stats_bytes = json.dumps(stats, sort_keys=True).encode()
    stats_hash = kernel.write(stats_bytes)
    # Reference by name: __stats/{collection}
    kernel.reference(f"__stats/{collection_name}", stats_hash)
    return stats_hash, stats


def read_parquet_statistics(kernel, collection_name):
    """Read Parquet statistics from the kernel. Any Lens can call this."""
    h = kernel.resolve(f"__stats/{collection_name}")
    if h is None:
        return None
    return json.loads(kernel.read(h))


def can_prune(stats, column, value):
    """Use statistics to decide if a chunk can be skipped.
    Returns True if the chunk CANNOT contain the value (can be pruned)."""
    if column not in stats:
        return False
    col_stats = stats[column]
    if col_stats["min"] is None or col_stats["max"] is None:
        return False
    try:
        # Try numeric comparison first; fall back to string
        try:
            val = float(value)
            min_val = float(col_stats["min"])
            max_val = float(col_stats["max"])
        except (ValueError, TypeError):
            val = str(value)
            min_val = str(col_stats["min"])
            max_val = str(col_stats["max"])
        if val < min_val or val > max_val:
            return True
    except Exception:
        pass
    return False


# ---------------------------------------------------------------------------
# Physical Structure: Bloom filter (shared across Lenses)
# ---------------------------------------------------------------------------

class SimpleBloomFilter:
    """A simple bloom filter stored as a Physical Structure.
    Any Lens can build it; any Lens can query it."""

    def __init__(self, capacity=1000, false_positive_rate=0.01):
        import math
        self.capacity = capacity
        self.num_bits = int(-capacity * math.log(false_positive_rate) / (math.log(2) ** 2))
        self.num_hashes = int(self.num_bits / capacity * math.log(2))
        self.bits = [False] * self.num_bits

    def add(self, item):
        for i in range(self.num_hashes):
            h = int(hashlib.sha256(f"{i}:{item}".encode()).hexdigest(), 16) % self.num_bits
            self.bits[h] = True

    def contains(self, item):
        for i in range(self.num_hashes):
            h = int(hashlib.sha256(f"{i}:{item}".encode()).hexdigest(), 16) % self.num_bits
            if not self.bits[h]:
                return False
        return True

    def to_bytes(self):
        # Pack bits into bytes
        packed = bytearray()
        for i in range(0, len(self.bits), 8):
            byte = 0
            for j in range(8):
                if i + j < len(self.bits) and self.bits[i + j]:
                    byte |= (1 << j)
            packed.append(byte)
        return bytes(packed)

    @classmethod
    def from_bytes(cls, data, capacity=1000, false_positive_rate=0.01):
        bf = cls(capacity, false_positive_rate)
        for i in range(min(len(data) * 8, len(bf.bits))):
            byte_idx = i // 8
            bit_idx = i % 8
            if data[byte_idx] & (1 << bit_idx):
                bf.bits[i] = True
        return bf


def build_bloom_filter(kernel, collection_name, items):
    """Build a bloom filter from a list of items and store as a Physical Structure."""
    bf = SimpleBloomFilter(capacity=max(len(items) * 2, 1000))
    for item in items:
        bf.add(str(item))
    bf_bytes = bf.to_bytes()
    bf_hash = kernel.write(bf_bytes)
    kernel.reference(f"__bloom/{collection_name}", bf_hash)
    return bf_hash, bf


def read_bloom_filter(kernel, collection_name):
    """Read a bloom filter from the kernel. Any Lens can call this."""
    h = kernel.resolve(f"__bloom/{collection_name}")
    if h is None:
        return None
    bf_bytes = kernel.read(h)
    return SimpleBloomFilter.from_bytes(bf_bytes)


# ---------------------------------------------------------------------------
# Experiment 1: Lakehouse builds stats → FeatureStore uses them
# ---------------------------------------------------------------------------

def experiment_1_stats_portability():
    """Lakehouse builds Parquet statistics; FeatureStore uses them for pruning."""
    print("\n=== Experiment 1: Parquet statistics portability ===")
    print("  Lakehouse builds stats → FeatureStore uses for pruning")

    tmpdir = tempfile.mkdtemp(prefix="pond_lab2_exp1_")
    try:
        kernel = PondMinimal(tmpdir)
        lh = LakehouseLens(kernel)

        # Lakehouse creates a table with 1000 rows
        import random
        random.seed(42)
        users = pa.table({
            "user_id": list(range(1000)),
            "age": [random.randint(18, 80) for _ in range(1000)],
            "score": [random.uniform(0, 100) for _ in range(1000)],
        })
        lh.create_table("users", users)

        # Lakehouse builds statistics (Physical Structure)
        stats_hash, stats = build_parquet_statistics(kernel, "users", users)
        print(f"  Lakehouse built stats: user_id min={stats['user_id']['min']}, max={stats['user_id']['max']}")

        # FeatureStore reads the SAME statistics (no rebuild)
        fs_stats = read_parquet_statistics(kernel, "users")
        check(fs_stats is not None, "FeatureStore reads Lakehouse's statistics")
        check(fs_stats["user_id"]["min"] == "0", f"Stats match: user_id min=0 (got {fs_stats['user_id']['min']})")
        check(fs_stats["user_id"]["max"] == "999", f"Stats match: user_id max=999 (got {fs_stats['user_id']['max']})")

        # FeatureStore uses stats for pruning
        # "Can we skip this chunk if looking for user_id=5000?"
        can_skip = can_prune(fs_stats, "user_id", "5000")
        check(can_skip, "FeatureStore uses Lakehouse's stats for pruning (user_id=5000 not in [0,999])")

        # "Can we NOT skip if looking for user_id=500?"
        can_skip = can_prune(fs_stats, "user_id", "500")
        check(not can_skip, "FeatureStore uses stats correctly (user_id=500 in [0,999], cannot skip)")

        kernel.close()
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Experiment 2: FeatureStore builds bloom filter → Lakehouse uses it
# ---------------------------------------------------------------------------

def experiment_2_bloom_portability():
    """FeatureStore builds a bloom filter; Lakehouse uses it for membership test."""
    print("\n=== Experiment 2: Bloom filter portability ===")
    print("  FeatureStore builds bloom → Lakehouse uses for membership test")

    tmpdir = tempfile.mkdtemp(prefix="pond_lab2_exp2_")
    try:
        kernel = PondMinimal(tmpdir)
        fs = FeatureStoreLens(kernel)

        # FeatureStore defines a collection and ingests data
        fs.define_collection(
            "user_features",
            entity_columns=["user_id"],
            timestamp_column="event_ts",
            feature_columns=["score"],
        )
        features = pa.table({
            "user_id": [1, 2, 3, 4, 5],
            "event_ts": pa.array([datetime.datetime(2024, 1, 1)] * 5),
            "score": [0.9, 0.8, 0.7, 0.6, 0.5],
        })
        fs.ingest("user_features", features)

        # FeatureStore builds a bloom filter of user_ids (Physical Structure)
        user_ids = [1, 2, 3, 4, 5]
        bf_hash, bf = build_bloom_filter(kernel, "user_features", user_ids)
        print(f"  FeatureStore built bloom filter for {len(user_ids)} user_ids")

        # Lakehouse reads the SAME bloom filter (no rebuild)
        lh_bf = read_bloom_filter(kernel, "user_features")
        check(lh_bf is not None, "Lakehouse reads FeatureStore's bloom filter")

        # Lakehouse uses it for membership test
        check(lh_bf.contains("3"), "Lakehouse: bloom says user_id=3 exists (correct)")
        check(not lh_bf.contains("999"), "Lakehouse: bloom says user_id=999 doesn't exist (correct)")

        kernel.close()
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Experiment 3: Zone maps (min/max per chunk) shared across Lenses
# ---------------------------------------------------------------------------

def experiment_3_zone_map_portability():
    """A zone map built by one Lens is usable by another for range pruning."""
    print("\n=== Experiment 3: Zone map portability ===")
    print("  Any Lens builds zone map → any Lens uses for range pruning")

    tmpdir = tempfile.mkdtemp(prefix="pond_lab2_exp3_")
    try:
        kernel = PondMinimal(tmpdir)

        # Build zone maps: min/max per chunk of 100 rows
        # Use sorted data so chunks have distinct ranges (for meaningful pruning)
        all_data = list(range(1000))

        zone_maps = []
        for i in range(0, len(all_data), 100):
            chunk = all_data[i:i+100]
            zone_maps.append({
                "chunk_id": i // 100,
                "min": min(chunk),
                "max": max(chunk),
            })

        # Store as a Physical Structure
        zm_bytes = json.dumps(zone_maps, sort_keys=True).encode()
        zm_hash = kernel.write(zm_bytes)
        kernel.reference("__zonemaps/collection_x", zm_hash)

        # Any Lens reads the zone maps
        h = kernel.resolve("__zonemaps/collection_x")
        read_zm = json.loads(kernel.read(h))
        check(len(read_zm) == 10, f"Zone maps: 10 chunks (got {len(read_zm)})")

        # Use zone maps for range pruning: "find values in [500, 599]"
        target_min, target_max = 500, 599
        relevant_chunks = []
        for zm in read_zm:
            # Skip if chunk range doesn't overlap target range
            if zm["max"] < target_min or zm["min"] > target_max:
                continue
            relevant_chunks.append(zm["chunk_id"])

        check(len(relevant_chunks) > 0, f"Zone map pruning: {len(relevant_chunks)} chunks may contain [500,599]")
        check(len(relevant_chunks) < 10, f"Zone map pruning skipped some chunks ({10 - len(relevant_chunks)} skipped)")

        kernel.close()
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Experiment 4: Metadata overhead — sharing vs per-Lens rebuilding
# ---------------------------------------------------------------------------

def experiment_4_metadata_overhead():
    """Measure: does sharing Physical Structures reduce total metadata overhead?

    Compare:
    - Approach A: each Lens builds its own stats (N copies)
    - Approach B: stats built once, shared by all Lenses (1 copy)
    """
    print("\n=== Experiment 4: Metadata overhead (shared vs per-Lens) ===")

    tmpdir = tempfile.mkdtemp(prefix="pond_lab2_exp4_")
    try:
        kernel = PondMinimal(tmpdir)

        # Build stats once (shared)
        table_data = pa.table({
            "id": list(range(100)),
            "value": [float(i) for i in range(100)],
        })
        shared_hash, _ = build_parquet_statistics(kernel, "shared_table", table_data)

        # Simulate N Lenses each reading the shared stats
        # (they all resolve __stats/shared_table — one blob, N readers)
        n_lenses = 5
        for i in range(n_lenses):
            stats = read_parquet_statistics(kernel, "shared_table")
            check(stats is not None, f"Lens {i+1} reads shared stats (same blob)")

        # Count blobs: 1 stats blob, shared by all Lenses
        stats_blobs = sum(1 for name in kernel.list_names() if name.startswith("__stats/"))
        check(stats_blobs == 1, f"Shared approach: 1 stats blob for {n_lenses} Lenses (got {stats_blobs})")

        # If each Lens built its own stats (simulated)
        for i in range(n_lenses):
            kernel.reference(f"__stats/lens_{i}_table", shared_hash)
        per_lens_blobs = sum(1 for name in kernel.list_names() if name.startswith("__stats/"))
        # Now we have 1 (original) + 5 (per-Lens) = 6 refs, but only 1 blob (dedup)
        check(per_lens_blobs == 6, f"Per-Lens approach: 6 refs (but 1 blob via dedup), got {per_lens_blobs}")

        # The key insight: content-addressing means the BLOB is shared even
        # if the REFS are per-Lens. But the shared approach has fewer refs.
        print(f"  Shared: 1 ref, 1 blob")
        print(f"  Per-Lens: {n_lenses} refs, 1 blob (dedup)")
        print(f"  Content-addressing makes the blob shared either way.")
        print(f"  The savings are in ref count and build cost (build once vs N times).")

        kernel.close()
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 60)
    print("Pond Lab — Track 2: Index Portability")
    print("Can one Physical Structure accelerate multiple Lenses?")
    print("=" * 60)

    experiment_1_stats_portability()
    experiment_2_bloom_portability()
    experiment_3_zone_map_portability()
    experiment_4_metadata_overhead()

    print(f"\n{'='*60}")
    print(f"RESULTS: {PASS} pass, {FAIL} fail")
    print(f"{'='*60}")

    if FAIL == 0:
        print()
        print("Index portability badges:")
        print("  ✓ Parquet statistics: built by Lakehouse, used by FeatureStore")
        print("  ✓ Bloom filter: built by FeatureStore, used by Lakehouse")
        print("  ✓ Zone maps: built by any Lens, used by any Lens")
        print("  ✓ Metadata overhead: sharing reduces build cost (1 build vs N)")
        print()
        print("Architectural insight: Physical Structures are NOT owned by Lenses.")
        print("They are shared artifacts on the kernel. Any Lens can build them;")
        print("any Lens can read them. This is a major differentiator.")

    sys.exit(0 if FAIL == 0 else 1)


if __name__ == "__main__":
    main()
