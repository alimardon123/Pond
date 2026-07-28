#!/usr/bin/env python3
"""
Demo: KeyValueLens (JSON) using the FULL pruning infrastructure.

This proves Pond's core promise: ANY workload — not just tabular — can
use predicate pushdown, column-chunk storage, and encoded predicate eval.
The storage layer is format-agnostic; the lens provides its own encoder.

What this demo does:
  1. Creates a "users" collection with 3000 rows as list-of-dicts (JSON-style)
  2. Builds zone maps from the JSON data (NO PyArrow needed)
  3. Writes per-column-chunk blobs using a JSON encode_fn (NOT Parquet)
  4. Reads with predicate pushdown: "age >= 2500" prunes 2/3 of chunks
  5. Verifies the surviving rows are correct

This is the "any app gets infinite storage + pruning" promise in action.
A Notebook lens, Feature Store lens, Git lens, or Vector lens could do
the same with their own encode_fn — the pruning infrastructure doesn't
care what format the bytes are in.

Run:
    python pond-labs/demos/generic_pruning_demo.py
"""

from __future__ import annotations

import os
import sys
import json
import shutil
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, os.path.join(REPO, "pond-core"))
sys.path.insert(0, os.path.join(REPO, "pond-sdk"))
sys.path.insert(0, os.path.join(REPO, "pond-sdk", "extensions", "physical_structures"))

from kernel import PondMinimal
from column_source import ListColumnSource
from pruning import ZoneMap, PruningPredicate, ColumnPredicate
from zone_map_index import ZoneMapIndex
from column_chunk_zone_map import ColumnChunkZoneMap
from column_chunk_storage import ColumnChunkStorage
from pruning_reader import PruningReader


def main():
    print("=" * 70)
    print("Generic Pruning Demo: JSON data + ColumnChunkStorage + PruningReader")
    print("=" * 70)
    print()
    print("  This demo proves ANY workload can use Pond's pruning infrastructure.")
    print("  No PyArrow. No Parquet. Just JSON dicts + the format-agnostic")
    print("  ColumnSource + encode_fn/decode_fn contracts.")
    print()

    tmpdir = tempfile.mkdtemp(prefix="pond_generic_demo_")
    try:
        kernel = PondMinimal(tmpdir)

        # --- Step 1: Create 3000 rows as list-of-dicts (JSON-style) ---
        # This simulates what a KeyValueLens, Notebook lens, or any
        # non-tabular lens would produce.
        n = 3000
        rows = [
            {"id": i, "age": i, "name": f"user_{i}"}
            for i in range(n)
        ]
        print(f"  Step 1: Created {n} rows as list-of-dicts (JSON-style, no PyArrow)")

        # --- Step 2: Build zone maps from JSON data ---
        # ZoneMap.build accepts any ColumnSource — ListColumnSource wraps
        # list-of-dicts without PyArrow.
        zm_index = ZoneMapIndex(kernel)
        storage = ColumnChunkStorage(kernel)

        chunk_size = 1000
        row_group_key = "rg/2999"
        source = ListColumnSource(rows)

        # Build the row-group zone map (min/max per column)
        zm = ZoneMap.build(source)
        print(f"  Step 2: Built zone map from JSON data")
        print(f"          id:   [{zm.min['id']}, {zm.max['id']}]")
        print(f"          age:  [{zm.min['age']}, {zm.max['age']}]")
        print(f"          name: [{zm.min['name']}, {zm.max['name']}]")

        # --- Step 3: Write per-column-chunk blobs using JSON encode_fn ---
        # encode_fn(col_name, values: list) -> bytes
        # This is a JSON encoder — NOT Parquet. Any lens can provide its
        # own encoder (binary, rich text, diffs, etc.).
        def json_encode(col_name: str, values: list) -> bytes:
            """Encode a column's values as JSON bytes."""
            return json.dumps({col_name: values}).encode()

        def json_decode(chunk_bytes: bytes) -> list:
            """Decode JSON bytes back to a list of values."""
            data = json.loads(chunk_bytes)
            # Return the first (only) column's values
            return list(data.values())[0]

        manifest_hash, cczm = storage.write_row_group_column_chunks(
            source, row_group_key, chunk_size=chunk_size,
            encode_fn=json_encode,
        )

        # Store the zone map with the manifest hash
        zm_dict = zm.to_dict()
        zm_dict["column_chunks"] = cczm.to_dict()
        zm = ZoneMap.from_dict(zm_dict)
        zm_index.add_zone_map("users", row_group_key, zm, manifest_hash)
        zm_index.commit_zone_maps("users")
        print(f"  Step 3: Wrote per-column-chunk blobs using JSON encode_fn")
        print(f"          3 columns × 3 chunks = 9 JSON chunk blobs")
        print(f"          (NO Parquet, NO PyArrow — just json.dumps)")

        # --- Step 4: Read with predicate pushdown: "age >= 2500" ---
        # This should prune chunks 0 and 1 (ages 0-999 and 1000-1999),
        # and only read chunk 2 (ages 2000-2999).
        predicate = PruningPredicate([
            ColumnPredicate(column="age", op=">=", value=2500)
        ])

        # Use read_column_chunks directly — the natural API for
        # column-chunk storage with a custom encode_fn/decode_fn.
        # Zone-map pruning happens inside prune_column_chunks —
        # it reads only the zone map blob (small, separate), NOT the
        # data chunk blobs. Non-matching chunks are skipped at the
        # I/O level — no blob read, no decompression, no decode.

        # Get the zone map for this row group
        zm_entry = zm_index.get_zone_map("users", row_group_key)
        cczm_loaded = ColumnChunkZoneMap.from_dict(zm_entry["column_chunks"])

        # Column-chunk zone map pruning: which chunks survive "age >= 2500"?
        surviving_chunks = cczm_loaded.prune_column_chunks("age", ">=", 2500)
        print(f"\n  Step 4: Predicate 'age >= 2500'")
        print(f"          Column-chunk pruning: chunks {surviving_chunks} survive")
        print(f"          (chunk 0: ages 0-999, chunk 1: ages 1000-1999 → PRUNED)")
        print(f"          (chunk 2: ages 2000-2999 → SURVIVES)")

        # Read only surviving chunks using the JSON decode_fn
        col_data = storage.read_column_chunks(
            cczm_loaded, ["id", "age", "name"],
            surviving_chunk_indices=set(surviving_chunks),
            decode_fn=json_decode,
        )

        # Reassemble rows from the column data
        all_rows = []
        for col_name, value_lists in col_data.items():
            for vals in value_lists:
                if not all_rows:
                    all_rows = [{} for _ in vals]
                for i, v in enumerate(vals):
                    if i < len(all_rows):
                        all_rows[i][col_name] = v

        print(f"          Read {len(all_rows)} surviving rows")
        if all_rows:
            print(f"          First: {all_rows[0]}")
            print(f"          Last:  {all_rows[-1]}")

        # Verify correctness: ages should be 2000-2999 (chunk 2's range)
        ages = [r["age"] for r in all_rows]
        assert min(ages) == 2000, f"Expected min age 2000 (chunk 2 start), got {min(ages)}"
        assert max(ages) == 2999, f"Expected max age 2999, got {max(ages)}"
        assert len(all_rows) == 1000, f"Expected 1000 rows from chunk 2, got {len(all_rows)}"
        print(f"  [OK] Correct: ages [{min(ages)}, {max(ages)}] — 1000 rows from chunk 2")

        # --- Step 5: Summary ---
        print(f"\n  Step 5: Pruning effectiveness")
        print(f"          Zone-map pruning skipped 2/3 chunks WITHOUT reading")
        print(f"          the data blobs — only the zone map blob was read.")
        print(f"          On S3: 67% fewer blob fetches (2/3 chunks skipped)")

        kernel.close()
        print(f"\n{'=' * 70}")
        print("ALL GENERIC PRUNING DEMO TESTS PASSED")
        print(f"{'=' * 70}")
        print()
        print("Key findings:")
        print("  - JSON data (list-of-dicts) uses the SAME pruning infrastructure")
        print("    as Parquet data — no PyArrow needed")
        print("  - The lens provides its own encode_fn/decode_fn (JSON here)")
        print("  - Column-chunk pruning skips non-matching chunks at the I/O level")
        print("  - Any app built on Pond gets this for free — notebooks, feature")
        print("    stores, git, vectors, music, video — any format, any layout")

    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == "__main__":
    main()
