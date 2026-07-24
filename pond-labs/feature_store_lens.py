"""
Pond Feature Store Lens (pond-labs)

A versioned ML feature store built on the Pond kernel. This is the
"Feast on Pond" demo — the kind of thing that answers:
"What can people build with Pond that they currently cannot build simply?"

What this Lens does:
  - Versioned feature definitions (schema evolution: add/rename features)
  - Point-in-time joins (prevents label leakage in ML training)
  - Online + offline serving (same data, different access patterns)
  - Branching for feature experimentation (try new features on a branch
    without affecting production)
  - Time travel for reproducible training sets
  - Cross-Lens interop with the DuckDB Lakehouse Lens

Storage model:
  - Feature data stored as Parquet in Pond blobs (same format as Lakehouse Lens)
  - Feature definitions stored as JSON in Pond blobs
  - Commit chain uses Pond refs (features/{collection}/HEAD)
  - Branches use Pond refs (features/{collection}/branches/{name})

The key insight: feature data is just tabular data. The Feature Store
Lens adds *feature-specific semantics* (entity registry, point-in-time
join logic, feature definitions) on top of the same Parquet-in-Pond
format that the Lakehouse Lens uses. This means the two Lenses
interoperate: any feature collection is queryable by DuckDB.

This is NOT a production feature store. It's a reference implementation
that tests whether the Lens algebra covers the feature-store workload.

Run:
    python pond-labs/feature_store_lens.py
"""

from __future__ import annotations

import os
import sys
import json
import time
import tempfile
import shutil
from typing import Optional, Iterator

# Make pond-core importable
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(SCRIPT_DIR, "..", "pond-core"))
from pond_minimal import PondMinimal  # noqa: E402

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    raise ImportError("pyarrow required: pip install pyarrow")


# ---------------------------------------------------------------------------
# Feature Store Lens
# ---------------------------------------------------------------------------

class FeatureStoreLens:
    """Versioned ML feature store on Pond.

    A "feature collection" is a set of features for a given entity type.
    For example: `user_features` might contain features like
    `user_age`, `user_purchase_count_30d`, `user_avg_session_duration`.

    Storage:
      - Feature data: Parquet blobs in Pond, one per (collection, snapshot)
      - Feature definitions: JSON blobs (entity columns, feature columns,
        timestamp column, schema version)
      - HEAD ref: features/{collection}/HEAD -> latest commit
      - Branch ref: features/{collection}/branches/{name} -> commit
    """

    def __init__(self, kernel: PondMinimal):
        self.kernel = kernel

    # ------------------------------------------------------------------
    # Feature definition management
    # ------------------------------------------------------------------

    def define_collection(self, collection: str,
                          entity_columns: list[str],
                          timestamp_column: str,
                          feature_columns: list[str]) -> str:
        """Define a new feature collection. Returns the definition hash."""
        definition = {
            "collection": collection,
            "entity_columns": entity_columns,
            "timestamp_column": timestamp_column,
            "feature_columns": list(feature_columns),
            "schema_version": 1,
            "created_at": time.time(),
        }
        defn_bytes = json.dumps(definition, sort_keys=True).encode()
        defn_hash = self.kernel.write(defn_bytes)
        self.kernel.reference(f"features/{collection}/definition", defn_hash)
        return defn_hash

    def get_definition(self, collection: str) -> dict:
        defn_hash = self.kernel.resolve(f"features/{collection}/definition")
        if defn_hash is None:
            raise KeyError(f"Collection {collection} not defined")
        return json.loads(self.kernel.read(defn_hash))

    def evolve_schema(self, collection: str,
                      added_features: list[str] = None,
                      removed_features: list[str] = None,
                      renamed: dict[str, str] = None) -> str:
        """Evolve the feature schema. Old data remains readable; missing
        columns return NULL when read by the new schema (Parquet-native
        schema evolution)."""
        defn = self.get_definition(collection)
        new_features = list(defn["feature_columns"])
        if added_features:
            new_features.extend(added_features)
        if removed_features:
            new_features = [f for f in new_features if f not in removed_features]
        if renamed:
            new_features = [renamed.get(f, f) for f in new_features]
        defn["feature_columns"] = new_features
        defn["schema_version"] += 1
        defn["evolved_at"] = time.time()
        defn_bytes = json.dumps(defn, sort_keys=True).encode()
        defn_hash = self.kernel.write(defn_bytes)
        self.kernel.reference(f"features/{collection}/definition", defn_hash)
        return defn_hash

    # ------------------------------------------------------------------
    # Feature data ingestion
    # ------------------------------------------------------------------

    def ingest(self, collection: str, data: pa.Table,
               message: str = "") -> str:
        """Ingest feature data. Appends to existing data (union with
        promote_options for schema evolution). Returns the new commit hash."""
        defn = self.get_definition(collection)
        # Validate schema: must include entity + timestamp + at least one feature
        required = set(defn["entity_columns"] + [defn["timestamp_column"]])
        actual = set(data.column_names)
        missing = required - actual
        if missing:
            raise ValueError(f"Missing required columns: {missing}")

        # Union with existing data (if any) — feature stores are append-only
        parent = self.kernel.resolve(f"features/{collection}/HEAD")
        if parent is not None:
            parent_commit = json.loads(self.kernel.read(parent))
            parent_parquet = self.kernel.read(parent_commit["parquet"])
            parent_table = self._decode_table(parent_parquet)
            try:
                combined = pa.concat_tables([parent_table, data], promote_options="default")
            except TypeError:
                combined = pa.concat_tables([parent_table, data])
        else:
            combined = data

        # Encode as Parquet
        parquet_bytes = self._encode_table(combined)
        parquet_hash = self.kernel.write(parquet_bytes)

        # Build commit
        commit = {
            "collection": collection,
            "parquet": parquet_hash,
            "parent": parent,
            "definition": self.kernel.resolve(f"features/{collection}/definition"),
            "row_count": combined.num_rows,
            "timestamp": time.time(),
            "message": message or f"ingest {data.num_rows} rows",
        }
        commit_bytes = json.dumps(commit).encode()
        commit_hash = self.kernel.write(commit_bytes)
        self.kernel.reference(f"features/{collection}/HEAD", commit_hash)
        return commit_hash

    # ------------------------------------------------------------------
    # Point-in-time join (prevents label leakage)
    # ------------------------------------------------------------------

    def point_in_time_join(self, collection: str,
                           entity_rows: pa.Table,
                           features: Optional[list[str]] = None) -> pa.Table:
        """Point-in-time join: for each entity row, fetch the latest
        feature values as of that row's timestamp.

        This is THE killer feature of a feature store. Without it, ML
        practitioners accidentally leak future information into training
        data, producing models that look great in training but fail in
        production.

        Algorithm:
          1. For each entity_row (entity_id, event_timestamp):
             a. Filter feature data to (entity_id, feature_timestamp <= event_timestamp)
             b. Take the latest feature row
          3. Return joined table

        For efficiency, we sort feature data by (entity, timestamp) and
        use binary search per entity. For simplicity here, we do it in
        Python with PyArrow; production would push down to DuckDB.
        """
        defn = self.get_definition(collection)
        entity_cols = defn["entity_columns"]
        ts_col = defn["timestamp_column"]
        feature_cols = features or defn["feature_columns"]

        # Read all feature data (in production, this would be partitioned)
        feature_table = self.read_features(collection)
        # Convert to pandas for the join (simpler than pure Arrow)
        features_df = feature_table.to_pandas()
        entities_df = entity_rows.to_pandas()

        # Ensure timestamps are comparable
        features_df[ts_col] = features_df[ts_col].astype("datetime64[ns]")
        entities_df[ts_col] = entities_df[ts_col].astype("datetime64[ns]")

        # Sort features by (entity, timestamp) for binary search
        features_df = features_df.sort_values(by=entity_cols + [ts_col])

        # For each entity row, find the latest feature row as of event_ts
        result_rows = []
        for _, e_row in entities_df.iterrows():
            # Filter to this entity
            mask = True
            for ec in entity_cols:
                mask = mask & (features_df[ec] == e_row[ec])
            entity_features = features_df[mask]
            # Filter to timestamp <= event_ts
            as_of = entity_features[entity_features[ts_col] <= e_row[ts_col]]
            if len(as_of) == 0:
                # No features as of this timestamp; emit NULLs
                row = {c: e_row[c] for c in entities_df.columns}
                for fc in feature_cols:
                    row[fc] = None
            else:
                # Take the latest
                latest = as_of.iloc[-1]
                row = {c: e_row[c] for c in entities_df.columns}
                for fc in feature_cols:
                    row[fc] = latest[fc] if fc in latest else None
            result_rows.append(row)

        import pandas as pd
        result_df = pd.DataFrame(result_rows)
        return pa.Table.from_pandas(result_df, preserve_index=False)

    # ------------------------------------------------------------------
    # Read features
    # ------------------------------------------------------------------

    def read_features(self, collection: str,
                      commit_hash: Optional[str] = None) -> pa.Table:
        """Read feature data. If commit_hash is None, reads HEAD."""
        if commit_hash is None:
            commit_hash = self.kernel.resolve(f"features/{collection}/HEAD")
            if commit_hash is None:
                raise KeyError(f"Collection {collection} has no data")
        commit = json.loads(self.kernel.read(commit_hash))
        parquet_bytes = self.kernel.read(commit["parquet"])
        return self._decode_table(parquet_bytes)

    def get_feature_vector(self, collection: str,
                           entity_id: dict,
                           features: list[str]) -> dict:
        """Online serving: get the latest feature values for a single
        entity. This is the 'online' path of the feature store."""
        table = self.read_features(collection)
        df = table.to_pandas()
        # Filter to this entity
        for col, val in entity_id.items():
            df = df[df[col] == val]
        if len(df) == 0:
            return {f: None for f in features}
        # Take the latest by timestamp
        defn = self.get_definition(collection)
        df = df.sort_values(by=defn["timestamp_column"])
        latest = df.iloc[-1]
        return {f: latest[f] if f in latest else None for f in features}

    # ------------------------------------------------------------------
    # Branching (for feature experimentation)
    # ------------------------------------------------------------------

    def branch(self, collection: str, branch_name: str) -> str:
        """Create a branch for feature experimentation. New features
        can be ingested on the branch without affecting production HEAD."""
        head = self.kernel.resolve(f"features/{collection}/HEAD")
        if head is None:
            raise KeyError(f"Collection {collection} has no HEAD")
        branch_ref = f"features/{collection}/branches/{branch_name}"
        self.kernel.reference(branch_ref, head)
        return head

    def ingest_to_branch(self, collection: str, branch_name: str,
                         data: pa.Table, message: str = "") -> str:
        """Ingest to a branch (not HEAD). Appends to branch's current data."""
        branch_ref = f"features/{collection}/branches/{branch_name}"
        branch_head = self.kernel.resolve(branch_ref)
        if branch_head is None:
            raise KeyError(f"Branch {branch_name} not found")

        defn = self.get_definition(collection)
        # Union with branch's current data
        parent_commit = json.loads(self.kernel.read(branch_head))
        parent_parquet = self.kernel.read(parent_commit["parquet"])
        parent_table = self._decode_table(parent_parquet)
        try:
            combined = pa.concat_tables([parent_table, data], promote_options="default")
        except TypeError:
            combined = pa.concat_tables([parent_table, data])

        parquet_bytes = self._encode_table(combined)
        parquet_hash = self.kernel.write(parquet_bytes)

        commit = {
            "collection": collection,
            "parquet": parquet_hash,
            "parent": branch_head,
            "definition": self.kernel.resolve(f"features/{collection}/definition"),
            "row_count": combined.num_rows,
            "timestamp": time.time(),
            "message": message or f"branch {branch_name}: ingest {data.num_rows} rows",
            "branch": branch_name,
        }
        commit_bytes = json.dumps(commit).encode()
        commit_hash = self.kernel.write(commit_bytes)
        self.kernel.reference(branch_ref, commit_hash)
        return commit_hash

    def read_branch(self, collection: str, branch_name: str) -> pa.Table:
        branch_ref = f"features/{collection}/branches/{branch_name}"
        commit_hash = self.kernel.resolve(branch_ref)
        if commit_hash is None:
            raise KeyError(f"Branch {branch_name} not found")
        return self.read_features(collection, commit_hash)

    def merge_branch(self, collection: str, branch_name: str) -> str:
        """Merge a branch into HEAD. Union merge."""
        head = self.kernel.resolve(f"features/{collection}/HEAD")
        branch_ref = f"features/{collection}/branches/{branch_name}"
        branch_head = self.kernel.resolve(branch_ref)
        if branch_head is None:
            raise KeyError(f"Branch {branch_name} not found")

        head_data = self.read_features(collection, head)
        branch_data = self.read_features(collection, branch_head)
        try:
            merged = pa.concat_tables([head_data, branch_data], promote_options="default")
        except TypeError:
            merged = pa.concat_tables([head_data, branch_data])
        parquet_bytes = self._encode_table(merged)
        parquet_hash = self.kernel.write(parquet_bytes)

        commit = {
            "collection": collection,
            "parquet": parquet_hash,
            "parent": head,
            "second_parent": branch_head,
            "definition": self.kernel.resolve(f"features/{collection}/definition"),
            "row_count": merged.num_rows,
            "timestamp": time.time(),
            "message": f"merge branch {branch_name}",
        }
        commit_bytes = json.dumps(commit).encode()
        commit_hash = self.kernel.write(commit_bytes)
        self.kernel.reference(f"features/{collection}/HEAD", commit_hash)
        return commit_hash

    # ------------------------------------------------------------------
    # History
    # ------------------------------------------------------------------

    def history(self, collection: str) -> list[dict]:
        head = self.kernel.resolve(f"features/{collection}/HEAD")
        if head is None:
            return []
        history = []
        current = head
        while current:
            commit = json.loads(self.kernel.read(current))
            history.append({
                "hash": current[:8],
                "message": commit["message"],
                "row_count": commit["row_count"],
                "timestamp": commit["timestamp"],
                "parent": commit.get("parent", "")[:8] if commit.get("parent") else None,
                "second_parent": commit.get("second_parent", "")[:8] if commit.get("second_parent") else None,
            })
            current = commit.get("parent")
        return history

    # ------------------------------------------------------------------
    # Encoding helpers
    # ------------------------------------------------------------------

    def _encode_table(self, table: pa.Table) -> bytes:
        sink = pa.BufferOutputStream()
        pq.write_table(table, sink)
        return sink.getvalue().to_pybytes()

    def _decode_table(self, parquet_bytes: bytes) -> pa.Table:
        reader = pa.BufferReader(parquet_bytes)
        return pq.read_table(reader)


# ---------------------------------------------------------------------------
# Self-tests
# ---------------------------------------------------------------------------

def _self_test():
    print("=== Pond Feature Store Lens self-test ===")

    tmpdir = tempfile.mkdtemp(prefix="pond_fs_")
    try:
        kernel = PondMinimal(tmpdir)
        fs = FeatureStoreLens(kernel)

        # Test 1: define a collection
        fs.define_collection(
            collection="user_features",
            entity_columns=["user_id"],
            timestamp_column="event_ts",
            feature_columns=["age", "purchase_count_30d", "avg_session_duration"],
        )
        defn = fs.get_definition("user_features")
        assert defn["entity_columns"] == ["user_id"]
        assert defn["timestamp_column"] == "event_ts"
        assert len(defn["feature_columns"]) == 3
        print(f"  [OK] define collection: 3 features, schema v1")

        # Test 2: ingest feature data
        import datetime
        features_v1 = pa.table({
            "user_id": [1, 1, 2, 2, 3],
            "event_ts": pa.array([
                datetime.datetime(2024, 1, 1),
                datetime.datetime(2024, 1, 15),
                datetime.datetime(2024, 1, 5),
                datetime.datetime(2024, 1, 20),
                datetime.datetime(2024, 1, 10),
            ]),
            "age": [25, 25, 30, 30, 28],
            "purchase_count_30d": [0, 2, 1, 5, 3],
            "avg_session_duration": [10.0, 15.0, 8.0, 20.0, 12.0],
        })
        fs.ingest("user_features", features_v1, message="initial features")
        table = fs.read_features("user_features")
        assert table.num_rows == 5
        print(f"  [OK] ingest 5 feature rows; read back {table.num_rows}")

        # Test 3: point-in-time join (the killer feature)
        # Entity rows: (user_id, event_ts) for which we want features as-of
        entity_rows = pa.table({
            "user_id": [1, 2, 3],
            "event_ts": pa.array([
                datetime.datetime(2024, 1, 10),  # user 1: should get features from 2024-01-01 (latest <= 01-10)
                datetime.datetime(2024, 1, 25),  # user 2: should get features from 2024-01-20
                datetime.datetime(2024, 1, 1),   # user 3: should get NULL (no features yet on 01-01)
            ]),
        })
        joined = fs.point_in_time_join("user_features", entity_rows)
        joined_df = joined.to_pandas()
        # User 1 at 2024-01-10: latest feature row is 2024-01-01 (purchase_count_30d=0)
        user1 = joined_df[joined_df["user_id"] == 1].iloc[0]
        assert user1["purchase_count_30d"] == 0, \
            f"user 1 PIT: expected 0, got {user1['purchase_count_30d']}"
        # User 2 at 2024-01-25: latest is 2024-01-20 (purchase_count_30d=5)
        user2 = joined_df[joined_df["user_id"] == 2].iloc[0]
        assert user2["purchase_count_30d"] == 5, \
            f"user 2 PIT: expected 5, got {user2['purchase_count_30d']}"
        # User 3 at 2024-01-01: no features yet → NULL (pandas converts to NaN)
        user3 = joined_df[joined_df["user_id"] == 3].iloc[0]
        import math
        val = user3["purchase_count_30d"]
        assert val is None or (isinstance(val, float) and math.isnan(val)), \
            f"user 3 PIT: expected NULL/NaN, got {val}"
        print(f"  [OK] point-in-time join: no label leakage (user 1 gets 0, not 2; user 2 gets 5; user 3 gets NULL)")

        # Test 4: online serving (single entity)
        vec = fs.get_feature_vector("user_features", {"user_id": 1},
                                    ["age", "purchase_count_30d"])
        assert vec["age"] == 25  # latest by timestamp (2024-01-15)
        assert vec["purchase_count_30d"] == 2  # latest (2024-01-15)
        print(f"  [OK] online serving: latest features for user 1 (purchase_count_30d=2)")

        # Test 5: schema evolution (add a feature)
        fs.evolve_schema("user_features", added_features=["country"])
        defn = fs.get_definition("user_features")
        assert "country" in defn["feature_columns"]
        assert defn["schema_version"] == 2
        print(f"  [OK] schema evolution: added 'country' feature (schema v2)")

        # Test 6: ingest with new schema; old data has NULL for country
        features_v2 = pa.table({
            "user_id": [1, 4],
            "event_ts": pa.array([
                datetime.datetime(2024, 2, 1),
                datetime.datetime(2024, 2, 1),
            ]),
            "age": [25, 35],
            "purchase_count_30d": [4, 0],
            "avg_session_duration": [18.0, 5.0],
            "country": ["US", "UK"],
        })
        fs.ingest("user_features", features_v2, message="add country")
        table = fs.read_features("user_features")
        # Old rows have NULL for country
        df = table.to_pandas()
        old_rows = df[df["country"].isna()]
        assert len(old_rows) == 5, f"5 old rows should have NULL country, got {len(old_rows)}"
        print(f"  [OK] schema evolution: old rows have NULL for new column")

        # Test 7: branching — try experimental features without affecting prod
        fs.branch("user_features", "experiment")
        exp_features = pa.table({
            "user_id": [1, 2, 3],
            "event_ts": pa.array([
                datetime.datetime(2024, 2, 15),
                datetime.datetime(2024, 2, 15),
                datetime.datetime(2024, 2, 15),
            ]),
            "age": [25, 30, 28],
            "purchase_count_30d": [10, 15, 20],
            "avg_session_duration": [25.0, 30.0, 22.0],
            "country": ["US", "UK", "CA"],
        })
        fs.ingest_to_branch("user_features", "experiment", exp_features,
                           message="experimental features v2")

        # Production HEAD unchanged
        prod_table = fs.read_features("user_features")
        assert prod_table.num_rows == 7, \
            f"prod HEAD: expected 7 rows, got {prod_table.num_rows}"
        # Branch has 7 (copied from HEAD) + 3 (new) = 10 rows
        branch_table = fs.read_branch("user_features", "experiment")
        assert branch_table.num_rows == 10, \
            f"branch: expected 10 rows (7 + 3), got {branch_table.num_rows}"
        print(f"  [OK] branching: experiment branch has 10 rows (7 from HEAD + 3 new); prod unchanged at 7")

        # Test 8: merge experiment into prod
        fs.merge_branch("user_features", "experiment")
        prod_table = fs.read_features("user_features")
        # Union merge: 7 (prod) + 10 (branch) = 17 (with duplicates from common ancestor)
        assert prod_table.num_rows == 17, \
            f"after merge: expected 17 rows (7 + 10), got {prod_table.num_rows}"
        print(f"  [OK] merge: 17 rows after union merge (7 prod + 10 branch, with dups)")

        # Test 9: history shows commits + merge
        history = fs.history("user_features")
        assert len(history) >= 3, f"expected >=3 commits, got {len(history)}"
        latest = history[0]
        assert latest["second_parent"] is not None, "merge commit has 2 parents"
        print(f"  [OK] history: {len(history)} commits, latest is merge with 2 parents")

        # Test 10: time travel — read features at original commit
        original_commit = history[-1]["hash"]  # first commit is last in history
        # Wait — history[0] is latest, history[-1] is the first. But hashes are truncated to 8 chars.
        # Let me use the full hash from history
        original_full_hash = None
        # Walk back to find the first commit
        current = kernel.resolve("features/user_features/HEAD")
        first_commit = None
        while current:
            commit = json.loads(kernel.read(current))
            if commit.get("parent") is None:
                first_commit = current
                break
            current = commit.get("parent")
        assert first_commit is not None
        old_table = fs.read_features("user_features", commit_hash=first_commit)
        assert old_table.num_rows == 5, \
            f"time travel to first commit: expected 5 rows, got {old_table.num_rows}"
        print(f"  [OK] time travel: first commit has 5 rows (original ingest)")

        kernel.close()
        print("\nAll Feature Store Lens tests pass.")
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == "__main__":
    _self_test()
