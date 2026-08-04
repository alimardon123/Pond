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
  - Commit chain uses Pond refs (collections/{name}/HEAD)
  - Branches use Pond refs (collections/{name}/_branches/{branch})

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

# Make pond-core and pond-sdk importable
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(SCRIPT_DIR, "..", "..", "pond-core"))
sys.path.insert(0, os.path.join(SCRIPT_DIR, "..", "..", "pond-sdk"))
from kernel import PondMinimal  # noqa: E402
from base_lens import PondLens  # noqa: E402
# ProllyTree imports removed — use UnifiedStorage instead
# This lens is in pond-labs (experimental) and needs migration to UnifiedStorage

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    raise ImportError("pyarrow required: pip install pyarrow")


# ---------------------------------------------------------------------------
# Feature Store Lens
# ---------------------------------------------------------------------------

# Magic key prefix for row groups in the ProllyTreeIndex (same convention
# as LakehouseLens — duplicated intentionally per the design principle that
# production lenses own their own code rather than inheriting from each other).
_FS_RG_PREFIX = "rg/"


class FeatureStoreLens(PondLens):
    """Versioned ML feature store on Pond.

    Extends PondLens directly (NOT LakehouseLens). Owns its ProllyTreeIndex
    storage code — row groups stored as Parquet blobs keyed by rg/{max_pk}.
    This duplication is intentional: per the design principles, production
    lenses should not inherit from each other. Each lens is independent and
    can be removed without affecting others.

    Feature collections ARE interoperable with LakehouseLens collections
    because they share the same storage convention (rg/ row groups in
    ProllyTreeIndex) and the same commit format (binary ProllyLensBase
    commits). But the code is independent — no cross-lens imports.

    Adds (beyond PondLens):
      - Feature definition management (entity columns, feature columns)
      - Feature data ingestion (ProllyTreeIndex-backed row group storage)
      - Point-in-time join (prevents label leakage in ML training)
      - Online serving (point lookup via get_feature_vector)
      - Schema evolution (add/rename features)
      - Branching, merge, time travel (via ProllyTreeIndex)
    """

    def __init__(self, kernel: PondMinimal):
        super().__init__(kernel)
        self._duckdb = None
        self._attached_indexer = None

    def attach_indexer(self, indexer) -> None:
        """Attach a CollectionMetadata or CollectionIndexer for auto-notify.

        After attaching, every commit (ingest, ingest_to_branch, merge_branch)
        auto-notifies the indexer. EAGER indexes refresh immediately; LAZY
        indexes accumulate staleness.

        Usage:
            meta = CollectionMetadata(kernel)
            meta.register_eager_index('features', 'by_user', extractor, scan_fn)
            lens.attach_indexer(meta)
            lens.ingest('features', data)  # auto-refreshes
        """
        self._attached_indexer = indexer

    def _notify_indexers(self, collection: str) -> None:
        """Notify attached indexer after a commit. Best-effort."""
        if self._attached_indexer is not None:
            try:
                self._attached_indexer.notify_write(collection)
            except Exception:
                pass

    @property
    def duckdb(self):
        """Lazy DuckDB connection (only created when needed)."""
        if self._duckdb is None:
            try:
                import duckdb
                self._duckdb = duckdb.connect()
            except ImportError:
                pass
        return self._duckdb

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
        self.kernel.reference(self._definition_ref(collection), defn_hash)
        return defn_hash

    def get_definition(self, collection: str) -> dict:
        defn_hash = self.kernel.resolve(self._definition_ref(collection))
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
        self.kernel.reference(self._definition_ref(collection), defn_hash)
        return defn_hash

    # ------------------------------------------------------------------
    # Feature data ingestion (own ProllyTreeIndex storage — no LakehouseLens dependency)
    # ------------------------------------------------------------------

    def ingest(self, collection: str, data: pa.Table,
               message: str = "") -> str:
        """Ingest feature data. Appends to existing data (union with
        promote_options for schema evolution). Returns the new commit hash.

        Uses ProllyTreeIndex for storage: splits data into row groups,
        stores each as a Parquet blob keyed by rg/{max_pk} in the Prolly
        tree. The feature definition is validated before ingestion.
        """
        defn = self.get_definition(collection)
        # Validate schema: must include entity + timestamp + at least one feature
        required = set(defn["entity_columns"] + [defn["timestamp_column"]])
        actual = set(data.column_names)
        missing = required - actual
        if missing:
            raise ValueError(f"Missing required columns: {missing}")

        key_col = defn["entity_columns"][0] if defn["entity_columns"] else None

        # Read existing data (if any) and union with new data
        if self.collection_exists(collection):
            existing = self.read_features(collection)
            try:
                combined = pa.concat_tables([existing, data], promote_options="default")
            except TypeError:
                combined = pa.concat_tables([existing, data])
        else:
            combined = data

        commit_hash = self._write_row_groups(collection, combined, key_col,
                                       message=message or f"ingest {data.num_rows} rows")
        self._notify_indexers(collection)
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
    # Read features (own ProllyTreeIndex storage — no LakehouseLens dependency)
    # ------------------------------------------------------------------

    def read_features(self, collection: str,
                      commit_hash: Optional[str] = None) -> pa.Table:
        """Read feature data. If commit_hash is None, reads HEAD.

        Reads ALL row groups from the ProllyTreeIndex at the given commit
        and concatenates them into one PyArrow Table.
        """
        if commit_hash is None:
            commit_hash = self.kernel.resolve(self._head_ref(collection))
            if commit_hash is None:
                raise KeyError(f"Collection '{collection}' not found")
        return self._read_all_row_groups(collection, commit_hash)

    def read_branch(self, collection: str, branch_name: str) -> pa.Table:
        """Read a branch's data as a PyArrow Table."""
        ref = self._branch_ref(collection, branch_name)
        commit_hash = self.kernel.resolve(ref)
        if commit_hash is None:
            raise KeyError(f"Branch '{branch_name}' not found in '{collection}'")
        return self.read_features(collection, commit_hash)

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
    # Branching (own implementation — no LakehouseLens dependency)
    # ------------------------------------------------------------------

    def branch(self, collection: str, branch_name: str) -> str:
        """Create a branch for feature experimentation. New features
        can be ingested on the branch without affecting production HEAD."""
        head = self.kernel.resolve(self._head_ref(collection))
        if head is None:
            raise KeyError(f"Collection {collection} has no HEAD")
        self.kernel.reference(self._branch_ref(collection, branch_name), head)
        return head

    def ingest_to_branch(self, collection: str, branch_name: str,
                         data: pa.Table, message: str = "") -> str:
        """Ingest to a branch (not HEAD). Appends to branch's current data.

        Uses ProllyTreeIndex for storage (own implementation).
        """
        defn = self.get_definition(collection)
        key_col = defn["entity_columns"][0] if defn["entity_columns"] else None

        # Read branch's current data and union with new data
        existing = self.read_branch(collection, branch_name)
        try:
            combined = pa.concat_tables([existing, data], promote_options="default")
        except TypeError:
            combined = pa.concat_tables([existing, data])

        commit_hash = self._write_row_groups_to_branch(collection, branch_name, combined,
                                                  key_col,
                                                  message=message or f"branch {branch_name}: ingest {data.num_rows} rows")
        self._notify_indexers(collection)
        return commit_hash

    def merge_branch(self, collection: str, branch_name: str) -> str:
        """Merge a branch into HEAD. Union merge with 2-parent commit.

        Uses ProllyTreeIndex for storage (own implementation).
        """
        head = self.kernel.resolve(self._head_ref(collection))
        branch_ref = self._branch_ref(collection, branch_name)
        branch_head = self.kernel.resolve(branch_ref)
        if branch_head is None:
            raise KeyError(f"Branch {branch_name} not found")

        head_data = self.read_features(collection, head)
        branch_data = self.read_features(collection, branch_head)
        try:
            merged = pa.concat_tables([head_data, branch_data], promote_options="default")
        except TypeError:
            merged = pa.concat_tables([head_data, branch_data])

        commit_hash = self._write_merge(collection, merged, head, branch_head,
                                  message=f"merge branch {branch_name}")
        self._notify_indexers(collection)
        return commit_hash

    # ------------------------------------------------------------------
    # History (inherited from PondLens — handles binary commits natively)
    # ------------------------------------------------------------------
    # history() is inherited from PondLens. It walks the commit chain and
    # handles both binary commits (ProllyLensBase, which is what we use)
    # and legacy JSON commits via type-byte dispatch. No override needed.

    # ==================================================================
    # Internal helpers: ProllyTreeIndex row group storage.
    #
    # DUPLICATED from LakehouseLens intentionally. Per the design
    # principles, production lenses own their own code rather than
    # inheriting from each other. This keeps lenses independent and
    # removable. The storage convention (rg/ row groups in
    # ProllyTreeIndex) is shared, but the code is not.
    # ==================================================================

    def _write_row_groups(self, name: str, table: pa.Table,
                          key_col: Optional[str],
                          message: str = "",
                          row_group_size: Optional[int] = None) -> str:
        """Write a table as row groups in ProllyTreeIndex, commit to HEAD.

        Splits `table` into row groups, stores each as a Parquet blob
        keyed by rg/{max_pk}. REPLACES existing row groups.
        """
        base = ProllyLensBase(self.kernel, name)
        n_rows = table.num_rows

        # Delete existing row group keys (replace, not accumulate)
        existing_state = base.read_all()
        for k in existing_state.keys():
            if k.startswith(_FS_RG_PREFIX):
                base.stage_delete(k)

        if n_rows == 0:
            return base.commit(message or "write: empty table")

        # Default: one row group = whole table (OLAP-style)
        if row_group_size is None:
            row_group_size = max(n_rows, 1)

        # Determine keys
        if key_col is not None and key_col in table.column_names:
            sorted_table = table.sort_by(key_col)
            key_array = sorted_table.column(key_col).to_pylist()
        else:
            sorted_table = table
            key_array = list(range(n_rows))

        # Stage each row group
        for start in range(0, n_rows, row_group_size):
            end = min(start + row_group_size, n_rows)
            group_table = sorted_table.slice(start, end - start)
            parquet_bytes = self._encode_table(group_table)
            parquet_hash = self.kernel.write(parquet_bytes)
            max_pk = key_array[end - 1]
            rg_key = f"{_FS_RG_PREFIX}{max_pk}"
            base.stage(rg_key, parquet_hash)

        n_groups = (n_rows + row_group_size - 1) // row_group_size
        return base.commit(message or f"write: {n_rows} rows in {n_groups} row groups")

    def _write_row_groups_to_branch(self, name: str, branch_name: str,
                                     table: pa.Table,
                                     key_col: Optional[str],
                                     message: str = "") -> str:
        """Write row groups to ProllyTreeIndex and commit to a branch ref."""
        ref = self._branch_ref(name, branch_name)
        parent = self.kernel.resolve(ref)
        if parent is None:
            raise KeyError(f"Branch '{branch_name}' not found in '{name}'")

        # Temporarily point HEAD at the branch so ProllyLensBase builds on it
        original_head = self.kernel.resolve(self._head_ref(name))
        self.kernel.reference(self._head_ref(name), parent)
        try:
            commit_hash = self._write_row_groups(name, table, key_col, message)
        finally:
            if original_head is not None:
                self.kernel.reference(self._head_ref(name), original_head)
        # Move the new commit to the branch ref
        self.kernel.reference(ref, commit_hash)
        return commit_hash

    def _write_merge(self, name: str, table: pa.Table,
                     first_parent: str, second_parent: str,
                     message: str) -> str:
        """Write merged data as row groups and create a 2-parent merge commit."""
        base = ProllyLensBase(self.kernel, name)
        n_rows = table.num_rows

        # Delete existing row group keys
        existing_state = base.read_all()
        for k in existing_state.keys():
            if k.startswith(_FS_RG_PREFIX):
                base.stage_delete(k)

        if n_rows > 0:
            key_array = list(range(n_rows))
            for start in range(0, n_rows, max(n_rows, 1)):
                end = min(start + max(n_rows, 1), n_rows)
                group_table = table.slice(start, end - start)
                parquet_bytes = self._encode_table(group_table)
                parquet_hash = self.kernel.write(parquet_bytes)
                max_pk = key_array[end - 1]
                rg_key = f"{_FS_RG_PREFIX}{max_pk}"
                base.stage(rg_key, parquet_hash)

        # Create a 2-parent merge commit with the staged row groups.
        # Uses the public create_merge_commit() API instead of reaching
        # into _compute_full_state, _staged_add, _staged_del, _commit_index.
        commit_hash = base.create_merge_commit(
            parent=first_parent,
            second_parent=second_parent,
            message=message,
        )
        return commit_hash

    def _read_all_row_groups(self, name: str, commit_hash: str) -> pa.Table:
        """Read ALL row groups from the ProllyTreeIndex at commit_hash."""
        raw = self.kernel.read_blob(commit_hash)

        # Binary commit (type byte 3 = ProllyLensBase commit)
        if len(raw) > 0 and raw[0] == 3:
            commit = BinaryProllyTree.decode_commit(raw)
            snapshot_root = commit.get("snapshot")
            if snapshot_root is None:
                # Delta-only commit — walk to find snapshot
                base = ProllyLensBase(self.kernel, name)
                state = base.read_state_at_commit(commit_hash)
            else:
                state = ProllyTree.read_all(self.kernel, snapshot_root)
        else:
            # Legacy JSON commit
            commit = json.loads(raw)
            if "parquet" in commit:
                parquet_bytes = self.kernel.read(commit["parquet"])
                return self._decode_table(parquet_bytes)
            raise ValueError(f"Cannot decode commit {commit_hash} for '{name}'")

        # Filter to row-group keys
        rg_keys = sorted(k for k in state.keys() if k.startswith(_FS_RG_PREFIX))
        if not rg_keys:
            return pa.table({})

        tables = []
        for k in rg_keys:
            parquet_bytes = self.kernel.read_blob(state[k])
            tables.append(self._decode_table(parquet_bytes))

        try:
            return pa.concat_tables(tables, promote_options="default")
        except TypeError:
            return pa.concat_tables(tables)

    @staticmethod
    def _encode_table(table: pa.Table) -> bytes:
        """Encode a PyArrow Table as Parquet bytes."""
        sink = pa.BufferOutputStream()
        pq.write_table(table, sink)
        return sink.getvalue().to_pybytes()

    @staticmethod
    def _decode_table(parquet_bytes: bytes) -> pa.Table:
        """Decode Parquet bytes into a PyArrow Table."""
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
        # Walk back to find the first commit using inherited history()
        # (handles both binary and JSON commit formats).
        history = fs.history("user_features")
        first_commit = history[-1]["hash"]  # last in history is the first commit
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
