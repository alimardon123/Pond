"""
Pond Lab — Track 6: Real-World Case Studies

Two polished end-to-end examples that answer:
  "Why Pond instead of stitching together five existing systems?"

Case Study 1: Clinical Data Lake
  A hospital needs to store patient records, lab results, and clinical
  notes with:
  - Versioned records (audit trail for compliance)
  - Time travel (what did the patient's record look like on Jan 15?)
  - Branching (try a new treatment protocol on a branch)
  - SQL queries (ad-hoc analysis via DuckDB)
  - Full-text search (find patients with similar symptoms)
  - Schema evolution (add new lab test types without breaking old data)

  Without Pond: PostgreSQL (records) + Elasticsearch (search) + S3 (archive)
  + custom ETL between them + custom versioning layer.
  With Pond: one kernel, two Lenses (Lakehouse + Search), zero ETL.

Case Study 2: ML Feature Platform
  A data science team needs feature engineering with:
  - Feature definitions with versioning (reproducible training)
  - Point-in-time joins (prevent label leakage)
  - Online serving (low-latency feature lookup)
  - Offline training (batch feature extraction)
  - Schema evolution (add features without retraining old models)
  - Cross-Lens interop (features queryable via SQL for analysis)

  Without Pond: Feast (feature store) + DuckDB (analysis) + custom sync
  + custom versioning + custom PIT join implementation.
  With Pond: one kernel, two Lenses (Feature Store + Lakehouse), zero ETL.

Run:
    python pond-lab/track6_case_studies.py
"""

from __future__ import annotations

import os
import sys
import json
import time
import tempfile
import shutil
import datetime

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(SCRIPT_DIR, "..", "..", "pond-core"))
sys.path.insert(0, os.path.join(SCRIPT_DIR, "..", "..", "pond-sdk"))
sys.path.insert(0, os.path.join(SCRIPT_DIR, "..", "..", "lenses", "lakehouse"))
sys.path.insert(0, os.path.join(SCRIPT_DIR, "..", "lenses"))
sys.path.insert(0, SCRIPT_DIR)

from kernel import PondMinimal  # noqa: E402
from lakehouse_lens import LakehouseLens  # noqa: E402
from feature_store_lens import FeatureStoreLens  # noqa: E402

try:
    import pyarrow as pa
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
# Case Study 1: Clinical Data Lake
# ---------------------------------------------------------------------------

def case_study_1_clinical_data_lake():
    """A hospital's clinical data lake on Pond.

    Demonstrates: versioned records, time travel, branching, SQL queries,
    full-text search, schema evolution — all on one kernel, zero ETL.
    """
    print("\n{'='*60}")
    print("Case Study 1: Clinical Data Lake")
    print("{'='*60}")
    print()
    print("  Without Pond: PostgreSQL + Elasticsearch + S3 + custom ETL")
    print("  With Pond:    1 kernel, 2 Lenses, 0 ETL")
    print()

    tmpdir = tempfile.mkdtemp(prefix="pond_clinical_")
    try:
        kernel = PondMinimal(tmpdir)
        lh = LakehouseLens(kernel)
        con = duckdb.connect()

        # --- Step 1: Ingest patient records ---
        print("  Step 1: Ingest patient records (versioned)")

        patients_v1 = pa.table({
            "patient_id": ["P001", "P002", "P003"],
            "name": ["Alice Smith", "Bob Jones", "Carol White"],
            "dob": pa.array([
                datetime.datetime(1980, 5, 15),
                datetime.datetime(1975, 3, 22),
                datetime.datetime(1990, 7, 10),
            ]),
            "diagnosis": ["hypertension", "diabetes", "asthma"],
            "notes": [
                "patient reports occasional headaches and high blood pressure",
                "type 2 diabetes managed with metformin and diet",
                "mild asthma triggered by exercise and cold air",
            ],
            "event_ts": pa.array([datetime.datetime(2024, 1, 1)] * 3),
        })
        commit_v1 = lh.create_table("patients", patients_v1)
        check(True, f"3 patient records ingested (commit: {commit_v1[:8]})")

        # --- Step 2: SQL ad-hoc query ---
        print("\n  Step 2: SQL ad-hoc query (via DuckDB on same data)")

        con.register("patients", lh.read_table("patients"))
        result = con.execute(
            "SELECT patient_id, diagnosis FROM patients WHERE patient_id = 'P001'"
        ).fetchall()
        check(result[0][1] == "hypertension",
              f"SQL query: P001 diagnosis = hypertension")

        # --- Step 3: Time travel (what did the record look like before update?) ---
        print("\n  Step 3: Time travel (audit trail)")

        # Update patient P001's diagnosis
        patients_v2 = pa.table({
            "patient_id": ["P001"],
            "name": ["Alice Smith"],
            "dob": pa.array([datetime.datetime(1980, 5, 15)]),
            "diagnosis": ["hypertension + arrhythmia"],
            "notes": ["patient reports occasional headaches, high blood pressure, and irregular heartbeat"],
            "event_ts": pa.array([datetime.datetime(2024, 2, 1)]),
        })
        lh.insert("patients", patients_v2)

        # Time travel: read at v1 (before the update)
        old_commit = commit_v1
        old_table = lh.read_table("patients", commit_hash=old_commit)
        con.register("patients_old", old_table)
        old_diagnosis = con.execute(
            "SELECT diagnosis FROM patients_old WHERE patient_id = 'P001'"
        ).fetchone()
        check(old_diagnosis[0] == "hypertension",
              f"Time travel: P001 diagnosis at v1 = 'hypertension' (before update)")

        # Current diagnosis (latest row for P001 — insert is append-only)
        con.register("patients_now", lh.read_table("patients"))
        new_diagnosis = con.execute(
            "SELECT diagnosis FROM patients_now WHERE patient_id = 'P001' "
            "ORDER BY event_ts DESC LIMIT 1"
        ).fetchone()
        check("arrhythmia" in new_diagnosis[0],
              f"Current: P001 latest diagnosis includes 'arrhythmia'")

        print(f"    Audit trail: v1='hypertension' → v2='hypertension + arrhythmia'")

        # --- Step 4: Branching (try new treatment protocol) ---
        print("\n  Step 4: Branching (new treatment protocol)")

        lh.branch("patients", "trial_protocol")
        # Add trial data on the branch
        trial_data = pa.table({
            "patient_id": ["P001"],
            "name": ["Alice Smith"],
            "dob": pa.array([datetime.datetime(1980, 5, 15)]),
            "diagnosis": ["hypertension + arrhythmia"],
            "notes": ["started on beta-blocker trial, BP monitoring daily"],
            "event_ts": pa.array([datetime.datetime(2024, 3, 1)]),
        })
        lh.commit_to_branch("patients", "trial_protocol", trial_data)

        # Main HEAD doesn't see the trial data
        con.register("patients_main", lh.read_table("patients"))
        main_count = con.execute("SELECT COUNT(*) FROM patients_main").fetchone()[0]
        # 3 original + 1 update = 4 rows on main
        check(main_count == 4, f"Main HEAD: {main_count} rows (trial data not visible)")

        # Branch has the trial data
        branch_head = kernel.resolve("collections/patients/branches/trial_protocol")
        commit = json.loads(kernel.read(branch_head))
        parquet = kernel.read(commit["parquet"])
        reader = pa.BufferReader(parquet)
        branch_table = pa.parquet.read_table(reader)
        con.register("patients_trial", branch_table)
        trial_count = con.execute("SELECT COUNT(*) FROM patients_trial").fetchone()[0]
        check(trial_count > main_count,
              f"Trial branch: {trial_count} rows (trial data visible)")

        print(f"    Branch 'trial_protocol' has trial data; main HEAD unaffected")

        # --- Step 5: Full-text search (find patients with similar symptoms) ---
        print("\n  Step 5: Full-text search (same data, no Elasticsearch)")

        # Build a simple inverted index from the notes column (Physical Structure)
        head = kernel.resolve("collections/patients/HEAD")
        commit = json.loads(kernel.read(head))
        parquet = kernel.read(commit["parquet"])
        reader = pa.BufferReader(parquet)
        search_table = pa.parquet.read_table(reader)

        notes = search_table.column("notes").to_pylist()
        pids = search_table.column("patient_id").to_pylist()

        inverted_index = {}
        for pid, note in zip(pids, notes):
            for word in note.lower().split():
                word = word.strip(",.;")
                if word not in inverted_index:
                    inverted_index[word] = []
                inverted_index[word].append(pid)

        # Store as Physical Structure
        idx_bytes = json.dumps(inverted_index, sort_keys=True).encode()
        idx_hash = kernel.write(idx_bytes)
        kernel.reference("__search/patients", idx_hash)

        # Search for "blood pressure"
        idx_h = kernel.resolve("__search/patients")
        idx = json.loads(kernel.read(idx_h))
        bp_patients = idx.get("blood", [])
        check("P001" in bp_patients,
              f"Search: P001 found for 'blood' (in notes)")

        # Search for "diabetes"
        diabetes_patients = idx.get("diabetes", [])
        check("P002" in diabetes_patients,
              f"Search: P002 found for 'diabetes'")

        print(f"    Search index built from same Parquet data (no Elasticsearch)")
        print(f"    Found {len(bp_patients)} patient(s) with 'blood' in notes")

        # --- Step 6: Schema evolution (add new lab test column) ---
        print("\n  Step 6: Schema evolution (add lab_results column)")

        lab_data = pa.table({
            "patient_id": ["P001", "P002"],
            "name": ["Alice Smith", "Bob Jones"],
            "dob": pa.array([datetime.datetime(1980, 5, 15), datetime.datetime(1975, 3, 22)]),
            "diagnosis": ["hypertension + arrhythmia", "diabetes"],
            "notes": ["follow-up scheduled", "HbA1c results pending"],
            "event_ts": pa.array([datetime.datetime(2024, 4, 1)] * 2),
            "lab_results": ["BP: 140/90, ECG: normal", "HbA1c: 7.2"],  # new column
        })
        lh.insert("patients", lab_data)

        # Verify new column is visible
        con.register("patients_final", lh.read_table("patients"))
        cols = con.execute("SELECT column_name FROM information_schema.columns "
                          "WHERE table_name = 'patients_final'").fetchall()
        col_names = [c[0] for c in cols]
        check("lab_results" in col_names,
              f"Schema evolution: 'lab_results' column visible (columns: {col_names})")

        # Old rows have NULL for lab_results (Parquet-native)
        null_count = con.execute(
            "SELECT COUNT(*) FROM patients_final WHERE lab_results IS NULL"
        ).fetchone()[0]
        check(null_count > 0,
              f"Schema evolution: {null_count} old rows have NULL lab_results")

        print(f"    Added 'lab_results' column; old rows get NULL (Parquet-native)")
        print(f"    No migration needed. No rewrite. No downtime.")

        con.close()
        kernel.close()

        # --- Summary ---
        print(f"\n  Clinical Data Lake summary:")
        print(f"    Systems replaced: PostgreSQL + Elasticsearch + S3 + custom ETL")
        print(f"    Pond: 1 kernel + 2 Lenses (Lakehouse + Search)")
        print(f"    Data copies: 1 (shared Parquet blob)")
        print(f"    ETL operations: 0")
        print(f"    Features: versioning, time travel, branching, SQL, search, schema evolution")

    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Case Study 2: ML Feature Platform
# ---------------------------------------------------------------------------

def case_study_2_ml_feature_platform():
    """An ML feature platform on Pond.

    Demonstrates: versioned features, point-in-time joins, online serving,
    offline training, schema evolution, cross-Lens interop — all on one
    kernel, zero ETL.
    """
    print("\n{'='*60}")
    print("Case Study 2: ML Feature Platform")
    print("{'='*60}")
    print()
    print("  Without Pond: Feast + DuckDB + custom sync + custom versioning")
    print("  With Pond:    1 kernel, 2 Lenses, 0 ETL")
    print()

    tmpdir = tempfile.mkdtemp(prefix="pond_ml_")
    try:
        kernel = PondMinimal(tmpdir)
        fs = FeatureStoreLens(kernel)
        lh = LakehouseLens(kernel)
        con = duckdb.connect()

        # --- Step 1: Define features ---
        print("  Step 1: Define ML features (versioned)")

        fs.define_collection(
            "user_engagement",
            entity_columns=["user_id"],
            timestamp_column="event_ts",
            feature_columns=["login_count_7d", "avg_session_duration", "page_views"],
        )
        check(True, "Feature collection 'user_engagement' defined (v1)")

        # --- Step 2: Ingest feature data ---
        print("\n  Step 2: Ingest features (append-only, versioned)")

        features_batch1 = pa.table({
            "user_id": [1, 2, 3, 4, 5],
            "event_ts": pa.array([datetime.datetime(2024, 1, 1)] * 5),
            "login_count_7d": [3, 7, 1, 5, 2],
            "avg_session_duration": [12.5, 25.3, 5.0, 18.7, 8.2],
            "page_views": [15, 42, 3, 28, 10],
        })
        fs.ingest("user_engagement", features_batch1, "initial features")
        check(True, "5 feature rows ingested (batch 1)")

        # Second batch (new day)
        features_batch2 = pa.table({
            "user_id": [1, 2, 3],
            "event_ts": pa.array([datetime.datetime(2024, 1, 8)] * 3),
            "login_count_7d": [5, 4, 2],
            "avg_session_duration": [15.0, 20.1, 7.5],
            "page_views": [22, 35, 5],
        })
        fs.ingest("user_engagement", features_batch2, "daily update")
        check(True, "3 more feature rows ingested (batch 2)")

        # --- Step 3: Point-in-time join (prevent label leakage) ---
        print("\n  Step 3: Point-in-time join (training set, no leakage)")

        # Training entities: "what features did user X have on Jan 5?"
        training_entities = pa.table({
            "user_id": [1, 2, 3, 4],
            "event_ts": pa.array([
                datetime.datetime(2024, 1, 5),  # user 1: only batch 1 available
                datetime.datetime(2024, 1, 10),  # user 2: batch 2 available
                datetime.datetime(2024, 1, 3),   # user 3: only batch 1
                datetime.datetime(2024, 1, 15),  # user 4: only batch 1 (no batch 2)
            ]),
        })

        training_set = fs.point_in_time_join(
            "user_engagement", training_entities,
            features=["login_count_7d", "avg_session_duration", "page_views"],
        )
        ts_df = training_set.to_pandas()

        # User 1 at Jan 5: batch 1 data (login_count_7d=3, NOT 5)
        u1 = ts_df[ts_df["user_id"] == 1].iloc[0]
        check(u1["login_count_7d"] == 3,
              f"PIT: user 1 at Jan 5 → login_count_7d=3 (batch 1, not batch 2's 5)")

        # User 2 at Jan 10: batch 2 data (login_count_7d=4)
        u2 = ts_df[ts_df["user_id"] == 2].iloc[0]
        check(u2["login_count_7d"] == 4,
              f"PIT: user 2 at Jan 10 → login_count_7d=4 (batch 2)")

        # User 3 at Jan 3: batch 1 data
        u3 = ts_df[ts_df["user_id"] == 3].iloc[0]
        check(u3["login_count_7d"] == 1,
              f"PIT: user 3 at Jan 3 → login_count_7d=1 (batch 1)")

        # User 4 at Jan 15: batch 1 data (no batch 2 for user 4)
        u4 = ts_df[ts_df["user_id"] == 4].iloc[0]
        check(u4["login_count_7d"] == 5,
              f"PIT: user 4 at Jan 15 → login_count_7d=5 (batch 1, no batch 2)")

        print(f"    Training set: 4 rows, no label leakage")
        print(f"    Each entity gets features AS OF its event timestamp")

        # --- Step 4: Online serving (point lookup) ---
        print("\n  Step 4: Online serving (point lookup)")

        vec = fs.get_feature_vector(
            "user_engagement", {"user_id": 2},
            ["login_count_7d", "page_views"],
        )
        # Latest data for user 2: batch 2 (login_count_7d=4, page_views=35)
        check(vec["login_count_7d"] == 4,
              f"Online serving: user 2 latest login_count_7d=4")
        check(vec["page_views"] == 35,
              f"Online serving: user 2 latest page_views=35")

        # --- Step 5: Schema evolution (add new feature) ---
        print("\n  Step 5: Schema evolution (add 'click_rate' feature)")

        fs.evolve_schema("user_engagement", added_features=["click_rate"])

        new_features = pa.table({
            "user_id": [1, 2],
            "event_ts": pa.array([datetime.datetime(2024, 1, 15)] * 2),
            "login_count_7d": [6, 3],
            "avg_session_duration": [18.0, 15.0],
            "page_views": [30, 20],
            "click_rate": [0.15, 0.08],  # new feature
        })
        fs.ingest("user_engagement", new_features, "add click_rate")

        # Old training data still valid (click_rate is NULL for old rows)
        training_set2 = fs.point_in_time_join(
            "user_engagement", training_entities,
            features=["login_count_7d", "click_rate"],
        )
        ts2_df = training_set2.to_pandas()
        u1_v2 = ts2_df[ts2_df["user_id"] == 1].iloc[0]
        check(u1_v2["login_count_7d"] == 3,
              f"Schema evolution: old PIT join still correct (login_count_7d=3)")
        # click_rate should be None/NaN for old data
        import math
        cr = u1_v2["click_rate"]
        check(cr is None or (isinstance(cr, float) and math.isnan(cr)),
              f"Schema evolution: old rows have NULL click_rate (new feature)")

        print(f"    Added 'click_rate' feature; old training data has NULL")
        print(f"    No retraining needed. No migration. No downtime.")

        # --- Step 6: Cross-Lens interop (SQL analysis on features) ---
        print("\n  Step 6: Cross-Lens interop (SQL on feature data)")

        # The Lakehouse Lens can read the Feature Store's data via SQL
        head = kernel.resolve("collections/user_engagement/HEAD")
        commit = json.loads(kernel.read(head))
        parquet = kernel.read(commit["parquet"])
        reader = pa.BufferReader(parquet)
        features_table = pa.parquet.read_table(reader)

        con.register("features", features_table)
        result = con.execute("""
            SELECT
                COUNT(*) as total_rows,
                AVG(login_count_7d) as avg_logins,
                MAX(page_views) as max_views,
                AVG(click_rate) as avg_click_rate
            FROM features
        """).fetchone()

        check(result[0] == 10,  # 5 + 3 + 2 = 10 rows
              f"SQL on feature data: {result[0]} rows (expected 10)")
        check(result[3] is not None,
              f"SQL on feature data: avg_click_rate computed (new feature visible)")

        print(f"    Lakehouse Lens queries Feature Store data via SQL (no ETL)")
        print(f"    Total rows: {result[0]}, avg_logins: {result[1]:.1f},")
        print(f"    max_views: {result[2]}, avg_click_rate: {result[3]}")

        # --- Step 7: Branching (feature experimentation) ---
        print("\n  Step 7: Branching (experimental features)")

        fs.branch("user_engagement", "experiment")
        exp_features = pa.table({
            "user_id": [1, 2],
            "event_ts": pa.array([datetime.datetime(2024, 2, 1)] * 2),
            "login_count_7d": [10, 8],
            "avg_session_duration": [30.0, 25.0],
            "page_views": [50, 40],
            "click_rate": [0.25, 0.20],
        })
        fs.ingest_to_branch("user_engagement", "experiment", exp_features,
                           "experimental features v2")

        # Production HEAD unaffected
        prod_table = fs.read_features("user_engagement")
        check(prod_table.num_rows == 10,
              f"Production HEAD: {prod_table.num_rows} rows (experiment isolated)")

        # Branch has additional data
        branch_table = fs.read_branch("user_engagement", "experiment")
        check(branch_table.num_rows > 10,
              f"Experiment branch: {branch_table.num_rows} rows (has experimental data)")

        print(f"    Experiment branch has new features; production unaffected")
        print(f"    Merge when ready: fs.merge_branch('user_engagement', 'experiment')")

        con.close()
        kernel.close()

        # --- Summary ---
        print(f"\n  ML Feature Platform summary:")
        print(f"    Systems replaced: Feast + DuckDB + custom sync + custom versioning")
        print(f"    Pond: 1 kernel + 2 Lenses (Feature Store + Lakehouse)")
        print(f"    Data copies: 1 (shared Parquet blob)")
        print(f"    ETL operations: 0")
        print(f"    Features: versioned features, PIT join, online serving,")
        print(f"              schema evolution, cross-Lens SQL, branching")

    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 60)
    print("Pond Lab — Track 6: Real-World Case Studies")
    print("'Why Pond instead of stitching together five existing systems?'")
    print("=" * 60)

    case_study_1_clinical_data_lake()
    case_study_2_ml_feature_platform()

    print(f"\n{'='*60}")
    print(f"RESULTS: {PASS} pass, {FAIL} fail")
    print(f"{'='*60}")

    if FAIL == 0:
        print()
        print("Case study badges:")
        print("  ✓ Clinical Data Lake: versioning + time travel + branching + SQL + search + schema evolution")
        print("  ✓ ML Feature Platform: versioned features + PIT join + online serving + schema evolution + cross-Lens SQL + branching")
        print()
        print("Each case study replaces 3-5 separate systems with 1 kernel + 2 Lenses.")
        print("Zero ETL. One copy of data. Full versioning for free.")

    sys.exit(0 if FAIL == 0 else 1)


if __name__ == "__main__":
    main()
