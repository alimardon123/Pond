"""
Customer Analytics Dashboard — an EXTERNAL USER VALIDATION of the Pond
Feature Store.

This script is written by a developer who has NEVER seen the Pond project
before. It uses only:
  - PondMinimal kernel (3 primitives)
  - pond-sdk (View, IndexedView, CrossView)
  - pond-feature-store (FeatureStore)
  - pond-arrow (ArrowView)
  - standard library

Goal: build a real customer analytics application that exercises the
Feature Store end-to-end:

  1. Ingest 200 synthetic customers
  2. Define 5 raw + 3 derived features
  3. Write feature values
  4. Build a churn-prediction training set via point-in-time JOIN
  5. Online lookup (single customer feature vector)
  6. Batch dashboard (feature matrix for all 200 customers)
  7. Region analytics via ArrowView -> DuckDB SQL
  8. Restart test (close kernel, reopen, verify dashboard works)

Run:
    python validation/customer_analytics_app.py
"""

from __future__ import annotations

import os
import sys
import json
import time
import random
import shutil
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Path setup — the Pond packages live as siblings, not as installed modules.
# I had to figure this out by reading the feature_store.py header (which
# does the same path setup for its own imports).
# ---------------------------------------------------------------------------

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
for _pkg in ("pond-core", "pond-sdk", "pond-feature-store", "pond-arrow"):
    sys.path.insert(0, os.path.join(_REPO_ROOT, _pkg))

from pond_minimal import PondMinimal
from view_sdk import View, CrossView
from feature_store import FeatureStore
from arrow_view import ArrowView

# Try to import duckdb for the SQL analytics section. If it isn't available,
# we'll skip that section gracefully.
try:
    import duckdb
    _HAVE_DUCKDB = True
except ImportError:
    _HAVE_DUCKDB = False


# ---------------------------------------------------------------------------
# Synthetic data generation
# ---------------------------------------------------------------------------

REGIONS = ["NA", "EU", "APAC", "LATAM"]
PLANS = ["free", "pro", "enterprise"]

# Fixed seed so the run is reproducible (also makes "did I break something?"
# easier to answer across restart tests).
RANDOM_SEED = 42


def generate_customers(n: int = 200) -> list[dict]:
    """Generate `n` synthetic customer records.

    Each record has:
        customer_id      str  "cust_000" .. "cust_199"
        signup_date      str  ISO date (UTC)
        signup_ts        float  unix timestamp (seconds)
        region           str  one of REGIONS
        plan             str  one of PLANS
        lifetime_value   float  dollars
        churn_risk_score float  0..1
    """
    rng = random.Random(RANDOM_SEED)
    customers = []
    base_ts = datetime(2023, 1, 1, tzinfo=timezone.utc).timestamp()
    for i in range(n):
        cid = f"cust_{i:03d}"
        # Spread signups across 365 days
        signup_ts = base_ts + rng.randint(0, 365) * 86400
        signup_date = datetime.fromtimestamp(signup_ts, tz=timezone.utc).strftime("%Y-%m-%d")
        region = rng.choice(REGIONS)
        plan = rng.choices(PLANS, weights=[5, 3, 1])[0]
        # Lifetime value: enterprise customers tend to be worth more
        plan_mult = {"free": 1.0, "pro": 3.0, "enterprise": 10.0}[plan]
        ltv = round(rng.uniform(50, 400) * plan_mult, 2)
        # Churn risk: weakly correlated with plan (free customers churn more)
        base_risk = {"free": 0.6, "pro": 0.3, "enterprise": 0.1}[plan]
        risk = round(min(1.0, max(0.0, base_risk + rng.gauss(0, 0.15))), 4)
        customers.append({
            "customer_id": cid,
            "signup_date": signup_date,
            "signup_ts": signup_ts,
            "region": region,
            "plan": plan,
            "lifetime_value": ltv,
            "churn_risk_score": risk,
        })
    return customers


# ---------------------------------------------------------------------------
# Section 1: ingest customers into a source View
# ---------------------------------------------------------------------------

def ingest_customers(kernel: PondMinimal, customers: list[dict]) -> View:
    """Ingest the customer records as a regular Pond View.

    I'm using a plain View here (not an IndexedView) because the source
    data is just a snapshot — I don't need auto-indexing on it.
    """
    src = View(kernel, "customers_src")
    for c in customers:
        src.put(c["customer_id"], c)
    src.commit(f"ingest {len(customers)} customers")
    return src


# ---------------------------------------------------------------------------
# Section 2 + 3: define features and write values
# ---------------------------------------------------------------------------

def define_features(fs: FeatureStore, now_ts: float) -> None:
    """Define the 5 raw + 3 derived features.

    Note: `transformation` is a descriptive string only — the FeatureStore
    does NOT execute transformations. I had to compute the derived feature
    values myself and write them like raw feature values. This is
    documented as future work in FEATURE_STORE_USE_CASE.md.
    """
    # 5 raw features
    fs.define_feature("customer_ltv", "float", "customers_src",
                      "lifetime_value field",
                      "Customer lifetime value in dollars")
    fs.define_feature("customer_churn_risk", "float", "customers_src",
                      "churn_risk_score field",
                      "Customer churn risk score 0..1")
    fs.define_feature("customer_region", "string", "customers_src",
                      "region field",
                      "Customer region code (NA/EU/APAC/LATAM)")
    fs.define_feature("customer_plan_tier", "string", "customers_src",
                      "plan field",
                      "Customer plan tier (free/pro/enterprise)")
    fs.define_feature("customer_tenure_days", "int", "customers_src",
                      "floor((now - signup_ts) / 86400)",
                      "Days since customer signup")

    # 3 derived features
    fs.define_feature("is_high_value", "bool", "customers_src",
                      "lifetime_value > 1000",
                      "Whether LTV exceeds $1000")
    fs.define_feature("is_at_risk", "bool", "customers_src",
                      "churn_risk_score > 0.7",
                      "Whether churn risk exceeds 0.7")
    fs.define_feature("region_avg_ltv", "float", "customers_src",
                      "AVG(lifetime_value) GROUP BY region",
                      "Average LTV for the customer's region")

    fs.register_entity_type("customer", "customer_id",
                            "Application customer")
    fs.commit(f"define 8 features + customer entity type @ ts={int(now_ts)}")


def write_feature_values(fs: FeatureStore, customers: list[dict],
                          write_ts: float) -> None:
    """Write all 8 feature values for every customer at `write_ts`.

    For the derived features:
      - is_high_value: computed from lifetime_value
      - is_at_risk:    computed from churn_risk_score
      - region_avg_ltv: computed by grouping customers by region and
        averaging lifetime_value. This requires a GROUP BY operation,
        which the FeatureStore doesn't provide natively — I compute the
        aggregates externally and write them as feature values.
    """
    # Pre-compute region averages (the GROUP BY the FeatureStore can't do)
    region_ltv_sum: dict[str, float] = {r: 0.0 for r in REGIONS}
    region_ltv_cnt: dict[str, int] = {r: 0 for r in REGIONS}
    for c in customers:
        r = c["region"]
        region_ltv_sum[r] += c["lifetime_value"]
        region_ltv_cnt[r] += 1
    region_avg = {r: round(region_ltv_sum[r] / region_ltv_cnt[r], 2)
                  for r in REGIONS}

    for c in customers:
        cid = c["customer_id"]
        # Raw features
        fs.write_feature_value("customer_ltv", cid, c["lifetime_value"],
                                timestamp=write_ts)
        fs.write_feature_value("customer_churn_risk", cid,
                                c["churn_risk_score"], timestamp=write_ts)
        fs.write_feature_value("customer_region", cid, c["region"],
                                timestamp=write_ts)
        fs.write_feature_value("customer_plan_tier", cid, c["plan"],
                                timestamp=write_ts)
        # tenure_days: int. The schema validator rejects non-integer floats
        # (e.g. 25.5), so I have to ensure this is a real int.
        tenure_days = int((write_ts - c["signup_ts"]) // 86400)
        fs.write_feature_value("customer_tenure_days", cid, tenure_days,
                                timestamp=write_ts)

        # Derived features
        fs.write_feature_value("is_high_value", cid,
                                c["lifetime_value"] > 1000.0,
                                timestamp=write_ts)
        fs.write_feature_value("is_at_risk", cid,
                                c["churn_risk_score"] > 0.7,
                                timestamp=write_ts)
        fs.write_feature_value("region_avg_ltv", cid,
                                region_avg[c["region"]],
                                timestamp=write_ts)

    fs.commit(f"batch write 8 features x {len(customers)} customers @ ts={int(write_ts)}")
    return region_avg


# ---------------------------------------------------------------------------
# Section 4: build a churn prediction training set
# ---------------------------------------------------------------------------

def build_training_events(customers: list[dict], n_events: int = 50
                           ) -> list[dict]:
    """Generate `n_events` historical churn events.

    Each event has:
        entity_id  str  customer_id
        timestamp  float  event time (>= write_ts so features are visible)
        did_churn  int   0 or 1 (the label)
        region     str   (preserved as context, not used as a feature
                          here — the feature store joins features only)
    """
    rng = random.Random(RANDOM_SEED + 1)
    events = []
    # We need event timestamps AFTER the feature write_ts so point-in-time
    # JOIN returns non-None values. The caller passes write_ts separately.
    # For now, just pick customers and assign plausible churn outcomes
    # based on churn_risk_score (so the training set has signal).
    for _ in range(n_events):
        c = rng.choice(customers)
        # Did the customer actually churn? Correlated with risk score.
        did_churn = 1 if rng.random() < c["churn_risk_score"] else 0
        events.append({
            "entity_id": c["customer_id"],
            # timestamp filled in by caller (we need it >= write_ts)
            "did_churn": did_churn,
            "region": c["region"],
        })
    return events


def training_set_demo(fs: FeatureStore, customers: list[dict],
                       write_ts: float, n_events: int = 50) -> None:
    """Build and print a churn prediction training set via point-in-time JOIN."""
    print("\n--- 4. Training set via point-in-time JOIN ---")
    events = build_training_events(customers, n_events=n_events)
    # Assign event timestamps strictly after write_ts so features are
    # visible (point-in-time correctness: feature values with ts <= event_ts)
    rng = random.Random(RANDOM_SEED + 2)
    for e in events:
        e["timestamp"] = write_ts + rng.randint(1, 7) * 86400  # 1..7 days later

    feature_names = [
        "customer_ltv", "customer_churn_risk", "customer_region",
        "customer_plan_tier", "customer_tenure_days",
        "is_high_value", "is_at_risk", "region_avg_ltv",
    ]
    dataset = fs.get_training_dataset(events, feature_names)

    print(f"  Built {len(dataset)} training rows from {len(events)} events.")
    print(f"  Each row has {len(feature_names)} feature columns + event fields.")
    # Show 3 sample rows
    print(f"\n  Sample rows (first 3):")
    for row in dataset[:3]:
        # Trim the row for readability
        trim = {k: v for k, v in row.items()
                if k in ("entity_id", "timestamp", "did_churn",
                         "customer_ltv", "customer_churn_risk",
                         "is_at_risk", "region_avg_ltv")}
        print(f"    {trim}")

    # Label leakage check: every event_ts > write_ts, so all features
    # should be present (non-None) for all events.
    leaked_or_missing = sum(1 for r in dataset
                            if any(r.get(f) is None for f in feature_names))
    print(f"\n  Label-leakage / missing-feature check: "
          f"{leaked_or_missing}/{len(dataset)} rows have at least one None feature.")
    if leaked_or_missing == 0:
        print(f"  PASS: no missing features (events all after write_ts).")
    else:
        print(f"  WARN: some rows have None features — investigate.")

    # Pseudo-model: churn correlation
    churned = [r for r in dataset if r["did_churn"] == 1]
    clean   = [r for r in dataset if r["did_churn"] == 0]
    if churned and clean:
        avg_risk_churned = sum(r["customer_churn_risk"] for r in churned) / len(churned)
        avg_risk_clean   = sum(r["customer_churn_risk"] for r in clean)   / len(clean)
        print(f"\n  Pseudo-model signal:")
        print(f"    avg churn_risk for churned: {avg_risk_churned:.3f}")
        print(f"    avg churn_risk for clean:   {avg_risk_clean:.3f}")
        if avg_risk_churned > avg_risk_clean:
            print(f"    PASS: churned customers have higher risk scores.")


# ---------------------------------------------------------------------------
# Section 5: online lookup (single customer feature vector)
# ---------------------------------------------------------------------------

def online_lookup_demo(fs: FeatureStore, customer_id: str) -> None:
    print("\n--- 5. Online lookup (single customer) ---")
    t0 = time.perf_counter()
    feature_names = [
        "customer_ltv", "customer_churn_risk", "customer_region",
        "customer_plan_tier", "customer_tenure_days",
        "is_high_value", "is_at_risk", "region_avg_ltv",
    ]
    vec = fs.get_feature_vector(customer_id, feature_names)
    elapsed_ms = (time.perf_counter() - t0) * 1000
    print(f"  customer_id: {customer_id}")
    for k, v in vec.items():
        print(f"    {k}: {v}")
    print(f"  Latency: {elapsed_ms:.2f} ms")


# ---------------------------------------------------------------------------
# Section 6: batch dashboard (feature matrix for ALL customers)
# ---------------------------------------------------------------------------

def batch_dashboard_demo(fs: FeatureStore, customers: list[dict]) -> list[dict]:
    print("\n--- 6. Batch dashboard (feature matrix, all 200 customers) ---")
    entity_ids = [c["customer_id"] for c in customers]
    feature_names = [
        "customer_ltv", "customer_churn_risk", "customer_region",
        "customer_plan_tier", "customer_tenure_days",
        "is_high_value", "is_at_risk", "region_avg_ltv",
    ]
    t0 = time.perf_counter()
    matrix = fs.get_feature_matrix(entity_ids, feature_names)
    elapsed_ms = (time.perf_counter() - t0) * 1000
    print(f"  matrix shape: {len(matrix)} rows x {1 + len(feature_names)} cols")
    print(f"  Latency: {elapsed_ms:.2f} ms "
          f"({elapsed_ms / len(matrix):.3f} ms/customer)")

    # Sanity: no None values expected (all customers have all features)
    n_missing = sum(1 for row in matrix
                    if any(row.get(f) is None for f in feature_names))
    print(f"  rows with any None feature: {n_missing}/{len(matrix)}")

    # Show a 5-row preview of the dashboard table
    print(f"\n  Dashboard preview (first 5 rows):")
    print(f"    {'customer_id':<14}{'region':<8}{'plan':<12}{'ltv':>9}{'risk':>7}{'tenure':>8}{'hi_val':>8}{'at_risk':>8}{'rgn_avg':>10}")
    for row in matrix[:5]:
        print(f"    {row['entity_id']:<14}{row['customer_region']:<8}"
              f"{row['customer_plan_tier']:<12}{row['customer_ltv']:>9.2f}"
              f"{row['customer_churn_risk']:>7.3f}"
              f"{row['customer_tenure_days']:>8}"
              f"{str(row['is_high_value']):>8}"
              f"{str(row['is_at_risk']):>8}"
              f"{row['region_avg_ltv']:>10.2f}")

    # Compute dashboard summary
    n_high_value = sum(1 for r in matrix if r["is_high_value"])
    n_at_risk    = sum(1 for r in matrix if r["is_at_risk"])
    avg_ltv      = sum(r["customer_ltv"] for r in matrix) / len(matrix)
    avg_risk     = sum(r["customer_churn_risk"] for r in matrix) / len(matrix)
    print(f"\n  Dashboard summary:")
    print(f"    total customers:  {len(matrix)}")
    print(f"    high-value (LTV>$1000): {n_high_value}")
    print(f"    at-risk (risk>0.7):     {n_at_risk}")
    print(f"    avg LTV:          ${avg_ltv:.2f}")
    print(f"    avg churn risk:   {avg_risk:.3f}")
    return matrix


# ---------------------------------------------------------------------------
# Section 7: region analytics via ArrowView -> DuckDB
# ---------------------------------------------------------------------------

def region_analytics_demo(kernel: PondMinimal, matrix: list[dict]) -> None:
    print("\n--- 7. Region analytics via ArrowView -> DuckDB ---")
    if not _HAVE_DUCKDB:
        print("  (duckdb not installed — skipping)")
        return

    av = ArrowView(kernel, "customer_analytics")
    # Each row in the matrix is the feature vector for one customer.
    # ArrowView.put_row wants (primary_key, row_dict). The matrix row
    # already has entity_id; I'll use it as the primary key too.
    for row in matrix:
        pk = row["entity_id"]
        # put_row mutates the row (adds _pk field). I make a copy to
        # avoid mutating the caller's matrix.
        av.put_row(pk, dict(row))
    av.commit(f"load {len(matrix)} customer feature rows for SQL analytics")

    con = duckdb.connect()
    table_name = av.to_duckdb(con, "customers")
    print(f"  Registered {av.count_rows()} rows as DuckDB table '{table_name}'.")

    # Query 1: AVG LTV by region (the GROUP BY that the FeatureStore can't do)
    print(f"\n  Query: SELECT region, AVG(customer_ltv) FROM customers GROUP BY region")
    rows = con.execute(
        "SELECT customer_region, AVG(customer_ltv), COUNT(*) "
        "FROM customers GROUP BY customer_region ORDER BY customer_region"
    ).fetchall()
    for r in rows:
        print(f"    {r[0]}: avg_ltv=${r[1]:.2f}, n={r[2]}")

    # Query 2: at-risk high-value customers
    print(f"\n  Query: SELECT customer_id, customer_ltv, customer_churn_risk "
          f"FROM customers WHERE is_at_risk AND is_high_value "
          f"ORDER BY customer_churn_risk DESC LIMIT 5")
    rows = con.execute(
        "SELECT entity_id, customer_ltv, customer_churn_risk "
        "FROM customers WHERE is_at_risk AND is_high_value "
        "ORDER BY customer_churn_risk DESC LIMIT 5"
    ).fetchall()
    for r in rows:
        print(f"    {r[0]}: ltv=${r[1]:.2f}, risk={r[2]:.3f}")

    # Query 3: plan tier distribution
    print(f"\n  Query: SELECT customer_plan_tier, COUNT(*) FROM customers GROUP BY customer_plan_tier")
    rows = con.execute(
        "SELECT customer_plan_tier, COUNT(*) "
        "FROM customers GROUP BY customer_plan_tier "
        "ORDER BY customer_plan_tier"
    ).fetchall()
    for r in rows:
        print(f"    {r[0]}: {r[1]}")

    con.close()


# ---------------------------------------------------------------------------
# Section 8: restart test
# ---------------------------------------------------------------------------

def restart_test(bench_dir: str, customers: list[dict],
                  sample_customer_id: str) -> bool:
    print("\n--- 8. Restart test (close kernel, reopen) ---")
    # We'll re-open a NEW kernel against the SAME bench_dir. The FeatureStore
    # should reconstruct all features, values, and versioning from the
    # kernel's content-addressed object store.
    kernel2 = PondMinimal(bench_dir)
    fs2 = FeatureStore(kernel2, "feature_store")

    features = fs2.list_features()
    print(f"  features after restart: {features}")
    expected = {"customer_ltv", "customer_churn_risk", "customer_region",
                "customer_plan_tier", "customer_tenure_days",
                "is_high_value", "is_at_risk", "region_avg_ltv"}
    missing = expected - set(features)
    if missing:
        print(f"  FAIL: missing features after restart: {missing}")
        kernel2.close()
        return False

    entities = fs2.list_entity_types()
    print(f"  entity types after restart: {entities}")
    if "customer" not in entities:
        print(f"  FAIL: entity type 'customer' not present after restart")
        kernel2.close()
        return False

    # Verify one customer's feature vector still resolves
    vec = fs2.get_feature_vector(sample_customer_id, sorted(expected))
    print(f"  feature vector for {sample_customer_id} after restart:")
    for k in sorted(vec.keys()):
        print(f"    {k}: {vec[k]}")

    # Verify point-in-time JOIN still works (re-run a small one)
    write_ts = customers[0]["signup_ts"] + 86400 * 400  # any post-write ts
    test_events = [
        {"entity_id": sample_customer_id, "timestamp": write_ts + 86400,
         "did_churn": 0},
    ]
    rows = fs2.get_training_dataset(test_events, ["customer_ltv", "is_at_risk"])
    if not rows or rows[0].get("customer_ltv") is None:
        print(f"  FAIL: point-in-time JOIN returned None after restart")
        kernel2.close()
        return False
    print(f"  point-in-time JOIN after restart: PASS "
          f"(customer_ltv={rows[0]['customer_ltv']}, "
          f"is_at_risk={rows[0]['is_at_risk']})")

    # Freshness cache survives? (the _meta/latest_ts/{feature} blobs are
    # in the kernel, so they should)
    fresh = fs2.get_freshness("customer_ltv")
    if fresh is None:
        print(f"  WARN: freshness cache for 'customer_ltv' missing after restart "
              f"(falls back to O(N) scan)")
    else:
        print(f"  freshness for 'customer_ltv' after restart: {fresh:.0f}s")

    kernel2.close()
    print(f"  Restart test: PASS")
    return True


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 70)
    print("Pond Feature Store — Customer Analytics Dashboard (external validation)")
    print("=" * 70)

    # Use a fresh bench dir so the run is reproducible
    bench_dir = "/tmp/pond_customer_analytics"
    if os.path.exists(bench_dir):
        shutil.rmtree(bench_dir)
    os.makedirs(bench_dir)

    kernel = PondMinimal(bench_dir)

    # --- Section 1: synthetic data + source View ---
    print("\n--- 1. Synthetic customer data + source View ---")
    customers = generate_customers(200)
    print(f"  generated {len(customers)} synthetic customers")
    src = ingest_customers(kernel, customers)
    print(f"  source View 'customers_src' count: {src.count()}")

    # --- Section 2 + 3: FeatureStore + feature definitions ---
    print("\n--- 2 + 3. FeatureStore: define 8 features + write values ---")
    fs = FeatureStore(kernel, "feature_store")

    # Use a "now" timestamp that is comfortably after all signups so
    # tenure_days is positive and stable across the run.
    now_ts = datetime(2024, 1, 1, tzinfo=timezone.utc).timestamp()
    define_features(fs, now_ts)
    print(f"  defined {len(fs.list_features())} features")
    print(f"  feature list: {fs.list_features()}")

    # Write all feature values at one batch timestamp
    write_ts = now_ts
    region_avg = write_feature_values(fs, customers, write_ts)
    print(f"  wrote 8 features x {len(customers)} customers = "
          f"{8 * len(customers)} feature value records")
    print(f"  feature_store count (incl. metadata): {fs.count()}")
    print(f"  region averages (precomputed externally): {region_avg}")

    # --- Section 4: training set ---
    training_set_demo(fs, customers, write_ts, n_events=50)

    # --- Section 5: online lookup ---
    sample_id = customers[0]["customer_id"]
    online_lookup_demo(fs, sample_id)

    # --- Section 6: batch dashboard ---
    matrix = batch_dashboard_demo(fs, customers)

    # --- Section 7: region analytics via DuckDB ---
    region_analytics_demo(kernel, matrix)

    # --- Section 8: restart test ---
    # Make sure everything staged is committed before close.
    # (We've been committing throughout, but defensively check.)
    if fs.base.has_staged():
        fs.commit("final commit before restart")
    kernel.close()
    restart_ok = restart_test(bench_dir, customers, sample_id)

    # Summary
    print("\n" + "=" * 70)
    print("Dashboard run complete.")
    print(f"  restart test: {'PASS' if restart_ok else 'FAIL'}")
    print("=" * 70)


if __name__ == "__main__":
    main()
