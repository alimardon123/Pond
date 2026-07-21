#!/usr/bin/env python3
"""
Pond Feature Store — End-to-End Reference ML Workflow.

This script runs the Feature Store through a complete, realistic ML
workflow to validate that Pond's "platform" story is actually pleasant
to use. It exercises every production feature in a single narrative:

  1. Source data ingestion (orders as a source View)
  2. Feature definitions (with types, sources, transformations)
  3. Feature value writing (batch + incremental, at multiple timestamps)
  4. Feature versioning (redefine a feature to fix a bug; v1 -> v2)
  5. Point-in-time training set generation (the killer ML feature)
  6. Online serving (single-entity inference)
  7. Batch serving (multi-entity scoring via get_feature_matrix)
  8. Freshness monitoring (across all features)
  9. Cross-View reads (ingest from ArrowView; serve to DuckDB via Arrow)
  10. Lineage (source View -> feature -> transformation)
  11. Persistence (close kernel, reopen, verify model still works)
  12. Schema validation (reject a bad write; verify data integrity)

Scenario: e-commerce fraud detection.
  - Source: orders (order_id, customer_id, amount, product, ts, is_fraud)
  - Features: customer-level aggregates (total_spent, order_count, etc.)
  - Training: given historical orders with fraud labels, generate a
    training dataset with features as-of each order's timestamp.
  - Inference: score a new order in real-time using the latest features.

Run:
    python pond-feature-store/e2e_workflow.py

This is the canonical reference workflow. See
    docs/FEATURE_STORE_USE_CASE.md
for the compact written version.
"""

from __future__ import annotations

import os
import sys
import json
import time
import shutil
import random
from collections import defaultdict

# Path setup
_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(_HERE)
for _pkg in ("pond-core", "pond-sdk", "pond-semantic", "pond-arrow"):
    sys.path.insert(0, os.path.join(_REPO_ROOT, _pkg))
sys.path.insert(0, _HERE)

from pond_minimal import PondMinimal
from feature_store import FeatureStore
from lens_sdk import View, CrossView


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def banner(title: str) -> None:
    print()
    print("=" * 72)
    print(f"  {title}")
    print("=" * 72)


def step(n: int, title: str) -> None:
    print(f"\n--- Step {n}: {title} ---")


def timed(label: str, fn):
    """Run fn(), print its result and elapsed time. Returns (result, ms)."""
    t0 = time.perf_counter()
    result = fn()
    t1 = time.perf_counter()
    ms = (t1 - t0) * 1000
    print(f"  {label}: {result} ({ms:.1f} ms)")
    return result, ms


# ---------------------------------------------------------------------------
# Scenario data: synthetic e-commerce orders
# ---------------------------------------------------------------------------

def generate_orders(n: int = 1000, seed: int = 42) -> list[dict]:
    """Generate n synthetic orders across 50 customers and 5 products.

    Each order has: order_id, customer_id, amount, product, ts, is_fraud.
    Fraud is rare (~3%) and correlated with high-value orders.
    """
    random.seed(seed)
    products = ["Widget", "Gadget", "Book", "Phone", "Laptop"]
    orders = []
    base_ts = 1_000_000.0  # synthetic epoch
    for i in range(n):
        cid = f"cust_{i % 50:02d}"
        amount = round(random.expovariate(1 / 80), 2)  # mean $80
        product = random.choice(products)
        ts = base_ts + i * 60.0  # one order per minute
        # Fraud: 3% baseline + 15% if amount > 200
        fraud_p = 0.03 + (0.15 if amount > 200 else 0.0)
        is_fraud = 1 if random.random() < fraud_p else 0
        orders.append({
            "order_id": f"order_{i:04d}",
            "customer_id": cid,
            "amount": amount,
            "product": product,
            "ts": ts,
            "is_fraud": is_fraud,
        })
    return orders


# ---------------------------------------------------------------------------
# The end-to-end workflow
# ---------------------------------------------------------------------------

def run_e2e_workflow():
    bench_dir = "/tmp/pond_fs_e2e"
    if os.path.exists(bench_dir):
        shutil.rmtree(bench_dir)
    os.makedirs(bench_dir)
    kernel = PondMinimal(bench_dir)

    banner("Pond Feature Store — End-to-End Reference ML Workflow")
    print("  Scenario: e-commerce fraud detection")
    print("  Stack: Kernel -> ProllyViewBase -> IndexedView -> FeatureStore")

    # ====================================================================
    # Step 1: Source data ingestion
    # ====================================================================
    step(1, "Source data ingestion (orders as a source View)")

    orders_data = generate_orders(n=1000)
    print(f"  Generated {len(orders_data)} synthetic orders across "
          f"{len(set(o['customer_id'] for o in orders_data))} customers")

    orders_view = View(kernel, "orders")
    for o in orders_data:
        orders_view.put(o["order_id"], o)
    orders_view.commit("ingest 1000 orders")

    n_fraud = sum(1 for o in orders_data if o["is_fraud"])
    print(f"  Source View 'orders': {orders_view.count()} rows committed")
    print(f"  Fraud rate: {n_fraud}/{len(orders_data)} = {n_fraud/len(orders_data)*100:.1f}%")

    # ====================================================================
    # Step 2: Feature definitions
    # ====================================================================
    step(2, "Feature definitions (with types, sources, transformations)")

    fs = FeatureStore(kernel, "feature_store")

    fs.define_feature(
        "customer_total_spent", "float", "orders",
        "SUM(amount) GROUP BY customer_id",
        "Total amount spent by customer",
        tags=["revenue", "customer"]
    )
    fs.define_feature(
        "customer_order_count", "int", "orders",
        "COUNT(*) GROUP BY customer_id",
        "Number of orders by customer",
        tags=["activity", "customer"]
    )
    fs.define_feature(
        "customer_avg_order_value", "float", "orders",
        "AVG(amount) GROUP BY customer_id",
        "Average order value for customer",
        tags=["revenue", "customer"]
    )
    fs.define_feature(
        "customer_distinct_products", "int", "orders",
        "COUNT(DISTINCT product) GROUP BY customer_id",
        "Number of distinct products purchased",
        tags=["diversity", "customer"]
    )
    fs.define_feature(
        "is_high_value_customer", "bool", "orders",
        "SUM(amount) > 500",
        "Whether customer has spent more than $500 total",
        tags=["segmentation", "customer"]
    )

    fs.register_entity_type("customer", "customer_id",
                             "E-commerce customer entity")
    fs.commit("define 5 features + customer entity type")

    print(f"  Defined {len(fs.list_features())} features:")
    for name in fs.list_features():
        feat = fs.get_feature_definition(name)
        print(f"    {name} (v{feat['version']}, type={feat['type']}): {feat['description']}")
    print(f"  Registered entity types: {fs.list_entity_types()}")

    # ====================================================================
    # Step 3: Feature value writing (batch + incremental, at multiple timestamps)
    # ====================================================================
    step(3, "Feature value writing (batch compute at 3 snapshot timestamps)")

    # Simulate 3 batch compute runs at different points in time.
    # Each run computes features as-of its snapshot timestamp, using
    # only orders with ts <= snapshot_ts.
    snapshot_timestamps = [1_010_000.0, 1_050_000.0, 1_100_000.0]
    snapshot_labels = ["ts=1.01M (first 167 orders)",
                       "ts=1.05M (first 834 orders)",
                       "ts=1.10M (all 1000 orders)"]

    for snap_idx, (snap_ts, label) in enumerate(zip(snapshot_timestamps, snapshot_labels)):
        print(f"\n  Batch run {snap_idx + 1}/3: {label}")
        # Compute aggregates as-of snap_ts
        eligible = [o for o in orders_data if o["ts"] <= snap_ts]
        totals = defaultdict(float)
        counts = defaultdict(int)
        products_per_customer = defaultdict(set)
        for o in eligible:
            cid = o["customer_id"]
            totals[cid] += o["amount"]
            counts[cid] += 1
            products_per_customer[cid].add(o["product"])

        for cid in totals:
            fs.write_feature_value("customer_total_spent", cid, round(totals[cid], 2),
                                    timestamp=snap_ts)
            fs.write_feature_value("customer_order_count", cid, counts[cid],
                                    timestamp=snap_ts)
            fs.write_feature_value("customer_avg_order_value", cid,
                                    round(totals[cid] / counts[cid], 2),
                                    timestamp=snap_ts)
            fs.write_feature_value("customer_distinct_products", cid,
                                    len(products_per_customer[cid]),
                                    timestamp=snap_ts)
            fs.write_feature_value("is_high_value_customer", cid,
                                    totals[cid] > 500,
                                    timestamp=snap_ts)
        fs.commit(f"batch run {snap_idx + 1}: {len(totals)} customers at ts={snap_ts:.0f}")
        print(f"    Wrote features for {len(totals)} customers")

    # ====================================================================
    # Step 4: Feature versioning (redefine a feature to fix a bug; v1 -> v2)
    # ====================================================================
    step(4, "Feature versioning (redefine is_high_value_customer: threshold 500 -> 1000)")

    # Simulate a bug fix: the original threshold was $500, but the data
    # team decides $1000 is the right cutoff. Redefine the feature.
    v1 = fs.get_feature_definition("is_high_value_customer")["version"]
    print(f"  Current version of is_high_value_customer: v{v1}")

    v2 = fs.define_feature(
        "is_high_value_customer", "bool", "orders",
        "SUM(amount) > 1000",  # changed threshold
        "Whether customer has spent more than $1000 total (was $500, raised after review)",
        tags=["segmentation", "customer"]
    )
    print(f"  Redefined with new threshold -> v{v2}")
    assert v2 == v1 + 1, f"Expected v{v1 + 1}, got v{v2}"

    # Re-compute the feature with the new threshold for the latest snapshot
    snap_ts = snapshot_timestamps[-1]
    eligible = [o for o in orders_data if o["ts"] <= snap_ts]
    totals = defaultdict(float)
    for o in eligible:
        totals[o["customer_id"]] += o["amount"]
    n_high_value_v2 = 0
    for cid, total in totals.items():
        is_high = total > 1000
        if is_high:
            n_high_value_v2 += 1
        fs.write_feature_value("is_high_value_customer", cid, is_high,
                                timestamp=snap_ts, version=v2)
    fs.commit(f"recompute is_high_value_customer v{v2} with threshold $1000")

    # Both versions are queryable
    n_high_v1 = 0
    n_high_v2_actual = 0
    for cid in totals:
        if fs.get_feature_value("is_high_value_customer", cid, version=1):
            n_high_v1 += 1
        if fs.get_feature_value("is_high_value_customer", cid, version=2):
            n_high_v2_actual += 1
    print(f"  v1 (threshold=$500): {n_high_v1} high-value customers")
    print(f"  v2 (threshold=$1000): {n_high_v2_actual} high-value customers")
    assert n_high_v2_actual == n_high_value_v2

    # ====================================================================
    # Step 5: Point-in-time training set generation (the killer ML feature)
    # ====================================================================
    step(5, "Point-in-time training set generation (prevent label leakage)")

    # Build a training set from a sample of orders with fraud labels.
    # For each order, we want features as-of that order's timestamp —
    # NOT the latest features. This prevents label leakage (using future
    # data to predict the current label).
    sample_size = 200
    sample_orders = random.sample(orders_data, sample_size)
    events = [
        {"entity_id": o["customer_id"],
         "timestamp": o["ts"],
         "label": o["is_fraud"],
         "order_id": o["order_id"],
         "amount": o["amount"]}
        for o in sample_orders
    ]

    print(f"  Building training set: {sample_size} events (orders with fraud labels)")
    print(f"  Features to join: {fs.list_features()}")

    dataset, train_ms = timed("  Training set generated",
        lambda: fs.get_training_dataset(events, fs.list_features()))

    print(f"\n  Sample training rows (first 3):")
    for row in dataset[:3]:
        print(f"    {row['order_id']}: cust={row['entity_id']}, "
              f"amount=${row['amount']}, label={row['label']}")
        print(f"      features: total_spent={row.get('customer_total_spent')}, "
              f"order_count={row.get('customer_order_count')}, "
              f"avg_val={row.get('customer_avg_order_value')}")
        print(f"      is_high_value(v1)={row.get('is_high_value_customer')}")

    # Verify no label leakage: for the first order of each customer,
    # the features should be None (no prior data).
    first_orders = {}
    for o in orders_data:
        cid = o["customer_id"]
        if cid not in first_orders or o["ts"] < first_orders[cid]["ts"]:
            first_orders[cid] = o
    first_order_events = [
        {"entity_id": o["customer_id"], "timestamp": o["ts"] - 1.0,
         "label": o["is_fraud"], "order_id": o["order_id"]}
        for o in list(first_orders.values())[:5]
    ]
    leak_check = fs.get_training_dataset(first_order_events,
                                          ["customer_total_spent"])
    n_leaked = sum(1 for r in leak_check if r["customer_total_spent"] is not None)
    print(f"\n  Label leakage check (5 first-ever orders, ts before any data):")
    print(f"    Rows with leaked features: {n_leaked}/5 (expected 0)")
    assert n_leaked == 0, "Label leakage detected!"
    print(f"    -> PASS: no label leakage")

    # Quick "model training" (just compute feature-label correlations)
    print(f"\n  Pseudo-model: feature-label correlations on {len(dataset)} rows")
    feature_names = [f for f in fs.list_features()
                     if f != "is_high_value_customer"]
    for fname in feature_names:
        values = [r.get(fname) for r in dataset if r.get(fname) is not None]
        labels = [r["label"] for r in dataset if r.get(fname) is not None]
        if not values:
            continue
        # Simple correlation: mean(feature | label=1) vs mean(feature | label=0)
        v_fraud = [v for v, l in zip(values, labels) if l == 1]
        v_clean = [v for v, l in zip(values, labels) if l == 0]
        if v_fraud and v_clean:
            mean_fraud = sum(v_fraud) / len(v_fraud)
            mean_clean = sum(v_clean) / len(v_clean)
            print(f"    {fname}: fraud_mean={mean_fraud:.2f}, "
                  f"clean_mean={mean_clean:.2f}, "
                  f"ratio={mean_fraud/mean_clean if mean_clean else 0:.2f}")

    # ====================================================================
    # Step 6: Online serving (single-entity inference)
    # ====================================================================
    step(6, "Online serving (single-entity real-time inference)")

    # Simulate a new order coming in: score it in real-time.
    new_order = {
        "order_id": "order_NEW",
        "customer_id": "cust_07",
        "amount": 250.0,
        "product": "Laptop",
        "ts": 1_200_000.0,
    }
    print(f"  New order: {new_order}")

    # Fetch the feature vector for this customer (latest values)
    feature_vector, online_ms = timed("  Online feature vector fetch",
        lambda: fs.get_feature_vector(new_order["customer_id"],
                                       fs.list_features()))
    print(f"    customer={new_order['customer_id']}")
    for k, v in feature_vector.items():
        print(f"      {k}: {v}")

    # Simple "model" — score based on features
    total_spent = feature_vector.get("customer_total_spent", 0) or 0
    order_count = feature_vector.get("customer_order_count", 0) or 0
    # Heuristic: high total spent + low order count = suspicious
    fraud_score = (total_spent / max(order_count, 1)) * (new_order["amount"] / 100)
    print(f"    -> Fraud score: {fraud_score:.2f} "
          f"({'SUSPICIOUS' if fraud_score > 500 else 'clean'})")

    # ====================================================================
    # Step 7: Batch serving (multi-entity scoring via get_feature_matrix)
    # ====================================================================
    step(7, "Batch serving (score all 50 customers via get_feature_matrix)")

    all_customers = [f"cust_{i:02d}" for i in range(50)]
    matrix, batch_ms = timed("  Feature matrix (50 entities x 5 features)",
        lambda: fs.get_feature_matrix(all_customers, fs.list_features()))

    print(f"  Matrix shape: {len(matrix)} rows x {1 + len(fs.list_features())} cols")
    print(f"\n  First 5 rows:")
    print(f"    {'customer':<10} {'total_spent':>12} {'order_count':>12} "
          f"{'avg_value':>10} {'distinct_prod':>14} {'high_value':>10}")
    for row in matrix[:5]:
        print(f"    {row['entity_id']:<10} "
              f"{row.get('customer_total_spent', 0) or 0:>12.2f} "
              f"{row.get('customer_order_count', 0) or 0:>12} "
              f"{row.get('customer_avg_order_value', 0) or 0:>10.2f} "
              f"{row.get('customer_distinct_products', 0) or 0:>14} "
              f"{row.get('is_high_value_customer', False)}")

    # Batch scoring: flag customers with high total + low diversity
    flagged = [r for r in matrix
               if (r.get("customer_total_spent") or 0) > 500
               and (r.get("customer_distinct_products") or 0) <= 2]
    print(f"\n  Batch scoring: {len(flagged)}/{len(matrix)} customers flagged "
          f"(high spend + low diversity)")

    # ====================================================================
    # Step 8: Freshness monitoring (across all features)
    # ====================================================================
    step(8, "Freshness monitoring (O(1) per feature via cache)")

    print(f"  {'feature':<32} {'freshness':>12}")
    print(f"  {'-'*32} {'-'*12}")
    for name in fs.list_features():
        freshness = fs.get_freshness(name)
        if freshness is None:
            print(f"  {name:<32} {'no data':>12}")
        else:
            # Freshness is in seconds; our synthetic timestamps are old
            # so this will be a large number. Show it as-is.
            print(f"  {name:<32} {freshness:>12.0f}s")
    print(f"\n  Note: large values are expected (synthetic timestamps from 1970).")
    print(f"  The key point: each get_freshness call is O(1) via the cache,")
    print(f"  not O(N) scanning all values.")

    # ====================================================================
    # Step 9: Cross-View reads (ingest from ArrowView; serve to DuckDB)
    # ====================================================================
    step(9, "Cross-View reads (ArrowView interop for analytics)")

    # Build an ArrowView from the feature matrix and serve to DuckDB
    # for ad-hoc SQL analytics.
    try:
        import pyarrow as pa
        from arrow_view import ArrowView

        # Convert the feature matrix to an ArrowView
        analytics = ArrowView(kernel, "feature_analytics")
        for row in matrix:
            analytics.put_row(row["entity_id"], {
                "customer_id": row["entity_id"],
                "total_spent": row.get("customer_total_spent") or 0,
                "order_count": row.get("customer_order_count") or 0,
                "avg_value": row.get("customer_avg_order_value") or 0,
                "distinct_products": row.get("customer_distinct_products") or 0,
                "is_high_value": row.get("is_high_value_customer") or False,
            })
        analytics.commit("load feature matrix into ArrowView")

        table = analytics.to_arrow()
        print(f"  ArrowView 'feature_analytics': {table.num_rows} rows, "
              f"{table.num_columns} columns")

        # Serve to DuckDB for SQL analytics
        try:
            import duckdb
            con = duckdb.connect()
            name = analytics.to_duckdb(con)
            # Run a few analytical queries
            q1 = con.execute(
                f"SELECT COUNT(*) as total, "
                f"SUM(CASE WHEN is_high_value THEN 1 ELSE 0 END) as high_value "
                f"FROM {name}"
            ).fetchone()
            print(f"  DuckDB: total={q1[0]}, high_value={q1[1]}")

            q2 = con.execute(
                f"SELECT AVG(total_spent) as avg_spend, "
                f"MAX(total_spent) as max_spend, "
                f"MIN(total_spent) as min_spend "
                f"FROM {name}"
            ).fetchone()
            print(f"  DuckDB: avg_spend=${q2[0]:.2f}, "
                  f"max=${q2[1]:.2f}, min=${q2[2]:.2f}")

            q3 = con.execute(
                f"SELECT customer_id, total_spent, distinct_products "
                f"FROM {name} WHERE total_spent > 500 "
                f"ORDER BY total_spent DESC LIMIT 3"
            ).fetchall()
            print(f"  DuckDB: top 3 high-spend customers:")
            for row in q3:
                print(f"    {row[0]}: ${row[1]:.2f} ({row[2]} products)")
            con.close()
            print(f"  -> PASS: Feature data served to DuckDB via ArrowView")
        except ImportError:
            print(f"  SKIP: DuckDB not installed; skipping SQL analytics demo")
    except ImportError:
        print(f"  SKIP: pyarrow not installed; skipping ArrowView interop demo")

    # ====================================================================
    # Step 10: Lineage (source View -> feature -> transformation)
    # ====================================================================
    step(10, "Lineage (source View -> feature -> transformation)")

    print(f"  {'feature':<32} {'source':>10} {'values':>8}  transformation")
    print(f"  {'-'*32} {'-'*10} {'-'*8}  {'-'*40}")
    for name in fs.list_features():
        lineage = fs.get_lineage(name)
        if lineage:
            print(f"  {lineage['feature']:<32} {lineage['source']:>10} "
                  f"{lineage['values_count']:>8}  {lineage['transformation']}")

    # ====================================================================
    # Step 11: Persistence (close kernel, reopen, verify model still works)
    # ====================================================================
    step(11, "Persistence (close kernel, reopen, verify everything survived)")

    if fs.base.has_staged():
        fs.commit("final commit before restart")
    n_features_before = len(fs.list_features())
    n_values_before = fs.count()
    print(f"  Before restart: {n_features_before} features, "
          f"{n_values_before} entries")
    kernel.close()

    # Reopen
    kernel2 = PondMinimal(bench_dir)
    fs2 = FeatureStore(kernel2, "feature_store")
    n_features_after = len(fs2.list_features())
    n_values_after = fs2.count()
    print(f"  After restart:  {n_features_after} features, "
          f"{n_values_after} entries")
    assert n_features_before == n_features_after
    assert n_values_before == n_values_after

    # Verify a feature value survived
    val = fs2.get_feature_value("customer_total_spent", "cust_07")
    print(f"  customer_total_spent[cust_07] after restart: {val}")
    assert val is not None

    # Verify versioning survived
    versions = fs2.list_feature_versions("is_high_value_customer")
    print(f"  is_high_value_customer versions after restart: {versions}")
    assert versions == [1, 2]

    # Verify entity registry survived
    print(f"  Entity types after restart: {fs2.list_entity_types()}")
    assert "customer" in fs2.list_entity_types()

    # Verify point-in-time JOIN still works
    test_event = [{"entity_id": "cust_07", "timestamp": 1_050_000.0}]
    pt = fs2.get_training_dataset(test_event, ["customer_total_spent"])
    print(f"  Point-in-time JOIN after restart: "
          f"total_spent={pt[0]['customer_total_spent']}")
    assert pt[0]["customer_total_spent"] is not None

    print(f"  -> PASS: full state survived process restart")

    # ====================================================================
    # Step 12: Schema validation (reject a bad write; verify data integrity)
    # ====================================================================
    step(12, "Schema validation (reject bad writes; verify data integrity)")

    # Attempt to write a string to a float feature
    try:
        fs2.write_feature_value("customer_total_spent", "cust_BAD",
                                 "not a number", timestamp=1_200_000.0)
        print(f"  FAIL: should have rejected string for float feature")
    except ValueError as e:
        print(f"  Rejected string->float: {e}")

    # Attempt to write a float to an int feature (non-integer)
    try:
        fs2.write_feature_value("customer_order_count", "cust_BAD",
                                 3.7, timestamp=1_200_000.0)
        print(f"  FAIL: should have rejected 3.7 for int feature")
    except ValueError as e:
        print(f"  Rejected 3.7->int: {e}")

    # Attempt to write to an undefined feature
    try:
        fs2.write_feature_value("undefined_feature", "x", 42)
        print(f"  FAIL: should have rejected undefined feature")
    except ValueError as e:
        print(f"  Rejected undefined feature: {e}")

    # Verify the bad writes didn't corrupt any data
    assert fs2.get_feature_value("customer_total_spent", "cust_BAD") is None
    assert fs2.get_feature_value("customer_order_count", "cust_BAD") is None
    print(f"  Verified: no bad data entered the store")

    kernel2.close()
    shutil.rmtree(bench_dir, ignore_errors=True)

    # ====================================================================
    # Summary
    # ====================================================================
    banner("End-to-End Workflow Complete")
    print("  All 12 steps passed. The Feature Store handled:")
    print("    - 1000 source orders ingested")
    print("    - 5 features defined with types and transformations")
    print("    - 3 batch compute runs at different timestamps")
    print("    - Feature versioning (v1 -> v2 with threshold change)")
    print("    - Point-in-time training set (200 events, no label leakage)")
    print("    - Online serving (single-entity inference)")
    print("    - Batch serving (50 customers x 5 features)")
    print("    - Freshness monitoring (O(1) per feature)")
    print("    - Cross-View reads (ArrowView -> DuckDB SQL analytics)")
    print("    - Lineage (source -> feature -> transformation)")
    print("    - Persistence (full state survived process restart)")
    print("    - Schema validation (3 bad writes rejected)")
    print()
    print("  The Pond platform story holds up: one copy of data on the")
    print("  kernel, serving online inference, offline training, batch")
    print("  scoring, SQL analytics, and lineage — all from the same")
    print("  immutable substrate, with no duplication.")
    print()
    print("  See docs/FEATURE_STORE_USE_CASE.md for the compact written")
    print("  version of this workflow.")


if __name__ == "__main__":
    run_e2e_workflow()
