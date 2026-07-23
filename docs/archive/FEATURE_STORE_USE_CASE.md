# Feature Store Reference Use Case

> A compact, concrete walkthrough of the Pond Feature Store running
> a realistic ML workflow end-to-end. This is NOT an RFC — it's a
> use case document. The goal is to show that Pond's "platform story"
> is actually pleasant to use, not just architecturally clean.
>
> **Companion code:** `pond-feature-store/e2e_workflow.py` runs this
> exact scenario. All 12 steps pass.

---

## Scenario: E-commerce Fraud Detection

An e-commerce platform wants to detect fraudulent orders in
real-time. The data team needs to:

1. Ingest order events as they arrive.
2. Compute customer-level aggregate features (total spent, order
   count, average value, product diversity).
3. Train a fraud-classification model on historical orders, using
   features as-of each order's timestamp (no label leakage).
4. Serve the model online: when a new order arrives, fetch the
   customer's latest features and score it.
5. Monitor feature freshness.
6. Run ad-hoc SQL analytics on feature data via DuckDB.
7. Redefine features when business logic changes (versioning).
8. Trust that all of this survives process restarts.

This is the canonical feature-store workload. The rest of this
document walks through each step concretely.

---

## 1. Input data

**Source View:** `orders` — 1000 synthetic order events across 50
customers and 5 products.

```python
orders_data = [
    {"order_id": "order_0000", "customer_id": "cust_00",
     "amount": 84.32, "product": "Widget", "ts": 1000000.0,
     "is_fraud": 0},
    {"order_id": "order_0001", "customer_id": "cust_01",
     "amount": 152.10, "product": "Laptop", "ts": 1000060.0,
     "is_fraud": 1},
    # ... 998 more ...
]
```

Ingested as a regular Pond `View`:

```python
orders = View(kernel, "orders")
for o in orders_data:
    orders.put(o["order_id"], o)
orders.commit("ingest 1000 orders")
```

**Fraud rate:** ~3% baseline + 15% for high-value orders (amount > $200).

---

## 2. Feature definitions

Five features, each with a declared type, source View, and
transformation description:

| Feature | Type | Source | Transformation |
|---|---|---|---|
| `customer_total_spent` | float | orders | `SUM(amount) GROUP BY customer_id` |
| `customer_order_count` | int | orders | `COUNT(*) GROUP BY customer_id` |
| `customer_avg_order_value` | float | orders | `AVG(amount) GROUP BY customer_id` |
| `customer_distinct_products` | int | orders | `COUNT(DISTINCT product) GROUP BY customer_id` |
| `is_high_value_customer` | bool | orders | `SUM(amount) > 500` |

```python
fs = FeatureStore(kernel, "feature_store")
fs.define_feature("customer_total_spent", "float", "orders",
                   "SUM(amount) GROUP BY customer_id",
                   "Total amount spent by customer")
# ... 4 more ...
fs.register_entity_type("customer", "customer_id", "E-commerce customer")
fs.commit("define 5 features + customer entity type")
```

**Schema validation** is enforced on every write. A string written
to a float feature is rejected with `ValueError`. This prevents
corrupt data from breaking downstream ML models.

---

## 3. Feature value writing (batch compute at 3 snapshots)

Three batch compute runs, each producing features as-of a snapshot
timestamp (using only orders with `ts <= snapshot_ts`):

| Run | Snapshot ts | Eligible orders | Customers updated |
|---|---|---|---|
| 1 | 1,010,000 | ~167 | ~17 |
| 2 | 1,050,000 | ~834 | ~50 |
| 3 | 1,100,000 | 1000 | 50 |

```python
for snap_ts in [1_010_000.0, 1_050_000.0, 1_100_000.0]:
    eligible = [o for o in orders_data if o["ts"] <= snap_ts]
    # compute aggregates...
    for cid in totals:
        fs.write_feature_value("customer_total_spent", cid,
                                round(totals[cid], 2), timestamp=snap_ts)
        # ... 4 more features ...
    fs.commit(f"batch run at ts={snap_ts}")
```

Each write is **schema-validated** (type-checked against the feature's
declared type) and updates the **freshness cache**
(`_meta/latest_ts/{feature_name}`) for O(1) freshness queries.

---

## 4. Feature versioning (redefining a feature)

The data team decides the `$500` threshold for
`is_high_value_customer` is too low. They redefine it to `$1000`:

```python
v2 = fs.define_feature("is_high_value_customer", "bool", "orders",
                        "SUM(amount) > 1000",  # changed threshold
                        "Whether customer has spent more than $1000 total")
# v2 = 2 (v1 is preserved)
```

Both versions remain queryable:

```python
fs.get_feature_value("is_high_value_customer", "cust_07", version=1)  # True (threshold=$500)
fs.get_feature_value("is_high_value_customer", "cust_07", version=2)  # True (threshold=$1000)
fs.get_feature_value("is_high_value_customer", "cust_07")             # True (latest = v2)
```

This enables **reproducible ML training**: train against v1 while v2
is in development, then switch when ready.

---

## 5. Training dataset creation (point-in-time JOIN)

**The killer feature.** Given 200 historical orders with fraud
labels, generate a training dataset where each row has the features
as-of that order's timestamp — preventing label leakage.

```python
events = [
    {"entity_id": "cust_07", "timestamp": 1000060.0,
     "label": 1, "order_id": "order_0001", "amount": 152.10},
    # ... 199 more ...
]
dataset = fs.get_training_dataset(events, fs.list_features())
```

Result: each row has the original event fields plus 5 feature
columns, with values looked up via **binary search** on per-entity
timelines.

**Label leakage check:** for the first-ever order of each customer
(timestamp before any feature data exists), the features are `None` —
no future data leaked into the training set.

**Pseudo-model output** (feature-label correlations on the 200-row
training set):

| Feature | Fraud mean | Clean mean | Ratio |
|---|---|---|---|
| `customer_total_spent` | $726.55 | $478.40 | 1.52 |
| `customer_order_count` | 7.83 | 5.94 | 1.32 |
| `customer_avg_order_value` | $97.42 | $79.34 | 1.23 |
| `customer_distinct_products` | 3.33 | 3.15 | 1.06 |

Fraudulent orders tend to come from customers with higher total spend
and more orders — a sensible signal for a fraud model.

---

## 6. Online serving (single-entity inference)

A new order arrives. Fetch the customer's latest feature vector and
score it:

```python
new_order = {"order_id": "order_NEW", "customer_id": "cust_07",
             "amount": 250.0, "product": "Laptop", "ts": 1200000.0}

feature_vector = fs.get_feature_vector("cust_07", fs.list_features())
# {'customer_avg_order_value': 83.86,
#  'customer_distinct_products': 5,
#  'customer_order_count': 20,
#  'customer_total_spent': 1677.28,
#  'is_high_value_customer': True}

# Simple heuristic model:
fraud_score = (feature_vector["customer_total_spent"]
               / max(feature_vector["customer_order_count"], 1)) \
              * (new_order["amount"] / 100)
# -> 209.66 (clean)
```

**Measured latency:** 4.5 ms for the feature vector fetch (O(log N)
via the `by_entity` index, plus the freshness cache).

---

## 7. Batch serving (multi-entity scoring)

Score all 50 customers at once via `get_feature_matrix`:

```python
matrix = fs.get_feature_matrix(
    entity_ids=[f"cust_{i:02d}" for i in range(50)],
    feature_names=fs.list_features()
)
# 50 rows x 6 columns (entity_id + 5 features)
```

**Measured latency:** 12.6 ms for 50 entities x 5 features.

This is O(N + M·log N) instead of O(N·M·log N) for the naive
`get_feature_vector` loop — ~500× faster at scale (10K entities x 50
features).

Batch scoring flags customers with high spend + low product
diversity (a fraud signal in this scenario).

---

## 8. Freshness monitoring

Each `write_feature_value` call updates a cached "latest timestamp"
under `_meta/latest_ts/{feature_name}`. `get_freshness` reads this
cache in **O(1)** instead of scanning all values:

```python
for name in fs.list_features():
    freshness = fs.get_freshness(name)  # O(1) per feature
    print(f"{name}: {freshness:.0f}s ago")
```

This matters for monitoring dashboards that check freshness across
hundreds of features every few seconds.

---

## 9. Cross-View reads (ArrowView -> DuckDB)

The feature matrix is loaded into an `ArrowView` and served to
DuckDB for ad-hoc SQL analytics — without copying data out of Pond:

```python
analytics = ArrowView(kernel, "feature_analytics")
for row in matrix:
    analytics.put_row(row["entity_id"], {**row, "customer_id": row["entity_id"]})
analytics.commit("load feature matrix")

import duckdb
con = duckdb.connect()
analytics.to_duckdb(con, "feature_analytics")

# Now run SQL on Pond data:
con.execute("SELECT AVG(total_spent), MAX(total_spent), MIN(total_spent) FROM feature_analytics").fetchone()
# ($1661.39, $2552.56, $1086.64)

con.execute("SELECT customer_id, total_spent FROM feature_analytics WHERE total_spent > 500 ORDER BY total_spent DESC LIMIT 3").fetchall()
# cust_39: $2552.56, cust_27: $2380.97, cust_19: $2307.13
```

**One copy of data** on the Pond kernel, serving both online
inference (via FeatureStore) and analytical SQL (via ArrowView ->
DuckDB). No ETL pipeline, no data duplication.

---

## 10. Lineage

For each feature, the Feature Store tracks its source View and
transformation:

```python
for name in fs.list_features():
    lineage = fs.get_lineage(name)
    # {'feature': 'customer_total_spent', 'version': 1,
    #  'source': 'orders', 'transformation': 'SUM(amount) GROUP BY customer_id',
    #  'type': 'float', 'values_count': 150}
```

This answers "where did this feature come from?" without a separate
lineage system.

---

## 11. Persistence (restart)

Close the kernel, reopen it, verify everything survived:

```python
kernel.close()
kernel2 = PondMinimal(bench_dir)
fs2 = FeatureStore(kernel2, "feature_store")

# 5 features, 800 entries — same as before restart
fs2.list_features()  # ['customer_avg_order_value', 'customer_distinct_products',
                     #  'customer_order_count', 'customer_total_spent',
                     #  'is_high_value_customer']

# Feature values survived
fs2.get_feature_value("customer_total_spent", "cust_07")  # 1677.28

# Versioning survived
fs2.list_feature_versions("is_high_value_customer")  # [1, 2]

# Entity registry survived
fs2.list_entity_types()  # ['customer']

# Point-in-time JOIN still works
fs2.get_training_dataset([{"entity_id": "cust_07", "timestamp": 1050000.0}],
                          ["customer_total_spent"])
# [{'customer_total_spent': 1552.1}]
```

All data is in the kernel's content-addressed object store
(SQLite + filesystem by default; S3, FDB, Redis also supported).
The Feature Store itself is stateless — it reads everything from the
kernel on each call.

---

## 12. Schema validation (data integrity)

Three bad-write attempts, all rejected:

```python
# String to float feature
fs.write_feature_value("customer_total_spent", "x", "not a number")
# ValueError: Value 'not a number' (type=str) does not match feature type 'float'.

# Non-integer float to int feature
fs.write_feature_value("customer_order_count", "x", 3.7)
# ValueError: Value 3.7 (type=float) does not match feature type 'int'.

# Write to undefined feature
fs.write_feature_value("undefined_feature", "x", 42)
# ValueError: Feature 'undefined_feature' (version None) is not defined.
```

No bad data entered the store. Downstream ML models are protected
from type-mismatch corruption.

---

## What this proves

The Pond platform story holds up under a realistic ML workload:

1. **One copy of data** on the kernel serves online inference,
   offline training, batch scoring, SQL analytics, and lineage —
   without duplication or ETL.
2. **The killer feature (point-in-time JOIN)** works correctly,
   preventing label leakage. This is what distinguishes a real
   feature store from a key-value store.
3. **Schema validation, versioning, and persistence** are
   production-grade. Bad writes are rejected; old versions remain
   queryable; data survives restarts.
4. **Cross-View reads** via ArrowView let the same data serve both
   the FeatureStore (for ML) and DuckDB (for SQL analytics) without
   copying.
5. **The kernel stayed frozen.** All of this is built on the 3
   primitives (Write, Read, Reference) at ~140 LOC. No kernel
   modifications, no special cases.

The architecture is not just clean on paper — it's pleasant to use
in practice. The "platform validation" phase is complete for the
Feature Store flagship.

---

## Measurements summary

| Operation | Latency | Notes |
|---|---|---|
| Ingest 1000 orders | (batch) | One commit |
| Define 5 features + entity type | (batch) | One commit |
| 3 batch compute runs (50 customers x 5 features x 3 snapshots) | (batch) | 3 commits |
| Point-in-time JOIN, 200 events x 5 features | ~50 ms | Binary search per entity timeline |
| Online feature vector fetch (1 entity x 5 features) | 4.5 ms | O(log N) via index |
| Batch feature matrix (50 entities x 5 features) | 12.6 ms | O(N + M·log N) |
| get_freshness (per feature) | O(1) | Via cache |
| DuckDB SQL on 50-row ArrowView | <5 ms | Via ArrowView.to_duckdb |
| Process restart + full state recovery | <100 ms | Kernel reopen |

All measurements on a single-node prototype with the filesystem
backend. Production backends (S3, FDB) will have different
latency profiles but the same algorithmic complexity.

---

## What this use case does NOT cover (future work)

These are deliberately out of scope for the reference use case:

1. **Streaming ingestion.** Currently batch-only. A streaming
   variant would subscribe to a `StreamingView` and ingest
   incrementally.
2. **Feature transformations.** The user computes feature values
   externally. A transformation engine would let the user define
   `transformation="SUM(amount) GROUP BY customer_id"` and have
   the Feature Store compute it.
3. **Materialized feature tables.** For 10K+ QPS online serving,
   a materialized table (per RFC-0005) would pre-compute the
   latest value per entity per feature.
4. **Distributed coordination.** Multi-writer Feature Store
   requires Raft/CRDT. Deferred until "what is replicated?" is
   answered (per the roadmap).
5. **Liquid-clustering materialization.** Per
   `docs/LIQUID_CLUSTERING_COMPARISON.md`, a future
   `ClusteredFeatureStore` could sort feature values by Hilbert
   curve on (entity_id, timestamp) for faster range queries. This
   is a Layer 2 materialization, not a Feature Store concern.

---

## Companion files

- **Code:** `pond-feature-store/e2e_workflow.py` — runs all 12 steps.
- **Implementation:** `pond-feature-store/feature_store.py` — the
  FeatureStore class (~600 LOC).
- **CLI:** `pond-feature-store/cli.py` — 16-command CLI.
- **Spec:** `rfcs/RFC-0011-feature-store.md` — the production
  specification.
- **SDK contract:** `SDK_SPEC.md` — the SDK the Feature Store is
  built on.

Run the workflow yourself:
```bash
python pond-feature-store/e2e_workflow.py
```
