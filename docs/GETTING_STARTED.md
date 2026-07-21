# Getting Started with the Pond Feature Store

> A 5-minute onboarding path. By the end of this doc, you'll have
> defined a feature, written values, and served them online and
> offline — all on the Pond kernel.
>
> **This is the way Pond is supposed to feel.** If anything here
> feels awkward, that's a finding — please report it.

---

## What is the Pond Feature Store?

A feature store for ML: define features, write feature values, serve
them online (point lookup) and offline (batch + point-in-time JOIN
for training sets). Built on the Pond kernel — a 3-primitive
content-addressed object store (~140 LOC). No JVM, no Spark, no
separate online/offline stores. One copy of data serves all workloads.

**In 60 lines of Python, you get:**
- Schema-validated feature writes (rejects type mismatches)
- Feature versioning (redefine → v2; both versions queryable)
- Point-in-time training set generation (prevents label leakage)
- Online serving (O(log N) via composite index)
- Batch serving (single-scan feature matrix)
- O(1) freshness monitoring
- Persistence (data survives restart)
- ArrowView integration (serve to DuckDB/Polars/pandas via Arrow IPC)

---

## Prerequisites

Python 3.12+. The Pond packages live in the repo — no pip install
needed (yet; a `pyproject.toml` is future work).

```bash
git clone https://github.com/alimardon123/Pond.git
cd Pond
```

The packages are:
- `pond-core/` — the kernel (3 primitives, ~140 LOC, FROZEN)
- `pond-sdk/` — View, IndexedView, KeylessView, CrossView, tombstones
- `pond-feature-store/` — the FeatureStore (this guide)
- `pond-arrow/` — ArrowView adapter (optional, for DuckDB/Polars interop)

---

## Your first feature store (5 minutes)

```python
import sys, os
# Path setup — add Pond packages to sys.path
HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)  # or wherever you cloned Pond
for pkg in ("pond-core", "pond-sdk", "pond-feature-store"):
    sys.path.insert(0, os.path.join(REPO, pkg))

from pond_minimal import PondMinimal
from feature_store import FeatureStore

# 1. Create a kernel (the storage substrate)
kernel = PondMinimal("/tmp/my_feature_store")

# 2. Create a FeatureStore (extends IndexedView extends ProllyViewBase extends Kernel)
fs = FeatureStore(kernel, "my_fs")

# 3. Define a feature (type-checked on every write)
fs.define_feature(
    "user_age", "int", "users",
    transformation="age field",  # descriptive only — you compute the value
    description="User's age in years"
)
fs.commit("define user_age feature")

# 4. Write feature values (schema-validated)
fs.write_feature_value("user_age", "user:1", 25, timestamp=1000.0)
fs.write_feature_value("user_age", "user:2", 30, timestamp=1000.0)
fs.write_feature_value("user_age", "user:3", "twenty-five", timestamp=1000.0)
# -> ValueError: Value 'twenty-five' (type=str) does not match feature type 'int'.
fs.commit("write user ages")

# 5. Online serving (point lookup, O(log N))
age = fs.get_feature_value("user_age", "user:1")
print(f"user:1 age = {age}")  # 25

# 6. Batch serving (feature matrix)
matrix = fs.get_feature_matrix(
    entity_ids=["user:1", "user:2", "user:3"],
    feature_names=["user_age"]
)
print(matrix)
# [{'entity_id': 'user:1', 'user_age': 25},
#  {'entity_id': 'user:2', 'user_age': 30},
#  {'entity_id': 'user:3', 'user_age': None}]

kernel.close()
```

That's it. You have a working feature store with schema validation,
versioning, online serving, and batch serving.

---

## The killer feature: point-in-time training sets

The point-in-time JOIN is what makes a feature store useful for ML
training. Without it, you get **label leakage** — your training set
accidentally includes feature values from the future, leading to
over-optimistic models that fail in production.

```python
# Write feature values at multiple timestamps
fs.define_feature("user_balance", "float", "transactions", "running balance")
fs.write_feature_value("user_balance", "u1", 100.0, timestamp=1000.0)
fs.write_feature_value("user_balance", "u1", 50.0,  timestamp=2000.0)  # withdrawal
fs.write_feature_value("user_balance", "u1", 75.0,  timestamp=3000.0)  # deposit
fs.commit("balance history")

# Training events: 3 events for u1 at different timestamps
events = [
    {"entity_id": "u1", "timestamp": 500.0,  "label": 0},  # before any balance
    {"entity_id": "u1", "timestamp": 1500.0, "label": 1},  # balance was 100
    {"entity_id": "u1", "timestamp": 2500.0, "label": 0},  # balance was 50
    {"entity_id": "u1", "timestamp": 3500.0, "label": 1},  # balance was 75
]

# Generate the training set — features joined as-of each event timestamp
dataset = fs.get_training_dataset(events, ["user_balance"])
# dataset[0]["user_balance"] = None    (ts=500, no balance yet)
# dataset[1]["user_balance"] = 100.0   (ts=1500, correct)
# dataset[2]["user_balance"] = 50.0    (ts=2500, correct)
# dataset[3]["user_balance"] = 75.0    (ts=3500, correct)
```

No label leakage. The training set only includes feature values that
were known at each event's timestamp.

---

## Feature versioning (reproducible ML)

Redefine a feature → version increments. Both versions remain queryable.

```python
v1 = fs.define_feature("is_high_value", "bool", "orders", "SUM(amount) > 500")
fs.write_feature_value("is_high_value", "u1", True, timestamp=1000.0, version=1)
fs.commit("v1")

# Bug fix: threshold should be $1000, not $500
v2 = fs.define_feature("is_high_value", "bool", "orders", "SUM(amount) > 1000")
# v2 = 2

fs.write_feature_value("is_high_value", "u1", False, timestamp=1000.0, version=2)
fs.commit("v2")

# Both versions queryable
fs.get_feature_value("is_high_value", "u1", version=1)  # True (threshold=$500)
fs.get_feature_value("is_high_value", "u1", version=2)  # False (threshold=$1000)
fs.get_feature_value("is_high_value", "u1")             # False (latest = v2)
```

Train against v1 while v2 is in development, then switch when ready.

---

## Cross-View reads (ArrowView → DuckDB)

Serve feature data to DuckDB for ad-hoc SQL analytics — without
copying data out of Pond:

```python
import sys
sys.path.insert(0, os.path.join(REPO, "pond-arrow"))
from arrow_view import ArrowView
import duckdb

# Load the feature matrix into an ArrowView
analytics = ArrowView(kernel, "analytics")
for row in matrix:
    analytics.put_row(row["entity_id"], {**row, "customer_id": row["entity_id"]})
analytics.commit("load feature matrix")

# Serve to DuckDB
con = duckdb.connect()
analytics.to_duckdb(con, "customers")

# Run SQL on Pond data
print(con.execute(
    "SELECT customer_id, user_age FROM customers WHERE user_age > 28"
).fetchall())
```

One copy of data on the Pond kernel, serving both the FeatureStore
(for ML) and DuckDB (for SQL analytics). No ETL pipeline.

---

## Persistence (restart)

All data is in the kernel's content-addressed object store. Close
the kernel, reopen it, everything survives:

```python
kernel.close()

# Later (or in a different process):
kernel2 = PondMinimal("/tmp/my_feature_store")
fs2 = FeatureStore(kernel2, "my_fs")

print(fs2.list_features())  # ['user_age', 'user_balance', 'is_high_value']
print(fs2.get_feature_value("user_age", "user:1"))  # 25
print(fs2.list_feature_versions("is_high_value"))    # [1, 2]
```

No migrations, no schema registration. The FeatureStore reads
everything from the kernel on each call.

---

## The mental model

```
Kernel (Write, Read, Reference — 3 primitives, ~140 LOC, FROZEN)
  → ProllyViewBase (delta commits + Prolly trees + branching)
    → IndexedView (auto-indexing: lazy/eager/incremental)
      → FeatureStore (features + versioning + point-in-time JOIN + batch serving)
```

Four layers of composition. Each layer uses ONLY the layer below.
The kernel never changes. All richness is in the View layer.

---

## Elegant cross-view reading (ViewQuery)

A View feels like a **collection**: iterable, filterable, joinable.
This is the "direct, easy, simple and elegant way of reading data"
per the architecture review.

```python
# Iterate rows directly
for row in orders:
    print(row["order_id"], row["amount"])

# len(view) and `key in view` work
print(f"{len(orders)} orders")
if "order:1" in orders:
    print("order:1 exists")

# Filter with kwargs (field=value)
for row in orders.where(region="US"):
    ...

# Filter with a predicate
for row in orders.where(lambda r: r["amount"] > 100):
    ...

# Project
for row in orders.select("order_id", "amount"):
    ...

# Chain: where + select + map (lazy, nothing runs until you iterate)
us_totals = (orders
             .where(region="US")
             .select("order_id", "amount")
             .map(lambda r: {**r, "amount_usd": r["amount"] * 1.1})
             .collect())  # .collect() forces evaluation

# Cross-view JOIN
for row in orders.join(customers, on="customer_id"):
    print(row["order_id"], row["customer_name"])

# Chain join + where + select
us_orders = (orders
             .join(customers, on="customer_id")
             .where(region="US")
             .select("order_id", "amount", "name")
             .collect())
```

**Why this matters:** the query is LAZY. Nothing is read from the
View until you iterate or `.collect()`. This allows a future
execution engine (SQL, Polars, DataFusion) to push down filters and
projections to the kernel level — the API is designed for that
future optimizer. Today the evaluation is Python-level, but the
interface is already the right shape.

---

## Common pitfalls

1. **`transformation` is descriptive only.** The `transformation`
   argument to `define_feature` is a human-readable label, NOT
   executed. You must compute the feature value yourself and pass it
   to `write_feature_value`. A transformation engine is future work.

2. **`get_freshness` returns event-timestamp age, not wall-clock age.**
   If you write with `timestamp=1704067200` (Jan 1 2024) and call
   `get_freshness` from 2026, it returns ~80 million seconds. This is
   correct — freshness is about the data's nominal timestamp, not
   when you wrote it. For wall-clock freshness, pass `time.time()` as
   the `timestamp` argument.

3. **`write_feature_value` requires the feature to be defined first.**
   You can't write a value to an undefined feature. Call
   `define_feature` first, or you'll get a `ValueError`.

4. **Commits are required.** `write_feature_value` stages the write;
   you must call `fs.commit(message)` to make it durable. Uncommitted
   writes are lost on process exit.

5. **Schema validation rejects type mismatches.** `int` features
   reject strings and non-integer floats (e.g., `25.5`). `bool`
   features reject `1` and `0` (use `True`/`False`). See
   `SDK_SPEC.md` §2.1 for the full validation table.

---

## Where to go next

- **Full API reference:** `pond-feature-store/feature_store.py`
  (docstrings on every method)
- **SDK contract:** `SDK_SPEC.md` (13 ambiguities settled)
- **End-to-end example:** `pond-feature-store/e2e_workflow.py`
  (e-commerce fraud detection, 12 steps, runs end-to-end)
- **Reference use case:** `docs/FEATURE_STORE_USE_CASE.md`
  (compact written walkthrough)
- **Production spec:** `rfcs/RFC-0011-feature-store.md`
- **Design goals:** `DESIGN_GOALS.md` (the six principles)

---

## What's NOT in the Feature Store (yet)

These are deliberately deferred (see `docs/FEATURE_STORE_USE_CASE.md`
§"What this does NOT cover"):

- **Streaming ingestion** (batch-only for now)
- **Transformation engine** (you compute values externally)
- **Materialized online tables** (for 10K+ QPS serving)
- **Distributed coordination** (Raft/CRDT — deferred until "what is
  replicated?" is answered)
- **Liquid-clustering materialization** (a future Layer 2
  materialization for multi-column range queries, per
  `docs/LIQUID_CLUSTERING_COMPARISON.md`)

If you need any of these, the Feature Store is not yet the right tool.
For everything else — schema-validated, versioned, point-in-time-correct
feature serving on a single node — it's ready to use.
