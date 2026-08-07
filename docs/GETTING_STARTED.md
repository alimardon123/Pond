# Getting Started with Pond

> A 5-minute onboarding path. By the end, you'll have created a
> versioned table, queried it with SQL, branched it, time-traveled
> it, and seen cross-Lens interop — all on the Pond kernel.

---

## What is Pond?

A minimal immutable object runtime. The kernel is 3 operations
(`Write`, `Read`, `Ref`), ~140 LOC. Everything else — versioning,
schemas, SQL, feature stores, branching, time travel — is a Lens
on top.

**In this guide, you'll use the Lakehouse Lens** (DuckDB on Pond)
to see how the architecture feels.

---

## Step 1: Create a Pond lakehouse

```python
import sys, os
sys.path.insert(0, "bindings/python/core")
sys.path.insert(0, "lenses/lakehouse")

import pyarrow as pa
from lakehouse import PondLakehouse

lh = PondLakehouse("/tmp/my-pond")

# Create a table
users = pa.table({
    "id": [1, 2, 3],
    "name": ["alice", "bob", "carol"],
    "age": [30, 25, 35],
})
lh.create_table("users", users)

# Query it with SQL
result = lh.query("SELECT COUNT(*) AS cnt FROM users", table_name="users")
print(result.column("cnt")[0])  # → 3
```

---

## Step 2: Insert and query

```python
# Insert more rows
new_users = pa.table({
    "id": [4, 5],
    "name": ["dave", "eve"],
    "age": [40, 28],
})
lh.insert("users", new_users)

# Filter
result = lh.query(
    "SELECT name FROM users WHERE age > 30 ORDER BY name",
    table_name="users",
)
print([r.as_py() for r in result.column("name")])  # → ['carol', 'dave']
```

---

## Step 3: Time travel

```python
# Get history
history = lh.history("users")
original_commit = history[-1]["hash"]  # first commit

# Query at the original commit (before inserts)
result = lh.query_at(
    "SELECT COUNT(*) AS cnt FROM users",
    table_name="users",
    commit_hash=original_commit,
)
print(result.column("cnt")[0])  # → 3 (original, before inserts)
```

---

## Step 4: Branching

```python
# Create a dev branch
lh.branch("users", "dev")

# Commit to the dev branch (doesn't affect main HEAD)
dev_users = pa.table({
    "id": [6],
    "name": ["frank"],
    "age": [50],
})
lh.commit_to_branch("users", "dev", dev_users)

# Main HEAD still has 5 rows
result = lh.query("SELECT COUNT(*) AS cnt FROM users", table_name="users")
print(result.column("cnt")[0])  # → 5 (unchanged)

# Merge dev into main
lh.merge_branch("users", "dev")
result = lh.query("SELECT COUNT(*) AS cnt FROM users", table_name="users")
print(result.column("cnt")[0])  # → 6 (after merge)
```

---

## Step 5: Schema evolution

```python
# Add a new column (old rows get NULL — Parquet-native)
users_v2 = pa.table({
    "id": [7],
    "name": ["grace"],
    "age": [45],
    "email": ["grace@example.com"],
})
lh.insert("users", users_v2)

# Query the new column
result = lh.query(
    "SELECT name, email FROM users WHERE email IS NOT NULL",
    table_name="users",
)
print([r.as_py() for r in result.column("email")])  # → ['grace@example.com']
```

---

## Step 6: Cross-Lens interop (the killer demo)

The Feature Store Lens (`pond-labs/feature_store_lens.py`) reads the
**same data** as the Lakehouse Lens — no ETL, no sync, no duplicate
storage.

```bash
# Run the full interop demo
python pond-labs/interop_demo.py
```

This demonstrates:
- Feature Store writes → Lakehouse reads (via SQL)
- Lakehouse branches → Feature Store sees it
- Time travel across Lenses
- Schema evolution propagates
- Cross-Lens workflow (FS trains → LH analyzes → FS merges)

---

## What just happened?

You used 4 Pond features:
1. **Versioning** — every insert creates a new commit in the chain.
2. **Time travel** — read any past commit by hash.
3. **Branching** — create branches for experimentation; merge when ready.
4. **Cross-Lens interop** — Feature Store and Lakehouse share data natively.

All of this is built on the 3-operation kernel (`Write`, `Read`, `Ref`).
The Lakehouse Lens adds SQL via DuckDB; the Feature Store Lens adds
point-in-time joins. They share the same bytes.

---

## Where to go next

- **[docs/POND_WHITEPAPER.md](POND_WHITEPAPER.md)** — the full architecture (20 pages).
- **[docs/WHERE_POND_FAILS.md](WHERE_POND_FAILS.md)** — honest scope + Lens roadmap.
- **[docs/LENS_GUIDE.md](LENS_GUIDE.md)** — how to write your own Lens.
- **[DESIGN_GOALS.md](../DESIGN_GOALS.md)** — 7 design principles.
- **[pond-labs/](../pond-labs/)** — experiments and demos.

---

## Common pitfalls

**Pitfall 1: Re-registering tables with DuckDB.**
The current Lakehouse Lens registers tables with DuckDB on each query.
For production, cache registrations. (See `lenses/lakehouse/lakehouse.py`.)

**Pitfall 2: Union merge duplicates rows.**
The current merge policy is `union` (concat both tables). For row-level
merge with conflict detection, implement a 3-way merge Lens.

**Pitfall 3: Point-in-time joins prevent label leakage.**
If you're doing ML training, use `FeatureStoreLens.point_in_time_join()`
— not a naive join. The PIT join ensures features at time T only use
data from before T.
