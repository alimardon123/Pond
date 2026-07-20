# RFC-0011: Feature Store (Phase E Flagship)

## Status

**Accepted** — Phase E's flagship application. The implementation
(`pond-feature-store/feature_store.py`) passes:
1. The original Phase D test (recursive composition verified).
2. The Phase E production test (schema validation, versioning,
   point-in-time JOIN, batch serving, entity registry, persistence).

This is the first Pond application pushed to production quality. It
exercises the full SDK: indexes (lazy + multikey), CrossView (ingestion
from source Views), SemanticView (feature metadata as metrics),
branching/history (time travel), and tombstones (RFC-0008 for index
drops).

---

## 1. Motivation

Phase E (from `DESIGN_GOALS.md` §8) asks:

> Choose one flagship and make it excellent. Personally, I would
> choose one of these: 1. Feature Store (strongest fit with your
> current work). 2. Lakehouse metadata/catalog service (closest to
> your original motivation). 3. Git-compatible repository backend
> (excellent for validating versioning semantics).

The Feature Store was chosen because it exercises the most SDK
surface in a realistic ML workflow:
- **Indexes**: `by_entity` (lazy, single-key) and `by_feature`
  (lazy, composite-key via string concatenation).
- **CrossView**: `ingest_from_view` reads from any source View
  (SQL, Streaming, ArrowView) via `CrossView.read_all_from`.
- **SemanticView**: `register_with_semantic_view` exports features
  as semantic metrics for downstream BI/AI tools.
- **Branching + history**: feature values are versioned via the
  commit DAG; point-in-time queries walk the DAG.
- **Tombstones**: `drop_index` uses the RFC-0008 tombstone pattern.
- **Schema validation**: type-checked writes prevent corrupt data.
- **Persistence**: data survives process restart (kernel-backed).

A production feature store is the canonical application that
justifies the entire Pond architecture: one copy of data (the
kernel's content-addressed blobs) serving online inference (point
lookups), offline training (batch scans + point-in-time JOINs),
and semantic metadata (for BI tools) — without duplication.

---

## 2. Specification

### 2.1. The View Algebra for FeatureStore

Per RFC-0007, `V = (Σ, A, E, D, M)`:

| Component | FeatureStore |
|---|---|
| `Σ` (state space) | `(feature_definitions, entity_registry, feature_values, commit_dag)`. A view state is the set of all feature definitions, entity types, and feature values, plus the commit history. |
| `A` (algebra) | `define_feature`, `register_entity_type`, `write_feature_value`, `ingest_from_view`, `get_feature_value`, `get_feature_vector`, `get_feature_matrix`, `get_all_values`, `get_feature_values_at_time`, `get_training_dataset`, `get_lineage`, `register_with_semantic_view`, `get_freshness`, `list_features`, `list_feature_versions`, `list_entity_types`, `drop_feature` (future) |
| `E` (encode) | JSON (inherited from `IndexedView`) |
| `D` (decode) | JSON (inherited from `IndexedView`) |
| `M` (materializations) | Two Prolly-tree indexes: `by_entity` (entity_id → blob_hash) and `by_feature` (feature_name/version → blob_hash). Plus the `_meta/latest_ts/{feature_name}` freshness cache. |

FeatureStore extends `IndexedView` extends `ProllyViewBase` extends
`Kernel`. Four layers of recursive composition, each using ONLY the
layer below. The kernel is unchanged.

### 2.2. Storage model

All keys are content-addressed blobs in the kernel:

```
_features/{name}/{version}                    -> feature definition blob
_entities/{entity_type}                       -> entity type definition blob
_meta/latest_ts/{feature_name}                -> cached latest timestamp (O(1) freshness)
{feature_name}/v{version}/{entity_id}/{ts}    -> feature value blob
```

Index References (per SDK_SPEC.md §4.4):
```
{view_name}__index__by_entity   -> Prolly tree root (entity_id -> blob_hash)
{view_name}__index__by_feature  -> Prolly tree root (feature_name/v{version} -> blob_hash)
```

### 2.3. Feature definitions (with versioning)

```python
v1 = fs.define_feature("user_age", "int", "users", "age field")
# v1 = 1

v2 = fs.define_feature("user_age", "float", "users", "age in years")
# v2 = 2 (type changed -> new version)

v3 = fs.define_feature("user_age", "float", "users", "age in years")
# v3 = 2 (idempotent — same definition -> same version)
```

Versioning rules:
- **New feature:** version = 1.
- **Different definition** (type, source, or transformation changed):
  version = previous + 1.
- **Same definition** (idempotent redefinition): returns the existing
  version, no new version created.

Both versions remain queryable. `get_feature_value(name, eid)` returns
the latest version; `get_feature_value(name, eid, version=N)` returns
a specific version. This enables reproducible ML training (train
against v1 while v2 is in development).

### 2.4. Schema validation

`write_feature_value` validates the value against the feature's
declared type before writing. Supported types:

| Type | Validation |
|---|---|
| `int` | `isinstance(v, (int, float)) and not bool and float(v).is_integer()` |
| `float` | `isinstance(v, (int, float)) and not bool` |
| `string` | `isinstance(v, str)` |
| `bool` | `isinstance(v, bool)` |
| `vector` | `isinstance(v, list) and all(isinstance(x, (int, float)) for x in v)` |
| `any` | always valid |
| `json` | always valid (any JSON-serializable value) |

Raises `ValueError` on mismatch. This prevents corrupt data from
entering the store and breaking downstream ML models.

### 2.5. Point-in-time JOIN (the killer ML feature)

```python
events = [
    {"entity_id": "u1", "timestamp": 1500.0, "label": 1},
    {"entity_id": "u1", "timestamp": 2500.0, "label": 0},
]
dataset = fs.get_training_dataset(events, ["user_balance"])
# dataset[0] = {"entity_id": "u1", "timestamp": 1500.0, "label": 1,
#               "user_balance": <value as of ts=1500>}
# dataset[1] = {"entity_id": "u1", "timestamp": 2500.0, "label": 0,
#               "user_balance": <value as of ts=2500>}
```

The point-in-time JOIN is what makes a feature store useful for ML
training. Without it, you get **label leakage**: the training dataset
accidentally includes feature values from the future (after the event
timestamp), leading to over-optimistic model evaluation and production
failure.

Algorithm:
1. For each feature, build an in-memory timeline: `{entity_id: [(ts, value), ...]}`
   sorted by timestamp.
2. For each event, binary-search each feature's timeline for the
   rightmost value with `ts <= event_ts`.
3. Return a row with the event's original fields plus the joined
   feature values (or `None` if no value exists as-of the event).

Complexity: `O(F * V + E * F * log V)` where F = features, V = avg
values per feature, E = events. For typical ML workloads (10 features,
100K values per feature, 1M events), this is fast enough for offline
training. For online inference, use `get_feature_matrix` instead.

### 2.6. Batch online serving (feature matrix)

```python
matrix = fs.get_feature_matrix(
    entity_ids=["u1", "u2", "u3", "u4", "u5"],
    feature_names=["user_balance", "user_age"]
)
# Returns 5 rows, each with entity_id + the two feature values.
```

More efficient than calling `get_feature_vector` in a loop because it
scans each feature's values once (not once per entity). Complexity:
`O(N + M * V)` where N = entities, M = features, V = avg values per
feature. For 10K entities and 50 features, this is ~500x faster than
the naive loop.

### 2.7. O(1) freshness via cache

`write_feature_value` updates a cached "latest timestamp" for the
feature, stored under `_meta/latest_ts/{feature_name}`.
`get_freshness` reads this cache in O(1) instead of scanning all
values. This matters for monitoring dashboards that check freshness
across hundreds of features every few seconds.

### 2.8. Persistence

All data is stored in the kernel's content-addressed object store
(SQLite + filesystem by default; S3, FDB, Redis also supported).
Data survives process restart. The `test_production_features` test
verifies this by closing the kernel, reopening it, and checking that
all features, values, versions, and entity types are intact.

### 2.9. Cross-View ingestion

```python
fs.ingest_from_view(source_view=orders_view,
                     feature_name="total_spent",
                     entity_field="customer_id",
                     value_field="amount",
                     timestamp_field="ts")
```

Reads all rows from `orders_view` via `CrossView.read_all_from`,
extracts the entity_id / value / timestamp from each row, and calls
`write_feature_value` (which validates the schema). Works with any
source View type: `View`, `IndexedView`, `ArrowView`, `SQLView`,
`StreamingView`, etc.

---

## 3. Tests

### 3.1. Original Phase D test (preserved)

`test_feature_store()` — verifies recursive composition: Kernel →
ProllyViewBase → IndexedView → FeatureStore. Exercises feature
definitions, value ingestion from a source View, online serving,
offline serving, point-in-time (simple), lineage, semantic integration,
and freshness.

### 3.2. Phase E production test (new)

`test_production_features()` — verifies all production features:

1. **Schema validation**: valid int write accepted; string-to-int
   rejected; 25.5-to-int rejected; write to undefined feature rejected.
2. **Feature versioning**: v1=1, redefine with different type -> v2=2,
   idempotent redefinition -> v2=2, multi-version query returns correct
   values, `list_feature_versions` returns [1, 2].
3. **Entity registry**: register two entity types, list them, retrieve
   one with correct join_key.
4. **Point-in-time JOIN**: 4 events at timestamps 500/1500/2500/3500
   against a feature with values at 1000/2000/3000. Verifies: ts=500
   returns None (no data yet), ts=1500 returns 100.0 (correct),
   ts=2500 returns 50.0 (correct), ts=3500 returns 75.0 (correct).
   Label leakage prevented.
5. **Batch online serving**: `get_feature_matrix` for 5 entities x 2
   features. Verifies correct values and `None` for missing entities.
6. **O(1) freshness**: `get_freshness` returns a non-negative number
   via the cached latest timestamp.
7. **Persistence**: close the kernel, reopen it, verify all features,
   values, versions, and entity types survived.

All tests pass.

---

## 4. What this proves

1. **The Pond architecture supports a production-quality application.**
   The Feature Store is not a toy — it has schema validation,
   versioning, point-in-time JOIN, batch serving, and persistence.
   These are the features that distinguish a real feature store
   (Feast, Tecton, Hopsworks) from a key-value store.

2. **The full SDK surface is exercised in a realistic workflow.**
   Indexes (lazy + composite-key), CrossView (ingestion),
   SemanticView (metadata export), branching/history (time travel),
   tombstones (index drops), and persistence — all in one application.

3. **The kernel stays unchanged.** FeatureStore is a Layer 3
   application built entirely on the SDK. No kernel modifications,
   no new primitives, no special cases. The 3-primitive kernel
   (Write, Read, Reference) is sufficient.

4. **The removability discipline holds.** `pond-feature-store` depends
   only on `pond-sdk` (and transitively on `pond-core`). Deleting
   `pond-feature-store` does not affect any lower layer. The SDK and
   kernel are unaware that the Feature Store exists.

5. **The view_laws.py harness still passes.** The Feature Store is
   a Layer 3 subclass of `IndexedView`; the existing CI (which tests
   `View`, `IndexedView`, `SemanticView`, `Multikey`, `KeylessView`)
   continues to pass. The algebra is preserved.

---

## 5. Future work (NOT in this RFC)

These are candidates for future RFCs, not part of RFC-0011:

1. **Online streaming ingestion.** Currently `ingest_from_view` does a
   batch read. A streaming variant would subscribe to a `StreamingView`
   and ingest incrementally as new records arrive.

2. **Feature transformations.** Currently the user computes feature
   values externally and writes them. A transformation engine would
   let the user define `transformation="SUM(amount) GROUP BY customer_id"`
   and have the Feature Store compute it from the source View.

3. **Materialized feature tables.** For high-throughput online serving
   (10K QPS), a materialized table (per RFC-0005) would pre-compute
   the latest value per entity per feature, avoiding the index lookup.

4. **Distributed coordination.** Multi-writer Feature Store requires
   distributed consensus (Raft, CRDT, or external coordinator). This
   is deferred per the roadmap (Phase D first, then revisit replication).

5. **Tiered storage.** Hot feature values in memory/SSD; cold history
   in S3. The kernel's backend-independence (RFC-0003) makes this
   possible without Feature Store changes.

6. **Liquid-clustering materialization.** Per
   `docs/LIQUID_CLUSTERING_COMPARISON.md`, a future `ClusteredFeatureStore`
   could sort feature values by Hilbert curve on (entity_id, timestamp)
   for faster range queries. This is a Layer 2 materialization, not
   a Feature Store concern.

---

## 6. Relationship to other RFCs

- **Depends on:** RFC-0003 (Kernel), RFC-0007 (View Algebra),
  RFC-0008 (Tombstones for index drops), RFC-0005 (Materialization —
  the freshness cache and indexes are materializations).
- **Implements:** Phase E of `DESIGN_GOALS.md` §8 (one flagship).
- **Uses:** SDK_SPEC.md §2.6 (auto-key, for future streaming
  ingestion), §4.2.1 (multikey, for tag-based feature indexing),
  §8.1 (CrossView, for ingestion).
- **Does not modify:** any kernel code, any SDK code, any existing
  View code, any RFC. FeatureStore is purely additive — a Layer 3
  application consuming the SDK.
