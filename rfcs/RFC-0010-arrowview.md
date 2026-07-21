# RFC-0010: ArrowView — Phase D Compatibility Adapter

## Status

**Accepted** — Phase D's first compatibility target. The ArrowView
implementation (`pond-arrow/arrow_view.py`) passes:
1. Its own test suite (6/6 tests: basic round-trip, DuckDB interop,
   Polars interop, versioning, delete/update, index integration).
2. The `view_laws.py` property-test harness (all 6 RFC-0007 laws pass,
   run via `python pond-arrow/run_arrow_view_laws.py`).

This proves Pond can be the storage layer underneath the Arrow
ecosystem without modifying DuckDB, Polars, pandas, or DataFusion.

---

## 1. Motivation

Phase D of the roadmap (from `DESIGN_GOALS.md` §8) asks:

> Prove Pond can underpin existing ecosystems without modifying
> them: Arrow, DuckDB, Polars, DataFusion, Lance, Iceberg-compatible
> metadata adapters. Each is an adapter View satisfying the RFC-0007
> algebra.

Apache Arrow is the natural first target because:
- Arrow is the *lingua franca* of the modern data ecosystem. DuckDB,
  Polars, DataFusion, Lance, pandas, and many other systems read and
  write Arrow natively.
- Arrow's columnar format is content-agnostic — it doesn't impose a
  schema or workload model. This makes it compatible with Pond's
  "store bytes, interpret in the View" philosophy.
- Arrow IPC streams are self-describing bytes — perfect for storage
  in Pond's content-addressed object store.

If Pond can serve data as Arrow, every Arrow-compatible system can
read Pond data without knowing Pond exists. This is the LTAP vision
made concrete.

---

## 2. Specification

### 2.1. The View Algebra for ArrowView

Per RFC-0007, `V = (Σ, A, E, D, M)`:

| Component | ArrowView |
|---|---|
| `Σ` (state space) | `(pyarrow.Table, commit_dag)`. A view state IS a single Arrow table plus the commit history. |
| `A` (algebra) | `put_row(pk, row_dict)`, `delete_row(pk)`, `commit(msg)`, `get_row(pk)`, `scan(columns, filter)`, `count_rows()`, `to_arrow()`, `to_duckdb(con)`, `to_polars()`, `to_pandas()`, `branch(name)`, `checkout(name)`, `merge(name)`, `history(limit)`, `diff(a,b)`, `create_arrow_index(name, extractor)`, `find_by_arrow(name, key)`, `drop_index(name)` |
| `E` (encode) | `pyarrow.Table → Arrow IPC stream bytes` via `pa.ipc.new_stream` |
| `D` (decode) | `Arrow IPC stream bytes → pyarrow.Table` via `pa.ipc.open_stream` |
| `M` (materializations) | Secondary indexes are Prolly trees mapping `_index/{name}/{key}` → snapshot blob hash. (See §2.4 for the simplification and future work.) |

### 2.2. Storage layout

Each commit's snapshot is a single Arrow IPC blob containing the full
table. The blob is stored in the kernel's object store; the commit's
HEAD Reference points to it.

For small tables (≤ a few thousand rows, ≤ a few MB), this is fine —
one kernel blob per snapshot, content-addressed deduplication works
naturally (same table → same hash → same blob).

For large tables, this is suboptimal — every commit rewrites the
full table. Future work (§5) will shard the table by Prolly chunk
boundaries, one Arrow IPC blob per chunk. This will give O(log N)
commits instead of O(N), matching the base `ProllyViewBase`'s
behavior.

### 2.3. Interoperability shims

ArrowView provides four convenience methods that hand the data to
external systems with zero copy (or near-zero copy):

| Method | Returns | Integration point |
|---|---|---|
| `to_arrow()` | `pyarrow.Table` | All Arrow-compatible systems |
| `to_duckdb(con, name?)` | `str` (the table name) | `con.register(name, table)` then SQL |
| `to_polars()` | `polars.DataFrame` | `pl.from_arrow(table)` (zero-copy) |
| `to_pandas()` | `pandas.DataFrame` | `table.to_pandas()` (zero-copy for many dtypes) |

The `to_duckdb` method is the most consequential: it lets a user run
arbitrary SQL queries against Pond data without copying the data out
of Pond. Example:

```python
view = ArrowView(kernel, "orders")
view.put_row("o1", {"product": "Widget", "amount": 100})
# ... more puts ...
view.commit("load orders")

import duckdb
con = duckdb.connect()
view.to_duckdb(con, "orders")
result = con.execute("SELECT product, SUM(amount) FROM orders GROUP BY product").fetchall()
```

Pond is the storage layer; DuckDB is the query engine; Arrow is the
contract between them. This is exactly the Phase D vision.

### 2.4. Index integration (simplified)

ArrowView reuses the SDK's `create_index` / `drop_index` infrastructure
(inherited from `View`). The current implementation stores indexes as
Prolly trees mapping `_index/{name}/{key}` → snapshot blob hash.

This is a **simplification**: `find_by_arrow` resolves the index to the
snapshot, then linear-scans the snapshot for the first matching row.
The lookup is O(N), not O(log N). This is acceptable for the Phase D
proof-of-concept (the goal is to prove the API works, not to be fast);
the future work below describes how to make it truly O(log N).

The tombstone pattern (RFC-0008) is preserved: `drop_index` rebinds
the index Reference to `TOMBSTONE_HASH`, and `find_by_arrow` returns
`None` for tombstoned indexes immediately.

---

## 3. Tests

The test suite in `pond-arrow/arrow_view.py` has 6 tests, all
passing:

1. **Basic round-trip** — put 3 rows, commit, verify `to_arrow()`
   returns a Table with 3 rows and 4 columns (including the
   auto-added `_pk` field). Verify `get_row` returns the right row.
   Verify `scan(filter=...)` and `scan(columns=...)` work.
2. **Arrow interop** — register the View's data with DuckDB, run
   `SELECT product, SUM(amount) FROM orders GROUP BY product`, verify
   the result. Same for Polars (`filter + sum`). This proves Pond
   data is usable by external systems without copying.
3. **Versioning** — create a branch, add a row on the branch, verify
   the main branch is unchanged and the branch has 3 rows. Verify
   `history()` returns ≥ 2 commits.
4. **Delete and update** — update a row via `put_row` with an existing
   primary key (overwrite). Delete a row via `delete_row`. Verify
   `count_rows` and `get_row` reflect the changes.
5. **Index integration** — `create_arrow_index`, `find_by_arrow`,
   `drop_index`. Verify dropped indexes return `None` from
   `find_by_arrow` (tombstone pattern from RFC-0008).

### 3.1. View algebra compliance

Run `python pond-arrow/run_arrow_view_laws.py` to verify ArrowView
satisfies all 6 RFC-0007 laws:

```
View Algebra Law Report (6 checks)
  [PASS] Law 1: Round-trip (D(E(s)) = s): all 10 samples round-trip correctly
  [PASS] Law 2: Purity (operations are deterministic)
  [PASS] Law 3: Encoding preservation (every reachable state is persistable)
  [PASS] Law 4: Materialization determinism
  [PASS] Law 5: Composition (structural)
  [PASS] Law 6: Kernel independence (blobs are opaque)
  ALL LAWS SATISFIED
```

The state space Σ for ArrowView is `pyarrow.Table` (not dict). The
harness's `sample_data` returns Tables; `encode/decode` round-trip
Tables through Arrow IPC bytes. This is a meaningful generalization
of the algebra: it proves the algebra admits Views whose state is
not a dict, as long as the `E/D` pair satisfies Law 1.

---

## 4. What this proves

1. **Pond can be the storage layer underneath the Arrow ecosystem.**
   DuckDB, Polars, pandas, and DataFusion can all read Pond data via
   Arrow, without knowing Pond exists. This is the Phase D vision.

2. **The View algebra (RFC-0007) generalizes to non-dict state spaces.**
   ArrowView's Σ is `pyarrow.Table`. The 6 laws still hold. This
   confirms the algebra is a real specification, not a dict-shaped
   tautology.

3. **The tombstone pattern (RFC-0008) composes with ArrowView.**
   `drop_index` uses `drop_name` from `pond-sdk/maintenance.py`.
   `find_by_arrow` checks for tombstones via `resolve_active`. No
   special-case code in ArrowView.

4. **The Layer 0 kernel stays unchanged.** ArrowView uses the same
   3 primitives (`write`, `read`, `reference`) as every other View.
   No kernel modifications, no new primitives, no special cases.

5. **The removability discipline (PACKAGES.md §3) holds.** `pond-arrow`
   depends only on `pond-sdk` (which depends only on `pond-core`).
   Deleting `pond-arrow` does not affect any lower layer.

---

## 5. Future work

### 5.1. Chunked storage (large tables)

Current: one Arrow IPC blob per commit snapshot. O(N) commit cost.

Future: shard the table by Prolly chunk boundaries (sort by primary
key, chunk at ~64 rows or ~4 KB). Each chunk is a separate kernel
blob; the Prolly tree maps primary-key-range → chunk-hash. Commits
become O(log N) (only the affected chunk is rewritten). This matches
the base `ProllyViewBase`'s behavior, applied to Arrow data.

### 5.2. True O(log N) indexes

Current: `find_by_arrow` is O(N) (linear-scans the snapshot).

Future: store per-row blob hashes in the index, OR use the multi-
valued index pattern from `SDK_SPEC.md` §4.4.1 (list of primary keys
at each leaf, then `get_row` for each).

### 5.3. Schema enforcement

Current: schema is inferred from the first `put_row`. Subsequent
rows with different keys silently miss fields (null in Arrow).

Future: optional `schema` parameter at construction; reject `put_row`
calls that don't conform. PyArrow's `Table.cast` and schema
validation make this straightforward.

### 5.4. Streaming reads (large result sets)

Current: `to_arrow()` returns the full table in memory.

Future: `to_arrow_stream()` returns an iterator of `RecordBatch`
objects, decoded from the kernel blob incrementally. This matters
for large tables that don't fit in memory.

### 5.5. Multi-dimensional clustering (per Liquid Clustering comparison)

Per `docs/LIQUID_CLUSTERING_COMPARISON.md`, a future `ClusteredArrowView`
could sort rows by their Hilbert-curve key (computed from N cluster
columns) before encoding as Arrow IPC. This would give Pond the
multi-column range-query performance of Databricks Liquid Clustering
while preserving Pond's content-addressing, versioning, and cross-
View sharing. This is a Layer 2 materialization (per RFC-0005), not
a kernel feature.

---

## 6. Relationship to other RFCs

- **Depends on:** RFC-0003 (Kernel Specification — the 3 primitives
  ArrowView is built on), RFC-0007 (View Algebra — ArrowView
  satisfies the 5-tuple + 6 laws), RFC-0008 (Deletion as Data —
  drop_index uses tombstones).
- **Implements:** Phase D of `DESIGN_GOALS.md` §8 (compatibility
  with the Arrow ecosystem).
- **Informs:** a future RFC for `ClusteredArrowView` (per
  `docs/LIQUID_CLUSTERING_COMPARISON.md` §3.3 Lesson 1).
- **Does not modify:** any kernel code, any existing View code, any
  RFC. ArrowView is purely additive.
