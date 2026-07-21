# Pond SDK Specification

> **Status:** Authoritative contract for the Pond SDK. Every method
> signature, return shape, and behavior in this document is
> checkable; violations are bugs.
>
> **Audience:** View authors (both human and AI agents) building
> Views on top of `pond-sdk`. If you read only one SDK document,
> read this one.
>
> **Source of truth:** This document supersedes informal descriptions
> in `docs/LENS_AUTHORS_GUIDE.md`. Where the two disagree, this
> document is correct.

---

## 0. The ambiguities this document settles

The external validation report (`validation/vector_report.md`)
identified 10 ambiguities (A–J) that caused friction when a fresh
agent tried to build a `VectorView` from the SDK spec. A second
external validation (`validation/graph_challenge_report.md`)
found 3 more (K–M). This document settles each one. Cross-reference
table:

| ID | Ambiguity | Settled in section |
|---|---|---|
| A | How is a kernel obtained? | §1.1 |
| B | Index extractor call signature | §4.2 |
| C | `get()` complexity | §3.2 |
| D | Merge semantics | §6.1 |
| E | Index persistence and naming | §4.4 |
| F | `drop_index` / `unregister_index` | §4.5 (per RFC-0008) |
| G | `diff(a, b)` parameter types | §6.3 |
| H | `history()` return shape | §6.2 |
| I | `put_raw` semantics | §2.3 |
| J | Commit object format | §7 |
| K | Multi-key (multi-valued) indexes | §4.2.1 (new) |
| L | Auto-key mode and primary-keyless Views | §2.6 (new) |
| M | Cross-View read/write semantics | §8.1 (new) |

---

## 1. Obtaining a kernel (Ambiguity A)

### 1.1. Constructor contract

```python
from pond_minimal import PondMinimal

kernel = PondMinimal(base_dir: str)
```

`PondMinimal(base_dir)` constructs and returns a kernel instance. It
is NOT a factory; it IS the kernel. The `base_dir` is the filesystem
path where the kernel stores its object store and root namespace
SQLite database. The kernel creates `base_dir/.pond/` if it does not
exist.

### 1.2. What the kernel provides

The kernel exposes exactly 3 core primitives plus 3 supporting
operations:

| Method | Signature | Purpose |
|---|---|---|
| `write(data: bytes) -> str` | core | Create an immutable content-addressed blob. Returns its 64-char hex SHA-256 hash. |
| `read(hash_or_name: str) -> bytes` | core | Read a blob by hash, or resolve a name then read. |
| `reference(name: str, hash: str) -> None` | core | Set a mutable name→hash mapping. The ONLY mutable operation. **Validates that `hash` refers to an existing blob** — raises `ValueError` if not. |
| `resolve(name: str) -> str \| None` | supporting | Resolve a name to its current hash. Returns `None` if unbound. |
| `list_names() -> list[str]` | supporting | List all names in the root namespace. |
| `read_blob(hash: str) -> bytes` | supporting | Read a blob by hash directly (no name resolution). Performance shortcut. |

The 3 supporting operations are pure derivations of the 3 core
primitives (e.g., `resolve` is `SELECT hash FROM roots WHERE name=?`
on the kernel's internal SQLite). They do not extend the kernel's
algebra. See `RFC-0003` for the formal kernel specification.

> **⚠ Important consequence of `reference()` validation:** you cannot
> bind a name to a hash whose blob does not exist on disk. This
> matters for tombstones (see §4.5, §8): `drop_name` handles this
> for you by pre-writing the tombstone marker blob before rebinding.
> Do NOT call `kernel.reference(name, TOMBSTONE_HASH)` directly
> without first writing the marker blob — it will raise
> `ValueError`.

### 1.3. Constructing a View

A View is constructed with its kernel instance and a name:

```python
view = View(kernel, name: str)
# or:
view = IndexedView(kernel, name: str)
# or, for a custom View:
view = MyView(kernel, name: str)   # MyView extends View or IndexedView
```

The `name` is the View's identifier in the kernel's root namespace.
It appears in:
- The kernel Reference pointing to the View's HEAD commit:
  `kernel.resolve(name)` returns the HEAD commit hash.
- Branch References: `f"{name}__branch__{branch_name}"`.
- Index References: `f"{name}__index__{index_name}"`.

The name must be a non-empty string. It must not contain `__`
(double-underscore) — that sequence is reserved for the kernel
namespace convention (see §2.5).

### 1.4. Lifetime

The kernel holds an open SQLite connection. Call `kernel.close()` to
release it. The kernel is NOT thread-safe by default; see
`engineering/01_concurrency.py` for the thread-safe wrapper.

---

## 2. Writing data (Ambiguity I: `put_raw`)

### 2.1. `put(key, data) -> str`

```python
view.put(key: str, data: Any) -> str
```

Stages a key→blob mapping for the next commit. The data is encoded
via `view.encode(data)` (default: JSON), written to the kernel as a
blob, and the resulting hash is staged under `key`. Returns the
blob hash.

This is the standard write path. The data passes through `encode`;
the View does NOT see the raw bytes.

### 2.2. `delete(key) -> None`

```python
view.delete(key: str) -> None
```

Stages a deletion for `key`. The deletion is applied at the next
commit. Calling `delete` on a non-existent key is a no-op (not an
error).

### 2.3. `put_raw(key, blob_hash) -> None` (Ambiguity I)

```python
view.put_raw(key: str, blob_hash: str) -> None
```

Stages a key→blob_hash mapping **without** encoding or writing new
data. The `blob_hash` must refer to an existing blob in the kernel
(the View does NOT verify this; the kernel will fail at commit time
if the hash is unknown).

Use cases:
- Cross-View data sharing: copy a blob hash from one View to another
  without re-encoding the data (zero-copy).
- Re-indexing: build a new index that points to existing data blobs
  without rewriting them.

`put_raw` does NOT call `encode`. It does NOT call `kernel.write`.
It only stages the existing hash. The next `commit()` will include
this mapping in the delta.

If you pass a `blob_hash` that does not exist in the kernel, the
commit will succeed (the kernel does not validate staged entries),
but subsequent `get(key)` will fail with `ValueError: Blob not
found on disk`. Validate hashes before calling `put_raw`.

### 2.4. `commit(message) -> str`

```python
view.commit(message: str = "") -> str
```

Commits staged changes. Returns the commit hash (a 64-char hex
string). After commit, the staging area is cleared.

If `message` is empty, a default message is generated
(`f"{self.name} commit"`).

Raises `ValueError("Nothing to commit")` if no changes are staged.

### 2.5. Key naming conventions

Keys passed to `put`/`get`/`delete` are arbitrary strings, but the
following conventions apply:

- **Reserved prefix `_` (underscore):** keys starting with `_` are
  treated as internal by `get_all()` and `keys()` — they are excluded
  from the iterable state. Use `_`-prefixed keys for View-internal
  metadata (schema, indexes, semantic definitions).
- **No `__` (double-underscore) in keys:** this sequence is reserved
  for the kernel namespace convention
  (`f"{view_name}__index__{name}"`, `f"{view_name}__branch__{name}"`).
- **View authors choose their own key names** for non-internal data.
  Suggested patterns: `"node:{id}"`, `"user:{id}"`, `"order:{id}"`,
  `"edge:{from}:{to}:{type}"`. The SDK does not mandate a convention.

### 2.6. Auto-key mode and primary-keyless Views (Ambiguity L)

Pond Views require a primary key on every `put`. Some workloads
(event logs, time-series, audit trails, append-only streams) have
no natural primary key. For these, the SDK provides two ways to
auto-generate keys:

#### 2.6.1. `put_auto(data)` — auto-key on a regular View

```python
view = View(kernel, "events")
key = view.put_auto({"event": "click", "user": "u1", "ts": 1721500000})
# `key` is a 32-char hex string (UUID4 without dashes).
# Retrieve later via:
event = view.get(key)
```

`put_auto` is available on both `View` and `IndexedView`. It
generates a UUID4 hex key, calls `put(key, data)` internally, and
returns the generated key so the caller can retrieve the data later.

The key is generated via `uuid.uuid4().hex` — 32 hex characters,
~122 bits of entropy. Collisions are astronomically unlikely
(~10^-37 probability for 10^12 records).

**Hardening notes (settled per Phase B.4):**

1. **Key format is fixed:** `uuid.uuid4().hex` — 32 lowercase hex
   characters, no dashes. Do NOT parse the key; treat it as opaque.
   The format may change in a future major version (e.g., to ULID
   for time-sortability) but the contract is "32-char hex string
   that uniquely identifies the row within this View."

2. **Key uniqueness is per-View, not global.** Two `put_auto` calls
   on different Views may (with vanishing probability) return the
   same key — that's fine, they're in different namespaces. Within
   a single View, two `put_auto` calls with different `data` always
   return different keys (UUID4 collision probability is negligible).
   Within a single View, two `put_auto` calls with the SAME `data`
   return DIFFERENT keys (because the key is random, not derived
   from content). If you want content-addressed dedup, use
   `view.put(hashlib.sha256(view.encode(data)).hexdigest(), data)`
   instead — `put_auto` does NOT dedup.

3. **`put_auto` does NOT commit.** Like `put`, it stages the write.
   The caller must call `commit(message)` afterward. This matches
   the `put` contract — see §2.4.

4. **`put_auto` is NOT thread-safe.** Two concurrent `put_auto`
   calls on the same View instance may race on the staging area.
   For multi-threaded access, use the thread-safe kernel wrapper
   from `engineering/01_concurrency.py` and serialize View-level
   operations externally.

5. **The returned key is the primary key, not the blob hash.**
   `put_auto` returns the generated primary key (used for `get`,
   `delete`, `find_by`). The blob hash is internal; you don't need
   it. If you want the blob hash too, capture the return value of
   `view.put(key, data)` (which returns the blob hash) — but
   `put_auto` returns the primary key, not the blob hash. This is
   a deliberate asymmetry: `put` returns the blob hash (for
   `put_raw` chaining); `put_auto` returns the primary key (for
   `get` retrieval).

#### 2.6.2. `KeylessView` — primary-keyless as a first-class mode

```python
from view_sdk import KeylessView

view = KeylessView(kernel, "audit_log")
key = view.put(None, {"action": "login", "user": "alice"})  # key MUST be None
# or use put_many for batch inserts:
keys = view.put_many([
    {"action": "click", "user": "bob"},
    {"action": "view", "user": "bob"},
])
view.commit("audit events")
```

`KeylessView` is a subclass of `View` that overrides `put` to
require `key=None`. Passing a non-None key raises `TypeError` —
if you want to supply your own keys, use the regular `View` class.

`KeylessView.put(None, data)` is equivalent to `View.put_auto(data)`.
The class exists to make primary-keyless mode a first-class design
choice, not a per-call decision.

#### 2.6.3. Indexed lookups on keyless data

For keyless Views, lookups by primary key are usually meaningless
(the caller doesn't have the auto-generated key). Instead, register
indexes on fields WITHIN the data and use `find_by` / `find_all_by`:

```python
view = KeylessView(kernel, "events")
# (KeylessView doesn't have register_index; use IndexedView if you
# need auto-keying AND indexes.)
idx_view = IndexedView(kernel, "events")
idx_view.register_index("by_user", lambda d: d.get("user", ""), mode="lazy")
idx_view.register_index("by_event", lambda d: d.get("event", ""), mode="lazy")

k = idx_view.put_auto({"event": "click", "user": "alice"})
idx_view.commit("insert")

# Look up by indexed field — no need to know `k`:
clicks = idx_view.find_by("by_event", "click")
alice_events = idx_view.find_by("by_user", "alice")
```

#### 2.6.4. When to use which

| Pattern | When to use |
|---|---|
| `View.put(key, data)` | You have a natural primary key (user_id, order_id). |
| `View.put_auto(data)` | You mostly have keys but occasionally need auto (mixed workload). |
| `KeylessView.put(None, data)` | You never have keys; the entire View is append-only. |
| `IndexedView.put_auto(data)` + indexes | Append-only AND you need indexed lookups. |

---

## 3. Reading data (Ambiguity C: `get()` complexity)

### 3.1. `get(key) -> Optional[Any]`

```python
view.get(key: str) -> Optional[Any]
```

Reads a single key. Returns the decoded data, or `None` if the key
does not exist.

The data passes through `view.decode(bytes)` (default: JSON parse).

### 3.2. Complexity (Ambiguity C)

`get()` is **O(log N + K)** where:
- N = number of entries in the View
- K = number of delta commits since the last snapshot (K ≤ 4 by
  default, the `COMPACTION_THRESHOLD`)

The lookup walks the commit DAG from HEAD:
1. For each delta commit (≤ K), check if the key is in the delta
   (`+` or `-`). O(K).
2. When a snapshot commit is reached, binary-search the Prolly tree.
   O(log N).

If a secondary index is registered, `find_by(index_name, key)` is
also O(log N) but with a smaller constant (the index tree is
smaller than the data tree). For point lookups by primary key,
`get()` is sufficient — you do NOT need an index. For lookups by a
non-primary attribute, register an index and use `find_by()`.

**You should NOT register an index for the primary key.** `get()`
already provides O(log N) primary-key lookup. An index on the
primary key is redundant.

### 3.3. `find_by()` return shape

```python
view.find_by(index_name: str, index_key: str) -> Optional[Any]
```

Returns a **single decoded value** (the first match), or `None` if:
- the index has no entry for `index_key`, OR
- the index has been tombstoned (dropped via `unregister_index` or
  `drop_index` — see §4.5).

For **multi-valued lookups** (multiple entries match the same
`index_key`), use:

```python
view.find_all_by(index_name: str, index_key: str) -> list[Any]
```

Returns a **list of decoded values** (possibly empty). Returns `[]`
if the index is tombstoned or has no matching entries.

### 3.4. `get_all() -> dict[str, Any]`

```python
view.get_all() -> dict[str, Any]
```

Reads the entire current state as a `{key: decoded_data}` dict.
Keys starting with `_` (internal — schema, index, semantic
metadata) are excluded.

O(N/chunk_size + K) kernel reads. Use only for small Views or
one-time bulk reads; for large Views, prefer `keys()` + `get()`.

### 3.5. `keys() -> list[str]`, `count() -> int`, `exists(key) -> bool`

Standard read helpers. `keys()` returns non-internal keys. `count()`
returns the number of non-internal entries. `exists(key)` returns
True iff the key is in the current state.

---

## 4. Indexes (Ambiguities B, E, F)

### 4.1. Two index APIs

The SDK provides two index APIs:

| Class | Use case |
|---|---|
| `View` (in `view_sdk.py`) | Manual index management: `create_index`, `drop_index`, `lookup_by_index`. Indexes are built eagerly when created, never auto-updated. Use when you control index lifecycle explicitly. |
| `IndexedView` (in `auto_index.py`) | Automatic index management: `register_index`, `unregister_index`, `find_by`. Indexes are built lazily on first read, updated eagerly or lazily per the `mode` parameter. Use for most applications. |

Both classes store indexes the same way (see §4.4). They differ
only in update policy.

### 4.2. Index extractor signature (Ambiguity B)

```python
extractor: Callable[[Any], str]
```

The extractor receives the **decoded data** (the same value returned
by `view.get(key)`). It does NOT receive the key. It does NOT
receive the raw bytes. It returns a single string — the index key.

Example:
```python
def by_region(data: dict) -> str:
    return data.get("region", "")

view.register_index("by_region", by_region)
```

If you need to index by the primary key, you must store the primary
key inside the data (e.g., as a field `"id": key`). The extractor
cannot access the primary key directly. This is a deliberate design
choice: it keeps the extractor pure (function of data only, not of
position).

If your data does not contain its own primary key and you need to
index by it, add the primary key to the data before `put()`:
```python
view.put("user:1", {"id": "user:1", "name": "Alice"})
view.register_index("by_id", lambda d: d["id"])
```

### 4.2.1. Multi-key (multi-valued) indexes (Ambiguity K)

The extractor may return either a single string or a **list of
strings**. When it returns a list, the row is indexed under EACH
key in the list. This is the "multi-key index" pattern.

```python
# Index a document by all of its tags (list-valued field)
view.register_index("by_tag", lambda d: d.get("tags", []))

view.put("doc1", {"title": "Arrow Guide", "tags": ["arrow", "storage", "perf"]})
view.put("doc2", {"title": "Pond RFC",    "tags": ["storage", "kernel"]})
view.put("doc3", {"title": "DuckDB Tips", "tags": ["arrow", "sql"]})
view.commit("insert 3 docs")

# Now find_by("by_tag", "arrow") returns doc1 OR doc3 (first match).
# Use find_all_by("by_tag", "arrow") for all matches.
arrow_doc = view.find_by("by_tag", "arrow")
```

Extractor return value semantics:

| Return type | Behavior |
|---|---|
| `str` (single key) | Row indexed under that one key. Backward-compatible with the original single-key API. |
| `list[str]` (multi-key) | Row indexed under each key in the list. Use for list-valued fields (tags, categories) or when one row should be findable via multiple lookup keys. |
| `None` or `[]` (empty) | Row is NOT indexed. Use this to conditionally skip indexing (e.g., only index active users: `lambda d: d["user_id"] if d.get("active") else None`). |
| Other types (int, etc.) | Coerced to string via `str()`. Avoid relying on this; always return `str` or `list[str]`. |

For composite keys (e.g., `(col_a, col_b)`), use string concatenation
with a separator: `lambda d: f"{d['a']}:{d['b']}"`. This gives
prefix-scan capability on `col_a` but not on `col_b` alone. True
multi-dimensional range queries require a Hilbert-curve
materialization (see `docs/LIQUID_CLUSTERING_COMPARISON.md`).

**Hardening notes (settled per Phase B.4):**

1. **Order of returned keys is preserved in the index, but does not
   affect lookup results.** If the extractor returns
   `["alpha", "beta", "gamma"]`, the row is indexed under all three
   keys. `find_by(index_name, "alpha")`, `find_by(index_name, "beta")`,
   and `find_by(index_name, "gamma")` all return the row. The order
   in the list affects only the order in which keys are added to the
   Prolly tree during `_rebuild_index` (which is then sorted by the
   tree itself, so the order is irrelevant for lookups).

2. **Duplicate keys in the returned list are deduplicated.** If the
   extractor returns `["alpha", "alpha", "beta"]`, the row is indexed
   under `"alpha"` (once) and `"beta"` (once). The index tree stores
   one entry per unique key.

3. **Multiple rows with the same index_key: last-writer-wins for
   `find_by`, all-matches for `find_all_by`.** If two rows both
   return `"alpha"` from the extractor, the index entry for
   `"alpha"` points to the second row's blob hash (last writer
   wins). `find_by(index_name, "alpha")` returns the second row.
   To retrieve all matching rows, use `find_all_by` (which currently
   returns a list of up to 1 row due to the single-value index
   format — see §4.4.1 for the multi-valued index pattern that
   would return all matches).

4. **Extractor exceptions propagate.** If the extractor raises an
   exception (e.g., `KeyError` on a missing field), the exception
   propagates up through `put` / `_rebuild_index`. The View does
   NOT silently skip the row. If you want to skip rows with missing
   fields, catch the exception inside the extractor and return
   `None`:
   ```python
   def safe_tag_extractor(d):
       try:
           return d.get("tags", [])
       except Exception:
           return None  # row not indexed
   ```

5. **The extractor is called once per row per index rebuild.** For
   `eager` mode, that's once per commit. For `lazy` mode, that's
   once per rebuild (which happens when staleness exceeds budget).
   For incremental updates, the extractor is called once per
   `put` (to track the new row's index keys). The extractor should
   be cheap (O(1) on the data). If your extractor is expensive
   (e.g., parses a JSON string), cache the parsed result inside
   the data dict before `put`.

6. **The extractor receives the DECODED data, not the raw bytes.**
   For `View` and `IndexedView`, this is the dict returned by
   `view.decode(view.encode(data))` — i.e., the same value the
   caller passed to `put`. For `ArrowView`, the extractor receives
   a row dict (converted from the Arrow Table). See §4.2 for the
   full signature.

### 4.3. Index modes (`IndexedView` only)

When registering an auto-index, choose a mode:

| Mode | Write cost | Read cost | Use case |
|---|---|---|---|
| `"eager"` | O(N) per commit (rebuild on every commit) | O(log N), always fresh | OLAP, read-heavy workloads |
| `"lazy"` (default) | O(1) per commit (no index update) | O(log N) if fresh, O(N + log N) if stale (rebuild then lookup) | OLTP, streaming, mixed workloads |
| `"background"` | O(1) per commit | O(log N), periodically fresh | NOT IMPLEMENTED — placeholder |

For `lazy` mode, set `staleness_budget` (default 5). The index is
rebuilt when `commits_since_last_build > staleness_budget`. Lower
budget = fresher indexes but more rebuilds; higher budget = fewer
rebuilds but more stale reads.

### 4.4. Index persistence and naming (Ambiguity E)

Indexes are stored as **kernel blobs** in the kernel's object store.
Each index is a tree-like structure mapping
`f"_index/{index_name}/{index_key}"` to either a single data blob's
hash (single-valued index) or a serialized list of data blob hashes
(multi-valued index — see §4.4.1 below). The index's root hash is
stored in the kernel's root namespace under the name
`f"{view_name}__index__{index_name}"`.

Concretely:
- The index is a kernel blob (created via `kernel.write`).
- The index's root pointer is a kernel Reference (created via
  `kernel.reference`).
- Indexes are NOT in-memory. They survive process restarts.
- Indexes are NOT stored separately from data. They live in the same
  object store.

**Index format:** the SDK's built-in `View` and `IndexedView` classes
store indexes as Prolly trees (content-addressed B-trees — see
`pond-sdk/prolly_view.py` for the implementation). The Prolly tree
format is **internal to `ProllyViewBase`**; View authors do NOT need
to know it. If you are building a custom View directly on the kernel
(without extending `View`/`IndexedView`), you may store your index as
any kernel blob format you choose (JSON dict, length-prefixed binary,
etc.) — the only requirements are (a) the root pointer is a Reference
named `f"{view_name}__index__{index_name}"` and (b) the format is
deterministic (rebuildable from the View's state, per RFC-0005
Materialization Law 1).

To list all indexes for a View: `view.list_indexes()` (returns
active indexes; tombstoned indexes excluded — see §4.5).

#### 4.4.1. Multi-valued indexes

When multiple data entries share the same `index_key` (e.g., multiple
nodes of type `"user"`), the index must store **multiple** data blob
hashes for that key. Two storage patterns are supported:

1. **List-at-leaf (recommended):** the index entry at
   `_index/{index_name}/{index_key}` is a serialized list of data
   blob hashes (JSON list, or length-prefixed binary).
   `find_all_by(index_name, index_key)` deserializes the list and
   returns all matching values.
2. **Multi-entry (alternative):** store one index entry per match,
   with keys like `_index/{index_name}/{index_key}/{ordinal}`.
   More complex to scan, but allows incremental insertion without
   rewriting the leaf.

The SDK's built-in `IndexedView` uses pattern 1 by default. Custom
Views may choose either pattern.

### 4.5. `drop_index` / `unregister_index` (Ambiguity F, per RFC-0008)

```python
# In View class:
view.drop_index(index_name: str) -> bool

# In IndexedView class:
view.unregister_index(index_name: str) -> None
```

Both methods logically delete the index using the **tombstone
pattern** (RFC-0008):

1. The index's Reference (`{view_name}__index__{index_name}`) is
   rebound to `TOMBSTONE_HASH` (a fixed, globally-known hash).
2. Subsequent `lookup_by_index()` / `find_by()` calls return `None`
   immediately — readers see the index as deleted without waiting
   for compaction.
3. `list_indexes()` excludes tombstoned indexes.
4. The previously-pointed-to index tree blob becomes unreachable;
   `PondGC.collect()` (in `engineering/02_gc.py`) will sweep it on
   the next run.
5. `compact_tombstones(kernel)` (in `pond-sdk/maintenance.py`) can
   later remove the name's row from the roots SQLite table,
   reclaiming ~80 bytes of name-row storage. This is optional
   maintenance; the system is correct without it.

`drop_index` returns `True` if the index existed and was dropped,
`False` if it was not registered or already tombstoned.
`unregister_index` returns `None` and is idempotent.

To revive a tombstoned index: call `create_index` / `register_index`
again. The new index overwrites the tombstone Reference.

---

## 5. Version control

### 5.1. `branch(name) -> str`

```python
view.branch(name: str) -> str
```

Creates a new branch pointing at the current HEAD. Returns the
fully-qualified branch name (`f"{view_name}__branch__{name}"`).
Branching is O(1) — it creates one new Reference.

Raises `ValueError("No commits to branch from")` if the View has no
commits.

### 5.2. `checkout(name) -> None`

```python
view.checkout(name: str) -> None
```

Switches the View's HEAD to the named branch. After checkout, new
commits advance the branch's head, not the previous branch's head.

The staging area is cleared on checkout. Pending changes are lost.
Commit staged changes before checkout if you want to keep them.

**Current-branch tracking is IN-MEMORY.** The currently-checked-out
branch is stored in a `View` instance attribute
(`self._active_branch` in the SDK). It is NOT persisted in the
kernel namespace. On process restart, the View reverts to the
default branch (the unnamed HEAD Reference at
`kernel.resolve(view.name)`). To track current-branch across
restarts, store it yourself under a name like
`f"{view.name}__current_branch"` and call `checkout()` after
constructing the View.

### 5.3. `list_branches() -> list[str]`

Returns the short names (without the `{view_name}__branch__` prefix)
of all branches.

---

## 6. History, diff, merge (Ambiguities D, G, H)

### 6.1. Merge semantics (Ambiguity D)

```python
view.merge(branch_name: str, message: str = "") -> str
```

Merges `branch_name` into the current branch. Returns the merge
commit's hash.

**Semantics: union merge, with the merged branch winning on
conflict.** Specifically:

1. Read the current branch's full state: `state_current = read_all()`
2. Read the merged branch's full state: `state_branch = read_state_from_commit(branch_head)`
3. Compute the merged state: `merged = dict(state_current); merged.update(state_branch)`
4. Build a new Prolly tree from `merged`, write a new snapshot
   commit, advance HEAD.

This is **not** a 3-way merge. There is no common-ancestor
computation, no conflict detection, no merge markers. If both
branches modified the same key, the merged branch's value wins
silently. This is the simplest correct merge policy; future RFCs
may introduce 3-way merge as an option.

**Merge commit parents:** the merge commit has **1 parent** (the
current HEAD at merge time). The merged branch's commit hash is NOT
recorded as a second parent. This means `history()` walks only the
current branch's chain — the merged branch's individual commits are
not reachable via `history()`. They ARE reachable via `diff()`
against the merge commit. A future RFC may introduce git-style
2-parent merge commits with DAG-aware `history()`.

When to use:
- Append-only workloads (no conflicts possible).
- Workloads where the merged branch is authoritative by design
  (e.g., merging a feature branch into main where the feature
  branch's changes should win).

When NOT to use:
- Workloads with concurrent edits to the same keys (use external
  conflict resolution).
- Workloads requiring audit trails of conflicts (use `diff()` first,
  resolve manually, then `put()` + `commit()`).

### 6.2. `history(limit) -> list[dict]` (Ambiguity H)

```python
view.history(limit: int = 20) -> list[dict]
```

Returns a list of commit records, most-recent first. Each record is
a dict with EXACTLY these keys:

| Key | Type | Description |
|---|---|---|
| `"commit"` | `str` | The first 12 characters of the commit hash (a short hash, like Git). |
| `"message"` | `str` | The commit message. |
| `"timestamp"` | `float` | Unix timestamp (seconds since epoch). |
| `"index"` | `int` | The commit's position along the current branch's chain (0 = first commit on this branch). Per-branch count, NOT a global DAG topological order. |
| `"type"` | `str` | Either `"snapshot"` or `"delta"`. |

`history()` walks the **single-parent chain** from HEAD backwards.
It does NOT traverse merge-commit second parents (because merge
commits have only 1 parent in this SDK — see §6.1). To inspect the
merged branch's history, call `checkout(branch_name)` first, then
`history()`.

Example:
```python
[
    {"commit": "abc123def456", "message": "add user:4",
     "timestamp": 1721500000.0, "index": 3, "type": "delta"},
    {"commit": "789012abcdef", "message": "initial",
     "timestamp": 1721400000.0, "index": 0, "type": "snapshot"},
]
```

### 6.3. `diff(a, b) -> dict` (Ambiguity G)

```python
view.diff(a: str, b: str) -> dict
```

Computes the diff between two commits. `a` and `b` are **commit
hash prefixes** (the first N characters of a commit hash, minimum
~8 characters for uniqueness). They are NOT branch names. They are
NOT tags.

The diff walks the commit DAG from HEAD to find commits whose hash
starts with `a` (resp. `b`), then reads the full state at each
commit, then computes the set difference.

Returns a dict with EXACTLY these keys:

| Key | Type | Description |
|---|---|---|
| `"added"` | `dict[str, str]` | Keys present in `b` but not `a`. Values are the first 12 chars of the blob hash. |
| `"removed"` | `dict[str, str]` | Keys present in `a` but not `b`. Values are the first 12 chars of the blob hash. |
| `"modified"` | `dict[str, dict]` | Keys present in both but with different blob hashes. Values are `{"old": hash[:12], "new": hash[:12]}`. |

Raises `ValueError(f"Commit '{prefix}' not found")` if `a` or `b`
does not match any commit in the View's history.

To diff a branch against HEAD: first resolve the branch to its head
commit hash via `kernel.resolve(f"{view_name}__branch__{branch_name}")`,
then pass the first 12 chars of that hash to `diff()`.

---

## 7. Commit object format (Ambiguity J)

A commit is a kernel blob encoded in a binary format (see
`pond-sdk/binary_encoding.py`, `BinaryProllyTree.encode_commit`).
The format is:

```
[1B: type=3 (commit marker)]
[32B: parent commit hash (or 32 zero bytes if no parent)]
[32B: snapshot tree root hash (or 32 zero bytes if delta-only)]
[4B: delta_plus_count (uint32 LE)]
[for each delta_plus entry:]
  [2B: key_len (uint16 LE)]
  [key_len B: key bytes (UTF-8)]
  [32B: blob hash]
[4B: delta_minus_count (uint32 LE)]
[for each delta_minus key:]
  [2B: key_len (uint16 LE)]
  [key_len B: key bytes (UTF-8)]
[2B: message_len (uint16 LE)]
[message_len B: message bytes (UTF-8)]
[8B: timestamp (float64 LE)]
[4B: commit index (uint32 LE)]
```

A commit is one of two types:

1. **Snapshot commit**: `snapshot` field is non-null. The commit
   contains a full Prolly tree root. Reads can binary-search this
   tree directly. `delta_plus` and `delta_minus` are empty.
2. **Delta commit**: `snapshot` field is null. The commit contains
   only the changes (`delta_plus` for additions/modifications,
   `delta_minus` for deletions). Reads must walk the parent chain
   to find a snapshot, then apply deltas in reverse order.

The `COMPACTION_THRESHOLD` (default 4) controls when the View
writes a snapshot commit instead of a delta commit. After every 4
delta commits, the next commit is a snapshot (full state written).
**The first commit on a View is always a snapshot** (a delta commit
with no parent is nonsensical — there is nothing to delta against).

**Who needs to know this format?**

- **View authors extending `View` or `IndexedView`:** do NOT need
  to know this format. `ProllyViewBase` encodes and decodes commits
  transparently.
- **Developers building alternative View implementations directly
  on the kernel (without extending `View`/`IndexedView`):** you may
  use any commit format you choose (JSON, length-prefixed binary,
  etc.) — the only requirements are (a) the commit is a kernel blob,
  (b) the HEAD commit hash is stored in a Reference named
  `view.name`, and (c) the format is deterministic. The binary
  format above is what `ProllyViewBase` uses; you do not have to
  match it unless you want cross-View interoperability with
  `ProllyViewBase`-based Views.

---

## 8. Tombstone helpers (per RFC-0008)

The SDK provides Layer 1 helpers for logical deletion (see
RFC-0008). These are in `pond-sdk/maintenance.py`:

```python
# Add pond-sdk/ to your PYTHONPATH, then:
from maintenance import (
    TOMBSTONE_HASH,        # The globally-known tombstone marker hash
    drop_name,             # Logically delete a name (rebind to TOMBSTONE_HASH)
    is_dropped,            # True iff name is bound to TOMBSTONE_HASH
    resolve_active,        # Resolve a name, returning None for tombstoned
    compact_tombstones,    # Layer 0.5 maintenance: remove tombstoned name rows
)
```

**Import path:** the helpers live in `pond-sdk/maintenance.py`. To
import them, add the `pond-sdk/` directory to your `PYTHONPATH`
(e.g., `sys.path.insert(0, "/path/to/pond-sdk")`) and use
`from maintenance import ...`. If you are extending the SDK's `View`
or `IndexedView` class, the import is already set up for you.

**Important: `drop_name` handles the marker-blob pre-write for you.**
`drop_name(kernel, name)` internally calls `_ensure_tombstone_blob(kernel)`
which writes the constant marker blob `b"__pond_tombstone__"` (whose
SHA-256 IS `TOMBSTONE_HASH`) before calling
`kernel.reference(name, TOMBSTONE_HASH)`. This is necessary because
the kernel's `reference()` validates that the target hash refers to
an existing blob (see §1.2). Do NOT call
`kernel.reference(name, TOMBSTONE_HASH)` directly without first
writing the marker blob — it will raise
`ValueError("Hash ... does not refer to an existing blob")`.

Use these helpers when you need to delete names (e.g., dropping a
View, dropping a branch, dropping an index). The kernel itself does
NOT have a `delete_name` primitive; tombstones are data, not a
kernel feature. See RFC-0008 for the full rationale.

### 8.1. Cross-View read/write semantics (Ambiguity M)

The `CrossView` class (in `pond-sdk/view_sdk.py`) provides static
methods for reading and writing across Views. Semantics:

```python
from view_sdk import CrossView

# Read a single key from one View's current HEAD
data = CrossView.read_from(source_view, "user:1")

# Read all non-internal keys from one View's current HEAD
all_data = CrossView.read_all_from(source_view)

# Stage a write on a target View (does NOT commit; caller must commit)
CrossView.write_to(target_view, "user:1", data)
target_view.commit("copied user:1")

# Zero-copy blob sharing: copy the HASH, not the CONTENT
ok = CrossView.share_blob(from_view=source_view, from_key="user:1",
                          to_view=target_view, to_key="user:1")
# Both Views now reference the same kernel blob (content-addressed dedup)

# Bulk copy: pipe all keys from source to target
n = CrossView.pipe(source_view, target_view)
# Or with a transformer (re-encode on the way through):
n = CrossView.pipe(source_view, target_view,
                   transformer=lambda k, v: (k.upper(), {"src": v}))
target_view.commit(f"piped {n} keys")
```

**Five semantics rules (settled):**

1. **Source = HEAD commit of the source View's currently-checked-out
   branch.** CrossView does NOT take a commit-hash argument; it
   always reads from the source View's current HEAD. To read from
   a specific historical commit, check out that commit's branch
   first, then call CrossView.

2. **Tombstoned indexes are skipped.** `read_all_from` returns only
   non-internal user keys (those NOT starting with `_`). Tombstoned
   index References (which resolve to `TOMBSTONE_HASH`) are not
   visible because they live in the root namespace, not in the
   View's state.

3. **Zero-copy sharing.** `share_blob` copies the blob HASH, not
   the blob CONTENT. Both Views reference the same kernel blob
   (content-addressed dedup for free). The blob's lifetime is the
   union of the two Views' lifetimes — it is GC'd only when neither
   View references it.

4. **No cross-View atomicity.** `write_to` followed by `commit` on
   the target View is atomic for the target, but there is no
   cross-View atomic commit. If you need atomic multi-View commits,
   use a higher-level coordinator (future RFC).

5. **Pipe is non-transactional.** `pipe` reads the source's current
   state at call time and writes to the target's staging area. The
   target is NOT committed; the caller must call `to_view.commit()`
   after `pipe` returns. If the source changes during the pipe, the
   changes are NOT visible to the in-progress pipe (the state was
   snapshotted at call time).

**Hardening notes (settled per Phase B.4):**

1. **`pipe` iterates keys in arbitrary order.** The order of keys
   returned by `from_view.base.read_all()` is the Prolly tree's
   sorted order (lexicographic by key). This is deterministic for
   a given source state, but the caller should NOT rely on a
   specific order — if order matters (e.g., for reproducible
   builds), sort the keys explicitly after `pipe`.

2. **`pipe` is NOT atomic with respect to the source.** The source
   View may be modified concurrently while `pipe` is running. The
   `read_all()` call snapshots the state at one instant, but the
   Prolly tree may be rebuilt between the snapshot and the iteration.
   In practice, this is safe because `read_all()` returns a dict
   (materialized in memory); subsequent modifications to the source
   View do not affect the in-progress pipe. But the pipe may miss
   writes that happened during the iteration.

3. **`share_blob` does NOT verify the blob still exists.** It looks
   up `from_view.base.lookup(from_key)` at call time and stages
   the returned hash. If the source View is later modified (e.g.,
   the key is deleted, or a tombstone is applied), the target's
   staged reference is unaffected — the blob hash is still valid
   (the kernel's content-addressed store keeps the blob until GC).
   The target's commit will succeed.

4. **Cross-View operations do NOT create a transaction log.** There
   is no record that "View B's row X came from View A's row Y."
   If you need provenance tracking, store the source View name and
   key in the target row's data:
   ```python
   def transformer(key, data):
       return key, {**data, "_provenance": {"source_view": from_view.name, "source_key": key}}
   ```
   This is a View-level concern, not a CrossView feature.

5. **`write_to` does NOT check for key conflicts.** If the target
   View already has a row with the same key, `write_to` silently
   overwrites it (the new blob hash replaces the old in the staging
   area; the old blob is GC'd later). To detect conflicts, call
   `to_view.exists(key)` before `write_to`.

6. **CrossView methods are NOT thread-safe.** They use the source
   and target Views' internal staging areas, which are not protected
   by locks. For multi-threaded access, serialize CrossView calls
   externally.

---

## 9. Encoding (override in subclasses)

The default `View` class uses JSON for encoding:

```python
def encode(self, data: Any) -> bytes:
    return json.dumps(data, sort_keys=True).encode()

def decode(self, data: bytes) -> Any:
    return json.loads(data)
```

Subclasses override these to use a different format. Examples:

| View | Encode | Decode |
|---|---|---|
| `SQLView` | Arrow → Parquet | Parquet → Arrow |
| `VectorView` | `struct.pack` floats | `struct.unpack` |
| `GitView` | raw file bytes | raw file bytes |
| Default `View` | JSON | JSON |

The encode/decode pair MUST satisfy Law 1 (round-trip) from
RFC-0007: `decode(encode(d)) == d` for all `d`. This is checkable;
the `view_laws.py` harness verifies it.

---

## 10. What is deliberately NOT in the SDK

- **No query planner.** Views expose `get`, `find_by`, `get_all`.
  There is no SQL parser, no optimizer, no cost model. SQLView
  (Layer 3) adds SQL parsing on top of the SDK; the SDK itself is
  query-language-agnostic.
- **No transactions.** `commit` is atomic for a single View. There
  is no cross-View atomic commit, no 2PC, no isolation levels.
  Views that need transactions implement their own protocol.
- **No schema.** The SDK stores bytes; the View tracks its own
  schema. There is no schema registry, no schema evolution support.
- **No compression.** Views compress their blobs before `put()`.
  The SDK does not compress automatically.
- **No cache.** The SDK reads from the kernel on every `get()`.
  Views cache if they want to (e.g., `IndexedView`'s
  `_cached_entries`).
- **No concurrency control.** The SDK is single-threaded. Use the
  thread-safe wrapper from `engineering/01_concurrency.py` for
  multi-threaded access.

These omissions are deliberate. Each is a Layer 3 or Layer 4
concern; adding it to the SDK would violate the design goal of
keeping the SDK minimal. See `docs/NON_GOALS.md` for the full list.

---

## 11. Compliance checklist for new Views

Before claiming a View is SDK-compliant, verify:

- [ ] The View extends `View` or `IndexedView`, OR is built directly
      on the kernel following §7's "alternative View implementations"
      guidance (in which case the format requirements from §7 apply).
- [ ] The View's `encode`/`decode` pair satisfies round-trip
      (Law 1 of RFC-0007).
- [ ] The View's operations are deterministic (Law 2 of RFC-0007).
- [ ] The View does not call `kernel.reference` inside `get` or
      `find_by` (read paths are pure).
- [ ] The View does not reach past the SDK (no direct SQL on the
      kernel's `root_db`, except via `maintenance.compact_tombstones`).
- [ ] The View's indexes (if any) are stored as kernel blobs
      following the naming convention in §4.4. The format may be
      Prolly tree (if extending `ProllyViewBase`) or any
      deterministic kernel-blob format (if building directly on the
      kernel).
- [ ] The View's `drop_index` / `unregister_index` use the
      tombstone pattern from §4.5 (via `drop_name` from
      `maintenance.py`, NOT via direct `kernel.reference` to
      `TOMBSTONE_HASH` — see §1.2 and §8).
- [ ] The View passes the `view_laws.py` property-test harness
      (see `pond-sdk/view_laws.py`).

---

## 12. Relationship to other documents

- **Depends on:** RFC-0003 (Kernel Specification — the 3 primitives
  the SDK is built on), RFC-0007 (View Algebra — the 5-tuple and
  6 laws the SDK implements), RFC-0008 (Deletion as Data — the
  tombstone pattern used by `drop_index`).
- **Supersedes:** informal descriptions in
  `docs/LENS_AUTHORS_GUIDE.md`. Where the two disagree, this
  document is correct.
- **Operationalized by:** `pond-sdk/view_laws.py` (the property-test
  harness that verifies SDK compliance).
- **External validation:** the 10 ambiguities settled here were
  identified by `validation/vector_report.md`. A second external
  validation (re-running the vector challenge with this spec) is
  the success criterion for Phase B.
