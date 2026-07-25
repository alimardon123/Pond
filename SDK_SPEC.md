# Pond SDK Specification

> **Status:** Authoritative contract for the Pond SDK. Every method
> signature, return shape, and behavior in this document is
> checkable; violations are bugs.
>
> **Audience:** Lens authors (both human and AI agents) building
> Lenses on top of `pond-sdk/`. If you read only one SDK document,
> read this one.
>
> **Source of truth:** This document supersedes informal descriptions
> in `docs/LENS_GUIDE.md`. Where the two disagree, this document is
> correct.

---

## 0. What this document settles

This document settles 13 ambiguities (A–M) that caused friction when
external validators tried to build Lenses from the SDK spec alone.

| ID | Ambiguity | Settled in section |
|---|---|---|
| A | How is a kernel obtained? | §1.1 |
| B | Index extractor call signature | §4.2 |
| C | `get()` complexity | §3.2 |
| D | Merge semantics | §6.1 |
| E | Index persistence and naming | §4.4 |
| F | `drop_index` / `unregister_index` | §4.5 |
| G | `diff(a, b)` parameter types | §6.3 |
| H | `history()` return shape | §6.2 |
| I | `put_raw` semantics | §2.3 |
| J | Commit object format | §7 |
| K | Multi-key (multi-valued) indexes | §4.2.1 |
| L | Auto-key mode and primary-keyless Lenses | §2.6 |
| M | Cross-Lens read/write semantics | §8.1 |

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
primitives. They do not extend the kernel's algebra. See
`docs/POND_FORMAL_ALGEBRAS.md` §9 for the formal substrate specification.

> **⚠ Important consequence of `reference()` validation:** you cannot
> bind a name to a hash whose blob does not exist on disk. This
> matters for tombstones (see §4.5, §8): `drop_name` handles this
> for you by pre-writing the tombstone marker blob before rebinding.
> Do NOT call `kernel.reference(name, TOMBSTONE_HASH)` directly
> without first writing the marker blob — it will raise
> `ValueError`.

### 1.3. Terminology: KeyValueLens / Lens / View

The preferred term is **KeyValueLens** (the app-facing KEY-VALUE
lens). Two legacy aliases are kept for backward compatibility:
  - `Lens`  = `KeyValueLens`  (old class name)
  - `View`  = `KeyValueLens`  (older class name)

All three names refer to the same class and are interchangeable.
New code and documentation should use `KeyValueLens`. The class
lives in `pond-sdk/keyvalue_lens.py`; the old `pond-sdk/lens_sdk.py`
is now a backward-compat shim that re-exports from `keyvalue_lens`.

KeyValueLens is NOT the universal base class — that's `PondLens`
in `pond-sdk/pond_lens.py`. KeyValueLens is a peer of `LakehouseLens`
and `FeatureStoreLens`; all three extend `PondLens` directly.

### 1.4. Constructing a Lens

A KeyValueLens is constructed with its kernel instance and a name:

```python
from keyvalue_lens import KeyValueLens

lens = KeyValueLens(kernel, name: str)
# or, for an auto-indexed lens:
from auto_index import IndexedLens
lens = IndexedLens(kernel, name: str)
# or, for a custom lens:
class MyLens(KeyValueLens):
    ...
lens = MyLens(kernel, name: str)
```

The `name` is the lens's identifier in the kernel's root namespace.
It appears in:
- The HEAD reference: `collections/{name}/HEAD` → latest commit hash.
- Branch references: `collections/{name}/branches/{branch}`.
- Index references: `{name}__index__{index_name}` (legacy convention).
- Definition reference: `collections/{name}/definition` (optional metadata).

The name must be a non-empty string. It must not contain `__`
(double-underscore) — that sequence is reserved for the kernel
namespace convention (see §2.5).

### 1.5. Lifetime

The kernel holds an open SQLite connection. Call `kernel.close()` to
release it. The kernel is NOT thread-safe by default.

---

## 2. Writing data (Ambiguity I: `put_raw`)

### 2.1. `put(key, data) -> str`

```python
lens.put(key: str, data: Any) -> str
```

Stages a key→blob mapping for the next commit. The data is encoded
via `lens.encode(data)` (default: JSON), written to the kernel as a
blob, and the resulting hash is staged under `key`. Returns the
blob hash.

This is the standard write path. The data passes through `encode`;
the Lens does NOT see the raw bytes.

### 2.2. `delete(key) -> None`

```python
lens.delete(key: str) -> None
```

Stages a deletion for `key`. The deletion is NOT immediate; it
takes effect on the next `commit()`. Internally, this stages a
tombstone (see §8 and RFC-0008 in `docs/archive/rfcs/`).

### 2.3. `put_raw(key, blob_hash) -> None` (Ambiguity I)

```python
lens.put_raw(key: str, blob_hash: str) -> None
```

Stages a key→blob mapping **without encoding**. The `blob_hash` must
refer to an already-written blob. This is the "raw bytes" write
path — the Lens does NOT call `encode`; it stages a pre-existing
blob hash directly.

Use cases:
- **Cross-Lens blob sharing:** Lens A writes a blob; Lens B wants
  to reference the same blob under a different key without copying.
- **Pre-computed blobs:** the application has already encoded the
  data (e.g., Parquet bytes) and wants to stage the hash directly.

### 2.4. `commit(message) -> str`

```python
commit_hash = lens.commit(message: str = "") -> str
```

Atomically commits all staged puts and deletes. Returns the new
commit hash. The commit is atomic: either all staged changes are
applied, or none are. After commit, the staging area is cleared.

The commit creates a new commit blob in the kernel containing:
- `parent`: the previous HEAD commit hash (or `None` for the first commit)
- `second_parent`: the second parent for merge commits (or `None`)
- `snapshot`: the Prolly tree root hash (for snapshot commits)
- `delta`: the staged changes (for delta commits)
- `message`: the commit message
- `timestamp`: wall-clock time
- `index`: commit sequence number

See §7 for the full commit object format.

### 2.5. Naming conventions

The kernel's root namespace is flat. The following naming conventions
are used by the SDK:

| Pattern | Purpose | Example |
|---|---|---|
| `collections/{name}/HEAD` | HEAD commit reference (shared namespace) | `collections/analytics/orders/HEAD` |
| `collections/{name}/branches/{branch}` | Branch reference | `collections/analytics/orders/branches/dev` |
| `collections/{name}/definition` | Optional lens-specific metadata | `collections/analytics/orders/definition` |
| `collections/{name}/snapshot` | Latest snapshot pointer (ProllyLensBase) | `collections/analytics/orders/snapshot` |
| `{name}__index__{index}` | Index reference (legacy convention) | `analytics/orders__index__by_region` |
| `__schema/{name}/v{version}` | Schema version (Schema Registry) | `__schema/user_features/v1` |
| `__stats/{name}` | Statistics (Physical Structure) | `__stats/users` |
| `__bloom/{name}` | Bloom filter (Physical Structure) | `__bloom/user_features` |

### 2.6. Auto-key mode and primary-keyless Lenses (Ambiguity L)

```python
key = lens.put_auto(data: Any) -> str
```

Stages data with an auto-generated primary key (UUID4 hex, 32 chars).
Returns the generated key. Use this when your data does not have a
natural primary key (event logs, time-series, append-only streams).

For primary-keyless Lenses (where every entry is append-only and
looked up by scan, not by key), use `KeylessLens` (in
`pond-sdk/keyvalue_lens.py`):

```python
from keyvalue_lens import KeylessLens

lens = KeylessLens(kernel, "events")
lens.put(None, {"event": "click", "user": "u1", "ts": 1721500000})
```

---

## 3. Reading data

### 3.1. `get(key) -> Any`

```python
data = lens.get(key: str) -> Any
```

Reads the value for `key` from the current HEAD. Returns `None` if
the key does not exist or has been deleted. The blob is read from
the kernel and decoded via `lens.decode(bytes)`.

### 3.2. `get()` complexity (Ambiguity C)

- **`KeyValueLens` (the app-facing KV lens):** O(log N) — uses a
  Prolly tree (ProllyLensBase) for O(log N) point lookup. This is
  the only KV lens class; `Lens` and `View` are aliases for it.
- **`ProllyLensBase` (the storage backend):** O(log N) — the
  underlying Prolly tree implementation used by KeyValueLens.

**Recommendation:** extend `KeyValueLens` for production KV lenses.
For auto-indexing, extend `IndexedLens` (in `auto_index.py`), which
adds eager/lazy auto-index management on top of the same Prolly
tree storage.

### 3.3. `get_all() -> dict`

```python
all_data = lens.get_all() -> dict[str, Any]
```

Returns all key→value pairs in the current HEAD. O(N) — reads the
entire snapshot.

### 3.4. `find_by(index_name, value) -> list`

```python
results = lens.find_by(index_name: str, value: Any) -> list
```

Finds all keys whose indexed value matches `value`. Requires that
index `index_name` has been created (see §4). O(log N) lookup via
the index's Prolly tree, then O(K) to fetch K matching blobs.

---

## 4. Index management

### 4.1. `create_index(name, extractor) -> str`

```python
index_hash = lens.create_index(
    name: str,
    extractor: Callable[[str, bytes], Any]
) -> str
```

Builds a secondary index. The `extractor` is called for each
(key, blob_bytes) pair in the current snapshot and returns the
indexed value. The index is stored as a Prolly tree mapping
`indexed_value → blob_hash`.

**Important:** `create_index` works on METADATA ONLY. It does NOT
touch data blobs. It scans the snapshot once, builds the index tree,
and stores it as a new blob. Data blobs are immutable and never
rewritten.

### 4.2. Index extractor signature (Ambiguity B)

```python
def extractor(key: str, blob_bytes: bytes) -> Any:
    # key: the key under which the blob is stored
    # blob_bytes: the raw blob bytes (NOT decoded)
    # returns: the value to index on
    ...
```

The extractor receives RAW bytes, not decoded data. This is because
the index is a Physical Structure (kernel-level), not a Lens-level
construct. The extractor must parse the bytes itself if it needs
structured data.

**Example:**
```python
def extract_region(key: str, blob_bytes: bytes) -> str:
    row = json.loads(blob_bytes)  # parse JSON
    return row.get("region", "")

lens.create_index("by_region", extract_region)
```

### 4.2.1. Multi-key indexes (Ambiguity K)

For multi-valued indexes (where one row maps to multiple index
values), the extractor returns a `list`:

```python
def extract_tags(key: str, blob_bytes: bytes) -> list:
    row = json.loads(blob_bytes)
    return row.get("tags", [])  # multiple values per row

lens.create_index("by_tag", extract_tags)
```

The index stores each (value, key) pair separately. `find_by("by_tag",
"python")` returns all keys whose `tags` list contains `"python"`.

### 4.3. `find_by_index(name, value)` — alias for `find_by`

```python
results = lens.find_by_index(name: str, value: Any) -> list
```

Alias for `find_by(name, value)` (see §3.4).

### 4.4. Index persistence and naming (Ambiguity E)

Indexes are persisted as kernel blobs, referenced by:
```
{name}__index__{index_name}
```

For example, if the Lens name is `analytics/orders` and the index
name is `by_region`, the index reference is:
```
analytics/orders__index__by_region
```

The index blob is a serialized Prolly tree (binary format, see
`pond-sdk/binary_encoding.py`). The tree maps `indexed_value →
blob_hash`.

### 4.5. `drop_index` / `unregister_index` (Ambiguity F)

```python
from maintenance import drop_name

drop_name(kernel, f"{name}__index__{index_name}")
```

Dropping an index uses the **tombstone pattern** (RFC-0008): the
index reference is rebound to a tombstone marker blob. The index
blob itself becomes unreachable and is collected by GC.

**Do NOT** call `kernel.reference(name, TOMBSTONE_HASH)` directly —
the kernel validates that the hash exists. Use `drop_name` from
`pond-sdk/maintenance.py`, which pre-writes the tombstone marker.

### 4.6. `refresh_index(name, extractor)`

```python
lens.refresh_index(name: str, extractor: Callable) -> str
```

Rebuilds an existing index from the current snapshot. METADATA ONLY
— data blobs are not touched. The old index blob becomes unreachable;
the new index blob is stored and referenced.

### 4.7. `list_indexes() -> list`

```python
index_names = lens.list_indexes() -> list[str]
```

Returns the names of all indexes for this Lens. Scans the kernel's
root namespace for `{name}__index__*` references.

---

## 5. Branching and merging

### 5.1. `branch(name) -> str`

```python
head_hash = lens.branch(name: str) -> str
```

Creates a new branch pointing at the current HEAD. O(1) — no data
is copied. The branch is stored as a kernel reference:
`{lens_name}__branch__{name}`.

### 5.2. `checkout(name) -> None`

```python
lens.checkout(name: str) -> None
```

Switches the Lens's HEAD to point at the named branch. Subsequent
commits go to the branch, not the main HEAD.

### 5.3. `merge(branch_name) -> str` (Ambiguity D)

```python
merge_hash = lens.merge(branch_name: str) -> str
```

Merges a branch into the current HEAD. Creates a merge commit with
TWO parents (`parent` = current HEAD, `second_parent` = branch HEAD).

**Merge semantics:** the default merge is **union, last-writer-wins**.
The merged state is the union of both branches' key→value mappings;
where both branches modified the same key, the branch's value wins.

**Custom merge:** subclasses can override `merge()` to implement
3-way merge, CRDT merge, or domain-specific merge. The kernel only
records the topology (2-parent commit); the Lens defines the
semantics.

### 5.4. `list_branches() -> list`

```python
branches = lens.list_branches() -> list[str]
```

Returns the names of all branches for this Lens.

---

## 6. History and diff

### 6.1. `history(limit) -> list` (Ambiguity H)

```python
commits = lens.history(limit: int = 20) -> list[dict]
```

Returns a list of commit dictionaries, most recent first. Each
dictionary contains:

```python
{
    "hash": str,           # commit hash (64-char hex)
    "parent": str | None,  # parent commit hash
    "second_parent": str | None,  # second parent (merge commits)
    "message": str,        # commit message
    "timestamp": float,    # wall-clock time
    "index": int,          # commit sequence number
}
```

### 6.2. `diff(a, b) -> dict` (Ambiguity G)

```python
changes = lens.diff(a: str, b: str) -> dict
```

Computes the difference between two commits. Parameters `a` and `b`
are commit hashes (strings). Returns:

```python
{
    "added": dict,     # keys in b but not in a: {key: value}
    "removed": dict,   # keys in a but not in b: {key: value}
    "modified": dict,  # keys in both with different values: {key: (old, new)}
}
```

### 6.3. `undo(steps) -> str`

```python
new_head = lens.undo(steps: int = 1) -> str
```

Moves HEAD back `steps` commits. Creates a new commit that is a
copy of the target commit's snapshot. Does NOT delete history; the
old commits remain reachable for time travel.

---

## 7. Commit object format (Ambiguity J)

A commit is a JSON blob stored in the kernel:

```json
{
    "type": "commit",
    "parent": "abc123...",       // parent commit hash (or null)
    "second_parent": "def456...", // second parent (merge commits, or null)
    "snapshot": "ghi789...",     // Prolly tree root hash (snapshot commits)
    "delta": {                   // staged changes (delta commits, or null)
        "additions": {"key1": "hash1", ...},
        "deletions": ["key2", ...]
    },
    "message": "commit message",
    "timestamp": 1721500000.0,
    "index": 42
}
```

A commit is either:
- **Snapshot commit:** `snapshot` is set, `delta` is null. Contains
  a full Prolly tree root.
- **Delta commit:** `delta` is set, `snapshot` is null. Contains
  only the changed keys.
- **Merge commit:** `second_parent` is set. Always a snapshot commit
  (contains the full merged state).

The Tiered Commit Model (delta + snapshot + snapshot pointer) is a
Lens-level strategy, not a kernel concept. The kernel stores
commits; the Lens decides the tiering policy.

---

## 8. Cross-Lens semantics (Ambiguity M)

### 8.1. Cross-Lens read/write

Multiple Lenses can share the same kernel namespace. If two Lenses
use the same `name`, they share:
- The same Prolly tree (same snapshot)
- The same commit DAG (same history)
- The same branches
- The same indexes

**One write → all Lenses see it immediately.** No ETL, no sync, no
manifest. This is the core architectural insight (see
`pond-lab/track1_compat_matrix.py` for the formal compatibility
contract).

### 8.2. Cross-Lens blob sharing

Lenses with different names can still share blobs via `put_raw`:

```python
# Lens A writes a blob
blob_hash = lens_a.put("key1", data)

# Lens B references the same blob under a different key
lens_b.put_raw("key2", blob_hash)
```

Both Lenses now point at the same immutable bytes. No copying.

### 8.3. Cross-Lens Physical Structure sharing

Physical Structures (indexes, statistics, bloom filters) are stored
as kernel blobs with naming conventions (`__stats/{name}`,
`__bloom/{name}`). Any Lens can read them. See
`pond-lab/track2_index_portability.py` for the proof.

---

## 9. Tombstone helpers

Tombstone helpers implement deletion-as-data (RFC-0008). These are
in `pond-sdk/maintenance.py`:

| Function | Signature | Purpose |
|---|---|---|
| `drop_name(kernel, name)` | `(kernel, name: str) -> None` | Rebind a name to a tombstone marker. Pre-writes the marker blob. |
| `is_dropped(kernel, name)` | `(kernel, name: str) -> bool` | Check if a name is tombstoned. |
| `resolve_active(kernel, name)` | `(kernel, name: str) -> str \| None` | Resolve a name, returning None if tombstoned. |
| `compact_tombstones(kernel)` | `(kernel) -> dict` | Remove tombstone markers and their targets. Returns stats. |

**Do NOT** call `kernel.reference(name, TOMBSTONE_HASH)` directly —
the kernel validates that the hash exists. Use `drop_name`, which
pre-writes the marker blob.

---

## 10. Encoding (override in subclasses)

The default `View` class uses JSON for encoding:

```python
def encode(self, data: Any) -> bytes:
    return json.dumps(data, sort_keys=True).encode()

def decode(self, data: bytes) -> Any:
    return json.loads(data)
```

Subclasses override these to use a different format. Examples:

| Lens | Encode | Decode | Location |
|---|---|---|---|
| `LakehouseLens` | PyArrow Table → Parquet | Parquet → PyArrow Table | `lenses/lakehouse/lakehouse.py` |
| `FeatureStoreLens` | PyArrow Table → Parquet | Parquet → PyArrow Table | `pond-labs/feature_store_lens.py` |
| `VectorLens` | `struct.pack` floats | `struct.unpack` | `lenses/vector/vector_view.py` |
| Default `KeyValueLens` | JSON | JSON | `pond-sdk/keyvalue_lens.py` |

The encode/decode pair MUST satisfy Law 1 (round-trip):
`decode(encode(d)) == d` for all `d`. This is verified by
`pond-sdk/lens_laws.py`.

---

## 11. What is deliberately NOT in the SDK

- **No query planner.** Lenses expose `get`, `find_by`, `get_all`.
  There is no SQL parser, no optimizer, no cost model. The Lakehouse
  Lens adds SQL via DuckDB on top of the SDK; the SDK itself is
  query-language-agnostic.
- **No transactions.** `commit` is atomic for a single Lens. There
  is no cross-Lens atomic commit, no 2PC, no isolation levels. The
  `TwoPhaseCommitCoordinator` in `services/replication/` provides
  cross-Collection atomicity as an application-level service (per
  A7: coordinator out-of-model).
- **No schema in the SDK.** The SDK stores bytes; the Lens tracks
  its own schema. The Schema Registry (`services/schema/`) provides
  versioned schemas as a cross-cutting service, not an SDK feature.
- **No compression in the SDK.** The Transport Layer
  (`services/transport/`) handles compression + encryption as a
  cross-cutting service. The SDK does not compress automatically.
- **No cache.** The SDK reads from the kernel on every `get()`.
  Lenses cache if they want to (e.g., `IndexedLens`'s
  `_cached_entries`).
- **No concurrency control.** The SDK is single-threaded.

These omissions are deliberate. Each is a Layer 3 or cross-cutting
concern; adding it to the SDK would violate the design goal of
keeping the SDK minimal. See `docs/NON_GOALS.md` for the full list.

---

## 12. Compliance checklist for new Lenses

Before claiming a Lens is SDK-compliant, verify:

- [ ] The Lens extends `KeyValueLens` (for KV-style lenses) or
      `PondLens` directly (for lenses with custom storage like
      `LakehouseLens`), OR is built directly on the kernel following
      §7's commit format.
- [ ] The Lens's `encode`/`decode` pair satisfies round-trip
      (Law 1 of the Lens algebra).
- [ ] The Lens's operations are deterministic (Law 2).
- [ ] The Lens does not call `kernel.reference` inside `get` or
      `find_by` (read paths are pure).
- [ ] The Lens does not reach past the SDK (no direct SQL on the
      kernel's `root_db`, except via `maintenance.compact_tombstones`).
- [ ] The Lens's indexes (if any) are stored as kernel blobs
      following the naming convention in §4.4.
- [ ] The Lens's `drop_index` uses the tombstone pattern from §4.5
      (via `drop_name` from `maintenance.py`).
- [ ] The Lens passes the `lens_laws.py` property-test harness
      (see `pond-sdk/lens_laws.py`).
- [ ] The Lens passes the bidirectional compatibility matrix
      (see `pond-lab/track1_compat_matrix.py`) when tested against
      at least one other Lens.

---

## 13. Relationship to other documents

- **Depends on:** `docs/POND_FORMAL_ALGEBRAS.md` §9 (Substrate
  Algebra — the 6 substrates and 10 axioms the SDK is built on),
  the Lens algebra (L1-L7 laws the SDK implements), RFC-0008
  (Deletion as Data — the tombstone pattern used by `drop_index`).
- **Supersedes:** informal descriptions in `docs/LENS_GUIDE.md`.
  Where the two disagree, this document is correct.
- **Operationalized by:** `pond-sdk/lens_laws.py` (the property-test
  harness that verifies SDK compliance) and `pond-lab/track1_compat_matrix.py`
  (the bidirectional compatibility matrix).
- **External validation:** the 13 ambiguities settled here were
  identified by external validators (reports in `archive/validation/`).
