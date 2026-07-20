# RFC-0008: Deletion as Data

## Status

Draft — addresses the only architectural challenge raised by external
validation (vector_report.md, finding F).

## Abstract

The external validation report flagged a real concern: the kernel
has `Reference(name, hash)` but no `Unreference(name)`. Once a name
is bound, it lingers forever. The validator proposed adding a
`delete_name` primitive.

This RFC argues against the fourth primitive. Deletion is expressible
as data via tombstone references. The "lingering name" problem is a
maintenance concern, not a kernel-calculus concern. Immutability and
deletion are in tension by nature; the kernel is honest about this.
A fourth primitive would be a lie.

---

## 1. The concern

From `validation/vector_report.md`, finding F:

> The kernel has `reference` and `resolve` but **no delete-name
> primitive**. You can add a name but never remove one. Impossible
> to truly drop an index; the name lingers forever.

And from §4:

> Truly dropping an index. The kernel has `reference(name, hash)`
> and `resolve(name)` but **no `delete_name` or `unreference`**.
> Once a name is created, it exists forever. `drop_index` and
> `unregister_index` are therefore unimplementable at the kernel
> level — they can only stop *tracking* the index in the view, not
> remove its metadata from the store.

This is a real concern. But it conflates two distinct operations:

1. **Logical deletion** — `drop_index(name)` should cause subsequent
   `resolve(name)` to behave as if the name does not exist.
2. **Physical deletion** — the row for `name` in the `roots` table
   should be removed, reclaiming storage.

The validator's complaint is mostly about (1) (correctness), with
a secondary concern about (2) (storage hygiene). We address each
separately.

---

## 2. Logical deletion is expressible as data

Define a Layer 1 convention:

```python
TOMBSTONE_HASH = hashlib.sha256(b"__pond_tombstone__").hexdigest()
# = "f3d4...e7c8" (deterministic, globally known)
```

Then:

```python
def drop_name(kernel, name):
    """Logical deletion: rebind the name to TOMBSTONE_HASH."""
    kernel.reference(name, TOMBSTONE_HASH)

def is_deleted(kernel, name):
    """Check whether a name has been logically deleted."""
    return kernel.resolve(name) == TOMBSTONE_HASH

def resolve_or_none(kernel, name):
    """Resolve a name, returning None for tombstoned names."""
    h = kernel.resolve(name)
    if h is None or h == TOMBSTONE_HASH:
        return None
    return h
```

### Why this works

- The kernel's three primitives are unchanged. No fourth primitive.
- The tombstone is just a regular blob hash. The kernel does not
  need to know it is special.
- `resolve(name)` continues to return a hash; readers check for
  `TOMBSTONE_HASH` and interpret it as "deleted."
- The convention lives at Layer 1 (State Calculus), not Layer 0
  (Storage Calculus). It is a View-level pattern, not a kernel
  feature.

### Why this is the *correct* answer

Immutable storage systems *cannot* truly delete data. They can only:

- Stop pointing to it (Git's `git gc`, Pond's tombstone).
- Copy-on-write compact away the old version (ZFS, LMDB).

Any system that pretends to support "true" deletion is either
tombstoning (and lying about it) or copy-on-write compacting (and
paying the write amplification cost). The kernel is honest: it
tombstones, and the tombstone is visible at the API surface.

The validator's proposed `delete_name` primitive would be a lie —
it would appear to delete, but the blob (and possibly the binding
history) would still exist on disk until a separate compaction
pass. Better to expose the truth.

---

## 3. Physical deletion is a maintenance concern

The "lingering name" problem (storage waste from accumulated
tombstones) is real but bounded:

- Each tombstoned name costs ~80 bytes in the `roots` SQLite table
  (name + hash + timestamp).
- A system with 10,000 tombstoned names pays ~800 KB of overhead.
- A system with 1,000,000 tombstoned names pays ~80 MB.

For most workloads this is negligible. For workloads that create
and delete millions of names (e.g., a CI system creating
`build-<timestamp>` branches), it adds up.

The right solution is a **maintenance operation**, not a kernel
primitive:

```python
def compact_tombstones(kernel):
    """Remove names whose current binding is TOMBSTONE_HASH.
    This is a Layer 0.5 maintenance operation, like VACUUM in
    PostgreSQL or `git gc` in Git."""
    names = kernel.list_names()
    for name in names:
        if kernel.resolve(name) == TOMBSTONE_HASH:
            kernel.root_db.execute(
                "DELETE FROM roots WHERE name = ?", (name,)
            )
```

This operation:
- Is **idempotent** (running it twice has the same effect as once).
- Is **safe** (only removes names already marked deleted; no
  surprise data loss).
- Is **optional** (the system is correct without it; it only reclaims
  space).
- Lives at **Layer 0.5** (storage maintenance), not Layer 0 (storage
  calculus). The kernel algebra is unchanged.

### Comparison to existing systems

| System | "Delete" operation | Reality |
|---|---|---|
| PostgreSQL | `DROP TABLE` | Marks catalog entries; `VACUUM` reclaims |
| Git | `git branch -d` | Removes ref; blob remains until `git gc` |
| Git | `git rm` | Removes from tree; blob remains until `git gc` |
| S3 | `DeleteObject` | Adds delete marker; actual deletion async |
| FoundationDB | `clear(key)` | Tombstone until next compaction |
| Pond | `Reference(name, TOMBSTONE_HASH)` | Tombstone; `compact_tombstones` reclaims |

Pond's behavior matches industry practice. The only difference is
that Pond makes the tombstone *visible* at the API, rather than
hiding it behind a `delete()` that pretends to be immediate.

---

## 4. Privacy: the one case where this matters

There is one case where tombstoning is genuinely insufficient:
**privacy-mandated deletion**. If a name itself contains sensitive
data (e.g., `user_email_addr@example.com` as a name) and a user
exercises a "right to be forgotten," the name must be *physically*
removed from the store, not just tombstoned.

For this case, `compact_tombstones` is the answer — but it must be
run promptly after the deletion, and the operator must verify that
the name is gone from the SQLite file (which may require VACUUMing
SQLite itself).

For most Pond workloads, this is not a concern. Layer 0 names are
*internal* identifiers (branch heads, snapshot pointers, view
roots), not user-supplied data. User-supplied data lives *inside*
blobs (Layer 1+), and those blobs are content-addressed — they can
be unreferenced and GC'd via the existing GC reachability law.

### Design recommendation

Pond's documentation should explicitly state:

> Layer 0 names are internal identifiers. Do not store sensitive
> user data in Layer 0 names. User data lives in blobs, addressed
> by content hash, and is GC'd when no longer reachable.

This is a usage guideline, not a kernel constraint.

---

## 5. What about deleting blobs? (Reconciliation with existing PondGC)

A related question: can you delete a *blob* (not just a name)?

**This is already implemented.** `engineering/02_gc.py` ships a
`PondGC` class — a View-level garbage collector that:

1. Walks all names in the root namespace.
2. For each name, resolves to its hash, reads the blob, finds all
   64-char hex strings in the blob content (heuristic), and recursively
   marks those objects as reachable.
3. Sweeps the object store, deleting any blob not marked reachable.

This is documented as Composition Law 3 in `FORMAL_SPEC.md` and as
a Non-Goal in `NON_GOALS.md` ("Pond's kernel has no GC. Orphaned
objects accumulate. GC is a View-level utility (`PondGC` in
`engineering/02_gc.py`)").

### How tombstones compose with existing PondGC

Tombstones and PondGC are complementary, not competing:

| Concern | Mechanism | Layer | File |
|---|---|---|---|
| Logical name deletion | `Reference(name, TOMBSTONE_HASH)` | Layer 1 | `pond-sdk` (new helpers) |
| Physical name-row removal | `compact_tombstones(kernel)` | Layer 0.5 | `pond-maintenance` (new) |
| Blob reclamation | `PondGC.collect(kernel)` | Layer 0.5 | `engineering/02_gc.py` (existing) |

The interaction is clean:

1. **`drop_name(kernel, name)`** rebinds the name to `TOMBSTONE_HASH`.
   The previously-pointed-to blob is now unreachable (no name points
   to it; the tombstone points to a fixed, shared, near-empty blob).

2. **`PondGC.collect(kernel)`** runs its existing reachability walk.
   The previously-pointed-to blob is no longer reachable → it gets
   swept. No change to `PondGC` required. The tombstone hash itself
   is reachable (it appears in the `roots` table), but the *blob*
   it points to is tiny and shared — reclamation of the *original*
   blob is what matters.

3. **`compact_tombstones(kernel)`** removes the row for the tombstoned
   name from the `roots` SQLite table, reclaiming the ~80 bytes of
   name-row storage. This is a separate concern from blob reclamation.

### One subtle point: the heuristic in PondGC

`PondGC` uses a regex (`[0-9a-f]{64}`) to find embedded hashes in
blob content. This is **conservative** — it never deletes a reachable
object, but it may keep an unreachable one if its hash happens to
appear in some blob's content by coincidence.

With tombstones, this heuristic still works correctly:

- A tombstoned name's `roots` row points to `TOMBSTONE_HASH`.
- `PondGC` reads the blob at `TOMBSTONE_HASH`, finds no embedded
  hashes (it's a constant marker blob), marks `TOMBSTONE_HASH` as
  reachable, and stops.
- The previously-pointed-to blob is not visited from any name →
  not marked → swept.

So tombstones are safe with the existing heuristic GC. No change
to `PondGC.collect()` is required for tombstone support. The only
new code is `compact_tombstones` for the (separate, much smaller)
name-row storage concern.

### What if a View wants precise (non-heuristic) GC?

Views that want precise reachability — e.g., a `SQLView` that tracks
exactly which blobs are in its tree, no false positives — can
implement their own `walk_references(kernel, h)` that follows the
View's specific blob structure (tree → entries → child hashes) rather
than the heuristic. The `PondGC` class is designed to be overridable
for this case (see its docstring: "Views with specific GC policies
can implement their own GC that walks their specific reference
chains instead of using the heuristic").

This RFC does not change that design. It only adds the tombstone
convention at Layer 1 and the `compact_tombstones` maintenance op
at Layer 0.5.

---

## 6. SDK changes

The SDK gains two Layer 1 helpers (not kernel primitives):

```python
# In pond-sdk, NOT in pond-core
TOMBSTONE_HASH = hashlib.sha256(b"__pond_tombstone__").hexdigest()

def drop_name(kernel, name):
    """Logical deletion of a name. Idempotent."""
    kernel.reference(name, TOMBSTONE_HASH)

def is_dropped(kernel, name):
    """True if name is bound to TOMBSTONE_HASH."""
    return kernel.resolve(name) == TOMBSTONE_HASH

def resolve_active(kernel, name):
    """Resolve a name, returning None for unbound or tombstoned names."""
    h = kernel.resolve(name)
    if h is None or h == TOMBSTONE_HASH:
        return None
    return h
```

Views that support `drop_index`, `drop_branch`, `unregister_view`,
etc. use `drop_name` internally. The validator's `drop_index` use
case is now implementable.

### Test of the proposal

The validator's specific complaint was: "drop_index is impossible
because the name lingers forever."

With this RFC:

```python
def drop_index(self, index_name):
    """Drop an index. After this, find_by(index_name, ...) returns []."""
    drop_name(self.kernel, f"{self.name}:index:{index_name}")
    self._index_cache.pop(index_name, None)

def find_by(self, index_name, key):
    """Look up by index. Returns [] if index is dropped."""
    if is_dropped(self.kernel, f"{self.name}:index:{index_name}"):
        return []
    # ... existing logic
```

`drop_index` is now implementable. The name "lingers" in the SQLite
table (true), but it is logically deleted — `find_by` returns `[]`,
`register_index` again creates a fresh binding, and
`compact_tombstones` reclaims the storage row when convenient.

The validator's correctness concern is resolved. The storage-hygiene
concern is delegated to a maintenance operation, as it should be.

---

## 7. What about the "no delete in the kernel" lower-bound proof?

The kernel's lower-bound proof (RFC-0003) shows that 3 primitives
are necessary: any 2-primitive merge fails to express either
immutability, addressability, or name-mutability.

Does adding a fourth primitive (`Unreference`) weaken the lower
bound? No — the lower bound is a *minimum* (≥3), not an exact
count (=3). Adding a fourth primitive would not contradict the
lower bound; it would just be unnecessary.

The argument against a fourth primitive is **not** "the lower bound
forbids it." The argument is:

1. Deletion is expressible as data (Section 2).
2. The expression is clean (no special cases, no kernel-level
   semantics for tombstones).
3. The kernel's responsibility is storage algebra, not storage
   hygiene. Hygiene is a maintenance concern (Section 3).
4. Adding `Unreference` would create a false promise (that deletion
   is immediate) and would require defining its semantics for
   reference history, branching, and snapshots — none of which the
   kernel currently tracks.

Point 4 is the strongest. The kernel currently has no concept of
"reference history" — `Reference(name, hash)` is a destructive
update. To define `Unreference` properly, we would need to decide:

- Does `Unreference(name)` also erase the history of past bindings?
- If a name is unreferenced, can it be re-referenced later?
- What happens to blobs that were only reachable through the
  unreferenced name?
- Does `Unreference` propagate to snapshots that captured the name?

Each of these is a design question with no obviously correct answer.
Each would add complexity to the kernel. None of them arise with
the tombstone approach, because the tombstone is just another
binding — it composes with the existing semantics without
introducing new cases.

---

## 8. Conclusion

**Do not add a fourth primitive.**

- Deletion is expressible as data via `Reference(name, TOMBSTONE_HASH)`.
- The tombstone is a Layer 1 convention; the kernel is unchanged.
- Physical reclamation is a Layer 0.5 maintenance operation
  (`compact_tombstones`), analogous to `VACUUM` / `git gc`.
- Privacy-mandated deletion is handled by prompt compaction plus
  SQLite VACUUM; Layer 0 names should not contain sensitive data
  anyway (usage guideline).
- The lower-bound proof is unaffected.
- The semantic complexity of a true `Unreference` primitive is not
  worth the benefit, especially since the benefit is largely
  cosmetic (the tombstone approach is correct; it just exposes the
  truth).

The external validator's concern was real but misdiagnosed. The
fix is at Layer 1 (SDK helpers) and Layer 0.5 (maintenance), not
at Layer 0 (kernel calculus).

---

## 9. Implementation checklist

- [ ] Add `TOMBSTONE_HASH` constant to `pond-sdk` (not `pond-core`).
- [ ] Add `drop_name`, `is_dropped`, `resolve_active` SDK helpers.
- [ ] Update `IndexedView.drop_index` to use `drop_name`.
- [ ] Update `View` base class to expose `drop_branch` via `drop_name`.
- [ ] Add `compact_tombstones(kernel)` to a new `pond-maintenance`
      package (Layer 0.5).
- [ ] Document the "Layer 0 names are internal identifiers" usage
      guideline in `VIEW_AUTHORS_GUIDE.md`.
- [ ] Add a test: tombstone a name, verify `resolve` returns
      `TOMBSTONE_HASH`, verify `resolve_active` returns None,
      verify `compact_tombstones` removes the row.

---

## 10. Relationship to other RFCs and existing code

- **Depends on:** RFC-0003 (Kernel Specification — the three
  primitives this RFC refuses to extend).
- **Reconciles with:** `engineering/02_gc.py` (existing PondGC —
  handles blob reclamation; tombstones complement it by handling
  name-row reclamation). No conflict: tombstones and PondGC operate
  on different layers (names vs. blobs) and compose cleanly.
- **Reconciles with:** `FORMAL_SPEC.md` Composition Law 3 (GC
  reachability) — unchanged. Tombstones just make some names point
  to a fixed marker; the reachability walk still works.
- **Reconciles with:** `NON_GOALS.md` ("Pond's kernel has no GC.
  Orphaned objects accumulate. GC is a View-level utility") —
  unchanged. `compact_tombstones` is also a View/maintenance-level
  utility, not a kernel primitive.
- **Informs:** RFC-0007 §9 (the algebra does not need a deletion
  axis; tombstones are just data).
- **Closes:** the open question from
  `validation/vector_report.md` finding F.
- **Does not modify:** any kernel code. The kernel stays at ~140 LOC
  and 3 primitives.
