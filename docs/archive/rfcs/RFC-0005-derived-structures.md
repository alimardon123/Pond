# RFC-0005: Materialization Calculus

## Status

Draft — potentially the most important conceptual contribution since
the lower-bound proof.

## Abstract

Every auxiliary object in Pond (indexes, materialized views, statistics,
bloom filters, feature vectors, semantic aggregates, search indexes,
zone maps, histograms) is the same thing: a **materialization** — a
function of a snapshot. This RFC formalizes that unification.

> **Terminology note (post-review):** This RFC was originally titled
> "Derived Structure Calculus." The architecture review (see
> `DESIGN_GOALS.md` and `worklog.md`) recommended adopting the
database-literature term "materialization" — the same concept used by
> materialized views, materialized features, materialized aggregates.
> The calculus `materialization = f(snapshot)` is more elegant and
> aligns with industry vocabulary. The old term "derived structure"
> remains as a synonym in earlier documents; new work uses
> "materialization."
>
> In RFC-0007 (View Algebra), materializations are the `M` component
> of the Lens 5-tuple `V = (Σ, A, E, D, M)`.

---

## 1. The Observation

Currently, Pond has:
- Secondary indexes (auto_index.py)
- Materialized views (RFC-0004: DerivedView pattern)
- Statistics (min/max per column)
- Bloom filters
- Feature vectors (feature_store.py)
- Semantic aggregates (semantic_view.py)
- Search indexes (full-text, vector)
- Zone maps
- Histograms

Each is implemented as a separate concept with separate code. But they
all share the same fundamental structure:

```
derived = f(snapshot)
```

A snapshot is a point-in-time view of a Lens's state (a commit hash).
A materialization is any object computed from that snapshot.

---

## 2. Formal Definition

A **Materialization** M is a 4-tuple:

```
M = (Source, Function, Trigger, Storage)
```

where:

### Source: the snapshot(s) the structure is derived from
- Usually: the latest commit of a single View
- Could be: multiple Views (e.g., a join index across two Views)
- Could be: a specific historical commit (e.g., time-travel statistics)

### Function: the derivation function
- Takes: snapshot state (key→blob_hash mapping)
- Returns: derived data (any bytes — an index tree, a JSON aggregate, a bloom filter)
- Must be deterministic: same snapshot → same derived data

### Trigger: when to recompute
- **Eager**: recompute on every commit (slow writes, always-fresh reads)
- **Lazy**: recompute on read when stale (fast writes, eventually-fresh reads)
- **Background**: recompute periodically (fast writes, periodic refresh)
- **Manual**: recompute only when explicitly called (user control)

### Storage: where the derived data lives
- Usually: a Prolly tree (content-addressed, like indexes)
- Could be: a raw blob (e.g., a single statistics JSON)
- Could be: a separate View (e.g., a materialized view IS a Lens)

---

## 3. The Materialization Laws

### Law 1: Determinism
```
∀ snapshot S: Function(S) = M
```
The same snapshot always produces the same materialization. This is
guaranteed by content-addressing: the same keys→hashes always produce
the same derived bytes.

### Law 2: Derivability
```
∀ M: ∃ S such that Function(S) = M
```
Every materialization can be recomputed from its source snapshot.
If the materialization is lost, it can be rebuilt. This is the
"materialization = cache" principle — materializations are never canonical.

### Law 3: Staleness
```
If Source changes from S₁ to S₂, then:
  M₁ = Function(S₁)  (stale)
  M₂ = Function(S₂)  (fresh)
  M₁ ≠ M₂ (unless Function is constant for S₁ and S₂)
```
A materialization may be stale (computed from an old snapshot).
Staleness is bounded by the Trigger policy:
- Eager: staleness = 0 (always fresh)
- Lazy with budget K: staleness ≤ K commits
- Background with interval T: staleness ≤ T time

### Law 4: Independence
```
Materializations do not affect the source snapshot.
```
Computing, updating, or deleting a materialization does NOT modify
the data it was derived from. (Kernel Law 1: immutability + Law 4:
references don't mutate objects.)

### Law 5: Composability
```
If M₁ = f₁(S) and M₂ = f₂(M₁), then M₂ = (f₂ ∘ f₁)(S)
```
Materializations can be derived from other materializations.
Example: a search index (M₂) derived from a materialized view (M₁)
derived from a table (S). The composition f₂ ∘ f₁ is itself a
materialization.

---

## 4. Unification: everything is a Materialization

| Current concept | Source | Function | Trigger | Storage |
|---|---|---|---|---|
| Secondary index | View snapshot | extract key → blob_hash | lazy/eager | Prolly tree |
| Materialized view | Source View(s) | transform query | manual/lazy | View (Prolly tree) |
| Statistics (min/max) | View snapshot | compute per-column stats | lazy | JSON blob |
| Bloom filter | View snapshot | hash all keys | lazy | Binary blob |
| Feature vector | Feature Store | compute features for entity | eager/lazy | JSON blob |
| Semantic aggregate | Source View | compute metric (sum/count/avg) | manual | JSON blob |
| Search index (FTS) | View snapshot | tokenize → inverted index | lazy | Prolly tree |
| Zone map | Parquet file | compute min/max per zone | eager | JSON blob |
| Histogram | View snapshot | bucket values | lazy | JSON blob |
| Vector index (HNSW) | Vector View | build ANN graph | eager | Binary blob |

**ALL of these are the same abstraction:** `materialization = f(snapshot)`.

The only differences are:
1. The Function (what to compute)
2. The Trigger (when to recompute)
3. The Storage format (how to store the result)

---

## 5. The Materialization API

```python
class Materialization:
    """A materialization: f(snapshot) → stored result."""

    def __init__(self, name: str, source_view: View,
                 function: Callable[[dict], bytes],
                 trigger: str = "lazy",  # "eager" | "lazy" | "background" | "manual"
                 staleness_budget: int = 5):
        self.name = name
        self.source = source_view
        self.function = function
        self.trigger = trigger
        self.staleness_budget = staleness_budget
        self._cached_result: Optional[bytes] = None
        self._last_built_at_commit: int = -1

    def get(self) -> bytes:
        """Get the derived structure, recomputing if stale."""
        if self._needs_rebuild():
            self._rebuild()
        return self._cached_result

    def _needs_rebuild(self) -> bool:
        if self._cached_result is None:
            return True
        if self.trigger == "manual":
            return False
        staleness = self.source._commit_count - self._last_built_at_commit
        return staleness > self.staleness_budget

    def _rebuild(self) -> None:
        """Recompute from source snapshot."""
        snapshot = self.source.base.read_all()
        self._cached_result = self.function(snapshot)
        self._last_built_at_commit = self.source._commit_count
```

---

## 6. What this unification gives us

### 6.1. One API for all materializations
Instead of separate APIs for indexes, statistics, bloom filters, etc.,
there's one API: `Materialization(name, source, function, trigger)`.

### 6.2. One set of laws
All materializations satisfy the same 5 laws (determinism, derivability,
staleness, independence, composability). No special cases.

### 6.3. One trigger policy
Eager/lazy/background/manual applies uniformly. An index can be lazy;
a bloom filter can be eager; a histogram can be manual. Same mechanism.

### 6.4. Composability
A search index can be derived from a materialized view (which is derived
from a table). The composition is itself a materialization. No special
"materialization-of-materialization" API needed.

### 6.5. GC simplification
All materializations are rebuildable from their source. GC can safely
delete any materialization — it will be rebuilt on next access. This
simplifies the GC story (Composition Law 3: GC reachability).

### 6.6. The "Materialization" admission rule
A new concept enters the Materialization layer if and only if:
1. It is a function of a snapshot (deterministic, derivable)
2. It is NOT the source data itself (not canonical)
3. It can be rebuilt from the source (lossless re-derivation)

If a concept fails any criterion, it's NOT a materialization — it's
either source data (canonical) or a Lens (has its own commit history).

---

## 7. Relationship to the Kernel

Materializations are **entirely above the kernel**. The kernel knows
nothing about them. The kernel provides:
- Write (to store derived data as blobs)
- Read (to retrieve derived data)
- Reference (to name derived data)

The Materialization layer sits between the kernel and the Lens layer:
```
Kernel (3 primitives)
  → Materialization Layer (f(snapshot) → stored result)
    → View Layer (domain-specific logic)
```

Or, more accurately, Materializations are used BY Views:
```
Kernel
  → ProllyViewBase (versioning, branching, history)
    → View (domain logic + Materializations for optimization)
```

A View uses Materializations for:
- Indexes (fast lookups)
- Statistics (query optimization)
- Bloom filters (skip unnecessary reads)
- Materialized views (precomputed results)
- Feature vectors (ML serving)

All of these are `f(snapshot)` — derived, rebuildable, non-canonical.

---

## 8. Open Questions

1. **Incremental derivation.** Can the Function be incremental?
   (e.g., "only recompute the changed entries" instead of "recompute
   from full snapshot")? The current incremental index is one
   specialization; can it be generalized?

2. **Multi-source derivation.** Can a Derived Structure depend on
   multiple sources? (e.g., a join index across two Views). If so,
   staleness tracking becomes more complex (which source changed?).

3. **Derivation cost model.** How expensive is the Function? Some
   functions are O(N) (full scan), others are O(delta) (incremental).
   The Trigger policy should consider cost, not just staleness.

4. **Derivation DAG.** If D₂ depends on D₁ depends on S, and S changes,
   what's the propagation order? This is a build-system problem
   (like Make/Bazel). Could Pond adopt a similar model?

These are research questions. The Materialization Calculus is the
starting point, not the final answer.
