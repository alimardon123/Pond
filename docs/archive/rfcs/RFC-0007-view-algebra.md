# RFC-0007: The View Algebra

## Status

**Accepted** — promoted from Draft after the property-test harness
(`pond-sdk/lens_laws.py`) was built and verified against:

1. The SDK's own `View`, `IndexedView`, and `SemanticView` classes
   (all pass all 6 laws; run via
   `python pond-sdk/run_lens_laws_ci.py`).
2. An externally-built `GraphView` constructed from `SDK_SPEC.md`
   alone (no access to pond-sdk internals; passes all 6 laws; run via
   `python validation/run_graph_lens_laws.py`).

This RFC is now the authoritative specification of what a Pond Lens
IS, mathematically. Violations are release-blocking bugs (RFC-0009
metric E1, target 0).

## Abstract

RFC-0001 asked the question: *what is a Lens, mathematically?* It
proposed `V = (State, Encode, Decode, Commit, Resolve)` as a draft
answer and listed composition as an open question.

This RFC closes that open question. We give a cleaner definition,
prove that Views compose, prove that all existing Views satisfy the
algebra, and use the algebra to settle the Semantic-adapter question
(OssieView, CubeView, dbtView are Views, not adapters-as-afterthought).

The shift from RFC-0001 is: stop conflating "what a Lens IS" with
"what a Lens DOES." `Commit` and `Resolve` are operations; picking
two of them and freezing them into the definition was arbitrary.
The algebra admits the full operation set; the definition admits
only the structural skeleton.

> **Acceptance evidence:** the `lens_laws.py` harness in
> `pond-sdk/` implements all 6 law checks as property tests. It
> passes for all 3 SDK View classes AND for an external GraphView
> built from `SDK_SPEC.md` alone (validation/graph_view_external.py,
> from Task 12 external validation). The harness is CI-runnable via
> `python pond-sdk/run_lens_laws_ci.py`.

---

## 1. Motivation

Pond has 14+ View implementations and an external validation report.
The external validator was able to build a VectorView from the SDK
spec, but reported friction: ambiguity in contracts, missing
defaults, invented conventions. The friction was not architectural
(architecture: 9/10). It was specification friction.

The root cause: we never specified what a Lens IS. The SDK
documentation describes how to *use* a Lens, not what a Lens *is*.
A formal definition would:

1. Give Lens authors a checkable contract (does my code satisfy the
   algebra?).
2. Make composition principled (when do two Views compose? what is
   the composite?).
3. Settle questions like "is a Semantic adapter a Lens?" by reducing
   them to "does it satisfy the algebra?"
4. Enable automated View testing (algebraic laws are checkable
   invariants).

---

## 2. Definition

A **View** V over a kernel K is a 5-tuple:

```
V = (Σ, A, E, D, M)
```

where:

### Σ — state space

The set of all possible view states. A view state is the complete
information the Lens needs to answer any query. It is not "in-memory
buffer state" (RFC-0001's `State` was ambiguous about this); it is
the *logical* state — the snapshot the Lens presents to its users.

For versioned Views, Σ includes the commit DAG: a state is a node
in the DAG, plus the snapshot it carries. `Σ = CommitDAG × Snapshot`.

For non-versioned Views (rare), Σ is just the snapshot.

```
Σ = View-specific set
```

### A — algebra

A signature of operations on Σ. Each operation `σ ∈ A` is a (partial)
function:

```
σ : Σ × X_σ → Σ
```

where `X_σ` is the operation's argument type (e.g., for `put`,
`X_put = (key, value)`; for `commit`, `X_commit = message`).

The algebra includes both mutators (`put`, `delete`, `commit`,
`branch`, `merge`) and accessors (`get`, `history`, `diff`). Mutators
produce new states; accessors are pure functions of state.

Critically, **`A` is not fixed by the algebra**. Different Views have
different algebras. SQLView has `CREATE_TABLE`, `INSERT`, `SELECT`;
VectorView has `INSERT`, `SEARCH`, `DELETE`. The algebra is what
makes a Lens a Lens-of-something-specific.

### E : Σ → Blob — encode

Maps a view state to a kernel blob (bytes). Pure function. The View
chooses the format (Parquet, Arrow IPC, struct-packed floats, JSON,
etc.).

```
E : Σ → Blob
```

### D : Blob → Σ — decode

Inverse of E. Maps a kernel blob back to a view state. Pure function.

```
D : Blob → Σ
```

### M — materialization set

A set of materialization functions. Each `m ∈ M` is a pure function
from a view state to a materialized state:

```
m : Σ → Σ_m
```

Materializations are derived structures: indexes, statistics, bloom
filters, feature vectors, semantic aggregates, search indexes, zone
maps, caches. They are *deterministic functions of state* — given
the same view state, every materialization produces the same output.
(RFC-0005 calls these "Derived Structures"; this RFC adopts the
database-literature term "Materialization" — see RFC-0005 update.)

Materializations may be lazy (computed on demand), eager (computed
on commit), or incremental (delta-computed from a previous
materialization). The trigger policy is part of `m`'s implementation,
not part of the algebra.

---

## 3. Laws

A 5-tuple `(Σ, A, E, D, M)` is a Lens iff it satisfies the following
six laws.

### Law 1: Round-trip

`D(E(s)) = s` for all `s ∈ Σ`.

Encoding is lossless. The View can persist any state and recover it
exactly. This is the contract that makes the kernel trustworthy as
a Lens backend.

### Law 2: Purity of operations

Every `σ ∈ A` is a pure (partial) function on Σ. Same state + same
arguments → same resulting state. No hidden state, no side effects
on the kernel other than through E.

(Note: "pure" here means *semantically* pure. The implementation
may use kernel mutation — `kernel.Reference` — but the observable
state-transition function is pure.)

### Law 3: Encoding preservation

For every `σ ∈ A` and `s ∈ Σ` where `σ(s, x)` is defined:

```
E(σ(s, x)) is well-defined
```

I.e., every reachable state is persistable. No operation produces
a state that cannot be encoded. (This rules out Views with
"in-memory-only" states that can never be committed.)

### Law 4: Materialization determinism

Every `m ∈ M` is a pure function of state. For all `s ∈ Σ`:

```
m(s) is uniquely determined by s
```

Materializations are *derived*, not authoritative. They can always
be rebuilt from the view state. This is what makes them safely
cacheable, incrementally maintainable, and discardable.

### Law 5: Composition

If `V1 = (Σ1, A1, E1, D1, M1)` and `V2 = (Σ2, A2, E2, D2, M2)` are
Views over the same kernel, then their composite

```
V1 ⊕ V2 = (Σ1 × Σ2, A1 ⊕ A2, E1 ⊕ E2, D1 ⊕ D2, M1 ⊕ M2)
```

is also a Lens, where:

- `A1 ⊕ A2` is the disjoint union of operations (an operation in V1
  acts on the first component; an operation in V2 acts on the second).
- `E1 ⊕ E2 : Σ1 × Σ2 → Blob` is the paired encoding: encode both
  states, write them as a length-prefixed pair `(E1(s1), E2(s2))`.
- `D1 ⊕ D2 : Blob → Σ1 × Σ2` is the paired decoding.
- `M1 ⊕ M2` is the disjoint union of materialization functions.

This law is what makes layered architecture work: a higher-layer
View (e.g., FeatureStoreView) is a composite of lower-layer Views
(e.g., IndexedView ⊕ ProllyViewBase), and the composite is itself
a Lens satisfying the same algebra.

### Law 6: Kernel independence

For any view state `s ∈ Σ`, `E(s)` is a finite byte string. The
kernel can store and retrieve it without any knowledge of the Lens's
structure. The kernel never needs to inspect blob contents to
satisfy its own laws.

This is the downward-only-dependency rule, formalized. The View
depends on the kernel; the kernel never depends on the Lens.

---

## 4. Theorem: all existing Views satisfy the algebra

We verify the six laws against the eight reference Views.

| View | Σ | A (sample) | E | D | M | Laws 1–6 |
|---|---|---|---|---|---|---|
| SQLView | `(schema, table_data, commit_dag)` | `CREATE_TABLE, INSERT, SELECT, UPDATE, DELETE, ALTER, COMMIT, BRANCH, MERGE` | Arrow→Parquet | Parquet→Arrow | `{secondary_indexes, statistics, zone_maps}` | ✓ |
| VectorView | `(dim, records, commit_dag)` | `INSERT, SEARCH, DELETE, GET` | struct-pack floats | struct-unpack | `{hnsw_index, by_id_index}` | ✓ |
| StreamView | `(topic, partitions, offsets, records, commit_dag)` | `PRODUCE, CONSUME, COMMIT_OFFSET` | length-prefix frames | parse frames | `{consumer_state, retention_watermark}` | ✓ |
| GitView | `(staged_files, tree, commit_dag)` | `INIT, ADD, COMMIT, BRANCH, CHECKOUT, MERGE, DIFF` | raw bytes / tree-encoding | raw bytes / tree-decoding | `{}` (Git has no derived structures by default) | ✓ |
| NotebookView | `(pages, attachments, commit_dag)` | `CREATE_PAGE, EDIT, SEARCH, ATTACH` | JSON + raw bytes | JSON parse | `{search_index, page_metadata}` | ✓ |
| FeatureStoreView | `(features, entities, snapshots, commit_dag)` | `DEFINE_FEATURE, GET_ONLINE, GET_OFFLINE, POINT_IN_TIME` | Parquet + JSON meta | Parquet→Arrow + JSON parse | `{feature_vectors, point_in_time_index, freshness_monitor}` | ✓ |
| SemanticView | `(model, mappings, commit_dag)` | `DEFINE_ENTITY, DEFINE_MEASURE, QUERY, TRANSLATE` | JSON (OssieAdapter format) | JSON parse | `{semantic_cache, query_plan}` | ✓ |
| GraphView | `(nodes, edges, adjacency, commit_dag)` | `ADD_NODE, ADD_EDGE, NEIGHBORS, TRAVERSE` | JSON | JSON parse | `{adjacency_index, reachability_index}` | ✓ |

**Law 1 (round-trip):** Each View's encode/decode pair is verified by
round-trip tests in its test suite.

**Law 2 (purity):** Each View's operations are deterministic functions
of state. Verified by replay tests: same operation sequence on same
initial state produces same final state.

**Law 3 (encoding preservation):** Every operation produces a state
that the Lens can encode. (If it couldn't, the Lens couldn't commit
— and every View's commit operation works.)

**Law 4 (materialization determinism):** All materializations are
rebuilt from state. Verified by delete-and-rebuild tests: drop a
materialization, rebuild, verify identical output.

**Law 5 (composition):** Verified structurally for the known composites:
- `IndexedView ⊕ ProllyViewBase` = the standard Layer 2 base
- `FeatureStoreView ⊕ IndexedView ⊕ ProllyViewBase` = the Layer 3 flagship
- `SemanticView ⊕ FeatureStoreView` = the Semantic adapter stack

**Law 6 (kernel independence):** Verified by backend substitution.
The same Lens code runs unchanged on 6 backends (FS, memory, SQLite,
Redis, S3, FDB). The kernel never inspects blob contents.

---

## 5. Theorem: Views compose (the open question from RFC-0001)

RFC-0001 asked: *can Views compose? Is there an algebra of View
composition?*

**Answer: yes.** Law 5 above defines `⊕` (parallel composition).
We also define `∘` (sequential composition):

### Sequential composition: V1 ∘ V2

If V1's state space can be *interpreted* as V2's state space — i.e.,
there exist pure functions `f : Σ1 → Σ2` and `g : Σ2 → Σ1` with
`f ∘ g = id_Σ2` — then V1 ∘ V2 is a Lens with state space Σ2 and
algebra `A2` lifted through `g`.

Concretely: V1 ∘ V2 is "V2, but stored using V1's encoding." This is
how a `LakeFSView` can be a GitView-with-extra-metadata, or how an
`OCIView` can be a GitView-with-manifest-format.

### Why this matters

This settles the Semantic-adapter question (Section 6) and the
Layered Architecture (RFC-0006) on a single formal foundation.
Each layer is a Lens; each layer composes with the layer below via
`⊕` or `∘`. There is no special-case "adapter" concept — adapters
are Views, and Views compose.

---

## 6. Application: Semantic adapters ARE Views

RFC-0001's draft definition left Semantic adapters in an awkward
spot: they didn't fit cleanly into `(State, Encode, Decode, Commit,
Resolve)` because their `Encode`/`Decode` depend on an external
format (Ossie, Cube, dbt).

The 5-tuple algebra settles this cleanly. A Semantic adapter IS a
View; its `E`/`D` pair is just the adapter's serialization format.

| View | Σ | E / D format |
|---|---|---|
| `OssieView` | `(entities, measures, mappings, commit_dag)` | Ossie YAML/JSON |
| `CubeView` | `(cubes, dimensions, measures, commit_dag)` | Cube.js schema |
| `DbtView` | `(models, sources, tests, commit_dag)` | dbt `schema.yml` + compiled SQL |

All three satisfy the six laws. All three compose with the kernel
through `E`/`D`. All three can be layered over `FeatureStoreView`
through `⊕` to give "semantic layer over feature store."

**Architectural consequence:** the `pond-semantic` package is not
an adapter layer; it is a Lens family. The kernel does not know
Semantic exists. The SDK does not need a special Semantic API. The
algebra admits all three uniformly.

---

## 7. Application: View equivalence and interop

Two Views `V1` and `V2` are **format-equivalent** iff they have the
same `E`/`D` pair. Format-equivalent Views can read each other's
kernel blobs:

```
D1 = D2  ⟹  V1 can read V2's persisted state
```

Two Views are **state-equivalent** iff they have the same `Σ`. Two
Views are **algebra-equivalent** iff they have the same `Σ` and `A`.
Algebra-equivalent Views are interchangeable for users.

This gives us a formal foundation for `LENS_INTEROP_SPEC.md` (which
previously used informal notions of "compatible Views").

---

## 8. Application: automated View verification

Each law is checkable by a property test:

| Law | Property test |
|---|---|
| 1. Round-trip | `assert D(E(s)) == s` for generated `s` |
| 2. Purity | `assert σ(s, x) == σ(s, x)` (deterministic replay) |
| 3. Encoding preservation | `assert E(σ(s, x)) is not None` for all reachable `σ(s, x)` |
| 4. Materialization determinism | `assert m(s) == m(s)` and `assert rebuild(m(s)) == m(s)` after drop |
| 5. Composition | `assert (V1 ⊕ V2) satisfies laws 1–4` |
| 6. Kernel independence | `assert kernel_backend_substitution(V) works` |

A `lens_laws.py` test harness can verify any View implementation
against the algebra. This is the SDK polish work proposed for Phase B
of the roadmap.

---

## 9. What is deliberately NOT in the algebra

### Policies
The user's review proposed `View = State + Translation + Derived
Structures + Policies`. We omit policies from the algebra because
policies (retention, GC, access control, replication) constrain
*which transitions are permitted* — they do not change *what a Lens
is*. A SQLView with a 30-day retention policy and a SQLView with a
7-year retention policy are the same View with different policies.

Policies are a Layer 4 concern (Policy Calculus), to be addressed
in a future RFC. The kernel stayed clean by deferring concerns; the
View definition does the same.

### History
History is not a separate component — it is *part of Σ*. A versioned
View's state space includes the commit DAG. The algebra does not
need a separate "history" axis because history is just the structure
of `Σ` for versioned Views.

### Concurrency
The algebra is sequential. Concurrency (locking, MVCC, isolation
levels) is a Layer 4 concern, not a Lens-definition concern. The
algebra describes what a Lens IS; concurrency describes how
multiple actors interact over a Lens.

---

## 10. Open questions

1. **Is the 5-tuple minimal?** Could `M` be derived from `A` (every
   operation can be backed by a materialization)? Probably not —
   materializations are pure functions of state, operations are
   state transitions; they are different kinds of things.

2. **Is the 5-tuple complete?** Are there View properties not
   captured by `(Σ, A, E, D, M)`? Candidates: schema evolution,
   migration, observability. These may be Layer 4 concerns or may
   require extending the algebra.

3. **What is the *initial* View?** In category-theoretic terms, is
   there an identity View `I` such that `V ⊕ I = V` for all V?
   Candidate: the trivial View with `Σ = Blob`, `A = {id}`, `E = id`,
   `D = id`, `M = {}`. This would make `⊕` a monoidal operation.

4. **Is there a dual to materialization?** Materialization goes
   `Σ → Σ_m` (state to derived state). Is there a useful
   `Σ_m → Σ` direction (derived state back to state)? This would
   model *computed views* in the SQL sense.

5. **Does the algebra admit a notion of View *quotient*?** I.e.,
   can we define `V / ~` for some equivalence `~` on `Σ`? This
   would model "the same View at a coarser granularity" — useful
   for aggregation Views.

These are research questions. The 5-tuple algebra is a foundation,
not a final answer. But unlike RFC-0001's draft, this foundation
is checkable, composable, and admits the existing 8 Views without
special cases.

---

## 11. Relationship to other RFCs

- **Supersedes:** RFC-0001 §2 (the draft `V = (State, Encode, Decode,
  Commit, Resolve)` definition). RFC-0001 §3–7 (laws, verification,
  composition) are subsumed.
- **Depends on:** RFC-0003 (Kernel Specification — the kernel
  primitives that `E`/`D` operate over).
- **Used by:** RFC-0004 (View Composition — now formalized as
  Law 5).
- **Refines:** RFC-0005 (Derived Structures — now `M`, the
  materialization set; renamed per database literature).
- **Settles:** RFC-0006 (Layered Architecture — each layer is a Lens
  composite per Law 5).

---

## 12. Status of this RFC

This RFC is **Accepted**. The six laws have been verified against
the eight reference Views by inspection AND against the SDK's three
View classes (View, IndexedView, SemanticView) plus an externally-
built GraphView via the automated `lens_laws.py` property-test
harness. The harness is CI-runnable
(`python pond-sdk/run_lens_laws_ci.py`) and is now metric E1
(RFC-0009): a hard constraint with target 0 violations.

Future work: extend the harness to cover all Layer 3 domain Views
(SQLView, StreamingView, GitView, NotebookView, FeatureStoreView)
once each has a `ViewContract` adapter. The harness is View-agnostic;
adding a new Lens requires only writing its contract.
