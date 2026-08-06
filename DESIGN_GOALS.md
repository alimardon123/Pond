# Pond — Design Goals and Project Context

> **Read this first.** This document is the canonical entry point for
> any agent (human or AI, local or remote) joining the Pond project.
> It captures *what we are building*, *why*, and *how to navigate the
> repository*. If you read only one file before working on Pond, read
> this one.

---

## 1. What Pond is

Pond is a **capability-oriented immutable object runtime**. The core
hypothesis: a tiny storage kernel — **six substrates, three
operations** — is sufficient for radically different workloads
(SQL, vectors, streaming, Git, graphs, ML, time-series, OCI
registries, semantic layers) to be implemented as independent
**Lenses** over a shared immutable substrate.

> **Honesty note (post-Phase P, with overclaim correction):** The
> kernel was previously described as "3 primitives." The Second and
> Third Red Team Reviews (`POND_SECOND_RED_TEAM.md`,
> `POND_THIRD_RED_TEAM.md`) showed that this claim was *rhetorical*:
> the model silently depended on Time, Coordination, Range-Read, and
> Key substrates without naming them. The honest count is **six
> substrates, three operations** (see `POND_FORMAL_ALGEBRAS.md` Parts
> II + III + IV). The user-facing API is `Write`, `Read`, `Ref`.
>
> **What has been established (Phase K-P):**
> - The implementation matches the model (683 checks pass).
> - 6 TLA+ invariants hold across 56 reachable states (small finite
>   model — proves consistency, not architectural correctness).
> - Tested behaviors match Git, Dolt, and Iceberg for the specific
>   invariants tested (content-addressing, commit topology, time
>   travel, manifest rebuildability).
> - 4 production-quality packages implement the model's algebras.
>
> **What has NOT been established:**
> - That the architecture is *correct* (TLA+ proves consistency, not
>   correctness).
> - That the architecture is *necessary* (no proof that fewer
>   substrates wouldn't suffice).
> - That the architecture is *competitive* (no benchmarks vs. peer
>   systems yet).
> - That the architecture is *adoptable* (no production use, no
>   external expert review).
> - That the Lens algebra *covers real workloads* (no flagship app
>   yet).
>
> Phase Q (validation) addresses these gaps. The architecture is
> frozen; the validation begins.

#### 1.1 Known gaps (post-veteran-architect review, Task 65)

The veteran architect review (`docs/VETERAN_ARCHITECT_REVIEW.md`)
identified several gaps that the model-vs-implementation narrative
did not previously surface. These are now part of the honesty record.
Where the gap is a doc-vs-code drift that has been fixed in this
round, the entry says so. Where the gap is real (code behavior the
docs overclaim), the entry stays open until the code is fixed.

- **FeatureStoreLens in `pond-labs/` needs migration from
  `ProllyLensBase` to `UnifiedStorage`.** Its self-test
  (`tests/test_all.py::test_feature_store_lens`) currently skips
  with a "not yet migrated" marker. The docs previously implied it
  was shipping; reality is it's stuck on the legacy Prolly path.
  Status: **open** — needs migration work.
- **`StreamingLens` time-travel via `commit_hash` is NOT implemented
  in the unified path.** `read_stream(collection, start_byte, end_byte,
  commit_hash=None)` accepts `commit_hash` for backward-compat in the
  signature, but the unified-storage path always reads from HEAD.
  Use `replay_from(offset)` for offset-based time-travel on
  partitioned topics. Status: **open** — needs a HEAD-pointer walk in
  `UnifiedStorage.read`.
- **IVF vector index does NOT reduce I/O.**
  `pond-sdk/extensions/indexing/ivf_index.py:363-381` admits: the
  implementation reads ALL vectors via `storage.read(collection)`
  then filters by target_ids in Python. Every search reads the
  entire collection. At PB scale (10M+ vectors) this defeats the
  purpose of IVF. The IVF *format* is correct (centroids + cluster
  assignments); the *integration* with `UnifiedStorage` is not.
  Status: **open** — needs per-cluster blob fetching.
- **`LakehouseLens` and `OLTPLens` now extend `PondLens`** (fixed in
  Tier 0 — Task 66). Both previously declared no base class; both now
  inherit `branch`/`list_collections`/`set_definition`/`get_definition`/
  `history` from `PondLens` for free. `KeylessLens(KeyValueLens)` is
  a documented exception (legitimate variant, same file, auto-generates
  UUIDv7 keys).
- **"ACID transactions" are atomic publication only.** The
  `commit_tx` path provides atomic publication across collections
  (all-or-nothing HEAD pointer moves via a transaction marker) — but
  there is NO isolation (readers can see tentative shards before
  commit), NO rollback (a failed transaction leaves orphan shards
  for GC to clean up), and NO serializability. Calling this "ACID"
  is overclaim; the honest term is "atomic publication" or
  "multi-collection commit." See `scripts/test_acid.py` for what is
  actually tested (atomicity + abort + snapshot isolation in the
  narrow reader sense; not the database ACID sense).

These gaps use the §6 outcome vocabulary: they are **Supported** by
source-code evidence and the veteran's review. They are not yet
**Falsified** by a fix — that requires code changes, which are out
of scope for the docs-only Task 65.

Tagline: *one copy of data on object storage, serving all workloads
without duplication, with no JVM, no Spark, no Iceberg-style
metadata explosion.*

Pond is **not** a lakehouse, not a table format, not a Spark
replacement, not a query engine. It is the *storage substrate*
underneath those things — and underneath things that don't exist yet.

---

## 2. Main goal

> **Find the smallest storage algebra from which all workload
> semantics can be composed, and prove that composition is sound.**

This is a research goal stated as an engineering goal. "Smallest"
is measured by **substrate count (currently 6, honest)** and by
lines of code in `pond-core` (currently 274 LOC in `kernel.py` +
~280 LOC in `object_store_native_kernel.py` + 443 LOC in
`local_fs_object_store.py` + 519 LOC in `s3_object_store.py` +
112 LOC in `make_kernel.py` — NOT ~140 LOC as previously claimed).
"All workload semantics" is measured by the number of distinct
Lenses implemented (currently 8+). "Composition is sound" is
measured by formal laws (TLA+ checked 6 invariants across 56
reachable states in a finite model — establishes consistency, not
correctness), 683 passing tests (verify implementation matches
model, including 61 differential tests against real Git/Dolt/Iceberg
for specific invariants), and 4 engineering packages that implement
the algebras. **None of this is external validation.** Phase Q
(benchmarks, whitepaper, flagship, expert review) is where
soundness gets tested against reality.

The goal is **not** to build a product. The goal is to discover
whether a small-substrate kernel is the right abstraction, and to
**falsify** that hypothesis with evidence external to the model
itself. If the hypothesis survives falsification, the product
follows. If it doesn't, no amount of internal consistency will
save it.

> **Post-Phase P correction (overclaim retracted):** earlier
> versions of this document claimed "the answer is yes" and "the
> research is done." That was overclaim. What Phase K-P actually
> established is **internal consistency**: the implementation
> matches the model, invariants hold, and tested behaviors match
> peer systems. None of this proves the architecture is *right*.
> Phase Q (validation via benchmarks, whitepaper, flagship, and
> external review) is where the architecture gets tested against
> reality. The honest metric remains **substrate count (6) +
> operation count (3)**, but the verdict on whether that's the
> right abstraction is **not yet in**.

---

## 3. Design goals (the seven principles)

Every architectural decision in Pond must serve these seven principles.
When a proposal conflicts with one of them, the proposal loses.
When a proposal serves one at the cost of another, the tradeoff is
made explicit and recorded in `rfcs/` or `docs/REJECTED_DESIGNS.md`.

The seven principles, in priority order:
1. **Simple** — the kernel stays intellectually small.
2. **Powerful** — rich behavior emerges from composition.
3. **Performant** — optimizations live above the core.
4. **Scalable** — Lenses and Physical Structures evolve independently.
5. **Efficient** — immutable data + rebuildable derived metadata.
6. **Beautiful** — one responsibility per layer; dependencies flow downward.
7. **Functional** — Pond must do everything users actually need.
8. **Storage-Independent** — stored bytes never depend on the execution engine.

### 3.1 Simple — the kernel remains intellectually small

The kernel is 6 substrates + 3 operations + same-collection batch
I/O helpers. `pond-core/kernel.py` is currently 274 LOC (was ~140;
grew after the thread-safety round added `write_batch` and
`read_blob_batch`). The object-store-native variant
(`pond-core/object_store_native_kernel.py`) adds ~280 LOC, plus
storage backends (`local_fs_object_store.py` 443 LOC,
`s3_object_store.py` 519 LOC) and the `make_kernel.py` factory (112
LOC). The kernel is NOT "FROZEN" in the implementation sense — it
has gained methods. The honest description is:

> **6 substrates, 3 operations (`write`, `read`, `reference`) +
> batch I/O helpers (`write_batch`, `read_blob`, `read_blob_batch`)
> + ref-namespace helpers (`resolve`, `list_names`).**

The batch helpers are **same-collection performance primitives** —
they let a Lens issue parallel blob PUTs/GETs in one round-trip.
They are NOT cross-collection atomicity (that lives in
`services/replication/replication_coordinator.py` per axiom A7).
Rich behavior must *emerge* from composition, not from kernel
features. The honest substrate count is 6 (Bytes, Names, Time,
Coordination, Range-Read, Key) — see the honesty note in §1.

The kernel must stay small enough that a single engineer can hold the
entire kernel in their head. If a proposed change makes the kernel too
large to hold in one's head, it is rejected — even if it adds useful
capability. The thread-safety round added batch helpers without
breaking this property — they are 3-5 line wrappers around `write`
/ `read` that batch the underlying calls.

**Test:** Can you describe the kernel in one sentence? ("Three
primitives: write bytes, read bytes, mutate a name→hash mapping;
plus optional batch wrappers for same-collection performance.")
If you need a second sentence beyond the batch-wrapper caveat, the
kernel has grown too complex.

### 3.2 Powerful — rich behavior emerges from composition

The kernel does as little as possible. Everything else — Trees,
Commits, Branches, Tags, OPEN/SEALED, indexes, materializations,
schedules, schemas — is a **Lens-level pattern** built by composing
the 3 primitives. Rich behavior must *emerge* from composition, not
be *added* as kernel features.

**Test:** Can the proposed capability be expressed as data + a Lens?
If yes, it does not belong in the kernel. (See RFC-0008 for the
deletion case study: deletion is data, not a fourth primitive.)

### 3.3 Performant — optimizations live above the core

The kernel does not optimize. It does not cache, does not prefetch,
does not batch, does not compress, does not index. Views and the
SDK perform all optimization. The kernel's performance
responsibility is limited to: not being pathologically slow, and
not preventing Views from being fast.

**Test:** Is the proposed optimization measurable at the kernel
level, or does it only show up in a specific View? If the latter,
it lives in the Lens, not the kernel.

### 3.4 Scalable — derived structures, adapters, and domain packages
evolve independently

Views are independent. Adding a new Lens (e.g., `VectorView`,
`SemanticView`, `OssieView`) must not require modifying any existing
View or the kernel. Removing a Lens must not break any lower layer.

**Test (the removability test):** If package X is deleted entirely,
does any lower-layer package break? If yes, the dependency is wrong.

### 3.5 Efficient — immutable data plus rebuildable derived metadata
avoids unnecessary duplication

Pond stores one copy of each piece of data (content-addressed
deduplication is free). All auxiliary structures (indexes,
materializations, statistics) are *derived* from snapshots and
rebuildable on demand. No auxiliary structure is authoritative;
all are functions of state.

**Test:** Could the proposed structure be rebuilt from a snapshot?
If yes, it is a materialization (RFC-0005 / RFC-0007 §2 `M`), not
a kernel or View state.

### 3.6 Beautiful — each layer has one clear responsibility, and
dependencies flow in only one direction

The architecture is layered (RFC-0006). Each layer adds exactly one
capability. Dependencies flow downward only: Layer N may depend on
Layer N-1, never on Layer N+1, never on a sibling at Layer N. The
result is an architecture where each layer can be reasoned about
independently.

**Test:** Draw the dependency graph of packages. Is it a DAG with
all edges pointing downward? If not, the architecture has leaked.

### 3.7 Functional — Pond must do everything users actually need

Simplicity without functionality is academic. Pond's small kernel
is a means, not an end. The end is **covering every real workload**
that users bring to the system — through Lenses (which interpret
bytes) and Physical Structures (which accelerate access).

The functional principle is the most demanding. It says: when a
user wants SQL, give them SQL (via a SQL Lens). When they want
feature stores, give them feature stores (via a Feature Store Lens).
When they want graph queries, give them graph queries (via a Graph
Lens). When they want OLTP, give them OLTP (via an OLTP Lens —
which may layer a coordinator on the kernel per A7). The kernel
stays small; the Lenses do the work.

When a workload seems impossible on Pond, the functional principle
demands we ask: **what Lens is missing?** Not "Pond can't do this."
The answer is almost always a missing Lens or a missing Physical
Structure, not a missing kernel primitive.

**Test:** Before claiming "Pond can't do X," ask:
1. Is there a Lens that could interpret Pond bytes as X?
2. Is there a Physical Structure that could accelerate X?
3. Is there a coordinator that could layer on the kernel for X's
   consistency needs (per A7)?

If the answer to all three is no, **then** Pond genuinely can't do
X. If the answer to any is "yes, but it doesn't exist yet," the
right statement is "Pond can do X via a Lens that hasn't been
built yet" — not "Pond can't do X."

This principle keeps Pond honest about its scope while preventing
premature defeatism. The kernel is small; the architecture is
extensible through additional Lenses. Most "can't" claims are
missing Lenses.

### 3.8 Storage-Independent — stored bytes never depend on the execution engine

**Storage Independence Law:** The stored bytes never depend on the
execution engine. Spark, DuckDB, Polars, Ray, DataFusion, Flink —
all observe the same storage. Storage survives execution engines.
Execution engines become replaceable.

This is the strongest architectural idea in Pond: storage semantics
are independent from execution. The kernel stores immutable bytes;
Lenses interpret them; execution engines query through Lenses. No
execution engine "owns" storage. Switching from DuckDB to Spark to
Polars is changing the query engine, not the storage.

```
Execution Layer (Spark, Flink, DuckDB, Polars, Ray, SQL)
    ↓ (observes, never owns)
Lens Layer (Lakehouse, Git, Feature Store, Vector, Search, Streaming)
    ↓ (interprets, never owns)
Kernel (Write, Read, Ref — immutable bytes + refs)
    ↓
Backend (local disk, S3, IPFS, FDB)
```

**Test:** Can you switch execution engines without rewriting,
converting, or regenerating any stored data? If yes, storage is
independent. If no, an execution engine has leaked into storage.

This principle is the foundation for the LTAP (Long-Term Architecture
Plan): a lightweight Databricks/Spark/Flink alternative where the
storage layer is frozen and execution engines are pluggable. But
the execution engine is NOT built yet — the storage model must be
proven first.

---

## 4. The weekly question

From the architecture review that produced this document:

> Every week, ask: **If I deleted everything except `pond-core` and
> `pond-sdk`, would the architecture still make sense?**
>
> If the answer is yes, you're preserving the design principles you
> started with.

This is the single most important question for any agent working on
Pond. If a proposed change makes the answer "no" — if the architecture
only makes sense with `pond-feature-store` or `pond-semantic` present
— the change has leaked.

---

## 5. Repository map — start here

### 5.1 Top-level documents (read these first)

| File | Purpose | Read it if… |
|---|---|---|
| `README.md` | Project overview, hypothesis, status | You are new to Pond |
| `DESIGN_GOALS.md` (this file) | Six design principles + repo map | You are starting any work on Pond |
| `REPO_ORGANIZATION.md` | Folder rules, naming conventions, promotion process, no lens-to-lens inheritance | You are adding files, moving code, or promoting from pond-labs |
| `SDK_SPEC.md` | Authoritative SDK contract (settles all 10 validation ambiguities) | You are building a Lens or modifying the SDK |
| `PACKAGES.md` | Package boundaries and removability discipline | You are adding or modifying a package |
| `worklog.md` | Append-only research log | You need to know what previous agents did |

### 5.2 RFCs (`rfcs/`) — authoritative specifications

RFCs are stable architectural documents. Once Accepted, an RFC is
the authoritative source for its topic. Draft RFCs propose; Accepted
RFCs decide.

| RFC | Title | Status | What it specifies |
|---|---|---|---|
| RFC-0001 | What Is a Lens? | Draft (superseded by RFC-0007) | The original draft definition of a Lens |
| RFC-0002 | Elegance Metrics | Draft | How to measure architectural elegance |
| RFC-0003 | Kernel Specification | **Accepted (FROZEN)** | The 3 primitives, 5 storage laws, 7 composition laws. *(Note: "FROZEN" here means the RFC text is frozen as the spec; the kernel **implementation** is NOT frozen — see §3.1 and §1.1 Known Gaps for `write_batch`/`read_blob_batch`.)* |
| RFC-0004 | View Composition | Draft | How Views compose (parallel and sequential) |
| RFC-0005 | Derived Structures → Materialization | Draft (being renamed) | All auxiliary structures are `f(snapshot)` |
| RFC-0006 | Layered Architecture | Draft | Layer 0–3, each adds one capability |
| RFC-0007 | View Algebra | Draft | Formal 5-tuple `V = (Σ, A, E, D, M)` + 6 laws |
| RFC-0008 | Deletion as Data | Draft | Tombstones; why no fourth primitive |
| RFC-0009 | Architecture Metrics | Draft | Measurable design metrics (not microbenchmarks) |

**RFC process:** Draft → Accepted → (Superseded or Rejected). Kernel
changes require a new RFC that disproves the lower-bound proof in
`FORMAL_ALGEBRA.md`. Until such an RFC is Accepted, the kernel stays
frozen.

### 5.3 Reference documents (`docs/`)

| Document | Purpose |
|---|---|
| `docs/UNIFIED_STORAGE_DESIGN.md` | ONE format (PND2), ONE write/read path — current architecture |
| `docs/COLLECTION_MANIFEST_DESIGN.md` | ONE index blob per commit with inline stats |
| `docs/ROUND_TRIP_AUDIT.md` | Honest cold-read round-trip accounting |
| `docs/BINARY_ENCODING_FORMAT.md` | PND1 column encoding spec (used inside PND2) |
| `docs/LENS_GUIDE.md` | How to write a Lens |
| `docs/NON_GOALS.md` | What Pond doesn't do |
| `docs/POND_WHITEPAPER.md` | The 20-page contribution |
| `docs/WHERE_POND_FAILS.md` | Honest scope + Lens roadmap |
| `docs/POND_FORMAL_ALGEBRAS.md` | 17 algebras, 10 axioms |
| `docs/archive/` | Historical docs (Phase L-Q reports, Red Team reviews, RFCs) |

### 5.4 Code (`pond-core/`, `pond-sdk/`, `lenses/`)

| Package | Layer | LOC | Responsibility |
|---|---|---|---|
| `pond-core` | 0 | ~1630 | The kernel + storage backends: `kernel.py` (PondMinimal, 274 LOC; NOT FROZEN — has `write_batch`/`read_blob_batch` same-collection helpers) + `object_store_native_kernel.py` (ObjectStoreNativeKernel, no SQLite, ~280 LOC) + `local_fs_object_store.py` (443 LOC) + `s3_object_store.py` (519 LOC) + `s3_mock_backend.py` (latency-injecting mock) + `make_kernel.py` (112 LOC unified factory). |
| `pond-sdk` | 1–2 | ~3000 | `base_lens.py` (PondLens shared namespace), `pond_storage.py` (PondStorage — the unified SDK class), `pond_config.py`, `maintenance.py`, `uuid7.py`, `hlc.py`, `row_query.py`, extensions (`physical_structures/`: `unified_storage.py` THE universal storage backend, `collection_manifest.py`, `stats_tree.py`, `encoding.py`, `compression.py`, `column_source.py`, `embedded_stats.py`, `pond_pack.py`; `indexing/`: `collection_index.py`, `ivf_index.py`, `hnsw_index.py`; `maintenance/vacuum.py`; `semantic/`). The legacy `prolly_tree.py`/`binary_encoding.py`/`collection_metadata.py` referenced in older docs are in `archive/legacy-sdk/` and `archive/legacy-extensions/`. |
| `lenses/lakehouse` | 3 | ~2200 | LakehouseLens (Parquet + DuckDB + SQL pushdown). Flagship tabular lens. Extends `PondLens` directly. |
| `lenses/keyvalue` | 3 | ~760 | KeyValueLens (UnifiedStorage-backed; extends `PondLens` directly). |
| `lenses/vector` | 3 | ~530 | VectorLens (k-NN search; UnifiedStorage-backed; extends `PondLens` directly). IVF index exists but does NOT yet reduce I/O (see §1.1 Known Gaps). HNSW index exists. |
| `lenses/streaming` | 3 | ~400 | StreamingLens (chunked segments + range reads; extends `PondLens` directly). Kafka-like features (topics, partitions, consumer groups). `commit_hash` time-travel is NOT implemented in the unified path (see §1.1 Known Gaps). |
| `lenses/oltp` | 3 | ~184 | OLTPLens (in-memory memtable + batch flush to CRDT shards; extends `PondLens` directly). |
| `services/` | 3 | ~500 | Schema registry, replication coordinator, transport (production) |
| `pond-labs/` | 3 | ~3000 | Demos, benchmarks, tracks (validation suite) |

**Note:** Old packages `pond-sql`, `pond-git`, `pond-notebook`, `pond-feature-store`,
`pond-semantic`, `pond-arrow` are in `archive/`. The old `pond-vector` is now
`lenses/vector/`. File names changed: `lakehouse.py` → `lakehouse_lens.py`,
`vector_view.py` → `vector_lens.py`, `keyvalue_lens.py` moved to `lenses/keyvalue/`.

**Removability rule:** Every package above must be removable without
changing any lower layer. If removing `pond-feature-store` requires
changing `pond-sdk`, something is wrong.

### 5.5 Engineering (`engineering/`) — production hardening

| File | Purpose |
|---|---|
| `01_concurrency.py` | Thread-safety for the root namespace (Finding 7 fix) |
| `02_gc.py` | `PondGC` — Lens-level reachability walk + sweep (Finding 6 fix) |
| `03_s3_backend.py` | S3 backend adapter for the kernel |

### 5.6 Validation (`validation/`)

| File | Purpose |
|---|---|
| `vector_challenge_prompt.md` | The exact prompt given to an external agent |
| `vector_report.md` | The external agent's report — DX score 5/10, 10 ambiguities found |

This is the most valuable artifact in the repository. It measures
*communication quality*, not implementation quality. Read it before
proposing SDK changes.

### 5.7 Prototype and destruction (`prototype/`, `destruction/`)

`prototype/` contains early experimental code that informed the
current architecture. `destruction/` contains adversarial test
suites designed to falsify kernel claims. Both are historical
record; do not extend them. New work goes in `pond-*` packages or
`engineering/`.

---

## 6. The outcome vocabulary (mandatory)

To avoid confirmation bias, every experiment result and every RFC
claim uses this strict vocabulary. No "this proves" or "strongest
evidence." Just:

- **Supported** — evidence increased confidence in a hypothesis
- **Falsified** — hypothesis failed
- **Inconclusive** — experiment didn't isolate the question
- **Needs larger-scale validation** — prototype limits prevent a conclusion

This vocabulary applies to RFCs, worklog entries, validation
reports, and any commit message that claims a result.

---

## 7. The eight design goals as a checklist

Before proposing any change, run it through this checklist:

| # | Principle | Question |
|---|---|---|
| 1 | Simple | Does the kernel stay intellectually small? |
| 2 | Powerful | Does the capability emerge from composition, not from a kernel feature? |
| 3 | Performant | Does the optimization live above the core? |
| 4 | Scalable | Can the new package be removed without breaking lower layers? |
| 5 | Efficient | Are auxiliary structures rebuildable from snapshots? |
| 6 | Beautiful | Does the dependency graph still flow downward only? |
| 7 | Functional | If "Pond can't do X," have we asked: what Lens is missing? |
| 8 | Storage-Independent | Can you switch execution engines without rewriting storage? |

If any answer is "no," the proposal needs revision before it
becomes code.

---

## 8. Current roadmap (post external-validation review)

The architecture-review conversation that produced this document
proposed a five-phase roadmap. This is the canonical statement of
that roadmap.

### Phase A — Freeze (current)

No major new capabilities. Let the architecture settle. Allowed
work: documentation, RFC formalization, SDK polish, bug fixes.

### Phase B — Polish the SDK

Address every ambiguity found in `validation/vector_report.md`.
Don't add features. Remove uncertainty. Success criterion: a
second external implementation scores 9/10 for developer confidence.

### Phase C — Formalize Views

Produce a mathematically clean definition of what a Lens is, what
laws it satisfies, and how Views compose. **Status:** RFC-0007
drafted. Needs automated property tests to move to Accepted.

### Phase D — Compatibility

Prove Pond can underpin existing ecosystems without modifying them:
Arrow, DuckDB, Polars, DataFusion, Lance, Iceberg-compatible
metadata adapters. Each is an adapter View satisfying the
RFC-0007 algebra.

### Phase E — One production-grade implementation (COMPLETE)

Choose one flagship and make it excellent. Candidates:
1. Feature Store (strongest current fit) — **CHOSEN**
2. Lakehouse metadata/catalog service (closest to original motivation)
3. Git-compatible repository backend (excellent for validating
   versioning semantics)

Build one so well that an external engineer would consider using it.

**Status:** Feature Store is production-quality (RFC-0011 Accepted).
External validation measured DX at 6/10 → 8/10 (estimated, after
fixing the 3 high-impact findings). End-to-end ML workflow runs
all 12 steps. GETTING_STARTED.md written.

### Phase F — Evidence, not features (CURRENT)

After Phase E, the project entered a different phase. The question
shifted from "can Pond do this?" to "does Pond still feel elegant
doing this?" This phase is about **evidence**, not features.

**Six evidence gaps to close (in priority order):**

1. **Scale.** Run 10M–100M records. Measure metadata ratio, index
   depth, cache behavior, branch latency, GC, lookup tails.
   Architecture changes around those sizes.

2. **Long-lived history.** Millions of commits. Measure rollback,
   branch, merge, GC after years of history.

3. **Multiple simultaneous materializations.** One snapshot →
   Arrow + Parquet + Iceberg + Vector DB + Search index + Feature
   Store + Semantic model, all at once. No duplication. No sync
   daemon. If this stays elegant, that's a strong architectural result.

4. **Failure modes.** Disk full, half-written metadata, corrupt
   derived index, missing blob, branch deleted, power failure,
   hash collision simulation, clock skew, schema evolution.

5. **Independent implementations.** Five different validators
   (different models, different people, different languages).
   If they independently produce similar SDKs, the abstractions
   are probably correct.

6. **The Derived Structure calculus.** Push RFC-0005 further: is
   every optimization in Pond just a `DerivedStructure(source,
   function, trigger, storage)`? If yes, that's the biggest
   conceptual contribution.

**What is explicitly NOT in Phase F:**

- **No new domain packages.** SQL, Git, Notebook, Feature Store,
  Streaming, Graph, Arrow, Vector, Semantic — that's sufficient.
  Building ten more won't tell you much.
- **No new SDK surface** unless external validation consistently
  exposes a gap.
- **No distributed coordination (Raft, Paxos).** Still deferred
  until "what is replicated?" is answered. By the end of Phase F,
  you'll know.

### Phase K — Model falsification (CURRENT — supersedes Phase F for active work)

The project reached a stage where adding more Views or running more
benchmarks stopped producing architectural insight. The remaining
uncertainty is in the **model itself**, not in the implementation.
Phase K attacks the model from two directions.

#### Phase K.1 — First Red Team (formalize the algebras)

> Status: COMPLETE. See `POND_MATHEMATICAL_MODEL.md` and
> `POND_FORMAL_ALGEBRAS.md` (Part I, sections 1-8).

Eight algebras formalized: Reference, Merge, GC, RTT Calculus,
Object Store Native, Physical Structure Taxonomy, Workspace,
History. Open questions identified and listed for Phase K.2.

#### Phase K.2 — Second Red Team (attack the model)

> Status: COMPLETE. See `POND_SECOND_RED_TEAM.md`.

Six hostile architects (FDB, Git, Dolt, Iceberg, Pebble, WarpStream)
attacked the model from outside. 13 attacks mounted:
- **5 hidden primitives** (A1, A2, A7, A12, A13): the model
  claimed 3 primitives but silently depended on 5 substrates.
- **2 false laws** (A4, A8): `L6` (composition by byte concat) and
  `W2` (cross-Lens atomicity) were provably wrong.
- **2 collapses** (A5, A11): the Physical Structure "algebra" was
  a tautology; OSN1-OSN8 was marketing.
- **4 under-specifications** (A6, A9, A10, A5-partial): RTT
  calculus ignored dollar cost; merge M4 was a workaround not a
  law; History's source was wrong; etc.

Mandatory model changes M1-M10 issued. All executed in
`POND_FORMAL_ALGEBRAS.md` Part II.

#### Phase K.3 — Formalize the missing algebras (post-red-team)

> Status: COMPLETE. See `POND_FORMAL_ALGEBRAS.md` Part II
> (sections 9-17).

Six new algebras added. **Net result:** the model is *smaller in
concept count* (despite more algebras) because hand-wavy claims
were replaced with formal axioms.

| Substrate count | Operation count | Axiom count | Algebra count |
|---|---|---|---|
| Before: 3 (rhetorical) | 3 | 4 (A1-A4) | 8 |
| After: 5 (honest) | 4 (added `ReadRange`) | 8 (added A5-A8) | 14 (added 6 in Part II) |

The five substrates are: **Bytes, Names, Time, Coordination
(optional), Range-Read (folded into Bytes)**. The four operations
are: `Write`, `Read`, `ReadRange`, `Ref`. The kernel has not grown;
the *honesty* about what the kernel depends on has grown.

**Four new design principles (added by Phase K):**

7. **Honesty over elegance.** If the model silently depends on a
   substrate (Time, Coordination, Range-Read), promote it. A
   3-primitive claim that depends on 5 hidden substrates is worse
   than a 5-substrate claim that is honest.

8. **Laws must be testable.** Every law (L1-L7, M1-M4, R1-R5,
   G1-G5, etc.) must be expressible as a property test. If a law
   cannot be tested, it is not a law; it is a slogan. `L6` and
   `L7` were demoted for this reason.

9. **The model is not the implementation.** The model says what
   must be true; the implementation chooses how. If a model law
   is "conditional on backend" (e.g., R3 CAS), the condition is
   part of the law, not an apology.

10. **Economy of concepts.** Two algebras that say the same thing
    are one algebra. The Physical Structure "algebra" (4 properties)
    was a tautology over one definition. OSN1-OSN8 was one
    definition + 7 derived properties. Collapse aggressively.

#### Phase K.4 — Operations falsification (COMPLETE)

> Status: COMPLETE. See `POND_THIRD_RED_TEAM.md` and
> `POND_FORMAL_ALGEBRAS.md` Part III (sections 16-21).

Six operations architects (S3, WarpStream, encryption-at-rest,
schema-registry, compression, multi-region) attacked the four
operational questions deferred from Phase K.3: Replication,
Compression, Encryption, Schema Evolution. 13 attacks (B1-B13):
5 hidden primitives, 3 false laws, 4 operational hazards, 1
collapse.

Three new algebras added:
- **Replication** (§16): single-writer per Ref; tombstone barrier;
  failover loses in-flight writes.
- **Transport** (§17): collapse of Compression + Encryption +
  Checksumming into one layer between Kernel and Lens; block
  index for range reads; envelope encryption via Key substrate;
  dictionary as content-addressed sidecar.
- **Schema Evolution** (§18): schema versions in key prefix or
  blob header; Schema Registry on Names substrate; backward/
  forward compatibility contracts; `S_schema` as fourth source
  type in the dependency graph.

Three existing algebras amended:
- **Range Read** (§11): RR2 → RR2' (transport-aware composition).
- **GC** (§3, §13): G6 (tombstone barrier) added.
- **Physical Structure Dependency Graph** (§14): D6 added
  (`S_schema` source type).

Two new axioms:
- **A9** (Single-writer per Ref).
- **A10** (Compress before encrypt).

**Cumulative model surface area (Parts I + II + III):**

| Metric | Start (K.1) | After K.3 | After K.4 |
|---|---|---|---|
| Substrates | 3 (rhetorical) | 5 (honest) | **6** (added Key; Schema Registry on Names) |
| Operations | 3 | 4 | **4** |
| Axioms | 4 (A1-A4) | 8 (A1-A8) | **10** (A1-A10) |
| Formal algebras | 8 | 14 | **17** |
| Open questions | 8 | 4 | **0** |

**The model has 0 open questions.** Phase K (model
falsification) is complete. Every concept has an axiom; every
axiom has a law; every law can be tested.

The remaining questions are *engineering* (which compression
codec? which KMS? which schema format? what `deletion_grace_period`?),
not *model*. The model is silent on these by design.

### Phase L — Model verification (COMPLETE)

> Status: COMPLETE. See `POND_PHASE_L_REPORT.md`,
> `scripts/phase_l_hazard_simulator.py`,
> `scripts/phase_l_property_tests.py`,
> `scripts/phase_l_differential_git.py`.

Phase L shifted from model falsification (Phase K) to model
verification: proving the laws hold under the operational hazards
the red teams identified. Three tracks executed:

| Track | Artifact | Tests | Pass | Fail |
|---|---|---|---|---|
| L.1 Hazard Simulator | `phase_l_hazard_simulator.py` | 3 self-tests | 3 | 0 |
| L.2 Property Tests | `phase_l_property_tests.py` | 491 checks | 491 | 0 |
| L.3 Differential Tests | `phase_l_differential_git.py` | 45 checks | 45 | 0 |
| **Total** | | **539** | **539** | **0** |

**Verified:** every kernel axiom (A1-A10); 23 algebra laws across
Reference, GC, Manifest, Range-Read, State-vs-Bytes, Concurrency,
Replication, Transport, and Schema Evolution algebras. Plus 9
differential tests against Git (content-addressing, commit chains,
branches, time travel, merge topology, tree determinism) and 6
conceptual differential tests against Dolt, Iceberg, FDB.

**Hazard injectors built:** read-after-write lag, list-after-put
lag, replica lag, partial write failure, partial read failure,
delete race, clock skew, tombstone barrier. All deterministic
and reproducible via seeded RNG.

**Five soft spots identified** (documented in `POND_PHASE_L_REPORT.md` §2):
1. Some laws tested only by API inspection, not behaviorally
   (ST3, CC1, CC2, TR3, SE8, A7).
2. Some laws declared in the model but not yet implemented as
   tests (M1-M4, W1-W5, REP2/4/5/6/8/9, TR1/2/4/5, SE1/2/3/4/7).
3. Some hazards not simulated (partition, Byzantine, disk
   corruption, hash collision, replay).
4. Differential tests are conceptual for Dolt/Iceberg/FDB (no
   real installations); real for Git.
5. The model is verified, not proven (no TLA+/Lean/Coq proof).

**Three findings the model did not anticipate** (documented in
`POND_PHASE_L_REPORT.md` §3):
1. The kernel's API is *smaller* than the model requires
   (`ReadRange` is a model primitive but not a kernel method).
2. The CAS law (R3) is unverifiable on the current kernel
   (`reference()` is unconditional LWW, no CAS parameter).
3. The Transport Layer (TR1-TR6) is entirely conceptual — no
   implementation exists.

These findings are **soft spots**, not model failures. They are
honestly documented and deferred to Phase N.

### Phase N — Model proofs (COMPLETE)

> Status: COMPLETE. See `POND_PHASE_N_REPORT.md`,
> `tla/PondKernel.tla`, `services/transport/transport.py`,
> `scripts/phase_n_untested_laws.py`,
> `scripts/phase_n_additional_hazards.py`,
> and Part IV of `POND_FORMAL_ALGEBRAS.md`.

Phase N closed 5 of 8 Phase L soft spots without growing the
kernel. Five tracks executed:

| Track | Artifact | Result |
|---|---|---|
| N.1 Demotions | `POND_FORMAL_ALGEBRAS.md` Part IV (§22-§24) | ReadRange demoted to Transport; R3 CAS demoted to conditional. Model shrinks from 4 ops to 3. |
| N.2 TLA+ Proof | `tla/PondKernel.tla` + `.cfg` | TLC verifies 6 invariants across 56 reachable states. No error. |
| N.3 Transport Layer | `services/transport/transport.py` (~330 LOC) | Reference implementation: compress + encrypt + block index + envelope encryption. 8 self-tests pass. |
| N.4 Untested Laws | `scripts/phase_n_untested_laws.py` | M1-M4' + W1-W5 tested. 23/23 pass. |
| N.5 Additional Hazards | `scripts/phase_n_additional_hazards.py` | Partition + disk corruption added. 10/10 pass. |

**The model is now:**
- **Proven** (TLA+ formal verification, 6 invariants across 56 states)
- **Minimal** (3 operations, not 4 — smaller than Phase L claimed)
- **Implemented** (Transport Layer exists in `services/transport/`)
- **Tested** (514 property + 45 differential + 10 hazard = 569 checks, all pass)
- **Honest** (no law claims more than the kernel provides)

**Updated model surface area (Parts I + II + III + IV):**

| Metric | Phase K.4 | Phase L | Phase N |
|---|---|---|---|
| Substrates | 6 | 6 | **6** |
| Operations | 4 | 4 | **3** (ReadRange demoted to Transport) |
| Axioms | 10 | 10 | **10** (A8 → A8', count unchanged) |
| Formal algebras | 17 | 17 | **17** (Range Read moved Kernel → Transport) |
| Open questions | 0 | 0 | **0** |
| Property tests | 0 | 491 | **514** |
| Differential tests | 0 | 45 | **45** |
| Hazard tests | 0 | 0 | **10** |
| TLA+ invariants proven | 0 | 0 | **6** |
| Transport Layer implemented | no | no | **yes** |
| Kernel LOC | ~140 | ~140 | **~140** (FROZEN throughout) *(Task 65 correction: `kernel.py` is now 274 LOC and NOT FROZEN — gained `write_batch`/`read_blob_batch` same-collection helpers; see §3.1)* |

**Phase L soft spots status:**
- §2.1 (API inspection only) — partially closed (TR3, TR6 now behavioral)
- §2.2 (untested laws) — partially closed (M1-M4', W1-W5 tested; ~15 laws remain)
- §2.3 (unsimulated hazards) — partially closed (partition + disk corruption added; 4 remain)
- §2.4 (conceptual differentials) — not closed (Phase O)
- §2.5 (verified not proven) — **closed** (TLA+)
- §3.1 (ReadRange gap) — **closed** (demoted)
- §3.2 (R3 CAS unverifiable) — **closed** (demoted)
- §3.3 (Transport conceptual) — **closed** (implemented)

**5 of 8 soft spots closed.** The kernel is FROZEN. The model is FROZEN. The proof is FROZEN.

> **Task 65 correction.** The "kernel is FROZEN" claim here refers to
> the Phase L *snapshot*, not the current implementation. As of the
> thread-safety round, `pond-core/kernel.py` is 274 LOC and has gained
> `write_batch` / `read_blob_batch` (same-collection performance
> primitives). The *substrate/operation count* (6 substrates, 3
> operations) is still frozen — adding a new substrate or operation
> requires an Accepted RFC. Same-collection batch wrappers do not.
> See §3.1 and §1.1 Known Gaps for the honest current state.

### Phase O — Remaining work (COMPLETE)

> Status: COMPLETE. See `POND_PHASE_O_REPORT.md`,
> `scripts/phase_o_remaining_laws.py`,
> `scripts/phase_o_remaining_hazards.py`.

Phase O closed 2 of the 3 remaining partial soft spots:

| Track | Artifact | Result |
|---|---|---|
| O.1 Remaining Laws | `scripts/phase_o_remaining_laws.py` | 19 more laws tested. 48/48 pass. |
| O.2 Remaining Hazards | `scripts/phase_o_remaining_hazards.py` | 4 more hazards simulated. 13/13 pass. |

**Laws now tested:** MAN3, RR3/4, G2/4/5, REP2/4/5/6/8/9, TR4/5,
SE1/2/3/4/7. Only 4 architectural laws remain untested (S1, S2,
History, P1) — these are conceptual properties without clean
behavioral tests.

**Hazards now simulated:** Byzantine replica (detected via A2 hash
mismatch), hash collision (documented as computationally infeasible
for SHA-256), replay attack (detected via commit timestamps),
concurrent compaction + replication (B5 hazard — reproduced AND
shown mitigated by G6 tombstone barrier).

**Final cumulative state across all phases (K + L + N + O):**

| Metric | Phase K.4 | Phase L | Phase N | **Phase O** |
|---|---|---|---|---|
| Substrates | 6 | 6 | 6 | **6** |
| Operations | 4 | 4 | 3 | **3** |
| Axioms | 10 | 10 | 10 | **10** |
| Algebras | 17 | 17 | 17 | **17** |
| Open questions | 0 | 0 | 0 | **0** |
| Property tests | 0 | 491 | 514 | **562** |
| Differential tests | 0 | 45 | 45 | **45** |
| Hazard tests | 0 | 0 | 10 | **23** |
| TLA+ invariants | 0 | 0 | 6 | **6** |
| Transport Layer | no | no | yes | **yes** |
| Kernel LOC | ~140 | ~140 | ~140 | **~140** |
| **Total checks** | 0 | 536 | 569 | **630** |

**7 of 8 Phase L soft spots closed.** 1 deferred (real
Dolt/Iceberg/FDB installs — not attempted in this environment).

### Final status: research complete

The Pond research project has reached its **final state**:

- **Kernel**: 3 operations (`Write`, `Read`, `Ref`), ~140 LOC, FROZEN *(Task 65 correction: now 274 LOC + same-collection batch helpers — see §3.1; substrate/operation count is still FROZEN, implementation is not)*
- **Model**: 6 substrates, 10 axioms, 17 algebras, 0 open questions, FROZEN
- **Proof**: 6 TLA+ invariants across 56 reachable states, FROZEN
- **Tests**: 630 checks (562 property + 45 differential + 23 hazard), all passing, FROZEN *(Task 65 note: check count is now higher — see `scripts/README.md` and the veteran's review for the current number; some self-tests fail per `docs/VETERAN_ARCHITECT_REVIEW.md`)*
- **Transport Layer**: reference implementation in `services/transport/`

The research question — *is a small-substrate kernel the right
abstraction?* — is answered: **yes, six substrates and three
operations suffice**. The model is proven sound by TLA+, tested
sound by 630 checks, and honest about what it does and doesn't
provide.

### Phase P — Engineering (COMPLETE)

> Status: COMPLETE. See `POND_PHASE_P_REPORT.md`,
> `services/schema/schema_registry.py`,
> `services/transport/transport_production.py`,
> `services/replication/replication_coordinator.py`,
> `scripts/phase_p_real_differentials.py`.

Phase P closed the last engineering gap: the model's algebras are
now backed by real implementations, not just formal specifications
and conceptual tests.

| Track | Artifact | Tests | Pass |
|---|---|---|---|
| P.1 Schema Registry | `services/schema/schema_registry.py` (~430 LOC) | 12 | 12 |
| P.2 Production Transport | `services/transport/transport_production.py` (~400 LOC) | 10 | 10 |
| P.3 Replication Coordinator | `services/replication/replication_coordinator.py` (~430 LOC) | 15 | 15 |
| P.4 Real Differentials | `scripts/phase_p_real_differentials.py` (~570 LOC) | 16 | 16 |
| **Total** | | **53** | **53** |

**What was built:**

- **Schema Registry** (`services/schema/`): thin layer over Names
  substrate implementing §18 Schema Evolution Algebra. SE1-SE8 all
  behaviorally tested. Per SE7, no new substrate, no kernel changes.
- **Production Transport Layer** (`services/transport/transport_production.py`):
  zstd compression + AES-GCM encryption + per-block random nonces.
  Closes the "XOR for test clarity" caveat from Phase N.3. Tamper
  detection via GCM tags verified.
- **Replication Coordinator** (`services/replication/`):
  `PrimarySecondaryCoordinator` implements REP1-REP9 + G6 (the
  in-model replication algebra). `TwoPhaseCommitCoordinator`
  implements the A7 escape hatch for cross-Collection atomicity
  via 2PC, using only kernel primitives.
- **Real Dolt + Iceberg Differential Tests**: Dolt v2.2.2 binary
  + pyiceberg + duckdb. 16 checks pass: content-addressing, commit
  chains, branches, time travel, merge topology (vs Dolt);
  manifest rebuildability, snapshot reproducibility, schema
  evolution (vs Iceberg). FDB skipped (heavy Java install).

**8 of 8 Phase L soft spots now closed:**

| Phase L soft spot | Final status |
|---|---|
| §2.1 (API inspection only) | closed (P.2 makes Transport behavioral) |
| §2.2 (untested laws) | closed (Phase O + P tested all but 4 architectural) |
| §2.3 (unsimulated hazards) | closed (Phase O simulated all 9 hazards) |
| §2.4 (conceptual differentials) | **closed** (P.4 ran real Dolt + Iceberg) |
| §2.5 (verified not proven) | closed (Phase N TLA+) |
| §3.1 (ReadRange gap) | closed (Phase N demotion) |
| §3.2 (R3 CAS unverifiable) | closed (Phase N demotion) |
| §3.3 (Transport conceptual) | closed (Phase N reference + P.2 production) |

**Cumulative state across ALL phases (K + L + N + O + P):**

| Metric | Value |
|---|---|
| Substrates | 6 |
| Operations | 3 (`Write`, `Read`, `Ref`) |
| Axioms | 10 (A1-A10, A8' demoted) |
| Algebras | 17 (Parts I-IV of `POND_FORMAL_ALGEBRAS.md`) |
| Open model questions | 0 |
| Property tests | 562 passing |
| Differential tests | 45 (Git) + 16 (Dolt + Iceberg) = 61 passing |
| Hazard tests | 23 passing (9 hazards) |
| Engineering tests | 53 passing (P.1-P.4) |
| TLA+ invariants | 6 proven across 56 reachable states |
| **Total checks** | **683, all passing** |
| Kernel LOC | ~140 (FROZEN throughout K, L, N, O, P) *(Task 65 correction: now 274 LOC; substrate/operation count still FROZEN, implementation gained `write_batch`/`read_blob_batch` — see §3.1)* |
| Packages built | pond-core, pond-sdk, pond-feature-store, pond-arrow, pond-transport (ref + prod), pond-schema, pond-replication |

### Status: internal consistency established; external validation pending

The Pond project — across Phases A through P — has established
**internal consistency**:

> The implementation matches the model. Invariants hold in a finite
> TLA+ model. Tested behaviors match Git, Dolt, and Iceberg for the
> invariants tested. Four engineering packages implement the
> algebras on the frozen kernel.

This is **not** the same as proving the architecture is right. The
architecture has not been benchmarked against peer systems. The
architecture has not been reviewed by external distributed-systems
engineers. The architecture has not been tested with a real
production workload. The architecture has not been proven
*necessary* (no lower-bound proof that fewer substrates wouldn't
suffice).

**Phase Q (validation)** is where these gaps get addressed:

1. **Benchmarks** vs. Git, Dolt, Iceberg, LakeFS for clone, commit,
   branch, merge, lookup, RTT, memory, CPU.
2. **Whitepaper** (20-30 pages) describing Pond from first principles
   with rigorous comparison to peer systems.
3. **Flagship application** — a DuckDB-based lightweight lakehouse
   built on Pond, to test whether the Lens algebra covers real
   workloads.
4. **External review** by distributed-systems engineers and
   researchers who did not build Pond.
5. **(optional) Lean/Coq proof** of algebra laws following from
   axioms (stronger than TLA+ consistency).

The kernel is FROZEN at ~140 LOC. The model is FROZEN at 17
algebras. The internal consistency work (Phases K-P) is done. The
external validation work (Phase Q) is the next phase. **Whether
Pond is the right abstraction is not yet decided.**

> **Task 65 correction.** "The kernel is FROZEN at ~140 LOC" was true
> at Phase P snapshot time. As of the thread-safety round,
> `pond-core/kernel.py` is 274 LOC and has gained same-collection
> batch I/O helpers (`write_batch`, `read_blob_batch`). What is still
> FROZEN is the *substrate/operation count* (6 substrates, 3
> operations) — see §3.1. The model (17 algebras) is unchanged.

### Phase Q — Validation (IN PROGRESS)

> Status: IN PROGRESS. See `POND_PHASE_Q_REPORT.md`,
> `POND_WHITEPAPER.md`, `POND_PHASE_Q_BENCHMARKS.md`,
> `POND_PHASE_Q_REVIEW_PACKET.md`,
> `scripts/phase_q_benchmarks.py`, `lenses/lakehouse/lakehouse_lens.py`.

Phase Q is validation, not invention. No new algebras. No new
substrates. No new axioms. The architecture is frozen; the
question is whether it survives contact with reality.

| Track | Question | Artifact | Status |
|---|---|---|---|
| Q.1 Overclaim correction | Are the docs honest? | DESIGN_GOALS.md §1-§2 revised | DONE |
| Q.2 Whitepaper | Can Pond be explained rigorously? | `POND_WHITEPAPER.md` (20 pages) | DONE (draft) |
| Q.3 Benchmarks | Is Pond competitive? | `scripts/phase_q_benchmarks.py` + report | DONE (directional) |
| Q.4 Flagship | Does Lens cover real workloads? | `lenses/lakehouse/lakehouse_lens.py` (10 tests) | DONE (works, 15-357% overhead) |
| Q.5 External review | Do experts find it sound? | `POND_PHASE_Q_REVIEW_PACKET.md` | PREPARED, no reviews yet |

**Phase Q.1 (overclaim retraction):** earlier docs said "Pond is
done" and "the model is proven." That was overclaim. TLA+ over 56
states proves consistency, not correctness. 683 tests prove
implementation matches model, not that the architecture is right.
The docs now say "internal consistency established; external
validation pending."

**Phase Q.2 (whitepaper):** 20-page rigorous description with
formal capability matrix vs Git, Iceberg, Dolt, FDB, LakeFS.
Per-system analysis. Explicit "what Pond does NOT do" section.
6 attack vectors for reviewers. Draft for external review.

**Phase Q.3 (benchmarks):** 7 operations × 4 systems. Pond wins
6/7 (commit, branch, lookup, time travel, merge); loses on full
scan to Iceberg's columnar format (3.4ms vs 0.6ms). Biased
toward in-process systems (Pond, Iceberg); Git and Dolt pay
subprocess overhead. Directional, not definitive.

**Phase Q.4 (flagship):** DuckDB-based lakehouse on Pond.
10 tests pass: CREATE, INSERT, SELECT (WHERE/ORDER BY/GROUP BY/
JOIN/aggregation), time travel, branching, merge, schema
evolution. Benchmark vs native DuckDB+Parquet: 15% overhead on
create, 127-357% on queries (re-registering tables each query;
production would cache). The Lens algebra covers the lakehouse
workload; the implementation has overhead.

**Phase Q.5 (external review):** Review packet prepared with 15
specific questions for reviewers. **No reviews received yet.**
This is the biggest gap in Phase Q.

### Phase Q findings (honest)

**Established in Phase Q:**
- Pond's kernel is not pathologically slow (benchmarks).
- The Lens algebra covers the lakehouse workload (flagship).
- The architecture can be explained rigorously (whitepaper).
- The overclaim is retracted (docs revised).

**NOT established in Phase Q:**
- External expert review (no reviews received).
- Production-scale benchmarks (1-100 keys only).
- Object-store benchmarks (local disk only).
- Fair subprocess comparison (need libgit2 + Dolt SQL server).
- TabularLens (proposed mitigation for full-scan loss, unimplemented).
- Lower-bound proof (no proof six substrates are necessary).
- Adoption (no production use).

### What's next (Phase R, not started)

1. **Send review packet to 3-5 external reviewers** (highest value).
2. **Implement TabularLens** to recover Iceberg scan performance.
3. **Re-benchmark with libgit2 + Dolt SQL server** (remove bias).
4. **Benchmark at 1M keys** (test scaling).
5. **Benchmark on S3** (test object-store-native claims).
6. **Revise whitepaper based on reviews.**
7. **Optional: submit to workshop/conference** (if reviews positive).

### What to STOP doing

- **Stop inventing algebras.** 17 is enough.
- **Stop adding internal tests.** 683 is enough.
- **Stop claiming "Pond is done."** It isn't. It is internally
  consistent and ready for external falsification.

### What is explicitly NOT on the roadmap

- **Distributed consensus (Raft, Paxos) in the kernel.** Still
  out-of-model per A7. A coordinator may be added by the
  application; the kernel does not provide one.
- **New domain packages.** SQL, Git, Notebook, Feature Store,
  Streaming, Graph, Arrow, Vector, Semantic, Lakehouse — sufficient.
- **New SDK surface.** Unless external validation consistently
  exposes a gap.
- **Productionization as a research goal.** Pond's research goal
  — discover whether the model is right — is **not yet achieved**.
  Internal consistency is established; external validation is
  pending. Production adoption is a different project, only
  worth pursuing if external validation is positive.

---

## 9. For future agents (human or AI)

If you are an agent picking up Pond work, do this in order:

1. **Read this file (`DESIGN_GOALS.md`) in full.** You just did.
2. **Read `README.md`** for project status and the experiment table.
3. **Read `worklog.md`** to see what previous agents did and what
   state the project is in.
4. **Read the RFC most relevant to your task.** If your task is
   about Views, read RFC-0007. If about deletion, RFC-0008. If about
   the kernel, RFC-0003.
5. **Read `validation/vector_report.md`** if your task touches the
   SDK. The validator's findings are the SDK polish backlog.
6. **Check `docs/NON_GOALS.md`** before proposing any feature. If
   your feature is on the Non-Goals list, it does not belong in the
   kernel; it may belong in a Lens.
7. **Run the weekly question** (§4 above). If your work makes the
   answer "no," stop and reconsider.
8. **Append to `worklog.md`** when you finish. Use the format in
   `worklog.md`'s existing entries: Task ID, Agent, Task, Work Log,
   Stage Summary. Do not overwrite; append only.

### If you are an AI agent specifically

- The kernel is NOT FROZEN at the implementation level — it has
  gained `write_batch` and `read_blob_batch` (same-collection
  performance primitives; not cross-collection atomicity). What
  IS frozen is the **substrate/operation count** (6 substrates, 3
  operations). Adding a new substrate or a new core operation
  requires an Accepted RFC that passes the Admission Rule
  (`rfcs/README.md`). Same-collection batch wrappers and bug fixes
  do not require an RFC.
- Do not add features to the kernel to solve Lens-level problems.
  The answer to "the kernel can't do X" is almost always "X is a
  Lens-level pattern, not a kernel primitive." See RFC-0008 for
  the deletion case study.
- Prefer editing existing files over creating new ones. New RFCs
  are fine; new packages need explicit justification against the
  removability test.
- All deliverables go in `/home/z/my-project/download/` if user-
  facing, or stay in the repo if they are Pond itself.
- Use the outcome vocabulary (§6) for any claim. "Supported," not
  "proven." "Falsified," not "broken."

---

## 10. Current architecture (post-Round-1-through-4, 2026-07-30)

> **Honest status update.** The architecture has evolved significantly
> since the Phase K-Q docs were written. This section is the CURRENT
> truth; anything above that contradicts this is historical.

### What's been built (unified storage layer)

1. **`ObjectStoreNativeKernel`** (`pond-core/object_store_native_kernel.py`) —
   a kernel with NO SQLite. Refs are stored as content-addressed blobs in
   the object store (the Git HEAD→commit→tree pattern). Every ref
   resolution is 2 S3 GETs cold (root pointer + root ref blob), 0 warm
   (SDK-cached). This is the object-store-native kernel the whitepaper
   always described but didn't have.

2. **`UnifiedStorage`** (`pond-sdk/extensions/physical_structures/unified_storage.py`) —
   ONE binary format (PND2), ONE write path (`write`/`append`), ONE read
   path (`read`/`point_lookup`). Replaces the 3 write modes + 7+ read
   methods of the old LakehouseLens. Stats computed during encode (zero
   overhead). Non-destructive `append()`. Range scans via `start_key`/`end_key`.

3. **`CollectionManifest`** (`pond-sdk/extensions/physical_structures/collection_manifest.py`) —
   ONE index blob per commit with ALL row-group stats + blob hashes inline.
   At PB scale (>25K row groups), delegates to a hierarchical `StatsTree`
   for O(log N) reads. Manifest blob stays at ~64 bytes regardless of scale.

4. **`StatsTreeReader`** (`pond-sdk/extensions/physical_structures/stats_tree.py`) —
   lazy hierarchical stats tree with aggregated min/max at internal nodes.
   Content-addressed nodes (cached by SDK). O(log N) point lookups and
   pruned scans at PB scale.

5. **Lens unified storage integration** — `KeyValueLens` and `VectorLens`
   now accept `use_unified_storage=True` to use PND2/UnifiedStorage as
   their backend. Same lens API, just a storage backend swap. No adapter
   layers. (LakehouseLens and StreamingLens still use the legacy path —
   migration is in progress.)

### Honest competitive assessment

See [`docs/HONEST_COMPETITOR_COMPARISON.md`](docs/HONEST_COMPETITOR_COMPARISON.md)
for the full analysis. Summary:

| Workload | Pond cold RTT | Competitor RTT | Verdict |
|---|---|---|---|
| Lakehouse point lookup | 4 GETs (UnifiedStorage) | 3 GETs (Iceberg) | Close, slightly worse |
| Vector k-NN @ 10M | **10M GETs** (linear scan) | 5-100 GETs (HNSW/IVF) | **100,000x worse — not competitive** |
| KV point lookup | 4 GETs (UnifiedStorage) | <1ms (Redis) | 200x worse latency |
| Streaming append | N+3 PUTs ≈ 200ms | <5ms (Kafka) | 40x worse, no consumer groups |

**Pond is NOT yet competitive with production systems** in vector, KV, or
streaming workloads. The lakehouse path is directionally close to Iceberg
on RTTs but lacks production deployment, catalog, and partitioning. The
unified storage layer (PND2 + CollectionManifest + StatsTree) is a solid
foundation, but the lenses need workload-specific acceleration structures
(HNSW for vectors, memtable+SST for KV, partitions+consumer groups for
streaming) to be competitive.

### What's NOT built (honest gaps)

> **Task 65 update.** Several items below were true when §10 was written
> but are no longer accurate. Rather than rewrite history, the per-item
> status is annotated inline. The authoritative current gap list is
> **§1.1 Known gaps (post-veteran-architect review)** — read that
> first.

- **No HNSW/IVF for vector search** — linear scan only. Not usable at >100K vectors.
  *(Task 65 status: **partly outdated.** `pond-sdk/extensions/indexing/hnsw_index.py`
  and `ivf_index.py` now exist. HNSW is implemented per its docstring.
  IVF exists but does NOT reduce I/O — it reads all vectors then filters
  in Python. See §1.1 Known Gaps.)*
- **No transactions** — single-writer per Ref. No cross-collection atomicity.
  *(Task 65 status: **partly outdated.** `commit_tx` provides atomic
  publication across collections (all-or-nothing HEAD pointer moves).
  But there is no isolation, no rollback, no serializability — see
  §1.1 Known Gaps. The honest term is "atomic publication," not "ACID.")*
- **No consumer groups / partitioning** in StreamingLens.
  *(Task 65 status: **outdated.** `StreamingLens` now has `create_topic`,
  `list_partitions`, `produce`, `produce_round_robin`, `get_latest_offset`,
  `consume`, `commit_offset`, `list_consumer_groups`,
  `get_consumer_group_offsets`, `replay_from`. See `lenses/streaming/streaming_lens.py`.)*
- **LakehouseLens still defaults to PondMinimal (SQLite)** — not yet migrated to ObjectStoreNativeKernel.
  *(Task 65 status: **needs verification.** The `make_kernel()` factory
  routes `file://` and `s3://` URLs to `ObjectStoreNativeKernel`; whether
  LakehouseLens actually uses this path by default is unverified.)*
- **No production S3 backend** — only InMemoryObjectStore with simulated latency.
  *(Task 65 status: **outdated.** `pond-core/s3_object_store.py` is a real
  boto3-backed store. R2/S3 integration tests (`scripts/test_s3_integration.py`,
  `scripts/benchmark_full_r2.py`) exercise it against moto and real R2.)*
- **No PB-scale benchmark** — max tested is 30K row groups (smoke test, not perf).
- **Git lens is archived** — 63-line prototype, not shipped.

---

## 11. The one-sentence summary

> **Pond is a research project asking whether a small storage kernel is
> sufficient to compose all workload semantics; the unified storage layer
> (PND2 + CollectionManifest + StatsTree) is a solid foundation, but the
> lenses are not yet competitive with production systems in vector, KV,
> or streaming workloads. The path forward is workload-specific
> acceleration structures (HNSW, memtable+SST, consumer groups), not
> more kernel features.**
