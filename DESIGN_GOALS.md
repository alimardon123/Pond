# Pond — Design Goals and Project Context

> **Read this first.** This document is the canonical entry point for
> any agent (human or AI, local or remote) joining the Pond project.
> It captures *what we are building*, *why*, and *how to navigate the
> repository*. If you read only one file before working on Pond, read
> this one.

---

## 1. What Pond is

Pond is a **capability-oriented immutable object runtime**. The core
hypothesis: a tiny 3-primitive storage kernel (`Write`, `Read`,
`Reference`) is sufficient for radically different workloads — SQL,
vectors, streaming, Git, graphs, ML, time-series, OCI registries,
semantic layers — to be implemented as independent **Views** over a
shared immutable substrate.

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
is measured by primitive count (currently 3) and by lines of code
in `pond-core` (currently ~140). "All workload semantics" is
measured by the number of distinct Views implemented (currently 8+).
"Composition is sound" is measured by formal laws and by external
validation (a developer with no prior Pond context can build a View
from the SDK spec).

The goal is **not** to build a product. The goal is to discover
whether a 3-primitive kernel is the right abstraction. If it is,
the product follows for free. If it isn't, no amount of product
work will save it.

---

## 3. Design goals (the six principles)

Every architectural decision in Pond must serve these six principles.
When a proposal conflicts with one of them, the proposal loses.
When a proposal serves one at the cost of another, the tradeoff is
made explicit and recorded in `rfcs/` or `docs/REJECTED_DESIGNS.md`.

### 3.1 Simple — the kernel remains intellectually small

The kernel is 3 primitives, ~140 LOC, stdlib only. It must stay
small enough that a single engineer can hold the entire kernel in
their head. If a proposed change makes the kernel too large to hold
in one's head, it is rejected — even if it adds useful capability.

**Test:** Can you describe the kernel in one sentence? ("Three
primitives: write bytes, read bytes, mutate a name→hash mapping.")
If you need a second sentence, the kernel has grown too complex.

### 3.2 Powerful — rich behavior emerges from composition

The kernel does as little as possible. Everything else — Trees,
Commits, Branches, Tags, OPEN/SEALED, indexes, materializations,
schedules, schemas — is a **View-level pattern** built by composing
the 3 primitives. Rich behavior must *emerge* from composition, not
be *added* as kernel features.

**Test:** Can the proposed capability be expressed as data + a View?
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
it lives in the View, not the kernel.

### 3.4 Scalable — derived structures, adapters, and domain packages
evolve independently

Views are independent. Adding a new View (e.g., `VectorView`,
`SemanticView`, `OssieView`) must not require modifying any existing
View or the kernel. Removing a View must not break any lower layer.

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
| `PACKAGES.md` | Package boundaries and removability discipline | You are adding or modifying a package |
| `worklog.md` | Append-only research log | You need to know what previous agents did |

### 5.2 RFCs (`rfcs/`) — authoritative specifications

RFCs are stable architectural documents. Once Accepted, an RFC is
the authoritative source for its topic. Draft RFCs propose; Accepted
RFCs decide.

| RFC | Title | Status | What it specifies |
|---|---|---|---|
| RFC-0001 | What Is a View? | Draft (superseded by RFC-0007) | The original draft definition of a View |
| RFC-0002 | Elegance Metrics | Draft | How to measure architectural elegance |
| RFC-0003 | Kernel Specification | **Accepted (FROZEN)** | The 3 primitives, 5 storage laws, 7 composition laws |
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
| `FORMAL_SPEC.md` | 5 storage laws + 7 composition laws + preconditions/postconditions |
| `FORMAL_ALGEBRA.md` | Mathematical definition + 8 theorems + lower-bound proof |
| `VIEW_AUTHORS_GUIDE.md` | 6 guarantees + 7 conventions + 12 unspecified (the View boundary) |
| `VIEW_INTEROP_SPEC.md` | 10 ambiguities from independent implementation, classified |
| `REJECTED_DESIGNS.md` | 15+ rejected architectural decisions with reasons |
| `NON_GOALS.md` | 15 things Pond deliberately does NOT solve |
| `PEER_COMPARISON.md` | vs Git, Irmin, IPFS, LakeFS, FDB, Dolt |
| `PROBLEM_TAXONOMY.md` | 7 categories for classifying all issues |

### 5.4 Code (`pond-core/`, `pond-sdk/`, `pond-*`)

| Package | Layer | LOC | Responsibility |
|---|---|---|---|
| `pond-core` | 0 | ~140 | The 3 primitives. FROZEN. Do not modify without an Accepted RFC. |
| `pond-sdk` | 1–2 | (see repo) | `View` base class, `IndexedView`, common View patterns |
| `pond-sql` | 3 | (see repo) | SQL View (CREATE/INSERT/SELECT/UPDATE/DELETE/ALTER + indexes + time travel) |
| `pond-streaming` | 3 | (see repo) | Streaming View (topics, consumer groups, offsets) |
| `pond-git` | 3 | (see repo) | Git View (init/add/commit/branch/checkout/merge/diff) |
| `pond-notebook` | 3 | (see repo) | Notebook View (pages, search, attachments, history) |
| `pond-feature-store` | 3 | (see repo) | Feature Store View (the current flagship) |
| `pond-semantic` | 3 | (see repo) | Semantic View (OssieAdapter; future CubeAdapter, DbtAdapter) |
| `pond-vector` | 3 | (see repo) | Vector View (k-NN search) — built by external validation |

**Removability rule:** Every package above must be removable without
changing any lower layer. If removing `pond-feature-store` requires
changing `pond-sdk`, something is wrong.

### 5.5 Engineering (`engineering/`) — production hardening

| File | Purpose |
|---|---|
| `01_concurrency.py` | Thread-safety for the root namespace (Finding 7 fix) |
| `02_gc.py` | `PondGC` — View-level reachability walk + sweep (Finding 6 fix) |
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

## 7. The six design goals as a checklist

Before proposing any change, run it through this checklist:

| # | Principle | Question |
|---|---|---|
| 1 | Simple | Does the kernel stay intellectually small? |
| 2 | Powerful | Does the capability emerge from composition, not from a kernel feature? |
| 3 | Performant | Does the optimization live above the core? |
| 4 | Scalable | Can the new package be removed without breaking lower layers? |
| 5 | Efficient | Are auxiliary structures rebuildable from snapshots? |
| 6 | Beautiful | Does the dependency graph still flow downward only? |

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

Produce a mathematically clean definition of what a View is, what
laws it satisfies, and how Views compose. **Status:** RFC-0007
drafted. Needs automated property tests to move to Accepted.

### Phase D — Compatibility

Prove Pond can underpin existing ecosystems without modifying them:
Arrow, DuckDB, Polars, DataFusion, Lance, Iceberg-compatible
metadata adapters. Each is an adapter View satisfying the
RFC-0007 algebra.

### Phase E — One production-grade implementation

Choose one flagship and make it excellent. Candidates:
1. Feature Store (strongest current fit)
2. Lakehouse metadata/catalog service (closest to original motivation)
3. Git-compatible repository backend (excellent for validating
   versioning semantics)

Build one so well that an external engineer would consider using it.

### What is explicitly NOT on the roadmap

- **Distributed consensus (Raft, Paxos).** Do not even think about
  it until Phase D is complete. The question "what is replicated?"
  (Views? Derived structures? Snapshots? References?) must be
  answered before the mechanism is chosen.

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
   kernel; it may belong in a View.
7. **Run the weekly question** (§4 above). If your work makes the
   answer "no," stop and reconsider.
8. **Append to `worklog.md`** when you finish. Use the format in
   `worklog.md`'s existing entries: Task ID, Agent, Task, Work Log,
   Stage Summary. Do not overwrite; append only.

### If you are an AI agent specifically

- The kernel is FROZEN. Do not modify `pond-core/pond_minimal.py`
  without an Accepted RFC that passes the Admission Rule
  (`rfcs/README.md`).
- Do not add features to the kernel to solve View-level problems.
  The answer to "the kernel can't do X" is almost always "X is a
  View-level pattern, not a kernel primitive." See RFC-0008 for
  the deletion case study.
- Prefer editing existing files over creating new ones. New RFCs
  are fine; new packages need explicit justification against the
  removability test.
- All deliverables go in `/home/z/my-project/download/` if user-
  facing, or stay in the repo if they are Pond itself.
- Use the outcome vocabulary (§6) for any claim. "Supported," not
  "proven." "Falsified," not "broken."

---

## 10. The one-sentence summary

> **Pond is a research project asking whether three storage
> primitives are sufficient to compose all workload semantics; the
> answer so far is supported, not proven, and the path from here
> is to formalize, polish, and prove compatibility — not to add
> more features.**
