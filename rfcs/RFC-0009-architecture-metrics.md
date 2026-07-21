# RFC-0009: Architecture Metrics

## Status

Draft — the measurement framework for the six design goals in
`DESIGN_GOALS.md`.

## Abstract

Pond's existing benchmarks measure *engineering* (write throughput,
point-lookup latency, metadata ratio). These are necessary but not
sufficient. They do not measure *design*. A project can have excellent
microbenchmarks and a decaying architecture.

This RFC defines a complementary metric set that measures the six
design goals directly. Each metric is measurable, repeatable, and
aligned with a specific principle from `DESIGN_GOALS.md`. The goal:
make architectural drift detectable the same way performance
regression is detectable.

---

## 1. Motivation

The architecture review that produced this RFC made the case
forcefully:

> Not latency. Not throughput. Architecture.
>
> Metrics like:
> * Number of concepts exposed to a View author.
> * Number of kernel calls required for a feature.
> * Boilerplate lines per new View.
> * Number of duplicated algorithms across packages.
> * Number of assumptions leaking between layers.
> * Cognitive complexity of implementing a View.
>
> These align much better with your original design goals than
> microbenchmarks.

Microbenchmarks measure *how fast the system runs*. Architecture
metrics measure *how well the system is designed*. The two are
orthogonal: a fast system can have a decaying architecture, and a
well-designed system can be slow. Pond's goal is to be both — but
if architecture and performance come into conflict, the design goals
(`DESIGN_GOALS.md`) say architecture wins.

This RFC defines the measurement framework for that side of the
tradeoff.

---

## 2. The metric set

Each metric maps to one of the six design goals in `DESIGN_GOALS.md`.

| # | Metric | Design goal | Direction |
|---|---|---|---|
| A1 | Kernel primitive count | Simple | lower is better |
| A2 | Kernel LOC | Simple | lower is better |
| A3 | Concepts exposed to a View author | Simple | lower is better |
| B1 | Kernel calls per typical feature | Powerful | lower is better |
| B2 | Boilerplate LOC per new View | Powerful | lower is better |
| B3 | Duplicated algorithms across packages | Powerful | lower is better |
| C1 | Layer-leak count (N+1 reaching past N) | Beautiful | 0 is required |
| C2 | Removability test failures | Scalable | 0 is required |
| C3 | Cognitive complexity of a reference View | Beautiful | lower is better |
| D1 | Materialization rebuild correctness | Efficient | 100% required |
| D2 | Materialization determinism | Efficient | 100% required |
| E1 | View algebra law violations (RFC-0007) | Beautiful | 0 is required |
| E2 | View equivalence class size | Powerful | higher is better |

The metrics are split into:
- **Hard constraints** (C1, C2, D1, D2, E1): must be zero / 100%.
  Any nonzero value is a release-blocking bug.
- **Trend metrics** (A1, A2, A3, B1, B2, B3, C3, E2): tracked over
  time. A regression is a yellow flag; a sustained regression over
  three measurements is a red flag.

---

## 3. Detailed definitions

### A1. Kernel primitive count

The number of public operations on the kernel's primary interface.

**Measurement:** count `def` statements in `pond-core/pond_minimal.py`
that are part of the public API (not prefixed with `_`, not
helpers). As of this writing: 3 (`write`, `read`, `reference`) plus
3 supporting (`resolve`, `list_names`, `read_blob`). The target is
3 core + 2 supporting (`resolve` is a property of `reference`;
`list_names` is a property of the namespace; `read_blob` is a
performance shortcut that could be expressed as `read(hash)` and
should be considered for removal).

**Target:** ≤ 3 core primitives. Supporting operations allowed only
if they cannot be expressed as compositions of the core 3.

### A2. Kernel LOC

Lines of code in `pond-core/pond_minimal.py`, excluding comments and
blank lines.

**Measurement:** `cloc pond-core/pond_minimal.py` (or equivalent).

**Target:** ≤ 200 LOC. Currently ~140 LOC. A 50-LOC increase is a
yellow flag; a 100-LOC increase is a red flag requiring RFC
justification.

### A3. Concepts exposed to a View author

The number of distinct concepts a View author must understand to
build a working View.

**Measurement:** enumerate the public API surface of `pond-sdk`'s
`View` and `IndexedView` classes. Count distinct concepts (methods,
attributes, configuration knobs). The `validation/vector_report.md`
is the ground truth: the validator had to understand ~20 concepts to
build a VectorView.

**Target:** ≤ 15 concepts for a basic View; ≤ 25 for an
`IndexedView`. New concepts require explicit justification.

### B1. Kernel calls per typical feature

The number of `kernel.write` / `kernel.read` / `kernel.reference`
calls required to implement a typical View feature (e.g., "insert one
row," "commit one snapshot," "lookup by index").

**Measurement:** instrument the kernel with a call counter; run a
reference View's test suite; report the median calls-per-feature.

**Target:** Track over time. A 2× regression is a yellow flag
(meaning the View is doing more work than necessary); a 5×
regression is a red flag.

### B2. Boilerplate LOC per new View

The number of lines a View author must write to build a minimal
"Hello World" View (e.g., a View that stores and retrieves one kind
of record, with commit and branch support).

**Measurement:** count lines in the smallest reference View
implementation (currently the `VectorView` from
`validation/vector_report.md`, ~250 LOC including binary encoding).

**Target:** ≤ 100 LOC for a minimal View. ≤ 200 LOC for a View
with indexes and history.

### B3. Duplicated algorithms across packages

The number of algorithms implemented more than once across
`pond-*` packages. Examples: tree-walking, commit-DAG traversal,
binary encoding, delta computation.

**Measurement:** static analysis — search for similar function
signatures and bodies across packages. Manual review for nontrivial
duplicates.

**Target:** 0. Any duplicate is a candidate for extraction into
`pond-sdk` (Layer 1–2) or a shared utility package.

### C1. Layer-leak count (HARD CONSTRAINT)

The number of cases where Layer N+1 reaches past Layer N (i.e.,
depends on Layer N-1 or below, skipping Layer N). Also includes any
case where the kernel (`pond-core`) imports from a higher layer.

**Measurement:** static dependency analysis. Build the import graph;
check for edges that skip layers.

**Target:** 0. Any nonzero value is a release-blocking bug.

### C2. Removability test failures (HARD CONSTRAINT)

For each package P in `pond-*`, the removability test asks: "If P is
deleted entirely, do any lower-layer packages break?"

**Measurement:** for each package, comment out its imports in lower
layers; run the lower layers' test suites; report failures.

**Target:** 0 failures. Any failure means a lower layer has leaked
a dependency upward — a release-blocking bug.

### C3. Cognitive complexity of a reference View

McCabe-style cyclomatic complexity of the `commit` and `resolve`
methods in a reference View implementation.

**Measurement:** run a cyclomatic-complexity analyzer (e.g.,
`radon cc`) on the reference View's `commit` and `resolve` methods.

**Target:** ≤ 10 per method. > 15 is a yellow flag; > 20 is a red
flag (the View is doing too much in one place; refactor).

### D1. Materialization rebuild correctness (HARD CONSTRAINT)

For each materialization M in the system, dropping M and rebuilding
it must produce a result identical to the original.

**Measurement:** for each materialization, snapshot the output,
drop the materialization, rebuild, compare. Run as a property test
on every commit.

**Target:** 100% pass. Any failure means a materialization has
hidden state — violating Materialization Law 1 (determinism,
RFC-0005).

### D2. Materialization determinism (HARD CONSTRAINT)

For each materialization M, computing M(snapshot) twice must produce
identical results.

**Measurement:** for each materialization, compute it twice from
the same snapshot, compare. Run as a property test on every commit.

**Target:** 100% pass. Any failure means a materialization is
non-deterministic — a correctness bug.

### E1. View algebra law violations (HARD CONSTRAINT)

The number of violations of the six View Algebra laws (RFC-0007) in
the existing View implementations.

**Measurement:** the `view_laws.py` property-test harness (to be
built in Phase B). Runs the six law checks against every registered
View.

**Target:** 0 violations. Any violation is a release-blocking bug.

### E2. View equivalence class size

The number of Views that share a given `(Σ, E, D)` triple — i.e.,
the number of Views that can read each other's persisted state.

**Measurement:** cluster Views by their `(Σ, E, D)` signature;
report the size of each cluster.

**Target:** Higher is better. A large equivalence class means many
Views can interoperate; a small class means each View is an island.
The long-term goal is that all SQL-family Views (SQL, DuckDB, Polars,
DataFusion adapters) are in one equivalence class via Arrow IPC.

---

## 4. Measurement cadence

| Metric type | Cadence | Action on regression |
|---|---|---|
| Hard constraints (C1, C2, D1, D2, E1) | Every commit (CI) | Block release |
| Trend metrics (A1, A2, A3, B1, B2, B3, C3, E2) | Every release | Open issue; review at next architecture sync |
| External validation (DX score) | Every major release | Run `validation/vector_challenge_prompt.md` with a fresh agent; track score over time |

The external validation cadence is the most important. The first
validation scored DX = 5/10. The goal of Phase B is to raise this
to 9/10. Without re-running the validation, we cannot know whether
SDK changes actually improved DX.

---

## 5. What this metric set deliberately does NOT measure

- **Latency, throughput, memory.** These are engineering metrics,
  measured by the existing benchmarks. They are necessary but not
  sufficient.
- **Feature count.** Number of View types, number of supported
  workloads. Adding features without architectural discipline is a
  regression, not progress.
- **Code churn.** Lines changed per commit. High churn can indicate
  either healthy iteration or architectural instability; the metric
  alone cannot distinguish.
- **Issue count, PR count, contributor count.** These measure
  community health, not architecture.
- **Test coverage.** Measures engineering hygiene, not design quality.

These omissions are deliberate. Architecture metrics and engineering
metrics are orthogonal; mixing them produces noise.

---

## 6. Relationship to existing metrics

The existing benchmarks (in `prototype/benchmark.py`,
`prototype/bench_*.py`) measure:

- Write throughput: 25K rows/sec
- Point lookup: 45–67 µs p50
- Index update: 103× faster than rebuild
- Metadata ratio: 68% (down from 125%)
- Architectural compression: 68–75% code reduction

These remain valid and continue to be measured. RFC-0009 does not
replace them; it complements them. The full measurement picture is:

| Layer | What it measures | Existing | RFC-0009 |
|---|---|---|---|
| Engineering | Speed, size, efficiency | ✓ (benchmarks) | — |
| Architecture | Design quality, drift | — | ✓ |
| Communication | External DX | partial (`validation/`) | ✓ (cadence formalized) |

---

## 7. Implementation checklist

- [ ] Add `view_laws.py` property-test harness (Phase B SDK polish).
- [ ] Add `arch_metrics.py` script that computes A1, A2, A3, B1, B2,
      B3, C1, C2, C3 on the current repo.
- [ ] Add a CI job that runs the hard-constraint checks (C1, C2, D1,
      D2, E1) on every commit.
- [ ] Add a release-time job that computes the trend metrics and
      appends them to `architecture-metrics-history.csv`.
- [ ] Re-run `validation/vector_challenge_prompt.md` with a fresh
      agent at every major release; record DX score in
      `architecture-metrics-history.csv`.
- [ ] Define baselines: record current values for all metrics, so
      regressions are detectable.

---

## 8. Relationship to other RFCs

- **Depends on:** RFC-0007 (View Algebra — defines E1's laws),
  RFC-0005 (Materialization — defines D1, D2's correctness
  criteria).
- **Operationalizes:** `DESIGN_GOALS.md` §3 (the six design goals).
  Each metric in this RFC maps to a design goal.
- **Informs:** Phase B (SDK polish) — the SDK polish work should
  move the trend metrics in the right direction.
- **Does not modify:** any kernel or View code. This RFC is
  measurement only.
