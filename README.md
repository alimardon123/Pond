# Pond

> *One copy. Infinite execution. Zero coordination unless necessary.*

Pond is a **capability-oriented immutable object runtime** — not another
lakehouse, not another table format, not another Spark.

The core hypothesis: a tiny 3-primitive storage kernel (`Write`, `Read`,
`Reference`) is sufficient for radically different workloads — SQL, vectors,
streaming, Git, graphs, ML, time-series, OCI registries, and beyond — to be
implemented as independent **Views** over a shared immutable substrate.

---

## Outcome vocabulary (used throughout)

To avoid confirmation bias, every experiment result uses this strict
vocabulary. No "this proves" or "strongest evidence." Just:

- **Supported** — evidence increased confidence in a hypothesis
- **Falsified** — hypothesis failed
- **Inconclusive** — experiment didn't isolate the question
- **Needs larger-scale validation** — prototype limits prevent a conclusion

---

## Project status

| Phase | Status | Outcome |
|---|---|---|
| Architecture (RFC 1) | Frozen | — |
| Prototype (v0.1-v0.4) | Complete | Supported (internal consistency) |
| Stress testing (metadata, S3, crash, concurrency) | Complete | 2 falsified, 4 supported |
| Universality (8 Views, 0 kernel changes) | Complete | Supported (empirical, not proof) |
| Minimality (3 primitives confirmed) | Complete | Supported (empirical, not proof) |
| Identity experiments (8 adversarial tests) | Complete | 6 supported, 2 confirmed primitives |
| **Mathematical destruction** | Complete | 6 supported, 2 falsified (known) |
| **Economic destruction** | Complete | 7 supported, 0 falsified |
| **Distributed destruction** | Complete | 6 supported, 2 falsified (known), 1 inconclusive |
| **Storage destruction** | Complete | 6 backends supported, 0 falsified |
| **Scale destruction** | Complete | 4 supported, 1 falsified (known), 2 need validation |
| **Human destruction** | Complete | 5 workloads supported, 5 doc gaps found |
| Concurrency (MVCC, thread-safe root namespace) | Pending | — |
| Replication (Raft) | Pending | — |
| S3 backend | Pending | — |
| Planner / IR | Pending | — |

The "destruction" phases are trying to break the architecture, not prove it
works. Each experiment ends in Supported / Falsified / Inconclusive /
Needs larger-scale validation.

---

## The minimal kernel

```
Write : bytes -> hash              (create immutable content-addressed blob)
Read  : hash | name -> bytes       (fetch blob by hash, or resolve name then fetch)
Ref   : name × hash -> ()          (mutable name -> hash mapping; the ONLY mutable op)
```

Three operations. No Tree. No Commit. No OPEN/SEALED. No lifecycle. No SQL.
No Parquet. No Arrow. No format or workload concepts.

Everything else — Tree, Commit, Tag, Branch, OPEN/SEALED, lifecycle, history,
branching, time travel — is a **View-level pattern** built from these 3
primitives.

**Important caveat:** the 3-primitive kernel is an *empirical hypothesis*,
not a proof. It is supported by 8 workloads + 6 alien workloads + 8 identity
experiments. The destruction phases are designed to falsify it.

---

## Repository layout

```
Pond/
├── README.md                              # this file
├── worklog.md                             # full research worklog
├── docs/
│   └── pond_rfc1_storage_and_versioned_state.pdf   # RFC 1 (formal spec)
├── scripts/
│   └── pond_rfc1.py                       # ReportLab script that generated the RFC
├── prototype/                             # v0.1-v0.5 code + benchmarks
└── destruction/                           # destruction-phase experiments
    ├── 01_mathematical.py                 # complexity budget for every operation
    ├── 02_economic.py                     # amplification factors at 100TB S3 scale
    ├── 03_distributed.py                  # partition, clock skew, exactly-once
    ├── 04_storage.py                      # S3/Azure/GCS/HDFS/Redis/FDB/Postgres
    ├── 05_scale.py                        # 10B blobs, 100M namespaces, 1B commits
    └── 06_human.md                        # can a stranger implement Git/Iceberg/OCI?
```

---

## The destruction plan

The architecture is frozen. The next phase is trying to destroy it.

### Stage 1: Mathematical destruction
Try to prove every operation has bad asymptotics. If any is O(N²) at scale,
the architecture fails.

| Operation | Target complexity | Status |
|---|---|---|
| Read latest | O(1) | — |
| Read version N | O(log N) | — |
| Branch | O(1) | — |
| Snapshot | O(1) | — |
| GC | O(reachable) | — |
| Replication | ? | — |
| Compaction | ? | — |
| Merge | ? | — |
| Clone | ? | — |
| Diff | ? | — |

### Stage 2: Economic destruction
At 100TB on S3, measure: storage amplification, write amplification, read
amplification, metadata amplification, request amplification, CPU/memory
amplification, AWS bill. If metadata dominates or request count explodes,
the architecture fails economically.

### Stage 3: Distributed destruction
Partition, split-brain, lost packets, clock skew, duplicate writes, retries,
out-of-order commits, exactly-once assumptions. If any distributed failure
corrupts the kernel, the architecture fails.

### Stage 4: Storage destruction
Implement the kernel over S3, Azure Blob, GCS, HDFS, Redis, FoundationDB,
Postgres. If any backend requires kernel special cases, the architecture
fails storage-independence.

### Stage 5: Scale destruction
10 billion blobs. 100 million namespaces. 1 billion commits. 1 trillion
references. If any operation degrades non-linearly, the architecture fails.

### Stage 6: Human destruction
Give the kernel to someone who knows nothing about Pond. Ask them to
implement Git, Iceberg, OCI, Feature Store, LakeFS without talking to you.
If they can't, the kernel isn't actually simple.

---

## Quick start

```bash
pip install duckdb pyarrow

# The minimal kernel + 8 Views
cd prototype
python3 bench_minimality.py           # 8 Views on 3 primitives

# Destruction experiments (the current phase)
cd ../destruction
python3 01_mathematical.py            # complexity budget for every operation
python3 02_economic.py                # amplification at 100TB S3 scale
python3 03_distributed.py             # partition, clock skew, exactly-once
python3 04_storage.py                 # backend independence test
python3 05_scale.py                   # extreme scale simulation
```

---

## Retractions (honest correction of overclaims)

Earlier versions of this README made claims stronger than the evidence
supports. Retracting:

1. **"Content-addressing makes the kernel inherently resilient to retries,
   duplicates, clock skew, split-brain."** — Overclaimed. Content-addressing
   handles idempotent writes and dedup. It does NOT handle: lost updates,
   concurrent reference races, namespace coordination, transactional
   visibility, causal consistency, lease expiration, conflict resolution.
   Those require additional mechanisms (Raft, MVCC, CRDTs) the kernel
   does not yet have.

2. **"Metadata is 0.002% of data at 100TB."** — Benchmark result, not
   architectural statement. Depends on workload (blob size, table count,
   commit frequency). For 1KB ML checkpoints, the ratio is much higher.
   For 1B tables, the root namespace dominates. Correct for the specific
   workload tested; not a universal property.

3. **"Mathematical destruction proved asymptotic complexity."** — It
   benchmarked one implementation (Python + SQLite). It did NOT prove
   the architecture's complexity bounds. A different implementation
   could behave differently. Analytical claims (O(1), O(log N)) are
   hypotheses, not theorems.

4. **"The kernel needs no modifications."** — Premature. The destruction
   phase evaluated the kernel against workloads I designed. Workloads
   I didn't design (CRDTs, multi-writer namespaces, causal consistency)
   might require kernel changes. The kernel is *probably* sufficient
   for the workloads tested; it is *not proven* sufficient for all
   possible workloads.

5. **"Storage independence is supported."** — True for the 6 backends
   tested (FS, memory, SQLite, Redis, S3, FDB-analytical). Not tested
   on real S3, real FDB, HDFS, Azure Blob, GCS. The architecture
   *should* work on all of them (kernel uses only PutObject + GetObject
   semantics), but empirical validation is pending.

---

## What the destruction phase DID NOT question (Identity Destruction II)

The destruction phase tested the kernel against designed workloads. It did
NOT question the kernel's foundational assumptions:

- **Is Reference primitive?** It's the only mutating operation. Why is the
  centralized operation in the kernel? Could namespace be a View concern?
- **Is the namespace model right?** `name -> hash` is one model. Could it
  be `(name, epoch)`, paths, tenant+name, capability tokens, graph edges,
  content queries?
- **Is the kernel an API or laws?** APIs evolve; invariants endure. Should
  the architecture be specified as laws (immutability, addressability,
  name-mutability) rather than operations (Write/Read/Reference)?
- **Can names disappear?** Reference overwrites, but can a name be deleted?
  What happens to reachability?
- **Can references be CRDTs?** For multi-writer/multi-region scenarios.
- **Can two namespaces overlap?** Compose? Conflict?

These are the Identity Destruction II questions. See `destruction/II_identity/`.

---

## Comparison set (revised)

Pond is NOT competing with Iceberg, Delta, or table formats. The real
comparison set — systems with similar ambitions:

- **Git** — immutable object graph + mutable refs
- **Irmin** — content-addressable store + mutable references (OCaml)
- **IPFS/IPNS** — content addressing + naming
- **LakeFS** — versioned data namespaces on object storage
- **FoundationDB** — minimal substrate with layered architecture
- **Dolt** — versioned structured data using prolly trees

Pond's differentiation (if it holds): a smaller substrate than any of these,
specified as laws rather than APIs, with Views as the primary extension
mechanism rather than baked-in semantics.

---

## Architectural metrics

| Metric | Goal | Current |
|---|---|---|
| Kernel LOC | ≤ 200-300 | ~140 (`pond_minimal.py`) |
| Number of primitives | ≤ 3 unless admission rule satisfied | 3 |
| Kernel dependencies (workload-specific libs) | 0 | 0 (only stdlib) |
| View independence | Any View removable | Supported (Capability Independence Test) |
| Storage portability | FS, S3, Redis, Postgres, memory | FS + Postgres verified; others pending Stage 4 |
| Canonical copies | Exactly 1 durable representation | 1 (immutable blobs) |
| Capability leakage | 0 kernel mods per new View | 0 across 14 workloads |
| Long-term stability | API almost never changes | 3 primitives stable since v0.4 |

---

## The Kernel Admission Rule

A feature enters `pond_minimal.py` ONLY if ALL five criteria pass:

1. **Universal** — required by 3+ structurally different Views
2. **Impossible outside the kernel** — if a View can implement it, it stays out
3. **Immutable** — kernel tracks no mutable state except name → hash
4. **Storage-independent** — no knowledge of formats or workload types
5. **Decades-stable** — could Linux keep this syscall for 30 years?

See `prototype/ADMISSION_RULE.py` for the full rule with the feature audit table.

---

## The philosophy

> **One copy. Infinite execution. Zero coordination unless necessary.**

- **One copy** — exactly one durable canonical representation (immutable blobs).
  Everything else (caches, indexes, MVs) is derived and rebuildable.
- **Infinite execution** — capabilities compose without limit. SQL, streaming,
  vectors, ML, graph, future workloads all run over the same substrate.
- **Zero coordination unless necessary** — everything stays local (single-node,
  single-process) unless the system can prove coordination is required for
  correctness. The discipline behind SQLite, Git, TigerBeetle, DuckDB.

---

## What Pond is NOT

- Not another Iceberg (Pond's kernel has no table format)
- Not another Spark (Pond's kernel has no execution engine)
- Not another DuckDB (DuckDB is one View; the kernel is backend-agnostic)
- Not a microkernel (the OS analogy is bounded — kernel = storage, Views = engines)
- Not a Git clone (Tree/Commit are View patterns, not kernel primitives)

## What Pond IS

A **universal immutable object runtime** — the smallest storage algebra
we've found so far (Write + Read + Reference) from which SQL, vectors,
streaming, Git, graphs, ML, time-series, OCI registries, and alien workloads
(Minecraft, Blender, CAD, genome, medical imaging, Photoshop) all derive as
independent Views. The destruction phases are trying to falsify this claim.

---

## License

MIT (see LICENSE file when added). All prototype code is open.

## Contributing

This is a research prototype going through destruction testing. The most
valuable contribution is a workload, scale, or failure mode that breaks the
kernel — open an issue with the scenario and what kernel change it would
require.

---

## Repository layout

```
Pond/
├── README.md                              # this file
├── worklog.md                             # full research worklog
├── docs/
│   └── pond_rfc1_storage_and_versioned_state.pdf   # RFC 1 (formal spec)
├── scripts/
│   └── pond_rfc1.py                       # ReportLab script that generated the RFC
└── prototype/
    ├── README.md                          # detailed prototype findings
    ├── ADMISSION_RULE.py                  # 5-criterion kernel admission rule
    ├── pond.py                            # v0.1 original kernel (with format leaks)
    ├── pond_kernel.py                     # v0.2 kernel with Tree/Commit helpers
    ├── pond_minimal.py                    # v0.4 minimal 3-primitive kernel (~140 LOC)
    ├── views.py                           # SQL/Vector/Stream/Git Views (v0.2)
    ├── views_minimal.py                   # 8 Views on the minimal kernel
    ├── more_views.py                      # Graph/ML/TimeSeries/OCI Views
    ├── demo.py                            # end-to-end demo
    ├── benchmark.py                       # original benchmark suite
    ├── stress_1m_seals.py                 # exposed O(N²) flat-tree bug
    ├── stress_realistic_seals.py          # validated hierarchical tree at scale
    ├── bench_metadata_locality.py         # Finding 4: cold LIMIT 10 touches 4 objects
    ├── bench_s3_simulation.py             # Finding 5: time travel is O(depth)
    ├── bench_crash_consistency.py         # Finding 6: orphans accumulate, no GC
    ├── bench_concurrent_writers.py        # Finding 7: root store not thread-safe
    ├── bench_universality.py              # one blob, three Views, zero copies
    ├── bench_universality_v2.py           # 4 Views, 1 kernel
    ├── bench_universality_stress.py       # 8 Views, 0 kernel changes
    ├── bench_capability_independence.py   # Views pass in isolation
    ├── bench_minimality.py                # 8 Views work on 3 primitives
    ├── bench_minimality_extreme.py        # tries to remove each of the 3
    └── bench_identity.py                  # 8 adversarial identity experiments
```

---

## Key findings (honest, with caveats)

### What's been validated

- **3 primitives are sufficient** for 8 structurally different Views (SQL,
  Vector, Streaming, Git, Graph, ML, TimeSeries, OCI) — all run on the
  minimal kernel with zero modifications.
- **6 alien workloads** (Minecraft, Blender, CAD, Genome, PACS, Photoshop)
  built on Pond with zero kernel changes.
- **Storage independence** — the kernel runs on local FS and Postgres
  (simulated) with zero changes. Designed for S3/Redis/memory.
- **Anti-Iceberg** — kernel has zero dependencies on Parquet, Arrow, DuckDB,
  Iceberg, or Spark. If they all disappeared, the kernel survives unchanged.
- **Time test** — Write/Read/Reference would make sense in 2045, 2100, or
  any year. They encode nothing about today's technology.

### What's been falsified (and fixed)

- **O(N²) metadata growth** in flat-tree design (Finding 2) — fixed by
  hierarchical trees (Git model + delta chain).
- **Time travel is O(depth)** in S3 calls (Finding 5a) — needs skip
  pointers, not yet implemented.
- **No GC** — orphaned objects accumulate after crashes (Finding 6).
- **Root store not thread-safe** — SQLite thread-binding breaks concurrent
  writers (Finding 7).
- **Format leaks** in v0.1's `pond.py` (Parquet/Arrow hardcoded in `seal()`)
  — fixed in v0.2 by splitting kernel from Views.

### Honest caveats

- The 3-primitive kernel is an **empirical hypothesis**, not a proof. It's
  supported by 8 workloads + 6 alien workloads + 8 identity experiments.
  There may exist future workloads that require a fourth primitive.
- Views are **demos**, not production-quality.
- **Performance at scale** is untested on real S3 / PB-scale data.
- **Concurrency** is single-writer — the next milestone.
- **Replication** (Raft) is not yet implemented.

---

## Quick start

```bash
pip install duckdb pyarrow

# The minimal kernel + 8 Views
cd prototype
python3 bench_minimality.py           # 8 Views on 3 primitives

# Adversarial identity experiments
python3 bench_identity.py             # 8 experiments, honest verdicts

# The original end-to-end demo
python3 demo.py                       # write → seal → read → time travel → branch

# Stress tests (the ones that found real bugs)
python3 bench_universality_stress.py  # 8 Views, 0 kernel changes
python3 bench_capability_independence.py  # Views are decoupled
python3 bench_crash_consistency.py    # DAG never corrupts, but orphans accumulate
python3 bench_concurrent_writers.py   # root store not thread-safe (known issue)
```

---

## Architectural metrics

| Metric | Goal | Current |
|---|---|---|
| Kernel LOC | ≤ 200-300 | ~140 (`pond_minimal.py`) |
| Number of primitives | ≤ 3 unless admission rule satisfied | 3 |
| Kernel dependencies (workload-specific libs) | 0 | 0 (only stdlib) |
| View independence | Any View removable | ✓ |
| Storage portability | FS, S3, Redis, Postgres, memory | FS + Postgres verified |
| Canonical copies | Exactly 1 durable representation | 1 (immutable blobs) |
| Capability leakage | 0 kernel mods per new View | 0 across 14 workloads |
| Long-term stability | API almost never changes | 3 primitives stable since v0.4 |

---

## The Kernel Admission Rule

A feature enters `pond_minimal.py` ONLY if ALL five criteria pass:

1. **Universal** — required by 3+ structurally different Views
2. **Impossible outside the kernel** — if a View can implement it, it stays out
3. **Immutable** — kernel tracks no mutable state except name → hash
4. **Storage-independent** — no knowledge of formats or workload types
5. **Decades-stable** — could Linux keep this syscall for 30 years?

See `prototype/ADMISSION_RULE.py` for the full rule with the feature audit table.

---

## The philosophy

> **One copy. Infinite execution. Zero coordination unless necessary.**

- **One copy** — exactly one durable canonical representation (immutable blobs).
  Everything else (caches, indexes, MVs) is derived and rebuildable.
- **Infinite execution** — capabilities compose without limit. SQL, streaming,
  vectors, ML, graph, future workloads all run over the same substrate.
- **Zero coordination unless necessary** — everything stays local (single-node,
  single-process) unless the system can prove coordination is required for
  correctness. The discipline behind SQLite, Git, TigerBeetle, DuckDB.

---

## What Pond is NOT

- Not another Iceberg (Pond's kernel has no table format)
- Not another Spark (Pond's kernel has no execution engine)
- Not another DuckDB (DuckDB is one View; the kernel is backend-agnostic)
- Not a microkernel (the OS analogy is bounded — kernel = storage, Views = engines)
- Not a Git clone (Tree/Commit are View patterns, not kernel primitives)

## What Pond IS

A **universal immutable object runtime** — the smallest storage algebra
we've found so far (Write + Read + Reference) from which SQL, vectors,
streaming, Git, graphs, ML, time-series, OCI registries, and alien workloads
(Minecraft, Blender, CAD, genome, medical imaging, Photoshop) all derive as
independent Views.

---

## License

MIT (see LICENSE file when added). All prototype code is open.

## Contributing

This is a research prototype. The architecture is frozen; the next phase is
engineering (concurrency, replication, S3 backend, GC). If you find a
workload that breaks the kernel, that's the most valuable contribution
possible — open an issue with the workload and what kernel change it
would require.
