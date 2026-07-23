# Pond

> *One copy of data on object storage, serving all workloads without
> duplication, with no JVM, no Spark, no Iceberg-style metadata explosion.*

Pond is a **minimal immutable object runtime** — not another lakehouse,
not another table format, not another Spark.

The core hypothesis: a tiny storage kernel (3 operations on 6 substrates,
~140 LOC) is sufficient for radically different workloads — SQL, vectors,
streaming, Git, graphs, ML, time-series — to be implemented as
independent **Lenses** over a shared immutable substrate.

---

## The Kernel

The kernel owns three operations. Nothing else.

| Operation | Description |
|---|---|
| `Write(bytes) → hash` | Create immutable, content-addressed blob. Same bytes → same hash. Dedup is free. |
| `Read(hash) → bytes` | Fetch blob by hash (or by name). |
| `Ref(name, hash) → ()` | Mutable name→hash mapping. The **only** mutation in the system. |

~140 lines of code. **FROZEN.** No codec registry. No envelope. No manifest.
No query planner. No consensus. The kernel stores and retrieves bytes; it
knows nothing about what the bytes mean.

[→ Read the whitepaper](docs/POND_WHITEPAPER.md) for the full architecture.

---

## The Layer Hierarchy

```
Applications                ← SQL, Git, Feature Store, Notebook, Lakehouse
    ↓
Lenses                      ← Interpretation (encode/decode; code, not data)
    ↓
Physical Structures         ← Acceleration (indexes, stats — deterministic)
    ↓
Collections                 ← Named objects with namespace
    ↓
Kernel                      ← Bytes + History + Names (FROZEN, ~140 LOC)
    ↓
Backend                     ← Local disk, S3, IPFS, FoundationDB, …
```

Dependencies flow downward only. Each layer adds exactly one capability.
No layer leaks upward. The kernel never changes.

---

## The 7 Design Principles

Every architectural decision must serve these (see [DESIGN_GOALS.md](DESIGN_GOALS.md) §3):

1. **Simple** — the kernel stays intellectually small (~140 LOC).
2. **Powerful** — rich behavior emerges from composition.
3. **Performant** — optimizations live above the core.
4. **Scalable** — Lenses and Physical Structures evolve independently.
5. **Efficient** — immutable data + rebuildable derived metadata.
6. **Beautiful** — one responsibility per layer; dependencies flow downward.
7. **Functional** — Pond must do everything users actually need (via Lenses).

---

## What's in this repo

| Directory | Purpose |
|---|---|
| [`pond-core/`](pond-core/) | The kernel (FROZEN, ~140 LOC). 3 primitives: Write, Read, Ref. |
| [`pond-sdk/`](pond-sdk/) | Lens SDK: ProllyViewBase, Lens base class, indexes, query API. |
| [`lenses/`](lenses/) | Lens implementations: **lakehouse** (DuckDB, flagship), **vector** (ANN search). |
| [`services/`](services/) | Cross-cutting: **transport** (compression+encryption), **schema** (registry), **replication** (coordinator). |
| [`pond-labs/`](pond-labs/) | Experiments: feature_store_lens, interop_demo (killer demo), loc_benchmark. |
| [`docs/`](docs/) | Whitepaper, formal algebras, benchmarks, where-Pond-fails, lens guide. |
| [`scripts/`](scripts/) | Test suites (646 checks) and benchmarks. |
| [`tla/`](tla/) | TLA+ formal specification (6 invariants, 56 reachable states). |
| [`archive/`](archive/) | Historical code and docs (preserved for reference). |

---

## Quick start

```bash
# Run the flagship: DuckDB lakehouse on Pond
python lenses/lakehouse/lakehouse.py

# Run the killer demo: bidirectional Lens interop
python pond-labs/interop_demo.py

# See the LOC saved (81% reduction vs building from scratch)
python pond-labs/loc_benchmark.py

# Run the 646 verification checks
python scripts/phase_l_property_tests.py    # 491 property tests
python scripts/phase_l_differential_git.py  # 45 Git differential tests
python scripts/phase_n_untested_laws.py     # 23 merge + workspace tests
python scripts/phase_o_remaining_laws.py    # 48 remaining law tests
```

---

## The honest scope

Pond is not a universal storage substrate today. It excels at:

- **Versioned tabular data** (lakehouse) — the flagship
- **ML feature stores** — point-in-time joins, branching, time travel
- **Audit logs / event sourcing** — immutability is native
- **Configuration management** — branching for environment promotion

Pond struggles at (but has a Lens roadmap to fix):
- High-frequency OLTP, distributed consensus, hot-key contention,
  streaming joins, GPU data, millions of tiny objects, full-text search

For each struggle, there is a Lens design that closes the gap. See
[docs/WHERE_POND_FAILS.md](docs/WHERE_POND_FAILS.md) for the full
mapping.

**The honest, ambitious claim:** Pond's kernel is too small to do any
single workload optimally. But the Lens algebra is rich enough to do
every workload competitively, plus give every workload free time travel,
branching, and cross-Lens interop that no peer system provides.

---

## Reading order (for new contributors)

1. **This file** — 5-minute intro.
2. [docs/POND_WHITEPAPER.md](docs/POND_WHITEPAPER.md) — the contribution (20 pages).
3. [docs/WHERE_POND_FAILS.md](docs/WHERE_POND_FAILS.md) — honest scope + Lens roadmap.
4. [docs/LENS_GUIDE.md](docs/LENS_GUIDE.md) — how to write a Lens.
5. [DESIGN_GOALS.md](DESIGN_GOALS.md) — 7 design principles + roadmap.

---

## In one sentence

> Pond is an immutable object-store kernel built from three primitive
> operations (write, read, reference). Everything else — versioning,
> schemas, replication, transport, indexes, lenses, views — is
> implemented as layers above the kernel rather than embedded inside it.
