# Pond RFC Index

Pond uses RFCs for stable architectural documents. Each RFC is a
specification, not an experiment log. Once accepted, an RFC is the
authoritative source for its topic.

## Active RFCs

| RFC | Title | Status |
|---|---|---|
| RFC-0001 | What Is a Lens? | Draft (superseded by RFC-0007) |
| RFC-0002 | Elegance Metrics | Draft |
| RFC-0003 | Kernel Specification (Frozen) | **Accepted** |
| RFC-0004 | View Composition and Interoperability | Draft |
| RFC-0005 | Materialization Calculus (renamed from Derived Structures) | Draft |
| RFC-0006 | Layered Architecture | Draft |
| RFC-0007 | View Algebra | **Accepted** (verified by `lens_laws.py` CI harness) |
| RFC-0008 | Deletion as Data | Draft |
| RFC-0009 | Architecture Metrics | Draft |
| RFC-0010 | ArrowView (Phase D Compatibility Adapter) | **Accepted** (verified by `pond-arrow/run_arrow_lens_laws.py`) |
| RFC-0011 | Feature Store (Phase E Flagship) | **Accepted** (verified by `pond-feature-store/feature_store.py` production tests) |
| RFC-0012 | The Lens Architecture | **Accepted** (context-based interpretation; verified by falsification test) |
| RFC-0013 | The Lens Interpretation Contract | **Accepted** (formal contract; verified by falsification test) |

## Reference Documents (not RFCs — supporting material)

| Document | Purpose |
|---|---|
| `DESIGN_GOALS.md` (top-level) | Six design principles + repo map; **read this first** |
| `FORMAL_SPEC.md` | 5 storage laws + 7 composition laws + preconditions/postconditions |
| `FORMAL_ALGEBRA.md` | Mathematical definition + 8 theorems + lower-bound proof + equivalence analysis |
| `LENS_AUTHORS_GUIDE.md` | 6 guarantees + 7 conventions + 12 unspecified (the Lens boundary) |
| `LENS_INTEROP_SPEC.md` | 10 ambiguities from independent implementation, classified |
| `REJECTED_DESIGNS.md` | 15+ rejected architectural decisions with reasons |
| `NON_GOALS.md` | 15 things Pond deliberately does NOT solve |
| `PEER_COMPARISON.md` | vs Git, Irmin, IPFS, LakeFS, FDB, Dolt |
| `PROBLEM_TAXONOMY.md` | 7 categories for classifying all issues |

## RFC Process

1. **Draft:** proposed, not yet reviewed
2. **Accepted:** reviewed and adopted as the authoritative spec
3. **Superseded:** replaced by a later RFC
4. **Rejected:** reviewed and not adopted (see REJECTED_DESIGNS.md)

Kernel changes require a new RFC that:
- Shows a workload that cannot be expressed with 3 primitives
- Passes the 5-criterion Admission Rule
- Disproves the lower-bound proof (FORMAL_ALGEBRA.md section 3)

Until such an RFC is accepted, the kernel stays frozen.
