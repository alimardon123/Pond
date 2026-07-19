# RFC-0002: Elegance Metrics

## Status

Draft — defining how to measure elegance, not performance.

## Abstract

Pond's original goals include "beautiful architecture" and "simplicity."
These are currently unmeasured. This RFC defines elegance metrics that
align with those goals better than throughput or latency.

---

## Metrics

### M1: Lines of code per View (irreducible)

**What:** Strip a View to its irreducible translation layer (remove
convenience methods, error handling, documentation, caching). Count
the remaining lines.

**Target:** 50-200 lines for most Views. If one View is 500+ lines,
that's an ergonomics signal.

**Current:** (from the View Compression Study)
| View | Meaningful lines | Est. irreducible |
|---|---|---|
| SQLView | 403 | 70 |
| VectorView | 403 | 70 |
| StreamView | 403 | 55 |
| GitView | 403 | 55 |
| GraphView | 332 | 115 |
| MLView | 332 | 100 |
| TimeSeriesView | 332 | 70 |
| OCIView | 332 | 85 |

**Assessment:** all Views are within the 50-200 range (after stripping).
GraphView is the highest (115) due to adjacency index construction.

---

### M2: Kernel calls per operation

**What:** Count how many kernel primitives (Write, Read, Reference)
each View operation requires.

**Target:** minimal. Write/Read/Reference should be the ONLY storage calls.

**Current:**
| Operation | Write | Read | Reference |
|---|---|---|---|
| Commit (any View) | 3 (blob + tree + commit) | 1 (read parent) | 1 |
| Read latest | 0 | 3 (commit + tree + blob) | 0 |
| Branch | 0 | 0 | 1 |
| Snapshot | 0 | 0 | 0 (just record the hash) |

**Assessment:** 0-5 kernel calls per operation. Minimal.

---

### M3: Concepts exposed to View authors

**What:** How many concepts must a View author understand?

**Target:** the kernel API (3 operations) + the laws (5 storage + 7 composition) + the View Author's Guide (6 guarantees + 7 conventions). That's 3 + 12 + 13 = 28 concepts.

**Current:** measured by the independent implementation challenge. The
fresh agent needed to understand 3 primitives + 12 laws. The 10
ambiguities show where the spec was insufficient.

**Assessment:** 28 concepts is manageable. Compare: Spark has ~100+
concepts (RDD, DataFrame, Dataset, Catalyst, Tungsten, Structured
Streaming, etc.).

---

### M4: Boilerplate (code repeated across Views)

**What:** How much code is duplicated across Views?

**Target:** minimal. If every View reimplements the same pattern
(Tree+Commit), that's boilerplate.

**Current:** every View reimplements:
- Tree pattern (write_tree, read_tree) — ~10 lines
- Commit pattern (write_commit, read_commit) — ~10 lines
- Parent inheritance logic — ~5 lines

Total boilerplate per View: ~25 lines.

**Assessment:** 25 lines is low. A shared library (`pond_view_helpers.py`)
could eliminate it. Currently classified as **Ergonomics**.

---

### M5: Duplicate logic across Views

**What:** Beyond boilerplate, how much logic is reinvented?

**Target:** minimal. If Views reimplement the same algorithm (e.g.,
reachability walk for GC, skip pointers for time travel), that's
duplicate logic.

**Current:**
- GC: implemented once as PondGC (View-level utility)
- Skip pointers: not yet implemented (any View needing time travel
  would reimplement)
- Index patterns: 5 Views need indexes, each reimplements

**Assessment:** index patterns are the biggest source of duplication.
A shared index library (View-level) would help. Classified as
**Ergonomics**.

---

### M6: Cognitive complexity

**What:** How hard is it to understand a View?

**Target:** a View author should be able to read the spec and
implement a View in a few hours.

**Current:** the independent implementation challenge took one agent
a single session to implement GitView. The 10 ambiguities required
guessing, but the implementation was correct.

**Assessment:** a few hours is reasonable. The 10 ambiguities (now
documented) would reduce this further.

---

### M7: Reinvention frequency

**What:** How often do View authors reinvent the same abstractions?

**Target:** low. If 3+ Views reinvent the same thing, it should be
a shared library.

**Current:**
- Tree+Commit pattern: reinvented 8/8 Views → should be shared
- GC reachability walk: reinvented 0/8 (shared PondGC) → good
- Index patterns: reinvented 5/8 → should be shared
- Skip pointers: reinvented 0/8 (not yet needed) → TBD

**Assessment:** Tree+Commit and index patterns should be shared
libraries. This is **Ergonomics**, not Architecture.

---

## Summary

| Metric | Current | Target | Status |
|---|---|---|---|
| M1: Irreducible LOC per View | 55-115 | 50-200 | ✓ within target |
| M2: Kernel calls per operation | 0-5 | minimal | ✓ minimal |
| M3: Concepts exposed | 28 | <30 | ✓ manageable |
| M4: Boilerplate per View | ~25 lines | <50 | ✓ low |
| M5: Duplicate logic | indexes (5 Views) | low | ⚠ needs shared library |
| M6: Cognitive complexity | few hours | <1 day | ✓ reasonable |
| M7: Reinvention frequency | Tree+Commit (8/8), indexes (5/8) | low | ⚠ needs shared libraries |

**Overall:** elegance is good. Two areas need improvement (shared
index library, shared Tree+Commit helpers) — both are Ergonomics,
not Architecture.
