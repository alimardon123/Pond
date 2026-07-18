# Adversarial View Design — Summary

The goal was NOT to prove Views exist. It was to find where the kernel
forces unnatural implementation patterns. The method: View Compression
(strip each View to irreducible translation) and friction analysis
(classify each responsibility as kernel issue / View issue / acceptable).

## What the friction analysis found

### 5 kernel friction points (clustered around indexes)

| View | Friction | Classification |
|---|---|---|
| VectorView | Linear scan for ANN (no HNSW/IVF) | Kernel issue — no index primitive |
| GitView | O(N) history walk (no skip pointers) | Kernel issue — but Finding 5a says View-level |
| GraphView | Builds own adjacency index | Kernel issue — no index primitive |
| MLView | O(N) checkpoint history walk | Kernel issue — same as Git |
| TimeSeriesView | O(N) segment walk | Kernel issue — same pattern |

**The pattern:** 4 of 5 friction points are "O(N) walk because no index."
The 5th (VectorView ANN) is "linear scan because no vector index."

### The question this raises: is "index" a missing kernel primitive?

Currently the kernel has:
- Write (create immutable blob)
- Read (fetch blob by hash/name)
- Reference (mutable name → hash)

Views that need fast lookup (by path, by step, by timestamp, by vector
similarity) must build their own indexes as Trees. This works (Views
CAN exist) but forces every View to reinvent indexing.

**Should the kernel admit an index primitive?**

Apply the 5-criterion Admission Rule:
1. **Universal?** 5 of 8 Views need indexes (Vector, Git, Graph, ML, TimeSeries). Passes.
2. **Impossible outside kernel?** Views CAN build indexes as Trees. Fails.
3. **Immutable?** Indexes are derived (rebuildable from data). Passes.
4. **Storage-independent?** Yes — an index is just a structure over blobs. Passes.
5. **Decades-stable?** Yes — "fast lookup by key" is timeless. Passes.

**Verdict: 4 of 5 pass. Fails criterion 2 (Views can build indexes).**

Per the Admission Rule, index stays OUT of the kernel. Views build their
own. But this means every View that needs fast lookup reinvents the same
pattern (Tree as index). This is the friction the study found.

### Two ways to resolve the friction

**Option A: Keep index out of the kernel (current).**
- Each View builds its own index as a Tree pattern.
- Friction: duplicated index logic across Views.
- Benefit: kernel stays minimal; Views choose index structure.

**Option B: Provide a shared "index View" as a library.**
- Not a kernel primitive — a shared library that Views import.
- Provides common index patterns (B-tree, hash, HNSW) as reusable code.
- Friction reduced: Views import instead of reinvent.
- Kernel unchanged.

**Recommendation: Option B.** The friction is real but doesn't justify a
kernel change (criterion 2 fails). A shared index library (View-level,
not kernel-level) resolves the duplication without growing the kernel.

## View Compression results

| View | Meaningful lines | Est. irreducible | Friction ratio |
|---|---|---|---|
| SQLView | 403 | 70 | 5.8x |
| VectorView | 403 | 70 | 5.8x |
| StreamView | 403 | 55 | 7.3x |
| GitView | 403 | 55 | 7.3x |
| GraphView | 332 | 115 | 2.9x |
| MLView | 332 | 100 | 3.3x |
| TimeSeriesView | 332 | 70 | 4.7x |
| OCIView | 332 | 85 | 3.9x |

**Interpretation:** friction ratios are 2.9x to 7.3x. The Views have
3-7x more code than the irreducible estimate. This is mostly View-issue
(buffering, serialization, error handling) rather than kernel-issue.

**The outlier:** GraphView has the highest irreducible estimate (115
lines) because it has 9 responsibilities, including adjacency index
construction. This is the View with the most kernel friction — it's
building infrastructure (indexes) that other Views also need.

## What this changes about the architecture

**No kernel changes needed.** The friction points are real but don't
pass the Admission Rule. The kernel stays at 3 primitives.

**A shared index library is the recommended fix.** Not a kernel primitive
— a View-level library (`pond_index.py`) that provides common index
patterns. Views import it instead of reinventing. This is engineering
work, not architectural change.

**Finding 5a (O(N) time travel) is confirmed as the main friction.** It
appears in 4 of 5 kernel friction points. The fix (skip pointers) is
View-level, not kernel-level. A shared library could provide skip-pointer
infrastructure that Git/ML/TimeSeries Views import.

## What this does NOT change

- The 3-primitive kernel (Write/Read/Reference) is unchanged.
- The 5 laws (immutability, addressability, name-mutability, references-don't-mutate, backend-independence) are unchanged.
- The Admission Rule is unchanged (index fails criterion 2).
- The rejected designs stay rejected.

## Honest caveat

This study used estimated irreducible sizes (15 lines per responsibility).
A true compression study would actually strip each View to its minimum
and measure. The estimates are approximate; the friction ratios are
directional, not precise.

The friction findings (5 kernel points, clustered around indexes) are
the real output. The line counts are supporting evidence, not the
conclusion.

## Next steps

1. **Build the shared index library** (`pond_index.py`) — View-level,
   not kernel. Provides B-tree, hash, skip-pointer patterns. Views import.
2. **Actually strip Views to irreducible** — measure, don't estimate.
3. **Independent implementation challenge** — have another AI implement
   GitView using only the formal spec (laws), not the existing code.
   Compare. Convergence = laws are sufficient; divergence = laws underspecified.
4. **Composition laws** — expand the formal spec beyond storage laws to
   algebraic properties (reference chains, GC reachability, backend substitution).
