# Problem Taxonomy

All issues, findings, and observations in Pond are classified into
exactly one of these categories. This replaces the ad-hoc "Finding N"
numbering and makes the project easier to understand.

---

## Categories

### Architecture
The kernel's laws, primitives, or composition properties are insufficient
or wrong. Fixing this requires changing the kernel.

**Examples:** a workload that cannot be expressed with 3 primitives;
a law that doesn't hold under adversarial conditions.

**Action:** kernel change (requires passing the 5-criterion Admission Rule
AND disproving the lower-bound proof).

---

### Specification
The laws are correct but the formal spec is ambiguous, incomplete, or
misleading. Fixing this requires clarifying the spec, not changing the kernel.

**Examples:** the 10 ambiguities from the independent implementation
challenge (HEAD tracking, object format, tree structure, etc.).

**Action:** update FORMAL_SPEC.md, LENS_AUTHORS_GUIDE.md, or
LENS_INTEROP_SPEC.md.

---

### Engineering
The implementation has a bug, missing feature, or performance issue.
The architecture is correct; the code is wrong or incomplete.

**Examples:** GC not implemented (was "Finding 6"); SQLite thread-binding
(was "Finding 7"); S3 backend not yet implemented (was "unvalidated").

**Action:** implement the feature or fix the bug in the engineering/
directory. No kernel change.

---

### Performance
The architecture is correct and the implementation works, but the
performance is unacceptable. The fix is optimization, not redesign.

**Examples:** time travel is O(N) without skip pointers (was "Finding 5a");
linear scan for vector search; sequential S3 GETs.

**Action:** implement the optimization at the View level (skip pointers,
HNSW index, parallel GETs). No kernel change.

---

### Ergonomics
The architecture works but the developer experience is awkward. The
View author has to write too much boilerplate or reinvent the same
patterns repeatedly.

**Examples:** every View reimplements the Tree+Commit pattern; no shared
index library; no standard serialization format.

**Action:** create shared libraries (View-level, not kernel-level).
Document common patterns in the View Author's Guide.

---

### Research
The question is open. We don't know the answer yet. More research
(formal reasoning, experiments, or independent implementations) is needed.

**Examples:** can Views compose? Is the 5-tuple View definition minimal?
Is Pond genuinely different from Git/Irmin/Dolt, or just a cleaner API?

**Action:** design experiments, write RFCs, attempt proofs.

---

### Out of Scope
The issue is real but Pond deliberately does not solve it. See NON_GOALS.md.

**Examples:** SQL optimizer, distributed consensus, vector search engine,
query planner, transactions, schema registry, cache, index, scheduler,
authorization, compression, replication, streaming engine.

**Action:** none. Document in NON_GOALS.md. Do not add to kernel.

---

## Reclassification of Existing Findings

| Old Label | Category | Description |
|---|---|---|
| Finding 2 (flat tree O(N²)) | **Engineering** (fixed) | Hierarchical tree implemented |
| Finding 5a (time travel O(N)) | **Performance** | Needs View-level skip pointers |
| Finding 6 (no GC) | **Engineering** (fixed) | PondGC implemented |
| Finding 7 (SQLite thread-binding) | **Engineering** | Needs thread-safe root store or FDB backend |
| CAS candidate | **Architecture** (rejected) | Fails universality criterion |
| Multi-tenancy | **Architecture** (rejected) | Solved at View level (capability tokens) |
| 10 ambiguities | **Specification** | Documented in LENS_INTEROP_SPEC.md |
| Index friction (5 Views) | **Ergonomics** | Needs shared index library (View-level) |
| View composition | **Research** | Open question in RFC-0001 |
| Is Pond novel? | **Research** | Equivalence analysis says "no" (isomorphic to Git/IPFS/Irmin/Dolt/LakeFS) |
| Raft / replication | **Out of Scope** | Not yet; research first |
| SQL optimizer | **Out of Scope** | View concern, not kernel |
