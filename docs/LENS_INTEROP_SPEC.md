# View Interop Spec — What the Laws Do NOT Cover

This document is derived directly from the Independent Implementation
Challenge (destruction/V_independent/). A fresh agent implemented a
GitView from the formal spec alone and identified 10 ambiguities.

Each ambiguity is now explicitly classified: is it a kernel gap, a
convention, or intentionally unspecified?

This document exists so future View authors do not accidentally assume
more than the kernel guarantees.

---

## The 10 Ambiguities

### 1. HEAD / "current branch" tracking

**Status:** Intentionally unspecified (U8 in View Author's Guide)

**Problem:** The namespace maps `name → hash` only. There is no
`name → name` indirection. How does a View record "currently on
branch main"?

**Resolution:** View-level convention. The independent implementation
invented HEAD-as-immutable-object: store `{"type":"head","branch":"refs/heads/main"}`
as a blob, bind the name `"HEAD"` to it. Each checkout writes a new
HEAD object and re-points `HEAD`.

**Why not in the kernel:** HEAD is a version-control concept. OCI
Views don't have HEAD. ML Views don't have HEAD. TimeSeries Views
don't have HEAD. Adding HEAD to the kernel would bake Git semantics
into the substrate.

---

### 2. resolve vs read

**Status:** Minor API inconsistency

**Problem:** The formal spec lists 3 operations (Write, Read, Reference).
The task brief also mentions `resolve(name) → hash`. Is `resolve` part
of the API or a convenience?

**Resolution:** `resolve` is a convenience method, not a primitive.
It's equivalent to "what hash does this name currently point to?"
without reading the bytes. Views can derive it from `Read` (read by
name, the kernel resolves internally), but having `resolve` separately
avoids unnecessary blob reads.

**Recommendation:** include `resolve` in the API documentation as a
convenience method, not a law.

---

### 3. Root namespace scope

**Status:** Intentionally unspecified (partially addressed by U12)

**Problem:** Is the namespace flat global? Hierarchical? Are there
reserved names?

**Resolution:** The namespace is flat global. There are no reserved
names (though Views should avoid collisions via naming conventions like
`refs/heads/*`). Hierarchy is a View-level concern (encode structure
in the name string: `refs/heads/main`, `tenant_A/orders`).

**Why not in the kernel:** hierarchical namespaces impose a structure
that not all Views want. Flat + convention is more flexible.

---

### 4. Object format

**Status:** Intentionally unspecified (U1)

**Problem:** The kernel stores bytes. JSON? Protobuf? Custom binary?
Two Views using different formats cannot read each other's objects.

**Resolution:** View-level choice. The convention (C1) is JSON for
readability, but Views MAY use any format. There is no "Pond object
format" — each View defines its own.

**Why not in the kernel:** format specification would couple the kernel
to a serialization library. The kernel is bytes-only by design.

---

### 5. Tree structure

**Status:** Intentionally unspecified (U2)

**Problem:** Flat path→blob map vs. nested directory trees?

**Resolution:** View-level choice. Convention (C3) is flat (simpler).
Git uses nested (more efficient for subdirectory operations). Both work
on the kernel. Views choose based on workload needs.

**Why not in the kernel:** tree structure is a workload concern. SQL
Views might not use trees at all (just name → latest blob). Git Views
might use nested trees. Graph Views use adjacency lists. No universal
tree structure exists.

---

### 6. Staging area / index

**Status:** Intentionally unspecified (U3)

**Problem:** Is staging kernel state or View state?

**Resolution:** View state. Typically in-memory (a dict of pending
changes). The kernel does not persist staging state. Views that need
persistent staging implement it as a blob.

**Why not in the kernel:** staging is a version-control concept. OCI
Views don't stage (they push layers directly). Streaming Views don't
stage (they append to OPEN objects). Adding staging to the kernel
would bake Git semantics into the substrate.

---

### 7. Error semantics

**Status:** Intentionally unspecified (U4)

**Problem:** How are errors delivered? Exceptions? Result types?
What are the exact failure modes?

**Resolution:** The kernel names error conditions (`NOT_FOUND`,
`HASH_NOT_FOUND`) but the representation is implementation-defined.
Views wrap kernel calls in their own error handling.

**Why not in the kernel:** error representation is language-dependent.
Python uses exceptions. Rust uses Result. Go returns (value, error).
The kernel specifies conditions, not representation.

---

### 8. Multi-parent / merge semantics

**Status:** Intentionally unspecified (U7)

**Problem:** Can commits have multiple parents? How does merge work?

**Resolution:** The kernel allows multi-parent commits (parents is a
list in the commit blob — the kernel stores it opaquely). Merge
semantics are View-level: read both parents' trees, resolve conflicts,
write a new merged tree + commit.

**Why not in the kernel:** different workloads merge differently.
Git uses 3-way merge. CRDTs use commutative merge. SQL uses UNION.
No universal merge exists.

---

### 9. Author / timestamp / identity

**Status:** Intentionally unspecified (U9)

**Problem:** The kernel doesn't track author, timestamp, or identity.

**Resolution:** Views add these fields to their commit blobs. The
kernel stores them opaquely as bytes.

**Why not in the kernel:** not all Views need author/timestamp. OCI
manifests have different metadata. ML checkpoints have different
metadata. Adding fixed metadata fields to the kernel would impose
a schema that not all Views want.

---

### 10. Checkout semantics

**Status:** Intentionally unspecified (U10)

**Problem:** What does checkout do? Is there a working tree?

**Resolution:** The kernel has no checkout or working tree concept.
A View's checkout is: resolve branch name to commit hash, update HEAD
(convention C6), present the commit's tree to the user. The "working
tree" is whatever the View presents.

**Why not in the kernel:** checkout is a version-control concept.
OCI Views don't checkout. Streaming Views don't checkout. Adding
checkout to the kernel would bake Git semantics into the substrate.

---

## Summary Classification

| # | Ambiguity | Classification | Why |
|---|---|---|---|
| 1 | HEAD tracking | Unspecified (U8) | Git-specific; not universal |
| 2 | resolve vs read | Minor API inconsistency | Include resolve as convenience |
| 3 | Namespace scope | Unspecified (flat global) | Hierarchy is View concern |
| 4 | Object format | Unspecified (U1) | Format coupling is Iceberg-like |
| 5 | Tree structure | Unspecified (U2) | No universal tree structure |
| 6 | Staging area | Unspecified (U3) | Git-specific; not universal |
| 7 | Error semantics | Unspecified (U4) | Language-dependent |
| 8 | Merge semantics | Unspecified (U7) | No universal merge |
| 9 | Author/timestamp | Unspecified (U9) | Not all Views need it |
| 10 | Checkout semantics | Unspecified (U10) | Git-specific; not universal |

**8 of 10 are intentionally unspecified (by design).**
**1 is a minor API inconsistency (resolve).**
**1 is a convention recommendation (HEAD-as-object).**

None of the 10 are kernel gaps. All are correctly left to Views.
This document makes that explicit so future View authors don't
assume more than the kernel guarantees.
