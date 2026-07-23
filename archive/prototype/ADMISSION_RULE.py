"""
Pond Kernel Admission Rule (architectural law, not implementation).

A feature enters the storage kernel ONLY if it satisfies ALL five criteria.
If any criterion fails, the feature belongs in a Lens, not the kernel.

This file is documentation, not code. It exists to be referenced in code
review: every PR that touches pond_kernel.py must answer "which criterion
of the Admission Rule justifies this change?"

The five criteria:

  1. UNIVERSAL
     Required by almost every View (SQL, Vector, Streaming, Git, and
     future graph/ML/document/OCI/time-series/etc. Views).
     Test: would 3+ structurally different Views use this feature?
     If only one View needs it, it doesn't belong in the kernel.

  2. IMPOSSIBLE OUTSIDE THE KERNEL
     If a Lens can implement the feature using existing kernel syscalls,
     the feature stays out of the kernel.
     Test: can I implement this as a Tree pattern, a Commit convention,
     or a Lens-level cache? If yes, it's a Lens concern.

  3. IMMUTABLE
     The kernel never owns mutable semantics. Mutable state (name -> hash
     mappings in the root namespace) is the only exception, and it's the
     smallest possible mutable surface.
     Test: does this feature require the kernel to track changing state
     beyond name -> hash? If yes, it's a Lens concern.

  4. STORAGE-INDEPENDENT
     The kernel cannot know about: Arrow, Parquet, Delta, Iceberg, JSON,
     protobuf, SQL, vectors, rows, columns, events, tables, schemas,
     images, audio, video, model weights, edges, nodes, layers, segments.
     Test: does this feature reference any format or workload type? If
     yes, it's a Lens concern.

  5. DECADES-STABLE
     Could Linux keep this syscall for 30 years? If not, it doesn't belong.
     Test: is this feature likely to be obsoleted by hardware, format, or
     workload evolution in the next decade? If yes, it's a Lens concern.

---

Applying the rule to current kernel features:

  | Feature                  | 1.Universal | 2.Outside | 3.Immutable | 4.Storage-indep | 5.Decades | Verdict |
  |--------------------------|-------------|-----------|-------------|-----------------|-----------|---------|
  | Read(blob_hash)          | Y           | Y         | Y           | Y               | Y         | KERNEL  |
  | Write(handle, bytes)     | Y           | Y         | Y           | Y               | Y         | KERNEL  |
  | Seal(handle) -> hash     | Y           | Y         | Y           | Y               | Y         | KERNEL  |
  | Reference(name, hash)    | Y           | Y         | Y (mutable) | Y               | Y         | KERNEL  |
  | Tree (entries dict)      | Y           | Y         | Y           | Y               | Y         | KERNEL  |
  | Commit (tree+parent+ts)  | Y           | Y         | Y           | Y               | Y         | KERNEL  |
  | Hierarchical trees       | Y           | N (could  | Y           | Y               | Y         | KERNEL  |
  | (subtree compaction)     |             | be View)  |             |                 |           | (just.) |
  | Skip pointers for time   | N           | Y         | Y           | Y               | ?         | VIEW    |
  | travel                   | (only SQL+  |           |             |                 |           |         |
  |                          |  Git need)  |           |             |                 |           |         |
  | Vector index (HNSW)      | N           | Y         | N (mutable  | N (vectors)     | ?         | VIEW    |
  |                          |             |           | graph)      |                 |           |         |
  | Parquet read             | N           | Y         | Y           | N (Parquet)     | ?         | VIEW    |
  | Schema registry          | N           | Y         | N (mutable  | N (SQL)         | ?         | VIEW    |
  |                          |             |           | schema)     |                 |           |         |
  | Compaction policy        | N           | Y         | N (trigger  | Y               | ?         | VIEW    |
  |                          |             |           | state)      |                 |           |         |

---

The two close calls:

  * Hierarchical trees (subtree compaction): Could arguably be a Lens
    concern (a Lens could implement its own tree-walk caching). But every
    View that builds a Tree benefits from hierarchy, so it's universal.
    Admitted to kernel — but watch for drift.

  * Skip pointers for time travel: Only SQL and Git Views need time
    travel; Vector, Streaming, Document, OCI don't. Fails criterion 1.
    Stays in View (each View that needs it implements its own skip list
    as a Tree pattern).

---

The rule in one sentence:

  > If a Lens can do it, a Lens must do it.
"""


# This file is documentation. No code.
KERNEL_ADMISSION_RULE = """
A feature enters pond_kernel.py ONLY if ALL five criteria pass:

1. UNIVERSAL        — required by 3+ structurally different Views
2. IMPOSSIBLE OUTSIDE — cannot be implemented as a Tree pattern or View cache
3. IMMUTABLE        — kernel tracks no mutable state except name -> hash
4. STORAGE-INDEPENDENT — no knowledge of formats or workload types
5. DECADES-STABLE   — could Linux keep this syscall for 30 years?

If any criterion fails, the feature belongs in views.py.
"""
