# View Author's Guide — A Contract Document

This is NOT a tutorial. It is a **contract** that specifies the boundary
between the kernel and Views: what the kernel guarantees, what is convention,
and what is intentionally left unspecified.

If you are writing a Lens, read this before writing any code. If you find
yourself assuming something not listed here as a guarantee, your View is
depending on behavior the kernel does not promise.

---

## Part 1: What the Kernel GUARANTEES

These are the invariants the kernel upholds. Views can rely on these
unconditionally.

### G1: Content-addressed immutability
Once `Write(data)` returns hash `h`, the bytes at `h` are permanently
stored and never change. `Read(h)` will return exactly `data` for the
lifetime of the kernel instance. No operation can modify, overwrite,
or delete the bytes at `h`.

### G2: Deduplication
`Write(data)` called twice with the same `data` returns the same `h`.
The bytes are stored once. This is a side effect of content-addressing,
not a separate mechanism.

### G3: Name resolution
`Read(name)` resolves the name to its current hash via the root
namespace, then returns the bytes at that hash. The resolution is
atomic with respect to the read: once a name is resolved to a hash,
the read sees that hash's bytes even if another writer updates the
name concurrently.

### G4: Last-writer-wins on Reference
`Reference(name, h)` atomically updates `name` to point to `h`. If
another writer called `Reference(name, h2)` concurrently, one wins
and the other's update is lost. The kernel does NOT detect or report
lost updates.

### G5: Hash existence check
`Reference(name, h)` fails with `HASH_NOT_FOUND` if `h` was never
returned by `Write`. A name cannot point to a nonexistent hash.

### G6: Backend independence
The same sequence of `Write` and `Reference` operations produces the
same hashes and the same name→hash mappings on any backend. The
kernel does not require rename, append, seek, directories, or
filesystem semantics.

---

## Part 2: What is CONVENTION (not guaranteed, but recommended)

These are patterns that the existing Views use. They are NOT enforced
by the kernel. A View that follows these conventions will be more
compatible with other Views, but the kernel does not require them.

### C1: JSON serialization for metadata
Trees and commits are typically serialized as JSON. This is readable,
debuggable, and sufficient for most workloads. Views MAY use protobuf,
MessagePack, or custom binary formats instead — but they won't be
readable by other Views.

### C2: Self-typing envelopes
Objects that are NOT raw data blobs (e.g., trees, commits) typically
include a `"type"` field in their JSON:
```json
{"type": "tree", "entries": {"file.txt": "abc123..."}}
{"type": "commit", "tree": "def456...", "parents": ["ghi789..."], ...}
```
This lets a Lens distinguish object kinds when reading by hash. Views
MAY use a different discriminator or none at all.

### C3: Flat tree (path → blob hash)
Trees typically map file paths directly to blob hashes:
```json
{"type": "tree", "entries": {"dir/file.txt": "abc123..."}}
```
This is simpler than Git's nested trees but doesn't support efficient
subdirectory operations. Views MAY use nested trees if needed.

### C4: Full-snapshot commits
Each commit's tree contains ALL files at that commit (inherited from
parent + staged changes). This is simpler than delta-based commits
but uses more metadata. Content-addressing still dedups identical
blobs across commits.

### C5: Ref naming convention
Branches and tags use prefixed names to avoid collisions:
- `refs/heads/<branch>` for branches
- `refs/tags/<tag>` for tags
- `HEAD` for the current branch pointer

Views MAY use any naming convention. Views that share a kernel
SHOULD use disjoint prefixes to avoid collisions.

### C6: HEAD-as-object
To persist "current branch" across restarts (since names can only
point to hashes, not other names), Views store HEAD as a small
immutable object bound to the name `"HEAD"`:
```json
{"type": "head", "branch": "refs/heads/main"}
```
Each `checkout` writes a new HEAD object and re-points `HEAD`.
Views MAY keep HEAD in process memory instead if persistence is
not needed.

### C7: Commit = {tree, parent(s), message, timestamp}
The commit structure is convention, not law:
```json
{
  "type": "commit",
  "tree": "<tree_hash>",
  "parents": ["<parent1_hash>", "<parent2_hash>"],
  "message": "commit message",
  "timestamp": 1234567890.0,
  "author": "optional"
}
```
`parents` is a list (supports multi-parent merges). Single-parent
commits have a list with one element. Root commits have an empty list.

---

## Part 3: What is INTENTIONALLY UNSPECIFIED

These are things the kernel does NOT define. Views must make their own
choices. Different Views may make different choices and be incompatible.

### U1: Object format
The kernel stores bytes. It does not know if those bytes are JSON,
protobuf, Parquet, raw floats, or anything else. Two Views using
different formats cannot read each other's objects.

**Lens author's responsibility:** choose a format, document it, and
handle serialization/deserialization in the Lens.

### U2: Tree structure
The kernel has no tree concept. "Tree" is a Lens-level pattern. A
View could use flat trees, nested trees, prolly trees, B-trees, or
no trees at all (just name → blob directly).

**Lens author's responsibility:** define how the Lens organizes
multiple blobs into a coherent structure.

### U3: Staging area / index
The kernel has no staging concept. Staging is Lens-level state
(typically in-memory). The kernel does not persist staging state.

**Lens author's responsibility:** decide how to buffer writes before
committing. Typically an in-memory dict, cleared on commit.

### U4: Error representation
The kernel names error conditions (`NOT_FOUND`, `HASH_NOT_FOUND`)
but does not specify how they're delivered (exceptions, result types,
error codes). Views must handle errors from kernel calls.

**Lens author's responsibility:** wrap kernel calls in error handling
appropriate for the Lens's language and conventions.

### U5: Multi-writer coordination
The kernel provides no CAS, no transactions, no MVCC. Concurrent
`Reference` calls to the same name use last-writer-wins. Views that
need multi-writer safety must implement their own coordination
(branches + merges, external locks, Raft).

**Lens author's responsibility:** decide on a concurrency model.
For single-writer: no special handling needed. For multi-writer:
use branches (CRDT pattern) or external coordination.

### U6: Garbage collection
The kernel does not GC. Orphaned objects (Write without Reference,
or Reference overwritten) accumulate forever. Views define their own
reachability and implement GC as a periodic walk.

**Lens author's responsibility:** decide if/when to GC. Define
reachability (which names are roots, how to walk transitive
references). Implement sweep.

### U7: Merge semantics
The kernel allows multi-parent commits (parents is a list in the
commit blob) but does not define merge. Merge is a Lens-level
operation: read both parent trees, resolve conflicts, write a new
merged tree + commit.

**Lens author's responsibility:** define conflict resolution rules.
Git uses 3-way merge. CRDTs use commutative merge. SQL might use
UNION. There is no universal merge.

### U8: HEAD / working tree
The kernel has no HEAD, no working tree, no "current state" beyond
what names point to. Views track "current branch" however they want
(typically HEAD-as-object, convention C6).

**Lens author's responsibility:** define how the Lens tracks its
current position in the commit DAG.

### U9: Timestamps and identity
The kernel does not track author, timestamp, or identity on objects.
Views add these fields to their commit blobs as needed.

**Lens author's responsibility:** decide what metadata to attach
to commits. The kernel stores it opaquely as bytes.

### U10: Checkout semantics
The kernel has no "checkout" concept. A View's checkout is typically:
1. Resolve the branch name to a commit hash
2. Read the commit's tree
3. Update HEAD (via HEAD-as-object or in-memory)
4. The "working tree" is whatever the Lens presents to the user

**Lens author's responsibility:** define what checkout means for
the Lens. The kernel provides no working tree abstraction.

### U11: Time travel performance
Walking the commit parent chain is O(N) in history depth. The kernel
provides no skip pointers, no history index, no logarithmic time
travel. Views that need fast time travel implement skip pointers
as a Lens-level pattern (every Kth commit stores a back-pointer).

**Lens author's responsibility:** decide if time travel is needed
and at what performance level. Implement skip pointers if needed.

### U12: Cross-View compatibility
Two Views using the same kernel CANNOT read each other's objects
unless they agree on format, tree structure, and ref naming. The
kernel provides no interop layer.

**Lens author's responsibility:** if cross-View compatibility is
needed, Views must agree on conventions (Part 2). The kernel does
not enforce or verify compatibility.

---

## Part 4: The View Boundary (summary)

```
┌─────────────────────────────────────────┐
│              KERNEL GUARANTEES           │
│  (G1-G6: immutability, dedup, resolve,  │
│   LWW, hash check, backend independence) │
├─────────────────────────────────────────┤
│              CONVENTIONS                 │
│  (C1-C7: JSON, self-typing, flat tree,  │
│   full-snapshot, ref naming, HEAD-as-    │
│   object, commit structure)              │
├─────────────────────────────────────────┤
│         INTENTIONALLY UNSPECIFIED        │
│  (U1-U12: format, tree structure,       │
│   staging, errors, concurrency, GC,      │
│   merge, HEAD, timestamps, checkout,     │
│   time travel, cross-View compat)        │
├─────────────────────────────────────────┤
│              VIEW CODE                   │
│  (everything the Lens author writes)     │
└─────────────────────────────────────────┘
```

**The boundary is sharp:** the kernel guarantees G1-G6. Everything
else is the Lens's responsibility. Conventions (C1-C7) are
recommended but not enforced. Unspecified items (U1-U12) are
explicitly the Lens's choice.

---

## Part 5: Checklist for View Authors

Before writing a Lens, answer these questions:

- [ ] What format will I serialize objects in? (U1)
- [ ] How will I structure trees? Flat, nested, or something else? (U2)
- [ ] Where will I store staging state? In-memory? (U3)
- [ ] How will I handle kernel errors? Exceptions? Result types? (U4)
- [ ] Do I need multi-writer support? If so, branches+merge or external coordination? (U5)
- [ ] Do I need GC? If so, what's my reachability definition? (U6)
- [ ] Do I need merge? If so, what conflict resolution? (U7)
- [ ] How will I track HEAD/current position? (U8)
- [ ] What metadata will I attach to commits? (U9)
- [ ] What does checkout mean for my View? (U10)
- [ ] Do I need fast time travel? If so, skip pointers? (U11)
- [ ] Do I need cross-View compatibility? If so, which conventions? (U12)

If you can answer all 12, you have a complete View design. The kernel
will not surprise you.
