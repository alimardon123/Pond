# What is Pond?

> One page. If you read nothing else, read this.

---

Pond is a minimal immutable object graph with universal history,
upon which multiple semantic interpretations (Lenses) and deterministic
physical structures can coexist without changing the underlying data.

---

## The Kernel

The kernel owns three things. Nothing else.

| Primitive | Description |
|---|---|
| **Bytes** | Immutable, content-addressed blobs. Pure payload. No envelope, no type tag, no header. The kernel does not know what format the bytes are in. |
| **History** | A commit DAG with parent pointers. Shared by all Lenses with the same name. Branching, merging, and time-travel operate on this DAG. |
| **Names** | Mutable name → hash references. The only mutation in the system. Names identify Lenses (objects) in the kernel. |

Three primitives: `Write(bytes) → hash`, `Read(hash|name) → bytes`,
`Reference(name, hash)`.

~140 lines of code. Frozen. No codec registry. No envelope. No manifest.

---

## The Lens

A Lens interprets bytes. It never owns bytes.

| Property | Rule |
|---|---|
| **Interprets** | A Lens encodes data into bytes (for writing) and decodes bytes into data (for reading). The encoding is the Lens's choice. |
| **Never owns** | The bytes belong to the kernel. The Lens is a translation layer, not a storage owner. |
| **Never modifies** | A Lens may interpret bytes during reading. It may never modify the stored bytes. |
| **Shares** | Multiple Lenses with the same name share the same byte graph. Each sees all keys, all history, all branches. |
| **Reads any** | Any Lens can read any blob written by any other Lens — the Resolver (code, not data) decodes based on key-prefix context. |

The interpretation layer lives in **code** (the Resolver), not in **data**
(the blob). The kernel stays format-agnostic.

---

## Physical Structures

Physical structures accelerate access. They never own data.

| Structure | Purpose |
|---|---|
| Indexes | O(log N) lookup by non-primary-key field |
| Bloom filters | Skip unnecessary reads |
| Zone maps | Skip irrelevant chunks |
| Statistics | Query optimization |
| Caches | Reduce latency |

All are deterministic functions of a snapshot. Deleting every physical
structure must never change the reconstructed dataset. Rebuilding a
physical structure from the same state always produces the same hash.

---

## What is Explicitly NOT in the Kernel

- Codec IDs, envelopes, type tags
- Manifests, enable-view metadata, sidecar files
- Schema blobs, format registries
- SQL optimizer, query planner, execution engine
- Distributed consensus, replication, networking
- Compression, caching, scheduling

All of these are emergent — they live above the kernel, in Lenses,
physical structures, or applications. The kernel is replaceable;
everything above is composable.

---

## The Architecture Laws

Pond's executable specification. If any law fails, the architecture
is violated.

1. **Identity** — once a blob hash exists, its contents never change.
2. **Reachability** — every committed reference resolves to exactly one blob.
3. **History** — replaying history reconstructs the same snapshot.
4. **Lens** — a Lens may interpret bytes; it may never modify them.
5. **Physical Structure** — deleting all structures never changes the dataset.
6. **Branch** — branch creation never duplicates blobs.
7. **Merge** — merge changes references, not blob contents.
8. **Determinism** — same writes, same ordering, same blob hashes.
9. **Scale** — at scale, count equals the number written.
10. **Index** — index rebuild at scale succeeds without errors.

---

## The Layer Hierarchy

```
Bytes • History • Names          ← Kernel (frozen, ~140 LOC)
        ↓
    Collections                        ← Named objects with type + namespace
        ↓
    Physical Structures             ← Acceleration (indexes, stats — deterministic)
        ↓
    Lenses                          ← Interpretation (code, not data)
        ↓
    Applications                    ← SQL, Git, Feature Store, Notebook
```

Dependencies flow downward only. Each layer adds exactly one capability.
No layer leaks upward. The kernel never changes.

### Collections

A Collection is a named object in the kernel — like a table in a database,
a repo in Git, or a notebook in Jupyter. It lives in a hierarchical
namespace (e.g., `analytics/orders`, `ml/features/user_stats`) and has:

- A **type** (which Lens family created it: "sql", "git", "feature_store", etc.)
- A **description** (human-readable)
- An optional **source** (parent Collection name, for materialized views)

The metadata is ONE small blob per Collection (stored as a kernel Name),
NOT per record. List all Collections via `Collection.list(kernel)` — see every
Collection with its type, like listing tables in a database. List Collections
in a namespace via `Collection.list(kernel, prefix="analytics/")`.

Namespaces are just the path structure of the Collection name (using `/`
as a separator, like a filesystem). No new kernel primitives — just a
naming convention. `Collection.list_namespaces(kernel)` shows all
namespaces.

Materialized views (indexes, aggregates, transforms) are just Collections
with `source` metadata pointing to their parent. No special API —
just pass `source` when creating the Collection. This gives lineage: any
Collection can trace back to its source.

---

## Design Goals

| Goal | How |
|---|---|
| **Simple** | The kernel is 3 primitives, ~140 LOC. It fits in your head. |
| **Powerful** | Rich behavior emerges from composition, not from kernel features. |
| **Performant** | Optimizations live above the kernel. The kernel doesn't slow you down. |
| **Scalable** | Physical structures, Lenses, and applications evolve independently. |
| **Efficient** | Immutable data plus rebuildable structures avoids duplication. |
| **Beautiful** | Each layer has one responsibility. Dependencies flow in one direction. |

---

## In One Sentence

> Pond stores immutable bytes with universal history; every higher-level
> capability is simply a different Lens over that substrate.
