# Non-Goals

Things Pond deliberately does **not** solve. This document prevents
future architectural drift by making explicit what is out of scope.

---

## Pond is NOT a...

### SQL optimizer
Pond's kernel has no query planner, no cost model, no statistics.
Views that need query optimization (SQLView) implement their own.
The kernel stores and retrieves bytes; it does not plan execution.

### Distributed consensus protocol
Pond's kernel has no Raft, no Paxos, no leader election.
Multi-writer coordination is a Lens/infrastructure concern.
The kernel provides last-writer-wins; Views build stronger coordination
on top (branches, CRDTs, external Raft).

### Vector search engine
Pond's kernel has no HNSW, no IVF, no ANN algorithm.
VectorView does linear scan. Production vector search requires a
Lens-level index library. The kernel stores vectors as bytes.

### Query planner / IR
Pond's kernel has no intermediate representation, no logical/physical
plan, no operator model. The Planner/IR is a future layer above the
kernel, not part of it.

### Transaction manager
Pond's kernel has no ACID transactions, no MVCC, no 2PC.
Reference is single-key, last-writer-wins. Views that need transactions
(atomic multi-name updates) implement their own protocol.

### Schema registry
Pond's kernel has no schema concept. Views track their own schemas.
The kernel stores bytes; it does not validate structure.

### Cache
Pond's kernel has no cache layer. Caching is a Lens/infrastructure
concern. The kernel reads from the backend on every Read; Views
cache if they want to.

### Index
Pond's kernel has no index primitive. Views build indexes as Tree
patterns. A shared index library (Lens-level) may emerge, but it's
not part of the kernel.

### Scheduler
Pond's kernel has no job scheduler, no pipeline executor, no morsel
driver. Execution scheduling is a Lens/backend concern.

### Authorization system
Pond's kernel has no auth, no RBAC, no capability enforcement.
Multi-tenancy is solved via separate kernel instances or capability
tokens (Lens-level naming conventions).

### Compression engine
Pond's kernel does not compress. Views compress their blobs before
Write. The kernel stores raw bytes.

### Replication layer
Pond's kernel has no replication. Single-node only.
Replication (Raft, async followers) is an infrastructure layer.

### Streaming engine
Pond's kernel has no streaming, no watermarks, no exactly-once.
StreamView provides append-only log semantics; full streaming
(differential dataflow) is a future View concern.

### Garbage collector
Pond's kernel has no GC. Orphaned objects accumulate.
GC is a Lens-level utility (PondGC in archive/engineering/02_gc.py).

### Time-travel accelerator
Pond's kernel has no skip pointers, no history index.
Time travel walks the parent chain (O(N)). Views implement skip
pointers if they need O(log N) time travel.

### Merge engine
Pond's kernel has no merge semantics. Multi-parent commits are
allowed (parents is a list in the commit blob), but conflict
resolution is a Lens concern.

### Working tree
Pond's kernel has no working tree, no checkout, no staging area.
These are version-control concepts that belong in GitView, not the kernel.

### Network protocol
Pond's kernel has no wire protocol, no client-server model.
The kernel is a library. Networking is an infrastructure layer.

---

## Why these are non-goals

Each of the above is either:
1. **Workload-specific** (SQL optimizer, vector search, schema registry) —
   not all Lenses need it. Adding it to the kernel would violate the
   Universality criterion of the Admission Rule.
2. **Infrastructure-specific** (replication, consensus, networking) —
   these are deployment concerns, not storage concerns. The kernel
   works the same whether single-node or distributed.
3. **Lens-level** (cache, index, GC, merge, time-travel) — Views can
   implement these using the 3 kernel primitives. Adding them to the
   kernel would violate the "Impossible outside kernel" criterion.

---

## The boundary

```
What Pond IS:     a minimal immutable object runtime (6 substrates, 3 operations)
What Pond is NOT: everything else (see above)
```

If a future proposal suggests adding any of the above to the kernel,
it must pass the 5-criterion Admission Rule. So far, none have passed.

---

## Multi-writer serializability on a single collection

Pond has no serialization point of its own, by design: no coordinator, no
catalog, no conditional writes. That is what buys convergence without a
sequencer and identical behaviour on local disk and object storage.

A precision worth keeping: this is **sequencer-free**, not coordination-free.
A writer that must PUT to the store, and a reader that must GET from it, are
not available when partitioned from it — so the object store *is* a
coordinator, and by CALM's definition (coordination-freeness is equivalent to
availability under partition) the stronger phrase is simply false. What Pond
removes is the thing it would otherwise have to *operate*: a transactor, a
catalog, a metadata service, a lease holder, a CAS loop on a hot pointer. It
rents a linearizable namespace; it does not run one. That is the real and
defensible claim, and it is not diminished by stating it accurately.

What that does and does not rule out is worth stating precisely, because it is
easy to overstate:

- **Available:** serializable transactions *within a writer* — that writer's
  head is its serialization point, and it can offer any isolation level it
  likes above that. Atomic multi-collection publish. Sequencer-free
  convergence *between* writers via per-field merge.
- **Not available:** a *single* serial order agreed between two writers who
  are concurrently mutating the same key and who both need one of their
  transactions to abort. Pond will converge them by version rather than abort
  either.

This is emphatically **not** the same as "you cannot run a database on Pond."
A database engine — Postgres included — performs its own concurrency control
in its own process: one WAL stream, one lock manager, one MVCC implementation.
Storage underneath it only has to durably persist an ordered stream, which is
exactly the single-writer case Pond serves. See
`docs/POSTGRES_ON_POND.md`.

The uncovered case is narrower than it sounds: two *independent database
engines* mutating the same rows with no channel between them. Aurora
Multi-Master shaped, not Postgres shaped. Almost nobody runs that, and the
systems that do all pay for a coordinator to get it.
