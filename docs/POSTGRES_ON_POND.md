# Postgres on Pond

## The mistake worth correcting first

An earlier review in this repo claimed you could not run Postgres on Pond,
because Pond has no serialization point and Postgres needs serializable
transactions. That conflated two different layers.

**Postgres does its own concurrency control.** MVCC, the lock manager,
predicate locks, snapshot isolation — all of it lives inside the Postgres
process. What Postgres asks of storage is much smaller:

1. durably persist one ordered WAL stream;
2. return page *(relation, fork, block)* as of an LSN;
3. do not lose or reorder anything.

That is a **single-writer, ordered-append, point-lookup** workload — precisely
the case Pond serves best. Two application servers doing
`UPDATE ... WHERE balance > 0` on the same row are serialized by Postgres's
lock manager long before storage sees anything; storage receives one WAL
stream either way.

So Postgres on Pond is not a compromise. It is the workload the design fits.

---

## The mapping

Two collections, both ordinary Pond collections with ordinary Pond keys.

### WAL

```
key:    (lsn: u64)
record: { payload: Bytes }
```

Monotonically increasing keys, so every append lands in the right-most leaf —
the cheapest possible insert for a content-defined tree, touching one leaf and
its ancestor path. `scan_range(from_lsn, to_lsn)` is replay. Retention is a
range delete.

### Pages

```
key:    (relfilenode: i64, forknum: i64, blockno: i64)
record: { page: Bytes(8192), lsn: Int }
```

The order-preserving tuple encoding puts every block of a relation in one
contiguous key range, so a sequential scan of a table is a contiguous byte
range and a single block is a point lookup — **1 warm GET, 2–3 cold**, flat to
petabytes, as measured in `docs/R2_VALIDATION.md`.

That is the whole storage model. No new concepts.

---

## Where the pageserver goes

Neon and Databricks Lakebase both put a **stateful pageserver tier** between
compute and object storage. Their own documentation is explicit that object
storage never sits on the critical read path.

They need that tier for two reasons: to apply WAL into materialized pages, and
to hold a coherent cache. Pond removes the second reason entirely — cache
entries are keyed by content hash, and a hash cannot name different bytes, so
there is no coherence protocol to run and no invalidation to get wrong.

What remains is WAL application, which is a *pure function*:

```
apply(base_page, wal_records) -> new_page
```

A pure function does not need to be a service. It runs as a **library** inside
the compute process, or as a stateless background compactor any node can run —
and because the output is content-addressed, two compactors racing produce
byte-identical pages, so the race is harmless and needs no lease.

> Neon's pageserver is a stateful tier you operate. Pond's equivalent is a
> function you call.

---

## Failover without CAS

The one genuinely hard question: when a primary dies and another takes over,
what stops both from writing?

Pond will not use compare-and-swap for it, because CAS exists on object storage
and not on a local filesystem, and a primitive available on only some backends
forks the correctness argument in two.

**Epoch-tagged writer identity.** A writer owns
`heads/epoch-<NNNN>.writer-<id>` and writes nowhere else. Taking over means:
one LIST to find the highest epoch, then write at epoch+1. Readers take the
highest epoch present.

The property this buys is not *prevention* — nothing without consensus
prevents two processes believing they are primary. It is that split-brain
**cannot corrupt anything**:

- the two primaries write disjoint key sets, so neither can overwrite the other;
- both histories stay complete and readable;
- the divergence is a *branch*, which Pond already knows how to diff and merge;
- the winner is decided by whoever is at the higher epoch, deterministically on
  every reader.

Compare: with CAS you get prevention plus an outage when the CAS holder is
partitioned. Here you get no outage and a visible, reconcilable fork.

And note who actually decides primacy in every real system, Neon included: an
orchestrator, a control plane, or a human — never the storage layer. Pond's job
is not to elect; it is to not corrupt when the election is wrong.

---

## What this gives Postgres that Neon does not

| | Neon / Lakebase | Postgres on Pond |
|---|---|---|
| Tiers to operate | compute + **pageserver** + safekeepers | compute only |
| Cache coherence | pageserver-managed | none needed (content-addressed) |
| Branching | storage trick behind a service | native, one pointer copy |
| Local dev | different stack | identical code path, local FS |
| Multi-region | replicate the service | copy files both ways, merge on read |

The last row is the one that is hard to get any other way: because no two
writers ever share a key, syncing two Pond stores is a plain bidirectional file
copy with no conflict resolution at the storage layer.

---

## Honest limits

- **Cold reads are 2–3 round trips.** Warm is microseconds; cold is object
  storage. For a working set that fits the NVMe tier this is a non-issue; for
  uniformly random access over a cold petabyte it is not a Postgres
  replacement. No design fixes that — Neon's answer is the same tier, just
  always on and always operated.
- **WAL append rate is bounded by PUT latency** unless batched. Group-commit
  applies exactly as it does on a local disk: one PUT per commit batch.
- **Two independent Postgres instances writing the same pages** is out of
  scope, as it is for Neon and for Postgres itself.

---

## Build order

1. `pond_engine` — the API a database needs (`append`, `get`, `scan_range`,
   `publish`) over index + record + cache. *In progress.*
2. WAL and page collections as a thin lens; replay and apply as pure functions.
3. `smgr` shim implementing Postgres's storage manager interface against that
   lens. This is the only Postgres-specific code, and it is small — the
   interface is roughly ten calls.
4. Run `make installcheck` (Postgres's own regression suite) against it. That
   is the acceptance test: not a benchmark, the actual suite.
