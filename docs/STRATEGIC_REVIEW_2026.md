# Pond — Strategic Review and Next Steps

Written against the stated goal: *a universal, multi-workload, PB-scale storage
system that behaves identically on local disk and object storage, needs no
coordinator and no extra service, avoids CAS/if-not-exists boilerplate,
minimises backend round trips, and lets any application plug in on top.*

---

## 1. Where Pond actually stands

Measured, not asserted. Everything below is from tests in this repo.

**Validated on live Cloudflare R2** (`docs/R2_VALIDATION.md`):

| Claim | Result |
|---|---|
| Constant-depth lookup | 500 → 50 000 records: 2.00 → 3.00 GETs |
| Warm lookup | 100 % cache hit, 96.7 µs p50, zero backend requests |
| Write amplification | 5.00 → 0.02 PUTs/record (263× via batching) |
| Insert read cost | 977 leaf reads → 12, at 500 k entries |

**Validated in-process**: history independence (1000 insertion orders → one
root hash), merge commutativity/associativity/idempotence, per-field merge,
never-drop-unknown-fields, geo-sync by plain file copy.

**The problem:**

```
core/index    1 763 lines   consumed by: benchmarks only
core/record   1 195 lines   consumed by: benchmarks only
core/cache      692 lines   consumed by: benchmarks only
------------------------------------------------------
core/storage  4 310 lines   consumed by: 13 crates (CLI, MCP, PyO3, SQL, all lenses)
core/codec    4 233 lines   consumed by: the same 13
```

**This was the problem, and it is now half solved.** `core/storage` dispatches
to the engine per collection, and the CLI reaches it end to end. What still
calls the legacy path directly: the lens crates, the SQL executor, and the
PyO3 bindings. Until those route through the dispatch too, engine collections
are reachable from the CLI and the Rust API but not from every surface.

---

## 2. The competitive landscape

Grouped by what they prove rather than by category.

### The problem is real — DuckLake says so

DuckDB's team built [DuckLake](https://ducklake.select/manifesto/) in 2025 on
an explicit critique of Iceberg: *"the involved sequence of file IO required to
run the smallest query… many separate sequential HTTP requests, creating a
lower bound to how fast reads or transactions can run."* That is Pond's thesis,
from the people who ship the most-used embedded analytics engine.

Their fix is the opposite of Pond's: put all metadata in a SQL database
(DuckDB/Postgres/MySQL). It works — ACID comes free, small writes inline — but
**you are now running a database to run your lake**, and that database is a
new single point of scaling, failure, and operations.

- **Strength:** fast metadata, real transactions, no small-file storm.
- **Weakness:** single-engine in practice; the catalog DB is a service.
- **Ugly:** you adopted a lakehouse to avoid running infrastructure.

### Iceberg / Delta / Paimon — the incumbents

- **Strength:** ecosystem. Spark, Trino, Flink, Snowflake, BigQuery all read
  them. This is a genuine moat and Pond will not out-ecosystem it.
- **Weakness:** 4–5 sequential round trips on a cold read (catalog →
  metadata.json → manifest-list → manifest → data); metadata grows with
  history; small-file problem; commits need a catalog for atomicity.
- **Ugly:** the metadata *is* the scaling problem the format was meant to
  solve. Paimon adds an LSM on top and needs Flink plus a catalog.

### SlateDB — the closest architectural cousin

[SlateDB](https://slatedb.io/) is an embedded LSM on object storage, in Rust,
with the same "bottomless storage, batch writes, cache reads" instincts.

- **Strength:** genuinely object-native, no service to run, real compaction.
- **Weakness:** **single writer, enforced by a manifest fencing protocol.**
  Multiple readers scale, writers do not.
- **Ugly:** fencing is exactly the coordination Pond's design refuses. Their
  correctness argument depends on it.

This is the sharpest comparison available: same substrate, same language, and
they chose fencing where Pond chose writer-partitioned namespaces.

### WarpStream / AutoMQ / Fluss — diskless streaming

- **Strength:** enormous cost win over Kafka; stateless agents; proven.
- **Weakness:** **every one of them has a central metadata service.**
  WarpStream's own docs: agents talk to "object storage *and a cloud metadata
  store*". Partition leadership, offsets, group coordination all live there.
- **Ugly:** "no disks" is marketed, "no coordinator" is not — and the metadata
  service becomes the thing you scale and page on.

### Neon / Databricks Lakebase — OLTP on object storage

This is the strongest challenge to Pond's goal, and it deserves to be quoted
exactly: *"The lakebase architecture intentionally keeps object storage off the
critical path. Object storage provides durability and scale, but never sits in
front of query execution."*

The state of the art in serverless Postgres **does not serve reads from object
storage.** It streams WAL to a stateful pageserver tier and serves queries from
there.

- **Strength:** real Postgres, real serializability, instant branching.
- **Weakness:** the pageserver is a stateful service with its own cache
  coherence, failover, and operational burden.
- **Ugly:** the branching everyone loves is a storage-layer trick wrapped in a
  service you cannot self-host casually.

### Dolt, LanceDB, Pixeltable, Redis

- **Dolt** — the only real git-for-data with true three-way merge and conflict
  resolution. Weakness: MySQL-bound, disk-first, not object-native. Its merge
  is genuinely better than Pond's current one and worth studying.
- **Lance / LanceDB** — excellent columnar-with-random-access format for AI.
  Weakness: format plus a catalog; versioning at scale still needs one.
- **Pixeltable** — multimodal tables with computed columns. It is an
  orchestration layer, not storage; a natural *consumer* of Pond, not a rival.
- **Redis** — not comparable on architecture, but it is the latency bar people
  compare against. Pond's warm number (96.7 µs) is in that conversation; its
  cold number is not, and never will be.

---

## 3. The position nobody occupies

Three properties, and no existing system has all three:

| System | No service to run | Multi-writer without coordination | Constant-depth metadata |
|---|---|---|---|
| Iceberg / Delta | ✅ (with a catalog…) | ❌ catalog serialises commits | ❌ 4–5 RTTs, grows |
| DuckLake | ❌ needs a database | ❌ database serialises | ✅ |
| SlateDB | ✅ | ❌ single-writer fencing | ✅ |
| WarpStream / Fluss | ❌ metadata service | ❌ | ✅ |
| Neon / Lakebase | ❌ pageserver tier | ❌ | n/a — S3 off critical path |
| Dolt | ❌ server | ❌ | ❌ |
| **Pond** | ✅ | ✅ writer-partitioned | ✅ 2–3 GETs measured |

The unoccupied square is **all three at once**, and Pond has now measured all
three. That is the claim worth building the product around:

> **A storage substrate where any number of writers, anywhere, converge with no
> coordinator, no catalog, no metadata service, and no conditional writes — at
> constant metadata cost regardless of size.**

The second differentiator follows from content addressing and is easy to
under-sell: **the cache needs no coherence protocol.** Neon runs a pageserver
tier partly to manage cache coherence. Pond cannot have a coherence bug,
because a hash cannot name different bytes. That is why 96.7 µs warm reads come
without an operational story.

---

## 4. What is honestly not achievable

Being straight about the boundary is what makes the rest credible.

**What Pond cannot provide is a serialization point it does not have.**
Serializable transactions across arbitrary rows and *mutually unaware writers*
need one. Pond deliberately has none. What it does offer:

- serializable transactions **within a single writer** (that writer's head is
  its serialization point);
- atomic multi-collection publish (already built, and now literally one object
  write);
- coordination-free convergence **across** writers, with per-field merge.

This is worth stating carefully, because the obvious reading of it is wrong.
It does **not** mean "no database can run on Pond" — see
[`POSTGRES_ON_POND.md`](POSTGRES_ON_POND.md). Postgres brings its own
serialization point: MVCC, its lock manager, and one primary that orders every
write. What it asks of storage is a durable ordered append (the WAL) and a page
lookup by `(relation, block)`. Both are Pond's best case, and the primary is a
single writer by construction, so nothing needs coordinating at the storage
layer at all.

The limit is narrower than "no OLTP": it is *two writers that do not know about
each other, resolving a conflict on the same row without talking*. Pond
converges those deterministically by per-field last-writer-wins, which is the
right answer for a feature store and the wrong one for a ledger. So: two
Postgres primaries on one Pond collection is not a thing. One Postgres primary
on Pond is, and so is "two app servers doing `UPDATE … WHERE balance > 0`" —
provided they go through that primary, exactly as they do today. Say the narrow
version in `NON_GOALS.md`; the broad version would be false.

**Cold reads will not beat a warm database.** 2–3 round trips to object storage
is 40–150 ms on a good day. The honest pitch is: cold is bounded and constant,
warm is microseconds, and *no tier in between needs operating*.

---

## 5. Next steps, in priority order

### P0 — The cutover — **done**

`core/engine` exists and `core/storage` dispatches to it per collection. The
marker is a definition object at `collections/{name}/definition`, and its
*absence* means legacy — so every collection written before the engine existed
keeps its old path with nothing migrated.

Both acceptance criteria are met. `core/storage/tests/cutover.rs` runs the same
operations through both paths and compares the rows; the CLI creates, writes,
and reads an engine-backed collection alongside a legacy one in the same
repository.

Measured on the same harness, same row:

| | round trips | writes | atomic? |
|---|---|---|---|
| legacy | 8 | 6 | no — spans 3 refs |
| engine | 5 | 2 | yes — one object |

PUTs are ~12× a GET on S3 pricing, so the 3× drop in writes matters more than
the round-trip count suggests.

**Still to do here:** the lens crates, SQL executor and PyO3 bindings still
call the legacy path directly rather than going through the dispatch, so they
cannot yet read engine collections. `manifest.rs` stays until they do.

### P1 — Parallel bulk load — **done**

Every node write used to be one sequential PUT, which bounded every benchmark
and would have bounded every real import. A tree level is now built in full and
written in one `put_batch`, which S3 issues 32-wide. The 50 000-record R2 build
that previously timed out completes, and reader open went from two sequential
round trips per writer to one batched read for all of them.

Remaining: bounded concurrency across *levels* for very wide trees, which only
matters above roughly 10⁴ nodes per level.

### P2 — Segments (PAX)

Records live inline in the index. That is right for small values and wrong for
a scan. Until segments exist, the analytics story is incomplete and the
"index segments, not rows" sizing that makes 1 PB → 8 M entries is theoretical.

### P3 — Retention and tombstone reclamation

Streaming needs range deletes and epoch-based tombstone reclamation, or
tombstones grow without bound. The current CRDT path has this problem today.

### P4 — Prove multi-writer on real infrastructure

The convergence tests run in one process. Run N writers across separate
processes and regions against one bucket, kill some mid-write, and assert
byte-identical roots. This is the headline claim; it deserves a real test.

### P5 — First plug-in target

Pick one and go deep rather than four shallow. Recommendation: **Kafka
protocol**, because it is the workload whose semantics Pond already matches
(append-only, offset-ordered, multi-writer, no cross-partition transactions),
and because WarpStream proved the market while still requiring the metadata
service Pond does not need.

---

## 6. Recommended positioning

Not "a better lakehouse" — that fight is ecosystem-bound and unwinnable. This:

> **The storage substrate with no moving parts.** One copy of your data on
> object storage, any workload, any number of writers worldwide, converging
> without a coordinator — and nothing to deploy, scale, or page on.

Every competitor above either runs a service or restricts you to one writer.
That is the sentence, and Pond is now the only system that has measured it.
