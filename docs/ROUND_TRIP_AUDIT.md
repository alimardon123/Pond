# Round-Trip Audit

Round trips are the objective function of this design: every latency claim in
the tree reduces to how many times a reader has to wait for object storage.
This document is the measurement of that number.

**It is generated, not written.** Regenerate it with:

```
cargo run --release -p pond_bench --bin roundtrips -- --markdown
```

and paste the table below. The previous version of this file was a hand-made
table describing the `CollectionManifest` read path, said to be "verified by
scripts/benchmark_round_trips.py" — a script that no longer exists, for a path
that new collections no longer take. A hand-written cost table drifts from the
code silently, which is the worst way for a cost table to be wrong.

## Definitions

**Round trip** — one dependent wait. A batch of any width counts once, because
its members are issued together and none waits on another's result. This is
what the wall clock is made of.

**Request** — one billable operation. A 32-wide batch is 32 requests and 1
round trip. This is what the bill is made of.

**Batch width** — requests ÷ round trips. 1.0 means nothing in that operation
ran in parallel.

**Cold** — a reader with no cache of any kind.

**Warm** — a *fresh* reader whose local-disk cache an earlier reader
populated: empty memory tier, warm disk tier. Not the same query repeated in
one process, which answers from its own memory and reports zero; that number
is true and useless. This is what a second process, a restarted process, or a
second query over overlapping data actually pays.

> **This column used to be unreachable.** When it was first published, the
> benchmark constructed the disk cache itself, and nothing on the shipped read
> path did: `pond_cache_config()` returned `CacheConfig::default()`, whose
> `disk_dir` is `None`, and `with_disk` had no caller outside benchmarks and
> tests. So the warm numbers were real for the benchmark and unobtainable
> through `read_rows`, which is the API everything else uses. Measured through
> that API, four consecutive scans of a 50,000-row collection each cost 22
> round trips and 2447 KiB — no caching whatsoever, every pass identical.
>
> The disk cache is now on by default, under the platform cache directory, and
> `POND_CACHE_DIR=off` disables it. The same four scans now cost **3 round
> trips and 0.1 KiB each**. Content addressing is what makes switching it on
> safe: an entry is named by the hash of its own bytes, so it cannot go stale
> and cannot collide, and `verify_on_read` re-hashes on the way out.

**Modelled ms** — `round_trips × 30 ms + MiB × 20 ms`, the arithmetic priced
at this repository's own R2 measurements (`docs/R2_VALIDATION.md`). The counts
are exact and are the real result; the milliseconds are those counts at a
stated rate. Same-region S3 is faster and cross-region slower — multiply.

## Measurements

| operation | rows (or writers, for the @writers rows) | cache | round trips | requests | batch width | KiB | modelled ms |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: |
| open | 1000 | cold | 2 | 2 | 1.0 | 0.1 | 60.0 |
| open | 1000 | warm | 2 | 2 | 1.0 | 0.1 | 60.0 |
| open | 10000 | cold | 2 | 2 | 1.0 | 0.1 | 60.0 |
| open | 10000 | warm | 2 | 2 | 1.0 | 0.1 | 60.0 |
| open | 100000 | cold | 2 | 2 | 1.0 | 0.1 | 60.0 |
| open | 100000 | warm | 2 | 2 | 1.0 | 0.1 | 60.0 |
| point read | 1000 | cold | 4 | 4 | 1.0 | 31.3 | 120.6 |
| point read | 1000 | warm | 2 | 2 | 1.0 | 0.1 | 60.0 |
| point read | 10000 | cold | 4 | 4 | 1.0 | 180.0 | 123.5 |
| point read | 10000 | warm | 2 | 2 | 1.0 | 0.1 | 60.0 |
| point read | 100000 | cold | 4 | 4 | 1.0 | 28.4 | 120.6 |
| point read | 100000 | warm | 2 | 2 | 1.0 | 0.1 | 60.0 |
| range scan 1k | 1000 | cold | 4 | 5 | 1.2 | 45.8 | 120.9 |
| range scan 1k | 1000 | warm | 2 | 2 | 1.0 | 0.1 | 60.0 |
| range scan 1k | 10000 | cold | 4 | 5 | 1.2 | 78.8 | 121.5 |
| range scan 1k | 10000 | warm | 2 | 2 | 1.0 | 0.1 | 60.0 |
| range scan 1k | 100000 | cold | 4 | 5 | 1.2 | 81.9 | 121.6 |
| range scan 1k | 100000 | warm | 2 | 2 | 1.0 | 0.1 | 60.0 |
| full scan | 1000 | cold | 4 | 5 | 1.2 | 45.8 | 120.9 |
| full scan | 1000 | warm | 2 | 2 | 1.0 | 0.1 | 60.0 |
| full scan | 10000 | cold | 4 | 9 | 2.2 | 399.5 | 127.8 |
| full scan | 10000 | warm | 2 | 2 | 1.0 | 0.1 | 60.0 |
| full scan | 100000 | cold | 4 | 51 | 12.8 | 4111.9 | 200.3 |
| full scan | 100000 | warm | 2 | 2 | 1.0 | 0.1 | 60.0 |
| projected scan 2/8 | 1000 | cold | 4 | 5 | 1.2 | 45.8 | 120.9 |
| projected scan 2/8 | 1000 | warm | 2 | 2 | 1.0 | 0.1 | 60.0 |
| projected scan 2/8 | 10000 | cold | 4 | 9 | 2.2 | 399.5 | 127.8 |
| projected scan 2/8 | 10000 | warm | 2 | 2 | 1.0 | 0.1 | 60.0 |
| projected scan 2/8 | 100000 | cold | 4 | 51 | 12.8 | 4111.9 | 200.3 |
| projected scan 2/8 | 100000 | warm | 2 | 2 | 1.0 | 0.1 | 60.0 |
| write 1 row | 1000 | cold | 3 | 3 | 1.0 | 0.2 | 90.0 |
| write 1 row | 10000 | cold | 3 | 3 | 1.0 | 0.2 | 90.0 |
| write 1 row | 100000 | cold | 3 | 3 | 1.0 | 0.2 | 90.0 |
| write 1k rows | 1000 | cold | 3 | 4 | 1.3 | 45.5 | 90.9 |
| write 1k rows | 10000 | cold | 2 | 2 | 1.0 | 44.0 | 60.9 |
| write 1k rows | 100000 | cold | 2 | 2 | 1.0 | 44.9 | 60.9 |
| point read @writers | 1 | cold | 3 | 3 | 1.0 | 10.7 | 90.2 |
| point read @writers | 4 | cold | 3 | 9 | 3.0 | 43.7 | 90.9 |
| point read @writers | 16 | cold | 3 | 33 | 11.0 | 172.7 | 93.4 |
| point read @writers | 64 | cold | 3 | 129 | 43.0 | 688.5 | 103.4 |
| full scan @writers | 1 | cold | 3 | 3 | 1.0 | 10.7 | 90.2 |
| full scan @writers | 4 | cold | 3 | 9 | 3.0 | 43.7 | 90.9 |
| full scan @writers | 16 | cold | 3 | 33 | 11.0 | 172.7 | 93.4 |
| full scan @writers | 64 | cold | 3 | 129 | 43.0 | 688.5 | 103.4 |

## What this says

**Opening costs 2 round trips, flat at every scale.** One LIST to find the
writers, one GET for the head. It does not grow with rows, and it does not
grow with history. That is the property the head layout was built for and it
holds.

**It also costs 2 round trips warm** — and that is a ceiling, not a floor.
Those two waits are ~60 ms of the model, they are paid before a single byte of
data is touched, and no cache can remove them: a reader that trusted a cached
head would not be reading the latest data, which is the one thing an open is
for. So **no read can be single-digit-millisecond while it includes an open**,
however good the local-disk cache gets. The disk cache is doing its job —
every warm operation, up to a full scan of 100k rows, costs exactly the open
and nothing more — and the open is now the whole warm cost.

**Point reads are flat: 4 cold round trips at 1k, 10k and 100k rows.** Two for
the open, two for the descent. The constant-depth claim holds over the range
measured.

**And flat in writers, which is the harder claim.** The `@writers` rows hold
the data fixed at 200 rows per writer and vary the writer count instead,
because those are different questions: row count asks whether cost grows with
data, writer count asks whether it grows with concurrency. Concurrency is what
this design actually promises — any number of writers converging without
coordination — and for a while a reader paid for it: a point read cost 141
round trips and **100 PUTs** at 64 writers, because a reader's view is the
merge of every writer's tree and merging is a write.

Reads now descend every root together instead of merging first: **3 round
trips at 1 writer and at 64**, with batch width rising 1.0 → 43.0 and no PUTs
at all. Requests still grow with writers — that is the bill, and batching does
not reduce it — while the wait does not. Those two numbers separating is the
whole point.

**Scans now wait per level, not per node.** They did not. A cold full scan of
100k rows was 51 sequential waits at batch width 1.0 — 1.6 s modelled — even
though every leaf hash is known from the internal nodes before the first leaf
is fetched. The traversal descended recursively, one node per round trip.
Walking a level at a time and reading it in one batch takes the same scan to
**4 waits at width 12.8, 200 ms** — an 8× reduction in wall clock for an
unchanged 51 requests, which is the right trade: requests are the bill, round
trips are the wait.

Point reads stay at 4 waits and cannot improve this way: a descent is
inherently sequential, since each level's hashes come from decoding the level
above. Fewer waits there needs a different mechanism, not a wider batch.

**Projection reads every byte it discards.** `projected scan 2/8` is identical
to `full scan` — same round trips, same 4111.9 KiB — at every scale. Selecting
two of eight columns reads all eight, because projection only skips *spilled*
fields and small columns live inline in the leaf. Against a columnar engine
reading two column chunks, this is the gap, and it is a layout problem rather
than a code one.

**A one-row write is 3 round trips, ~90 ms.** Nodes, then the head, and the
staging read before them. Two-digit milliseconds needs this at 2.

## The honest summary

| claim | status |
| --- | --- |
| Open cost flat in data and in history | holds — 2 round trips at every scale |
| Point read cost flat in data | holds — 4 cold round trips at every scale |
| Minimum round trips | scans yes — a 100k scan is 4 waits, down from 51. Point reads still 4, bounded by descent depth |
| Two-digit-ms reads and writes | cold: closer — 120–200 ms, down from 120–1600. warm: 60 ms, all of it the open |
| Single-digit-ms warm reads | **not reachable today** — the open alone is 2 round trips, and no cache can remove it |
| The disk cache is on the shipped read path | now yes — it was not when this was first published; 22 round trips to 3 through `read_rows` |
| Read cost flat in the number of writers | holds — 3 round trips and 0 PUTs at 1 writer and at 64, down from 141 waits and 100 PUTs |
| Reads never write | holds — 0 PUTs on every read path measured |
| Column projection reduces I/O | **no** — a 2-of-8 projection reads 100% of the bytes |
