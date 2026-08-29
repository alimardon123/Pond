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

**Modelled ms** — `round_trips × 30 ms + MiB × 20 ms`, the arithmetic priced
at this repository's own R2 measurements (`docs/R2_VALIDATION.md`). The counts
are exact and are the real result; the milliseconds are those counts at a
stated rate. Same-region S3 is faster and cross-region slower — multiply.

## Measurements

| operation | rows | cache | round trips | requests | batch width | KiB | modelled ms |
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
| range scan 1k | 1000 | cold | 5 | 5 | 1.0 | 45.8 | 150.9 |
| range scan 1k | 1000 | warm | 2 | 2 | 1.0 | 0.1 | 60.0 |
| range scan 1k | 10000 | cold | 5 | 5 | 1.0 | 78.8 | 151.5 |
| range scan 1k | 10000 | warm | 2 | 2 | 1.0 | 0.1 | 60.0 |
| range scan 1k | 100000 | cold | 5 | 5 | 1.0 | 81.9 | 151.6 |
| range scan 1k | 100000 | warm | 2 | 2 | 1.0 | 0.1 | 60.0 |
| full scan | 1000 | cold | 5 | 5 | 1.0 | 45.8 | 150.9 |
| full scan | 1000 | warm | 2 | 2 | 1.0 | 0.1 | 60.0 |
| full scan | 10000 | cold | 9 | 9 | 1.0 | 399.5 | 277.8 |
| full scan | 10000 | warm | 2 | 2 | 1.0 | 0.1 | 60.0 |
| full scan | 100000 | cold | 51 | 51 | 1.0 | 4111.9 | 1610.3 |
| full scan | 100000 | warm | 2 | 2 | 1.0 | 0.1 | 60.0 |
| projected scan 2/8 | 1000 | cold | 5 | 5 | 1.0 | 45.8 | 150.9 |
| projected scan 2/8 | 1000 | warm | 2 | 2 | 1.0 | 0.1 | 60.0 |
| projected scan 2/8 | 10000 | cold | 9 | 9 | 1.0 | 399.5 | 277.8 |
| projected scan 2/8 | 10000 | warm | 2 | 2 | 1.0 | 0.1 | 60.0 |
| projected scan 2/8 | 100000 | cold | 51 | 51 | 1.0 | 4111.9 | 1610.3 |
| projected scan 2/8 | 100000 | warm | 2 | 2 | 1.0 | 0.1 | 60.0 |
| write 1 row | 1000 | cold | 3 | 3 | 1.0 | 0.2 | 90.0 |
| write 1 row | 10000 | cold | 3 | 3 | 1.0 | 0.2 | 90.0 |
| write 1 row | 100000 | cold | 3 | 3 | 1.0 | 0.2 | 90.0 |
| write 1k rows | 1000 | cold | 3 | 4 | 1.3 | 45.5 | 90.9 |
| write 1k rows | 10000 | cold | 2 | 2 | 1.0 | 44.0 | 60.9 |
| write 1k rows | 100000 | cold | 2 | 2 | 1.0 | 44.9 | 60.9 |

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

**Batch width is 1.0 in every single row of the table.** Not one operation in
the profile issues a parallel request. A cold full scan of 100k rows is 51
sequential waits — 1.6 s modelled — where the leaf hashes are all known from
the internal nodes before any leaf is fetched, so they could go out as one
batch. The `ObjectStore` batch methods exist, the cache forwards them, and the
read path never calls them. This is the largest single latency gap in the
system and it is mechanical to close.

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
| Minimum round trips | **not yet** — batch width is 1.0 everywhere; a 100k scan takes 51 sequential waits |
| Two-digit-ms reads and writes | cold: no (120–1600 ms). warm: 60 ms, all of it the open |
| Single-digit-ms warm reads | **not reachable today** — the open alone is 2 round trips |
| Column projection reduces I/O | **no** — a 2-of-8 projection reads 100% of the bytes |
