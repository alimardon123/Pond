# R2 Validation

Measurements of Pond's storage claims against **live Cloudflare R2**, not local
disk or memory.

This matters because every number previously reported for this design came from
in-memory or local-disk runs, and the existing Phase Q benchmarks in this repo
were local disk at 1–100 keys. A design whose cost model is "round trips" has
to be measured where round trips actually cost something.

Reproduce with:

```bash
set -a && . .env && set +a          # AWS_ACCESS_KEY_ID / SECRET / POND_R2_URL
cargo run --release -p pond_bench --bin r2_validation
```

Credentials are read only from the environment. Every run writes under a unique
prefix and deletes it afterwards.

---

## How to read this

**Read the request counts, not the wall-clock times.**

The run below went through an egress proxy that adds several hundred
milliseconds per request — the baseline single-object GET measured **676 ms
p50**, where R2 accessed directly is typically tens of milliseconds. Absolute
latencies are therefore inflated and are *not* representative of R2.

Request counts are unaffected by that, and they are what the design is built
on. The relationship is visible in the numbers: lookup latency tracks
`GETs × baseline` almost exactly, which is the thesis — round trips dominate,
so the design target is to keep their number constant.

| Baseline: one object GET | p50 | p90 | p99 |
|---|---|---|---|
| (inflated by egress proxy) | 676 ms | 882 ms | 1092 ms |

---

## Claim 1 — constant-depth lookup

GETs per point lookup must stay flat as the collection grows.

| records | tree depth | GETs / lookup | p50 | p99 |
|---|---|---|---|---|
| 500 | 2 | **2.00** | 1.39 s | 3.01 s |
| 5 000 | 3 | **3.00** | 2.24 s | 2.58 s |
| 50 000 | 3 | **3.00** | 2.36 s | 2.85 s |

**100× more data changed lookup cost by 1.5×** — one extra level, then flat.
Depth 3 covers 5 000 and 50 000 alike, and at fanout ~512 the next level would
carry ~10⁸ entries.

Compare with the metadata chain other systems walk on a cold read: Iceberg
catalog → metadata.json → manifest-list → manifest → data is 4–5 sequential
round trips before any data is touched, and grows with table history.

---

## Claim 2 — warm lookup

With the index resident in the cache tier:

| records | node reads / lookup | p50 | p99 | cache hit rate |
|---|---|---|---|---|
| 20 000 | 3.00 | **96.7 µs** | 258 µs | **100 %** |

Every read was served locally: **zero backend requests**. Against a 676 ms
baseline GET, that is roughly a **7 000× latency reduction**, and it is the
single most important number here — it is what makes single-digit-millisecond
reads reachable on top of object storage.

This works with no invalidation logic at all. Cache entries are keyed by
content hash, and a hash cannot name different bytes, so an entry can never go
stale. Correctness is a property of the addressing scheme rather than of a
coherence protocol.

---

## Claim 3 — write amplification

The risk item. An insert rewrites its leaf and every ancestor, and PUTs are the
expensive operation on object storage (~12× a GET on S3 pricing). What matters
is whether batching amortizes it.

| batch size | PUTs | PUTs / record | bytes / record |
|---|---|---|---|
| 1 | 5 | 5.00 | 183 374 |
| 100 | 6 | 0.06 | 2 874 |
| 1 000 | 19 | **0.02** | 660 |

**Batching reduced per-record write cost 263×** on real object storage. A
single-record write costs ~5 PUTs — the tree depth — and a 1 000-record batch
costs 19 PUTs in total.

This is why writes land in a shard first and only compaction pays tree cost:
the shard path is 1 PUT per batch regardless, and compaction amortizes the
tree at these rates.

---

## Claim 4 — listing and reclamation at scale

Two operations whose cost is proportional to the *data* rather than to the
change, and which therefore only misbehave once a bucket is large. Both were
wrong until this run found them.

**Listing past one page.** S3 caps a listing at 1000 keys and continues with a
`NextContinuationToken`. That token is base64, so it ends in `=` padding — and
the client percent-encoded it once when building the URL and again when
building the SigV4 canonical string, so the signature covered `%253D` where the
wire carried `%3D`. Every listing beyond the first page failed with
`SignatureDoesNotMatch`.

| operation | objects | result |
|---|---|---|
| `list_paths("")` | 1 200 | **succeeded**, 8.1 s (previously: hard failure) |

This was not a cosmetic bug. `list_paths` is how the engine discovers writers,
how GC enumerates blobs, and how the benchmark harness cleans up after itself —
and the harness's cleanup silently deleted nothing for exactly this reason,
leaving **974 orphaned objects** in the bucket across earlier runs.

**Bulk delete.** Reclamation cannot be amortised by a tree or served from a
cache, so its request count *is* its cost. S3's `DeleteObjects` takes 1000 keys
per request:

| method | objects | requests | wall clock |
|---|---|---|---|
| one DELETE per object | 1 200 | 1 200 | ~10 min |
| `DeleteObjects` batches | 1 200 | **2** | **8.5 s** |

**600× fewer round trips, ~70× faster** on the same work and the same endpoint.
Extrapolated to a million dead nodes, that is a thousand requests instead of a
million.

---

## What this run also exercised

Several code paths had never executed against a real endpoint before. All pass
(`cargo test -p pond_s3 --test r2_integration -- --ignored`):

- **Multipart upload** — a 120 MB object through create/part/complete, read
  back byte-identical, including a ranged read across a part boundary. Only
  its part-layout arithmetic had been unit-tested; the wire protocol had never
  run.
- **Ranged GET** with a real `Range:` header, including the 416 that is
  translated to an empty result so local and S3 backends behave identically.
- **SigV4 against a non-AWS endpoint** with `region=auto`.
- **Custom CA bundle and proxy support** — added during this work, because the
  client previously trusted only compiled-in roots and ignored `HTTPS_PROXY`,
  which makes it unusable behind a TLS-inspecting proxy or against a private
  S3 deployment. It now honours `AWS_CA_BUNDLE` / `SSL_CERT_FILE` additively.

## Known limits of this run

- **Latency is proxy-inflated** (see above). Request counts are sound; the
  milliseconds are not R2's.
- **Bulk load was the binding constraint, and no longer is.** Every node write
  used to be one sequential PUT, so building a 50 000-record index took minutes
  through the proxy and the largest scale timed out entirely. A tree level is
  now built in full and written in one batch, which S3 issues 32-wide; the run
  that previously timed out completes. Lookup and amplification measurements
  are unaffected — batching changed when writes are issued, never what is
  written, which the byte-identical-root tests confirm.
- **Retry/backoff is not exercised here.** R2 will not produce 503s on demand;
  that path is covered by unit tests on the classification logic.
- Sizes span two orders of magnitude, not the 10⁹ keys the design targets.
  Extending that needs the parallel bulk-load path above.
