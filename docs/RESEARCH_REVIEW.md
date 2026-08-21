# What the external research changes, and what it does not

A three-track research review (prior art, object-storage economics, data
structures) was run against this design. This is the engineering response:
which findings were acted on, which were measured and confirmed, which apply
to a different design than the one in this repository, and which are still
open.

The rule applied throughout: **a claim that can be measured here was measured
here before it was believed.** Several were, and one of them was wrong in the
direction that mattered.

---

## Acted on

### 1. "Coordination-free" is the wrong claim → **sequencer-free**

Correct, and taken. A writer that must PUT to the object store is not available
when partitioned from it, so the store *is* a coordinator; coordination-freeness
is formally equivalent to availability under partition, which makes the
stronger phrase false. Separately, HAT (Bailis et al., VLDB'14) proves
serializability is not achievable with high availability, so "serializable and
coordination-free" contradicts a published impossibility result.

What Pond removes is the coordinator it would otherwise have to **operate** — a
transactor, a catalog, a metadata service, a CAS loop on a hot pointer. It
rents a linearizable namespace; it does not run one. That is still the
differentiating claim, so there was nothing to gain by overstating it.

Changed in `NON_GOALS.md` and `STRATEGIC_REVIEW_2026.md`.

### 2. Adversarial keys — **confirmed exploitable, then bounded**

The research flagged this from ATProto's production security notes. It applied
directly, and worse than the source suggested, because the minimum chunk size
was two entries.

Boundaries are decided by a hash of the key, and keys are application data. A
writer can search for keys whose fingerprint lands on a boundary and insert
only those, driving every chunk to the minimum — and with it the fanout, and
therefore the depth. Measured (`cargo run -p pond_bench --bin adversarial`):

| keys | nodes | depth |
|---|---|---|
| 20 000 mined | **10 009** | 3 |
| 20 000 ordinary | 20 | 2 |

**500x the objects**, at a mining rate of ~2 600 keys/s on one core of a
laptop. The gap widens with scale: a floor of two entries means a fanout of
two, so depth grows as log₂(n) rather than log_target(n) — depth ~20 at a
million keys instead of 2.

Fixed structurally rather than heuristically: the chunk floor is now a fraction
of the target (`min_entries_for`), so whatever the keys, fanout cannot fall
below it and depth cannot exceed log(n)/log(target/4). It costs nothing on
honest data, where chunks average the target and the floor almost never binds.

The research also suggested salting the boundary function per shard. That is
worth doing and is *not* done yet — see Open below — but it is a weaker
defence than the floor, because a writer who can read the collection can read
the salt.

### 3. Object-storage economics → node size retuned

The cited numbers (a GET billed per request; a ranged GET billed as a full one;
latency dominated by a fixed base at these sizes; no benefit below ~100 KB)
invert the classic node-size trade. Depth is the count of *dependent* round
trips, and those cannot be overlapped because each node names the next — so a
level saved is worth far more than the bytes it costs.

Measured over 4 M entries of ~100 bytes (`--bin nodesize`):

| target | depth | avg node | bytes rewritten per insert |
|---|---|---|---|
| 512 | 3 | 65 KB | 195 KB |
| 2048 | **2** | 256 KB | 511 KB |

Default raised to 2048. The expensive axis gets cheaper and the cheap axis gets
dearer, which is the right direction on this substrate.

Two defects surfaced while measuring:

- **A spurious boundary near the top cost a whole level.** With far fewer than
  `target` children, the expected number of boundaries is well under one; when
  one fired anyway it inserted a level rather than dividing anything usefully.
  A 128-child level split in two, turning a 2-deep tree into a 3-deep one.
  `chunk_level` now emits one node when the level fits in one.
- **The chunk config was never persisted**, despite a comment claiming it was
  "recorded in the root". Since it decides where boundaries fall it decides
  every node hash, so tuning the default would have silently rechunked existing
  collections. It is now pinned per collection in the definition.

---

## Confirmed, already known

- **The raw-bytes lens path rewrites the whole collection per append** —
  independently measured here before the review arrived (14 → 521 bytes across
  40 appends, O(N²)). Tracked; the streaming, OLTP and key-value lenses need to
  move to the engine's append path.
- **LIST is priced like a PUT.** `Reader::open` issues exactly one LIST, to
  discover writers, and its cost is O(writers) rather than O(data). Point reads
  issue none.

---

## Applies to a different design

Much of the document describes an architecture with Facts, Seals, a Horizon and
a Ripple plane. **None of that vocabulary exists in this repository** — it is
from a separate design track. The parts that assume it do not transfer as
written:

- **The prefix-stability bug** ("lexicographic enumeration is not a
  prefix-stable order") is a property of deriving a total order from *object
  name enumeration*. This engine derives no such order. Writers are partitioned
  by namespace, each publishes its own head, and readers merge them as a
  semilattice join — commutative, associative, idempotent — so there is no
  enumeration whose ordering could be violated by a late arrival.
- **The Horizon / visibility-watermark machinery** answers a question this
  design does not ask.

The C1 critique is the one that needs care, because it is aimed at a real
property of this code and is *partly* right — see below.

---

## The C1 critique — right in substance, and the fix is already the roadmap

The claim is that a prolly tree must index **run descriptors**, not rows, and
that an LSM belongs underneath. The evidence (Noms archived; Dolt's own
"prolly trees are not a great match for columnar storage"; Irmin removing
content addressing for a 360x index shrink; lakeFS reaching 10 PiB precisely by
content-addressing metadata and *not* data) is strong and consistent.

It is also, in substance, what `STRATEGIC_REVIEW_2026.md` already lists as P2:
records currently live inline as index values, which is right for small values
and wrong for a scan, and the sizing that turns 1 PB into ~8 M index entries
assumes the index addresses *segments*, not rows.

What the review adds is urgency and a sharper cost argument, and one number
worth keeping: the amplification floor for a copy-on-write tree is
chunk_size/record_size. That is a real bound on the unbatched single-row write,
and it is visible in this repo's own R2 measurements — 5.00 PUTs per record
unbatched, falling to 0.02 at a 1000-row batch. Batching is not an optimisation
here either; it is the operating point.

**Not yet acted on.** Moving records behind segment descriptors is a format
change of the same size as the engine itself, and doing it well needs the
segment layer (P2) first. It is the next structural piece of work, and the
research raises its priority above the remaining lens conversions.

---

## Open

1. **Salt the boundary function per collection.** Weaker than the chunk floor,
   but it stops a single mined key set from transferring between collections.
2. **Erasure / crypto-shredding.** The strongest prediction in the review, and
   entirely unaddressed here: immutable content-addressed storage has no
   mechanism for "delete this subject's data", and every system in the lineage
   discovered that late and paid for it. Encrypt per subject, keep keys in a
   small mutable keystore, destroy the key to erase. Structural GC handles
   cost; it does not handle law.
3. **Segments (C1 above).**
4. **Filters sized for object storage.** A false positive costs a billable
   request and tens of milliseconds, not the microseconds it costs on NVMe.
   Relevant once filters exist; they do not yet.
