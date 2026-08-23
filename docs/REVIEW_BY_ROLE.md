# Review by role

The same work looked at from several angles, because each one asks a different
question and they find different things. This is the record of what each found,
what was fixed, and what is still open — including the findings that were
uncomfortable.

The session's most serious bug was found by asking an operations question, not
a correctness one, about code whose correctness tests were all green.

---

## Critic

**"The obvious design breaks the thing you built the system for."**

The first plan for spilling large values was the standard one: pack many values
into a large segment object, store `(segment, offset, length)` in the index.
That is what every columnar system does and it would have been wrong here.
Which segment a value lands in depends on what else the writer happened to be
writing — so two writers holding identical data would produce different
descriptors, different node bytes, different root hashes. Convergence,
structural sharing and deterministic merge would all have died together, and
the tests that would have caught it are the ones about *two writers*, not the
ones about round trips.

Rejected in favour of a descriptor that is a pure function of the value: its
content hash. Identical values now deduplicate, which packing would have
prevented. `the_pointer_is_a_pure_function_of_the_value` pins it.

**"You fixed that class of bug once and then reintroduced it."** The chunk
target was pinned per collection because it decides node hashes. The spill
threshold decides whether a value is a record or a pointer — the same class of
decision — and was left as a constant. Two writers on different versions would
have produced different bytes for identical data. Fixed by `EngineConfig`, so
nothing affecting content addressing is read from a constant at write time.

---

## Site reliability / operations

**`pond gc` deleted engine collections. Two rows before, zero after.**

This is the finding that matters most and no correctness test would have found
it, because nothing was incorrect: the writes were right, the reads were right,
the merge was right. The failure was that a *different* subsystem had never
been told a new kind of reachability existed.

GC resolves refs with `kernel.resolve`, which reads `{"hash":"..."}`. Engine
heads hold their bytes directly, so `resolve` returned `None`, the heads were
unreachable, and every index node and spilled value was classified as garbage.

Fixed by walking heads → roots → index nodes → spill targets. All four levels
matter: stopping at the leaf keeps the tree and deletes the values it names,
leaving rows that decode to nothing. `tests/gc_safety.rs` covers each level and
was verified to fail without the fix.

**The general lesson, worth more than the fix:** every time data becomes
reachable a new way, everything that reasons about reachability has to learn
it. The audit that followed — running every CLI command against an engine
collection — found the rest fail honestly (`history`, `branches`, `undo`,
`revert`, `checkout` all report a clear error) rather than silently doing the
wrong thing.

---

## Performance

Two measurements changed decisions rather than confirming them.

**Node size.** On disk the trade is familiar; on object storage it inverts,
because a read costs a request and a ranged GET bills like a full one. Depth is
the count of *dependent* round trips and cannot be overlapped. Measured at 4 M
entries: target 512 gives depth 3 at 65 KB/node, target 2048 gives **depth 2**
at 256 KB/node. Retuned.

**Value size.** A one-row update at 100 KB values rewrote **322 MB**, because
the leaf holds thousands of entries and is rewritten whole. After spilling,
270 KB. The 100-byte case is unchanged and still rewrites 318 KB per
single-row update — inherent to a copy-on-write tree with small rows, and what
batching amortises (0.02 PUTs/record at 1000-row batches on R2).

**The spill threshold was measured, and the reasoning had been wrong.** 1 KiB
was argued from leaf arithmetic; measurement showed it is the one band where
spilling loses at *every* read/write mix — the bytes saved do not pay for the
requests added. Raised to 4 KiB.

The first version of that benchmark priced only requests and concluded spilling
never wins. That was the wrong cost function: a leaf rewritten whole is one PUT
however large it is, so counting requests alone makes an inline 200 MB leaf
look cheaper than a spilled 200 KB one. The uniformly negative result is what
exposed the modelling error.

**A measurement that could not be made stable, and why that is a finding.** The
one-row flush cost swung between 0.5x and 2.2x on identical inputs. Averaging
over probes narrowed it but did not fix it, because the cause is a feature: each
collection draws a random chunk salt, so boundaries fall differently every run
and leaves differ in occupancy. The absolute threshold was removed rather than
loosened until it passed — a flaky test is worse than none — and the comparative
assertion, which is stable in direction, carries the claim.

**Open:** `tree_for` writes an empty node when a collection is first touched —
a wasted round trip per new collection.

---

## Security

**Adversarial keys — confirmed exploitable, then bounded.** Boundaries are
decided by a hash of the key and keys are application data, so keys can be
mined. Measured: 20 000 mined keys produced 10 009 nodes against 20 for
ordinary keys — 500x the objects, at ~2 600 keys/s on one core. Bounded by
raising the chunk floor to a fraction of the target: re-measured at 3.7x with
no extra level, against a 4x theoretical worst case. A per-collection salt was
added on top, and is described as the weaker defence because it is not secret
from a reader.

**Content addressing is an existence oracle.** A spilled value is stored under
the hash of its content, so anyone who can probe the store and *guess* a value
can confirm it exists. That is inherent to content addressing, not new here,
but spilling widens it from index nodes to user data. It matters for a
multi-tenant deployment and should be stated in any security review.

**Erasure — mechanism built, not yet wired.** `core/crypto` implements
crypto-shredding: deterministic ChaCha20-Poly1305 with a synthetic IV, so
encryption preserves content addressing and dedup rather than destroying them,
and a keystore of named objects whose deletion makes a subject's ciphertext
noise everywhere at once. Seventeen tests, including the end-to-end contract.

Deliberately honest about what it is not: there are no callers on the data
path. The write path does not seal, the read path does not open, and no policy
says which fields belong to which subject. `docs/ERASURE.md` lists those in
order and states the costs — no cross-subject dedup, deterministic encryption
confirms guesses for key holders, and erasure is exactly as complete as the
destruction of the last copy of the key.

---

## Data architect

**`NULL` became `0` — fixed.** Writing `{"score": null}` read back
`{"score": 0}`, indistinguishable from a real zero, on both storage paths.
PND2 had no per-value null representation in either direction;
`PondColumn::null_bitmap` existed but was hardcoded `None` in every decoder arm
and written by no encoder.

Fixed additively behind a header flag, so a blob with no nulls is byte-identical
to before and older readers never look further. Threaded through the CLI, the
columnar bridge, `read_pnd2`, SQL, Python and the lakehouse lens.

**The first fix was incomplete, and that is the more useful finding.** It
landed in the CLI only, so `SELECT` still returned `0` where `read-rows`
returned `null` — worse than the original bug, because the answer depended on
how you asked. The cause was three independent implementations of "decoded
columns to JSON rows". There is now one, in `pond_core::to_json`, which makes
that class of divergence impossible rather than merely fixed — and
consolidating exposed a latent gap where SQL and Python had no `VT_BOOLEAN`
case at all.

**Row identity is explicit, and that is right.** A supplied `_rowid` names the
row; its absence means "these are new rows". The alternative — position — was
tried and silently made every write overwrite the previous one.

**Reader cost is linear in the number of writers that have *ever* published,
and nothing bounds it.** This is the largest open item in the design and it was
not visible until requests and round trips were counted separately.

The convergence story is that each writer publishes to its own head key and
readers merge every head they find. That removes coordination, and it is why
there is no CAS anywhere. What it also does is make the read path grow with the
writer population: heads are created by `publish` and are never removed, never
folded, and never expired. A writer that published once, years ago, is still
read on every reader open forever.

Measured (`cargo run --release -p pond_bench --bin writerscale`), holding data
per writer constant so only the writer count varies:

| writers | rows | reader open (reqs / round trips) | first scan (reqs / round trips) |
|---|---|---|---|
| 16  | 64   | 17 / 2  | 31 / 31   |
| 64  | 256  | 65 / 2  | 127 / 127 |
| 256 | 1024 | 257 / 2 | 511 / 511 |

Two different things are in that table, and only one of them is a problem.

*Reader open is fine.* 257 requests at 256 writers, but **2 round trips** — one
LIST, one batched read of every head. The cost is linear in requests and flat in
waits, which is exactly what the design intends, and it is only true because
`get_object_batch` is genuinely batched end to end.

*The first read of a collection was not fine.* 511 requests in 511 **dependent**
round trips, because the fold over writer roots was a left fold: each merge
needed the previous result. At an object-storage RTT that is roughly fifteen
seconds to open a collection holding a kilobyte of data.

Merge is a semilattice join — associative and commutative, asserted on the root
hash by `merge_is_idempotent_and_associative` — so the fold can be re-associated
into a balanced reduction whose levels are independent and run in parallel. That
is now what `Reader::reduce` does, and it produces a byte-identical root.

**It buys 3x, not `log W`, and the measurement is what corrected the estimate.**
The tempting argument is `log2(W)` levels so `log2(W)` waits; it is wrong,
because level `k` holds `W/2^k` merges over trees of about `2^k` entries and a
merge descends both sequentially. Depth stays `O(W)`. What improves is the wide,
cheap bottom of the reduction, and it plateaus:

| writers | wall clock | if fully sequential | speedup |
|---|---|---|---|
| 32  | 16 ms | 32 ms  | 2.0x |
| 128 | 44 ms | 128 ms | 2.9x |
| 256 | 86 ms | 256 ms | 3.0x |

Widening the fan-out past 32 makes it worse — 2.5x at a fan-out of 256 — as
thread scheduling overtakes the overlap gained.

**The ceiling is now removed, by compaction rather than by a faster fold.** 3x
off a line is still a line, and "multi-writer from any lens, any user worldwide"
implies a writer population that grows without bound. `pond compact` (or
`pond_engine::compact_heads`) reads every head, merges their roots, and
publishes the result as a single head under a reserved writer id whose
`observed` map names each head it absorbed. Readers drop any head sitting at an
absorbed identity, so they merge one root instead of W:

| writers | first scan, dependent round trips | after compaction |
|---|---|---|
| 16  | 31  | 1 |
| 64  | 127 | 1 |
| 256 | 511 | 1 |

Flat, not merely smaller.

**What makes it safe without a compare-and-swap** is that heads are claimed by
the *content hash of their bytes*, not by writer id. The dangerous case is a
writer publishing while compaction runs: its new head has different bytes, so a
different hash, so it does not match what was absorbed and is merged normally.
There is no window in which a publish can be swallowed. Both race tests fail if
the claim is changed to a writer id, which is how that was checked rather than
argued.

One further property falls out of the same choice: if a head is wrongly *not*
skipped, merge is idempotent, so the reader folds it in twice and gets the same
tree. The optimisation can cost time, never correctness.

Anyone can run it, any number of times, concurrently. A pond that is never
compacted is slower, never wrong.

**The head key carries the content hash, which is what makes the skip decidable
from the listing.** Without it a reader has to fetch every head to discover it
can ignore one, and the read path stays linear in writers however much
compaction runs. With it:

| writers | reader open + scan, after compaction |
|---|---|
| 4   | 3 requests |
| 32  | 3 requests |
| 128 | 3 requests |

Flat in requests, not only in merges.

The same hash makes deletion safe, so compaction retires what it absorbed
rather than only shadowing it. A pass deletes exactly the keys its own listing
returned; a writer publishing during the pass wrote a key that listing never
contained, so it cannot be deleted. No conditional delete is required — which
matters because object stores do not offer one and a local filesystem does not
either.

`publish` deliberately does *not* delete the head it supersedes. That would
cost a round trip on the commit path to save readers nothing, since
`latest_heads` never fetches a superseded key. Retirement is maintenance, like
garbage collection, and compaction sweeps superseded sequences along with
absorbed heads. The cost of the new layout on the write path is one LIST, to
recover the writer's own head on open: 6 round trips per engine write against
the legacy path's 8.

**The migration hazard here was real and was found by testing it rather than
reasoning about it.** The old layout put a flat object at `heads/writer-<id>`.
The first version of the new layout used `heads/writer-<id>/<seq>.<hash>` — on
an object store those coexist happily, because there are no directories, but on
a local filesystem a directory cannot share a path with a file, so an upgraded
pond failed its first publish with `EISDIR`. Keys now live under
`heads/writer/<id>/`, which cannot collide with any old key.

Two further compatibility points, both found the same way. A writer opening on
an upgraded pond finds nothing under its new prefix, so it falls back to the
legacy key — without that it starts empty, publishes at a higher sequence, and
its old rows are superseded by a head that does not contain them. And
`latest_heads` groups by (writer, layout) rather than by writer, so a
pre-sequence head is never treated as a stale version of a current one: the
writer that wrote it may never run again, and nothing else holds its rows.

**Column pruning does not exist yet.** A scan that wants one field still reads
whole records, and a spilled record is fetched whole. That is what a PAX
segment layout would fix, and it is the remaining half of the "index
descriptors, not rows" argument.

---

## Systems architect

**Two storage paths still exist**, and the dispatch is per collection with
absence meaning legacy — which is what let the engine land without migrating
anything. `manifest.rs` stays until the streaming, OLTP and key-value lenses
move off the raw-bytes API.

**All three lenses are off the whole-collection rewrite.** Measured:

| lens | before | after |
|---|---|---|
| streaming, 60 appends | 22 880 → 227 838 B (10.0x) | 5 519 → 14 027 B (2.5x) |
| key-value, point lookup | full scan | 1 read at 100 pairs, 2 at 5 000 |
| OLTP, one-row flush | 164 255 → 669 256 B (4.1x) | plateaus |

The shapes matter more than the ratios: the engine's cost is bounded by one
leaf, the rewrite's by the collection, so the gap widens without limit.

**Everything that decides bytes is now pinned per collection** — chunk target,
chunk salt, spill threshold. That invariant is worth stating explicitly,
because it has been violated twice by different mechanisms and both times the
symptom would have been silent divergence rather than an error.

---

## Product

An engine-backed collection can be created, written, read, queried with SQL,
and reached from Python and the lenses that speak columns. What it cannot do
yet:

- **Branch.** Reachable now, and coherent. `pond branch` dispatches to the
  engine, and the failure it used to have was worse than being unreachable: it
  reported success while `pond branches` reported none. Both were telling the
  truth about a different model. A branch here is an independent collection
  that shares structure with its source — that is what makes it an O(1)
  pointer copy — so there is nothing inside the source to list.

  The definition now records `branched_from` (format v4; v3 reads back as "not
  branched", and a hand-built v3 definition is decoded in the tests rather than
  assumed to work). `pond branches` lists the collections that name this one as
  their source, and branches of branches chain. Provenance confers no
  behaviour: a branch diverges, is written and is deleted exactly like any
  other collection.
- **Show history.** The engine publishes heads rather than a commit chain, so
  `pond history` reports "(no commits)". Honest, but the feature is missing
  rather than inapplicable: the head chain could carry it.

Both are gaps rather than defects, and both undersell the architecture.
