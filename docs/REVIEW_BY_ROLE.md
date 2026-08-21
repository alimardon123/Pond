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

**Open:** reading a spilled value costs one extra GET. Scans batch it, point
reads do not. The 1 KiB threshold is reasoned rather than measured against a
real read/write mix, and should be measured.

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

- **Branch.** `Engine::branch` exists and is tested, but `pond branch` still
  goes through the legacy commit chain and reports "no commits to branch from".
  Branching is one of the more compelling things this design makes cheap — an
  O(1) pointer copy — and it is currently unreachable from the CLI.
- **Show history.** The engine publishes heads rather than a commit chain, so
  `pond history` reports "(no commits)". Honest, but the feature is missing
  rather than inapplicable: the head chain could carry it.

Both are gaps rather than defects, and both undersell the architecture.
