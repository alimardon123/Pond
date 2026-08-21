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

**Erasure is still unaddressed and is the largest open risk.** Immutable
content-addressed storage has no mechanism for "delete this subject's data",
and spilling makes it harder rather than easier: a deduplicated blob may be
referenced by rows belonging to different subjects, so deleting a row does not
free it. The known answer is crypto-shredding — encrypt per subject, destroy
the key — and it has to be designed in rather than retrofitted.

---

## Data architect

**`NULL` becomes `0`.** Verified through the CLI: writing `{"score": null}`
reads back `{"score": 0}` — indistinguishable from a real zero. This is a
`TypedColumn` limitation, which has no null representation, so it affects the
legacy path identically and is not a cutover regression. `PondColumn` has a
`null_bitmap` field the write path never populates, and `Value::Null` exists in
the record model, so the information is lost precisely at the `TypedColumn`
boundary. Fixing it means changing a type used by thirteen crates.

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

**Those three lenses rewrite the whole collection per append** — measured at
14 bytes growing to 521 across 40 appends, so O(N²). For a *streaming* lens
that is the worst possible characteristic, and it is a property of the lens
design rather than the storage beneath it.

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
