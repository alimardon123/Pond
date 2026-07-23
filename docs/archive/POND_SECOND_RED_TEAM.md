# Pond Second Red Team Review

> Phase K.2 — Adversarial Falsification of the Mathematical Model.
> Not the implementation. Not the SDK. Only the model in
> `POND_MATHEMATICAL_MODEL.md` and `POND_FORMAL_ALGEBRAS.md`.
>
> **Question on trial:** Is the model — three primitives, eight
> algebras, twelve architecture laws — *inevitable*, or is it one
> convenient implementation wearing a formal costume?

---

## 0. Method

Six hostile architects sit at the table. Each is told to attack the
model from the perspective of a system they shipped. They are
*not* permitted to suggest features. They *are* permitted to:

- demand that an "algebra" be demoted to an "implementation,"
- demand that a hidden primitive be promoted to a first-class one,
- demand that a circular definition be broken,
- demand that an under-specified law be tightened into a theorem or
  withdrawn,
- declare that two algebras are the same algebra wearing two coats.

Each attack is given a severity:

- **S0 — Cosmetic.** Wording issue. No model change.
- **S1 — Under-specification.** The law is real but ambiguous; tighten.
- **S2 — Hidden primitive.** The model claims three primitives but
  silently depends on a fourth (or fifth). Promote or remove.
- **S3 — Circular definition.** An algebra depends on something it
  claims to derive. Break the loop.
- **S4 — False law.** A stated law is provably wrong. Withdraw or
  restrict.
- **S5 — Collapse.** Two "algebras" are one algebra. Merge them.

The verdict at the end tallies the severities and lists the
mandatory model changes.

---

## 1. The Panelists

| # | Architect | Shipped | What they care about |
|---|---|---|---|
| 1 | FoundationDB architect | FDB layer system, MVCC, atomic writes | Layered storage, transaction semantics, what "atomic" really means |
| 2 | Git architect | Git object database, packfiles, commit-graph | Content addressing, DAG semantics, what the kernel "knows" |
| 3 | Dolt architect | Dolt SQL database on top of a content-addressed prolly-tree store | How SQL semantics survive on top of immutable bytes |
| 4 | Iceberg architect | Iceberg catalog / snapshot / manifest-list / data-file layering | The boundary between logical and physical layout |
| 5 | Pebble/RocksDB architect | LSM tree, SST files, compaction | What "physical structure" actually means in a real engine |
| 6 | WarpStream architect | WarpStream object-store-native Kafka | What "object-store-native" really costs |

They have read the model. They have read the eight algebras. They
are hostile. Begin.

---

## 2. Attacks

### A1 — "Three primitives" is question-begging (S2)

**FoundationDB architect:**

> The kernel exposes `Write(bytes)→hash`, `Read(hash)→bytes`,
> `Ref(name,hash)`. You call this three primitives. But `Ref` is not a
> primitive — it is an *operation on a metadata store*. The metadata
> store is the primitive; `Ref`, `get`, `list`, `delete`, `CAS` are
> its operations.
>
> Concretely: in your `Reference Algebra §1.3` you list five
> operations on references: `set`, `get`, `list`, `delete`,
> `compare_and_swap`. These are the operations of a key-value store.
> You have not formalized the key-value store; you have formalized its
> API.
>
> In FDB we made this distinction explicit: the *storage subsystem*
> is a primitive; the *mutation log* (we call it the sequencer) is
> another primitive; the *commit proxy* is another. Three primitives
> is a marketing number. The honest count is: bytes layer + name
> layer + (concurrency control layer you have not formalized).

**Pond model author (defending):** `Ref` is one primitive (the
operation). The metadata store is its *implementation*. The model
describes the operation, not the substrate.

**FoundationDB architect (rebuttal):** Then `Write` is also one
operation on the bytes substrate — but you *do* formalize the bytes
substrate by axiom A1 (Immutability) and A2 (Content-addressing).
You formalize the substrate for the bytes layer but only the API for
the names layer. That asymmetry is the giveaway: you intuitively
understood that the bytes layer needs laws, but you treated the names
layer as if it were trivial. It is not. `compare_and_swap` is a
distributed-coordination primitive. S3 does not provide it. Your
model assumes it and pushes the problem to "the backend decides."

**Verdict:** Promote `Name` to a first-class substrate with its own
axioms. The kernel is **three substrates**: bytes, names, time (the
last is implicit, see A4). The three *operations* are the API on top.

---

### A2 — The Reference Algebra silently assumes a metadata store with transactional semantics (S2)

**WarpStream architect:**

> Your reference laws R1 (Atomicity) and R3 (CAS) are claims about a
> transactional metadata store. On S3, neither holds natively.
> R1 (`set` is atomic) is true *for a single object write* but you
> use it as if `set(name, hash)` were a transactional update of a
> row in a table. R3 (CAS) requires conditional writes, which S3
> supports only via `If-None-Match: *` (create-only) — not via
> conditional update of an existing key.
>
> You wrote in §1.5: "CAS requires 2 RTTs on S3 (no native CAS)."
> That is wrong. Two RTTs of *attempt* does not give you CAS — it
> gives you last-writer-wins with a race window. Two writers can both
> read the expected value, both write, and one wins. CAS requires
> either: (a) a real CAS primitive in the metadata store, or
> (b) consensus, or (c) accepting LWW with conflict detection
> post-hoc.
>
> Your OSN7 ("no local metadata dependence") directly collides with
> R3. You cannot have both without adding a fourth substrate: a
> coordination service.

**Verdict:** Either:

- withdraw OSN7's claim of full CAS support, and admit the model
  allows only LWW + post-hoc conflict detection, or
- add a fourth substrate (coordination/consensus) and formalize it.

This is **S2** (hidden primitive).

---

### A3 — The Lens Algebra's `L7 (Context-based interpretation)` is circular (S3)

**Dolt architect:**

> You say: "the codec used to decode a blob is determined by the key
> (context)." Fine. But the Resolver — `Resolver.decode(key_prefix,
> bytes)` — is itself code that maps key prefixes to codecs. Where
> does that mapping live?
>
> If the mapping lives in the kernel, then the kernel knows about
> codecs, which violates `L5` (kernel independence). If the mapping
> lives in the Lens, then the Lens must be bootstrapped before any
> blob can be decoded — but Lenses are themselves encoded as blobs
> (or as code in the application) — so how does the kernel know
> which Lens to invoke for the key prefix `lens/`?
>
> In Dolt we hit exactly this. The answer is: the Resolver is
> *out-of-band*. It is application code linked into the process. The
> kernel never sees it. But that means your `L7` is not a kernel
> law at all — it is an application convention. The kernel is
> codec-agnostic not by law but by *omission*: it doesn't decode,
> ever. Decoding is done by the application, which is free to use
> any strategy (key prefix, blob header, sidecar manifest, magic
> bytes, sniffing).
>
> Your `L7` makes context-based interpretation look like a law of the
> model. It is not. It is one strategy the application may choose.

**Verdict:** Demote `L7` from a Lens law to an *application
convention*. Replace it with:

> **L7' (Kernel never decodes).** For all `b`, `Read(Write(b)) = b`.
> The kernel returns bytes; interpretation is performed exclusively
> outside the kernel.

This is **S3** (circular): the Resolver was secretly an application.

---

### A4 — The Lens Algebra's `L6 (Composition)` is false (S4)

**Iceberg architect:**

> `L6` says: `E_{L₁⊕L₂}(s₁, s₂) = E_{L₁}(s₁) || E_{L₂}(s₂)` where
> `||` is byte concatenation. This is wrong, and Iceberg proves it.
>
> A Lens that encodes Arrow IPC frames and a Lens that encodes JSON
> rows cannot be composed by concatenation — the decoder cannot tell
> where one ends and the other begins. Real composition requires a
> *framing format*: length-prefix, tag-prefix, or TLV. Concatenation
> works only for self-delimiting codecs.
>
> Worse, `L6` claims `D_{L₁⊕L₂}(b) = (D_{L₁}(b₁), D_{L₂}(b₂))` —
> but `b = b₁ || b₂` is undefined without a delimiter. So `D` cannot
> recover the split. The "law" is a category error.
>
> In Iceberg, composition is *manifest-based*: the manifest lists
> data files, each with its own codec. The composition is at the
> file level, not the byte level. Pond should formalize this.

**Verdict:** Withdraw `L6` in its current form. Replace with:

> **L6' (Composition at the name level).** If `L₁` and `L₂` are
> Lenses over disjoint name prefixes, then both can be hosted in
> the same kernel without interference. Composition is *by names*,
> not by byte concatenation.

This is **S4** (false law). Mandatory withdrawal.

---

### A5 — The "Physical Structure" hypothesis is true only because the definition is rigged (S5)

**Pebble architect:**

> You define Physical Structure as `(Source, Function, Artifact)`
> where `Function: Snapshot → bytes` is a pure function. Then you
> say: "every storage optimization is a Physical Structure, except
> cache." That is not a discovery. That is the definition. You have
> *defined* optimization-as-pure-function, then *discovered* that
> optimizations are pure functions.
>
> The real question is: which structures can be expressed as pure
> functions of the snapshot? You answered: all of them, except those
> that depend on access patterns. So the actual content of the
> "algebra" is: "things that depend only on the snapshot are things
> that depend only on the snapshot." That is a tautology.
>
> In Pebble we have: SST files (immutable, derived from the LSM
> state), bloom filters (derived from SST contents), block indexes
> (derived from SST layout), table properties (derived from SST
> contents), compaction outputs (derived from inputs *and* the
> compaction strategy). The last is *not* a pure function of the
> snapshot — it depends on the compaction *schedule*, which is
> itself a function of access patterns and disk pressure. We do not
> call compaction a Physical Structure. We call it a maintenance
> operation.
>
> Your "Cache is not a Physical Structure" carve-out is the same
> pattern: when something is non-deterministic, you declare it
> out-of-scope. The algebra shrinks until everything inside it is
> deterministic-by-definition.
>
> Honest restatement: **a Physical Structure is anything that can be
> lost without data loss, because it can be recomputed.** That is
> the only claim. Everything else (determinism, rebuildability,
> independence, composition) follows from "can be recomputed."

**Verdict:** Demote the Physical Structure "algebra" to a single
definition + one theorem:

> **Definition.** A Physical Structure is an artifact `A` paired
> with a recompute function `f` such that `f(snapshot) = A`.
>
> **Theorem P1 (Rebuildability).** Every Physical Structure can be
> lost without data loss.

Everything else in §5 of the mathematical model and §6 of the formal
algebras is implementation detail, not algebra. This is **S5**
(collapse).

---

### A6 — The RTT Calculus is correct but ignores the dominant cost: bytes transferred (S1)

**WarpStream architect:**

> Your RTT cost table is in RTTs. But on object stores, RTTs are
> *not* the dominant cost. Bytes transferred (egress fees) and
> per-request costs (PUT/GET/LIST count) are. S3 charges
> ~$0.0004 per 1000 PUTs, ~$0.0004 per 1000 GETs, and ~$5/TB
> egress. A scan that takes 4 RTTs but transfers 100 GB costs
> $0.50 in egress. A scan that takes 40 RTTs but transfers 1 GB
> costs $0.005 in egress. Your model declares the second one
> worse (40 > 4 RTTs). The bill says the opposite.
>
> Your `Cost` vector in §4.1 *does* include `bytes_transferred`,
> but your theorems T1-T4 only bound RTTs. The theorems are true
> but economically misleading. You need a dollar-cost theorem or
> you need to stop using RTTs as the proxy.

**Verdict:** Add a `Cost-in-dollars` theorem:

> **T5 (Dollar bound).** For any operation `op`, the dollar cost is
> bounded by `α × RTTs + β × bytes + γ × requests`, where α, β, γ
> are backend constants. The RTT budget is *necessary but not
> sufficient*.

This is **S1** (under-specification).

---

### A7 — GC's reachability requires reading blobs, so "logical" and "physical" reachability diverge (S2 + S3)

**Git architect:**

> You define reachability in §3.3 of the formal algebras as:
> "a blob is reachable if there exists a path from any Reference
> to it." Then you say `path(ref, blob)` involves reading the
> ref's target and following hashes transitively. This requires
> *reading every blob on the path*.
>
> In Git, the packfile solves this with an index: `.idx` lists
> every blob hash in the pack, so reachability reduces to
> "is the hash in the .idx?" — no blob read needed. You call this
> "manifest-based GC" and list it as an optimization. But it is
> not an optimization. It is a *different reachability definition*.
>
> Logical reachability: "the blob is transitively referenced from
> some Ref." Requires blob walks.
> Physical reachability: "the blob is in some pack that is
> transitively referenced from some Ref." Requires only manifest
> reads.
>
> These two are *not equivalent*. A blob can be logically reachable
> but physically absent (the pack was deleted; the blob still
> exists as a standalone object). A blob can be physically present
> (in a pack) but logically orphaned (no Ref reaches the pack).
>
> Your GC algebra does not distinguish these. It must.

**Verdict:** Add a second GC algebra — **Physical Reachability** —
that operates over (manifest, pack) pairs rather than over raw
blobs. Define the equivalence condition under which physical and
logical reachability coincide. This is **S2 + S3** (hidden
primitive + circular definition: GC claims to operate on the kernel
but actually operates on a manifest-augmented kernel).

---

### A8 — The Workspace Algebra's `W2 (Atomicity)` is a distributed transaction claim with no protocol (S4)

**FoundationDB architect:**

> `W2` says: "`commit(ws)` either commits all staged changes or
> none." This is atomicity. Atomicity across what? Across multiple
> blobs and multiple references. The kernel has no atomic
> multi-write primitive. `Ref(name, hash)` is atomic for one name.
> There is no `RefBatch([{n1,h1}, {n2,h2}, ...])`.
>
> To implement `W2`, you need one of:
>
> 1. **A commit blob** that lists all the writes, then a single
>    `Ref` update pointing HEAD to the commit blob. This works
>    only if all writes go through HEAD. Cross-Collection atomic
>    writes still need a higher-level coordinator.
> 2. **2PC** — a coordinator that writes a prepare record, then
>    commits. The prepare record is itself a blob. The coordinator
>    must be a separate substrate.
> 3. **Consensus** — Raft/Paxos over the name layer. The name layer
>    becomes a replicated log.
>
> Your model picks (1) implicitly (commit blobs are atomic via the
> HEAD ref), but the Workspace algebra pretends to support
> cross-Lens transactions (`W4`), which (1) cannot deliver.
>
> Either `W4` is wrong (workspaces are per-Collection, not
> cross-Collection) or you need a coordinator substrate.

**Verdict:** Restrict `W4` to within-Collection atomicity.
Cross-Collection atomicity is out of model scope unless a
coordinator substrate is added. This is **S4** (false law).

---

### A9 — The "Merge is a snapshot" law (M4) is a workaround, not a law (S1)

**Dolt architect:**

> `M4`: "A merge commit always contains a snapshot." Why? Because
> "merge must produce a consistent state that doesn't require
> replaying two parent chains." That is not a law of the model.
> That is a workaround for not having a delta-merge algorithm.
>
> In Dolt we *do* have delta merges — the merge of two Prolly-tree
> deltas is itself a delta, applied to a common ancestor's
> snapshot. The merge commit carries the delta; readers materialize
> on demand. This costs an extra ancestor lookup but saves the
> snapshot write.
>
> Your `M4` declares this optimization illegal. It should declare
> it *optional*.

**Verdict:** Demote `M4` from a law to a default policy. Replace
with:

> **M4' (Merge has a well-defined result).** A merge commit's
> `snapshot` field may be either a full snapshot or a delta
> relative to `parent`. If it is a delta, readers materialize by
> replaying parent's snapshot plus the delta.

This is **S1** (under-specification).

---

### A10 — The "History as Physical Structure" claim is correct but underspecified (S1)

**Git architect:**

> §8 of the formal algebras concludes that "history is a Physical
> Structure." Correct. But the function is not `f(snapshot) →
> history_graph`. The snapshot is one state. History is over
> *commits*, not over snapshots.
>
> The correct formalization is: `f(commit_set) → history_graph`.
> The commit set is the *source*, not the snapshot. Two different
> snapshots can share commits (a snapshot is a function of a
> commit, not the other way around).
>
> This is not a nit. It changes the dependency graph: history
> depends on commits, not on snapshots. A snapshot can be lost
> (rebuildable from its commit's tree); history cannot be rebuilt
> from a snapshot alone, because the snapshot doesn't know its
> ancestors.

**Verdict:** Reframe. History is `f({commits}) → DAG`, sourced from
the *commit set*, not from a snapshot. This is **S1**.

---

### A11 — The "Object Store Native" definition is a marketing slogan (S5)

**WarpStream architect:**

> OSN1-OSN8 are properties, not an algebra. They are the *symptoms*
> of being object-store-native, not a *definition*. "Append-only
> writes" is a property of any content-addressed system — including
> local filesystems. "No rename" is a property of Git. "No directory
> assumptions" is a property of any flat key-value store. "Bounded
> RTT" is a property of any well-designed API.
>
> The actual definition of object-store-native is simpler:
>
> > **OSN.** The system is correct under the object-store consistency
> > model: eventual consistency on overwrite (which we avoid by
> > content-addressing), LIST-after-PUT eventual visibility, no
> > atomic rename, no atomic multi-key write, no CAS (except
> > create-only).
>
> Everything else in OSN1-OSN8 follows. "No local metadata
> dependence" (OSN7) is the only non-trivial one, and it is the one
> you currently fail (per §5.2). The other seven are consequences of
> content-addressing + flat namespace, which the kernel already has
> by axiom.

**Verdict:** Collapse OSN1-OSN8 to a single definition + a table of
derived properties. This is **S5**.

---

### A12 — The model has no Time primitive, but silently depends on one (S2)

**FoundationDB architect:**

> The commit structure (§3.1) has a `timestamp: wall-clock time`
> field and an `index: commit sequence number` field. These are
> used for: history ordering, timestamp-based merge, GC epoch
> bucketing, freshness checks ("O(1) freshness" in the Feature
> Store).
>
> But Time is not in your primitives. You have `Write`, `Read`,
> `Ref`. Where does `timestamp` come from? Whose clock? What
> happens when two writers have skewed clocks?
>
> In FDB, time is a *first-class substrate*: the sequencer hands
> out monotonically increasing timestamps. Without it, MVCC and
> snapshot isolation are impossible. Your model uses timestamps
> in three places (merge, GC, freshness) without formalizing
> where they come from.
>
> Add a Time substrate, or remove all uses of timestamps from the
> model. Pick one.

**Verdict:** Time is a hidden primitive. Either formalize it (a
monotonic logical clock substrate) or eliminate timestamps from the
model and re-derive merge/GC/freshness from causal order alone
(Lamport clocks, vector clocks). This is **S2**.

---

### A13 — The "Range Read" operation is missing (S2)

**Pebble architect:**

> Your kernel has `Read(hash)→bytes`. That is a *full-object read*.
> But your own pack files (§6.1 Physical Structure Taxonomy) require
> range reads — "Pack files support range reads" (OSN8). A range
> read is `Read(hash, offset, length)→bytes`. It is a *different
> operation*, with different cost, different failure modes, and
> different semantics on different backends.
>
> On S3, a range read is a `GET` with `Range: bytes=offset-length`
> header. Cost: ~1 RTT, billed as a single GET (no per-byte charge
> for the request itself, only egress). On local disk: `pread`.
> On a block device: same. On a tape archive: catastrophically
> expensive.
>
> Your RTT calculus (§4) silently treats `Read` as if it were
> free-of-bytes. It is not. A pack-file scan that requires reading
> 1000 different 4KB ranges from a 1GB pack is 1000 range reads,
> not 1 read. The bytes transferred matter (per A6), but the
> *operation* itself is also missing from the kernel API.
>
> Either Range Read is a kernel primitive (promote), or it is a
> Physical Structure optimization on top of `Read` (then formalize
> how a `Read(hash, offset, length)` is decomposed into a `Read(hash)`
> + in-memory slicing, and accept that this decomposition is
> backend-specific and out of model).

**Verdict:** Promote Range Read to a kernel primitive. The kernel
now has *four* operations: `Write`, `Read`, `ReadRange`, `Ref`.
This is **S2**.

---

## 3. Severity Tally

| Severity | Count | Attacks |
|---|---|---|
| S0 (cosmetic) | 0 | — |
| S1 (under-specification) | 4 | A6, A9, A10, A5-partial |
| S2 (hidden primitive) | 5 | A1, A2, A7, A12, A13 |
| S3 (circular definition) | 2 | A3, A7 |
| S4 (false law) | 2 | A4, A8 |
| S5 (collapse) | 2 | A5, A11 |

**Total: 13 attacks. 5 hidden primitives. 2 false laws. 2 collapses.**

The model survives — but barely. The three-primitive claim is
rhetorical, not mathematical. The honest count is **five
substrates** (bytes, names, time, coordination, range-read) and
**three operations** (Write, Read, Ref), with the understanding
that each substrate carries its own axioms.

---

## 4. Mandatory Model Changes

The following changes must be made to `POND_MATHEMATICAL_MODEL.md`
and `POND_FORMAL_ALGEBRAS.md`. They are not optional.

### M1 — Promote five substrates; demote three primitives (A1, A2, A12, A13)

The kernel is **not** three primitives. It is **five substrates**,
each with its own axioms:

| Substrate | Axioms | Operations |
|---|---|---|
| Bytes | A1 (Immutability), A2 (Content-addressing) | `Write(bytes)→hash`, `Read(hash)→bytes`, `ReadRange(hash, off, len)→bytes` |
| Names | A3 (Last-writer-wins), A4 (Referential integrity) | `Ref(name,hash)`, `get(name)`, `list(prefix)`, `delete(name)`, `CAS(name,exp,new)` |
| Time | A5 (Monotonic logical clock) | `now()→timestamp`, `compare(t1,t2)→order` |
| Coordination | A6 (Atomic commit blob), A7 (Coordinator optional for cross-Collection) | `commit_blob(writes)→hash` |
| Range-Read | A8 (Range reads are first-class; backend may decompose) | — (already in Bytes row) |

The "three operations" story is the *user-facing API*. The
"five substrates" story is the *model*. Both are true; only the
former was being told.

### M2 — Withdraw `L6` (composition by byte concat); replace with name-level composition (A4)

`L6' (Composition at the name level)`: Lenses compose by disjoint
name prefixes, not by byte concatenation. The model does not
formalize byte-level framing; that is a Lens-internal concern.

### M3 — Demote `L7` (context-based interpretation) to application convention (A3)

`L7' (Kernel never decodes)`: the kernel returns bytes;
interpretation is performed exclusively outside the kernel. The
context-based strategy is one convention; blob-header sniffing,
sidecar manifests, and magic bytes are equally valid.

### M4 — Collapse the Physical Structure algebra to definition + one theorem (A5)

A Physical Structure is anything that can be lost without data
loss, because it can be recomputed. All other properties
(determinism, independence, rebuildability) follow.

### M5 — Collapse OSN1-OSN8 to a single definition + derived properties table (A11)

**OSN.** The system is correct under the object-store consistency
model: eventual consistency on overwrite (avoided by
content-addressing), LIST-after-PUT eventual visibility, no atomic
rename, no atomic multi-key write, no CAS except create-only. All
other "object-store-native" properties derive from this.

### M6 — Add Physical Reachability as a separate GC algebra (A7)

GC operates over (Ref, manifest, pack) triples, not over (Ref, blob)
pairs. Define the equivalence condition under which physical and
logical reachability coincide: "every blob is in some pack, every
pack has a manifest, every manifest is reachable from some Ref."

### M7 — Restrict `W4` to within-Collection atomicity (A8)

Cross-Collection atomicity requires a coordinator substrate, which
is not part of the model. `W4` is restricted: a Workspace is
within-Collection. Cross-Collection coordination is the
application's responsibility.

### M8 — Demote `M4` from law to default policy (A9)

Merge commits may carry snapshots or deltas. The default is
snapshot for simplicity; delta merges are an optimization.

### M9 — Reframe History as `f(commit_set) → DAG` (A10)

History is sourced from the commit set, not from any single
snapshot. Snapshots are themselves a function of commits.

### M10 — Add a Dollar-cost theorem (A6)

**T5 (Dollar bound).** `cost(op) ≤ α·RTTs + β·bytes + γ·requests`.
The RTT budget is necessary but not sufficient.

---

## 5. What the Model Got Right

The red team is not required to be only negative. The following
claims survived attack and should be retained unchanged:

1. **A1, A2 (Immutability + Content-addressing).** Survived intact.
   These are the load-bearing axioms. They are why the rest works.

2. **A3 (Name mutability is the only mutation).** Survived. The
   panelists agreed this is the correct framing.

3. **L1, L2, L4, L5 (Lens round-trip, purity, determinism, kernel
   independence).** Survived. The Lens algebra is sound modulo
   `L6` and `L7`.

4. **Three-layer Merge (kernel topology → Lens semantics →
   application policy).** Survived. This is the right
   decomposition.

5. **GC as a maintenance operation, not a kernel concept.**
   Survived. The kernel never deletes; GC reclaims.

6. **Cache is not a Physical Structure.** Survived. The carve-out
   is correct, even though the rest of the Physical Structure
   algebra was collapsed.

7. **History is a Physical Structure.** Survived, with the source
   corrected from "snapshot" to "commit set" (M9).

8. **Tiered Commit Model is a Lens-level strategy, not a kernel
   concept.** Survived. The kernel doesn't know about tiers.

---

## 6. Net Effect on the Model

| Before | After |
|---|---|
| 3 primitives | 5 substrates, 3 operations |
| 8 formal algebras | 8 algebras (4 reformed, 2 collapsed, 1 added) |
| OSN1-OSN8 (8 properties) | 1 definition + 7 derived properties |
| 12 architecture laws | 12 laws (L6/L7 reformed, M4 demoted) |
| No Manifest algebra | Manifest algebra added (M6) |
| No Range-Read algebra | Range-Read folded into Bytes substrate (M1) |
| No Time substrate | Time substrate added (M1) |
| No Coordinator substrate | Coordinator optional, not in model (M7) |

The model **shrinks in surface area** (OSN collapses, Physical
Structure algebra collapses) and **grows in honesty** (5 substrates
instead of 3 primitives). The net effect is fewer concepts, more
axioms per concept — which is what the user asked for: "If an
algebra is not fundamental, eliminate it. If a missing algebra
exists, define it formally. The goal is to simplify the model, not
add features."

---

## 7. Next Steps (executed immediately after this review)

The following algebras will be formalized in the next sections of
`POND_FORMAL_ALGEBRAS.md`, in the order mandated by the red team:

1. **Manifest Algebra** (M6) — the bridge between logical
   reachability and physical reachability.
2. **Range Read Algebra** (A13, M1) — `ReadRange` as a first-class
   operation; its cost model; its decomposition rules.
3. **State vs Bytes** (A1, M1) — settle whether the primary
   substrate is "bytes" or "state." (Verdict: bytes; state is
   derived.)
4. **GC with Manifests** (M6) — the physical-reachability algebra.
5. **Physical Structure Dependency Graph** (A5, A10) — which
   structures depend on which sources.
6. **Concurrency / Consistency Algebra** (A2, A8, A12) — what the
   model guarantees and what it does not, given no coordinator
   substrate.

Each algebra will be presented as: definition, axioms, laws,
cost model, and the attacks it closes.
