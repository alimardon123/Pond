# Pond Third Red Team Review

> Phase K.4 — Operations Falsification.
> Not the model this time. The *operations*: replication, compression,
> encryption, schema evolution. These are the four open questions
> deferred from Part II §17 of `POND_FORMAL_ALGEBRAS.md`.
>
> **Question on trial:** Are these four operational concerns
> *out-of-model* (the model is silent, the application handles them),
> or are they *in-model* (the model must formalize them)?

---

## 0. Method

Six operations architects sit at the table. Each has shipped and
operated a real system at scale. Each is told to attack from the
perspective of their operations reality, not their design
preferences. They are *not* permitted to add features. They *are*
permitted to:

- declare that an operational concern is silently load-bearing and
  must be promoted to an algebra,
- declare that an operational concern is correctly out-of-model
  and should remain so,
- declare that an operational concern interacts with a kernel
  axiom in a way the model does not account for,
- declare that two of the four concerns are the same concern.

Severity grading is the same as K.2 (S0 cosmetic, S1
under-specification, S2 hidden primitive, S3 circular definition,
S4 false law, S5 collapse), with one new grade:

- **S6 — Operational hazard.** The model is silent on a real
  operational failure mode that affects correctness, not just
  performance. The model must either formalize the hazard or
  explicitly defer it with a documented boundary.

---

## 1. The Panelists

| # | Architect | Shipped | What they care about |
|---|---|---|---|
| 1 | S3 storage engineer | S3 lifecycle, multipart, conditional writes, list-after-put | Object-store operational reality, not the API spec |
| 2 | WarpStream operations engineer | Kafka-on-S3 at production scale | Streaming log semantics on object stores |
| 3 | Encryption-at-rest architect | Tink, age, per-key encryption in databases | Encryption boundary, KMS integration, range reads |
| 4 | Confluent Schema Registry maintainer | Avro/Protobuf/JSON schema evolution | Backward/forward compatibility, codec versioning |
| 5 | zstd / DuckDB compression engineer | zstd dictionaries, Parquet page compression | Compression layering, dictionary sharing, range reads |
| 6 | CockroachDB / Spanner multi-region architect | Geo-distributed SQL, replica placement | Multi-region consistency, replica convergence |

They have read the model (Parts I + II). They are hostile. Begin.

---

## 2. Attacks

### B1 — Replication is not "copy Refs and blobs" (S2 + S6)

**Multi-region architect:**

> Part II §17.7 says: "Replication is 'copy Refs and blobs to
> another backend.' But consistency across replicas is unspecified."
> That hand-wave is the whole problem. Replication is not a copy
> operation; it is a *convergence protocol*.
>
> Concretely: if Region A and Region B both accept writes (active-active),
> and A writes `Ref("orders", h_A)` while B writes `Ref("orders",
> h_B)`, what is the converged value? Three honest answers:
>
> 1. **Last-writer-wins by wall clock.** Requires synchronized
>    clocks. The model said (A5) that time is Lamport, not wall
>    clock. So LWW-by-wall-clock violates the model.
> 2. **Last-writer-wins by Lamport clock.** Lamport clocks are
>    only causally consistent, not totally ordered across
>    processes. Two unrelated writes get incomparable Lamport
>    timestamps. LWW is undefined.
> 3. **Application-resolved conflict.** The application sees both
>    `h_A` and `h_B` and decides. This is the Git model: a human
>    runs `git merge`. It works for code; it does not work for
>    "I want my bank balance to be correct."
>
> The model has no answer. "Copy Refs and blobs" describes the
> *mechanism*, not the *semantics*. Replication needs an algebra.

**S3 engineer (additional):**

> Even *single-region* replication has hazards the model doesn't
> mention. S3 multipart uploads are not atomic: an interrupted
> multipart upload leaves orphaned parts that count against your
> storage quota. S3 batch operations are eventually consistent
> across batches. S3 lifecycle policies can delete objects between
> replication and read. None of these are in the model.

**Verdict:** Add a Replication Algebra. Severity **S2 + S6**:
hidden primitive (convergence protocol) plus operational hazard.

---

### B2 — Compression breaks `ReadRange` unless compression is in the kernel (S4)

**Compression engineer:**

> Part II §11.3 RR2 says: "Range reads compose by concatenation."
> That is only true for *uncompressed* bytes. For compressed bytes
> (zstd, gzip, LZ4), byte offset `i` in the compressed stream
> corresponds to byte offset `j` in the logical stream only via the
> decompression state machine. You cannot slice a compressed blob
> at an arbitrary byte offset and decompress the slice.
>
> Three honest answers:
>
> 1. **Compression is a Lens-level concern.** The Lens compresses
>    before `Write`, decompresses after `Read`. `ReadRange` is
>    broken for compressed blobs: either the caller reads the whole
>    blob (defeating the point of compression for large blobs) or
>    the caller cannot range-read at all.
>
> 2. **Compression is a kernel concern.** The kernel knows about
>    compression, exposes `ReadRange` over the *logical* byte
>    stream, and internally translates to physical offsets. This
>    violates L5 (kernel never decodes) — but compression is not
>    decoding, it is transport encoding.
>
> 3. **Compression is a transport concern with a manifest.** Each
>    compressed blob has a sidecar "block index" listing (logical
>    offset → physical offset) for each compression block. The
>    kernel reads the block index, then range-reads the right
>    compression block. zstd frames already support this via the
>    `--no-content-size` flag and the skippable frames mechanism;
>    Parquet does it via page headers.
>
> Option 3 is the only one that preserves both `ReadRange` and L5.
> It requires the kernel to understand "block index" — which is
> *not* a codec, it is a transport-layer concern. The model must
> formalize this distinction.

**Verdict:** Add a Compression Algebra with a "transport encoding"
layer between the kernel and the Lens. Severity **S4**: RR2 is
false as stated.

---

### B3 — Encryption has the same range-read problem as compression, plus a key-management problem (S2 + S6)

**Encryption architect:**

> Same range issue as B2: you cannot slice an encrypted blob at
> an arbitrary byte offset and decrypt the slice. AES-GCM uses
> nonces and authentication tags per block; AEAD ciphertext is not
> slicable. So `ReadRange` over an encrypted blob requires the
> same "block index" mechanism as compression.
>
> But encryption adds a second problem the model is silent on:
> **key management**. Which key encrypts which blob?
>
> - **One key for all blobs.** Simple. One compromise = total
>   breach. No per-object access control.
> - **One key per blob.** Requires a key registry. The registry
>   is itself a substrate (the model has no key substrate).
> - **One key per Collection.** Compromise is bounded to one
>   Collection. The key is referenced by the Collection's metadata.
> - **Envelope encryption (AWS KMS style).** A master key encrypts
>   data-encryption keys; DEKs encrypt blobs. The master key lives
>   in a KMS. The DEKs are stored alongside the blobs.
>
> The model has no key substrate. Without one, encryption is
> magic. With one, the kernel gains a sixth substrate (Key) and
> the model is honest.
>
> Worse: encryption interacts with content-addressing (A2). Two
> identical plaintexts, encrypted with different nonces, produce
> different ciphertexts, different hashes, no dedup. Either:
> - dedup is broken for encrypted blobs (acceptable but must be
>   stated), or
> - converge encryption (deterministic encryption, e.g., SIV mode)
>   which weakens security.
>
> The model must pick.

**Verdict:** Add an Encryption Algebra with a Key substrate
promotion + an honest statement about dedup under encryption.
Severity **S2 + S6**.

---

### B4 — Schema evolution cannot be "new codec = new name prefix" (S4)

**Schema Registry maintainer:**

> Part II §17.10 says: "How does a Lens change its codec without
> breaking old blobs? Current answer: it can't; new codec = new
> Lens = new name prefix. This is restrictive."
>
> Restrictive is an understatement. It is *wrong* for any real
> system. Consider a Feature Store with 5 years of historical
> features encoded as Arrow IPC v1. Arrow IPC v2 ships (better
> delta encoding). The Feature Store wants to:
>
> - Read all 5 years of history (mix of v1 and v2 blobs).
> - Write new features in v2.
> - Eventually migrate old blobs to v2 (compaction).
>
> Under "new codec = new prefix," the Feature Store must:
> - Maintain two Lenses (`feature_v1`, `feature_v2`).
> - Maintain a query layer that reads from both.
> - Migrate by rewriting every blob (5 years of data!) under the
>   new prefix.
>
> That is not viable. Real schema evolution (Avro, Protobuf,
> Iceberg) works by *embedding the schema version in the blob* and
> having the codec resolve versions at decode time. The Lens knows
> how to read v1, v2, v3, ... and produces a unified in-memory
> representation.
>
> The model has two ways to admit this:
>
> 1. **The Lens carries a version-resolver.** `D(key, bytes) =
>    Resolver.decode(get_codec_version(key), bytes)`. The codec
>    version is encoded in the key (`feature/v1/...`, `feature/v2/...`)
>    or in a blob header. The Lens's `D` knows all versions it
>    supports. **This is consistent with L7' (kernel never
>    decodes)** — the kernel still doesn't decode; the Lens resolves.
>
> 2. **Embed schema in the blob.** A blob starts with a 4-byte
>    schema-id, the Lens reads the schema-id, looks up the schema
>    in a sidecar schema registry, and decodes. **This violates
>    L7'** because the kernel must read the schema-id (4 bytes)
>    before the Lens can decode. But this read can be a Range Read,
>    not a decode — so it is consistent *if* the kernel only
>    inspects bytes via `ReadRange`, never via `D`.
>
> Option 1 (version-in-key) is the cleanest. Option 2 (schema-in-blob)
> requires the kernel to do an opaque Range Read on the schema-id
> prefix, then hand the (schema-id, rest of bytes) to the Lens.
> Both work; both must be formalized.

**Verdict:** Add a Schema Evolution Algebra. Severity **S4**: the
"new codec = new prefix" answer is provably unworkable for any
real dataset.

---

### B5 — Replication and Compaction interact destructively (S3 + S6)

**S3 engineer:**

> Compaction (Part II §13.4) rewrites packs. Replication (B1) copies
> Refs and blobs. The interaction: a replica might observe `Ref(P, h_1)`
> (old pack) after the primary has already moved to `Ref(P, h_2)`
> (new pack) and started deleting `h_1`'s blobs. The replica reads
> the manifest, starts downloading blobs, finds half of them deleted.
>
> The model has no "tombstone barrier" — a guarantee that a blob
> referenced by a Ref is not deleted before all replicas have
> observed the Ref update. Without this, replication under
> compaction is unsafe.
>
> In S3 cross-region replication, this is solved by versioning +
> lifecycle policies that delay deletion by N days. The model needs
> the equivalent: a "deletion grace period" formalized as a kernel
> concept (or explicitly deferred to the application).

**WarpStream engineer:**

> Same hazard in streaming: a consumer reading from offset N might
> be mid-read when the producer truncates the log to reclaim space.
> The consumer's read fails. The model needs either:
> - A truncation barrier (consumers must commit a read-position
>   before truncation can pass it), or
> - An explicit acknowledgment that reads can fail mid-stream and
>   the consumer must retry from the last committed position.

**Verdict:** The Replication Algebra must include a "tombstone
barrier" or explicitly defer it. Severity **S3 + S6**.

---

### B6 — Compression dictionaries are external state that violates A2 (S2)

**Compression engineer:**

> zstd dictionaries give 5-10× better compression on small records
> (Feature Store features, log lines, JSON events). A dictionary
> is *trained* on a sample of the data, then *referenced* by every
> compressed blob: `compress(bytes, dict) → compressed_bytes`.
>
> The dictionary is not in the blob. If `f(snapshot) = compress(snapshot,
> dict(snapshot))`, the dictionary is a function of the snapshot —
> but if the dictionary is *trained once* and reused across many
> snapshots, it is external state.
>
> Two models:
> - **Per-snapshot dictionary.** Pure. But you retrain on every
>   snapshot (expensive) and lose cross-snapshot compression
>   (dictionaries are most effective when shared).
> - **Shared dictionary.** Impure. The compressed bytes depend on
>   `(bytes, dict)`, not on `bytes` alone. Two kernels with
>   different dictionaries produce different hashes for the same
>   logical state. Dedup across dictionaries is broken.
>
> The model has no concept of "shared dictionary." It is an
> external substrate, like Time. Promote or restrict.

**Verdict:** Either restrict dictionaries to per-snapshot (pure
but inefficient) or formalize a Dictionary substrate (impure but
practical). Severity **S2**.

---

### B7 — Multi-region writes require a "primary region" concept the model doesn't have (S2)

**Multi-region architect:**

> Active-active multi-region (B1) is hard. The pragmatic answer
> most systems use is **active-passive with a primary region**:
> all writes go to the primary; the primary replicates to secondaries;
> secondaries serve reads with eventual consistency.
>
> This requires a "primary region" concept. The model has no such
> concept. The model has Refs and blobs; "which region is primary"
> is operational state that lives outside the kernel.
>
> Two honest options:
>
> 1. **The model is single-region.** Multi-region is an
>    application-layer concern (the application runs a coordinator
>    per Part II A7). Document this explicitly.
>
> 2. **The model has a Region substrate.** The kernel knows which
>    region it is in, which region is primary, and routes writes
>    accordingly. This is a sixth substrate (after Bytes, Names,
>    Time, Coordination, Range-Read).
>
> Option 1 is consistent with the existing model (A7 already says
> the coordinator is out-of-model). Option 2 makes multi-region
> first-class. Pick.

**Verdict:** Confirm Option 1. Multi-region is out-of-model; the
Replication Algebra describes the *convergence contract* between
replicas, not the *region topology*. Severity **S2** (clarification
of an existing hidden assumption).

---

### B8 — Encryption-at-rest and Cross-Lens reads conflict (S3)

**Encryption architect:**

> Suppose Collection A is encrypted with Key K_A and Collection B
> is encrypted with Key K_B. A cross-Lens query (e.g., a JOIN via
> ViewQuery from Phase F) reads from both Collections.
>
> Who holds the keys? Three options:
> 1. The kernel holds all keys. The kernel becomes a key store.
>    Violates L5 (kernel never decodes) — actually it doesn't,
>    because decryption is not decoding. But it makes the kernel
>    security-critical.
> 2. Each Lens holds its own key. Cross-Lens queries must
>    re-encrypt or pass plaintext through the query engine.
>    Performance and security implications.
> 3. The application holds all keys. The kernel returns encrypted
>    bytes; the application decrypts before passing to the Lens.
>    This pushes the key substrate entirely out of the kernel.
>
> Option 3 is the cleanest. The kernel never sees plaintext. But
> it means Cross-Lens queries happen *above* the decryption layer,
> not inside it. The model must state this explicitly.

**Verdict:** Encryption is above the kernel, below the Lens. The
model has three layers (Kernel, Lens, Application); encryption is
a fourth layer between Kernel and Lens. Severity **S3** (clarifies
a circular dependency: Cross-Lens queries assumed to operate on
plaintext, but encryption is below Lens).

---

### B9 — Schema evolution across Lens versions requires a registry (S2)

**Schema Registry maintainer:**

> B4 said the Lens carries a version-resolver. But where does the
> resolver itself live? If it is in code (linked into the
> application), then schema evolution requires redeploying the
> application. That works for some systems (DuckDB extensions,
> Postgres extensions) but not for "read 5-year-old data with
> today's code."
>
> Real schema registries (Confluent, Iceberg, Protobuf) store
> schemas as data: the schema is itself a blob, versioned,
> addressable. The Lens's `D(key, bytes)` reads the schema-id
> from the blob (or key), fetches the schema blob, and decodes.
>
> This makes the Schema Registry a substrate. It is similar to the
> Manifest substrate (§10): a sidecar that the kernel reads
> opaquely (as bytes) and the Lens interprets.
>
> The model needs either:
> - A Schema Registry substrate (sixth substrate), or
> - An explicit statement that schemas live in code, not data, and
>   schema evolution requires code deployment.

**Verdict:** Promote the Schema Registry to a substrate or
restrict schemas to code. Severity **S2**.

---

### B10 — Compression + Encryption ordering is not commutative (S3)

**Compression engineer + Encryption architect (joint attack):**

> If you compress then encrypt: `enc(compress(b))`. Compressed
> bytes are high-entropy; encryption preserves entropy; the result
> is small and secure. Standard practice.
>
> If you encrypt then compress: `compress(enc(b))`. Encrypted
> bytes are high-entropy; compression cannot reduce them; the
> result is larger than the input. Anti-pattern.
>
> The model needs to specify the order. The order is a Lens-level
> decision, but the *constraint* (compress-before-encrypt) is a
> law of the model. Without it, a naive Lens could encrypt first.
>
> Worse: compression + encryption + content-addressing has a
> subtle hazard. Two identical plaintexts compressed with the same
> dictionary produce the same compressed bytes (deterministic),
> then encrypted with different nonces produce different ciphertexts
> (non-deterministic), so different hashes, no dedup. Three layers
> each with their own determinism rule.

**Verdict:** Add a Layering Law: compress before encrypt; both
are above the kernel, below the Lens. Severity **S3** (circular
dependency between compression and encryption layers).

---

### B11 — Replication convergence and Lamport clocks don't compose (S4)

**Multi-region architect:**

> Part II A5 says time is Lamport. Part II CC4 says wall-clock
> comparisons across processes are not supported. But replication
> convergence *requires* a total order. Two regions write
> `Ref("orders", h_A)` and `Ref("orders", h_B)`. To converge,
> the system must pick one. Lamport clocks don't give a total
> order across unrelated writers.
>
> The honest answers:
>
> 1. **Single-writer per Ref.** Only one region is primary for
>    each Ref. Other regions forward writes. Convergence is by
>    primary's order. (This is the CockroachDB / Spanner model:
>    per-key range leaders.)
>
> 2. **Vector clocks + application resolver.** Each Ref carries
>    a vector clock. Conflicts are detected; the application
>    resolves. (This is the Dynamo / Riak model.)
>
> 3. **LWW by wall clock + clock skew tolerance.** Accept that
>    clocks skew; bound the skew (NTP, TrueTime); accept last
>    writer wins within the skew window. (This is the Cassandra
>    model.)
>
> The model currently supports none of these. Pick one or
> explicitly defer.

**Verdict:** The Replication Algebra must specify a convergence
contract. The model picks: **single-writer per Ref** (option 1),
consistent with A7 (coordinator out-of-model). Cross-Ref
multi-writer requires application-level coordination. Severity
**S4** (the model's silence is false-by-omission).

---

### B12 — Schema evolution and Physical Structure dependency conflict (S3)

**Schema Registry maintainer:**

> Part II §14 says a Physical Structure is `f(source) → artifact`,
> pure function. But what if the function depends on the schema?
> A secondary index over `feature/v1/` is different from a
> secondary index over `feature/v2/` — the schema determines the
> fields, which determine the index keys.
>
> So `f(snapshot, schema_v) → index`. If the schema changes, the
> index must be rebuilt. The schema is *part of the source*.
>
> But the schema is not part of the snapshot — the snapshot is
> just bytes. The schema is a sidecar (per B9). So the source of
> a Physical Structure is `(snapshot, schema)`, not just `snapshot`.
>
> This means Part II §14 D1 ("sources are immutable") is
> incomplete: schemas are also sources, and schemas evolve. The
> dependency graph must include schemas as a source type.

**Verdict:** Add `S_schema` as a fourth source type in the
Physical Structure dependency graph. Severity **S3** (the
dependency graph was claimed complete; it wasn't).

---

### B13 — "Compression is a Lens concern" is the wrong default (S5)

**Compression engineer:**

> The model's instinct (Part II §17.8) is "compression is a
> Lens-level concern." That instinct is wrong for the same reason
> "encryption is a Lens concern" is wrong: every Lens needs
> compression, every Lens needs encryption, and re-implementing
> them per Lens is wasteful and inconsistent.
>
> Compression and encryption are *transport layers* between the
> kernel (bytes) and the Lens (interpreted state). They are not
> Lens-specific. The Lens sees plaintext, uncompressed bytes; the
> kernel stores encrypted, compressed bytes; the transport layer
> translates.
>
> This is not a new idea — TLS is a transport layer between TCP
> and HTTP. zstd frames, Parquet pages, and AES-GCM blocks are
> all transport-layer artifacts. The model should formalize a
> Transport Layer between Kernel and Lens, and fold compression
> + encryption + (possibly) checksumming into it.
>
> This collapses B2, B3, B6, B8, B10 into one algebra: the
> Transport Algebra.

**Verdict:** Collapse Compression, Encryption, and their
interactions into a single Transport Algebra. Severity **S5**
(collapse).

---

## 3. Severity Tally

| Severity | Count | Attacks |
|---|---|---|
| S0 (cosmetic) | 0 | — |
| S1 (under-specification) | 0 | — |
| S2 (hidden primitive) | 5 | B1, B3, B6, B7, B9 |
| S3 (circular definition) | 4 | B5, B8, B10, B12 |
| S4 (false law) | 3 | B2, B4, B11 |
| S5 (collapse) | 1 | B13 |
| S6 (operational hazard) | 4 | B1, B3, B5, B11 |

**Total: 13 attacks. 5 hidden primitives. 3 false laws. 1 collapse. 4 operational hazards.**

The model survives — but the four "operational" concerns cannot
remain out-of-model. Three of them (compression, encryption,
schema) collapse into two new algebras (Transport, Schema
Evolution). Replication stands alone.

---

## 4. Mandatory Model Changes

### N1 — Add a Replication Algebra (B1, B5, B7, B11)

Replication is a convergence contract, not a copy operation. The
model picks **single-writer per Ref** as the convergence contract,
consistent with A7. Cross-Ref multi-writer requires application
coordination.

The algebra must include:
- Replica topology (primary + secondaries).
- Convergence contract (primary's order wins).
- Tombstone barrier (deletions delayed until secondaries ack).
- Failover contract (what happens when primary fails).

### N2 — Add a Transport Algebra; collapse Compression + Encryption (B2, B3, B6, B8, B10, B13)

A Transport Layer sits between the Kernel (bytes) and the Lens
(interpreted state). The Transport Layer handles compression,
encryption, and checksumming. The Lens sees plaintext,
uncompressed bytes; the Kernel stores encrypted, compressed bytes.

The algebra must include:
- Layer order: compress → encrypt → checksum (or the inverse for read).
- Block index for range reads (transport-layer manifest).
- Key management (envelope encryption; master key in KMS, DEK
  inline).
- Shared dictionary support (per-snapshot dictionary = pure;
  shared dictionary = external state, must be referenced).
- Dedup under encryption (broken; explicitly accepted).

### N3 — Add a Schema Evolution Algebra (B4, B9, B12)

Schemas evolve. The Lens carries a version-resolver. Schema
versions are encoded in the key prefix (e.g., `feature/v1/`) or
in a 4-byte blob header. The Lens's `D` knows all versions it
supports.

The algebra must include:
- Schema versioning (key prefix or blob header).
- Schema registry as a substrate (or schemas in code — pick).
- Backward/forward compatibility contract (Lens must read all
  prior versions; may write only the latest).
- Schema as a fourth source type in the Physical Structure
  dependency graph (B12).

### N4 — Confirm multi-region is out-of-model (B7, B11)

Multi-region is application-level coordination (per A7). The
model is single-region. The Replication Algebra describes the
convergence contract between replicas, not the region topology.

### N5 — Add a Tombstone Barrier to the GC Algebra (B5)

GC must not delete a blob until all replicas have observed the
Ref update that orphaned it. The barrier is a "deletion grace
period" — a kernel-configurable delay between orphan-marking
and deletion. This is a new law (G6) on the existing GC algebra.

### N6 — Add `S_schema` as a fourth source type (B12)

The Physical Structure dependency graph (Part II §14) gains a
fourth source type: `S_schema`. Structures that depend on schema
(e.g., secondary indexes over a typed column) source from
`(S_snapshot, S_schema)`, not just `S_snapshot`.

### N7 — Withdraw RR2's "concatenation" claim; replace with transport-aware composition (B2)

RR2 (Part II §11.3) said range reads compose by byte
concatenation. This is only true for uncompressed, unencrypted
bytes. The corrected law:

> **RR2' (Composition).** `ReadRange(h, off, len) =
> ReadRange(h, off, k) || ReadRange(h, off+k, len-k)` *for
> uncompressed, unencrypted bytes*. For transport-encoded bytes,
> composition is via the transport block index (see Transport
> Algebra §X).

---

## 5. What the Model Got Right (operations edition)

1. **A7 (Coordinator out-of-model).** Survived. The Replication
   Algebra is consistent with A7: replication convergence is
   per-Ref single-writer, not distributed consensus.

2. **A6 (Atomic commit blob).** Survived. The commit blob is the
   unit of replication; replicas converge on commit-blob hashes.

3. **L5 / L7' (Kernel never decodes).** Survived. The Transport
   Layer is below the Lens; the kernel still doesn't decode. The
   kernel does range-reads on transport block indexes, but reading
   bytes is not decoding.

4. **A1, A2 (Immutability, Content-addressing).** Survived, with
   one caveat: dedup is broken under encryption (N2). This is
   documented, not silently violated.

5. **Manifest Algebra (§10).** Survived. The transport block
   index is a kind of manifest — same algebra, different scale
   (block-level vs pack-level).

6. **Physical Structure dependency graph (§14).** Survived, with
   one extension: `S_schema` added as a source type (N6).

7. **OSN (Object Store Native).** Survived. Replication hazards
   (B1, B5) are operational, not model violations; the OSN
   definition (§11 of Part II formal algebras) is unchanged.

---

## 6. Net Effect on the Model

| Before (after Phase K.3) | After (after Phase K.4) |
|---|---|
| 5 substrates (Bytes, Names, Time, Coordination, Range-Read) | 6 substrates (added Key, optionally Schema Registry) |
| 4 operations (Write, Read, ReadRange, Ref) | 4 operations (unchanged) |
| 14 formal algebras | 17 algebras (added Replication, Transport, Schema Evolution) |
| 8 axioms (A1-A8) | 10 axioms (added A9: single-writer per Ref; A10: compress-before-encrypt) |
| Open questions: 4 | Open questions: 0 (all four operational questions closed) |

**The model is now operationally complete.** All four deferred
questions from Phase K.3 are answered. The remaining unknowns are
*engineering* questions (which compression codec? which KMS?
which schema registry?) not *model* questions.

---

## 7. Next Steps (executed immediately after this review)

Three new algebras will be formalized in Part III of
`POND_FORMAL_ALGEBRAS.md`:

1. **Replication Algebra** (§16 in Part III) — single-writer per
   Ref; tombstone barrier; failover contract.
2. **Transport Algebra** (§17 in Part III) — compression +
   encryption + checksumming as one transport layer; block index;
   key management; dedup caveat.
3. **Schema Evolution Algebra** (§18 in Part III) — versioned
   codecs; schema registry substrate; backward/forward
   compatibility; `S_schema` source type.

Part III also amends two existing algebras:
- §11 Range Read: RR2 → RR2' (transport-aware composition).
- §14 Physical Structure Dependency Graph: add `S_schema`.
- §3 GC: add G6 (tombstone barrier).

After Part III, the model has 0 open questions. Phase K is
complete. The next phase (Phase L, not yet defined) shifts from
*model falsification* to *model verification*: prove the laws
hold under the operations hazards the red team identified.
