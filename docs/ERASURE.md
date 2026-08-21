# Erasure

How this system deletes a person's data when nothing underneath it can delete
anything.

---

## The problem

Everything below the lens layer is immutable and content-addressed. That is
what buys convergence without a coordinator, structural sharing between
versions, and a cache that cannot go stale. It also means there is no
mechanism for "delete this subject's data":

- a blob's name **is** its content, so it is referenced by every tree that ever
  contained it — including branches and every historical root;
- spilled values deduplicate, so one blob may be referenced by rows belonging
  to different subjects, and deleting a row does not free it;
- a reader holding an old root still resolves every node beneath it.

Structural garbage collection reclaims blobs that nothing references any more.
That solves **cost**. It does not solve **law**, because it cannot reach a blob
that history still references — and only one of those two problems has a
statutory deadline attached.

Every system in this lineage discovers this late:

| System | What erasure looks like |
|---|---|
| Datomic | Excision is *"irrevocable"*, asynchronous, capped at a few thousand datoms on a live system, unsupported for full-text attributes, and was absent from Datomic Cloud entirely |
| lakeFS | Its own docs: *"it appears to be impossible to really delete data"* |
| Nessie | GC ships as a separate product with its own Postgres, and has shipped bugs where it silently failed to delete |
| Irmin | For years the answer was export, delete the store, re-import |
| Dolt | Automatic GC became default-on only in 2026 |

The retrofit is always expensive. That is the argument for designing it in.

---

## The mechanism: crypto-shredding

Encrypt each subject's data under a key belonging to that subject alone, and
keep the keys somewhere mutable. **Erasing a subject destroys one key.**

The ciphertext stays — it must, since it cannot be found and rewritten — but it
becomes noise, and it becomes noise *everywhere at once*: in every branch,
every historical root, and every replica that already copied it. No scan, no
rewrite, no coordination.

```
subject key destroyed
        │
        ├── ciphertext in the current tree      → noise
        ├── ciphertext in every branch          → noise
        ├── ciphertext under every old root     → noise
        └── ciphertext already replicated       → noise
```

---

## Why the encryption is deterministic

Encrypting normally would break the layer underneath. A random nonce means the
same plaintext produces different ciphertext each time, so two writers with
identical data would compute different hashes and stop converging, and
rewriting an unchanged value would produce a new blob rather than sharing the
old one — collapsing structural sharing so every version stores everything
again.

So the nonce is derived from the key, the context, and the plaintext — a
synthetic IV. Identical input under one subject's key always yields identical
bytes, so hashes stay stable and dedup keeps working. The nonce repeats only
when the message repeats, which is the one case where reuse is harmless,
because the ciphertext is the same message.

Deriving the nonce from the key as well as the message is what makes two
subjects storing identical bytes produce *different* ciphertext — so no blob is
ever shared across subjects, which is what makes one subject's erasure
complete.

---

## What it costs, stated plainly

- **No cross-subject dedup.** By design: a blob shared between subjects could
  not be destroyed by one subject's erasure.
- **Deterministic encryption confirms guesses.** Someone holding a subject's
  key who already suspects a value can confirm it exists. This is the existence
  oracle content addressing already has, narrowed to holders of the key.
- **Erasure is exactly as complete as the destruction of the last copy of the
  key.** A keystore that is backed up, snapshotted, or replicated somewhere
  outliving the delete has not erased anything. The keystore is small
  *precisely* so it can be held somewhere with real deletion.
- **The context binding is load-bearing.** A ciphertext is bound to where it
  lives, so it cannot be moved to another field and still open.

---

## What exists

`core/crypto`:

- `SubjectKey` — 32 bytes, opaque in `Debug` because a key in a log is a
  subject who cannot be erased.
- `seal` / `open` — deterministic ChaCha20-Poly1305 with a synthetic IV and
  authenticated context.
- `KeyStore` — keys as *named objects*, not blobs. A blob is addressed by its
  content, so a key stored as a blob would be named after itself, and anything
  that had seen the hash could ask for it again.
- `erase(subject)` — idempotent, because a deletion request that arrives twice
  must not fail the second time.
- `subjects()` — audit what is erasable.

Seventeen tests, including the end-to-end contract: data sealed under a key is
readable, the key is erased, and the same ciphertext no longer opens — not even
under a freshly created key for the same subject.

Subject ids are hex-encoded into their path, so `../escape` is a subject name
rather than a traversal.

---

## How to use it

```rust
// A collection whose rows belong to subjects, named by a column.
engine_path::create_for_subjects(&kernel, "people", "owner")?;

// Every other column is now sealed under that row's subject key.
engine_path::write_rows(&kernel, "people", &columns, writer_id)?;

// Erase. One key destroyed; their values are noise everywhere at once.
subject::erase_subject(&kernel, "alice")?;

// The collection still reads. Alice's sealed fields are absent; everyone
// else's are untouched.
let rows = engine_path::read_rows(&kernel, "people")?;
```

**Default-deny.** Naming a subject column seals *every* other column. The
alternative — listing which columns hold personal data — has a failure mode
this does not: a field added later, by a lens that never heard of the policy,
would silently be stored in the clear. For a protection mechanism the safe
default is to protect everything and make the exceptions explicit.

The subject column itself stays readable. It is an identifier rather than
personal detail, and sealing it would leave nothing able to say which rows
belong to whom — including the erasure.

**An erased subject reads as absent, not as an error.** A scan over a million
rows that failed because one of them was erased would hand a denial of service
to anyone exercising their right to deletion. Absent is also the honest answer:
the value no longer exists. What it cannot distinguish is "erased" from "never
set" — the keystore is the record of which is which, since a subject with no
key is an erased subject.

**Refusals, and why.** Turning sealing on for a collection that already holds
rows is refused: those rows were written in the clear and would stay that way,
so the collection would be partly protected while reporting that it is
protected. Changing the subject column afterwards is refused for the same
reason — existing rows would be sealed under subjects nothing could name, and
so could never be erased. A row with no usable subject value is refused rather
than stored unsealed, because guessing a subject means that row can never be
erased with the one it actually belongs to.

---

## What the tests establish

- A subject's values are readable, then erased, and what remains is unreadable
  — while another subject's rows in the same collection are untouched.
- A scan over 50 rows still returns all 50 after 10 subjects are erased.
- **The plaintext is not on disk.** Every byte the store holds is searched for
  it, with a control that performs the same search against an unsealed
  collection and *does* find it — so a broken search cannot pass as a clean
  result.
- Sealing cannot be switched on after rows exist, and a row without a subject
  is refused.
- Sealing a 200-row batch fetches the key twice, not 201 times. Rows in a
  batch overwhelmingly share a subject — that is what a batch *is* here — so a
  lookup per row would turn one round trip into hundreds and defeat the
  batching the whole design rests on. The key cache is scoped to one operation
  rather than held globally: a key kept across operations is a key that
  outlives somebody else's erasure.
- Garbage collection cannot destroy a subject key. Keys are named objects and
  GC only deletes blobs, which is a deliberate property of the design rather
  than an accident, so it is pinned by a test.

---

## Keeping the keys elsewhere

Erasure is exactly as complete as the destruction of the last copy of the key.
Keys held in the data store are copied by every backup, snapshot and replica of
the data — and restoring one undoes every erasure performed since it was taken.

```rust
let keystore: Arc<dyn ObjectStore> = Arc::new(LocalFSObjectStore::new("/secure/keys")?);
let kernel = PondKernel::new_local("/data")?.with_keystore(keystore);
assert!(kernel.keystore_is_separate());
```

The keystore is a few bytes per subject precisely so it can live somewhere with
a retention policy of its own. A deployment that must be able to *prove*
erasure should check `keystore_is_separate()` at startup and refuse to run if
it is false, rather than discover after a restore that the keys came back with
the data.

The default is the data store, which is right for a single-machine pond and
wrong for anything with a backup schedule.

---

## Proving it happened

Absence is not evidence: erased data looks the same as data that never
existed. So erasure writes an append-only record.

```rust
subject::erase_subject_for(&kernel, "alice@example.com", "ticket-42")?;
subject::was_erased(&kernel, "alice@example.com")?;   // true
subject::erasure_log(&kernel)?;                        // every entry, oldest first
```

**The log does not contain subject ids.** A log naming erased subjects is a
directory of erased people, retained after their data was destroyed — the
opposite of what the erasure was for, and for an id that is itself personal
data (an email, a customer number) it would retain exactly what was supposed to
go.

Entries record a **salted hash** of the id instead. That answers the question
actually asked — *"was this subject erased?"*, by someone who already holds the
id — and does not let anyone enumerate who was. The salt is per-store, because
an unsalted hash of a low-entropy id is reversed by guessing, and because a log
from one deployment must not identify subjects in another.

The entry is written **after** the key is destroyed. Reversed, a failed
destruction would leave the log claiming an erasure that did not happen — and a
log that overstates is worse than one that lags, because the second is
discoverable and the first is believed. A repeated request is recorded again
rather than overwriting the first: asking twice is itself a fact.

---

## What leaks anyway

Worth stating, because encryption invites the assumption that nothing does.

- **Column names.** A column called `hiv_status` discloses by existing. Sealing
  protects values, not the schema.
- **Value lengths.** Ciphertext is the length of its plaintext plus a fixed
  overhead. Ordinary for authenticated encryption, and only worth padding
  against when the length itself is the secret.
- **Row counts and which subjects exist.** The subject column is in the clear
  by design, so how many rows a subject has is visible to anyone who can read
  the collection.
- **Deterministic encryption confirms guesses**, for someone who holds the key
  and already suspects a value.

---

## From the command line

```bash
export POND_KEYSTORE=/secure/keys      # keys and audit log live here, not with the data

pond subjects                          # who is erasable
pond erase alice@example.com --confirm --requested-by ticket-42
pond erasure-log                       # salted digests, timestamps, who asked
```

`--confirm` is required. The operation destroys a key, and the data it
protected cannot be recovered in any branch, in history, or in any replica —
that should not be one keystroke away.

`POND_KEYSTORE` accepts a path or an `s3://` URL. Unset means the data store.

---

## What is still missing

1. **Key rotation.** A subject's key is created once and never changes.
   Rotating it means re-sealing that subject's rows, which is a rewrite rather
   than a pointer change — cheap per subject, but not free, and there is no
   mechanism yet.
2. **A startup policy.** `keystore_is_separate()` exists and the CLI honours
   `POND_KEYSTORE`, but nothing *refuses to run* when the keys sit with the
   data. A deployment that must prove erasure wants that as a hard failure, not
   a documented recommendation.
3. **Erasure through the language bindings.** Rust and the CLI only.
