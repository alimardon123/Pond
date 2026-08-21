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
- Garbage collection cannot destroy a subject key. Keys are named objects and
  GC only deletes blobs, which is a deliberate property of the design rather
  than an accident, so it is pinned by a test.

---

## What is still missing

1. **Keystore durability separated from data durability.** Keys live in the
   same object store as the data today. That is the wrong place for the reason
   stated above: a backup of the keystore that outlives an erasure undoes it.
   The keystore is small precisely so it can be held somewhere with real
   deletion, and it should be.
2. **An erasure audit log** — what was erased, when, on whose request. Usually
   required alongside the erasure itself.
3. **Key rotation.** A subject's key is created once and never changes. Rotating
   it would require re-sealing their rows, which is a rewrite rather than a
   pointer change.
4. **Erasure through the CLI and the bindings.** Reachable from the Rust API
   only.
