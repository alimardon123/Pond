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

## What does not exist yet

This is the mechanism, not the integration. Still to do, in order:

1. **A policy for which fields are subject data**, and which subject a row
   belongs to. This is a deployment question, not a storage one, and it decides
   everything else.
2. **Sealing on the write path** — `engine_path` would seal field values under
   the row's subject key before they reach a record.
3. **Opening on the read path**, with a defined behaviour for a value whose key
   is gone. Returning "erased" is right; returning an error that fails the
   whole scan is not, since one erased subject must not make a collection
   unreadable.
4. **Keystore durability separated from data durability.** Same store today,
   which is the wrong place for the reason in the costs above.
5. **An erasure audit log** — what was erased, when, on whose request — which
   is usually required alongside the erasure itself.

Until those exist, this is a correct primitive with no callers, and the honest
description of the feature is "designed and implemented at the crypto layer,
not yet wired to the data path".
