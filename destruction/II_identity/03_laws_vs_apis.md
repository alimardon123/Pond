# Identity Destruction II — Experiment 3: Is the kernel an API or laws?

## The question

Up until now, the architecture has been specified as an API:

```
Write(bytes) -> hash
Read(hash | name) -> bytes
Reference(name, hash)
```

But APIs evolve. Linux's `open/read/write` changed over decades.
Git's plumbing changed. What endured was the *invariants* —
"everything is a file," "content-addressed immutable objects."

If Pond is to endure for decades, it should be specified as **laws**
(invariants), not APIs. The API is one possible realization of the laws.
Future versions might have different APIs that satisfy the same laws.

## Proposed laws (draft — to be attacked)

**Law 1: Objects are immutable.**
Once written, the bytes at a hash never change. The hash is a function
of the bytes (content-addressing), so changing the bytes would change
the hash — a different object.

**Law 2: Objects are addressable.**
Every object has a stable identifier (its hash). The identifier is
derivable from the object (anyone with the bytes can compute the hash).
The identifier is verifiable (anyone with the bytes can check the hash matches).

**Law 3: Names are mutable.**
Names (the namespace) are mutable mappings from human-readable strings
to object hashes. Names can be created, updated, and (possibly) deleted.
The namespace is the only mutable state in the system.

**Law 4: References never mutate objects.**
Updating a name to point to a different hash does not modify any object.
Objects are only created (via Write) and read (via Read), never modified.

**Law 5: Objects are backend-independent.**
The laws make no assumption about where objects are stored (filesystem,
S3, Redis, FDB). Any backend that can "store bytes by key" and "fetch
bytes by key" satisfies the laws.

## What operations satisfy these laws?

The current API (Write/Read/Reference) is one realization. But there
could be others:

**Alternative API 1: SetRoot + NamespaceView**
- Write(bytes) -> hash
- Read(hash) -> bytes
- SetRoot(hash) — single mutable pointer

This satisfies all 5 laws. The namespace (name -> hash) is a View
concern, stored as a blob at the root pointer. Different Views can
have different namespace models. (This is the IPFS/IPNS model.)

**Alternative API 2: Layered object store**
- Put(bytes) -> hash
- Get(hash) -> bytes
- (no namespace at all — pure object store)

This satisfies Laws 1, 2, 4, 5 but NOT Law 3 (no mutable names).
This is pure IPFS without IPNS. Not a database, but a valid
architecture for content-addressed storage.

**Alternative API 3: Transactional namespace**
- Write(bytes) -> hash
- Read(hash) -> bytes
- TxBegin() -> txn_id
- TxRef(txn_id, name, hash)
- TxCommit(txn_id)

This satisfies all 5 laws and adds transactional namespace updates.
Useful for multi-writer scenarios. The current API doesn't support this.

## The shift: from APIs to laws

If the architecture is specified as laws, then:

1. The current API (Write/Read/Reference) is one realization.
2. Future APIs (SetRoot, transactional, CRDT-based) are also valid
   realizations, as long as they satisfy the laws.
3. The laws are what endure; the API is what evolves.
4. Views target the laws, not the API. A View that only needs
   immutable objects + mutable names works on any API satisfying
   Laws 1-5.

## What this changes

**Documentation:** the primary specification is the laws, not the API.
The API is documented as "the current realization of the laws."

**Admission rule:** a feature enters the kernel if it's required to
satisfy a law, not if it's required by an API. (This is stricter.)

**Compatibility:** future API changes are allowed as long as they
satisfy the same laws. Views written against the laws continue to work.

**Experimentation:** alternative APIs can be tested without "changing
the architecture." The architecture IS the laws; the API is an
implementation detail.

## Open questions (to be attacked in future experiments)

- Is Law 3 (names are mutable) actually required? Could names be
  immutable too, with versioning handled another way?
- Is Law 1 (immutability) binary, or could there be tiered immutability
  (e.g., "mutable for 1 hour, then immutable")?
- Are there laws I'm missing? Laws that should be added?
- Are there laws I'm over-claiming? Laws that don't actually hold?

## Verdict

**The kernel should be specified as laws, not APIs.**

This is a real architectural shift. The current API (Write/Read/Reference)
is one realization of the 5 laws. Future realizations might be smaller
(SetRoot) or richer (transactional). Views target the laws, not the API.

This doesn't change the current implementation — but it changes how the
architecture is specified, documented, and evolved. The laws are the
enduring substrate; the API is a point-in-time realization.

**Status:** this is a hypothesis, not a final specification. The laws
need to be attacked (can each be falsified?) before they're frozen.
