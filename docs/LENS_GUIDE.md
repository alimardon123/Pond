# Lens Guide — Author's Contract + Interpretation + Interop

> This document merges three formerly-separate docs:
> - Lens Author's Guide (kernel guarantees, conventions, unspecified)
> - Lens Interpretation Contract (what a Lens can/cannot assume)
> - Lens Interop Spec (ambiguities the laws do not cover)
>
> If you are writing a Lens, read this before writing any code.

---

## Part 1: What the Kernel GUARANTEES

These are the invariants the kernel upholds. Lenses can rely on these
unconditionally.

### G1: Content-addressed immutability
Once `Write(data)` returns hash `h`, the bytes at `h` are permanently
stored and never change. `Read(h)` returns exactly `data` for the
lifetime of the kernel. No operation can modify, overwrite, or delete
the bytes at `h`.

### G2: Deduplication
`Write(data)` called twice with the same `data` returns the same `h`.
The bytes are stored once. (Side effect of content-addressing.)

### G3: Name resolution
`Read(name)` resolves the name to its current hash via the root
namespace, then returns the bytes at that hash. Resolution is atomic
with respect to the read.

### G4: Last-writer-wins on Reference
`Reference(name, h)` atomically updates `name` to point to `h`. If
another writer called `Reference(name, h2)` concurrently, one wins
and the other's update is lost. The kernel does NOT detect or report
lost updates.

### G5: Hash existence check
`Reference(name, h)` fails if `h` was never returned by `Write`. A
name cannot point to a nonexistent hash.

### G6: Backend independence
The same sequence of `Write` and `Reference` operations produces the
same hashes and the same name→hash mappings on any backend. The
kernel does not require rename, append, seek, directories, or
filesystem semantics.

---

## Part 2: What is CONVENTION (not guaranteed, but recommended)

### C1: JSON serialization for metadata
Trees and commits are typically serialized as JSON. Lenses MAY use
protobuf, MessagePack, or custom binary formats — but they won't be
readable by other Lenses.

### C2: Self-typing envelopes
Objects that are NOT raw data blobs (trees, commits) typically include
a `"type"` field in their JSON. This lets a Lens distinguish object
kinds when reading by hash.

### C3: Flat tree (path → blob hash)
Trees typically map file paths directly to blob hashes. Nested trees
are possible but not required.

---

## Part 3: What is INTENTIONALLY UNSPECIFIED

### U1: HEAD / "current branch" tracking
The kernel does not track a "current branch." HEAD is a convention;
Lenses manage their own HEAD ref (e.g., `{name}/HEAD`).

### U2: Branch listing
The kernel provides `list_names()` but does not distinguish branches
from other refs. A Lens filters by naming convention (e.g.,
`refs/heads/*`).

### U3: Merge semantics
The kernel records merge topology (parent + second_parent). The
*semantics* of merge (union, 3-way, CRDT) are Lens-defined.

### U4: Conflict detection
The kernel does not detect conflicts. A Lens that needs conflict
detection implements it (e.g., 3-way merge with common ancestor).

### U5: Time travel API
The kernel stores history (commits with parent pointers). The *API*
for time travel (`AS OF commit_hash`) is Lens-level.

### U6: Index management
The kernel does not know about indexes. Indexes are Physical
Structures (Prolly trees of key→hash) managed by the Lens/SDK.

### U7: Schema management
The kernel does not know about schemas. Schemas live in the Lens
code (or in a Schema Registry on the Names substrate, per §18).

### U8: Codec registry
The kernel does not have a codec registry. The resolver (which maps
key prefixes to codecs) is application-level code.

---

## Part 4: The Lens Interpretation Contract

### What the Kernel Stores

```
Bytes      (immutable, content-addressed blobs — pure payload, no envelope)
History    (commit DAG, parent pointers)
Names      (mutable name → hash references, including key prefixes)
```

**Nothing else.** No codec_ids. No envelopes. No manifests. No type tags.
The kernel is format-agnostic.

### What a Lens Can Assume

1. **The key prefix provides context.** Keys like `sql/user:1`,
   `git/tree:main`, `arrow/orders_table` carry a prefix that tells
   the resolver which codec to use.

2. **Any blob can be read by any Lens.** The resolver (code, not data)
   knows all registered codecs. Any Lens reading any key gets the
   decoded value — regardless of which Lens wrote it.

3. **Raw bytes are always available.** `get_raw(key)` returns pure
   payload bytes, bypassing the resolver. Fallback if decode fails.

4. **The commit DAG is shared.** All Lenses with the same name share
   the same history, branches, and snapshots.

### What a Lens Must NOT Assume

1. **Must NOT assume the blob carries type metadata.** The blob is
   pure payload. Type info is in the KEY, not the BYTES.

2. **Must NOT assume a global codec registry exists in the kernel.**
   The resolver is application-level. Different deployments can have
   different resolvers.

3. **Must NOT write metadata blobs.** No manifest, no enable_view,
   no sidecar files. Enablement is in code, not data.

4. **Must NOT assume its encoding is the only encoding.** Other
   Lenses may write blobs with different encodings.

### How Fallback Decoding Works

```
1. Lens calls get(key)
2. Resolver checks key prefix → finds matching codec
3. If codec found: decode payload → return decoded value
4. If codec not found: return raw payload bytes
5. If decode fails: return raw payload bytes
6. Caller can transform raw bytes into whatever format it needs
```

The fallback is always "raw bytes." The caller never gets nothing.

### The Resolver

The resolver is a **code-level** construct (not data-level):

```python
class ContextResolver:
    def register(self, prefix, encode, decode): ...
    def decode_for_key(self, key, raw): ...
```

- Lives in the application, not in the kernel.
- Each deployment registers its own codecs.
- ~30 LOC for the resolver; ~25 LOC for the Lens override.
- No global registry. No Pond Binary Format. No hidden coupling.

---

## Part 5: Cross-Lens Interop

### What works

- **Shared byte graph:** Multiple Lenses with the same name share
  the same Prolly tree and commit DAG. One write → all Lenses see it.
- **Branching is universal:** A branch created by one Lens is visible
  to all Lenses (same ref namespace).
- **Time travel is universal:** Any commit in the chain can be read
  by any Lens.
- **Schema evolution propagates:** Parquet-native (missing columns → NULL).

### What does NOT work (and why)

- **Cross-Lens decode:** A SQL Lens cannot decode a Git tree blob
  (different encoding). It gets raw bytes via `get_raw()`.
- **Cross-Lens merge semantics:** If two Lenses have different merge
  policies, merging through one Lens may confuse the other. The
  application must coordinate.

### Verification

This contract is verified by:
- `bindings/python/sdk/test_shared_lenses.py` — multiple Lenses sharing same byte graph
- `bindings/python/sdk/test_lens_architecture.py` — multi-Lens architecture proof
- `pond-labs/interop_demo.py` — bidirectional Feature Store ↔ Lakehouse interop (12/12 pass)

---

## Part 6: Summary for Lens Authors

| Question | Answer |
|---|---|
| Can I assume the blob has a type tag? | **No.** Type is in the key prefix. |
| Can I assume a global codec registry? | **No.** The resolver is application-level. |
| Can I write metadata blobs? | **No.** Enablement is in code, not data. |
| Can I assume my encoding is the only one? | **No.** Other Lenses may use different encodings. |
| Can I assume HEAD tracking? | **No.** HEAD is a convention; manage your own. |
| Can I assume the kernel detects conflicts? | **No.** Conflict detection is Lens-level. |
| Can I read another Lens's blobs? | **Yes**, via `get_raw()` (raw bytes). Decode may fail. |
| Can I share branches with another Lens? | **Yes.** Same ref namespace. |
| Can I share time travel with another Lens? | **Yes.** Same commit DAG. |

If you find yourself assuming something not listed here as a guarantee,
your Lens is depending on behavior the kernel does not promise.
