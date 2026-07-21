# The Lens Interpretation Contract

> One page. Defines what a Lens can assume, what it must not assume,
> and how interpretation works. This keeps the system from drifting.

---

## What the Kernel Stores

```
Bytes      (immutable, content-addressed blobs — pure payload, no envelope)
History    (commit DAG, parent pointers)
Names      (mutable name → hash references, including key prefixes)
```

**Nothing else.** No codec_ids. No envelopes. No manifests. No type tags.
The kernel is format-agnostic. It does not know what format the bytes are in.

---

## What a Lens Can Assume

1. **The key prefix provides context.** Keys like `sql/user:1`,
   `git/tree:main`, `arrow/orders_table` carry a prefix that tells
   the resolver which codec to use. This is like Git: Git knows it's
   asking for a commit/tree/blob/tag from context, not from the object.

2. **Any blob can be read by any lens.** The resolver (a CODE-level
   construct, not DATA) knows all registered codecs. Any lens reading
   any key gets the decoded value — regardless of which lens wrote it.

3. **Raw bytes are always available.** `get_raw(key)` returns the pure
   payload bytes, bypassing the resolver. This is the fallback: if the
   resolver doesn't know the codec, or decode fails, the caller gets
   raw bytes and can transform them later.

4. **The commit DAG is shared.** All lenses with the same name share
   the same history, branches, and snapshots. Branching via one lens
   is visible to all lenses.

---

## What a Lens Must NOT Assume

1. **Must NOT assume the blob carries type metadata.** The blob is
   pure payload. There is no envelope, no codec_id, no header. The
   type information is in the KEY (which the kernel owns as Names),
   not in the BYTES.

2. **Must NOT assume a global codec registry exists in the kernel.**
   The resolver is application-level code. Different deployments can
   have different resolvers with different codecs. The kernel does
   not know about codecs.

3. **Must NOT write metadata blobs.** No manifest, no enable_view,
   no sidecar files. The "enablement" is in the code (having a Lens
   instance with the right resolver), not in the data.

4. **Must NOT assume its encoding is the only encoding.** Other lenses
   may write blobs with different encodings. A lens reads what it can
   (via the resolver) and gets raw bytes for the rest.

---

## How Fallback Decoding Works

```
1. Lens calls get(key)
2. Resolver checks key prefix → finds matching codec
3. If codec found: decode payload → return decoded value
4. If codec not found: return raw payload bytes
5. If decode fails: return raw payload bytes
6. Caller can transform raw bytes into whatever format it needs
```

The fallback is always "raw bytes." The caller never gets nothing —
it always gets something it can work with, even if that's just the
raw payload for later transformation.

---

## How Cross-Lens Transforms Work

```python
# SQL lens reads an Arrow Table (decoded via resolver)
arrow_table = sql_lens.get("arrow/orders_table")

# SQL lens transforms it into SQL rows
for row in arrow_table.to_pylist():
    sql_lens.put(f"sql/order:{row['order_id']}", row)

# Both the Arrow Table and the SQL rows are in the same byte graph
# No copying, no duplication — just different interpretations
```

---

## What is Explicitly NOT Stored in the Kernel

- Codec IDs
- Envelopes / headers
- Manifests
- Enable-view metadata
- Sidecar files
- Type tags
- Schema blobs (schemas live in the resolver/lens code, not in the data)

---

## The Resolver

The resolver is a **code-level** construct (not data-level). It is:

```python
class ContextResolver:
    def register(self, prefix, encode, decode): ...
    def decode_for_key(self, key, raw): ...
```

- Lives in the application, not in the kernel.
- Each deployment registers its own codecs.
- Different implementations can have different resolvers.
- The resolver is ~30 LOC. The Lens override is ~25 LOC. Total: ~55 LOC.
- No global registry. No Pond Binary Format. No hidden coupling.

---

## Verification

This contract is verified by `experiments/resolver_comparison/falsification_context.py`:

- 25/25 universal reads succeeded (SQL, Arrow, Git, Notebook, FeatureStore)
- Bidirectional write/read: PASS
- Branch/merge/history: PASS
- Cross-lens index: PASS
- Zero metadata overhead: PASS (no manifest, no envelope, no sidecar)
- Pure bytes: PASS (JSON starts with `{`, Arrow starts with `0xFFFFFFFF`, Git starts with `100644`)
- Transform-later: PASS (SQL reads Arrow Table → transforms to rows; Arrow reads rows → transforms to Table)
- Kernel purity: PASS (Bytes, History, Names only)
- Cross-lens read overhead: 1.0x (no performance penalty vs same-lens read)
- Implementation size: ~55 LOC (vs ~200 LOC for the envelope approach)

**The kernel does NOT need an envelope. The interpretation layer lives
in CODE (the resolver), not in DATA (the blob).**
