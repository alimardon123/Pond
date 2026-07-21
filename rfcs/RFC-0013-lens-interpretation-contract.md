# RFC-0013: The Lens Interpretation Contract

## Status

**Accepted** — the formal specification of how Lenses interpret bytes.
Every Lens implementation must satisfy this contract.

---

## 1. Purpose

This RFC defines the contract between the Pond kernel and Lens
implementations. It specifies:

- What a Lens can assume about the bytes it reads
- What a Lens must NOT assume
- How fallback decoding works
- How cross-Lens reading works
- How cross-Lens transforms work
- What is explicitly NOT stored in the kernel

This contract is the boundary between the kernel (Bytes, History,
Names) and the interpretation layer (Lenses + Resolver).

---

## 2. What the Kernel Stores

```
Bytes      — immutable, content-addressed blobs. Pure payload.
              No envelope. No codec_id. No header. No type tag.
              The kernel does not know what format the bytes are in.

History    — commit DAG with parent pointers. Shared by all Lenses
              with the same name. Branching, merging, and time-travel
              operate on this DAG.

Names      — mutable name → hash references. Keys are strings that
              MAY carry a prefix (e.g., "sql/user:1", "git/tree:main").
              The prefix is part of the key, which the kernel already
              owns. The kernel does not interpret prefixes.
```

**Nothing else.** No codec registry. No manifest. No enable_view
metadata. No sidecar files. No schema blobs. No type tags.

---

## 3. What a Lens Can Assume

### 3.1. The key provides context

Keys MAY carry a prefix (e.g., `sql/`, `git/`, `arrow/`, `nb/`).
The prefix is a convention chosen by the Lens author. The Resolver
uses the prefix to determine which codec to use.

This is like Git: Git knows whether it's requesting a blob, tree,
commit, or tag from the context (which command asked, which reference
it resolved). The object itself doesn't carry its type.

### 3.2. Any blob can be read by any Lens

The Resolver (a CODE-level construct, not DATA) knows all registered
codecs. Any Lens reading any key gets the decoded value — regardless
of which Lens wrote it.

```python
sql_lens = ContextLens(kernel, "workspace", resolver, "sql/")
git_lens = ContextLens(kernel, "workspace", resolver, "git/")

sql_lens.put("sql/user:1", {"name": "Alice"})  # writes JSON bytes
git_lens.get("sql/user:1")  # reads JSON bytes → decoded dict (via resolver)
```

### 3.3. Raw bytes are always available

`get_raw(key)` returns the pure payload bytes, bypassing the Resolver.
This is the universal fallback: if the Resolver doesn't know the codec,
or decode fails, the caller gets raw bytes and can transform them later.

### 3.4. The commit DAG is shared

All Lenses with the same name share the same history, branches, and
snapshots. Branching via one Lens is visible to all Lenses. A commit
by one Lens appears in the history of all Lenses.

---

## 4. What a Lens Must NOT Assume

### 4.1. Must NOT assume the blob carries type metadata

The blob is pure payload. There is no envelope, no codec_id, no header.
The type information is in the KEY (which the kernel owns as Names),
not in the BYTES.

### 4.2. Must NOT assume a global codec registry exists in the kernel

The Resolver is application-level code. Different deployments can have
different Resolvers with different codecs. The kernel does not know
about codecs.

### 4.3. Must NOT write metadata blobs

No manifest. No enable_view. No sidecar files. No schema blobs.
The "enablement" is in the code (having a Lens instance with the
right Resolver), not in the data.

### 4.4. Must NOT assume its encoding is the only encoding

Other Lenses may write blobs with different encodings. A Lens reads
what it can (via the Resolver) and gets raw bytes for the rest.

---

## 5. How Fallback Decoding Works

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

## 6. How Cross-Lens Reading Works

```python
# SQL Lens writes a row (JSON bytes, key prefix "sql/")
sql_lens.put("sql/user:1", {"name": "Alice", "age": 30})

# Git Lens reads the SAME blob — decoded via the resolver
# (the resolver knows "sql/" prefix → JSON codec)
row = git_lens.get("sql/user:1")
assert row == {"name": "Alice", "age": 30}

# Arrow Lens reads the SAME blob — also decoded via the resolver
row = arrow_lens.get("sql/user:1")
assert row == {"name": "Alice", "age": 30}
```

The blob is pure JSON bytes. The Resolver decodes it for any Lens
that asks. No copying. No translation. No duplication.

---

## 7. How Cross-Lens Transforms Work

```python
# SQL Lens reads an Arrow Table (decoded via resolver)
arrow_table = sql_lens.get("arrow/orders_table")

# SQL Lens transforms it into SQL rows
for row in arrow_table.to_pylist():
    sql_lens.put(f"sql/order:{row['order_id']}", row)

# Both the Arrow Table and the SQL rows are in the same byte graph
# No copying, no duplication — just different interpretations
```

Transform-later is always available: read as raw bytes, parse
externally, write back in a different format.

---

## 8. The Resolver

The Resolver is a **code-level** construct (not data-level).

```python
class ContextResolver:
    def register(self, prefix: str, encode: Callable, decode: Callable): ...
    def encode_for_key(self, key: str, data: Any) -> bytes: ...
    def decode_for_key(self, key: str, raw: bytes) -> Any: ...
```

Properties:
- Lives in the application, not in the kernel.
- Each deployment registers its own codecs.
- Different implementations can have different Resolvers.
- ~30 LOC for the Resolver. ~25 LOC for the Lens override. Total: ~55 LOC.
- No global registry in the kernel. No Pond Binary Format. No hidden coupling.

---

## 9. What is Explicitly NOT Stored in the Kernel

| Thing | Why not |
|---|---|
| Codec IDs | The codec is determined by key prefix (context), not by the blob. |
| Envelopes / headers | The blob is pure payload. No wrapper. |
| Manifests | No enable_view metadata. The Lens IS the enablement. |
| Sidecar files | No per-format metadata files. |
| Type tags | The type is in the key prefix, not in the bytes. |
| Schema blobs | Schemas live in the Resolver/Lens code, not in the data. |

---

## 10. Verification

This contract is verified by
`experiments/resolver_comparison/falsification_context.py`:

| Criterion | Result |
|---|---|
| Universal readability | 25/25 reads (5 lenses × 5 blobs) |
| Bidirectional write/read | PASS (SQL→Arrow, Arrow→SQL, Git→FeatureStore) |
| Branch/merge/history | PASS (shared DAG, cross-lens branches) |
| Derived structures | PASS (cross-lens index) |
| Zero metadata overhead | PASS (no manifest, no envelope, no sidecar) |
| Pure bytes | PASS (JSON=`{`, Arrow=`0xFFFFFFFF`, Git=`100644`) |
| Transform-later | PASS (SQL↔Arrow transform) |
| Kernel purity | PASS (Bytes, History, Names only) |
| Cross-lens read overhead | 1.0x (zero penalty vs same-lens) |
| Implementation size | ~55 LOC |

---

## 11. Compliance Checklist for New Lenses

Before claiming a Lens is contract-compliant, verify:

- [ ] The Lens uses a key prefix convention (e.g., `mylens/`).
- [ ] The Lens registers its codec with the Resolver.
- [ ] The Lens does NOT write envelope/header bytes into blobs.
- [ ] The Lens does NOT write manifest or enable_view metadata.
- [ ] The Lens can read blobs written by other Lenses (via the Resolver).
- [ ] The Lens provides `get_raw(key)` for raw-byte fallback.
- [ ] The Lens supports branching, checkout, and history (inherited
      from the shared commit DAG).
- [ ] The Lens passes the falsification test scenario.

---

## 12. Relationship to Other RFCs

- **Depends on:** RFC-0003 (Kernel — Bytes, History, Names), RFC-0007
  (View Algebra — the laws Lenses must satisfy), RFC-0012 (Lens
  Architecture — context-based interpretation is the chosen approach).
- **Formalizes:** `docs/LENS_INTERPRETATION_CONTRACT.md` (the one-page
  version) into a formal RFC.
- **Supersedes:** the TypedBlob envelope approach (deprecated).
- **Does not modify:** any kernel code, any existing Lens code.
