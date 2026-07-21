# Resolver Comparison: Three Approaches to Universal Lens Readability

> **Status:** Research. NOT a decision. Three competing prototypes
> evaluated against six criteria. No winner is merged into Pond's
> core architecture. This document exists to inform the decision.

---

## The Question

> What is the smallest possible interpretation layer that allows
> every Lens to read every object while keeping the kernel completely
> format-agnostic?

The kernel owns: **Bytes, History, Names.** Nothing else.

The question is: how does a Lens read a blob written by a different
Lens, when the blob's encoding doesn't match?

---

## The Three Prototypes

### Prototype 1: Context-based Interpretation

**No metadata in blobs.** The interpretation comes from the KEY
PREFIX (e.g., `sql/user:1`, `git/tree:main`). The resolver looks at
the prefix to determine which codec to use.

Like Git: Git knows whether it's requesting a blob, tree, commit, or
tag from the context (which reference it resolved). The object itself
doesn't carry its type.

**Files:** `prototype1_context.py`

### Prototype 2: Minimal Envelope (TypedBlob)

**5-byte envelope per blob:** `[codec_id][payload_len][payload]`.
The codec_id tells a global CodecRegistry which decoder to use.

**Files:** `prototype2_envelope.py`, `pond-sdk/typed_blob.py`

### Prototype 3: Self-describing Payloads

**No envelope, no key context.** The payload format itself carries
enough information to be identified. The resolver SNIFFS the first
few bytes (like Unix `file(1)`):
- Starts with `{` or `[` → JSON
- Starts with `100644 blob` → Git tree
- Starts with `ARROW1` magic → Arrow IPC

**Files:** `prototype3_self_describing.py`

---

## Scorecard

| Criterion | Context | Envelope | Self-describing |
|---|---|---|---|
| Kernel simplicity (Bytes/History/Names only) | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐⭐ |
| Universal readability (any lens, any blob) | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ |
| Zero metadata overhead (no extra bytes per blob) | ⭐⭐⭐⭐⭐ | ⭐⭐ | ⭐⭐⭐⭐⭐ |
| Independent implementations (no shared registry) | ⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐ |
| Long-term extensibility (add new formats) | ⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐⭐ |
| Alignment with "bytes are just bytes" | ⭐⭐⭐⭐⭐ | ⭐⭐ | ⭐⭐⭐⭐⭐ |
| **Total** | **28/30** | **21/30** | **27/30** |

---

## Detailed Analysis

### Kernel Simplicity

- **Context (⭐⭐⭐⭐⭐):** The kernel stores pure bytes. The type
  information is in the key prefix, which is part of the kernel's
  existing "Names" concept. No new concept is added.
- **Envelope (⭐⭐⭐):** The kernel stores bytes, but they're
  "typed bytes" (envelope + payload). The kernel doesn't interpret
  the envelope, but the structure exists. This is a step toward a
  "Pond Binary Format."
- **Self-describing (⭐⭐⭐⭐⭐):** The kernel stores pure bytes.
  The format is inherent in the payload. No new concept is added.

### Universal Readability

- **Context (⭐⭐⭐⭐):** Any lens can read any blob — IF the key has
  a known prefix. Keys without prefixes can't be decoded. The lens
  needs to know the prefix convention.
- **Envelope (⭐⭐⭐⭐⭐):** Any lens can read any blob — always. The
  codec_id is in the blob. This is the strongest universal
  readability.
- **Self-describing (⭐⭐⭐⭐):** Any lens can read any blob — IF the
  format is self-describing. Raw bytes or custom formats without
  magic bytes can't be sniffed. But JSON, Git, Arrow, Parquet, CBOR
  are all self-describing.

### Zero Metadata Overhead

- **Context (⭐⭐⭐⭐⭐):** Zero overhead in the blob. The key is
  longer (has a prefix), but keys are part of the kernel's Names.
- **Envelope (⭐⭐):** 5 bytes per blob. For 1M blobs, that's 5MB
  of overhead. Not huge, but not zero.
- **Self-describing (⭐⭐⭐⭐⭐):** Zero overhead for self-describing
  formats. Formats that aren't self-describing need padding or
  can't be used.

### Independent Implementations

- **Context (⭐⭐⭐⭐):** Different implementations must agree on key
  prefix conventions ("sql/" means JSON, etc.). This is simple but
  requires coordination.
- **Envelope (⭐⭐⭐):** Different implementations must agree on
  codec_id assignments (codec_id 1 = JSON, etc.). This creates a
  formal registry — a "Pond Binary Format" spec. More coupling.
- **Self-describing (⭐⭐⭐⭐):** Sniffers are based on format
  standards (JSON spec, Git spec), not Pond-specific conventions.
  Any implementation that knows the format standard can read the
  data. Less Pond-specific coupling.

### Long-term Extensibility

- **Context (⭐⭐⭐⭐):** Add a new prefix, register a new codec. Old
  data unaffected. Old implementations can't read new prefixes
  (return raw bytes).
- **Envelope (⭐⭐⭐⭐):** Add a new codec_id. Old data unaffected.
  Old implementations don't know the new codec_id (return raw bytes).
- **Self-describing (⭐⭐⭐⭐):** Add a new sniffer. Old data
  unaffected. Old implementations can't sniff the new format
  (return raw bytes).

All three handle extensibility equally well.

### Alignment with "Bytes are Just Bytes"

- **Context (⭐⭐⭐⭐⭐):** Yes. Bytes are bytes. The key (Name)
  carries the type, but the kernel already owns Names. This is
  philosophically pure.
- **Envelope (⭐⭐):** No. Bytes are "typed bytes." The envelope
  creates a Pond-specific binary format. This drifts from the
  original philosophy.
- **Self-describing (⭐⭐⭐⭐⭐):** Yes. Bytes are bytes. The format
  is inherent in the payload (like a JPEG file — the file IS the
  format). This is philosophically pure.

---

## The Key Insight

**Both Context-based and Self-describing preserve the kernel's
purity.** The Envelope does not.

The Envelope's advantage (perfect universal readability) is offset
by its philosophical cost (typed bytes, Pond Binary Format, hidden
coupling via CodecRegistry).

The Self-describing approach is almost as good as the Envelope for
universal readability (all common formats are self-describing),
with zero philosophical cost.

The Context-based approach is also pure, but moves type information
into keys (which some might consider "metadata in Names").

---

## Recommendation (NOT a decision — a hypothesis)

**Hypothesis: Self-describing payloads + Context fallback is the
right architecture.**

```
Bytes (pure, no envelope)
   ↓
Resolver:
   1. Try to sniff the format (self-describing)
   2. If sniff fails, use key-prefix context
   3. If both fail, return raw bytes
   ↓
Decoded Object
```

This gives:
- Zero blob overhead (no envelope)
- Universal readability for common formats (JSON, Git, Arrow, Parquet)
- Fallback for non-self-describing formats (via key prefix)
- Kernel stays pure (Bytes, History, Names)
- No Pond Binary Format
- No CodecRegistry coupling

**But this is a hypothesis, not a decision.** The prototypes need
to be tested against real workloads (Arrow data, Git repos, feature
vectors) to verify that sniffing is reliable and the context fallback
covers the edge cases.

---

## What This Means for TypedBlob

`pond-sdk/typed_blob.py` is marked as **EXPERIMENTAL**. It is NOT
part of Pond's core architecture. It is Prototype 2, kept for
comparison. If the hypothesis above is confirmed, TypedBlob should
be removed in favor of the Self-describing + Context approach.

---

## Next Steps

1. **Test the Self-describing prototype against real formats:**
   Arrow IPC, Parquet, Git objects, CBOR. Verify sniffing is
   reliable.

2. **Test the Context fallback:** what happens when a format can't
   be sniffed? Is the key-prefix convention sufficient?

3. **Get external validation:** have a fresh agent try to build a
   Lens using each approach. Which feels most natural?

4. **Only after these experiments:** make a decision and document
   it in an RFC. Until then, all three prototypes are research
   artifacts.
