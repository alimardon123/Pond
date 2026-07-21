# LogLens — External Implementation Challenge Report

**Task ID:** 27
**Implementer:** general-purpose agent (no prior Pond context)
**Date:** 2026-07-21
**Artifacts:**
- Implementation: `validation/log_lens_external.py` (~336 non-blank LOC, including tests)
- Test output: all 7 requirements pass; 14 blobs, 2 names, 1892 data bytes, zero metadata

---

## 1. Was the Lens contract sufficient?

**Partially.** I could implement LogLens from the contract alone *in spirit* —
the philosophy, constraints, and intended behaviour were all clear enough
that I never had to read another Lens implementation. But I did have to
read the `View` base class source in `view_sdk.py` (which the task
explicitly permitted, but only to find the import path) to discover
**five concrete API details** the contract does not specify:

1. The constructor signature `Lens(kernel, name)` and the fact that
   `self.base = ProllyViewBase(kernel, name)` is set up for me.
2. The `put(key, data)` / `get(key)` / `get_raw(key)` / `commit(message)`
   / `branch(name)` / `checkout(name)` / `list_branches()` / `undo(steps)`
   method set.
3. The fact that `put` **stages** in memory and `commit` is what actually
   flushes to the kernel — I hit a `None` return from `get_raw` on my
   first run and had to diagnose the stage/commit split.
4. The fact that `encode(data)` / `decode(raw)` on the base class do NOT
   receive the key, so a Resolver-driven Lens must override `put`/`get`
   directly rather than overriding `encode`/`decode`.
5. That branching is implemented via kernel name refs
   (`<name>__branch__<branch>`), and that the "default branch" is just
   the `<name>` ref — there is no implicit `checkout("main")`.

What was clear from the contract alone:
- The 3 kernel primitives (write/read/reference) — crystal clear.
- The Resolver interface (`register`, `encode_for_key`, `decode_for_key`) —
  given verbatim in §8.
- The key-prefix convention (`log/`) — clear from §3.1.
- The "pure bytes, no envelope" rule — emphatic and unambiguous (§2, §4.1, §9).
- Cross-Lens reading — the §6 example is essentially a worked solution.
- Transform-later / `get_raw` — clear from §3.3 and §7.
- The shared commit DAG — clear from §3.4.
- The compliance checklist (§11) gave me a concrete verification path.

The contract is excellent at stating **constraints and philosophy**; it is
weak at stating **construction details**. An implementer who has never
seen the SDK will get the philosophy right but will have to discover the
API by reading source or by trial-and-error.

---

## 2. How long did it take?

| Phase | Estimate |
|---|---|
| Read the 4 documents (contract, architecture, kernel, design goals) + worklog | ~15 min |
| Locate the `Lens` base class in `view_sdk.py` (and incidentally discover `put`/`get`/`commit`/`branch`) | ~5 min |
| Implement `ContextResolver` + `ContextLens` + `LogLens` + `SqlLens` | ~20 min |
| Write the 7-requirement test | ~15 min |
| Debug the stage/commit split (first run returned `None` from `get_raw`) | ~10 min |
| Debug the branching isolation test (`checkout("main")` doesn't exist; switched to `undo`) | ~10 min |
| Write this report | ~10 min |
| **Total** | **~85 min** |

The contract's "~55 LOC" estimate for the Resolver + Lens override (§8) was
accurate: my `ContextResolver` is 37 LOC and `ContextLens` is 30 LOC
(67 LOC total, of which ~15 are docstrings/comments — so ~52 LOC of code,
matching the estimate).

---

## 3. What was unclear or missing from the contract?

Each gap is cited against the RFC section I tried to follow.

1. **The `ContextLens` class is never defined.** §3.2 and §6 show the
   *usage* (`ContextLens(kernel, "workspace", resolver, "sql/")`) but
   the contract never defines the class — its constructor signature,
   its inheritance, or how it delegates `encode`/`decode` to the
   resolver. I had to infer the whole class. *(§3.2, §6, §8)*

2. **Who supplies the key prefix — the Lens or the caller?** §3.1 says
   "keys MAY carry a prefix" and §6 shows the caller passing the full
   key (`sql_lens.put("sql/user:1", ...)`), but the contract never
   states explicitly that the Lens should NOT auto-prepend its prefix.
   I inferred "caller supplies the full key" from the examples, but a
   one-line statement would remove the ambiguity. *(§3.1, §6)*

3. **The stage/commit model is invisible.** The contract talks about
   `put` and `get` as if they are immediate, but the SDK's `put` only
   stages in memory; `commit` flushes to the kernel. An implementer
   following the contract alone will write `put` then `get` and get
   `None`. The contract should either (a) state that `put` stages and
   `commit` flushes, or (b) point to RFC-0007 for the lifecycle. *(§3.2,
   §3.3 — neither mentions commit)*

4. **The branching API is unspecified.** §3.4 says "branching via one
   Lens is visible to all Lenses" but never names the methods
   (`branch`, `checkout`, `list_branches`, `undo`, `merge`). I found
   them by reading the `View` base class. The contract should either
   name them or cite RFC-0007. *(§3.4)*

5. **The Resolver's prefix-match policy is unspecified.** §8 gives the
   interface but doesn't say what happens when two registered prefixes
   could match the same key (e.g., `log/` and `log/error/`). I chose
   longest-prefix-wins, which is the conventional choice, but it's an
   inference. *(§8)*

6. **Fallback ENCODING is unspecified.** §5 specifies fallback DECODING
   (return raw bytes), but says nothing about what `encode_for_key`
   should do when no codec matches the key prefix. I default to
   "pass-through if bytes, else JSON" — but the contract is silent.
   *(§5, §8)*

7. **There is no implicit "main" branch.** §3.4 says branching is
   shared, but doesn't explain that the default branch is just the
   `<name>` kernel ref, and that there is no `checkout("main")` to
   return to baseline. I had to use `undo(1)` to test isolation. This
   is a minor SDK ergonomic gap, not a contract gap per se, but it
   cost me ~10 minutes. *(§3.4)*

8. **The relationship between `ContextLens` and domain Lenses (like
   `LogLens`) is unspecified.** The contract shows `ContextLens` in
   examples but the task asks for a `LogLens`. Should `LogLens`
   *be* a `ContextLens` (subclass), or *use* one (composition)? I
   chose subclassing, which felt natural, but the contract doesn't
   guide this. *(§3.2, §11)*

---

## 4. Rate the contract clarity (1-10)

**Score: 7 / 10.**

**What's good (the 7):**
- The philosophy is communicated beautifully. The Linux analogy
  (§3.1, RFC-0012 §3) makes the "bytes are bytes, interpretation is in
  code" idea click in one read.
- The "what NOT to assume" section (§4) is excellent — it pre-empts the
  most likely mistakes (envelopes, manifests, global registries).
- The compliance checklist (§11) gives a concrete verification path. I
  used it as my test plan.
- The verification table (§10) sets concrete, falsifiable expectations
  (1.0x cross-lens overhead, ~55 LOC). My implementation matched both.
- The §6 worked example is essentially a spec — I copied its shape
  directly into my cross-Lens test.
- The contract is short (~250 lines) and never waffles. That alone is
  worth 2 points.

**What would need to change (the missing 3):**
- **Define `ContextLens`.** Even a 10-line skeleton would save every
  external implementer 20 minutes and remove the biggest inference.
- **State the stage/commit lifecycle.** One sentence in §3.2 ("`put`
  stages; `commit` flushes to the kernel") would prevent the `None`
  surprise.
- **Name the branching API** (or cite RFC-0007). §3.4 promises
  branching but doesn't tell you how to invoke it.
- **Specify the Resolver's prefix-match policy** (longest-wins,
  first-registered, or last-registered).
- **Specify fallback encoding**, not just fallback decoding.

With those five additions, this would be a 9. A 10 would additionally
ship a companion "Lens Author's Guide" with a complete worked example
(`LogLens` or similar) that an implementer can read alongside the
contract.

---

## 5. Did the architecture feel elegant?

**Yes — genuinely, not just diplomatically.** Three moments stood out:

1. **The codec-is-in-the-key insight.** When I realised that
   `sql_lens.get("log/...")` works *because the resolver dispatches by
   the key's prefix, not the Lens's prefix*, the whole architecture
   clicked. The Lens doesn't "own" its keys; it just has a preferred
   prefix. Any Lens can read any key. That felt like the architecture
   was doing the work, not me.

2. **Cross-Lens reading "just worked."** Once both Lenses shared the
   same Resolver and the same View name, `SqlLens` reading a log entry
   written by `LogLens` was a single line with no plumbing. The
   emergent-overlap thesis (RFC-0012 §3) is real: I didn't design for
   cross-Lens reading, I just didn't prevent it.

3. **`get_raw` as the honest escape hatch.** The transform-later
   requirement (§7) could have been an awkward special case. Instead,
   `get_raw(key)` simply bypasses the resolver and hands you bytes.
   That's the Unix `cat` model: the filesystem doesn't know JPEG, but
   it hands you the bytes and you interpret them. It kept the
   abstraction honest.

The one place it felt like "just another API to learn" was the
**stage-then-commit model**. The contract's `put`/`get` examples read
as if `put` is immediate, but the SDK requires an explicit `commit`
before `get` sees the data. That's a familiar pattern (Git's index),
but it's a SDK detail that breaks the otherwise-clean mental model
and isn't surfaced in the contract.

The kernel's 3-primitive minimalism is the real elegance. Walking
`pond_minimal.py` and seeing that the *entire* storage substrate is
`write(bytes)`, `read(hash_or_name)`, `reference(name, hash)` — and
that everything else (Trees, Commits, Branches, Lenses, cross-Lens
reading) is composed above that — is the kind of architecture that
makes you want to go re-read the Unix paper. The Lens contract is the
right *boundary*; it just needs a slightly thicker *skin* to let an
external implementer build a Lens without reading SDK source.

---

## Summary

The Lens Interpretation Contract is sufficient to convey the
**philosophy and constraints** of building a Lens, but under-specifies
the **construction details** (the `ContextLens` class, the stage/commit
lifecycle, the branching API, the resolver's match policy). An
implementer following only the contract will build a correct Lens, but
will spend ~30 minutes discovering SDK details by trial-and-error or by
reading `view_sdk.py`. The architecture itself is genuinely elegant —
the codec-in-the-key insight and the shared commit DAG make cross-Lens
reading feel emergent rather than engineered. With five small additions
(define `ContextLens`, state the lifecycle, name the branching API,
specify match policy, specify fallback encoding), the contract would
move from 7/10 to 9/10.
