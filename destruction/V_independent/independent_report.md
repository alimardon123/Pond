# Independent Git View — Honest Implementation Report

This report documents the experience of implementing a Git-like version control
View on top of the Pond Kernel, using **only** the provided Formal
Specification. No existing Pond code was read or consulted. The implementation
lives in `independent_git_view.py` and was verified by running the required
scenario end-to-end (output: `ALL CHECKS PASSED`).

---

## TL;DR

The spec was *sufficient to build a working, correct Git View*, but only
because I am already fluent in Git's object model. The spec defines the
**kernel** (immutable content-addressed objects + a mutable name→hash
namespace) precisely and elegantly. It defines **nothing** about how a View
should structure its objects, name its refs, track HEAD, serialize trees, or
stage changes. Every one of those was an invention. None of it was impossible,
but a large fraction was guessing guided by prior knowledge of Git — not by the
spec.

---

## 1. Was the spec sufficient? Could you implement without asking questions?

**Yes, it was sufficient to produce a working implementation — but only
conditionally.**

The spec gave me exactly what it promised: the three kernel operations
(`Write`, `Read`, `Reference`), their contracts, the five laws, and the
composition laws. With those, I could build blobs, trees, commits, branches,
and history walking. The kernel side was unambiguous and a pleasure to use:
content-addressing gave me dedup for free, immutable objects gave me safe
snapshotting, and `Reference` gave me O(1) branching exactly as the spec
advertised ("a branch is a name pointing to a commit hash").

I did **not** need to ask any clarifying question to get a correct, runnable
result. The reason, however, is important and honest: **I knew what Git is.**
The spec explicitly says "Deeper indirection is a View concern" and "Views
define reachability and implement GC." In other words, the spec deliberately
stops at the kernel and hands the rest to the View author. So "sufficient" is
true in the sense that the kernel contract is complete and stable; it is
**misleading** in the sense that a reader who did *not* already know Git would
have had to invent the entire object model from scratch with no guidance.

Concretely, the spec lets you build *the substrate*; it does not help you build
*the version control system*. That is by design, and it is fine — but it should
be stated plainly.

---

## 2. Where was the spec ambiguous?

The kernel itself was almost entirely unambiguous. The ambiguity lives at the
View boundary, where the spec is silent rather than contradictory. The
significant points:

1. **HEAD / "current branch" tracking.** This is the deepest ambiguity. The
   namespace maps `name -> hash` only. There is **no name -> name** indirection
   (the spec even says so: "Reference chains … Deeper indirection is a View
   concern"). But "current branch" is conceptually a pointer from HEAD to a
   *branch name*, not to a hash. The spec never says how a View is supposed to
   record "you are currently on branch `main`." I had to choose between (a)
   keeping it in View-process memory only, or (b) encoding it in an immutable
   object bound to a fixed name. I chose (b) for persistence; see §3.

2. **`resolve` vs `read`.** The "THE API" section lists exactly three
   operations (`Write`, `Read`, `Reference`). The task brief then says to
   *also* assume `kernel.resolve(name) -> str | None`. So is `resolve` part of
   the kernel API or a convenience? `Read` already resolves names internally,
   so `resolve` is technically derivable (call `Read` and catch the error, or
   parse). The brief inconsistency is minor but real.

3. **The "root namespace."** The spec mentions "the root namespace" (in the
   `Read` contract) but never defines its scope. Is it flat and global? Is
   there hierarchy? Are there reserved names? I assumed a flat global namespace
   and invented my own reserved-name convention (`refs/heads/...`, `HEAD`).

4. **Object format.** Completely unspecified. Bytes are bytes; the kernel does
   not know what they mean. A View must choose a serialization (I chose JSON)
   and a schema. Nothing in the spec constrains this, which is correct per the
   laws, but it means two independently-written Views cannot read each other's
   objects. There is no interop story.

5. **Tree structure.** Git uses nested trees (directory → subtree → blob). The
   spec says nothing. Flat path→blob map vs. nested trees is a free choice with
   real trade-offs (sharing, size, rename detection).

6. **Staging area / index.** Not mentioned at all. Is staging kernel state or
   View state? I treated it as transient View state (in-memory), like git's
   conceptual index.

7. **Error semantics.** The spec names two errors (`NOT_FOUND`,
   `HASH_NOT_FOUND`) but does not say how they are delivered (exceptions?
   result types?) or what the exact failure modes of each View operation are.
   I invented a small exception hierarchy.

8. **Multi-parent / merge semantics.** The spec's composition laws talk about
   reference chains and orphans but say nothing about commits with multiple
   parents. `log()` following "first parent" vs. topological order is
   unspecified.

9. **Author / timestamp / identity.** Not mentioned. I invented an `author`
   string and a `time.time()` timestamp on commits.

10. **What `checkout` does to the working tree.** The spec has no notion of a
    working tree. "Checking out" is purely a HEAD movement; the "files you
    see" are whatever the current commit's tree contains. I implemented
    `read_file` against the current commit's tree rather than maintaining a
    materialized working directory.

None of these blocked me, but each was a fork in the road where the spec
offered no signal.

---

## 3. What did you have to invent (patterns, conventions, structures)?

Almost the entire View layer. Specifically:

- **Three object types** with self-describing JSON schemas:
  - `blob`  = raw file bytes (no envelope).
  - `tree`  = `{"type":"tree","entries":{path: blob_hash}}` — a **flat**
    full-snapshot tree (current tree ∪ staged changes).
  - `commit` = `{"type":"commit","tree":..., "parents":[...], "message":...,
    "author":..., "timestamp":...}`.
- **Self-typing envelopes.** Because the kernel is bytes-only, I added a
  `"type"` field to trees and commits so a View can distinguish object kinds
  when reading by hash. (Blobs carry no envelope; they are just content.)
- **Ref naming convention:** `refs/heads/<branch>` → commit hash, deliberately
  mirroring Git so the model is familiar.
- **HEAD encoding as an immutable object.** Since names can't point to names, I
  store HEAD as a small object `{"type":"branch","name":...}` or
  `{"type":"detached","commit":...}` and bind the name `"HEAD"` to it via
  `Reference`. Every `checkout` writes a new HEAD object and re-points `HEAD`.
  This is the single most non-obvious invention, and it is what makes HEAD
  persist across process restarts.
- **Staging area** as an in-memory `dict[path -> bytes]`, cleared on
  `commit` and `checkout`.
- **Commit = full snapshot, not delta.** Each commit's tree is the complete
  path→blob map after staged changes are merged in. Simple, correct, space-
  wasteful. The kernel's content-addressing still dedups identical blobs
  across commits.
- **Linear-history `log()`** following the first parent, with a `seen` set as a
  cycle guard.
- **`checkout` semantics:** resolve as a branch first (via
  `refs/heads/<name>`), else fall back to a detached HEAD if the argument is a
  64-hex hash that resolves to a commit object.
- **An exception hierarchy** (`PondError` → `NotFound`, `NotACommit`) since the
  spec names error *conditions* but not their *representation*.
- **A mock in-memory kernel** (`InMemoryKernel`) to make the demo runnable. It
  is clearly marked as test harness, not part of the View, and the View never
  touches its internals — only the four sanctioned methods.
- **A 64-hex hash discriminator** (`_is_hex_hash`) so `Read`/`checkout` can
  tell "this looks like a hash" from "this looks like a name," matching the
  spec's `Read` contract.

Patterns I leaned on: **write-then-reference** (the spec mandates the hash
exist before `Reference`, so I always `write` the object before binding a name
to it), and **read-through resolution** (the View calls `read` with hashes it
already holds, and `resolve` only when it needs to know *whether* a name is
bound, e.g. branch existence checks).

---

## 4. What was impossible or required guessing?

Nothing was strictly **impossible**. The kernel is general enough that a Git
View is fully expressible. But several things required **guessing**, and a few
required **guessing that the spec actively declined to help with**:

- **Persistence of HEAD and the index across process restarts.** The spec
  never says whether a View is a long-lived process or a one-shot CLI. I
  *guessed* "must survive restart" and invented the HEAD-object pattern. Had I
  guessed "single process," I'd have kept HEAD in memory and the implementation
  would be simpler but non-persistent. The spec gives no way to know which is
  expected. (My demo verifies persistence by constructing a second `GitView` on
  the same kernel and showing HEAD is recovered.)

- **Whether `resolve` is real.** I needed it for "does this branch exist?" and
  "what's the current commit hash without materializing bytes?" I could have
  derived it from `read` + exception handling, but that is ugly and conflates
  existence with content. I *guessed* that `resolve` is an allowed convenience
  because the task brief explicitly lists it.

- **Tree shape (flat vs. nested).** I guessed flat. If a grader expected
  Git-faithful nested trees, my objects would not match — but the spec provides
  no basis to prefer one, so any choice is a guess.

- **Concurrency and multi-writer safety.** The spec explicitly disclaims this
  ("WHAT THE LAWS DO NOT GUARANTEE"). So I did not implement CAS or locking. If
  two Views commit to the same branch simultaneously, last-writer-wins on the
  `Reference` and history can fork silently. This is *guaranteed-unsolvable
  from the spec alone* — the spec says so. I treat it as an accepted
  limitation, not a bug.

- **Garbage collection.** Spec says "Views define reachability and implement
  GC" but gives no mechanism beyond "walk from names." I did not implement GC.
  Orphaned objects accumulate in my mock kernel forever. Implementing GC would
  require a reachability walk from `refs/heads/*` and `HEAD`, which is doable
  but was out of scope for the required operations. Noting it as a gap.

- **Merge / multi-parent commits.** Out of scope for the required ops, but the
  data model supports it (parents is a list). `log()` only follows first
  parent, so a merge's second parent is reachable only by explicit traversal.
  Guessed that linear history was acceptable.

- **Inter-View isolation.** The spec says "Views must use naming conventions or
  separate kernel instances." I used the naming-convention route (`refs/heads/`,
  `HEAD`). A second View sharing the same kernel and the same conventions
  would collide. True isolation requires separate kernel instances, which the
  spec permits but does not operationalize.

### Things I am confident about
The kernel contract is clean and I followed it exactly: every mutation in the
system is a `Reference` call; every persistent artifact is an immutable object
written via `write`; `read` is used purely for snapshots. The five laws hold in
my implementation. Branching is O(1) (one `Reference`). Reading at a commit is
a consistent snapshot. The required scenario passes, including the key
invariant: **`file3.txt` exists on `feature` and is correctly absent on
`main`**, and HEAD survives a fresh View instance bound to the same kernel.

### Bottom line
The spec is an excellent *kernel* spec and a poor *View* spec — deliberately,
since it explicitly defers View concerns. I could implement without asking
questions **only because I brought Git's design with me**. A spec-naive
engineer would have to invent the entire object model, ref layout, and HEAD
tracking with zero guidance, and would very likely produce something
incompatible with any other independently-written View. The kernel's laws are
universal; the View's conventions are not, and the spec offers no contract for
them.
