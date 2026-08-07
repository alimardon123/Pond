# Veteran Architect Review — Process

> **Purpose.** This document defines the canonical workflow for running
> a "Veteran Architect Review" on the Pond project. It exists so that
> every review follows the same disciplined process: stale docs are
> fixed BEFORE the review (so the reviewer sees reality, not drift),
> the review is compared against prior reviews (to measure progress),
> and the recommendations are evaluated against the project's vision
> before deciding what to do next.
>
> **When to run a Veteran Review.** Run one:
> - After completing a major tier of work (Tier 0, Tier 1, etc.)
> - Before any external-facing milestone (v1.0 binary, public blog post,
>   conference talk, investor pitch)
> - When the user explicitly asks for one
> - At minimum once per quarter as a health check

---

## The 5-step process

### Step 1 — Audit and fix stale docs (BEFORE the review)

**Goal:** ensure the reviewer sees reality, not drift. A reviewer who
finds doc-vs-code drift will lose trust in everything else, even if the
code is correct.

**What to do:**

1. Run `python3 scripts/verify_knowledge_graph.py` — confirm 100%
   coverage. If any files are missing, add them to `KNOWLEDGE_GRAPH.md`.
2. Run `PYTHONPATH=target/release python3 -m pytest tests/test_all.py -v`
   — confirm 0 failures (skips with documented reasons are OK). If any
   tests fail, either fix the code or skip with a clear reason.
3. Sweep the top-level docs for stale claims:
   - `README.md` — does every "feature" claim match the code?
   - `DESIGN_GOALS.md` §1.1 Known Gaps — are the gaps still accurate?
   - `docs/HONEST_COMPETITOR_COMPARISON.md` — does every "Supported /
     Falsified / Inconclusive" verdict match the code?
   - `SDK_SPEC.md` — does every code reference (class names, file paths,
     method signatures) match the current code?
   - `REPO_ORGANIZATION.md` — does every folder rule match the actual
     folder structure?
   - `PACKAGES.md` — does the dependency graph match reality?
4. Sweep the source for stale comments:
   - `grep -rn "TODO\|FIXME\|XXX\|HACK" bindings/python/core/ bindings/python/sdk/ lenses/`
   - For each TODO that references an issue now fixed, remove or update it.
5. Commit the doc fixes as a separate commit BEFORE running the review.
   The review commit should be purely additive (the review document
   itself), not mixed with doc fixes.

**Definition of done for Step 1:** the repo is in a state where a
reviewer reading any doc can trust that it accurately describes the
code. If you can't honestly say that, fix the docs before proceeding.

### Step 1.5 — Design principles compliance check (NEW)

**Goal:** verify the code/repo structure follows all 8 design principles.
This is a structural check, not just a doc check — it examines the actual
code organization, dependency graph, and API design.

**What to check** (against DESIGN_GOALS.md §3, the 8 principles):

1. **Simple (3.1):** Can the kernel be described in one sentence? Has it
   grown beyond "intellectually small"? Count the public API surface —
   if it's >10 methods, it may be too complex. Check: does any new
   feature add a kernel primitive that should be a lens/extension pattern?

2. **Powerful (3.2):** Can the proposed capability be expressed as data +
   a Lens? If a new feature requires kernel changes, it violates this
   principle. Check: are there kernel methods that should be lens-level?

3. **Performant (3.3):** Are optimizations in the right layer? The kernel
   should not cache, compress, index, or batch (beyond simple I/O
   batching). Check: is there optimization logic in bindings/python/core/ that
   belongs in bindings/python/sdk/ or ?

4. **Scalable (3.4):** The removability test — if package X is deleted
   entirely, does any lower-layer package break? Check: `grep -r` for
   imports that cross dependency boundaries (e.g., bindings/python/core importing
   from bindings/python/sdk).

5. **Efficient (3.5):** Are derived structures rebuildable from
   snapshots? Check: is there any authoritative metadata that can't
   be regenerated from the content-addressed blobs?

6. **Beautiful (3.6):** Is the dependency graph a DAG with all edges
   pointing downward? Check: draw the import graph and verify no cycles.
   Are folder names intuitive? Is the file structure ergonomic?

7. **Functional (3.7):** Before claiming "Pond can't do X," check:
   is there a missing Lens? A missing Physical Structure? A coordinator
   that could layer on the kernel? Most "can't" claims are missing
   lenses, not missing kernel primitives.

8. **Storage-Independent (3.8):** Can you switch execution engines
   without rewriting storage? Check: is any execution engine (DuckDB,
   Spark, Polars) imported in bindings/python/core/ or bindings/python/sdk/ (vs. only in
   pond-labs/ or lenses/)?

**Output:** a compliance table with ✅/❌/⚠️ for each principle, plus
specific file/line citations for any violations found. This table
becomes part of the review document (§"Design principles compliance").

### Step 2 — Run the Veteran Review (via subagent)

**Goal:** get an independent, brutally honest assessment from a
reviewer who has NOT been contaminated by the team's optimism.

**What to do:**

Launch a subagent (use `opus` model for the highest-quality reasoning)
with this prompt structure:

```
You are a veteran system architect and Fortune 100-level software
engineering manager reviewing the Pond storage system at
/home/z/my-project/pond_repo/. You have 25+ years of experience
building distributed systems, storage engines, and data platforms at
companies like Google, Meta, Databricks, Snowflake. You have NO prior
knowledge of this project — review it cold.

[If this is a re-review:]
Your previous review(s) are at:
- docs/VETERAN_ARCHITECT_REVIEW.md (V1)
- docs/VETERAN_ARCHITECT_REVIEW_V2.md (V2)
- ... etc.

Read them first to understand your prior stance, then assess what's
changed.

## What to read (in this order)
[list the canonical reading order — see below]

DO NOT read worklog.md (the team's research log — would bias your review).

## What to evaluate
[list the evaluation criteria — see below]

## Output
Save your review to docs/VETERAN_ARCHITECT_REVIEW_V{n}.md.
Structure: [see below]
```

**Canonical reading order** (give this to the subagent):

1. `DESIGN_GOALS.md` — the canonical entry point
2. `REPO_ORGANIZATION.md` — folder rules, dependency rules
3. `PACKAGES.md` — package structure
4. `SDK_SPEC.md` — the authoritative SDK contract
5. `README.md` — 5-minute intro
6. `bindings/python/core/kernel.py` — the storage kernel itself
7. `bindings/python/sdk/base_lens.py` — the shared namespace base
8. `bindings/python/sdk/extensions/physical_structures/unified_storage.py` — the
   universal storage backend (sample key methods, don't read all 5500 LOC)
9. `lenses/keyvalue/keyvalue_lens.py` — a production lens
10. `lenses/lakehouse/lakehouse_lens.py` — another production lens
11. `tests/architecture/architecture_laws.py` — the executable architecture spec
12. `docs/HONEST_COMPETITOR_COMPARISON.md` — the honest self-assessment
13. Run `python3 -m pytest tests/test_all.py -v` yourself
14. Run `python3 scripts/verify_knowledge_graph.py` yourself

**Evaluation criteria** (give this to the subagent):

1. Core design soundness — is the kernel sufficient for the workloads claimed?
2. ProllyTree/UnifiedStorage as universal backend — right call or wrong?
3. Lens architecture — does the "no lens-to-lens inheritance" rule scale?
4. Performance competitiveness — how do the numbers compare to real systems?
5. Storage-Independence claim — actually true given the custom PND2 format?
6. Concurrency & distributed scaling — does the CRDT model scale?
7. Maturity vs. hype — are there current overclaims?
8. Missing capabilities — what concrete features are missing?
9. Comparison to existing systems — Iceberg, DuckDB, Git, FAISS, etc.
10. The "weekly question" — if you deleted everything except bindings/python/core
    and bindings/python/sdk, would the architecture still make sense?

**Required output structure:**

1. Executive summary (3-5 sentences: is this ready to compete or not?)
2. What's genuinely good (specific architectural strengths, with evidence)
3. Critical gaps (top 5-10 things blocking production use, ranked by severity)
4. Design risks (architectural choices that might not scale)
5. Performance assessment (is the perf story credible? what's missing?)
6. Comparison to competitors (where does Pond win/lose?)
7. Recommendations (concrete next steps, prioritized)
8. Verdict (one paragraph: invest more, pivot, or kill?)

**For re-reviews, also:**

9. Tier verification — did the team actually fix what they claimed?
   (Read the code, don't trust the worklog.)
10. Comparison to prior review — what improved? what regressed? what's new?
11. Updated verdict — has the recommendation changed? why or why not?
12. Strategic answers to the user's current questions (if any)

### Step 3 — Compare current review vs. prior reviews

**Goal:** measure progress objectively. Don't just read the new review
in isolation — compare it to the prior reviews to see what changed.

**What to do:**

Create a comparison table:

| Aspect | V1 | V2 | V3 | ... | Trend |
|---|---|---|---|---|---|
| Verdict | "Invest narrowly" | "Invest, specialize" | ? | | improving / stagnating / regressing |
| Critical issues count | 10 | 8 (2 fixed, 0 new) | ? | | |
| Test suite | 17/5 pass/fail | 20/2 skip/0 fail | ? | | |
| Doc drift count | 48 missing files | 0 missing + 8 stale | ? | | |
| Overclaims | 5 (ACID, IVF, ...) | 0 (all corrected) | ? | | |
| Performance | 2-4x slower than DuckDB | ? | | | |

Save this comparison to `docs/VETERAN_REVIEW_COMPARISON.md` (append a
new section for each review round).

### Step 4 — Evaluate the veteran's recommendations

**Goal:** decide which recommendations to act on, which to defer, and
which to reject. The veteran is smart but doesn't know the project's
full context — the team must apply judgment.

**What to do:**

For each recommendation, ask:

1. **Does it serve the user's vision?** The user wants:
   - Reliable, powerful, performant, functional, extensible, simple,
     storage-independent, PB-scalable, portable as the backbone of any
     application
   - A small minimal lightweight binary (DuckDB philosophy)
   - Generic cross-language SDK solution
   - Future sibling project: execution engine (Spark/Flink alternative)
2. **Does it align with the 8 design principles?** (Simple, Powerful,
   Performant, Scalable, Efficient, Beautiful, Functional, Storage-Indep)
3. **What's the impact-to-effort ratio?** High impact + low effort = do
   first. Low impact + high effort = defer.
4. **Does it create new technical debt?** A recommendation that adds
   complexity without proportional benefit should be rejected.
5. **Is there a better alternative the veteran didn't consider?**

Categorize each recommendation:
- **Accept** — will do, with a timeline
- **Defer** — will do later, with a trigger condition
- **Reject** — won't do, with a reason
- **Modify** — will do a modified version, with the modification explained

Save this evaluation to `docs/VETERAN_REVIEW_EVALUATION_V{n}.md`.

### Step 5 — Decide what to do next and write it down

**Goal:** translate the evaluation into a concrete action plan.

**What to do:**

Write a "Next Steps" document that:

1. Summarizes the current state (tests, docs, known gaps)
2. Lists the accepted recommendations in priority order
3. For each, gives:
   - What to do (concrete, actionable)
   - Why (referencing the veteran's review + the user's vision)
   - Estimated effort
   - Success criteria (how do we know it's done?)
   - Dependencies (what must be done first?)
4. Identifies the single highest-priority next action
5. Lists what NOT to do (anti-recommendations) with reasons

Save this to `docs/NEXT_STEPS_V{n}.md` (or update the existing
`docs/NEXT_STEPS_DEEP_REVIEW.md`).

---

## Why this process exists

The first Veteran Review (V1) found 10 critical issues. The team fixed
6 of them in Tier 0. The V2 re-review verified the fixes were real, but
found 8 NEW doc-drift items introduced by the fix work itself — the
team fixed code in places and forgot to update the corresponding docs.

This is a recurring failure mode: every code change creates doc drift
if the docs aren't updated in the same commit. The Step 1 "audit and
fix stale docs BEFORE the review" exists to break this cycle.

The Step 3 "compare to prior reviews" exists because without it, the
team can't tell if they're making progress or just treading water. The
comparison table makes progress (or lack thereof) visible.

The Step 4 "evaluate recommendations" exists because the veteran is
smart but doesn't know the user's full vision. Some recommendations
might be technically correct but strategically wrong for this project.
The team must apply judgment, not blindly implement.

The Step 5 "decide what to do next" exists because reviews without
action are theater. Every review must produce a concrete next step.

---

## Anti-patterns to avoid

- **Don't skip Step 1.** Running a review on a repo with stale docs
  wastes the reviewer's time and produces a review that's "correct
  about the docs but wrong about the code." Always fix docs first.
- **Don't blindly implement all recommendations.** The veteran doesn't
  know the user's vision. Evaluate each recommendation against the
  vision before acting.
- **Don't run reviews too frequently.** A review every week is noise.
  Run them after meaningful work (a tier completion, a milestone, a
  quarter).
- **Don't hide bad reviews.** If the veteran says "kill the project,"
  that's valuable information. Share it with the user honestly.
- **Don't use the review as a substitute for the user's judgment.** The
  veteran advises; the user decides.

---

## Review history

| Review | Date | Verdict | Critical issues | Doc fixes before review |
|---|---|---|---|---|
| V1 | 2026-08-07 | "Invest narrowly, after Tier 0" | 10 | (none — V1 found the drift) |
| V2 | 2026-08-07 | "Invest, but specialize" | 8 (2 fixed, 8 new doc drift) | (Tier 0 fixes, but introduced drift) |
| V3 | (future) | ? | ? | (Step 1 audit done first) |
