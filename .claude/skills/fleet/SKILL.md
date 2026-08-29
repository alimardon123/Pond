---
name: fleet
description: Run a task through a multi-model pipeline — plan, implement on cheap models, adversarially review on an expensive model in a fresh context, fix, then gate on an explicit done-check. Use when asked to "run the fleet", "fleet this", "pipeline this", or when a task is large enough that delegated implementation plus independent review beats doing it inline.
---

# Fleet

A task pipeline that separates *doing* from *judging*, and puts them in different
contexts on different model tiers.

The core claim this design rests on: **an implementer cannot reliably review its own
work.** Not because cheap models are bad, but because the context that wrote the code
contains the same blind spots that produced the bug. Independent context is the
mechanism; model tiering is just cost optimization on top of it.

## Stages

| # | Stage | Agent | Tier | Contract |
|---|-------|-------|------|----------|
| 1 | Plan | orchestrator (you) | current | Decompose into independently-verifiable units + acceptance criteria |
| 2 | Recon | `scout` | Haiku | Locate files, trace call sites. Read-only. Optional — skip if scope is known |
| 3 | Implement | `coder` | Sonnet | One agent per unit, parallel. Structured report back |
| 4 | Review | `reviewer` | Opus | Fresh context. Adversarial. Reports, does not fix |
| 5 | Fix | `coder` | Sonnet | Receives ranked defects, fixes only those |
| 6 | Gate | `judge` | Opus | DONE / ITERATE / ESCALATE against original criteria |

Loop 4→5→6 while `ITERATE`, capped at **3 iterations**. On `ESCALATE`, stop and surface
to the human — repeated failures of the same defect class mean the spec or approach is
wrong, and more iterations will not fix it.

## Running it

### Stage 1 — Plan (do this yourself, do not delegate)

Decomposition is the highest-leverage step and the one where a cheap model costs the most.
Produce, before spawning anything:

- **Units of work** — each independently implementable and independently verifiable.
  If two units must be understood together to be correct, they are one unit.
- **Acceptance criteria** — concrete and checkable. "Handles malformed input" is not a
  criterion; "raises ValueError on leading zeros, empty identifiers, and trailing
  whitespace" is.
- **Verification command** — the exact command that proves the unit works.

If the task has fewer than two units and the acceptance criteria fit in a sentence, **do
not run the fleet.** Do it inline. The pipeline costs more than it returns on small work.

### Stage 3 — Implement

Spawn one `coder` per unit, in a single message so they run concurrently.

If units touch the same files and the repo is git-backed, pass `isolation: "worktree"`
so they cannot clobber each other. This costs ~200-500ms and disk per agent — skip it
when units are in disjoint files.

Give each coder: the unit spec, its acceptance criteria, its verification command, and
the relevant `path:line` refs from recon. Do not give it the whole plan — extra scope
invites scope creep.

### Stage 4 — Review (the load-bearing stage)

Spawn `reviewer` in a **fresh agent**, never by continuing the coder.

Hand it the diff and the acceptance criteria. You may hand it the coder's self-report,
but label it explicitly as a claim under test. Do not let it stand as ground truth —
in practice an implementer's self-declared "risks" list the places it already thought
about, so the surviving defect is somewhere else.

Require it to write probes the implementer did not author.

### Stage 5 — Fix

New `coder`, given only the ranked defects. Not a general "improve this" pass — scope it
to the findings, or you get churn.

### Stage 6 — Gate

`judge` decides against the **original** request, not against the last review. Pipelines
drift: three clean iterations of fixing review nits, while the feature that was actually
asked for was never built, is a FAIL.

## Model tiering

Set per call via `model:` on the Agent tool (`sonnet` / `haiku` / `opus` / `fable`), or
by the `model:` field in each agent's frontmatter. Frontmatter is the default; the tool
parameter overrides it per call.

To pin a specific version rather than an alias, put the full model ID in frontmatter
(e.g. `model: claude-opus-4-8`). Aliases track the current model in that tier and will
move under you; IDs do not.

Reasonable starting split — treat as a hypothesis to test on your own work, not received
wisdom:

- **Haiku** — recon, mechanical search, file location
- **Sonnet** — implementation on decided scope, test writing, refactors
- **Opus** — planning, review, done-gating

## Observability

Spawn implement-stage agents with `run_in_background: true` so the user can watch progress
and interject. Foreground only the stage whose result you immediately need.

## When NOT to use this

- Single-file changes with obvious acceptance criteria — just do it
- Exploratory work where the spec emerges as you go — the plan stage has nothing to fix
- Anything where you'd spend longer writing the unit spec than writing the code

The pipeline pays for itself on bulk implementation, wide refactors, and work where
being *wrong* is expensive. It is pure overhead on small, cheap-to-fix tasks.
