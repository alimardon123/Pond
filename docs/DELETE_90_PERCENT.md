# Delete 90% — Documentation Simplification Exercise

> Pretend we're preparing for SOSP. Delete half the documentation,
> half the reports, half the diagrams, half the examples. Ask: does
> the contribution become clearer?
>
> **Answer: yes.** This document identifies what to keep, what to
> cut, and what to merge.

---

## 1. Current state

The repo currently has **26,621 lines of documentation** across
~30 markdown files. This is too much. The signal is buried.

The five biggest files (by LOC):
1. `docs/POND_FORMAL_ALGEBRAS.md` — 2,406 lines (Parts I-IV)
2. `docs/POND_WHITEPAPER.md` — 931 lines
3. `docs/POND_STORAGE_MODEL.md` — 849 lines
4. `docs/POND_THIRD_RED_TEAM.md` — 704 lines
5. `docs/POND_SECOND_RED_TEAM.md` — 690 lines

Plus 1,928 lines of `worklog.md` and 968 lines of `DESIGN_GOALS.md`.

---

## 2. What to KEEP (the essential 10%)

These are the documents that *carry the contribution*. Everything
else should either be cut or merged into these.

### 2.1 KEEP: `docs/POND_WHITEPAPER.md` (931 lines → keep, trim to ~600)

The whitepaper is the contribution. It explains Pond from first
principles, compares to peers, and is honest about gaps. Cut:
- Appendix A (artifact inventory) — readers can `ls` the repo.
- Appendix B (how to attack) — fold into the body.
- Redundant restatements of the six substrates (appears in §2 and §3).

### 2.2 KEEP: `docs/WHERE_POND_FAILS.md` (395 lines → keep as is)

The most credible document in the repo. Honest scope. Don't touch.

### 2.3 KEEP: `docs/POND_FORMAL_ALGEBRAS.md` (2,406 lines → trim to ~1,200)

The formal model is the contribution's backbone. But it's 4 parts
across 2,406 lines. Trim:
- Part I (8 algebras, ~700 lines) — keep, but cut the redundant
  summaries at the end of each section.
- Part II (6 algebras, ~830 lines) — keep, but the "Summary of Part II"
  table (§16) duplicates the per-algebra sections. Cut.
- Part III (3 algebras, ~680 lines) — keep, but the "Open Questions"
  section (§21) is now closed. Cut.
- Part IV (2 demotions, ~165 lines) — keep; this is the honest
  correction that resulted from Phase L.

The formal algebras doc should be ~1,200 lines after trimming.

### 2.4 KEEP: `lenses/lakehouse/lakehouse.py` + `pond-labs/feature_store_lens.py` + `pond-labs/interop_demo.py`

These are the demonstrations. Code, not docs. Keep all three.

### 2.5 KEEP: `docs/POND_PHASE_Q_BENCHMARKS.md` (344 lines → keep, trim)

The benchmark report has honest numbers. Cut:
- The "what benchmarks do NOT measure" section — fold into one
  sentence in the conclusion.
- The appendix with raw output — readers can re-run.

### 2.6 KEEP: `DESIGN_GOALS.md` (968 lines → trim to ~400)

Currently has every phase from A to Q. Trim:
- Remove the per-phase history (Phase A through Phase P). The
  whitepaper replaces it.
- Keep only: §1 (what Pond is), §2 (main goal), §3 (six principles),
  §8 (current roadmap — Phase Q only), §9 (for future agents).

### 2.7 KEEP: `pond-core/pond_minimal.py` (FROZEN, do not touch)

The kernel. The contribution's core artifact.

### 2.8 KEEP: `tla/PondKernel.tla` + `tla/PondKernel.cfg` + `tla/README.md`

The TLA+ proof. Small, focused, demonstrates formal verification.

---

## 3. What to CUT (the 90%)

### 3.1 CUT: All Phase reports except Phase Q

- `docs/POND_PHASE_L_REPORT.md` (431 lines) — superseded by whitepaper.
- `docs/POND_PHASE_N_REPORT.md` (207 lines) — superseded by whitepaper.
- `docs/POND_PHASE_O_REPORT.md` (277 lines) — superseded by whitepaper.
- `docs/POND_PHASE_P_REPORT.md` (250 lines) — superseded by whitepaper.
- `docs/POND_PHASE_Q_REPORT.md` (250 lines) — keep the findings, cut
  the rest. Merge the "what changed" into the whitepaper.

**Total cut: ~1,400 lines.**

### 3.2 CUT: Red team reports (move to archive)

- `docs/POND_SECOND_RED_TEAM.md` (690 lines) — historically interesting,
  but the findings are already in the formal algebras. Move to
  `docs/archive/`.
- `docs/POND_THIRD_RED_TEAM.md` (704 lines) — same. Move to
  `docs/archive/`.

**Total cut: ~1,400 lines** (moved to archive, not deleted).

### 3.3 CUT: The mathematical model (superseded)

- `docs/POND_MATHEMATICAL_MODEL.md` (626 lines) — superseded by
  `POND_FORMAL_ALGEBRAS.md` Parts I-IV. The formal algebras doc is
  the corrected, demoted, post-red-team version. Cut the original.

**Total cut: ~626 lines.**

### 3.4 CUT: The storage model paper (superseded)

- `docs/POND_STORAGE_MODEL.md` (849 lines) — the original 15-chapter
  paper. Superseded by the whitepaper. Move to `docs/archive/`.

**Total cut: ~849 lines.**

### 3.5 CUT: Phase review reports

- `validation/red_team_review.md` (874 lines) — the first red team.
  Findings are in `POND_SECOND_RED_TEAM.md`. Move to archive.
- `validation/second_red_team_review.md` (543 lines) — duplicate of
  `docs/POND_SECOND_RED_TEAM.md`. Cut.

**Total cut: ~1,400 lines.**

### 3.6 CUT: Conceptual design docs that the whitepaper replaces

- `docs/FORMAL_ALGEBRA.md` (530 lines) — early version of formal
  algebras. Superseded. Cut.
- `docs/FORMAL_SPEC.md` (333 lines) — early version of the kernel
  spec. Superseded by `POND_FORMAL_ALGEBRAS.md` §9 (Substrate
  Algebra). Cut.
- `docs/POND_MODEL_REVISION.md` (249 lines) — intermediate revision
  notes. Superseded. Cut.

**Total cut: ~1,112 lines.**

### 3.7 CUT: Comparison docs (folded into whitepaper)

- `docs/PEER_COMPARISON.md` (424 lines) — folded into whitepaper §5.
  Cut.
- `docs/LIQUID_CLUSTERING_COMPARISON.md` (330 lines) — niche; the
  Liquid Clustering comparison was a research exercise. Cut.

**Total cut: ~754 lines.**

### 3.8 CUT: Use case docs (folded into pond-labs)

- `docs/FEATURE_STORE_USE_CASE.md` (444 lines) — superseded by
  `pond-labs/feature_store_lens.py` (which is the actual
  implementation). Cut.

**Total cut: ~444 lines.**

### 3.9 CUT: Worklog (move to archive)

- `worklog.md` (1,928 lines) — append-only log of every task.
  Historically interesting but not contribution. Move to
  `docs/archive/`.

**Total cut: ~1,928 lines** (moved to archive).

### 3.10 CUT: RFCs (move to archive)

- `rfcs/` (~13 RFCs) — the design decisions are in the formal
  algebras and whitepaper. Move RFCs to `docs/archive/rfcs/`.

**Total cut: ~2,000 lines** (moved to archive).

---

## 4. What to MERGE

### 4.1 MERGE: Lens contract + Lens authors guide

- `docs/LENS_INTERPRETATION_CONTRACT.md` + `docs/LENS_AUTHORS_GUIDE.md`
  + `docs/LENS_INTEROP_SPEC.md` — three docs about the same thing.
  Merge into one `docs/LENS_GUIDE.md` (~150 lines).

### 4.2 MERGE: Getting started + README

- `docs/GETTING_STARTED.md` + `README.md` + `POND.md` — three
  introductory docs. Merge into one `README.md` (~200 lines).

### 4.3 MERGE: Rejected designs into whitepaper

- `docs/REJECTED_DESIGNS.md` (305 lines) — fold the key rejections
  into the whitepaper's "What Pond does NOT do" section.

---

## 5. Target state

After deletion and merging:

| Doc | Lines | Status |
|---|---|---|
| `README.md` | ~200 | merged from 3 intro docs |
| `docs/POND_WHITEPAPER.md` | ~600 | trimmed from 931 |
| `docs/WHERE_POND_FAILS.md` | ~395 | unchanged |
| `docs/POND_FORMAL_ALGEBRAS.md` | ~1,200 | trimmed from 2,406 |
| `docs/POND_PHASE_Q_BENCHMARKS.md` | ~250 | trimmed from 344 |
| `docs/LENS_GUIDE.md` | ~150 | merged from 3 lens docs |
| `DESIGN_GOALS.md` | ~400 | trimmed from 968 |
| `tla/PondKernel.tla` | 159 | unchanged |
| `tla/PondKernel.cfg` | 16 | unchanged |
| `tla/README.md` | 47 | unchanged |
| **Total** | **~3,400** | **down from 26,621 (87% reduction)** |

Plus `docs/archive/` containing the historical docs (red teams,
storage model paper, RFCs, worklog) for anyone who wants the full
history.

---

## 6. Why this matters

The current 26,621 lines of docs obscure the contribution. A
reviewer opening the repo sees 30 markdown files and doesn't know
where to start. After the cut, the reviewer sees:

1. `README.md` — 5-minute intro.
2. `docs/POND_WHITEPAPER.md` — the contribution.
3. `docs/WHERE_POND_FAILS.md` — the honest scope.
4. The code (`pond-core/`, `lenses/lakehouse/`, `pond-labs/`).

That's it. The formal algebras are there for those who want rigor;
the TLA+ is there for those who want proof; the rest is archive.

**Great papers get smaller over time.** The Pond repo should too.

---

## 7. What NOT to delete

- **The kernel** (`pond-core/pond_minimal.py`) — frozen, never touch.
- **The TLA+ spec** — small, focused, demonstrates formal verification.
- **The whitepaper** — the contribution.
- **The falsification doc** — the credibility.
- **The flagship code** (lakehouse, feature store, interop) — the
  demonstrations.
- **The LOC benchmark** — the compelling metric.

Everything else is negotiable.

---

## 8. Implementation note

This document is a *recommendation*, not an executed deletion. The
actual deletion requires:

1. Creating `docs/archive/`.
2. Moving the cut docs there (preserving history).
3. Merging the merge-targets.
4. Updating cross-references in the kept docs.

This is a half-day of careful work. It should be done before
submitting the whitepaper for external review.

The signal-to-noise ratio matters. A reviewer who opens a 26K-line
repo bounces. A reviewer who opens a 3.4K-line repo reads.
