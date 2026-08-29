#!/usr/bin/env python3
"""workflowlint.py — refuse a workflow file GitHub would refuse to start.

Why this exists, precisely:

    - name: Run differential testing (reference vs Pond, 100 scenarios)
      run: python -c "... print(f'Passed: {r[\"passed\"]}...') ..."

That line sat in `.github/workflows/view-laws.yml` for weeks. A YAML plain
scalar may not contain ": " — the colon-space inside `'Passed: '` makes the
parser read a nested mapping — so the whole file was unparseable and every
run was a startup failure: conclusion "failure", zero jobs, finished in the
same second it began. 345 consecutive runs. The repository believed it had CI
and had none, and nothing said so, because a red X for a run that never
started looks exactly like a red X for a test that failed.

A workflow is the one file in the tree that is never executed locally, so it
is the one file whose syntax nothing checks until it is too late to matter.
This checks it.

The check is deliberately narrow: parse, then assert the handful of things
whose absence means "will not start". It is not a schema validator — GitHub
owns that — and it should stay cheap enough to run first in every gate.
"""

import sys
from pathlib import Path

try:
    import yaml
except ImportError:
    # Not a skip. A gate that quietly opts out when a dependency is missing is
    # how the bug above survived: something looked green while checking
    # nothing.
    sys.exit("workflowlint: PyYAML is required.\n  pip install pyyaml")

ROOT = Path(__file__).resolve().parent.parent
WORKFLOWS = ROOT / ".github" / "workflows"


def check(path):
    """Return a list of problems with one workflow file."""
    text = path.read_text()
    try:
        doc = yaml.safe_load(text)
    except yaml.YAMLError as e:
        mark = getattr(e, "problem_mark", None)
        where = f" at line {mark.line + 1}, column {mark.column + 1}" if mark else ""
        return [f"does not parse as YAML{where}: {getattr(e, 'problem', e)}"]

    if not isinstance(doc, dict):
        return ["is not a mapping"]

    problems = []

    # `on:` is the YAML 1.1 boolean `true`, which is why PyYAML hands it back
    # under the key True rather than the string. Accept either; GitHub does.
    if "on" not in doc and True not in doc:
        problems.append("has no `on:` trigger, so it can never fire")

    jobs = doc.get("jobs")
    if not isinstance(jobs, dict) or not jobs:
        problems.append("has no `jobs:`")
        return problems

    for name, job in jobs.items():
        if not isinstance(job, dict):
            problems.append(f"job {name!r} is not a mapping")
            continue
        # A job either runs somewhere or delegates to a reusable workflow.
        if "runs-on" not in job and "uses" not in job:
            problems.append(f"job {name!r} has neither `runs-on` nor `uses`")
        if "uses" not in job and not job.get("steps"):
            problems.append(f"job {name!r} has no steps")

    return problems


def main():
    if not WORKFLOWS.is_dir():
        return 0

    files = sorted(
        p for p in WORKFLOWS.iterdir() if p.suffix in (".yml", ".yaml")
    )
    if not files:
        return 0

    failed = False
    for path in files:
        problems = check(path)
        for p in problems:
            print(f"{path.relative_to(ROOT)}: {p}")
            failed = True

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
