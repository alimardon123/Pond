#!/usr/bin/env bash
# check.sh — the gate, in one place.
#
# Exists because summing "N passed" across suites is not a check. A suite that
# aborts contributes no line to sum, so the total silently drops and the run
# still looks green — which is how a commit went out with two failing tests and
# a lower total than the run before it.
#
# So this trusts cargo's exit status, not arithmetic over its output, and says
# plainly which gate failed.

set -uo pipefail
cd "$(dirname "$0")/.."

CLIPPY_ALLOW=(
  -A clippy::too_many_arguments
  -A clippy::type_complexity
  -A clippy::needless_range_loop
  -A clippy::large_enum_variant
  -A clippy::len_without_is_empty
  -A clippy::manual_range_contains
  -A clippy::manual_strip
  -A clippy::doc_lazy_continuation
)

fail() { echo "FAIL: $1"; exit 1; }

# Disk exhaustion presents as ordinary test failures ("No space left on
# device"), which wastes time looking for a bug that is not there. Say so up
# front instead.
avail_kb=$(df -Pk . | awk 'NR==2 {print $4}')
# A full workspace build, test and clippy run needs real headroom: rustdoc and
# the linker fail in ways that look like ordinary test failures when they run
# out. 2 GiB was not enough — a run failed at 7 GiB free.
if [ "$avail_kb" -lt 10485760 ]; then
  echo "warning: only $((avail_kb / 1024)) MiB free. A full run needs headroom;"
  echo "         failures here may be disk exhaustion rather than bugs."
  echo "         try: rm -rf target/debug/incremental"
fi

# Which compiler does "passing" refer to? CI pins one; if this machine has a
# different one, a green run here does not predict a green run there — the
# first CI run of this workflow failed on lints the local toolchain did not
# have. Read the pin from the workflow so there is one source of truth.
pinned=$(sed -n 's/^ *toolchain: "\(.*\)"/\1/p' .github/workflows/rust.yml | head -1)
local_rustc=$(rustc --version 2>/dev/null | awk '{print $2}')
if [ -n "$pinned" ] && [ -n "$local_rustc" ] && [ "$pinned" != "$local_rustc" ]; then
  echo "warning: CI pins rustc $pinned, this machine has $local_rustc."
  echo "         clippy lints differ between releases, so a pass here may"
  echo "         still fail there. rustup toolchain install $pinned"
fi

# The disk cache defaults to the platform cache directory, which is right for
# a real run and wrong for a test suite: the suite would write into the
# developer's home, share state between runs, and grow without anyone asking.
# Point it somewhere disposable for the duration.
POND_CACHE_DIR="$(mktemp -d)"
export POND_CACHE_DIR
trap 'rm -rf "$POND_CACHE_DIR"' EXIT

echo "== workflows =="
# Cheap, and first alongside the secret scan, for the same reason: a workflow
# file is never run locally, so a syntax error in one is invisible until CI
# reports a failure that never started. That happened here — see
# scripts/workflowlint.py.
python3 ./scripts/workflowlint.py || fail "workflows"

echo "== secrets =="
# First, and cheap. A credential that reaches a commit is not fixable by a
# later commit — the value stays in history and `git log -S` finds it — so the
# only useful place to catch one is before it lands.
./scripts/secretscan.sh || fail "secrets"

echo "== build =="
cargo build --workspace 2>&1 | grep -E "^error" -A 5 && fail "build"

echo "== test =="
test_out=$(mktemp)
if ! cargo test --workspace >"$test_out" 2>&1; then
  # E0460 is "found possibly newer version of crate", which means another
  # cargo process rebuilt a dependency underneath this one — two runs sharing
  # ./target. It arrives looking exactly like a real failure ("doctest
  # failed", a named crate, a wall of rustdoc arguments), and chasing it as
  # one wastes an afternoon. Same reasoning as the disk-space warning above:
  # a failure mode that impersonates a different failure mode should say what
  # it is.
  if grep -q "E0460" "$test_out"; then
    echo "  E0460: another cargo process is using ./target concurrently."
    echo "  This is contention, not a test failure. Re-run when it is done."
    fail "tests (concurrent cargo)"
  fi
  grep -E "^(error|test result: FAILED)|^---- |panicked at|assertion" -A 4 "$test_out" | head -40
  fail "tests"
fi
grep -E "^test result" "$test_out" |
  awk -F'[ ;]' '{p+=$4; f+=$6} END {printf "  %d passed, %d failed\n", p, f}'
rm -f "$test_out"

echo "== clippy =="
if ! cargo clippy --workspace --exclude pond_python --all-targets -- \
     -D warnings "${CLIPPY_ALLOW[@]}" >/dev/null 2>&1; then
  cargo clippy --workspace --exclude pond_python --all-targets -- \
    -D warnings "${CLIPPY_ALLOW[@]}" 2>&1 | grep -E "^error" -A 6 | head -20
  fail "clippy"
fi

echo "all gates passed"
