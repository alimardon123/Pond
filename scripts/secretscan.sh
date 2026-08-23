#!/usr/bin/env bash
# secretscan.sh — refuse to let a credential become permanent.
#
# This exists because it already happened. Live R2 credentials were pasted into
# a review document and committed; the working tree was redacted afterwards,
# but a commit is forever, and `git log -S` still finds them. Redaction is not
# revocation — the only real remedy was rotating the token, and nothing in the
# repository would have stopped the same thing happening the next day.
#
# So this scans what is *tracked*, which is the set that becomes permanent.
# `.env` is deliberately ignored: it is gitignored and is where secrets are
# supposed to live. Anything matching here is a secret in a file that will be
# pushed.
#
# It is intentionally narrow. A scanner that cries wolf is turned off, and a
# scanner that is turned off catches nothing.

set -uo pipefail
cd "$(dirname "$0")/.."

status=0
report() {
  echo "SECRET: $1"
  echo "  $2"
  status=1
}

# Only tracked files, and never the local env file.
files=$(git ls-files | grep -vE '^(\.env|scripts/secretscan\.sh)$' || true)
[ -z "$files" ] && exit 0

# Placeholders and examples are the whole reason a scanner gets switched off.
# Anything naming itself as one is not a leak, and this filter applies to every
# rule rather than to some of them — the first version missed
# `AKIAIOSFODNN7EXAMPLE`, the key printed in AWS's own documentation.
not_a_placeholder() {
  grep -viE 'example|placeholder|your[_-]?(key|token|secret)|xxx|<[^>]+>|\bfake\b|\bdummy\b|redacted|\$\{|%s|\{\{|test[_-]?(key|secret)' || true
}

# 1. AWS-style access key ids. The prefix makes this near-zero false positive.
if hits=$(git grep -nIE '\b(AKIA|ASIA)[0-9A-Z]{16}\b' -- $files 2>/dev/null | not_a_placeholder); then
  [ -n "$hits" ] && report "AWS access key id" "$hits"
fi

# 2. A secret assigned to a credential-named variable. Requires both a
#    credential-shaped name and a long opaque value, so `KEY = "id"` or a
#    documented placeholder does not trip it.
# A bare `token` is not on this list: in this repository it overwhelmingly
# means an S3 pagination continuation token, and a rule that fires on those is
# a rule someone deletes. The names kept are ones that cannot mean anything
# except a credential.
pattern='(aws_secret_access_key|aws_access_key_id|secret_access_key|api[_-]?key|client[_-]?secret|auth[_-]?token|access[_-]?token|bearer|password|passwd)["'"'"']?[[:space:]]*[=:][[:space:]]*["'"'"']?[A-Za-z0-9/+_-]{24,}'
if hits=$(git grep -nIiE "$pattern" -- $files 2>/dev/null | not_a_placeholder); then
  [ -n "$hits" ] && report "credential assigned in a tracked file" "$hits"
fi

# 3. The env file must never become tracked, whatever its contents.
if git ls-files --error-unmatch .env >/dev/null 2>&1; then
  report ".env is tracked" "it is where credentials live and must stay untracked"
fi

if [ "$status" -ne 0 ]; then
  echo
  echo "A tracked file appears to contain a credential."
  echo "Redacting it later does not help: the commit keeps the value and"
  echo "'git log -S' finds it. Move it to .env (gitignored) before committing."
  echo "If this is a false positive, make the placeholder name itself as one."
fi
exit "$status"
