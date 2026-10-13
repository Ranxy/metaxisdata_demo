#!/usr/bin/env bash
# Fail if anything that looks like a real credential ends up in the tree.
#
# This repo is public and only ever contains throwaway local demo values.
# Everything that is actually a secret -- OpenLineage ingestion keys, application
# keys, service-account keys -- is generated at runtime into the gitignored
# state/ volume and must never be committed.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

pattern='ol_[0-9a-fA-F]{32,}|asql_[A-Za-z0-9_-]{16,}|(^|[^A-Za-z0-9])sa_[A-Za-z0-9]{16,}|ghp_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,}|AKIA[0-9A-Z]{16}|---+BEGIN [A-Z ]*PRIVATE KEY---+'

if hits=$(grep -rInE "$pattern" . \
  --exclude-dir=.git \
  --exclude-dir=state \
  --exclude-dir=node_modules 2>/dev/null); then
  echo "check-secrets: possible credential in the working tree" >&2
  echo "$hits" >&2
  exit 1
fi

echo "check-secrets: no credentials found"
