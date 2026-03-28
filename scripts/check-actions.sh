#!/usr/bin/env bash

set -euo pipefail

repo_root=$(git rev-parse --show-toplevel 2>/dev/null || pwd)

cd "${repo_root}"

if [ ! -d .github/workflows ]; then
  echo "No .github/workflows directory found; skipping workflow audit."
  exit 0
fi

actionlint_bin=$(scripts/install-actionlint.sh)
"${actionlint_bin}"
