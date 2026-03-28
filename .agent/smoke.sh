#!/usr/bin/env bash

set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd "${script_dir}/.." && pwd)
hyperdex_root=$("${script_dir}/hyperdex-root.sh")

export HYPERDEX_ROOT="${hyperdex_root}"

(
  cd "${repo_root}"
  "${repo_root}/scripts/cabal.sh" test -f tests test:tests \
    --test-show-details=direct \
    --test-option=--plain \
    --test-option=--test-seed=1 \
    --test-option=--select-tests='hyhac-tests/CBString API Tests*'
)
