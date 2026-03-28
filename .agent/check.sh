#!/usr/bin/env bash

set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd "${script_dir}/.." && pwd)
hyperdex_root=$("${script_dir}/hyperdex-root.sh")

export HYPERDEX_ROOT="${hyperdex_root}"

(
  cd "${repo_root}"
  "${repo_root}/scripts/cabal.sh" build -f tests lib:hyhac test:tests
  "${repo_root}/scripts/test-with-hyperdex.sh"
)
