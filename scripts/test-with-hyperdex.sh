#!/usr/bin/env bash

set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
test_seed=${HYPERDEX_TEST_SEED:-1}

exec "${script_dir}/start-hyperdex.sh" \
  "${script_dir}/cabal.sh" \
  test \
  -f tests \
  test:tests \
  --test-show-details=direct \
  --test-option=--plain \
  --test-option=--test-seed="${test_seed}" \
  "$@"
