#!/usr/bin/env bash

set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd "${script_dir}/.." && pwd)

export PATH="${HOME}/.local/bin:${PATH}"

cd "${repo_root}"
scripts/install-prek.sh
prek run --all-files -c .pre-commit-config.yaml
