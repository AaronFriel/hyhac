#!/usr/bin/env bash

set -euo pipefail

repo_root=$(git rev-parse --show-toplevel 2>/dev/null || pwd)
prek_version=${PREK_VERSION:-0.3.8}
prek_sha256_x86_64_unknown_linux_gnu=${PREK_SHA256_X86_64_UNKNOWN_LINUX_GNU:-80ec6adb9f1883344de52cb943d371ecfd25340c4a6b5b81e2600d27e246cfa1}
local_bin=${LOCAL_BIN:-"${HOME}/.local/bin"}

cd "${repo_root}"

if command -v prek >/dev/null 2>&1 && prek --version 2>/dev/null | grep -q "${prek_version}"; then
  exit 0
fi

mkdir -p "${local_bin}"
archive=$(mktemp -t prek.XXXXXX.tar.gz)
trap 'rm -f "${archive}"' EXIT

curl -L --fail --silent --show-error \
  "https://github.com/j178/prek/releases/download/v${prek_version}/prek-x86_64-unknown-linux-gnu.tar.gz" \
  -o "${archive}"
printf '%s  %s\n' "${prek_sha256_x86_64_unknown_linux_gnu}" "${archive}" | sha256sum --check --status
tar -xzf "${archive}" -C "${local_bin}" --strip-components=1 \
  prek-x86_64-unknown-linux-gnu/prek
chmod +x "${local_bin}/prek"
