#!/usr/bin/env bash

set -euo pipefail

repo_root=$(git rev-parse --show-toplevel 2>/dev/null || pwd)
actionlint_version=${ACTIONLINT_VERSION:-1.7.11}
actionlint_sha256_linux_amd64=${ACTIONLINT_SHA256_LINUX_AMD64:-900919a84f2229bac68ca9cd4103ea297abc35e9689ebb842c6e34a3d1b01b0a}
actionlint_bin=${ACTIONLINT_BIN:-}
cache_root=${XDG_CACHE_HOME:-"${HOME}/.cache"}/hyhac/tools
actionlint_root="${cache_root}/actionlint/${actionlint_version}"

cd "${repo_root}"

if [ -n "${actionlint_bin}" ]; then
  printf '%s\n' "${actionlint_bin}"
  exit 0
fi

if command -v actionlint >/dev/null 2>&1; then
  command -v actionlint
  exit 0
fi

mkdir -p "${actionlint_root}"

if [ ! -x "${actionlint_root}/actionlint" ]; then
  archive=$(mktemp -t actionlint.XXXXXX.tar.gz)
  trap 'rm -f "${archive}"' EXIT
  curl -L --fail --silent --show-error \
    "https://github.com/rhysd/actionlint/releases/download/v${actionlint_version}/actionlint_${actionlint_version}_linux_amd64.tar.gz" \
    -o "${archive}"
  printf '%s  %s\n' "${actionlint_sha256_linux_amd64}" "${archive}" | sha256sum --check --status
  tar -xzf "${archive}" -C "${actionlint_root}" actionlint
fi

printf '%s\n' "${actionlint_root}/actionlint"
