#!/usr/bin/env bash

set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd "${script_dir}/.." && pwd)
requested_root=${HYPERDEX_ROOT:-"${repo_root}/../HyperDex"}

is_hyperdex_build_root() {
  local candidate=$1

  [ -x "${candidate}/hyperdex" ] &&
    [ -x "${candidate}/hyperdex-show-config" ] &&
    [ -d "${candidate}/.libs" ]
}

resolve_hyperdex_root() {
  local candidate=$1

  if is_hyperdex_build_root "${candidate}"; then
    printf '%s\n' "${candidate}"
    return 0
  fi

  if is_hyperdex_build_root "${candidate}/target"; then
    printf '%s\n' "${candidate}/target"
    return 0
  fi

  return 1
}

if resolved_root=$(resolve_hyperdex_root "${requested_root}"); then
  printf '%s\n' "${resolved_root}"
  exit 0
fi

echo "could not find a HyperDex build root under: ${requested_root}" >&2
echo "expected one of:" >&2
echo "  ${requested_root}" >&2
echo "  ${requested_root}/target" >&2
echo "Build HyperDex first. If you are using the sibling checkout's .agent setup," >&2
echo "that produces the build root at:" >&2
echo "  ${requested_root}/target" >&2
exit 1
