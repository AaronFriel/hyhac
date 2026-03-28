#!/usr/bin/env bash

set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd "${script_dir}/.." && pwd)
hyperdex_root=${HYPERDEX_ROOT:-"${repo_root}/../HyperDex"}
ghcup_env=${GHCUP_ENV:-"${HOME}/.ghcup/env"}
toolchain_lib_dir="${repo_root}/.toolchain/lib"

hyperdex_root=$(cd "${hyperdex_root}" && pwd)

if [ ! -d "${hyperdex_root}/.libs" ]; then
  echo "missing HyperDex build output: ${hyperdex_root}/.libs" >&2
  echo "set HYPERDEX_ROOT to a built HyperDex checkout" >&2
  exit 1
fi

if [ -f "${ghcup_env}" ]; then
  # Prefer the ghcup environment when it exists, but permit CI to provide
  # cabal and ghc directly on PATH.
  . "${ghcup_env}"
elif ! command -v cabal >/dev/null 2>&1; then
  echo "missing ghcup environment file: ${ghcup_env}" >&2
  echo "install GHCup or place cabal on PATH, then re-run this script" >&2
  exit 1
fi

mkdir -p "${toolchain_lib_dir}"

if [ ! -e "${toolchain_lib_dir}/libgmp.so" ]; then
  gmp_path=$(
    ldconfig -p 2>/dev/null | awk '
      $1 == "libgmp.so" { print $NF; found=1; exit }
      $1 == "libgmp.so.10" && fallback == "" { fallback=$NF }
      END {
        if (!found && fallback != "") {
          print fallback
        }
      }'
  )

  if [ -z "${gmp_path}" ]; then
    echo "could not locate libgmp.so or libgmp.so.10 via ldconfig" >&2
    exit 1
  fi

  ln -sf "${gmp_path}" "${toolchain_lib_dir}/libgmp.so"
fi

export HYPERDEX_EXEC_PATH="${hyperdex_root}"
export HYPERDEX_COORD_LIB="${hyperdex_root}/.libs/libhyperdex-coordinator"
export LIBRARY_PATH="${toolchain_lib_dir}${LIBRARY_PATH:+:${LIBRARY_PATH}}"
export LD_LIBRARY_PATH="${toolchain_lib_dir}:${hyperdex_root}/.libs${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}"
export PATH="${hyperdex_root}:${PATH}"

if [ "$#" -eq 0 ]; then
  echo "usage: scripts/cabal.sh <cabal-args...>" >&2
  exit 1
fi

command=$1
shift

case "${command}" in
  build|test|bench|run|repl|haddock)
    exec cabal "${command}" "$@" --extra-lib-dirs="${hyperdex_root}/.libs"
    ;;
  *)
    exec cabal "${command}" "$@"
    ;;
esac
