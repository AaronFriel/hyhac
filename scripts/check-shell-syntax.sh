#!/usr/bin/env bash

set -euo pipefail

repo_root=$(git rev-parse --show-toplevel 2>/dev/null || pwd)
shell_files=()
search_roots=(.agent scripts)

cd "${repo_root}"

for search_root in "${search_roots[@]}"; do
  if [ ! -d "${search_root}" ]; then
    continue
  fi

  while IFS= read -r path; do
    shell_files+=("${path}")
  done < <(find "${search_root}" -type f | sort)
done

shell_targets=()

for path in "${shell_files[@]}"; do
  first_line=$(head -n 1 "${path}" 2>/dev/null || true)
  case "${path}:${first_line}" in
    *.sh:*|*:'#!/bin/sh'*|*:'#!/usr/bin/env sh'*|*:'#!/bin/bash'*|*:'#!/usr/bin/env bash'*)
      shell_targets+=("${path}")
      ;;
  esac
done

if [ "${#shell_targets[@]}" -eq 0 ]; then
  echo "No shell files found."
  exit 0
fi

bash -n "${shell_targets[@]}"
