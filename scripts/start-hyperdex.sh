#!/usr/bin/env bash

set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd "${script_dir}/.." && pwd)
hyperdex_root=${HYPERDEX_ROOT:-"${repo_root}/../HyperDex"}
coord_host=${HYPERDEX_COORD_HOST:-127.0.0.1}
daemon_host=${HYPERDEX_DAEMON_HOST:-127.0.0.1}
cluster_dir=${HYPERDEX_CLUSTER_DIR:-}

if [ ! -x "${hyperdex_root}/hyperdex" ]; then
  echo "missing HyperDex launcher: ${hyperdex_root}/hyperdex" >&2
  echo "set HYPERDEX_ROOT to a built HyperDex checkout" >&2
  exit 1
fi

if [ ! -x "${hyperdex_root}/hyperdex-show-config" ]; then
  echo "missing HyperDex tool: ${hyperdex_root}/hyperdex-show-config" >&2
  exit 1
fi

if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 is required to choose free ports for the test cluster" >&2
  exit 1
fi

created_cluster_dir=0

if [ -z "${cluster_dir}" ]; then
  cluster_dir=$(mktemp -d "${TMPDIR:-/tmp}/hyhac-hyperdex.XXXXXX")
  created_cluster_dir=1
else
  rm -rf "${cluster_dir}"
  mkdir -p "${cluster_dir}"
fi

choose_port() {
  python3 - <<'PY'
import socket

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.bind(("127.0.0.1", 0))
print(sock.getsockname()[1])
sock.close()
PY
}

coord_port=${HYPERDEX_COORD_PORT:-$(choose_port)}
daemon_port=${HYPERDEX_DAEMON_PORT:-$(choose_port)}

while [ "${daemon_port}" = "${coord_port}" ]; do
  daemon_port=$(choose_port)
done

export HYPERDEX_EXEC_PATH="${hyperdex_root}"
export HYPERDEX_COORD_LIB="${hyperdex_root}/.libs/libhyperdex-coordinator"
export LD_LIBRARY_PATH="${hyperdex_root}/.libs${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}"
export PATH="${hyperdex_root}:${hyperdex_root}/bin${PATH:+:${PATH}}"
export HYPERDEX_COORD_HOST="${coord_host}"
export HYPERDEX_COORD_PORT="${coord_port}"
export HYPERDEX_DAEMON_HOST="${daemon_host}"
export HYPERDEX_DAEMON_PORT="${daemon_port}"

show_text_log() {
  local label=$1
  local path=$2

  if [ ! -f "${path}" ]; then
    return
  fi

  echo "${label}:" >&2
  sed -n '1,200p' "${path}" >&2
}

stop_process() {
  local pid=$1
  local name=$2

  if [ -z "${pid}" ]; then
    return
  fi

  if ! kill -0 "${pid}" 2>/dev/null; then
    return
  fi

  kill "${pid}" 2>/dev/null || true

  for _ in $(seq 1 50); do
    if ! kill -0 "${pid}" 2>/dev/null; then
      wait "${pid}" 2>/dev/null || true
      return
    fi
    sleep 0.1
  done

  echo "${name} did not exit after SIGTERM; sending SIGKILL" >&2
  kill -9 "${pid}" 2>/dev/null || true
  wait "${pid}" 2>/dev/null || true
}

cleanup() {
  local status=$?
  set +e

  stop_process "${daemon_pid:-}" "daemon"
  stop_process "${coord_pid:-}" "coordinator"

  if [ "${status}" -ne 0 ]; then
    show_text_log "coordinator log" "${cluster_dir}/coordinator.log"
    show_text_log "daemon log" "${cluster_dir}/daemon.log"
    show_text_log "show-config output" "${cluster_dir}/show-config.out"
    echo "cluster state left at: ${cluster_dir}" >&2
    exit "${status}"
  fi

  if [ "${created_cluster_dir}" -eq 1 ]; then
    rm -rf "${cluster_dir}"
  fi
}

trap cleanup EXIT INT TERM

mkdir -p "${cluster_dir}/coordinator" "${cluster_dir}/daemon1"

"${hyperdex_root}/hyperdex" coordinator \
  --foreground \
  --data="${cluster_dir}/coordinator" \
  --listen "${coord_host}" \
  --listen-port "${coord_port}" \
  >"${cluster_dir}/coordinator.log" 2>&1 &
coord_pid=$!

coord_ready=0

for _ in $(seq 1 100); do
  if ! kill -0 "${coord_pid}" 2>/dev/null; then
    echo "coordinator exited during startup" >&2
    exit 1
  fi

  if timeout 2 "${hyperdex_root}/hyperdex-show-config" -h "${coord_host}" -p "${coord_port}" \
      >"${cluster_dir}/show-config.out" 2>&1 &&
      grep -q '^version 1$' "${cluster_dir}/show-config.out"; then
    coord_ready=1
    break
  fi

  sleep 0.1
done

if [ "${coord_ready}" -ne 1 ]; then
  echo "coordinator did not become ready" >&2
  exit 1
fi

"${hyperdex_root}/hyperdex" daemon \
  --foreground \
  --threads 1 \
  --data="${cluster_dir}/daemon1" \
  --listen "${daemon_host}" \
  --listen-port "${daemon_port}" \
  --coordinator "${coord_host}" \
  --coordinator-port "${coord_port}" \
  >"${cluster_dir}/daemon.log" 2>&1 &
daemon_pid=$!

ready=0

for _ in $(seq 1 150); do
  if ! kill -0 "${coord_pid}" 2>/dev/null; then
    echo "coordinator exited during startup" >&2
    exit 1
  fi

  if ! kill -0 "${daemon_pid}" 2>/dev/null; then
    echo "daemon exited during startup" >&2
    exit 1
  fi

  if timeout 2 "${hyperdex_root}/hyperdex-show-config" -h "${coord_host}" -p "${coord_port}" \
      >"${cluster_dir}/show-config.out" 2>&1 &&
      grep -q "server .* ${daemon_host}:${daemon_port} AVAILABLE" "${cluster_dir}/show-config.out"; then
    ready=1
    break
  fi

  sleep 0.2
done

if [ "${ready}" -ne 1 ]; then
  echo "HyperDex cluster did not become ready" >&2
  exit 1
fi

if [ "$#" -gt 0 ]; then
  "$@"
  exit $?
fi

echo "HyperDex cluster is ready in ${cluster_dir}" >&2
echo "coordinator: ${coord_host}:${coord_port}" >&2
echo "daemon: ${daemon_host}:${daemon_port}" >&2

wait "${daemon_pid}"
