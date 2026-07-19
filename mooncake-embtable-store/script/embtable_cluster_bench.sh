#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd -- "${SCRIPT_DIR}/../.." && pwd)"

if [[ -n "${EMBTABLE_CLUSTER_BENCH_BIN:-}" ]]; then
  BIN="${EMBTABLE_CLUSTER_BENCH_BIN}"
else
  BIN="${ROOT_DIR}/build/mooncake-embtable-store/src/benchmarks/embtable_cluster_bench"
fi

if [[ ! -x "${BIN}" ]]; then
  echo "EmbTable benchmark executable not found: ${BIN}" >&2
  echo "Set EMBTABLE_CLUSTER_BENCH_BIN or build the project first." >&2
  exit 1
fi

exec "${BIN}" \
  --logtostderr=1 \
  --stderrthreshold=0 \
  --minloglevel=0 \
  --colorlogtostderr=1 \
  "$@"
