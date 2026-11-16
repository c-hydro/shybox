#!/usr/bin/env bash
# Run the isolated variant without activating or modifying an environment.
set -euo pipefail
parallel_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../../.." && pwd)"
parallel_python="${SHYBOX_PYTHON:-$parallel_root/conda/envs/shybox_geo_libraries/bin/python}"
if [[ ! -x "$parallel_python" ]]; then parallel_python=python3; fi
export PYTHONDONTWRITEBYTECODE=1
cd -- "$parallel_root"
exec "$parallel_python" -B -m workflow.runner.datasets_parallel.app_runner_workflow_hmc_datasets_parallel "$@"
