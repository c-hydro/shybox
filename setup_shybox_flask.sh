#!/usr/bin/env bash
# Create an independent dashboard environment; leave SHYBOX conda environments intact.
set -euo pipefail
task_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
task_venv="$task_root/dashboard/.venv"
if [[ -x "$task_root/conda/envs/shybox_base_libraries/bin/python" ]]; then
    task_python="$task_root/conda/envs/shybox_base_libraries/bin/python"
else
    task_python="python3"
fi
while [[ $# -gt 0 ]]; do
    case "$1" in
        --python|--venv)
            if [[ $# -lt 2 || -z "$2" ]]; then
                echo "Missing value for $1" >&2; exit 2
            fi
            if [[ "$1" == "--python" ]]; then task_python="$2"; else task_venv="$2"; fi
            shift 2 ;;
        -h|--help)
            echo "Usage: ./setup_shybox_flask.sh [--python /path/to/python3.12] [--venv /path/to/environment]"
            echo "Default environment: dashboard/.venv; requires Python >= 3.10."
            exit 0 ;;
        *) echo "Unknown option: $1" >&2; exit 2 ;;
    esac
done
"$task_python" -c 'import sys; sys.exit("Python >= 3.10 is required; use --python.") if sys.version_info < (3, 10) else None'
if [[ ! -x "$task_venv/bin/python" ]]; then
    "$task_python" -m venv "$task_venv"
fi
"$task_venv/bin/python" -m pip install --disable-pip-version-check -r "$task_root/dashboard/requirements.txt"
"$task_venv/bin/python" -c 'import flask, numpy, pandas, xarray, netCDF4; print("Dashboard dependencies are ready.")'
printf '\nRun from the repository root:\n  cd %q\n  %q -m dashboard.app --config dashboard/config.json\n\nOpen http://127.0.0.1:5050\n' "$task_root" "$task_venv/bin/python"
