#!/usr/bin/env bash
# Create an isolated environment for the archived HMC labs.
set -euo pipefail
task_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
task_venv="$task_root/labs/.venv"
task_python=python3
task_user_kernel=false
while [[ $# -gt 0 ]]; do
    case "$1" in
        --python|--venv)
            [[ $# -ge 2 && -n "$2" ]] || { echo "Missing value for $1" >&2; exit 2; }
            if [[ "$1" == --python ]]; then task_python="$2"; else task_venv="$2"; fi
            shift 2 ;;
        --register-user-kernel) task_user_kernel=true; shift ;;
        -h|--help)
            echo "Usage: $0 [--python python3.11] [--venv PATH] [--register-user-kernel]"
            echo "Create labs/.venv with JupyterLab and legacy lab dependencies (Python 3.10–3.11)."
            echo "Optional: register SHYBOX HMC Labs in an existing Jupyter installation."
            exit 0 ;;
        *) echo "Unknown option: $1" >&2; exit 2 ;;
    esac
done
"$task_python" -c 'import sys; sys.exit("Legacy labs require Python 3.10–3.11; select it with --python.") if not (3, 10) <= sys.version_info[:2] <= (3, 11) else None'
if [[ ! -x "$task_venv/bin/python" ]]; then
    "$task_python" -m venv "$task_venv"
fi
task_venv="$(cd -- "$task_venv" && pwd)"
"$task_venv/bin/python" -c 'import sys; assert (3, 10) <= sys.version_info[:2] <= (3, 11), "Existing environment requires Python 3.10–3.11"'
"$task_venv/bin/python" -m pip install --disable-pip-version-check -r "$task_root/labs/requirements.txt"
"$task_venv/bin/python" -m ipykernel install --sys-prefix --name shybox-hmc-labs --display-name "SHYBOX HMC Labs"
if "$task_user_kernel"; then
    "$task_venv/bin/python" -m ipykernel install --user --name shybox-hmc-labs --display-name "SHYBOX HMC Labs"
fi
"$task_venv/bin/python" -c 'import jupyterlab, ipympl, cartopy, geopandas, rasterio, xarray, netCDF4; print("JupyterLab dependencies are ready.")'
printf '\nStart the labs:\n  %q --venv %q\n' "$task_root/run_shybox_jupyter.sh" "$task_venv"
