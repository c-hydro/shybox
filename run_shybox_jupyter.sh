#!/usr/bin/env bash
# Open the repository labs without modifying the scientific environment.
set -euo pipefail
task_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
task_venv="$task_root/labs/.venv"
while [[ $# -gt 0 ]]; do
    case "$1" in
        --venv)
            [[ $# -ge 2 && -n "$2" ]] || { echo "Missing value for --venv" >&2; exit 2; }
            task_venv="$2"; shift 2 ;;
        -h|--help)
            echo "Usage: $0 [--venv PATH] [-- JupyterLab options]"
            echo "Example: $0 -- --no-browser --port=8889"
            exit 0 ;;
        --) shift; break ;;
        *) break ;;
    esac
done
if [[ ! -x "$task_venv/bin/python" ]]; then
    echo "Jupyter environment missing: $task_venv. Run setup_shybox_jupyter.sh first." >&2
    exit 1
fi
task_venv="$(cd -- "$task_venv" && pwd)"
export PYTHONPATH="$task_root/labs/hmc:$task_root/labs/hmc/library/common:$task_root/labs/hmc/library/jupyter:$task_root${PYTHONPATH:+:$PYTHONPATH}"
cd -- "$task_root/labs/hmc"
exec "$task_venv/bin/python" -m jupyterlab --ServerApp.root_dir="$task_root/labs/hmc" "$@"
