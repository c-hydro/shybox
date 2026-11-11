#!/usr/bin/env bash
set -euo pipefail

# ---- paths ----
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="${VENV_DIR:-${SCRIPT_DIR}/env_copy_state_hmc}"

PY_SCRIPT="${PY_SCRIPT:-${SCRIPT_DIR}/copy_data_hmc.py}"
CONFIG_JSON="${CONFIG_JSON:-${SCRIPT_DIR}/copy_data_hmc.json}"

# ---- options ----
DRY_RUN="${DRY_RUN:-0}"         # 1 = dry-run
RECURSIVE="${RECURSIVE:-0}"     # 1 = include subfolders

# ---- checks ----
[[ -f "${PY_SCRIPT}" ]] || { echo "[ERROR] Python script not found: ${PY_SCRIPT}" >&2; exit 1; }
[[ -f "${CONFIG_JSON}" ]] || { echo "[ERROR] Config JSON not found: ${CONFIG_JSON}" >&2; exit 1; }
[[ -d "${VENV_DIR}" ]] || { echo "[ERROR] Venv not found: ${VENV_DIR}. Run setup_copy_restart_env.sh first." >&2; exit 1; }

# shellcheck disable=SC1091
source "${VENV_DIR}/bin/activate"

args=( "--config" "${CONFIG_JSON}" )

if [[ "${RECURSIVE}" == "1" ]]; then
  args+=( "--recursive" )
fi
if [[ "${DRY_RUN}" == "1" ]]; then
  args+=( "--dry-run" )
fi

echo "[INFO] Using venv: ${VENV_DIR}"
echo "[INFO] Running: python ${PY_SCRIPT} ${args[*]}"
python "${PY_SCRIPT}" "${args[@]}"

