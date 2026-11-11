#!/usr/bin/env bash
set -euo pipefail

# ---- user settings ----
VENV_DIR="${VENV_DIR:-./env_copy_state_hmc}"
PYTHON_BIN="${PYTHON_BIN:-python3}"

# ---- checks ----
command -v "${PYTHON_BIN}" >/dev/null 2>&1 || {
  echo "[ERROR] ${PYTHON_BIN} not found in PATH" >&2
  exit 1
}

echo "[INFO] Creating venv in: ${VENV_DIR}"
"${PYTHON_BIN}" -m venv "${VENV_DIR}"

# shellcheck disable=SC1091
source "${VENV_DIR}/bin/activate"

echo "[INFO] Upgrading pip/setuptools/wheel"
python -m pip install --upgrade pip setuptools wheel

# This project uses only the standard library right now.
# If you later add dependencies, list them here (or in requirements.txt).
if [[ -f requirements.txt ]]; then
  echo "[INFO] Installing requirements.txt"
  pip install -r requirements.txt
else
  echo "[INFO] No requirements.txt found; no extra packages installed."
fi

echo "[OK] Environment ready."
echo "[INFO] Activate with: source ${VENV_DIR}/bin/activate"

