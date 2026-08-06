#!/usr/bin/env bash
set -Eeuo pipefail

# ==============================================================================
# SHYBOX COMPARE TS - MINICONDA INSTALLER + ENV CREATOR
#
# Usage:
#
#   chmod +x env_compare_ts.sh
#
#   # Install Miniconda in the same folder as this script
#   ./env_compare_ts.sh environmental.yaml
#
#   # Install Miniconda in a custom folder
#   ./env_compare_ts.sh environmental.yaml --folder /home/admin/shybox_env
#
# Result:
#
#   <folder>/miniconda3
#
# ==============================================================================

# ------------------------------------------------------------------------------
# Defaults
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

ENV_FILE="${1:-environment.yml}"
ENV_NAME="shybox_compare_ts"

INSTALL_ROOT="$SCRIPT_DIR"

# Remove first positional argument if present
if [[ $# -gt 0 ]]; then
    shift
fi

# ------------------------------------------------------------------------------
# Parse optional arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --folder|-folder)
            INSTALL_ROOT="${2:-}"
            shift 2
            ;;
        -h|--help)
            echo "Usage:"
            echo "  $0 environmental.yaml"
            echo "  $0 environmental.yaml --folder /path/to/install_root"
            exit 0
            ;;
        *)
            echo "[ERROR] Unknown option: $1"
            exit 1
            ;;
    esac
done

if [[ -z "$INSTALL_ROOT" ]]; then
    echo "[ERROR] Install folder is empty."
    exit 1
fi

CONDA_INSTALL_DIR="$INSTALL_ROOT/miniconda3"

MINICONDA_URL="https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh"
MINICONDA_INSTALLER="/tmp/Miniconda3-latest-Linux-x86_64.sh"

# ------------------------------------------------------------------------------
# Info
echo "========================================"
echo "Shybox compare TS environment installer"
echo "YAML file        : $ENV_FILE"
echo "Env name         : $ENV_NAME"
echo "Script folder    : $SCRIPT_DIR"
echo "Install folder   : $INSTALL_ROOT"
echo "Conda install dir: $CONDA_INSTALL_DIR"
echo "========================================"
echo

# ------------------------------------------------------------------------------
# Check YAML
if [[ ! -f "$ENV_FILE" ]]; then
    echo "[ERROR] YAML file not found: $ENV_FILE"
    exit 1
fi

# ------------------------------------------------------------------------------
# Prepare install folder
mkdir -p "$INSTALL_ROOT"

# ------------------------------------------------------------------------------
# Install Miniconda if missing
if [[ ! -f "$CONDA_INSTALL_DIR/etc/profile.d/conda.sh" ]]; then
    echo "[INFO] Miniconda not found in:"
    echo "       $CONDA_INSTALL_DIR"
    echo "[INFO] Installing Miniconda ..."

    if command -v curl >/dev/null 2>&1; then
        curl -L "$MINICONDA_URL" -o "$MINICONDA_INSTALLER"
    elif command -v wget >/dev/null 2>&1; then
        wget -O "$MINICONDA_INSTALLER" "$MINICONDA_URL"
    else
        echo "[ERROR] Neither curl nor wget is available."
        exit 1
    fi

    bash "$MINICONDA_INSTALLER" -b -p "$CONDA_INSTALL_DIR"
    rm -f "$MINICONDA_INSTALLER"

    echo "[INFO] Miniconda installed."
else
    echo "[INFO] Miniconda already available:"
    echo "       $CONDA_INSTALL_DIR"
fi

# ------------------------------------------------------------------------------
# Activate conda
# shellcheck disable=SC1090
source "$CONDA_INSTALL_DIR/etc/profile.d/conda.sh"

conda config --set channel_priority strict

echo
echo "[INFO] Conda version:"
conda --version

# ------------------------------------------------------------------------------
# Create or update environment
if conda env list | awk '{print $1}' | grep -qx "$ENV_NAME"; then
    echo
    echo "[INFO] Environment already exists: $ENV_NAME"
    echo "[INFO] Updating environment ..."
    conda env update -n "$ENV_NAME" -f "$ENV_FILE" --prune
else
    echo
    echo "[INFO] Creating environment from YAML ..."
    conda env create -f "$ENV_FILE"
fi

# ------------------------------------------------------------------------------
# Activate environment
conda activate "$ENV_NAME"

# ------------------------------------------------------------------------------
# Verify libraries
echo
echo "========================================"
echo "Verifying Python libraries ..."
echo "========================================"

python - <<'PY'
import sys

modules = [
    "numpy",
    "pandas",
    "xarray",
    "matplotlib",
    "netCDF4",
    "h5netcdf",
    "scipy",
    "pyproj",
    "rasterio",
]

failed = []

for module in modules:
    try:
        __import__(module)
        print(f"[OK] {module}")
    except Exception as exc:
        print(f"[FAILED] {module}: {exc}")
        failed.append(module)

print()
print(f"Python executable: {sys.executable}")
print(f"Python version   : {sys.version}")

if failed:
    raise SystemExit(f"Missing or broken modules: {failed}")

print()
print("Environment verification completed successfully.")
PY

# ------------------------------------------------------------------------------
# Final instructions
echo
echo "========================================"
echo "Installation completed"
echo "========================================"
echo
echo "To activate Conda in a new shell:"
echo "  source $CONDA_INSTALL_DIR/etc/profile.d/conda.sh"
echo
echo "To activate the environment:"
echo "  conda activate $ENV_NAME"
echo
echo "To run shybox compare TS:"
echo "  python shybox_compare_ts.py \\"
echo "    -settings_file configuration.json \\"
echo "    -time \"2026-03-31 08:00\""
echo