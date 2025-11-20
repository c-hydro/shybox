#!/usr/bin/env bash
set -Eeuo pipefail

# =======================================================================================
#  SHYBOX ENVIRONMENT SETUP SCRIPT
#
#  Purpose:
#    Install Miniconda (Python 3.12) and create/update a conda environment
#    using a YAML requirements file.
#
#  Usage:
#    bash shybox_conda_setup.sh [TAG] [ROOT_DIR] [ENV_FILE] [ENV_NAME] [YAML_FILE]
#
#  Examples:
#    # 1. Run with defaults (requirements file in same folder)
#    bash setup_shybox_base.sh
#
#    # 2. Custom tag (creates ./conda/<tag>_libraries)
#    bash setup_shybox_base.sh shybox_base
#
#    # 3. Fully custom
#    bash setup_shybox_base.sh shybox_base /opt/conda shybox_settings shybox_env requirements_shybox_base.yaml
#
#  Notes:
#    - The requirements YAML file must be in the same folder as this script by default.
#    - The script is idempotent: you can safely re-run it to update packages.
#    - An activation helper file is created at:
#          <ROOT_DIR>/<TAG>_settings
#      Use it with:
#          source <ROOT_DIR>/<TAG>_settings
# =======================================================================================

#-----------------------------------------------------------------------------------------
# Script information
script_name='SHYBOX ENVIRONMENT - PYTHON LIBRARIES FOR BASE LIBRARY - CONDA'
script_version="1.7.4"
script_date='2025/11/21'

# Official Miniconda (Python 3.12)
fp_env_file_miniconda='https://repo.anaconda.com/miniconda/Miniconda3-py312_25.7.0-2-Linux-x86_64.sh'

# Default arguments
fp_env_tag_default='shybox_base'
fp_env_folder_root_default='./conda/'
fp_env_file_reference_default='%ENV_TAG_settings'
fp_env_folder_libraries_default='%ENV_TAG_libraries'
fp_env_file_requirements_default='requirements_%ENV_TAG.yaml'
#-----------------------------------------------------------------------------------------

echo " ==================================================================================="
echo " ==> $script_name (Version: $script_version  Release_Date: $script_date)"
echo " ==> START ..."

# Parse args
if   [ $# -eq 0 ]; then
  fp_env_tag=$fp_env_tag_default
  fp_env_folder_root=$fp_env_folder_root_default
  fp_env_file_reference=$fp_env_file_reference_default
  fp_env_folder_libraries=$fp_env_folder_libraries_default
  fp_env_file_requirements=$fp_env_file_requirements_default
elif [ $# -eq 1 ]; then
  fp_env_tag=$1
  fp_env_folder_root=$fp_env_folder_root_default
  fp_env_file_reference=$fp_env_file_reference_default
  fp_env_folder_libraries=$fp_env_folder_libraries_default
  fp_env_file_requirements=$fp_env_file_requirements_default
elif [ $# -eq 2 ]; then
  fp_env_tag=$1; fp_env_folder_root=$2
  fp_env_file_reference=$fp_env_file_reference_default
  fp_env_folder_libraries=$fp_env_folder_libraries_default
  fp_env_file_requirements=$fp_env_file_requirements_default
elif [ $# -eq 3 ]; then
  fp_env_tag=$1; fp_env_folder_root=$2; fp_env_file_reference=$3
  fp_env_folder_libraries=$fp_env_folder_libraries_default
  fp_env_file_requirements=$fp_env_file_requirements_default
elif [ $# -eq 4 ]; then
  fp_env_tag=$1; fp_env_folder_root=$2; fp_env_file_reference=$3; fp_env_folder_libraries=$4
  fp_env_file_requirements=$fp_env_file_requirements_default
else
  fp_env_tag=$1; fp_env_folder_root=$2; fp_env_file_reference=$3; fp_env_folder_libraries=$4; fp_env_file_requirements=$5
fi

# Expand placeholders
fp_env_folder_root=${fp_env_folder_root/'%ENV_TAG'/$fp_env_tag}
fp_env_file_reference=${fp_env_file_reference/'%ENV_TAG'/$fp_env_tag}
fp_env_folder_libraries=${fp_env_folder_libraries/'%ENV_TAG'/$fp_env_tag}
fp_env_file_requirements=${fp_env_file_requirements/'%ENV_TAG'/$fp_env_tag}

echo ""
echo " ==> ARGS SELECTED:"
echo " ==> Tag .................. ${fp_env_tag}"
echo " ==> Conda prefix (root) .. ${fp_env_folder_root}"
echo " ==> Env ref file ......... ${fp_env_file_reference}"
echo " ==> Env name ............. ${fp_env_folder_libraries}"
echo " ==> Requirements YAML .... ${fp_env_file_requirements}"
echo ""

fp_env_path_reference="$fp_env_folder_root/$fp_env_file_reference"

# Helper: conda shell hook
_conda_hook() {
  eval "$("$fp_env_folder_root/bin/conda" shell.bash hook)"
}

# ----------------------------------------------------------------------------------------
# Install Miniconda if needed (do NOT pre-create the prefix)
echo " ====> CHECK MINICONDA PREFIX ..."
if [ -x "$fp_env_folder_root/bin/conda" ]; then
  echo " ====> FOUND Miniconda at $fp_env_folder_root"
else
  # If prefix directory exists, decide what to do
  if [ -d "$fp_env_folder_root" ]; then
    if [ -z "$(ls -A "$fp_env_folder_root")" ]; then
      echo " ====> Existing empty directory at $fp_env_folder_root — removing before install"
      rmdir "$fp_env_folder_root" || rm -rf "$fp_env_folder_root"
    else
      echo " !!!!! Directory $fp_env_folder_root already exists and is NOT a conda prefix."
      echo " !!!!! To avoid clobbering unrelated files, please choose a different ROOT_DIR,"
      echo " !!!!! or move/empty that directory, then re-run."
      exit 3
    fi
  fi

  echo " ====> Installing Miniconda to $fp_env_folder_root ..."
  tmp="${TMPDIR:-/tmp}"
  installer="$tmp/miniconda.sh"
  curl -fsSL "$fp_env_file_miniconda" -o "$installer"
  bash "$installer" -b -p "$fp_env_folder_root"
  echo " ====> Miniconda installed."
fi

# Accept Anaconda Terms of Service for non-interactive use
CONDA_BIN="$fp_env_folder_root/bin/conda"
echo " ====> Accepting Anaconda Terms of Service (non-interactive) ..."
"$CONDA_BIN" tos accept --override-channels --channel https://repo.anaconda.com/pkgs/main || true
"$CONDA_BIN" tos accept --override-channels --channel https://repo.anaconda.com/pkgs/r || true

# Load conda
_conda_hook

# Prefer conda-forge
conda config --add channels conda-forge >/dev/null 2>&1 || true
conda config --set channel_priority strict >/dev/null 2>&1 || true

# ----------------------------------------------------------------------------------------
# Create/update env
echo " ====> INSTALL/UPDATE PYTHON LIBRARIES FROM YAML ..."
if [ -f "$fp_env_file_requirements" ]; then
  echo " =====> Using YAML: $fp_env_file_requirements"
  if conda env list | awk '{print $1}' | grep -qx "$fp_env_folder_libraries"; then
    conda env update -n "$fp_env_folder_libraries" -f "$fp_env_file_requirements" --prune
  else
    conda env create -n "$fp_env_folder_libraries" -f "$fp_env_file_requirements"
  fi
else
  echo " !!!!! YAML not found at: $fp_env_file_requirements"
  echo " !!!!! Aborting."
  exit 2
fi
echo " ====> LIBRARIES DONE."

# ----------------------------------------------------------------------------------------
# Create activation helper (now that prefix surely exists)
echo " ====> CREATE ENVIRONMENTAL FILE ..."
(
  umask 022
  cat > "$fp_env_path_reference" <<EOF
# Source this file to activate the environment:
#   source "$fp_env_path_reference"
if ! command -v conda >/dev/null 2>&1; then
  if [ -f "$fp_env_folder_root/etc/profile.d/conda.sh" ]; then
    . "$fp_env_folder_root/etc/profile.d/conda.sh"
  else
    eval "\$("$fp_env_folder_root/bin/conda" shell.bash hook)"
  fi
fi
conda activate "$fp_env_folder_libraries"
EOF
)
echo " ====> ENV FILE CREATED: $fp_env_path_reference"

# ----------------------------------------------------------------------------------------
echo " ==> $script_name (Version: $script_version  Release_Date: $script_date)"
echo " ==> ... END"
echo " ==> Bye, Bye"
echo " ==================================================================================="

