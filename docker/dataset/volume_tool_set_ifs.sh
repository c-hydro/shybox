#!/bin/bash
set -e

echo "============================================"
echo " Managing Docker Volumes for DESTINE - CONVERTER IFS 2 HMC"
echo "============================================"

BASE_DSET="/home/fabio/Desktop/shybox/dset/case_study_destine/converter_hmc"
BASE_HPC="/home/fabio/Desktop/shybox/hpc/destine"

# Define volumes
declare -A VOLUMES=(
  ["case_study_destine_ifs_geo"]="$BASE_DSET/geo"
  ["case_study_destine_ifs_in"]="$BASE_DSET/data/ifs"
  ["case_study_destine_ifs_out"]="$BASE_HPC/data/converter_hmc/ifs"
  ["case_study_destine_ifs_log"]="$BASE_HPC/log/converter_hmc/ifs"
  ["case_study_destine_ifs_tmp"]="$BASE_HPC/tmp/converter_hmc/ifs"
)

# Step 1: Create folders with permissions
echo "[1/3] Preparing folders..."
for VOL in "${!VOLUMES[@]}"; do
  DIR="${VOLUMES[$VOL]}"
  echo " - $DIR"
  mkdir -p "$DIR"
  chmod 777 "$DIR"
done

# Step 2: Remove existing volumes
echo "[2/3] Removing old volumes..."
for VOL in "${!VOLUMES[@]}"; do
  docker volume rm "$VOL" 2>/dev/null || true
done

# Step 3: Create volumes
echo "[3/3] Creating new bind volumes..."
for VOL in "${!VOLUMES[@]}"; do
  DIR="${VOLUMES[$VOL]}"
  echo " + Creating $VOL -> $DIR"

  docker volume create \
    --name "$VOL" \
    --opt type=none \
    --opt device="$DIR" \
    --opt o=bind

  docker volume inspect "$VOL" --format "   Mounted at: {{.Options.device}}"
done

echo "============================================"
echo " Done. All volumes are ready."
echo "============================================"

