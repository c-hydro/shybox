#!/usr/bin/env bash
# Run the Marche deterministic/probabilistic forcing profiles with local datasets.
set -euo pipefail

compose_mode="${1:-deterministic}"
if [[ "$compose_mode" != deterministic && "$compose_mode" != probabilistic ]]; then
    echo "Usage: bash $0 [deterministic|probabilistic] [--dry-run] [-time 'YYYY-MM-DD HH:MM']" >&2
    exit 2
fi
if [[ $# -gt 0 ]]; then shift; fi

compose_script_folder="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
compose_repository="$(cd -- "$compose_script_folder/../../.." && pwd)"
compose_data_root="${SHYBOX_MARCHE_DATA:-/home/fabio/Desktop/shybox/dset/case_study_marche/runner_hmc_datasets/data}"

export PYTHONPATH="$compose_repository${PYTHONPATH:+:$PYTHONPATH}"
export DOMAIN_NAME="${DOMAIN_NAME:-marche}"
export PATH_GEO="${PATH_GEO:-/home/fabio/Desktop/shybox/dset/case_study_marche/rfarm/geo}"
export PATH_OBS="${PATH_OBS:-$compose_data_root/obs}"
export PATH_FRC_NWP="${PATH_FRC_NWP:-${PATH_FORECAST:-$compose_data_root/nwp}}"
export PATH_FRC_RFARM="${PATH_FRC_RFARM:-${PATH_RFARM:-$compose_data_root/rfarm}}"
export PATH_DST="${PATH_DST:-$compose_repository/exec/case_study_marche/runner_hmc_datasets/forcing}"
export PATH_LOG="${PATH_LOG:-$compose_repository/exec/case_study_marche/runner_hmc_datasets/log}"
export PATH_TMP="${PATH_TMP:-$compose_repository/tmp/case_study_marche/runner_hmc_datasets}"

exec "${SHYBOX_PYTHON:-python}" -B -m workflow.runner.datasets.app_runner_workflow_hmc_datasets_main \
    -settings_file "$compose_script_folder/app_runner_workflow_hmc_datasets_forcing_$compose_mode.json" "$@"
