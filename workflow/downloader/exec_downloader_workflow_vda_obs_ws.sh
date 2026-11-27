#!/bin/bash -e

# ----------------------------------------------------------------------------------------------------------------------------------
# shybox launcher.
set -euo pipefail

# script info
script_name='SHYBOX OBSERVATION DOWNLOADER'
script_version='1.0.0'
script_date='2026/11/26'
# ----------------------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------------------
# declaration(s)
script_folder='/home/idrologia/share/FloodProofs_v4/library/package_shybox/'
script_file='/home/idrologia/share/FloodProofs_v4/library/package_shybox/workflow/downloader/app_downloader_workflow_vda_obs_ws_main.py'
settings_file='/home/idrologia/share/FloodProofs_v4/fp_tools_downloading/ground_network/app_downloader_workflow_vda_obs_ws_server.json'
lock_folder='/home/idrologia/share/FloodProofs_v4/locks/downloader/ground_network/'
report_folder='/home/idrologia/share/FloodProofs_v4/reports/downloader/ground_network/'
virtual_env_python='/home/idrologia/share/FloodProofs_v4/library/conda_shybox_base/envs/shybox_base_libraries/bin/python'
run_variable='rain'

# check variables from system or script
settings_file="${SHYBOX_OBS_SETTINGS_FILE:-$settings_file}"
lock_folder="${SHYBOX_LOCK_DIR:-$lock_folder}"
report_folder="${SHYBOX_REPORT_DIR:-$report_folder}"
virtual_env_name="${SHYBOX_CONDA_ENV:-shybox_base_libraries}"
run_variable="${SHYBOX_RUN_VARIABLE:-$run_variable}"

# lock and report files
lock_file_template="${lock_folder%/}/lock_{run_name}_{run_variable}_{run_time}.lock"
report_file_template="${report_folder%/}/report_{run_name}_{run_variable}_{run_time}.env"
# ----------------------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------------------
# environmental variable(s)
export DOMAIN_NAME="${DOMAIN_NAME:-vda}"
export PATH_DATA="${PATH_DATA:-/home/idrologia/share/FloodProofs_v4/data/data_dynamic/source/ground_network/weather_stations/}"
export PATH_LOG="${PATH_LOG:-/home/idrologia/share/FloodProofs_v4/logs/downloader/ground_network/}"
export PYTHONPATH="$script_folder${PYTHONPATH:+:$PYTHONPATH}"
# ----------------------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------------------
# usage
usage() {
    cat <<'HELP'
Usage:
  exec_downloader_workflow_vda_obs_ws.sh [-f|--force] [--settings-file FILE] [realtime [TIME_PERIOD]]
  exec_downloader_workflow_vda_obs_ws.sh [-f|--force] [--settings-file FILE] history TIME_START TIME_END

Realtime uses the current Europe/Rome hour and a four-hour lookback by default.
History takes inclusive hourly bounds in the form 'YYYY-MM-DD HH:00'.
Use -f or --force to run despite a held lock and write a separate report.
SHYBOX_RUN_VARIABLE overrides the variable label used in lock and report names.
HELP
}

usage_error() {
    printf '%s\n' "$1" >&2
    usage >&2
    exit 2
}

force=false
arguments=()
while (($#)); do
    case "$1" in
        --settings-file)
            (($# >= 2)) || usage_error '--settings-file requires a path.'
            settings_file="$2"
            shift
            ;;
        -f|--force) force=true ;;
        -h|--help) usage; exit 0 ;;
        -*) usage_error "Unknown option $1." ;;
        *) arguments+=("$1") ;;
    esac
    shift
done
set -- "${arguments[@]}"
[[ "$settings_file" = /* ]] || settings_file="$PWD/$settings_file"
# ----------------------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------------------
# Convert local clock labels to seconds without applying a time-zone offset.
hour_seconds() {
    [[ "$1" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}\ [0-9]{2}:00$ ]] || return 1
    date -u -d "$1" +%s
}
# ----------------------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------------------
# run name
run_name="${1:-realtime}"
case "$run_name" in
    realtime)
        (($# <= 2)) || usage_error 'Realtime accepts one optional TIME_PERIOD.'
        time_end="$(TZ=Europe/Rome date '+%Y-%m-%d %H:00')"
        requested_period="${2:-${TIME_PERIOD:-4}}"
        [[ "$requested_period" =~ ^[0-9]{1,8}$ ]] || usage_error 'TIME_PERIOD must be a nonnegative integer.'
        time_period=$((10#$requested_period))
        end_seconds="$(hour_seconds "$time_end")"
        time_start="$(date -u -d "@$((end_seconds - time_period * 3600))" '+%Y-%m-%d %H:00')"
        ;;
    history)
        (($# == 3)) || usage_error 'History requires TIME_START and TIME_END.'
        time_start="$2"
        time_end="$3"
        start_seconds="$(hour_seconds "$time_start")" || usage_error 'Use hourly dates in the form YYYY-MM-DD HH:00.'
        end_seconds="$(hour_seconds "$time_end")" || usage_error 'Use hourly dates in the form YYYY-MM-DD HH:00.'
        ((start_seconds <= end_seconds)) || usage_error 'TIME_START must be earlier than or equal to TIME_END.'
        time_period=$(((end_seconds - start_seconds) / 3600))
        ;;
    *) usage_error "Unknown mode $run_name." ;;
esac

# run time
export TIME_RUN="$time_end" TIME_PERIOD="$time_period"
run_time="$(date -u -d "@$end_seconds" '+%Y%m%d_%H')"
[[ "$run_variable" =~ ^[a-zA-Z0-9_-]+$ ]] || usage_error 'Use only letters, numbers, underscores or hyphens in the run variable.'

render_run_path() {
    local path="$1"
    path="${path//\{run_name\}/$run_name}"
    path="${path//\{run_variable\}/$run_variable}"
    printf '%s\n' "${path//\{run_time\}/$run_time}"
}
# ----------------------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------------------
# set lock and report folder(s) and file(s)
mkdir -p -- "$lock_folder" "$report_folder"
lock_file="$(render_run_path "$lock_file_template")"
report_file="$(render_run_path "$report_file_template")"
exec 9>>"$lock_file"
flock -n -E 75 9 || {
    result=$?
    if ((result != 75)); then exit "$result"; fi
    if [[ "$force" == false ]]; then
        printf 'Run is locked: %s\n' "$lock_file" >&2
        exit "$result"
    fi
    exec 9>&-
    report_file="${report_file%.env}_forced_$$.env"
    printf 'Forcing run despite held lock %s\n' "$lock_file" >&2
}

started=$SECONDS
write_report() {
    local result=$? status=failed temporary=''
    trap - EXIT
    if ((result == 0)); then status=success; fi
    if temporary="$(mktemp -- "$report_file.XXXXXX")" &&
        printf 'STATUS=%q\nRUN_NAME=%q\nRUN_VARIABLE=%q\nTIME_RUN=%q\nTIME_START=%q\nTIME_END=%q\nTIME_PERIOD=%q\nEXECUTION_TIME=%q\nEXIT_CODE=%q\n' \
            "$status" "$run_name" "$run_variable" "$TIME_RUN" "$time_start" "$time_end" "$TIME_PERIOD" \
            "$((SECONDS - started))" "$result" > "$temporary" &&
        mv -f -- "$temporary" "$report_file"; then
        printf ' ==> %s. Report %s\n' "$status" "$report_file"
    else
        if [[ -n "$temporary" ]]; then rm -f -- "$temporary"; fi
        printf 'Could not write report %s\n' "$report_file" >&2
        if ((result == 0)); then result=1; fi
    fi
    exit "$result"
}
trap write_report EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
# ----------------------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------------------
# create python command
if [[ -n "${SHYBOX_PYTHON:-}" ]]; then
    python_command=("$SHYBOX_PYTHON")
elif [[ -n "${SHYBOX_CONDA_BIN:-}" ]]; then
    python_command=("$SHYBOX_CONDA_BIN" run --no-capture-output --name "$virtual_env_name" python)
else
    python_command=("$virtual_env_python")
fi
# Build the Python command
command_line=("${python_command[@]}" -B "$script_file" -settings_file "$settings_file" -time "$TIME_RUN")
# ----------------------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------------------
# info script start
printf ' ===================================================================================\n'
printf ' ==> %s (Version: %s Release_Date: %s)\n' "$script_name" "$script_version" "$script_date"
printf ' ==> START ...\n'
printf ' ==> %s from %s to %s (TIME_PERIOD=%s)\n' "$run_name" "$time_start" "$TIME_RUN" "$TIME_PERIOD"
printf ' ==> COMMAND LINE:'
printf ' %q' "${command_line[@]}"
printf '\n'

# run Python with the selected settings and time
"${command_line[@]}"

# info script end
printf ' ==> %s (Version: %s Release_Date: %s)\n' "$script_name" "$script_version" "$script_date"
printf ' ==> ... END\n'
printf ' ==> Bye, Bye\n'
printf ' ===================================================================================\n'
# ----------------------------------------------------------------------------------------------------------------------------------
