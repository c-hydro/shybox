#!/bin/bash -e

# SHYBOX observation downloader launcher.
set -euo pipefail

script_name='SHYBOX OBSERVATION DOWNLOADER'
script_version='1.0.0'
script_date='2026/11/26'

# Paths for this checkout.
script_folder='/home/fabio/Documents/Work_Area/Code_Development/Workspace/shybox'
script_file='/home/fabio/Documents/Work_Area/Code_Development/Workspace/shybox/workflow/downloader/app_downloader_workflow_obs_main.py'
settings_file='/home/fabio/Documents/Work_Area/Code_Development/Workspace/shybox/workflow/downloader/app_downloader_workflow_obs_rain_server.json'
lock_folder='/home/fabio/Documents/Work_Area/Code_Development/Workspace/shybox/exec/case_study_vda/downloader_obs/locks'
report_folder='/home/fabio/Documents/Work_Area/Code_Development/Workspace/shybox/exec/case_study_vda/downloader_obs/reports'
settings_file="${SHYBOX_OBS_SETTINGS_FILE:-$settings_file}"
lock_folder="${SHYBOX_LOCK_DIR:-$lock_folder}"
report_folder="${SHYBOX_REPORT_DIR:-$report_folder}"
virtual_env_name="${SHYBOX_CONDA_ENV:-shybox_base_libraries}"

usage() {
    cat <<'EOF'
Usage:
  exec_downloader_workflow_obs.sh [--settings-file FILE] [realtime [TIME_PERIOD]]
  exec_downloader_workflow_obs.sh [--settings-file FILE] history TIME_START TIME_END

Realtime uses the current Europe/Rome hour and a four-hour lookback by default.
History takes inclusive hourly bounds in the form 'YYYY-MM-DD HH:00'.
The server JSON is the default. Use --settings-file to select the local profile.

Environment overrides:
  SHYBOX_CONDA_ENV, SHYBOX_CONDA_BIN, SHYBOX_PYTHON
  SHYBOX_OBS_SETTINGS_FILE, DOMAIN_NAME, PATH_LOG, PATH_DATA, SHYBOX_RUN_DIR
  SHYBOX_LOCK_DIR, SHYBOX_REPORT_DIR
EOF
}

usage_error() {
    printf '%s\n' "$1" >&2
    usage >&2
    exit 2
}

arguments=()
while (($# > 0)); do
    case "$1" in
        --settings-file)
            (($# >= 2)) || usage_error '--settings-file requires a path.'
            settings_file="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        -*)
            usage_error "Unknown option $1."
            ;;
        *)
            arguments+=("$1")
            shift
            ;;
    esac
done
set -- "${arguments[@]}"

mode="${1:-realtime}"
case "$mode" in
    realtime)
        (($# <= 2)) || usage_error 'Realtime accepts one optional TIME_PERIOD.'
        window_args=("$(TZ=Europe/Rome date '+%Y-%m-%d %H:00')" "${2:-${TIME_PERIOD:-4}}")
        ;;
    history)
        (($# == 3)) || usage_error 'History requires TIME_START and TIME_END.'
        window_args=("$2" "$3")
        ;;
    *)
        usage_error "Unknown mode $mode."
        ;;
esac

if ! metadata_python="$(command -v python3)"; then
    printf '%s\n' 'Python 3 is required to validate dates and write run records.' >&2
    exit 1
fi
if ! window="$("$metadata_python" - "$mode" "${window_args[@]}" <<'PY'
import sys
from datetime import datetime, timedelta

date_format = "%Y-%m-%d %H:%M"

def parse_hour(value):
    timestamp = datetime.strptime(value, date_format)
    if timestamp.minute or timestamp.strftime(date_format) != value:
        raise ValueError("Use 'YYYY-MM-DD HH:00' for hourly dates.")
    return timestamp

mode, first, second = sys.argv[1:]
try:
    if mode == "realtime":
        end = parse_hour(first)
        if not second.isascii() or not second.isdecimal():
            raise ValueError("TIME_PERIOD must be a nonnegative integer number of hours.")
        period = int(second)
        start = end - timedelta(hours=period)
    else:
        start, end = parse_hour(first), parse_hour(second)
        if start > end:
            raise ValueError("TIME_START must be earlier than or equal to TIME_END.")
        period = (end - start) // timedelta(hours=1)
except (ValueError, OverflowError) as exc:
    sys.exit(str(exc))
print(start.strftime(date_format), end.strftime(date_format), period, end.strftime("%Y%m%d%H%M"), sep="\t")
PY
)"; then
    exit 2
fi
IFS=$'\t' read -r time_start time_run time_period time_tag <<< "$window"

if [[ "$settings_file" != /* ]]; then
    settings_file="$PWD/$settings_file"
fi
export PYTHONPATH="$script_folder${PYTHONPATH:+:$PYTHONPATH}"
export DOMAIN_NAME="${DOMAIN_NAME:-vda}"
export PATH_LOG="${PATH_LOG:-$script_folder/exec/case_study_vda/downloader_obs/log}"
export PATH_DATA="${PATH_DATA:-$script_folder/exec/case_study_vda/downloader_obs/data}"
export TIME_RUN="$time_run"
export TIME_PERIOD="$time_period"

run_folder="${SHYBOX_RUN_DIR:-$PATH_LOG/$DOMAIN_NAME/runs}"
run_name="$mode"
run_hour="${time_tag:0:8}_${time_tag:8:2}"
command -v flock >/dev/null || { printf '%s\n' 'flock is required for run locking.' >&2; exit 1; }
mkdir -p -- "$run_folder" "$lock_folder" "$report_folder"
run_folder="$(cd -- "$run_folder" && pwd)"
run_file="$run_folder/${run_name}_${time_tag}.json"
lock_file="$lock_folder/lock_${run_name}_${run_hour}.lock"
report_file="$report_folder/report_${run_name}_${run_hour}.env"
exec {lock_fd}>>"$lock_file"
if flock -n -E 75 "$lock_fd"; then
    :
else
    lock_status=$?
    if ((lock_status == 75)); then
        printf 'Run %s %s is locked. See %s\n' "$run_name" "$time_run" "$report_file" >&2
    fi
    exit "$lock_status"
fi

started_at="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
started_seconds=$SECONDS
failure_message=''

record_status() {
    "$metadata_python" - "$run_file" "$report_file" "$run_name" "$mode" "$time_run" "$time_start" "$time_period" \
        "$started_at" "$((SECONDS - started_seconds))" "$settings_file" "$virtual_env_name" \
        "$DOMAIN_NAME" "$$" "$1" "$2" "$3" "$failure_message" <<'PY'
import json
import shlex
import sys
from pathlib import Path
from tempfile import NamedTemporaryFile

(json_path, report_path, run_name, mode, time_run, time_start, period, started, seconds, settings,
 environment, domain, pid, status, finished, exit_code, message) = sys.argv[1:]
record = {
    "status": status, "run_name": run_name, "mode": mode, "domain": domain, "time_zone": "Europe/Rome",
    "time_run": time_run, "time_start": time_start, "time_end": time_run,
    "time_period": int(period), "started_at": started, "finished_at": finished or None,
    "execution_time": int(seconds), "execution_time_unit": "seconds",
    "exit_code": int(exit_code) if exit_code else None, "pid": int(pid),
    "settings_file": settings, "conda_environment": environment or None, "message": message or None,
}

def write_atomic(path, content):
    target = Path(path)
    with NamedTemporaryFile(mode="w", encoding="utf-8", dir=target.parent, delete=False) as stream:
        temporary = Path(stream.name)
        try:
            stream.write(content)
            stream.close()
            temporary.replace(target)
        finally:
            temporary.unlink(missing_ok=True)

report = "".join(
    f"{key.upper()}={shlex.quote('' if value is None else str(value))}\n"
    for key, value in record.items()
)
write_atomic(json_path, json.dumps(record, indent=2) + "\n")
write_atomic(report_path, report)
PY
}

finish() {
    local result=$? status completed_at
    trap - EXIT
    completed_at="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
    case "$result" in
        0) status='success' ;;
        130|143) status='interrupted' ;;
        *) status='failed' ;;
    esac
    if ! record_status "$status" "$completed_at" "$result"; then
        printf 'Could not publish run metadata to %s and %s\n' "$run_file" "$report_file" >&2
        if ((result == 0)); then status='failed'; result=1; fi
    fi
    printf ' ==> %s. Run report %s\n' "$status" "$report_file"
    exit "$result"
}

fail_run() {
    failure_message="$1"
    printf '%s\n' "$failure_message" >&2
    exit 1
}

trap finish EXIT
trap 'failure_message="Received SIGINT."; exit 130' INT
trap 'failure_message="Received SIGTERM."; exit 143' TERM
if [[ -n "${SHYBOX_PYTHON:-}" ]]; then virtual_env_name=''; fi
record_status running '' ''

[[ -f "$settings_file" && -r "$settings_file" ]] || fail_run "Cannot read settings file $settings_file."
[[ -f "$script_file" && -r "$script_file" ]] || fail_run "Cannot read downloader script $script_file."

if [[ -n "${SHYBOX_PYTHON:-}" ]]; then
    command -v "$SHYBOX_PYTHON" >/dev/null || fail_run "Python executable $SHYBOX_PYTHON is unavailable."
    python_command=("$SHYBOX_PYTHON")
elif [[ -z "${SHYBOX_CONDA_BIN:-}" && -x "$script_folder/conda/envs/$virtual_env_name/bin/python" ]]; then
    python_command=("$script_folder/conda/envs/$virtual_env_name/bin/python")
else
    conda_bin="${SHYBOX_CONDA_BIN:-$script_folder/conda/bin/conda}"
    if [[ -z "${SHYBOX_CONDA_BIN:-}" && ! -x "$conda_bin" ]]; then conda_bin=conda; fi
    command -v "$conda_bin" >/dev/null || fail_run "Conda executable $conda_bin is unavailable."
    python_command=("$conda_bin" run --no-capture-output --name "$virtual_env_name" python)
fi

if ! "${python_command[@]}" -B -c 'import psycopg; import workflow.downloader.app_downloader_workflow_obs_main'; then
    fail_run "Unable to import the downloader and psycopg using ${virtual_env_name:-$SHYBOX_PYTHON}."
fi

printf ' ===================================================================================\n'
printf ' ==> %s (Version: %s Release Date: %s)\n' "$script_name" "$script_version" "$script_date"
printf ' ==> START %s from %s to %s (TIME_PERIOD=%s)\n' "$mode" "$time_start" "$time_run" "$time_period"
printf ' ==> COMMAND LINE:'
printf ' %q' "${python_command[@]}" -B "$script_file" -settings_file "$settings_file" -time "$time_run"
printf '\n'
if "${python_command[@]}" -B "$script_file" -settings_file "$settings_file" -time "$time_run"; then
    printf ' ==> END\n'
else
    result=$?
    failure_message="Downloader exited with status $result."
    exit "$result"
fi
