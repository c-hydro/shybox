# utils_runs.py
from __future__ import annotations

from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, Tuple
import json

from flask import request

from utils_config import (
    CONFIG,
    DATA_ROOT,
    DYNAMIC_ROOT,
    HISTORY_DAYS,   # legacy name, but app.py patches it to TS_HISTORY_DAYS
    BASE_DIR,
    get_today,      # MUST be based on CONFIG["ts_debug_today"] in utils_config.py
)

# -----------------------------------------------------------------------------
# TS-scoped settings (canonical keys)
# -----------------------------------------------------------------------------
TS_HISTORY_DAYS = int(CONFIG.get("ts_history_days", HISTORY_DAYS))


# -----------------------------------------------------------------------------
# Runs configuration (time-series and maps)
# -----------------------------------------------------------------------------
_raw_runs = CONFIG.get("runs", {}) or {}

# Time-series runs:
# - If runs is grouped: CONFIG["runs"]["run_time_series"]
# - If runs is legacy flat: CONFIG["runs"]
if isinstance(_raw_runs, dict) and "run_time_series" in _raw_runs:
    RUNS: Dict[str, Any] = _raw_runs.get("run_time_series") or {}
    print("[CONFIG] using runs.run_time_series for time-series run configs")
else:
    RUNS = _raw_runs if isinstance(_raw_runs, dict) else {}
    print("[CONFIG] using top-level runs as time-series configs")

# Active time-series runs
_raw_active = CONFIG.get("active_time_series")

# Backwards compatibility
if _raw_active is None:
    legacy_single = CONFIG.get("active_run_time_series") or CONFIG.get("active_run")
    if legacy_single is not None:
        _raw_active = [legacy_single]

if isinstance(_raw_active, str):
    ts_active_list = [_raw_active]
elif isinstance(_raw_active, list):
    ts_active_list = [str(x) for x in _raw_active]
else:
    ts_active_list = []

# Keep only keys that exist
ACTIVE_TIME_SERIES: List[str] = [k for k in ts_active_list if k in RUNS]

# If still empty, default to the first run if any
if not ACTIVE_TIME_SERIES and RUNS:
    ACTIVE_TIME_SERIES = [list(RUNS.keys())[0]]

ACTIVE_RUN_KEY: Optional[str] = ACTIVE_TIME_SERIES[0] if ACTIVE_TIME_SERIES else None
ACTIVE_RUN = RUNS.get(ACTIVE_RUN_KEY) if ACTIVE_RUN_KEY else None

# Map run configurations (kept for future maps logic)
MAP_RUNS: Dict[str, Any] = {}
if isinstance(_raw_runs, dict) and "run_maps" in _raw_runs:
    MAP_RUNS = _raw_runs.get("run_maps") or {}
ACTIVE_MAPS = CONFIG.get("active_maps", [])


# -----------------------------------------------------------------------------
# Helpers for selecting run config and building paths
# -----------------------------------------------------------------------------
def get_current_run_config() -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
    """
    Determine current time-series run configuration from ?run_cfg=, falling back
    to active_time_series, then first available.
    """
    key = request.args.get("run_cfg")
    if key and key in RUNS:
        return key, RUNS[key]

    if ACTIVE_RUN_KEY and ACTIVE_RUN_KEY in RUNS:
        return ACTIVE_RUN_KEY, RUNS[ACTIVE_RUN_KEY]

    if RUNS:
        first_key = list(RUNS.keys())[0]
        return first_key, RUNS[first_key]

    return None, None


def build_run_directory(ts_dt: datetime, basin: Optional[str] = None) -> Optional[Path]:
    """
    Build the directory path for a given datetime and basin,
    using the current time-series run configuration.
    """
    _, ts_run_cfg = get_current_run_config()
    if not RUNS or ts_run_cfg is None:
        return None

    run_type = ts_run_cfg.get("run_type", "")
    path_template = ts_run_cfg.get("path_template")
    fmt_path = ts_run_cfg.get("format_of_date_path", "%Y/%m/%d/%H")

    if not path_template:
        return None

    date_path = ts_dt.strftime(fmt_path)
    context = {
        "dynamic_root": str(DYNAMIC_ROOT),
        "run_type": run_type,
        "date_path": date_path,
        "basin": basin or "",
        "section": "",
    }
    path_str = path_template.format(**context)
    run_dir = Path(path_str)
    if not run_dir.is_absolute():
        run_dir = BASE_DIR / run_dir
    return run_dir


# -----------------------------------------------------------------------------
# Recent runs and run selection
# -----------------------------------------------------------------------------
def list_recent_available_runs_for_basin(
    basin: str, ts_days_back: Optional[int] = None
) -> List[datetime]:
    """
    Scan the dynamic/legacy folders to find available time-series runs for a basin.

    NOTE:
      - TS "today" must come from utils_config.get_today(), which must read
        CONFIG["ts_debug_today"] (no debug_today anywhere).
    """
    if ts_days_back is None:
        ts_days_back = TS_HISTORY_DAYS

    ts_today = get_today()
    ts_runs: List[datetime] = []

    _, ts_run_cfg = get_current_run_config()
    if not RUNS or ts_run_cfg is None:
        # Legacy: only date-based
        file_glob = "timeseries.json"
        for i in range(ts_days_back):
            d = ts_today - timedelta(days=i)
            folder_name = d.strftime("%Y%m%d")
            base_dir = DATA_ROOT / folder_name / basin
            if not base_dir.exists():
                continue
            if any(base_dir.rglob(file_glob)):
                ts_runs.append(datetime(d.year, d.month, d.day, 0, 0))
        return sorted(ts_runs)

    file_glob = ts_run_cfg.get("file_glob", "hydrograph_*.json")

    for i in range(ts_days_back):
        d = ts_today - timedelta(days=i)
        for hour in range(24):
            ts_dt = datetime(d.year, d.month, d.day, hour, 0)
            run_dir = build_run_directory(ts_dt, basin=basin)
            if not run_dir or not run_dir.exists():
                continue
            if any(run_dir.glob(file_glob)):
                ts_runs.append(ts_dt)

    return sorted(ts_runs)


def get_selected_or_latest_run_for_basin(
    basin: str, ts_days_back: Optional[int] = None
) -> Tuple[datetime, List[datetime]]:
    """
    Read ?run= from query parameters; if valid and present in available runs, use it.
    Otherwise, fall back to the latest available run, or "ts_today 00:00" if none.
    """
    if ts_days_back is None:
        ts_days_back = TS_HISTORY_DAYS

    ts_available_runs = list_recent_available_runs_for_basin(basin, ts_days_back)
    raw = request.args.get("run")

    if raw:
        try:
            sel = datetime.fromisoformat(raw)
            if any(r == sel for r in ts_available_runs):
                return sel, ts_available_runs
        except ValueError:
            pass

    if ts_available_runs:
        return ts_available_runs[-1], ts_available_runs

    base = get_today()
    return datetime(base.year, base.month, base.day, 0, 0), ts_available_runs


# -----------------------------------------------------------------------------
# Loading dynamic time-series data
# -----------------------------------------------------------------------------
def load_timeseries_sections(
    ts_selected_run: datetime, basin: str
) -> Tuple[Dict[str, Any], Dict[str, str]]:
    """
    Load dynamic hydrographs for the given run and basin.

    Returns:
      ts_sections[section_name] = {"time_now": str|None, "series": list[dict]}
      ts_json_paths[section_name] = path to file
    """
    ts_sections: Dict[str, Any] = {}
    ts_json_paths: Dict[str, str] = {}

    _, ts_run_cfg = get_current_run_config()
    if not RUNS or ts_run_cfg is None:
        # Legacy layout: DATA_ROOT/YYYYMMDD/basin/section/timeseries.json
        d = ts_selected_run.date()
        folder_name = d.strftime("%Y%m%d")
        base_dir = DATA_ROOT / folder_name / basin
        if not base_dir.exists():
            return ts_sections, ts_json_paths

        for section_dir in sorted(p for p in base_dir.iterdir() if p.is_dir()):
            json_file = section_dir / "timeseries.json"
            if not json_file.exists():
                continue
            try:
                with json_file.open() as f:
                    payload = json.load(f)
            except Exception:
                continue

            ts_sections[section_dir.name] = {
                "time_now": payload.get("time_now"),
                "series": payload.get("series", []),
            }
            ts_json_paths[section_dir.name] = str(json_file)

        return ts_sections, ts_json_paths

    # New dynamic layout
    run_dir = build_run_directory(ts_selected_run, basin=basin)
    if not run_dir or not run_dir.exists():
        return ts_sections, ts_json_paths

    file_glob = ts_run_cfg.get("file_glob", "hydrograph_*.json")
    time_field = ts_run_cfg.get("time_field", "time_period")
    obs_field = ts_run_cfg.get("obs_field")
    fcst_field = ts_run_cfg.get("fcst_field")
    missing_value = float(ts_run_cfg.get("missing_value", -9998.0))
    basin_field = ts_run_cfg.get("basin_field", "section_domain")
    section_field = ts_run_cfg.get("section_field", "section_name")

    for json_file in sorted(run_dir.glob(file_glob)):
        try:
            with json_file.open() as f:
                payload = json.load(f)
        except Exception:
            continue

        basin_value = payload.get(basin_field)
        if basin_value and basin_value != basin:
            continue

        section_name = payload.get(section_field, json_file.stem)
        time_now = payload.get("time_run")

        time_str = payload.get(time_field, "")
        if not time_str:
            continue

        times_raw = [t.strip() for t in str(time_str).split(",") if t.strip()]

        # Convert "YYYY-MM-DD HH:MM" -> "YYYY-MM-DDTHH:MM:00"
        times_iso: List[str] = []
        for t in times_raw:
            if " " in t:
                date_part, time_part = t.split(" ", 1)
                ts_iso = f"{date_part}T{time_part}:00"
            else:
                ts_iso = t
            times_iso.append(ts_iso)

        series: List[Dict[str, Any]] = []

        if obs_field and obs_field in payload:
            obs_vals = [v.strip() for v in str(payload[obs_field]).split(",")]
        else:
            obs_vals = []

        if fcst_field and fcst_field in payload:
            fcst_vals = [v.strip() for v in str(payload[fcst_field]).split(",")]
        else:
            fcst_vals = []

        n = len(times_iso)
        for i in range(n):
            ts = times_iso[i]

            # Observed
            if i < len(obs_vals):
                try:
                    val_obs = float(obs_vals[i])
                    if val_obs != missing_value and val_obs >= 0.0:
                        series.append({"timestamp": ts, "value": val_obs, "type": "observed"})
                except ValueError:
                    pass

            # Forecast
            if i < len(fcst_vals):
                try:
                    val_fc = float(fcst_vals[i])
                    if val_fc != missing_value and val_fc >= 0.0:
                        series.append({"timestamp": ts, "value": val_fc, "type": "forecast"})
                except ValueError:
                    pass

        if not series:
            continue

        ts_sections[section_name] = {"time_now": time_now, "series": series}
        ts_json_paths[section_name] = str(json_file)

    return ts_sections, ts_json_paths


# -----------------------------------------------------------------------------
# Build multi-variable time series table for one JSON
# -----------------------------------------------------------------------------
def build_section_table_from_json(json_file: Path, ts_run_cfg: Optional[Dict[str, Any]]):
    """
    Build a multi-variable time-series table for one section from a hydrograph JSON.

    Returns:
      table: {
        "columns": [ {"key": ..., "label": ...}, ... ],
        "rows": [ {"time": "...", <series_key>: value_str, ...}, ... ]
      }
      meta: dict with non-series, non-time_field entries
    """
    if not json_file.exists():
        return {"columns": [], "rows": []}, {}

    with json_file.open() as f:
        payload = json.load(f)

    # Config
    if ts_run_cfg is None:
        time_field = "time_period"
        missing_value = -9998.0
    else:
        time_field = ts_run_cfg.get("time_field", "time_period")
        missing_value = float(ts_run_cfg.get("missing_value", -9998.0))

    # Time axis
    time_str = payload.get(time_field, "")
    times_raw = [t.strip() for t in str(time_str).split(",") if t.strip()]

    times_disp: List[str] = []
    for t in times_raw:
        # Expect "YYYY-MM-DD HH:MM"
        if " " in t:
            date_part, time_part = t.split(" ", 1)
            times_disp.append(f"{date_part} {time_part[:5]}")
        else:
            times_disp.append(t)

    # All series keys starting with "time_series_"
    series_keys = [k for k in payload.keys() if k.startswith("time_series_")]

    # Priority ordering: observed & simulated discharge first
    priority = [
        "time_series_discharge_observed",
        "time_series_discharge_simulated",
    ]
    ordered_keys = [k for k in priority if k in series_keys] + sorted(
        k for k in series_keys if k not in priority
    )

    def series_label(key: str) -> str:
        short = key[len("time_series_") :]
        if short == "discharge_observed":
            return "Q obs [m³/s]"
        elif short == "discharge_simulated":
            return "Q sim [m³/s]"
        elif short == "rain_observed":
            return "Rain obs [mm]"
        elif short == "air_temperature_observed":
            return "T air obs [°C]"
        elif short == "soil_moisture_simulated":
            return "Soil moisture sim [-]"
        else:
            return short.replace("_", " ")

    # Columns: time + each series
    columns = [{"key": "time", "label": "Time"}]
    for k in ordered_keys:
        columns.append({"key": k, "label": series_label(k)})

    # Pre-split each series string
    series_arrays: Dict[str, List[str]] = {}
    for k in ordered_keys:
        s_str = payload.get(k, "")
        if s_str:
            series_arrays[k] = [v.strip() for v in str(s_str).split(",")]
        else:
            series_arrays[k] = []

    rows: List[Dict[str, str]] = []
    for i, tlabel in enumerate(times_disp):
        row: Dict[str, str] = {"time": tlabel}
        for k in ordered_keys:
            arr = series_arrays.get(k, [])
            if i < len(arr):
                v_str = arr[i]
                try:
                    val = float(v_str)
                except ValueError:
                    row[k] = ""
                    continue
                # Treat missing or negative as empty
                if val == missing_value or val < 0.0:
                    row[k] = ""
                else:
                    row[k] = f"{val:.3f}"
            else:
                row[k] = ""
        rows.append(row)

    # Metadata: everything except time_field and time_series_* keys
    meta: Dict[str, Any] = {}
    for k, v in payload.items():
        if k == time_field:
            continue
        if k.startswith("time_series_"):
            continue
        # Avoid dumping gigantic strings
        if isinstance(v, str) and len(v) > 200:
            v = v[:200] + "..."
        meta[k] = v

    table = {"columns": columns, "rows": rows}
    return table, meta


# -----------------------------------------------------------------------------
# Basin summary cards for Home view
# -----------------------------------------------------------------------------
def get_basin_cards(basins: List[str], ts_days_back: Optional[int] = None):
    """
    Build basin cards with latest run and time_now info for each basin.
    """
    if ts_days_back is None:
        ts_days_back = TS_HISTORY_DAYS

    cards = []
    for basin in basins:
        ts_recent_runs = list_recent_available_runs_for_basin(basin, ts_days_back)
        if not ts_recent_runs:
            cards.append({"name": basin, "latest_date": None, "latest_time_now": None})
            continue

        ts_latest_run = ts_recent_runs[-1]
        ts_sections, _ = load_timeseries_sections(ts_latest_run, basin)
        ts_latest_time_now = None
        if ts_sections:
            first_section_name = sorted(ts_sections.keys())[0]
            ts_latest_time_now = ts_sections[first_section_name].get("time_now")

        cards.append(
            {"name": basin, "latest_date": ts_latest_run, "latest_time_now": ts_latest_time_now}
        )
    return cards

