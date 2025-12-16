# utils_config.py
from __future__ import annotations

from pathlib import Path
from datetime import date
from typing import Dict, Any, Optional
import json

# -----------------------------------------------------------------------------
# Base paths and default config
# -----------------------------------------------------------------------------
BASE_DIR = Path(__file__).parent
CONFIG_PATH = BASE_DIR / "config.json"

DEFAULT_CONFIG: Dict[str, Any] = {
    "data_root": "data",
    "dynamic_root": "data/dynamic",

    "basins": ["basin_01", "basin_02", "basin_03"],
    "default_basin": "basin_02",

    "sections_csv": "data/static/point/sections_marche_FloodPROOFS.csv",
    "dem_file": "data/static/gridded/marche.dem.txt",
    "choice_file": "data/static/gridded/marche.choice.txt",

    # Time-series (TS) keys (canonical)
    "ts_history_days": 3,
    "ts_debug_today": None,

    # Maps keys may exist in the same JSON, but are not used here:
    # "maps_history_days": ...
    # "maps_debug_today": ...

    # Default runs (legacy layout – config.json/app.py can override)
    "runs": {
        "run_type_1": {
            "run_type": "deterministic_weather_stations",
            "name": "Deterministic weather stations",

            "path_template": "{dynamic_root}/{run_type}/{date_path}/collections",
            "format_of_date_path": "%Y/%m/%d/%H",

            "file_glob": "hydrograph_*.json",
            "format_of_date_file": "%Y%m%d%H%M",

            "time_field": "time_period",
            "obs_field": "time_series_discharge_observed",
            "fcst_field": "time_series_discharge_simulated",

            "missing_value": -9998.0,
            "basin_field": "section_domain",
            "section_field": "section_name",
        }
    },
}


def load_config() -> Dict[str, Any]:
    """
    Load config.json and merge with DEFAULT_CONFIG.
    Values in config.json override defaults if not None.
    """
    cfg = DEFAULT_CONFIG.copy()
    if CONFIG_PATH.exists():
        try:
            with CONFIG_PATH.open() as f:
                user_cfg = json.load(f)
            cfg.update({k: v for k, v in user_cfg.items() if v is not None})
        except Exception as e:
            print("[WARNING] could not load config.json:", e)
    else:
        print("[INFO] config.json not found, using DEFAULT_CONFIG.")
    return cfg


def resolve_path(path_str: str) -> Path:
    """
    Turn a possibly-relative path into an absolute path under BASE_DIR.
    """
    p = Path(path_str)
    if not p.is_absolute():
        p = BASE_DIR / p
    return p


# -----------------------------------------------------------------------------
# Global config object (app.py may overwrite CONFIG after import)
# -----------------------------------------------------------------------------
CONFIG: Dict[str, Any] = load_config()

# -----------------------------------------------------------------------------
# Derived globals (initialized once, but refreshable)
# -----------------------------------------------------------------------------
DATA_ROOT: Path = BASE_DIR
DYNAMIC_ROOT: Path = BASE_DIR

SECTIONS_CSV: Path = BASE_DIR
DEM_FILE: Path = BASE_DIR
CHOICE_FILE: Path = BASE_DIR

# Back-compat public name, but value is TS-scoped
HISTORY_DAYS: int = 3

# TS debug date
TS_DEBUG_TODAY: Optional[date] = None


def _parse_debug_date(raw_value: Any) -> Optional[date]:
    if raw_value is None:
        return None

    if isinstance(raw_value, str):
        raw_str = raw_value.strip()
    else:
        raw_str = str(raw_value).strip()

    if not raw_str:
        return None

    # ISO: YYYY-MM-DD
    try:
        return date.fromisoformat(raw_str)
    except ValueError:
        pass

    # yyyymmdd
    if len(raw_str) == 8 and raw_str.isdigit():
        try:
            return date(int(raw_str[0:4]), int(raw_str[4:6]), int(raw_str[6:8]))
        except ValueError:
            return None

    # YYYY/MM/DD
    if "/" in raw_str:
        try:
            parts = raw_str.split("/")
            if len(parts) == 3:
                return date(int(parts[0]), int(parts[1]), int(parts[2]))
        except ValueError:
            return None

    return None


def refresh_from_config() -> None:
    """
    Recompute all derived globals from the current CONFIG dict.

    Call this:
      - once at import time (done below)
      - again after app.py overwrites utils_config.CONFIG
    """
    global DATA_ROOT, DYNAMIC_ROOT, SECTIONS_CSV, DEM_FILE, CHOICE_FILE
    global HISTORY_DAYS, TS_DEBUG_TODAY

    # Paths
    DATA_ROOT = resolve_path(CONFIG.get("data_root", DEFAULT_CONFIG["data_root"]))
    DYNAMIC_ROOT = resolve_path(CONFIG.get("dynamic_root", DEFAULT_CONFIG["dynamic_root"]))

    SECTIONS_CSV = resolve_path(CONFIG.get("sections_csv", DEFAULT_CONFIG["sections_csv"]))
    DEM_FILE = resolve_path(CONFIG.get("dem_file", DEFAULT_CONFIG["dem_file"]))
    CHOICE_FILE = resolve_path(CONFIG.get("choice_file", DEFAULT_CONFIG["choice_file"]))

    # TS history days (canonical)
    HISTORY_DAYS = int(CONFIG.get("ts_history_days", CONFIG.get("history_days", 3)))

    # TS debug today (canonical)
    TS_DEBUG_TODAY = _parse_debug_date(CONFIG.get("ts_debug_today"))

    if TS_DEBUG_TODAY is None:
        print("[CONFIG] ts_debug_today not set or invalid -> using real today()")
    else:
        print("[CONFIG] ts_debug_today parsed as", TS_DEBUG_TODAY.isoformat())


def refresh_ts_time_settings() -> None:
    """
    Backward-compatible name expected by app.py.
    Also refreshes paths + HISTORY_DAYS to keep everything coherent.
    """
    refresh_from_config()


def get_today() -> date:
    """
    Time-series 'today' used by utils_runs (and TS parts of app).
    """
    return TS_DEBUG_TODAY or date.today()


# Initialize derived globals on import
refresh_from_config()

