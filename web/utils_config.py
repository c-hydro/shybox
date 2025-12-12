from pathlib import Path
from datetime import date
from typing import Dict, Any, Optional
import json

# Base paths and config file
BASE_DIR = Path(__file__).parent
CONFIG_PATH = BASE_DIR / "config.json"

# Default config (same as old app.py)
DEFAULT_CONFIG: Dict[str, Any] = {
    "data_root": "data",
    "dynamic_root": "data/dynamic",

    "basins": ["basin_01", "basin_02", "basin_03"],
    "default_basin": "basin_02",

    "sections_csv": "data/static/point/sections_marche_FloodPROOFS.csv",
    "dem_file": "data/static/gridded/marche.dem.txt",
    "choice_file": "data/static/gridded/marche.choice.txt",

    "history_days": 3,
    "debug_today": None,

    # Default runs (simple legacy layout – config.json will override this)
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


CONFIG = load_config()

# Main paths
DATA_ROOT = resolve_path(CONFIG["data_root"])
DYNAMIC_ROOT = resolve_path(CONFIG.get("dynamic_root", "data/dynamic"))

SECTIONS_CSV = resolve_path(CONFIG["sections_csv"])
DEM_FILE = resolve_path(CONFIG["dem_file"])
CHOICE_FILE = resolve_path(CONFIG["choice_file"])

HISTORY_DAYS = int(CONFIG.get("history_days", 3))

# -------------------------------------------------------------------------
# debug_today handling (same behavior as original app.py)
# -------------------------------------------------------------------------
raw_debug = CONFIG.get("debug_today")
DEBUG_TODAY: Optional[date] = None

if raw_debug is not None:
    if isinstance(raw_debug, str):
        raw_str = raw_debug.strip()
    else:
        raw_str = str(raw_debug).strip()

    if raw_str:
        parsed: Optional[date] = None
        # ISO format: YYYY-MM-DD
        try:
            parsed = date.fromisoformat(raw_str)
        except ValueError:
            parsed = None

        # yyyymmdd
        if parsed is None and len(raw_str) == 8 and raw_str.isdigit():
            try:
                parsed = date(
                    int(raw_str[0:4]),
                    int(raw_str[4:6]),
                    int(raw_str[6:8]),
                )
            except ValueError:
                parsed = None

        # YYYY/MM/DD
        if parsed is None and "/" in raw_str:
            try:
                parts = raw_str.split("/")
                if len(parts) == 3:
                    parsed = date(int(parts[0]), int(parts[1]), int(parts[2]))
            except ValueError:
                parsed = None

        DEBUG_TODAY = parsed

if DEBUG_TODAY is None:
    print("[CONFIG] debug_today not set or invalid -> using real today()")
else:
    print("[CONFIG] debug_today parsed as", DEBUG_TODAY.isoformat())


def get_today() -> date:
    return DEBUG_TODAY or date.today()

