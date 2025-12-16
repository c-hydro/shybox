# app.py
from flask import Flask, render_template, request, jsonify, send_file, abort, url_for
from pathlib import Path
from typing import List, Optional, Dict, Any
import json, os, re
import io
import gzip, tempfile
import numpy as np

# -----------------------------------------------------------------------------
# Config loader (supports legacy + new nested schema; tolerates common JSON typos)
# -----------------------------------------------------------------------------
def _preprocess_config_text(raw: str) -> str:
    # Remove // and # comments (non-standard JSON, but common in config files)
    raw = re.sub(r"//.*?$", "", raw, flags=re.M)
    raw = re.sub(r"#.*?$", "", raw, flags=re.M)

    # Fix a common authoring mistake we have seen in this project:
    # missing closing braces between "geo" and "runs"
    raw = re.sub(
        r'\n\s*},\s*\n\s*\n\s*"runs"\s*:',
        '\n    }\n  },\n\n  "runs":',
        raw,
        count=1,
    )

    # Remove trailing commas before } or ]
    raw = re.sub(r",\s*([}\]])", r"\1", raw)
    return raw


def load_app_config(config_path: str) -> Dict[str, Any]:
    p = Path(config_path)
    raw = p.read_text(encoding="utf-8", errors="replace")
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        # Try again with tolerant preprocessing
        return json.loads(_preprocess_config_text(raw))


def build_compat_config(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """
    Compatibility layer.

    TS keys:
      - ts_history_days
      - ts_debug_today   (ONLY; debug_today is deprecated and must not be used)

    IMPORTANT:
      - Keep runs grouped (runs.run_time_series / runs.run_maps), so utils_runs
        can correctly detect time-series configs.
      - Do NOT reintroduce debug_today.
    """
    out = dict(cfg or {})

    # Optional legacy alias: history_days <- ts_history_days (keep TS canonical)
    if "history_days" not in out and "ts_history_days" in out:
        out["history_days"] = out["ts_history_days"]

    # Never keep/reintroduce old key
    out.pop("debug_today", None)

    # --- Geo aliases (keep nested form intact too) ---
    geo = (cfg or {}).get("geo") or {}
    layers = geo.get("layers") or {}
    sections = geo.get("sections") or {}

    terrain = (layers.get("terrain") or {}).get("file_name")
    river_network = (layers.get("river_network") or {}).get("file_name")
    sections_file = (sections.get("file_name"))

    # Multiple aliases to maximize compatibility with existing helper modules
    if sections_file:
        out.setdefault("sections_file", sections_file)
        out.setdefault("geo_sections_file", sections_file)
    if terrain:
        out.setdefault("dem_file", terrain)
        out.setdefault("geo_dem_file", terrain)
        out.setdefault("terrain_file", terrain)
    if river_network:
        out.setdefault("choice_file", river_network)
        out.setdefault("geo_river_file", river_network)
        out.setdefault("river_network_file", river_network)

    # --- Runs: KEEP GROUPED STRUCTURE ---
    runs = (cfg or {}).get("runs") or {}
    if isinstance(runs, dict) and ("run_time_series" in runs or "run_maps" in runs):
        # preserve grouped runs exactly
        out["runs"] = runs
        out.setdefault(
            "active_runs",
            list(
                dict.fromkeys(
                    (cfg.get("active_time_series") or []) + (cfg.get("active_maps") or [])
                )
            ),
        )
    else:
        # Legacy layout: runs already flat
        out["runs"] = runs

    return out
CMAP_DIR = Path(__file__).parent / "static" / "cmaps"

# Map variable name -> palette + fixed range
PALETTE_RULES = {
    # Air temperature (adjust range if needed)
    "AirTemperature": {"file": "lst.cmap", "vmin": -20.0, "vmax": 45.0, "units": "°C"},
    "AirT":           {"file": "lst.cmap", "vmin": -20.0, "vmax": 45.0, "units": "°C"},
    # Examples (change to your needs)
    "Rain":           {"file": "swe.cmap", "vmin": 0.0,   "vmax": 50.0, "units": "mm"},
    "Wind":           {"file": "sm.cmap",  "vmin": 0.0,   "vmax": 25.0, "units": "m/s"},
    "WindSpeed":      {"file": "sm.cmap",  "vmin": 0.0,   "vmax": 25.0, "units": "m/s"},
}

def pick_palette(var_name: str):
    """Return (cmap_file, vmin, vmax, units) or None if no rule."""
    rule = PALETTE_RULES.get(var_name)
    if not rule:
        return None
    return rule["file"], float(rule["vmin"]), float(rule["vmax"]), rule.get("units", "")

def load_listed_cmap(cmap_filename: str):
    """Load your .cmap JSON into a matplotlib ListedColormap."""
    from matplotlib.colors import ListedColormap

    p = CMAP_DIR / cmap_filename
    if not p.exists():
        raise FileNotFoundError(f"Missing cmap file: {p}")

    with p.open("r") as f:
        d = json.load(f)

    colors = d["colors"]  # list of [r,g,b,a] in 0..1
    return ListedColormap(colors, name=p.stem)



# -----------------------------------------------------------------------------
# Load config early, patch utils_config BEFORE importing other modules
# -----------------------------------------------------------------------------
import utils_config as _uc  # type: ignore

_CFG_PATH = os.environ.get("APP_CONFIG", "config.json")
_uc.CONFIG = build_compat_config(load_app_config(_CFG_PATH))

# TS-scoped history (canonical)
TS_HISTORY_DAYS = int(_uc.CONFIG.get("ts_history_days", _uc.CONFIG.get("history_days", 3)))

# MAP-scoped history (canonical)
MAPS_HISTORY_DAYS = int(_uc.CONFIG.get("maps_history_days", 3))

# Patch legacy module-level constant so existing helpers remain functional
_uc.HISTORY_DAYS = TS_HISTORY_DAYS

# Force utils_config to re-parse TS debug date AFTER CONFIG injection (recommended)
# You should implement this function in utils_config.py.
if hasattr(_uc, "refresh_ts_time_settings"):
    _uc.refresh_ts_time_settings()

CONFIG = _uc.CONFIG

def _print_search_locations():
    runs = CONFIG.get("runs", {})
    ts_runs = runs.get("run_time_series", {})
    mp_runs = runs.get("run_maps", {})

    print("\n[CONFIG] ===== SEARCH LOCATIONS =====")

    print("[CONFIG] ts_debug_today   =", CONFIG.get("ts_debug_today"))
    print("[CONFIG] maps_debug_today =", CONFIG.get("maps_debug_today"))
    print("[CONFIG] ts_history_days  =", CONFIG.get("ts_history_days"))
    print("[CONFIG] maps_history_days=", CONFIG.get("maps_history_days"))

    print("\n[CONFIG] --- TIME-SERIES SEARCH ---")
    for key, cfg in ts_runs.items():
        print(
            f"[CONFIG] TS {key}\n"
            f"  path_template       = {cfg.get('path_template')}\n"
            f"  date_path_format    = {cfg.get('format_of_date_path')}\n"
            f"  file_glob           = {cfg.get('file_glob')}\n"
            f"  file_date_format    = {cfg.get('format_of_date_file')}"
        )

    print("\n[CONFIG] --- MAPS SEARCH ---")
    for key, cfg in mp_runs.items():
        print(
            f"[CONFIG] MAPS {key}\n"
            f"  path_template       = {cfg.get('path_template')}\n"
            f"  date_path_format    = {cfg.get('format_of_date_path')}\n"
            f"  file_glob           = {cfg.get('file_glob')}\n"
            f"  file_date_format    = {cfg.get('format_of_date_file')}"
        )

    print("[CONFIG] =============================\n")


# Print ONCE (avoid Flask reloader spam)
if os.environ.get("WERKZEUG_RUN_MAIN") == "true":
    _print_search_locations()

# Now import the rest (after patching utils_config)
from utils_sections import load_csv_basins, load_all_sections
from utils_runs import (
    RUNS,
    ACTIVE_RUN_KEY,
    ACTIVE_RUN,
    get_current_run_config,
    get_selected_or_latest_run_for_basin,
    load_timeseries_sections,
    build_section_table_from_json,
    get_basin_cards,
)
from utils_grids import DEM_META, DEM_GRID, CHOICE_META, CHOICE_GRID

# For DEM image
try:
    from PIL import Image
except ImportError:
    Image = None

app = Flask(__name__)


# -----------------------------------------------------------------------------
# Config / JSON validation helpers
# -----------------------------------------------------------------------------
def _json_error_hints(raw_text: str, err_pos: int) -> str:
    """Return a short hint string for common JSON authoring mistakes."""
    window = raw_text[max(0, err_pos - 80) : err_pos + 80]
    if re.search(r",\s*[}\]]", window):
        return "Hint: trailing comma before '}' or ']'. Remove the comma."
    if "'" in window and '"' not in window:
        return "Hint: JSON requires double quotes for strings/keys."
    return "Hint: check commas, quotes, and brackets around the reported location."


def validate_json_file(json_path: str) -> Dict[str, Any]:
    """Validate a JSON file and return a structured report."""
    p = Path(json_path)
    report: Dict[str, Any] = {"path": str(p), "ok": False}
    if not p.exists():
        report["error"] = "File not found"
        return report

    raw = p.read_text(encoding="utf-8", errors="replace")
    try:
        json.loads(raw)
        report["ok"] = True
        return report
    except json.JSONDecodeError as e:
        report.update(
            {
                "error": e.msg,
                "line": e.lineno,
                "col": e.colno,
                "pos": e.pos,
                "hint": _json_error_hints(raw, e.pos),
                "excerpt": raw[max(0, e.pos - 120) : min(len(raw), e.pos + 120)],
            }
        )
        return report


@app.route("/debug/validate_config")
def debug_validate_config():
    """Return JSON validation report for the main config file."""
    cfg_path = os.environ.get("APP_CONFIG", "config.json")
    return jsonify(validate_json_file(cfg_path))


# -----------------------------------------------------------------------------
# Global data loaded at startup
# -----------------------------------------------------------------------------
BASINS = load_csv_basins()
SECTIONS_ALL = load_all_sections()
DEFAULT_BASIN = CONFIG.get("default_basin", BASINS[0] if BASINS else "")


# -----------------------------------------------------------------------------
# Run-config helpers
# -----------------------------------------------------------------------------
def _filter_run_configs_by_type(run_configs: Dict[str, Any], allowed_types: List[str]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, cfg in (run_configs or {}).items():
        if isinstance(cfg, dict) and cfg.get("type") in allowed_types:
            out[k] = cfg
    return out



# -----------------------------------------------------------------------------
# MAPS run discovery helpers (lightweight, independent from utils_runs)
# -----------------------------------------------------------------------------
from datetime import datetime, timedelta
from glob import glob

def _get_maps_run_configs() -> Dict[str, Any]:
    runs = (CONFIG.get("runs") or {}).get("run_maps") or {}
    return runs if isinstance(runs, dict) else {}

UI_DT_FMT = "%Y-%m-%d %H:00"
ISO_DT_FMT = "%Y-%m-%dT%H:%M"

def parse_ui_datetime(s):
    """
    Parse datetime strings coming from UI selectors.

    Supported formats:
      - YYYY-MM-DD HH:00   (UI format)
      - ISO 8601           (fallback / backward compatibility)
    """
    if not s:
        return None
    try:
        return datetime.strptime(s, UI_DT_FMT)
    except ValueError:
        try:
            return datetime.fromisoformat(s)
        except Exception:
            return None



def _parse_dt_from_filename(name: str, fmt: str) -> Optional[datetime]:
    # Extract digits and try to parse the last len(digits_expected) digits.
    digits = "".join(ch for ch in Path(name).stem if ch.isdigit())
    if not digits:
        return None
    # Build a sample string from format to estimate expected length.
    sample = datetime(2000, 1, 1, 0, 0).strftime(fmt)
    n = len(sample)
    if len(digits) < n:
        return None
    candidate = digits[-n:]
    try:
        return datetime.strptime(candidate, fmt)
    except Exception:
        return None

def _iter_month_starts(dt0: datetime, dt1: datetime):
    cur = datetime(dt0.year, dt0.month, 1)
    end = datetime(dt1.year, dt1.month, 1)
    while cur <= end:
        yield cur
        # advance one month
        if cur.month == 12:
            cur = datetime(cur.year + 1, 1, 1)
        else:
            cur = datetime(cur.year, cur.month + 1, 1)

def list_maps_available_runs(run_cfg: Dict[str, Any], history_days: int) -> List[datetime]:
    now = datetime.now()

    # Optional debug date (same idea as home())
    dbg = (CONFIG.get("maps_debug_today") or "").strip()
    if dbg:
        try:
            now = datetime.strptime(dbg, "%Y-%m-%d")
        except Exception:
            pass

    start = now - timedelta(days=max(0, int(history_days)))

    path_template = run_cfg.get("path_template")
    fmt_path = run_cfg.get("format_of_date_path")
    file_glob = run_cfg.get("file_glob")
    fmt_file = run_cfg.get("format_of_date_file")

    if not (path_template and fmt_path and file_glob and fmt_file):
        return []

    runs: List[datetime] = []

    # Scan each day folder (this matches %Y/%m/%d layouts)
    for dd in range(max(0, int(history_days)) + 1):
        day = now - timedelta(days=dd)
        date_path = day.strftime(fmt_path)
        folder = str(path_template).replace("{date_path}", date_path)

        for fp in glob(str(Path(folder) / file_glob)):
            dt = _parse_dt_from_filename(fp, fmt_file)
            if dt is None:
                continue
            if start <= dt <= now:
                runs.append(dt)

    # unique + sort desc
    return sorted(set(runs), reverse=True)


def pick_maps_selected_run(available: List[datetime]) -> datetime:
    # 1) query param ?maps_run=YYYY-mm-ddTHH:MM
    q = request.args.get("maps_run")
    if q:
        for fmt in ("%Y-%m-%dT%H:%M", "%Y-%m-%dT%H:%M:%S"):
            try:
                return datetime.strptime(q, fmt)
            except Exception:
                pass
    # 2) config debug date (maps_debug_today) as YYYY-mm-dd (uses 00:00)
    dbg = (CONFIG.get("maps_debug_today") or "").strip()
    if dbg:
        try:
            return datetime.strptime(dbg, "%Y-%m-%d")
        except Exception:
            pass
    # 3) fallback
    return available[0] if available else datetime.now()

def find_maps_file(run_cfg: Dict[str, Any], run_dt: datetime) -> Optional[str]:
    path_template = run_cfg.get("path_template")
    fmt_path = run_cfg.get("format_of_date_path")
    file_glob = run_cfg.get("file_glob")
    fmt_file = run_cfg.get("format_of_date_file")
    if not (path_template and fmt_path and file_glob and fmt_file):
        return None

    # Search inside the folder identified by dt's date_path
    folder = str(path_template).replace("{date_path}", run_dt.strftime(fmt_path))
    candidates = glob(str(Path(folder) / file_glob))
    if not candidates:
        return None

    # try exact match
    for fp in candidates:
        dt = _parse_dt_from_filename(fp, fmt_file)
        if dt and dt == run_dt:
            return fp

    # fallback: nearest available in that folder
    best_fp = None
    best_delta = None
    for fp in candidates:
        dt = _parse_dt_from_filename(fp, fmt_file)
        if not dt:
            continue
        delta = abs((dt - run_dt).total_seconds())
        if best_delta is None or delta < best_delta:
            best_delta = delta
            best_fp = fp
    return best_fp

# -----------------------------------------------------------------------------
# Small selection helpers
# -----------------------------------------------------------------------------
def get_selected_basin() -> str:
    basin = request.args.get("basin", DEFAULT_BASIN)
    if basin not in BASINS and BASINS:
        basin = BASINS[0]
    return basin


def get_selected_section(section_names: List[str]) -> Optional[str]:
    if not section_names:
        return None
    default = "all_sections" if "all_sections" in section_names else section_names[0]
    section = request.args.get("section", default)
    return section if section in section_names else default


# -----------------------------------------------------------------------------
# Routes: main views
# -----------------------------------------------------------------------------
@app.route("/")
@app.route("/home")
def home():
    import glob
    from datetime import datetime, timedelta

    # -----------------------------
    # Helpers (local, safe)
    # -----------------------------
    def _safe_fmt(template: str, **kwargs) -> str:
        """Format template with kwargs; ignore missing keys."""
        if not template:
            return ""
        try:
            return template.format(**kwargs)
        except KeyError:
            return template

    def _iter_days_back(today_dt: datetime, n_days: int):
        for dd in range(max(0, int(n_days)) + 1):
            yield today_dt - timedelta(days=dd)

    # -----------------------------
    # TS selection (unchanged)
    # -----------------------------
    selected_basin = get_selected_basin()

    ts_selected_run, ts_available_runs = get_selected_or_latest_run_for_basin(
        selected_basin, TS_HISTORY_DAYS
    )
    ts_selected_run_iso = ts_selected_run.strftime("%Y-%m-%dT%H:%M")

    ts_basin_cards = get_basin_cards(BASINS, TS_HISTORY_DAYS)

    selected_run_cfg_key, ts_run_cfg = get_current_run_config()
    run_configs = RUNS

    active_run_key = ACTIVE_RUN_KEY
    active_run_name = ACTIVE_RUN.get("name") if ACTIVE_RUN else None

    # ---- TS debug print: where we search (folder+files) ----
    ts_date_path_fmt = (ts_run_cfg or {}).get("format_of_date_path")
    ts_glob_pat = (ts_run_cfg or {}).get("file_glob")
    ts_path_tmpl = (ts_run_cfg or {}).get("path_template", "")

    ts_date_path = ts_selected_run.strftime(ts_date_path_fmt) if ts_date_path_fmt else ""
    ts_folder = _safe_fmt(ts_path_tmpl, date_path=ts_date_path, date=ts_date_path, basin=selected_basin)
    ts_search_glob = os.path.join(ts_folder, ts_glob_pat) if ts_folder and ts_glob_pat else ""
    ts_found_files = sorted(glob.glob(ts_search_glob)) if ts_search_glob else []

    print("\n[HOME] TIME-SERIES SEARCH (info)")
    print(f"  basin            = {selected_basin!r}")
    print(f"  run_cfg_key      = {selected_run_cfg_key!r}")
    print(f"  selected_run_iso = {ts_selected_run_iso!r}")
    print(f"  path_template    = {ts_path_tmpl!r}")
    print(f"  date_path_fmt    = {ts_date_path_fmt!r}")
    print(f"  resolved_folder  = {ts_folder!r}")
    print(f"  file_glob        = {ts_glob_pat!r}")
    print(f"  search_glob      = {ts_search_glob!r}")
    print(f"  n_files_matched  = {len(ts_found_files)}")
    for fp in ts_found_files[:5]:
        print(f"    - {fp}")

    # -----------------------------
    # MAPS selection (grid-only)
    # -----------------------------
    maps_runs = (CONFIG.get("runs") or {}).get("run_maps") or {}
    maps_selected_run_cfg_key = request.args.get("maps_run_cfg") or ""
    maps_selected_run_ui = request.args.get("maps_run") or ""   # now UI fmt 'YYYY-MM-DD HH:00'

    maps_available_runs = []  # list[str] in UI fmt
    maps_run_cfg = None

    if maps_runs:
        # pick cfg key (fallback to first key)
        if maps_selected_run_cfg_key not in maps_runs:
            maps_selected_run_cfg_key = next(iter(maps_runs.keys()))
        maps_run_cfg = maps_runs[maps_selected_run_cfg_key]

        maps_path_tmpl = (maps_run_cfg or {}).get("path_template", "")
        maps_date_path_fmt = (maps_run_cfg or {}).get("format_of_date_path")
        maps_file_glob = (maps_run_cfg or {}).get("file_glob")
        maps_file_date_fmt = (maps_run_cfg or {}).get("format_of_date_file")

        # decide "today" reference for scanning
        dbg = CONFIG.get("maps_debug_today")
        if isinstance(dbg, str):
            try:
                today_ref = datetime.strptime(dbg, "%Y-%m-%d")
            except Exception:
                today_ref = datetime.now()
        else:
            today_ref = datetime.now()

        # scan folders for last N days
        folder_set = set()
        for d in _iter_days_back(today_ref, CONFIG.get("maps_history_days", 3)):
            date_path = d.strftime(maps_date_path_fmt) if maps_date_path_fmt else ""
            folder = _safe_fmt(maps_path_tmpl, date_path=date_path, date=date_path)
            if folder:
                folder_set.add(folder)
        folder_list = sorted(folder_set)

        # gather files (tif OR nc OR nc.gz)
        all_files = []
        for folder in folder_list:
            pat = os.path.join(folder, maps_file_glob) if maps_file_glob else ""
            if pat:
                all_files.extend(glob.glob(pat))
        all_files = sorted(set(all_files))

        # parse run datetimes from filename using existing helper
        parsed_runs = []
        if maps_file_date_fmt:
            for fp in all_files:
                dt = _parse_dt_from_filename(fp, maps_file_date_fmt)
                if dt is not None:
                    parsed_runs.append(dt)

        parsed_runs = sorted(set(parsed_runs))
        maps_available_runs = [d.strftime(UI_DT_FMT) for d in parsed_runs]

        # choose selected maps run
        if maps_selected_run_ui not in maps_available_runs:
            maps_selected_run_ui = maps_available_runs[-1] if maps_available_runs else ""

        # ---- MAPS debug print ----
        print("\n[HOME] MAPS SEARCH (info)")
        print(f"  maps_run_cfg_key  = {maps_selected_run_cfg_key!r}")
        print(f"  selected_run_ui   = {maps_selected_run_ui!r}")
        print(f"  path_template     = {maps_path_tmpl!r}")
        print(f"  date_path_fmt     = {maps_date_path_fmt!r}")
        print(f"  file_glob         = {maps_file_glob!r}")
        print(f"  file_date_fmt     = {maps_file_date_fmt!r}")
        print(f"  history_days      = {CONFIG.get('maps_history_days', 3)!r}")
        print(f"  debug_today       = {CONFIG.get('maps_debug_today')!r}")
        print(f"  n_folders_checked = {len(folder_list)}")
        for f in folder_list[:5]:
            print(f"    - folder: {f}")
        print(f"  n_files_matched   = {len(all_files)}")
        for fp in all_files[:5]:
            print(f"    - {fp}")
        print(f"  n_runs_parsed     = {len(maps_available_runs)}")

    # -----------------------------
    # Render
    # -----------------------------
    return render_template(
        "home.html",
        basins=BASINS,
        selected_basin=selected_basin,
        selected_run_iso=ts_selected_run_iso,
        available_runs=ts_available_runs,
        basin_cards=ts_basin_cards,
        ts_history_days=TS_HISTORY_DAYS,
        history_days=TS_HISTORY_DAYS,
        SECTIONS_ALL=SECTIONS_ALL,
        run_configs=run_configs,
        selected_run_cfg_key=selected_run_cfg_key,
        active_run_key=active_run_key,
        active_run_name=active_run_name,
        dem_meta=DEM_META,

        # maps selectors
        maps_run_configs=maps_runs,
        maps_selected_run_cfg_key=maps_selected_run_cfg_key,
        maps_available_runs=maps_available_runs,
        maps_selected_run_iso=maps_selected_run_ui,  # keep name for template compatibility
    )


@app.route("/time_series_table")  # backward-compatible alias
@app.route("/ts_det_table", endpoint="ts_det_table_view")
def ts_det_table_view():
    selected_basin = get_selected_basin()

    ts_selected_run, ts_available_runs = get_selected_or_latest_run_for_basin(
        selected_basin, TS_HISTORY_DAYS
    )
    ts_selected_run_iso = ts_selected_run.strftime("%Y-%m-%dT%H:%M")

    ts_sections, ts_json_paths = load_timeseries_sections(ts_selected_run, selected_basin)
    ts_section_tables: Dict[str, Any] = {}
    ts_section_meta: Dict[str, Any] = {}

    _, ts_run_cfg = get_current_run_config()
    for sec_name, path_str in ts_json_paths.items():
        try:
            json_file = Path(path_str)
            table, meta = build_section_table_from_json(json_file, ts_run_cfg)
            ts_section_tables[sec_name] = table
            ts_section_meta[sec_name] = meta
        except Exception as e:
            print(f"[WARNING] failed to build table for {sec_name} from {path_str}: {e}")
            continue

    selected_run_cfg_key, _ = get_current_run_config()
    ts_run_configs = _filter_run_configs_by_type(RUNS, ["ts_deterministic"])

    return render_template(
        "ts_det_table_view.html",
        basins=BASINS,
        selected_basin=selected_basin,
        selected_run_iso=ts_selected_run_iso,
        sections=ts_sections,
        json_paths=ts_json_paths,
        available_runs=ts_available_runs,
        run_configs=ts_run_configs,
        selected_run_cfg_key=selected_run_cfg_key,
        active_run_key=ACTIVE_RUN_KEY,
        section_tables=ts_section_tables,
        section_meta=ts_section_meta,
        ts_history_days=TS_HISTORY_DAYS,
        history_days=TS_HISTORY_DAYS,
    )


@app.route("/time_series_chart")  # backward-compatible alias
@app.route("/ts_det_chart", endpoint="ts_det_chart_view")
def ts_det_chart_view():
    selected_basin = get_selected_basin()

    ts_selected_run, ts_available_runs = get_selected_or_latest_run_for_basin(
        selected_basin, TS_HISTORY_DAYS
    )
    ts_selected_run_iso = ts_selected_run.strftime("%Y-%m-%dT%H:%M")

    # Load sections → dict + json paths
    ts_sections_dict, ts_json_paths = load_timeseries_sections(ts_selected_run, selected_basin)
    ts_section_names = sorted(ts_sections_dict.keys())
    ts_section_options = ["all_sections"] + ts_section_names if ts_section_names else []
    ts_selected_section = get_selected_section(ts_section_options)

    ts_multi_mode = ts_selected_section == "all_sections"

    # ------------------------------------------------------------------
    # Parse each hydrograph_*.json into arrays for Plotly
    # ------------------------------------------------------------------
    def parse_section_json(json_path: str, section_key: str) -> Dict[str, Any]:
        if not json_path or not os.path.exists(json_path):
            return {
                "time_labels": [],
                "q_sim": [],
                "q_obs": [],
                "rain": [],
                "sm": [],
                "thr_alert": None,
                "thr_alarm": None,
                "time_now": ts_sections_dict.get(section_key, {}).get("time_now"),
                "json_path": json_path,
            }

        with open(json_path, "r") as fp:
            payload = json.load(fp)

        raw_times = payload.get("time_period", "")
        time_labels = [t.strip() for t in raw_times.split(",") if t.strip()]

        def parse_series(key: str) -> List[Optional[float]]:
            raw = payload.get(key)
            if not raw:
                return [None] * len(time_labels)

            vals: List[Optional[float]] = []
            for s in raw.split(","):
                s = s.strip()
                if not s:
                    vals.append(None)
                    continue
                try:
                    v = float(s)
                except ValueError:
                    vals.append(None)
                    continue
                # sentinel / missing values
                vals.append(None if v <= -9990 else v)

            if len(vals) < len(time_labels):
                vals += [None] * (len(time_labels) - len(vals))
            elif len(vals) > len(time_labels):
                vals = vals[: len(time_labels)]
            return vals

        q_sim = parse_series("time_series_discharge_simulated")
        q_obs = parse_series("time_series_discharge_observed")
        rain = parse_series("time_series_rain_observed")
        sm = parse_series("time_series_soil_moisture_simulated")

        try:
            thr_alert = float(payload.get("section_discharge_thr_alert"))
        except (TypeError, ValueError):
            thr_alert = None

        try:
            thr_alarm = float(payload.get("section_discharge_thr_alarm"))
        except (TypeError, ValueError):
            thr_alarm = None

        time_now = ts_sections_dict.get(section_key, {}).get("time_now")

        return {
            "time_labels": time_labels,
            "q_sim": q_sim,
            "q_obs": q_obs,
            "rain": rain,
            "sm": sm,
            "thr_alert": thr_alert,
            "thr_alarm": thr_alarm,
            "time_now": time_now,
            "json_path": json_path,
        }

    ts_charts_data: Dict[str, Dict[str, Any]] = {}
    for sec_name, path_str in ts_json_paths.items():
        p = Path(path_str)
        if not p.is_absolute():
            p = Path(CONFIG.get("dynamic_root", "data/dynamic")).parent / p
        ts_charts_data[sec_name] = parse_section_json(str(p), sec_name)

    # Convenience arrays for single-section mode
    ts_time: List[str] = []
    ts_q_sim: List[Optional[float]] = []
    ts_q_obs: List[Optional[float]] = []
    ts_rain: List[Optional[float]] = []
    ts_sm: List[Optional[float]] = []
    thr_alert: Optional[float] = None
    thr_alarm: Optional[float] = None
    time_now: Optional[str] = None
    json_path_selected: Optional[str] = None

    if not ts_multi_mode and ts_selected_section in ts_charts_data:
        cd = ts_charts_data[ts_selected_section]
        ts_time = cd["time_labels"]
        ts_q_sim = cd["q_sim"]
        ts_q_obs = cd["q_obs"]
        ts_rain = cd["rain"]
        ts_sm = cd["sm"]
        thr_alert = cd["thr_alert"]
        thr_alarm = cd["thr_alarm"]
        time_now = cd["time_now"]
        json_path_selected = cd["json_path"]

    selected_run_cfg_key, _ = get_current_run_config()
    ts_run_configs = _filter_run_configs_by_type(RUNS, ["ts_deterministic"])

    return render_template(
        "ts_det_chart_view.html",
        basins=BASINS,
        selected_basin=selected_basin,
        selected_run_iso=ts_selected_run_iso,
        sections=ts_section_options,
        selected_section=ts_selected_section,
        available_runs=ts_available_runs,
        run_configs=ts_run_configs,
        selected_run_cfg_key=selected_run_cfg_key,
        active_run_key=ACTIVE_RUN_KEY,
        charts_data=ts_charts_data,
        ts_time=ts_time,
        ts_q_sim=ts_q_sim,
        ts_q_obs=ts_q_obs,
        ts_rain=ts_rain,
        ts_sm=ts_sm,
        thr_alert=thr_alert,
        thr_alarm=thr_alarm,
        time_now=time_now,
        json_path=json_path_selected,
        ts_history_days=TS_HISTORY_DAYS,
        history_days=TS_HISTORY_DAYS,
    )


@app.route("/geo_sections")
def geo_sections_view():
    selected_basin = request.args.get("basin", "all")

    if selected_basin == "all":
        sections = SECTIONS_ALL
    else:
        sections = [s for s in SECTIONS_ALL if s["basin"] == selected_basin]

    return render_template(
        "geo_sections.html",
        sections=sections,
        basins=BASINS,
        selected_basin=selected_basin,
        dem_meta=DEM_META,
    )


@app.route("/geo_layers")
def geo_layers_view():
    selected_basin = request.args.get("basin", "all")

    if selected_basin == "all":
        sections = SECTIONS_ALL
    else:
        sections = [s for s in SECTIONS_ALL if s["basin"] == selected_basin]

    return render_template(
        "geo_layers.html",
        basins=BASINS,
        selected_basin=selected_basin,
        sections=sections,
        dem_meta=DEM_META,
    )


@app.route("/maps_view")
def maps_view():
    selected_basin = request.args.get("basin", "all")

    maps_run_configs = _get_maps_run_configs()
    maps_selected_run_cfg_key = request.args.get("maps_run_cfg")

    # pick cfg key
    if not maps_selected_run_cfg_key:
        active_maps = CONFIG.get("active_maps") or []
        maps_selected_run_cfg_key = active_maps[0] if active_maps else None
    if not maps_selected_run_cfg_key or maps_selected_run_cfg_key not in maps_run_configs:
        maps_selected_run_cfg_key = next(iter(maps_run_configs.keys()), None)

    requested_run_str = request.args.get("maps_run") or ""
    rcfg = maps_run_configs.get(maps_selected_run_cfg_key) or {}

    # variables (NetCDF only, but harmless for GeoTIFF)
    v = rcfg.get("variable_name") or []
    maps_variables = [v] if isinstance(v, str) else list(v)
    maps_selected_var = request.args.get("maps_var") or ""
    if not maps_selected_var and maps_variables:
        maps_selected_var = maps_variables[0]

    # available runs
    maps_available_runs_dt = list_maps_available_runs(rcfg, MAPS_HISTORY_DAYS)
    maps_available_runs = [d.strftime(UI_DT_FMT) for d in maps_available_runs_dt]

    requested_dt = parse_ui_datetime(requested_run_str)
    if requested_dt and requested_dt in maps_available_runs_dt:
        maps_selected_run_dt = requested_dt
    else:
        maps_selected_run_dt = maps_available_runs_dt[0] if maps_available_runs_dt else None

    maps_selected_run_iso = maps_selected_run_dt.strftime(UI_DT_FMT) if maps_selected_run_dt else ""

    # Selected file on disk (GeoTIFF or NetCDF depending on config)
    fp = find_maps_file(rcfg, maps_selected_run_dt) if maps_selected_run_dt else None

    # Determine run ISO param and whether config is NetCDF-like
    run_iso_param = maps_selected_run_dt.strftime("%Y-%m-%dT%H:%M") if maps_selected_run_dt else None
    file_glob = (rcfg.get("file_glob") or "").lower()
    is_netcdf = (".nc" in file_glob) or (fp or "").lower().endswith((".nc", ".nc.gz", ".gz"))

    # Build URLs for the template
    maps_tif_url = None
    maps_png_url = None

    if maps_selected_run_cfg_key and run_iso_param:
        if is_netcdf:
            # NetCDF -> GeoTIFF endpoint (needs var)
            maps_tif_url = url_for(
                "maps_nc_tif",
                maps_run_cfg=maps_selected_run_cfg_key,
                maps_run=run_iso_param,
                var=maps_selected_var,
            )
            # NetCDF -> PNG endpoint (needs var; your /maps/tif_png supports this)
            maps_png_url = url_for(
                "maps_tif_png",
                maps_run_cfg=maps_selected_run_cfg_key,
                maps_run=run_iso_param,
                var=maps_selected_var,
            )
        else:
            # GeoTIFF direct
            maps_tif_url = url_for(
                "maps_tif",
                maps_run_cfg=maps_selected_run_cfg_key,
                maps_run=run_iso_param,
            )
            # GeoTIFF -> PNG
            maps_png_url = url_for(
                "maps_tif_png",
                maps_run_cfg=maps_selected_run_cfg_key,
                maps_run=run_iso_param,
            )

    # Compute extent in EPSG:4326 for ImageStatic
    maps_png_extent = None
    if fp and os.path.exists(fp):
        if is_netcdf:
            try:
                import xarray as xr
                import gzip, tempfile

                if fp.lower().endswith(".gz"):
                    with gzip.open(fp, "rb") as f_in:
                        raw = f_in.read()
                    with tempfile.NamedTemporaryFile(suffix=".nc", delete=True) as tmp:
                        tmp.write(raw)
                        tmp.flush()
                        ds = xr.open_dataset(tmp.name, decode_times=False)
                else:
                    ds = xr.open_dataset(fp, decode_times=False)

                # HMC grid georef from attrs (reference = forcing grid itself)
                x0 = float(ds.attrs["xllcorner"])
                y0 = float(ds.attrs["yllcorner"])
                cs = float(ds.attrs["cellsize"])
                ncols = int(ds.attrs["ncols"])
                nrows = int(ds.attrs["nrows"])

                maps_png_extent = [x0, y0, x0 + cs * ncols, y0 + cs * nrows]

            except Exception as e:
                app.logger.exception("[MAPS] NetCDF extent failed for %s: %s", fp, e)
                maps_png_extent = None
        else:
            # GeoTIFF: most of your rasters are WGS84 lon/lat; use bounds directly
            try:
                import rasterio
                with rasterio.open(fp) as ds:
                    b = ds.bounds
                    maps_png_extent = [b.left, b.bottom, b.right, b.top]
            except Exception as e:
                app.logger.exception("[MAPS] GeoTIFF extent failed for %s: %s", fp, e)
                maps_png_extent = None

    # Sections overlay
    if selected_basin == "all":
        sections = SECTIONS_ALL
    else:
        sections = [s for s in SECTIONS_ALL if s.get("basin") == selected_basin]

    return render_template(
        "maps_view.html",
        basins=BASINS,
        selected_basin=selected_basin,
        dem_meta=DEM_META,
        sections=sections,
        maps_run_configs=maps_run_configs,
        maps_selected_run_cfg_key=maps_selected_run_cfg_key,
        maps_available_runs=maps_available_runs,
        maps_selected_run_iso=maps_selected_run_iso,
        maps_selected_file=fp,          # keep this name for template/debug
        maps_tif_url=maps_tif_url,
        maps_png_url=maps_png_url,
        maps_png_extent=maps_png_extent,
        maps_history_days=MAPS_HISTORY_DAYS,
        maps_variables=maps_variables,
        maps_selected_var=maps_selected_var,
    )

# -----------------------------------------------------------------------------
# Dynamic maps: serve selected GeoTIFF for OpenLayers
# -----------------------------------------------------------------------------
@app.route("/maps/tif")
def maps_tif():
    run_cfg_key = request.args.get("maps_run_cfg")
    run_iso = request.args.get("maps_run")
    if not run_cfg_key or not run_iso:
        abort(400, "Missing maps_run_cfg or maps_run")

    maps_run_configs = _get_maps_run_configs()
    rcfg = maps_run_configs.get(run_cfg_key)
    if not rcfg:
        abort(404, "Unknown maps_run_cfg")

    try:
        run_dt = datetime.strptime(run_iso, "%Y-%m-%dT%H:%M")
    except Exception:
        abort(400, "Invalid maps_run datetime")

    fp = find_maps_file(rcfg, run_dt)
    if not fp or not os.path.exists(fp):
        abort(404, "GeoTIFF not found for selected run")

    return send_file(fp, mimetype="image/tiff")


# -----------------------------------------------------------------------------
# Dynamic maps: GeoTIFF -> PNG on-the-fly (for ImageStatic overlay in OpenLayers)
# -----------------------------------------------------------------------------
@app.route("/maps/tif_png")
def maps_tif_png():
    run_cfg_key = request.args.get("maps_run_cfg")
    run_iso = request.args.get("maps_run")
    var = (request.args.get("var") or "").strip()  # needed for NetCDF

    if not run_cfg_key or not run_iso:
        abort(400, "Missing maps_run_cfg or maps_run")

    maps_run_configs = _get_maps_run_configs()
    rcfg = maps_run_configs.get(run_cfg_key)
    if not rcfg:
        abort(404, "Unknown maps_run_cfg")

    try:
        run_dt = datetime.strptime(run_iso, "%Y-%m-%dT%H:%M")
    except Exception:
        abort(400, "Invalid maps_run datetime")

    fp = find_maps_file(rcfg, run_dt)
    if not fp or not os.path.exists(fp):
        abort(404, "File not found for selected run")

    # Decide if this config is NetCDF-like
    file_glob = (rcfg.get("file_glob") or "").lower()
    is_netcdf = (".nc" in file_glob) or fp.lower().endswith((".nc", ".nc.gz", ".gz"))

    # Common deps for PNG
    try:
        import numpy as np
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import io
    except Exception as e:
        abort(500, f"Missing dependency to render PNG: {e}")

    if is_netcdf:
        if not var:
            abort(400, "Missing var (required for NetCDF PNG rendering)")

        # Open NetCDF (support .nc.gz) – same approach as maps_nc_tif
        try:
            import xarray as xr
            import gzip, tempfile

            if fp.lower().endswith(".gz"):
                with gzip.open(fp, "rb") as f_in:
                    raw = f_in.read()
                with tempfile.NamedTemporaryFile(suffix=".nc", delete=True) as tmp:
                    tmp.write(raw)
                    tmp.flush()
                    ds = xr.open_dataset(tmp.name, decode_times=False)
            else:
                ds = xr.open_dataset(fp, decode_times=False)
        except Exception as e:
            abort(500, f"Unable to open NetCDF: {e}")

        alias = {"Precipitation": "Rain", "AirT": "AirTemperature", "WindSpeed": "Wind"}
        var_name = var if var in ds.data_vars else alias.get(var, var)
        if var_name not in ds.data_vars:
            available = ", ".join(sorted(list(ds.data_vars.keys())))
            abort(404, f"Variable not found in NetCDF: {var}. Available: {available}")

        da = ds[var_name]
        if "time" in da.dims:
            try:
                da = da.isel(time=0)
            except Exception:
                pass

        arr = da.values
        if arr.ndim != 2:
            abort(500, f"Variable {var} is not 2D (shape={arr.shape})")

        arr = arr.astype("float32", copy=False)
        arr[~np.isfinite(arr)] = np.nan

        # Robust stretch
        if np.all(np.isnan(arr)):
            vmin, vmax = 0.0, 1.0
        else:
            vmin = float(np.nanpercentile(arr, 2))
            vmax = float(np.nanpercentile(arr, 98))
            if not np.isfinite(vmin) or not np.isfinite(vmax) or vmin == vmax:
                vmin = float(np.nanmin(arr))
                vmax = float(np.nanmax(arr)) if float(np.nanmax(arr)) != vmin else vmin + 1.0
        arr = np.clip(arr, vmin, vmax)

        # Build PNG
        buf = io.BytesIO()
        fig = plt.figure(figsize=(arr.shape[1] / 100.0, arr.shape[0] / 100.0), dpi=100)
        ax = plt.axes([0, 0, 1, 1])
        ax.axis("off")
        ax.imshow(arr, origin="lower")
        fig.savefig(buf, format="png", transparent=True)
        plt.close(fig)
        buf.seek(0)

        resp = send_file(buf, mimetype="image/png")
        resp.headers["Access-Control-Allow-Origin"] = "*"
        return resp

    # --- GeoTIFF path (your existing logic) ---
    try:
        import rasterio
    except Exception as e:
        abort(500, f"Missing rasterio dependency: {e}")

    with rasterio.open(fp) as ds:
        arr = ds.read(1).astype("float32")
        nodata = ds.nodata
        if nodata is not None:
            arr[arr == nodata] = np.nan

        if np.all(np.isnan(arr)):
            vmin, vmax = 0.0, 1.0
        else:
            vmin = float(np.nanpercentile(arr, 2))
            vmax = float(np.nanpercentile(arr, 98))
            if not np.isfinite(vmin) or not np.isfinite(vmax) or vmin == vmax:
                vmin = float(np.nanmin(arr))
                vmax = float(np.nanmax(arr)) if float(np.nanmax(arr)) != vmin else vmin + 1.0

        arr = np.clip(arr, vmin, vmax)

        buf = io.BytesIO()
        fig = plt.figure(figsize=(arr.shape[1] / 100.0, arr.shape[0] / 100.0), dpi=100)
        ax = plt.axes([0, 0, 1, 1])
        ax.axis("off")
        ax.imshow(arr)
        fig.savefig(buf, format="png", transparent=True)
        plt.close(fig)
        buf.seek(0)

    resp = send_file(buf, mimetype="image/png")
    resp.headers["Access-Control-Allow-Origin"] = "*"
    return resp



# -----------------------------------------------------------------------------
# Dynamic maps: NetCDF forcing -> GeoTIFF on-the-fly (for OpenLayers)
# -----------------------------------------------------------------------------
@app.route("/maps/nc_tif")
def maps_nc_tif():
    run_cfg_key = request.args.get("maps_run_cfg")
    run_iso = request.args.get("maps_run")
    var = (request.args.get("var") or "").strip()

    if not run_cfg_key or not run_iso:
        abort(400, "Missing maps_run_cfg or maps_run")
    if not var:
        abort(400, "Missing var")

    maps_run_configs = _get_maps_run_configs()
    rcfg = maps_run_configs.get(run_cfg_key)
    if not rcfg:
        abort(404, "Unknown maps_run_cfg")

    try:
        run_dt = datetime.strptime(run_iso, "%Y-%m-%dT%H:%M")
    except Exception:
        abort(400, "Invalid maps_run datetime")

    fp = find_maps_file(rcfg, run_dt)
    if not fp or not os.path.exists(fp):
        abort(404, "NetCDF not found for selected run")

    # Open NetCDF (support .nc.gz)
    try:
        if fp.lower().endswith(".gz"):
            with gzip.open(fp, "rb") as f_in:
                raw = f_in.read()
            with tempfile.NamedTemporaryFile(suffix=".nc", delete=True) as tmp:
                tmp.write(raw)
                tmp.flush()
                import xarray as xr
                ds = xr.open_dataset(tmp.name, decode_times=False)
        else:
            import xarray as xr
            ds = xr.open_dataset(fp, decode_times=False)
    except Exception as e:
        abort(500, f"Unable to open NetCDF: {e}")

    # Simple variable aliases (config may differ from file variable names)
    alias = {"Precipitation": "Rain", "AirT": "AirTemperature", "WindSpeed": "Wind"}
    var_name = var if var in ds.data_vars else alias.get(var, var)
    if var_name not in ds.data_vars:
        available = ", ".join(sorted(list(ds.data_vars.keys())))
        abort(404, f"Variable not found in NetCDF: {var}. Available: {available}")

    da = ds[var_name]
    # If variable has time, take first timestep
    if "time" in da.dims:
        try:
            da = da.isel(time=0)
        except Exception:
            pass

    arr = da.values
    if arr.ndim != 2:
        abort(500, f"Variable {var} is not 2D (shape={arr.shape})")

    # Compute an approximate georeferencing from lon/lat arrays (support many conventions)
    xmin = ymin = xmax = ymax = None

    def _pick_var(ds, names):
        for n in names:
            if n in ds.variables:
                return ds[n]
            if n in ds.coords:
                return ds.coords[n]
        return None

    lon_da = _pick_var(ds, ["longitude", "lon", "LONGITUDE", "LON", "x", "X", "XLONG", "nav_lon"])
    lat_da = _pick_var(ds, ["latitude", "lat", "LATITUDE", "LAT", "y", "Y", "XLAT", "nav_lat"])

    # Fallback: try coords attached to the selected dataarray
    if lon_da is None:
        for n in ["longitude", "lon", "x", "XLONG"]:
            if n in da.coords:
                lon_da = da.coords[n]
                break
    if lat_da is None:
        for n in ["latitude", "lat", "y", "XLAT"]:
            if n in da.coords:
                lat_da = da.coords[n]
                break

    try:
        if lon_da is not None and lat_da is not None:
            lon = lon_da.values
            lat = lat_da.values
            xmin = float(lon.min())
            xmax = float(lon.max())
            ymin = float(lat.min())
            ymax = float(lat.max())
    except Exception:
        pass

    if xmin is None or ymin is None or xmax is None or ymax is None:
        available = ", ".join(sorted(list(ds.variables.keys())))
        abort(500, f"Missing lon/lat variables or coords. Available variables: {available}")

    ny, nx = arr.shape
    xres = (xmax - xmin) / float(nx)
    yres = (ymax - ymin) / float(ny)

    # Build GeoTIFF in memory (EPSG:4326)
    try:
        import rasterio
        from rasterio.io import MemoryFile
        from rasterio.transform import from_origin
    except Exception as e:
        abort(500, f"Missing rasterio dependency: {e}")

    transform = from_origin(xmin, ymax, xres, yres)

    # Ensure float32 (OpenLayers handles it well)
    data = arr.astype("float32", copy=False)
    
    if not np.isfinite(data).any():
        abort(500, f"Selected variable {var} contains no finite values for this timestep")

    with MemoryFile() as memfile:
        with memfile.open(
            driver="GTiff",
            height=ny,
            width=nx,
            count=1,
            dtype="float32",
            crs="EPSG:4326",
            transform=transform,
        ) as dst:
            dst.write(data, 1)

        buf = io.BytesIO(memfile.read())

    fn = f"{run_cfg_key}_{run_iso}_{var}.tif".replace(":", "").replace("/", "_")
    return send_file(buf, mimetype="image/tiff", as_attachment=False, download_name=fn)


@app.route("/maps/legend_png")
def maps_legend_png():
    var = (request.args.get("var") or "").strip()
    if not var:
        abort(400, "Missing var")

    picked = pick_palette(var)
    if not picked:
        abort(404, f"No palette configured for var={var}")

    cmap_file, vmin, vmax, units = picked
    cmap = load_listed_cmap(cmap_file)

    import io
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.cm import ScalarMappable
    from matplotlib.colors import Normalize

    buf = io.BytesIO()
    fig = plt.figure(figsize=(2.2, 0.35), dpi=200)
    ax = plt.axes([0.05, 0.45, 0.9, 0.35])
    norm = Normalize(vmin=vmin, vmax=vmax)
    sm = ScalarMappable(norm=norm, cmap=cmap)
    cb = plt.colorbar(sm, cax=ax, orientation="horizontal")
    cb.set_label(f"{var} {units}".strip(), fontsize=7)
    cb.ax.tick_params(labelsize=6)

    fig.savefig(buf, format="png", transparent=True)
    plt.close(fig)
    buf.seek(0)

    resp = send_file(buf, mimetype="image/png")
    resp.headers["Access-Control-Allow-Origin"] = "*"
    return resp


# -----------------------------------------------------------------------------
# Static geo layers: DEM PNG + river GeoJSON
# -----------------------------------------------------------------------------
@app.route("/geo_layers/dem.png")
def maps_dem_png():
    """Generate a terrain-colored PNG of the DEM ASCII grid."""
    if Image is None:
        abort(500, "Pillow not installed")
    if DEM_META is None or DEM_GRID is None:
        abort(404, "DEM not available")

    ncols = DEM_META["ncols"]
    nrows = DEM_META["nrows"]
    nodata = DEM_META["nodata"]

    vals = [v for row in DEM_GRID for v in row if v != nodata]
    if not vals:
        abort(404, "DEM has no valid data")

    vmin = min(vals)
    vmax = max(vals)
    if vmax <= vmin:
        vmax = vmin + 1.0

    def terrain_color(t: float):
        t = max(0.0, min(1.0, t))
        if t < 0.25:
            f = t / 0.25
            r = int(30 + f * (80 - 30))
            g = int(70 + f * (140 - 70))
            b = int(30 + f * (60 - 30))
        elif t < 0.50:
            f = (t - 0.25) / 0.25
            r = int(80 + f * (200 - 80))
            g = int(140 + f * (200 - 140))
            b = int(60 + f * (40 - 60))
        elif t < 0.75:
            f = (t - 0.50) / 0.25
            r = int(200 + f * (160 - 200))
            g = int(200 + f * (110 - 200))
            b = int(40 + f * (40 - 40))
        else:
            f = (t - 0.75) / 0.25
            r = int(160 + f * (240 - 160))
            g = int(110 + f * (240 - 110))
            b = int(40 + f * (240 - 40))
        return r, g, b

    img = Image.new("RGBA", (ncols, nrows))
    pixels = img.load()

    for r in range(nrows):
        for c in range(ncols):
            val = DEM_GRID[r][c]
            if val == nodata:
                pixels[c, r] = (0, 0, 0, 0)
            else:
                t = (val - vmin) / (vmax - vmin)
                rr, gg, bb = terrain_color(t)
                pixels[c, r] = (rr, gg, bb, 255)

    buf = io.BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)
    return send_file(buf, mimetype="image/png")


@app.route("/geo_layers/rivers.geojson")
def maps_rivers_geojson():
    """Build a GeoJSON FeatureCollection from the river mask ASCII grid (river=1)."""
    if CHOICE_META is None or CHOICE_GRID is None:
        return jsonify({"type": "FeatureCollection", "features": []})

    ncols = CHOICE_META["ncols"]
    nrows = CHOICE_META["nrows"]
    x0 = CHOICE_META["xllcorner"]
    y0 = CHOICE_META["yllcorner"]
    cs = CHOICE_META["cellsize"]
    nodata = CHOICE_META["nodata"]

    ymax = y0 + cs * nrows
    features = []

    for r in range(nrows):
        for c in range(ncols):
            v = CHOICE_GRID[r][c]
            if v == nodata:
                continue
            if abs(v - 1.0) > 1e-6:
                continue

            lon = x0 + cs * (c + 0.5)
            lat = ymax - cs * (r + 0.5)

            features.append(
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [lon, lat]},
                    "properties": {"value": v},
                }
            )

    return jsonify({"type": "FeatureCollection", "features": features})

@app.route("/debug/config_keys")
def debug_config_keys():
    keys = ["ts_debug_today", "maps_debug_today", "ts_history_days", "maps_history_days"]
    return jsonify({k: CONFIG.get(k) for k in keys})


if __name__ == "__main__":
    app.run(debug=True, port=0)
