from flask import Flask, render_template, request, jsonify, send_file, abort
from pathlib import Path
from datetime import datetime
from typing import List, Optional, Dict, Any
import json, os, re
import io

from utils_config import CONFIG, HISTORY_DAYS
from utils_sections import load_csv_basins, load_all_sections
from utils_runs import (
    RUNS,
    ACTIVE_RUN_KEY,
    ACTIVE_RUN,
    get_current_run_config,
    list_recent_available_runs_for_basin,
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
    """Return a short hint string for common JSON authoring mistakes (e.g., trailing commas)."""
    window = raw_text[max(0, err_pos - 80): err_pos + 80]
    # Trailing comma before } or ]
    if re.search(r",\s*[}\]]", window):
        return "Hint: looks like a trailing comma before a closing '}' or ']'. Remove the comma."
    # Single quotes instead of double quotes
    if "'" in window and '"' not in window:
        return "Hint: JSON requires double quotes for strings/keys (use \")."
    return "Hint: check commas, quotes, and brackets around the reported location."

def validate_json_file(json_path: str) -> Dict[str, Any]:
    """Validate a JSON file and return a structured report (ok/error + line/col + hint)."""
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
        report.update({
            "error": e.msg,
            "line": e.lineno,
            "col": e.colno,
            "pos": e.pos,
            "hint": _json_error_hints(raw, e.pos),
        })
        # include a small excerpt around the error
        start = max(0, e.pos - 120)
        end = min(len(raw), e.pos + 120)
        excerpt = raw[start:end]
        report["excerpt"] = excerpt
        return report

@app.route("/debug/validate_config")
def debug_validate_config():
    """Return JSON validation report for the main config file."""
    # Try to discover the config path from common env var, fallback to ./config.json
    cfg_path = os.environ.get("APP_CONFIG", "config.json")
    report = validate_json_file(cfg_path)
    return jsonify(report)


# Global data loaded at startup
BASINS = load_csv_basins()
SECTIONS_ALL = load_all_sections()
DEFAULT_BASIN = CONFIG.get("default_basin", BASINS[0] if BASINS else "")


# -----------------------------------------------------------------------------
# Run-config helpers (generic refactor)
# -----------------------------------------------------------------------------
def _filter_run_configs_by_type(run_configs: Dict[str, Any], allowed_types: List[str]) -> Dict[str, Any]:
    """Return only run configs whose config['type'] is in allowed_types."""
    out: Dict[str, Any] = {}
    for k, cfg in (run_configs or {}).items():
        if isinstance(cfg, dict) and cfg.get("type") in allowed_types:
            out[k] = cfg
    return out


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
    if section in section_names:
        return section
    return default


# -----------------------------------------------------------------------------
# Routes: main views
# -----------------------------------------------------------------------------
@app.route("/")
@app.route("/home")
def home():
    selected_basin = get_selected_basin()
    selected_run, available_runs = get_selected_or_latest_run_for_basin(
        selected_basin, HISTORY_DAYS
    )
    selected_run_iso = selected_run.strftime("%Y-%m-%dT%H:%M")

    basin_cards = get_basin_cards(BASINS, HISTORY_DAYS)

    selected_run_cfg_key, selected_run_cfg = get_current_run_config()
    run_configs = RUNS

    active_run_key = ACTIVE_RUN_KEY
    active_run_name = ACTIVE_RUN.get("name") if ACTIVE_RUN else None

    return render_template(
        "home.html",
        basins=BASINS,
        selected_basin=selected_basin,
        selected_run_iso=selected_run_iso,
        available_runs=available_runs,
        basin_cards=basin_cards,
        history_days=HISTORY_DAYS,
        SECTIONS_ALL=SECTIONS_ALL,
        run_configs=run_configs,
        selected_run_cfg_key=selected_run_cfg_key,
        active_run_key=active_run_key,
        active_run_name=active_run_name,
        dem_meta=DEM_META,        # 👈 ADD THIS
    )

@app.route("/time_series_table")  # backward-compatible alias
@app.route("/ts_det_table", endpoint="ts_det_table_view")
def ts_det_table_view():

    # this was table_view content
    selected_basin = get_selected_basin()
    selected_run, available_runs = get_selected_or_latest_run_for_basin(
        selected_basin, HISTORY_DAYS
    )
    selected_run_iso = selected_run.strftime("%Y-%m-%dT%H:%M")

    sections, json_paths = load_timeseries_sections(selected_run, selected_basin)
    section_tables: Dict[str, Any] = {}
    section_meta: Dict[str, Any] = {}

    run_cfg_key, run_cfg = get_current_run_config()
    for sec_name, path_str in json_paths.items():
        try:
            json_file = Path(path_str)
            table, meta = build_section_table_from_json(json_file, run_cfg)
            section_tables[sec_name] = table
            section_meta[sec_name] = meta
        except Exception as e:
            print(f"[WARNING] failed to build table for {sec_name} from {path_str}: {e}")
            continue

    selected_run_cfg_key, _ = get_current_run_config()
    # Deterministic TS view: show only deterministic TS run-configs
    run_configs = _filter_run_configs_by_type(RUNS, ["ts_deterministic"])

    return render_template(
        "ts_det_table_view.html",
        basins=BASINS,
        selected_basin=selected_basin,
        selected_run_iso=selected_run_iso,
        sections=sections,
        json_paths=json_paths,
        available_runs=available_runs,
        run_configs=run_configs,
        selected_run_cfg_key=selected_run_cfg_key,
        active_run_key=ACTIVE_RUN_KEY,
        section_tables=section_tables,
        section_meta=section_meta,
    )


@app.route("/time_series_chart")  # backward-compatible alias
@app.route("/ts_det_chart", endpoint="ts_det_chart_view")
def ts_det_chart_view():

    selected_basin = get_selected_basin()
    selected_run, available_runs = get_selected_or_latest_run_for_basin(
        selected_basin, HISTORY_DAYS
    )
    selected_run_iso = selected_run.strftime("%Y-%m-%dT%H:%M")

    # Load sections → dict + json paths
    sections_dict, json_paths = load_timeseries_sections(selected_run, selected_basin)
    section_names = sorted(sections_dict.keys())
    section_options = ["all_sections"] + section_names if section_names else []
    selected_section = get_selected_section(section_options)

    multi_mode = selected_section == "all_sections"

    # ------------------------------------------------------------------
    # Parse each hydrograph_*.json into arrays for Plotly
    # ------------------------------------------------------------------
    def parse_section_json(json_path: str, section_key: str) -> Dict[str, Any]:
        """
        Read one hydrograph JSON and return a dict with:
          - time_labels (raw strings, e.g. '2025-12-01 03:00')
          - q_sim, q_obs, rain, sm (lists with None for missing)
          - thr_alert, thr_alarm
          - time_now (from sections_dict if available)
          - json_path (absolute path string)
        """
        if not json_path or not os.path.exists(json_path):
            return {
                "time_labels": [],
                "q_sim": [],
                "q_obs": [],
                "rain": [],
                "sm": [],
                "thr_alert": None,
                "thr_alarm": None,
                "time_now": sections_dict.get(section_key, {}).get("time_now"),
                "json_path": json_path,
            }

        with open(json_path, "r") as fp:
            payload = json.load(fp)

        # Time axis "YYYY-MM-DD HH:MM, ..."
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
                if v <= -9990:
                    vals.append(None)
                else:
                    vals.append(v)

            # align length to time_labels
            if len(vals) < len(time_labels):
                vals += [None] * (len(time_labels) - len(vals))
            elif len(vals) > len(time_labels):
                vals = vals[: len(time_labels)]
            return vals

        q_sim = parse_series("time_series_discharge_simulated")
        q_obs = parse_series("time_series_discharge_observed")
        rain = parse_series("time_series_rain_observed")
        sm   = parse_series("time_series_soil_moisture_simulated")

        try:
            thr_alert = float(payload.get("section_discharge_thr_alert"))
        except (TypeError, ValueError):
            thr_alert = None

        try:
            thr_alarm = float(payload.get("section_discharge_thr_alarm"))
        except (TypeError, ValueError):
            thr_alarm = None

        time_now = sections_dict.get(section_key, {}).get("time_now")

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

    charts_data: Dict[str, Dict[str, Any]] = {}
    for sec_name, path_str in json_paths.items():
        # json_paths may be relative – resolve to absolute
        p = Path(path_str)
        if not p.is_absolute():
            p = Path(CONFIG.get("dynamic_root", "data/dynamic")).parent / p
        charts_data[sec_name] = parse_section_json(str(p), sec_name)

    # Convenience arrays for single-section mode
    ts_time: List[str] = []
    ts_q_sim: List[Optional[float]] = []
    ts_q_obs: List[Optional[float]] = []
    ts_rain: List[Optional[float]] = []
    ts_sm:   List[Optional[float]] = []
    thr_alert: Optional[float] = None
    thr_alarm: Optional[float] = None
    time_now: Optional[str] = None
    json_path_selected: Optional[str] = None

    if not multi_mode and selected_section in charts_data:
        cd = charts_data[selected_section]
        ts_time = cd["time_labels"]
        ts_q_sim = cd["q_sim"]
        ts_q_obs = cd["q_obs"]
        ts_rain = cd["rain"]
        ts_sm   = cd["sm"]
        thr_alert = cd["thr_alert"]
        thr_alarm = cd["thr_alarm"]
        time_now = cd["time_now"]
        json_path_selected = cd["json_path"]

    selected_run_cfg_key, _ = get_current_run_config()
    # Deterministic TS view: show only deterministic TS run-configs
    run_configs = _filter_run_configs_by_type(RUNS, ["ts_deterministic"])

    return render_template(
        "ts_det_chart_view.html",
        basins=BASINS,
        selected_basin=selected_basin,
        selected_run_iso=selected_run_iso,
        sections=section_options,
        selected_section=selected_section,
        available_runs=available_runs,
        run_configs=run_configs,
        selected_run_cfg_key=selected_run_cfg_key,
        active_run_key=ACTIVE_RUN_KEY,
        # data for all sections (used for all_sections mode)
        charts_data=charts_data,
        # data for selected section
        ts_time=ts_time,
        ts_q_sim=ts_q_sim,
        ts_q_obs=ts_q_obs,
        ts_rain=ts_rain,
        ts_sm=ts_sm,
        thr_alert=thr_alert,
        thr_alarm=thr_alarm,
        time_now=time_now,
        json_path=json_path_selected,
    )


@app.route("/sections")
def sections_view():
    basins = BASINS
    selected_basin = request.args.get("basin", "all")

    if selected_basin == "all":
        sections = SECTIONS_ALL
    else:
        sections = [s for s in SECTIONS_ALL if s["basin"] == selected_basin]

    return render_template(
        "geo_sections.html",
        sections=sections,
        basins=basins,
        selected_basin=selected_basin,
        dem_meta=DEM_META,        
    )


# -----------------------------------------------------------------------------
# New Maps page (DEM + river + sections)
# -----------------------------------------------------------------------------
@app.route("/geo_maps")
def geo_maps_view():
    """
    Maps page:
      - DEM shown as raster from ASCII (via /maps/dem.png)
      - Rivers from ASCII mask (via /maps/rivers.geojson)
      - Sections as points (all basins or selected basin)
    """
    basins = BASINS
    selected_basin = request.args.get("basin", "all")

    if selected_basin == "all":
        sections = SECTIONS_ALL
    else:
        sections = [s for s in SECTIONS_ALL if s["basin"] == selected_basin]

    dem_meta = DEM_META  # may be None if file missing

    return render_template(
        "geo_maps.html",
        basins=basins,
        selected_basin=selected_basin,
        sections=sections,
        dem_meta=dem_meta,
    )



# -----------------------------------------------------------------------------
# New Dynamic Maps page (independent from static geo maps)
# -----------------------------------------------------------------------------
@app.route("/maps")
def maps_view():
    basins = BASINS
    selected_basin = request.args.get("basin", "all")
    return render_template(
        "maps_view.html",
        basins=basins,
        selected_basin=selected_basin,
        dem_meta=None,
        sections=[],
    )

@app.route("/maps/dem.png")
def maps_dem_png():
    """
    Generate a terrain-colored PNG of the DEM ASCII grid.
    NODATA cells are fully transparent.
    """
    if Image is None:
        print("[ERROR] Pillow is not installed")
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
        """
        Simple terrain palette:
          0.00–0.25 : dark green → medium green
          0.25–0.50: medium green → yellow
          0.50–0.75: yellow → brown
          0.75–1.00: brown → light gray/white
        """
        t = max(0.0, min(1.0, t))
        if t < 0.25:
            f = t / 0.25
            r = int(30   + f * (80 - 30))
            g = int(70   + f * (140 - 70))
            b = int(30   + f * (60 - 30))
        elif t < 0.50:
            f = (t - 0.25) / 0.25
            r = int(80   + f * (200 - 80))
            g = int(140  + f * (200 - 140))
            b = int(60   + f * (40 - 60))
        elif t < 0.75:
            f = (t - 0.50) / 0.25
            r = int(200  + f * (160 - 200))
            g = int(200  + f * (110 - 200))
            b = int(40   + f * (40  - 40))
        else:
            f = (t - 0.75) / 0.25
            r = int(160  + f * (240 - 160))
            g = int(110  + f * (240 - 110))
            b = int(40   + f * (240 - 40))
        return r, g, b

    img = Image.new("RGBA", (ncols, nrows))
    pixels = img.load()

    for r in range(nrows):
        for c in range(ncols):
            val = DEM_GRID[r][c]
            if val == nodata:
                pixels[c, r] = (0, 0, 0, 0)  # transparent
            else:
                t = (val - vmin) / (vmax - vmin)
                r_col, g_col, b_col = terrain_color(t)
                pixels[c, r] = (r_col, g_col, b_col, 255)

    buf = io.BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)
    return send_file(buf, mimetype="image/png")


@app.route("/maps/rivers.geojson")
def maps_rivers_geojson():
    """
    Build a GeoJSON FeatureCollection from the river mask ASCII grid.
    Only cells with value ~1 (river = 1) are emitted.
    """
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
            # only river=1
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




if __name__ == "__main__":
    app.run(debug=True, port=0)

