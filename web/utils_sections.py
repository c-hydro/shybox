import csv
from typing import List, Dict, Any

from utils_config import SECTIONS_CSV
from utils_grids import DEM_META, DEM_GRID, CHOICE_META, CHOICE_GRID, grid_value_at_lonlat


def load_csv_basins() -> List[str]:
    """
    Scan the sections CSV and return sorted unique basin names.
    """
    if not SECTIONS_CSV.exists():
        print("[WARNING] sections CSV not found:", SECTIONS_CSV)
        return []
    basins = set()
    with SECTIONS_CSV.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            basin = row.get("BASIN")
            if basin:
                basins.add(basin)
    return sorted(basins)


def load_all_sections() -> List[Dict[str, Any]]:
    """
    Load all sections from CSV and enrich with DEM and river_code
    using the preloaded ASCII grids.
    """
    sections: List[Dict[str, Any]] = []
    if not SECTIONS_CSV.exists():
        print("[WARNING] sections CSV not found:", SECTIONS_CSV)
        return sections

    with SECTIONS_CSV.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                sid = int(row["ID"])
                lon = float(row["LON"])
                lat = float(row["LAT"])
            except (KeyError, ValueError):
                continue

            basin = row.get("BASIN", "")
            name = row.get("SEC_NAME", row.get("NAME", f"Section {sid}"))

            dem_z = grid_value_at_lonlat(DEM_META, DEM_GRID, lon, lat)
            river_code = grid_value_at_lonlat(CHOICE_META, CHOICE_GRID, lon, lat)

            raw = dict(row)
            raw["dem_z"] = dem_z
            raw["river_code"] = river_code

            sections.append(
                {
                    "id": sid,
                    "lon": lon,
                    "lat": lat,
                    "basin": basin,
                    "name": name,
                    "dem_z": dem_z,
                    "river_code": river_code,
                    "raw": raw,
                }
            )

    return sections

