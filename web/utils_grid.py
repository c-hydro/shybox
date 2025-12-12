from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple

from utils_config import DEM_FILE, CHOICE_FILE


def load_ascii_grid(path: Path):
    """
    Load an ESRI ASCII grid, returning (meta, grid) where:
      meta: {ncols, nrows, xllcorner, yllcorner, cellsize, nodata}
      grid: list of list of float
    """
    if not path.exists():
        print("[WARNING] grid file not found:", path)
        return None, None

    with path.open(encoding="utf-8") as f:
        header: Dict[str, Any] = {}
        for _ in range(6):
            line = f.readline()
            if not line:
                break
            parts = line.split()
            if len(parts) < 2:
                continue
            key = parts[0].lower()
            val = parts[1]
            if key in ("ncols", "nrows"):
                header[key] = int(val)
            elif key in ("xllcorner", "yllcorner", "cellsize", "nodata_value"):
                header[key] = float(val)

        ncols = header.get("ncols")
        nrows = header.get("nrows")
        if not (ncols and nrows):
            print("[WARNING] invalid grid header in", path)
            return None, None

        grid: List[List[float]] = []
        for _row in range(nrows):
            vals: List[str] = []
            while len(vals) < ncols:
                line = f.readline()
                if not line:
                    break
                vals.extend(line.split())
            if len(vals) == 0:
                break
            grid.append([float(v) for v in vals[:ncols]])

        meta = {
            "ncols": ncols,
            "nrows": nrows,
            "xllcorner": header["xllcorner"],
            "yllcorner": header["yllcorner"],
            "cellsize": header["cellsize"],
            "nodata": header.get("nodata_value", -9999.0),
        }
        return meta, grid


def grid_value_at_lonlat(meta, grid, lon: float, lat: float) -> Optional[float]:
    """
    Sample the grid at given lon/lat using simple nearest-neighbor,
    same logic as original app.py.
    """
    if meta is None or grid is None:
        return None

    ncols = meta["ncols"]
    nrows = meta["nrows"]
    x0 = meta["xllcorner"]
    y0 = meta["yllcorner"]
    cs = meta["cellsize"]
    nodata = meta["nodata"]

    xmax = x0 + cs * ncols
    ymax = y0 + cs * nrows

    if not (x0 <= lon <= xmax and y0 <= lat <= ymax):
        return None

    row_from_top = int((ymax - lat) / cs)
    col = int((lon - x0) / cs)

    if not (0 <= row_from_top < nrows and 0 <= col < ncols):
        return None

    val = grid[row_from_top][col]
    if val == nodata:
        return None
    return val


# Load DEM and river-choice grids at import time
DEM_META, DEM_GRID = load_ascii_grid(DEM_FILE)

# We use CHOICE_FILE as river network mask:
#  - river = 1 (or > 0)
#  - land/water = 0 or nodata
CHOICE_META, CHOICE_GRID = load_ascii_grid(CHOICE_FILE)

