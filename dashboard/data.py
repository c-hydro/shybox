"""Read declared NetCDF collections; close datasets before returning JSON values."""
from datetime import datetime
import hashlib
import json
from pathlib import Path
import re
from threading import RLock
import warnings

import numpy as np
import pandas as pd
import xarray as xr


SERIES = {
    "discharge_simulated_analysis": "Analysis",
    "discharge_simulated_openloop": "Open loop",
    "discharge_observed": "Observed",
}
STATUSES = ("green", "yellow", "orange", "red", "unknown")
# NetCDF C-library builds are not generally safe for concurrent threaded access.
READ_LOCK = RLock()


class DataError(ValueError):
    """A declared result cannot be interpreted safely."""


def load_config(path):
    path = Path(path).resolve()
    cfg = json.loads(path.read_text())
    if not isinstance(cfg.get("runs"), dict) or not cfg["runs"]:
        raise ValueError('Configuration requires a nonempty "runs" object.')
    overview = cfg.setdefault("overview", {})
    if overview.setdefault("period", "all") not in ("all", "forecast"):
        raise ValueError("overview.period must be all or forecast.")
    cutoff = overview.setdefault("probability_cutoff", 0.5)
    if not isinstance(cutoff, (int, float)) or not 0 < cutoff <= 1:
        raise ValueError("overview.probability_cutoff must be in (0, 1].")
    for key, run in cfg["runs"].items():
        if run.get("mode") not in ("deterministic", "probabilistic"):
            raise ValueError(f"{key}: mode must be deterministic or probabilistic.")
        source = run.get("time_series", {})
        for field in ("folder", "file_pattern", "run_time_regex", "run_time_format"):
            if not isinstance(source.get(field), str) or not source[field]:
                raise ValueError(f"{key}: time_series.{field} is required.")
        folder = Path(source["folder"]).expanduser()
        source["folder"] = str((path.parent / folder).resolve())
        pattern = Path(source["file_pattern"])
        if pattern.is_absolute() or ".." in pattern.parts:
            raise ValueError(f"{key}: file_pattern must stay within folder.")
        expression = re.compile(source["run_time_regex"])
        if "time" not in expression.groupindex:
            raise ValueError(f"{key}: run_time_regex needs a named group 'time'.")
        if run["mode"] == "probabilistic":
            if not run.get("ensemble_dimension"):
                raise ValueError(f"{key}: ensemble_dimension is required.")
            quantiles = run.setdefault("quantiles", [0.05, 0.25, 0.5, 0.75, 0.95])
            if (len(quantiles) != 5 or quantiles[2] != 0.5
                    or any(not 0 <= q <= 1 for q in quantiles)
                    or any(a >= b for a, b in zip(quantiles, quantiles[1:]))):
                raise ValueError(f"{key}: use five increasing quantiles with median 0.5.")
    default = cfg.setdefault("default_run", next(iter(cfg["runs"])))
    if default not in cfg["runs"] or not cfg["runs"][default].get("enabled", True):
        raise ValueError("default_run must refer to an enabled configuration.")
    return cfg


class Catalog:
    def __init__(self, config):
        self.config = config

    def discover(self):
        """Discover files only inside enabled, declared collections."""
        collections = []
        for key, spec in self.config["runs"].items():
            if not spec.get("enabled", True):
                continue
            source = spec["time_series"]
            root = Path(source["folder"])
            runs, issues = [], []
            if root.is_dir():
                for path in sorted(root.glob(source["file_pattern"])):
                    if not path.is_file() or not path.resolve().is_relative_to(root):
                        continue
                    match = re.search(source["run_time_regex"], str(path.relative_to(root)))
                    try:
                        if match is None:
                            raise ValueError("timestamp does not match run_time_regex")
                        time = datetime.strptime(match["time"], source["run_time_format"])
                    except ValueError as exc:
                        issues.append(f"{path.name}: {exc}")
                        continue
                    relative = path.relative_to(root).as_posix()
                    token = hashlib.sha256(f"{key}/{relative}".encode()).hexdigest()[:20]
                    runs.append({"id": token, "time": time.isoformat(),
                                 "date": time.date().isoformat(), "file": relative})
            runs.sort(key=lambda item: (item["time"], item["file"]), reverse=True)
            collections.append({"id": key, "name": spec.get("name", key),
                                "mode": spec["mode"], "runs": runs, "issues": issues,
                                "maps": spec.get("maps", {"enabled": False})})
        return collections

    def resolve(self, collection_id, run_id):
        for collection in self.discover():
            if collection["id"] == collection_id:
                for run in collection["runs"]:
                    if run["id"] == run_id:
                        spec = self.config["runs"][collection_id]
                        return spec, run, Path(spec["time_series"]["folder"]) / run["file"]
        raise KeyError("Run not found in declared configurations.")

    def clean(self, values, discharge=False):
        values = np.asarray(values, dtype=float).copy()
        values[~np.isfinite(values)] = np.nan
        for marker in self.config.get("missing_values", [-9999, -9998]):
            values[values == marker] = np.nan
        if discharge:
            values[values < 0] = np.nan
        return values

    def _check(self, ds, spec):
        if "time" not in ds.coords or "sections" not in ds.sizes:
            raise DataError("NetCDF requires a time coordinate and sections dimension.")
        if not np.issubdtype(ds.time.dtype, np.datetime64):
            raise DataError("NetCDF time must decode to datetime values.")
        if not ds.time.size or np.isnat(ds.time.values).any():
            raise DataError("NetCDF time must contain valid timestamps.")
        if not ds.indexes["time"].is_monotonic_increasing or ds.indexes["time"].has_duplicates:
            raise DataError("NetCDF time must be sorted and unique.")
        names = [name for name in SERIES if name in ds]
        if not names:
            raise DataError("No supported discharge variables found.")
        ensemble = spec.get("ensemble_dimension")
        simulated = [n for n in names if n != "discharge_observed"]
        for name in names:
            expected = {"time", "sections"}
            if spec["mode"] == "probabilistic" and name in simulated:
                expected.add(ensemble)
            if set(ds[name].dims) != expected:
                raise DataError(f"{name}: expected dimensions {sorted(expected)}; got {ds[name].dims}.")
        if spec["mode"] == "probabilistic" and (not simulated or ds.sizes.get(ensemble, 0) == 0):
            raise DataError(f"Probabilistic output requires simulated members on '{ensemble}'.")
        return names

    def _metadata(self, ds, index):
        def value(name, default=None):
            if name not in ds:
                return default
            v = ds[name].isel(sections=index).item()
            if isinstance(v, bytes):
                return v.decode("utf-8", errors="replace")
            return v
        thresholds = [self.clean([value(f"threshold_level_{level}", np.nan)], True)[0]
                      for level in (1, 2, 3)]
        return {
            "index": index, "id": value("id", index),
            "name": str(value("section_name", f"Section {index + 1}")),
            "tag": str(value("tag", index)), "basin": str(value("basin", "Unspecified")),
            "longitude": number(value("longitude")), "latitude": number(value("latitude")),
            "area": number(value("catchment_area_km2")),
            "thresholds": [number(t) for t in thresholds],
        }

    def summary(self, collection_id, run_id, period, source):
        spec, run, path = self.resolve(collection_id, run_id)
        with READ_LOCK, xr.open_dataset(path, engine="netcdf4") as ds:
            names = self._check(ds, spec)
            if source != "all":
                if source not in names:
                    raise DataError("Selected discharge series is unavailable in this run.")
                names = [source]
            view = ds.sel(time=slice(pd.Timestamp(run["time"]), None)) if period == "forecast" else ds
            # One result file is opened per request; no process-wide dataset cache.
            arrays = {n: self.clean(view[n].transpose("sections", "time", ...).values, True)
                      for n in names}
            sections = []
            cutoff = self.config["overview"]["probability_cutoff"]
            for index in range(ds.sizes["sections"]):
                item = self._metadata(ds, index)
                valid = [a[index][np.isfinite(a[index])] for a in arrays.values()]
                peak = max((float(a.max()) for a in valid if a.size), default=None)
                level, probabilities = 0, [None, None, None]
                # Observations are context in ensemble mode, not additional members.
                ensemble_arrays = [a[index] for n, a in arrays.items() if n != "discharge_observed"]
                if spec["mode"] == "probabilistic":
                    peak = max((float(a[np.isfinite(a)].max()) for a in ensemble_arrays
                                if np.isfinite(a).any()), default=None)
                for j, threshold in enumerate(item["thresholds"]):
                    if threshold is None or peak is None:
                        continue
                    if spec["mode"] == "deterministic":
                        crossed = peak > threshold
                    else:
                        # Union over selected simulated products and times for each member.
                        hits = np.zeros(ds.sizes[spec["ensemble_dimension"]], dtype=bool)
                        available = hits.copy()
                        for a in ensemble_arrays:
                            available |= np.isfinite(a).any(axis=0)
                            hits |= (a > threshold).any(axis=0)
                        probability = float(hits[available].mean()) if available.any() else None
                        probabilities[j] = probability
                        crossed = probability is not None and probability >= cutoff
                    if crossed:
                        level = j + 1
                known = peak is not None and any(t is not None for t in item["thresholds"])
                item.update(peak=peak, status=STATUSES[level] if known else "unknown",
                            probabilities=probabilities)
                sections.append(item)
            times = [pd.Timestamp(t).isoformat() for t in view.time.values]
            return {"run": run, "mode": spec["mode"], "sections": sections,
                    "period": period, "source": source, "probability_cutoff": cutoff,
                    "start": times[0] if times else None, "end": times[-1] if times else None,
                    "basins": sorted({s["basin"] for s in sections}),
                    "sources": [{"id": n, "name": SERIES[n]} for n in self._check(ds, spec)],
                    "counts": {s: sum(item["status"] == s for item in sections) for s in STATUSES}}

    def series(self, collection_id, run_id, index):
        spec, run, path = self.resolve(collection_id, run_id)
        with READ_LOCK, xr.open_dataset(path, engine="netcdf4") as ds:
            names = self._check(ds, spec)
            if not 0 <= index < ds.sizes["sections"]:
                raise KeyError("Section not found.")
            traces = []
            for name in names:
                data = ds[name].isel(sections=index)
                if spec["mode"] == "probabilistic" and name != "discharge_observed":
                    values = self.clean(data.transpose("time", spec["ensemble_dimension"]).values, True)
                    # NumPy warns for all-missing times; those become JSON null deliberately.
                    with warnings.catch_warnings():
                        warnings.filterwarnings("ignore", message="All-NaN slice encountered")
                        quantiles = np.nanquantile(values, spec["quantiles"], axis=1)
                    traces.append({"id": name, "name": SERIES[name], "kind": "ensemble",
                                   "quantiles": spec["quantiles"], "values": matrix(quantiles),
                                   "member_counts": np.isfinite(values).sum(axis=1).tolist()})
                else:
                    traces.append({"id": name, "name": SERIES[name], "kind": "line",
                                   "values": vector(self.clean(data.values, True))})
            return {"section": self._metadata(ds, index), "run": run, "mode": spec["mode"],
                    "time": [pd.Timestamp(t).isoformat() for t in ds.time.values],
                    "traces": traces, "units": "m³/s"}


def number(value):
    return float(value) if value is not None and np.isfinite(float(value)) else None


def vector(values):
    return [number(v) for v in values]


def matrix(values):
    return [vector(row) for row in values]
