import calendar
import pandas as pd
from typing import Dict, Iterable, List, Tuple, Set, Optional

def _abs_join(base: str, *parts: str) -> str:
    base = base.rstrip("/")
    rest = "/".join(p.strip("/") for p in parts if p)
    return f"{base}/{rest}" if rest else base

def _render(pattern: str, ts: pd.Timestamp) -> str:
    return ts.strftime(pattern)

def _month_key(ts: pd.Timestamp, fmt: str = "%m%Y") -> str:
    return ts.strftime(fmt)  # e.g. '012000'

def _hours_in_month(year: int, month: int) -> int:
    # Calendar days × 24; time axis is in UTC (no DST gaps)
    _, ndays = calendar.monthrange(year, month)
    return ndays * 24

class TimeHandler:
    """
    - 'monthly' cadence: one NetCDF per calendar month with hourly time dimension.
      For each ts, we provide (file_path, hour_index_in_month).
    - 'daily' cadence: one file per calendar day (e.g., LAI raster at noon reused all day).
    - Handles arbitrary start/end (cross months/years), timezone-aware indexing,
      and an optional 'data_tz' (e.g., files in UTC while orchestration in Europe/Rome).
    """

    def __init__(
        self,
        start: str | pd.Timestamp,
        end: str | pd.Timestamp,
        config: Dict,
        tz: Optional[str] = None
    ):
        self.cfg = config
        self.proc_tz = tz or config.get("timezone", "Europe/Rome")
        self.data_tz = config.get("data_timezone", "UTC")  # where the file time axes live
        self.out_prefix = config.get("out", {}).get("prefix", "hmc.forcing.")
        self.out_suffix = config.get("out", {}).get("suffix", ".nc")
        self.out_fmt    = config.get("out", {}).get("fmt",    "%Y%m%d%H%M")
        self.datasets   = config["datasets"]  # dict of dataset configs

        # Build hourly index in processing timezone
        idx = pd.date_range(start=start, end=end, freq="H")
        self.index = idx.tz_localize(self.proc_tz) if idx.tz is None else idx.tz_convert(self.proc_tz)

    # --------------- public helpers ---------------
    def output_name(self, ts: pd.Timestamp) -> str:
        return f"{self.out_prefix}{ts.strftime(self.out_fmt)}{self.out_suffix}"

    def month_lengths_hours(self) -> Dict[str, int]:
        """
        Returns {'MMYYYY': hours_in_month, ...} for all months touched by self.index,
        computed in calendar days × 24 (UTC-safe).
        """
        lengths: Dict[str, int] = {}
        seen: Set[Tuple[int, int]] = set()
        for ts in self.index:
            ts_utc = ts.tz_convert(self.data_tz) if ts.tz is not None else ts  # align to data TZ
            y, m = ts_utc.year, ts_utc.month
            if (y, m) not in seen:
                seen.add((y, m))
                lengths[_month_key(ts_utc)] = _hours_in_month(y, m)
        return lengths

    def monthly_manifest(self) -> Dict[str, Dict[str, str]]:
        """
        {'MMYYYY': {'air_temperature': '/.../temperature_012000.nc', ...}, ...}
        Only for datasets with cadence='monthly'.
        """
        months: List[str] = []
        seen: Set[str] = set()
        for ts in self.index:
            ts_data = ts.tz_convert(self.data_tz) if ts.tz is not None else ts
            mk = _month_key(ts_data)
            if mk not in seen:
                seen.add(mk)
                months.append(mk)

        manifest: Dict[str, Dict[str, str]] = {}
        for mk in months:
            manifest[mk] = {}
            for name, ds in self.datasets.items():
                if ds["cadence"] == "monthly":
                    # FIXED: use pd.to_datetime instead of Timestamp.strptime
                    ts_rep = pd.to_datetime(mk, format="%m%Y").replace(day=1)
                    ts_rep = ts_rep.tz_localize(self.data_tz)
                    rel = _render(ds["file_pattern"], ts_rep)
                    manifest[mk][name] = _abs_join(ds["base_dir"], rel)
        return manifest

    def lai_manifest(self) -> List[str]:
        """Unique daily LAI absolute paths for the span."""
        seen: Set[str] = set()
        out: List[str] = []
        lai_cfg = {k: v for k, v in self.datasets.items() if k == "lai"}
        if not lai_cfg:
            return out
        cfg = lai_cfg["lai"]
        for ts in self.index:
            ts_data = ts.tz_convert(self.data_tz) if ts.tz is not None else ts
            rel = _render(cfg["file_pattern"], ts_data)  # usually %m/%d/CLIM_%m%d_...
            abspath = _abs_join(cfg["base_dir"], rel)
            if abspath not in seen:
                seen.add(abspath)
                out.append(abspath)
        return out

    # --------------- resolvers ---------------
    def _resolve_monthly(self, ds_cfg: Dict, ts_proc: pd.Timestamp) -> Tuple[str, int, int]:
        """
        For a given processing timestamp, return:
          (absolute_file_path, hour_index_in_month, hours_in_month)
        Index computed after converting ts to 'data_tz' to match file time axis.
        """
        ts_data = ts_proc.tz_convert(self.data_tz) if ts_proc.tz is not None else ts_proc
        # hour index = 24*(day-1) + hour
        hour_idx = (ts_data.day - 1) * 24 + ts_data.hour
        hours_mon = _hours_in_month(ts_data.year, ts_data.month)

        # Render file path using the month (pattern like 'var_%m%Y.nc' or nested dirs)
        # Use first-of-month at 00:00 in data TZ to render safely
        ts_rep = pd.Timestamp(ts_data.year, ts_data.month, 1, 0, 0, 0).tz_localize(self.data_tz)
        rel = _render(ds_cfg["file_pattern"], ts_rep)
        fpath = _abs_join(ds_cfg["base_dir"], rel)
        return fpath, hour_idx, hours_mon

    def _resolve_daily(self, ds_cfg: Dict, ts_proc: pd.Timestamp) -> str:
        ts_data = ts_proc.tz_convert(self.data_tz) if ts_proc.tz is not None else ts_proc
        rel = _render(ds_cfg["file_pattern"], ts_data)  # can contain %Y/%m/%d + filename
        return _abs_join(ds_cfg["base_dir"], rel)

    def resolve_for_ts(self, ts: pd.Timestamp) -> Dict[str, object]:
        """
        For a single hour, returns:
          - for each monthly dataset: {'path': str, 'hour_index': int, 'hours_in_month': int}
          - for each daily dataset: absolute path
          - '__output__': output filename
          - 'ts': timestamp in processing TZ
        """
        rec: Dict[str, object] = {}
        for name, cfg in self.datasets.items():
            cad = cfg["cadence"]
            if cad == "monthly":
                path, hidx, hlen = self._resolve_monthly(cfg, ts)
                rec[name] = {"path": path, "hour_index": hidx, "hours_in_month": hlen}
            elif cad == "daily":
                rec[name] = self._resolve_daily(cfg, ts)
            else:
                raise ValueError(f"Unsupported cadence '{cad}' for dataset '{name}'")
        rec["__output__"] = self.output_name(ts)
        rec["ts"] = ts
        return rec

    def iterate(self) -> Iterable[Dict[str, object]]:
        for ts in self.index:
            yield self.resolve_for_ts(ts)
