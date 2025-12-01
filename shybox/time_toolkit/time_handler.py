from __future__ import annotations

from typing import Optional, Dict, Any, Tuple, Set
import warnings

import pandas as pd


class TimeManager:
    """
    Manage time configuration with:

      - time_run       (reference run time)
      - time_start     (window start, Timestamp internally)
      - time_end       (window end, Timestamp internally)
      - time_period    (window length, Timedelta internally, optionally exposed as int)
      - time_frequency (sampling frequency, Timedelta)
      - time_rounding  (pandas offset alias, e.g. 'H', '30min', 'D')
      - tz             (timezone, e.g. 'Europe/Rome')

    Input for time_start / time_end:
      - datetime / pandas.Timestamp / parseable string
      - OR a template string containing '%', e.g. "%Y-%m-01 00:00"
        In this case:
          1) A raw window is computed first (using time_run, time_period, etc.)
          2) The template is applied to a base Timestamp (raw start/end or time_run)
          3) time_start/time_end are overridden by that result
          4) time_period is recomputed as time_end - time_start

    start_days_before (int):
      - 0 = today (time_run date) at 00:00
      - 1 = yesterday 00:00
      - 2 = two days ago 00:00
      etc., used only if time_start is not explicitly set.

    Output mode for core fields (string):
      - time_as_string: tuple of core field names that must be returned as strings.
        Valid names: "time_run", "time_start", "time_end".
        Example:
          time_as_string = ('time_start', 'time_end')
        Unknown names produce a warning but are ignored.

    Output mode for core fields (int):
      - time_as_int: tuple of field names that must be returned as integer.
        Currently supported: "time_period".
        Example:
          time_as_int = ('time_period',)
        Unknown names produce a warning but are ignored.

        For time_period, the integer is:
            int(time_period / time_frequency)
        i.e. number of time steps.

    Derived time keys (e.g. time_restart):
      - Defined via add_time_key(name, spec) where spec may contain:
          {
            "time_ref": "time_run" | "time_start" | "time_end" | other derived name,
            "time_step": -1,                        # integer
            "time_frequency": "h" | "3H" | "15min"  # optional, default self.time_frequency
            "time_template": "%Y-%m-%d %H:00",      # optional, applied after offset
            "time_as_str": True/False               # default False
          }

      - Then you can access:
          tm.time_restart      -> string or Timestamp
          tm.time_restart_ts   -> raw Timestamp

      - Also included in as_dict().
    """

    VALID_TIME_FIELDS = ("time_run", "time_start", "time_end")
    VALID_INT_FIELDS = ("time_period",)

    # ------------------------------------------------------------------ #
    # Constructor
    # ------------------------------------------------------------------ #

    def __init__(
        self,
        *,
        time_run_ts: pd.Timestamp,
        time_start_ts: pd.Timestamp,
        time_end_ts: pd.Timestamp,
        time_period: pd.Timedelta,
        time_frequency: pd.Timedelta,
        tz: str = "Europe/Rome",
        time_rounding: Optional[str] = None,
        time_start_template: Optional[str] = None,
        time_end_template: Optional[str] = None,
        time_as_string: Tuple[str, ...] = ("time_start", "time_end"),
        time_as_int: Tuple[str, ...] = (),
    ) -> None:
        self._tz = tz
        self._time_run = time_run_ts
        self._time_start = time_start_ts
        self._time_end = time_end_ts
        self._time_period = time_period     # always Timedelta internally
        self._time_frequency = time_frequency
        self._time_rounding = time_rounding
        self._time_start_template = time_start_template
        self._time_end_template = time_end_template

        # Core fields string-mode control
        self.time_as_string = self._validate_time_as_string(time_as_string)

        # Core fields int-mode control
        self.time_as_int = self._validate_time_as_int(time_as_int)

        # Derived time keys storage
        self._extra_times: Dict[str, pd.Timestamp] = {}
        self._extra_templates: Dict[str, Optional[str]] = {}
        self._extra_as_string: Set[str] = set()

    # ------------------------------------------------------------------ #
    # Static helpers
    # ------------------------------------------------------------------ #

    @staticmethod
    def _apply_template(base_ts: pd.Timestamp, template: str, tz: str) -> pd.Timestamp:
        """
        Apply a strftime template to base_ts and parse back to a tz-aware Timestamp.
        """
        s = base_ts.strftime(template)
        ts = pd.to_datetime(s)
        if ts.tzinfo is None:
            ts = ts.tz_localize(tz)
        else:
            ts = ts.tz_convert(tz)
        return ts

    @staticmethod
    def _parse_delta(val, default: Optional[str] = None) -> Optional[pd.Timedelta]:
        if val is None:
            if default is None:
                return None
            val = default
        if isinstance(val, pd.Timedelta):
            return val
        if isinstance(val, str):
            # handle shorthand like "h" -> "1h"
            if not any(ch.isdigit() for ch in val):
                val = "1" + val
        return pd.to_timedelta(val)

    @classmethod
    def from_config_to_nml(cls, obj,
                    start_days_before='2D',
                    time_as_string=("time_start", "time_end"),
                    time_as_int=("time_period",),
                    tz: str = "Europe/Rome") -> "TimeManager":
        """
        Build a TimeManager from a generic object.

        Supported input patterns:
          - obj["lut"]
          - obj["variables"]["lut"]
          - obj.lut
          - obj.variables["lut"]

        Raises:
          ValueError if no LUT dict is found.
        """

        lut = None

        # Case 1: object has attribute `.lut`
        if hasattr(obj, "lut"):
            lut = getattr(obj, "lut")

        # Case 2: object has attribute `.variables` with key "lut"
        elif hasattr(obj, "variables") and isinstance(obj.variables, dict):
            if "lut" in obj.variables:
                lut = obj.variables["lut"]

        # Case 3: obj is a dict with key "lut"
        elif isinstance(obj, dict) and "lut" in obj:
            lut = obj["lut"]

        else:
            raise ValueError(
                "Unable to locate 'lut' in config object: expected obj.lut, obj['lut'], or obj.variables['lut'].")

        if not isinstance(lut, dict):
            raise ValueError("'lut' found, but it is not a dictionary.")

        # ----------- convert LUT into TimeManager configuration ----------- #
        # time_period (int steps) + time_frequency -> timedelta
        period_steps = lut.get("time_period")
        if period_steps is None:
            raise ValueError("'time_period' is required in LUT.")

        freq_unit = lut.get("time_frequency", "H")
        time_period_str = f"{int(period_steps)}{freq_unit}"

        # build config for from_dict()
        cfg = {
            "time_run": lut.get("time_run"),
            "time_start": lut.get("time_start"),
            "time_end": lut.get("time_end"),
            "time_period": time_period_str,
            "time_frequency": lut.get("time_frequency"),
            "time_rounding": lut.get("time_rounding"),
            "start_days_before": start_days_before}

        # create TimeManager (time_period returned as int)
        tm = cls.from_dict(
            cfg,
            tz=tz,
            time_as_string=time_as_string,
            time_as_int=time_as_int,
        )

        # add time_restart if template exists
        if "time_restart" in lut:
            tm.add_time_key(
                "time_restart",
                {
                    "time_ref": "time_start",
                    "time_step": -1,
                    "time_frequency": tm.time_frequency,
                    "time_template": lut["time_restart"],
                    "time_as_str": True,
                },
            )

        return tm

    # ------------------------------------------------------------------ #
    # from_dict: main factory
    # ------------------------------------------------------------------ #

    @classmethod
    def from_dict(
        cls,
        cfg: Dict[str, Any],
        tz: str = "Europe/Rome",
        time_as_string: Tuple[str, ...] = ("time_start", "time_end"),
        time_as_int: Tuple[str, ...] = (),
    ) -> "TimeManager":
        """
        cfg keys:

          time_run           : datetime/str/Timestamp (default = now)
          time_start         : datetime/str/Timestamp OR template with '%'
          time_end           : datetime/str/Timestamp OR template with '%'
          time_period        : Timedelta/str, e.g. '6H', '1D'
          time_frequency     : Timedelta/str, default '1H'
          time_rounding      : pandas offset alias, e.g. 'H'
          start_days_before  : int (0=today, 1=yesterday, ...)

        Behavior:
          1) parse time_run, time_period, time_frequency
          2) detect templates for time_start/time_end (string with '%')
             and ignore them *as datetime* during raw logic
          3) compute raw time_start/time_end using:
               - explicit start/end
               - start_days_before
               - time_period
               - time_run + rounding
          4) apply templates (if any) to a base timestamp
             (raw start/end if present, otherwise time_run)
          5) recompute time_period = time_end - time_start
        """

        def _parse_time_run(val) -> pd.Timestamp:
            if val is None:
                ts = pd.Timestamp.now(tz=tz)
            elif isinstance(val, pd.Timestamp):
                ts = val
            else:
                ts = pd.to_datetime(val)
            if ts.tzinfo is None:
                ts = ts.tz_localize(tz)
            else:
                ts = ts.tz_convert(tz)
            return ts

        def _parse_time_value(val) -> Optional[pd.Timestamp]:
            if val is None:
                return None
            if isinstance(val, pd.Timestamp):
                ts = val
            else:
                ts = pd.to_datetime(val)
            if ts.tzinfo is None:
                ts = ts.tz_localize(tz)
            else:
                ts = ts.tz_convert(tz)
            return ts

        time_run_ts = _parse_time_run(cfg.get("time_run"))
        period = cls._parse_delta(cfg.get("time_period"))
        freq = cls._parse_delta(cfg.get("time_frequency"), default="1H")
        time_rounding = cfg.get("time_rounding")

        start_days_before = cfg.get("start_days_before")
        if start_days_before is not None:
            start_days_before = int(start_days_before)

        raw_start = cfg.get("time_start")
        raw_end = cfg.get("time_end")

        # detect templates: strings with '%'
        start_template = raw_start if isinstance(raw_start, str) and "%" in raw_start else None
        end_template = raw_end if isinstance(raw_end, str) and "%" in raw_end else None

        # for raw logic, ignore template strings as datetime
        t_start = None if start_template is not None else _parse_time_value(raw_start)
        t_end = None if end_template is not None else _parse_time_value(raw_end)

        # ---- 1) raw window resolution (no templates yet) --------------- #

        # use start_days_before if no explicit start
        if t_start is None and start_days_before is not None:
            base_day = time_run_ts.normalize()  # midnight of time_run date
            t_start = base_day - pd.Timedelta(days=start_days_before)

        # combinations
        if t_start is not None and t_end is not None:
            if period is None:
                period = t_end - t_start

        elif t_start is not None and t_end is None:
            if period is None:
                raise ValueError("When only time_start is given, time_period is required.")
            t_end = t_start + period

        elif t_start is None and t_end is not None:
            if period is None:
                raise ValueError("When only time_end is given, time_period is required.")
            t_start = t_end - period

        else:
            # neither start nor end given
            if period is None:
                raise ValueError(
                    "Need at least time_period when both time_start and time_end are missing."
                )
            t_end = time_run_ts
            if time_rounding is not None:
                t_end = t_end.floor(time_rounding)
            t_start = t_end - period

        # ---- 2) apply templates to override start/end ------------------ #

        if start_template is not None:
            base = t_start if t_start is not None else time_run_ts
            t_start = cls._apply_template(base, start_template, tz)

        if end_template is not None:
            base = t_end if t_end is not None else time_run_ts
            t_end = cls._apply_template(base, end_template, tz)

        # ---- 3) final consistency & recompute period ------------------- #

        if t_start > t_end:
            raise ValueError("time_start cannot be after time_end.")

        period = t_end - t_start  # recompute after templates

        return cls(
            time_run_ts=time_run_ts,
            time_start_ts=t_start,
            time_end_ts=t_end,
            time_period=period,
            time_frequency=freq,
            tz=tz,
            time_rounding=time_rounding,
            time_start_template=start_template,
            time_end_template=end_template,
            time_as_string=time_as_string,
            time_as_int=time_as_int,
        )

    # ------------------------------------------------------------------ #
    # time_as_string / time_as_int handling for core fields
    # ------------------------------------------------------------------ #

    def __dir__(self):
        """
        Make derived keys (time_restart, time_restart_ts, ...) visible in dir()
        so IDEs / debuggers show them like normal attributes.
        """
        base = set(super().__dir__())
        extra_names = set(self._extra_times.keys())
        extra_ts_names = {f"{name}_ts" for name in self._extra_times.keys()}
        return sorted(base | extra_names | extra_ts_names)

    def _validate_time_as_string(self, items: Tuple[str, ...]) -> set[str]:
        items_set = set(items)
        unknown = items_set - set(self.VALID_TIME_FIELDS)
        if unknown:
            warnings.warn(
                f"time_as_string contains unknown fields {unknown}. "
                f"Valid fields are: {self.VALID_TIME_FIELDS}. Ignoring unknown ones."
            )
            items_set -= unknown  # ignore unknown keys
        return items_set

    def _validate_time_as_int(self, items: Tuple[str, ...]) -> set[str]:
        items_set = set(items)
        unknown = items_set - set(self.VALID_INT_FIELDS)
        if unknown:
            warnings.warn(
                f"time_as_int contains unknown fields {unknown}. "
                f"Valid fields are: {self.VALID_INT_FIELDS}. Ignoring unknown ones."
            )
            items_set -= unknown
        return items_set

    def set_time_as_string(self, *fields: str) -> None:
        """
        Update which core time fields are returned as strings.
        Example: set_time_as_string('time_start', 'time_end')
        """
        self.time_as_string = self._validate_time_as_string(fields)

    def set_time_as_int(self, *fields: str) -> None:
        """
        Update which core fields are returned as integers.
        Currently supports: 'time_period'
        """
        self.time_as_int = self._validate_time_as_int(fields)

    # ------------------------------------------------------------------ #
    # Internal formatter
    # ------------------------------------------------------------------ #

    @staticmethod
    def _format_ts(ts: pd.Timestamp, template: Optional[str]) -> str:
        if template:
            return ts.strftime(template)
        return ts.isoformat()

    # ------------------------------------------------------------------ #
    # Public timestamp accessors (always Timestamp) for core fields
    # ------------------------------------------------------------------ #

    @property
    def time_run_ts(self) -> pd.Timestamp:
        return self._time_run

    @property
    def time_start_ts(self) -> pd.Timestamp:
        return self._time_start

    @property
    def time_end_ts(self) -> pd.Timestamp:
        return self._time_end

    @property
    def time_period_td(self) -> pd.Timedelta:
        """
        Always return the raw Timedelta for time_period.
        """
        return self._time_period

    @property
    def time_frequency(self) -> pd.Timedelta:
        return self._time_frequency

    @property
    def tz(self) -> str:
        return self._tz

    # ------------------------------------------------------------------ #
    # Core properties (string/int OR native, depending on flags)
    # ------------------------------------------------------------------ #

    @property
    def time_run(self):
        if "time_run" in self.time_as_string:
            return self._format_ts(self._time_run, None)
        return self._time_run

    @property
    def time_start(self):
        if "time_start" in self.time_as_string:
            return self._format_ts(self._time_start, self._time_start_template)
        return self._time_start

    @property
    def time_end(self):
        if "time_end" in self.time_as_string:
            return self._format_ts(self._time_end, self._time_end_template)
        return self._time_end

    @property
    def time_period(self):
        """
        Return:
          - int(time_period / time_frequency) if 'time_period' in time_as_int
          - raw Timedelta otherwise.
        """
        if "time_period" in self.time_as_int:
            if self._time_frequency == pd.Timedelta(0):
                raise ValueError("time_frequency is zero; cannot compute integer steps.")
            return int(self._time_period / self._time_frequency)
        return self._time_period

    # ------------------------------------------------------------------ #
    # Derived time keys
    # ------------------------------------------------------------------ #

    def _get_ref_timestamp(self, name: str) -> pd.Timestamp:
        """
        Resolve a reference name to a Timestamp.
        Allow core names and already-defined derived keys.
        """
        if name == "time_run":
            return self._time_run
        if name == "time_start":
            return self._time_start
        if name == "time_end":
            return self._time_end
        if name in self._extra_times:
            return self._extra_times[name]
        raise ValueError(f"Unknown time_ref '{name}' for derived time key.")

    def add_time_key(self, key: str, spec: Dict[str, Any]) -> None:
        """
        Add a derived time key, e.g.:

            add_time_key(
                "time_restart",
                {
                    "time_ref": "time_run",
                    "time_step": -1,
                    "time_frequency": "h",
                    "time_template": "%Y-%m-%d %H:00",
                    "time_as_str": True,
                },
            )

        spec fields:

          time_ref      : name of reference time
                          ('time_run', 'time_start', 'time_end', or another derived key)
          time_step     : integer multiplier (default 0)
          time_frequency: step size as str or Timedelta (default self.time_frequency)
                          e.g. "h", "3H", "15min", pd.Timedelta("1H")
          time_template : optional strftime template applied AFTER offset
          time_as_str   : bool, if True this key is returned as string, else Timestamp
        """
        if "time_ref" not in spec:
            raise ValueError("spec['time_ref'] is required to define a derived time key.")
        ref_name = spec["time_ref"]
        base_ts = self._get_ref_timestamp(ref_name)

        step = int(spec.get("time_step", 0))
        freq_val = spec.get("time_frequency", self._time_frequency)
        if isinstance(freq_val, pd.Timedelta):
            freq_td = freq_val
        else:
            freq_td = self._parse_delta(freq_val)
        target_ts = base_ts + step * freq_td

        template = spec.get("time_template")
        if template:
            target_ts = self._apply_template(target_ts, template, self._tz)

        # Store internal timestamp and template
        self._extra_times[key] = target_ts
        self._extra_templates[key] = template

        if spec.get("time_as_str", False):
            self._extra_as_string.add(key)
        else:
            self._extra_as_string.discard(key)

    def get_time_ts(self, key: str) -> pd.Timestamp:
        """
        Get the raw Timestamp for a core or derived time key.
        """
        if key == "time_run":
            return self._time_run
        if key == "time_start":
            return self._time_start
        if key == "time_end":
            return self._time_end
        if key in self._extra_times:
            return self._extra_times[key]
        raise KeyError(f"Unknown time key '{key}'.")

    # Dynamic attribute access for derived keys:
    def __getattr__(self, name: str):
        """
        Allow accessing derived times as attributes:

            tm.time_restart      -> string or Timestamp (according to _extra_as_string)
            tm.time_restart_ts   -> raw Timestamp for derived key 'time_restart'
        """
        # 1) Direct derived time: tm.time_restart
        if name in self._extra_times:
            ts = self._extra_times[name]
            template = self._extra_templates.get(name)
            if name in self._extra_as_string:
                return self._format_ts(ts, template)
            return ts

        # 2) Raw timestamp accessor: tm.<name>_ts (e.g. time_restart_ts)
        if name.endswith("_ts"):
            base = name[:-3]
            if base in self._extra_times:
                return self._extra_times[base]

        raise AttributeError(name)

    # ------------------------------------------------------------------ #
    # Range & export
    # ------------------------------------------------------------------ #

    @property
    def time_range(self) -> pd.DatetimeIndex:
        """Return a pandas date_range from time_start to time_end (inclusive)."""
        return pd.date_range(self._time_start, self._time_end, freq=self._time_frequency)

    def as_dict(self) -> Dict[str, Any]:
        """
        Export core + derived times.

        - Core fields use time_as_string / time_as_int rules.
        - Derived fields use their own time_as_str and template logic.
        """
        data = {
            "time_run": self.time_run,
            "time_start": self.time_start,
            "time_end": self.time_end,
            "time_period": self.time_period,        # may be int or Timedelta
            "time_frequency": self._time_frequency,
            "time_rounding": self._time_rounding,
            "tz": self._tz,
            "time_start_template": self._time_start_template,
            "time_end_template": self._time_end_template,
            "time_as_string": tuple(self.time_as_string),
            "time_as_int": tuple(self.time_as_int),
        }

        # Add derived keys
        for name, ts in self._extra_times.items():
            template = self._extra_templates.get(name)
            if name in self._extra_as_string:
                data[name] = self._format_ts(ts, template)
            else:
                data[name] = ts

        return data

    # ------------------------------------------------------------------ #
    # Flatten / alignment helpers (operate on timestamps)
    # ------------------------------------------------------------------ #

    def _adjust_for_new_start(self, new_start: pd.Timestamp, keep_end: bool = True) -> None:
        if new_start.tzinfo is None:
            new_start = new_start.tz_localize(self._tz)
        else:
            new_start = new_start.tz_convert(self._tz)

        self._time_start = new_start
        if keep_end:
            self._time_period = self._time_end - self._time_start
        else:
            self._time_end = self._time_start + self._time_period

    def _adjust_for_new_end(self, new_end: pd.Timestamp, keep_start: bool = True) -> None:
        if new_end.tzinfo is None:
            new_end = new_end.tz_localize(self._tz)
        else:
            new_end = new_end.tz_convert(self._tz)

        self._time_end = new_end
        if keep_start:
            self._time_period = self._time_end - self._time_start
        else:
            self._time_start = self._time_end - self._time_period

    def flatten_start(
        self,
        mode: str = "midnight",
        hour: Optional[int] = None,
        date: Optional[Any] = None,
        keep_end: bool = True,
    ) -> None:
        """
        Align/flatten time_start:

          mode = 'midnight'   -> set to 00:00 of same day
          mode = 'noon'       -> set to 12:00 of same or previous day (<= old)
          mode = 'hour'       -> set to given hour <= old start (possibly previous day)
          mode = 'time_run'   -> set to time_run (optionally already rounded)
          mode = 'custom_date'-> set to given date + hour (default old hour)

        keep_end:
          True  -> keep time_end fixed, recompute time_period
          False -> keep time_period fixed, shift time_end
        """
        old_start = self._time_start

        if mode == "midnight":
            new_start = old_start.normalize()

        elif mode == "noon":
            new_start = old_start.normalize() + pd.Timedelta(hours=12)
            if new_start > old_start:
                new_start -= pd.Timedelta(days=1)

        elif mode == "hour":
            if hour is None:
                raise ValueError("When mode='hour', an integer 'hour' (0–23) is required.")
            candidate = old_start.normalize() + pd.Timedelta(hours=int(hour))
            if candidate > old_start:
                candidate -= pd.Timedelta(days=1)
            new_start = candidate

        elif mode == "time_run":
            new_start = self._time_run
            if self._time_rounding is not None:
                new_start = new_start.floor(self._time_rounding)

        elif mode == "custom_date":
            if date is None:
                raise ValueError("When mode='custom_date', 'date' is required.")
            d = pd.to_datetime(date)
            if d.tzinfo is None:
                d = d.tz_localize(self._tz)
            else:
                d = d.tz_convert(self._tz)
            use_hour = self._time_start.hour if hour is None else int(hour)
            new_start = d.normalize() + pd.Timedelta(hours=use_hour)

        else:
            raise ValueError(f"Unknown mode={mode!r} for flatten_start.")

        self._adjust_for_new_start(new_start, keep_end=keep_end)

    def flatten_end(
        self,
        mode: str = "midnight",
        hour: Optional[int] = None,
        date: Optional[Any] = None,
        keep_start: bool = True,
    ) -> None:
        """
        Align/flatten time_end:

          mode = 'midnight'   -> set to next midnight (>= old end)
          mode = 'noon'       -> set to 12:00 same or next day (>= old end)
          mode = 'hour'       -> set to given hour >= old end (possibly next day)
          mode = 'custom_date'-> set to given date + hour (default old hour)

        keep_start:
          True  -> keep time_start fixed, recompute time_period
          False -> keep time_period fixed, shift time_start
        """
        old_end = self._time_end

        if mode == "midnight":
            new_end = (old_end + pd.Timedelta(days=1)).normalize()

        elif mode == "noon":
            candidate = old_end.normalize() + pd.Timedelta(hours=12)
            if candidate < old_end:
                candidate += pd.Timedelta(days=1)
            new_end = candidate

        elif mode == "hour":
            if hour is None:
                raise ValueError("When mode='hour', an integer 'hour' (0–23) is required.")
            candidate = old_end.normalize() + pd.Timedelta(hours=int(hour))
            if candidate < old_end:
                candidate += pd.Timedelta(days=1)
            new_end = candidate

        elif mode == "custom_date":
            if date is None:
                raise ValueError("When mode='custom_date', 'date' is required.")
            d = pd.to_datetime(date)
            if d.tzinfo is None:
                d = d.tz_localize(self._tz)
            else:
                d = d.tz_convert(self._tz)
            use_hour = self._time_end.hour if hour is None else int(hour)
            new_end = d.normalize() + pd.Timedelta(hours=use_hour)

        else:
            raise ValueError(f"Unknown mode={mode!r} for flatten_end.")

        self._adjust_for_new_end(new_end, keep_start=keep_start)

    # ------------------------------------------------------------------ #
    # Representation
    # ------------------------------------------------------------------ #

    def __repr__(self) -> str:
        return (
            f"TimeManager("
            f"time_run={self.time_run!r}, "
            f"time_start={self.time_start!r}, "
            f"time_end={self.time_end!r}, "
            f"time_period={self._time_period}, "
            f"time_frequency={self._time_frequency}, "
            f"time_rounding={self._time_rounding!r}, "
            f"tz={self._tz!r}, "
            f"time_start_template={self._time_start_template!r}, "
            f"time_end_template={self._time_end_template!r}, "
            f"time_as_string={tuple(self.time_as_string)!r}, "
            f"time_as_int={tuple(self.time_as_int)!r}, "
            f"derived_keys={list(self._extra_times.keys())!r})"
        )
