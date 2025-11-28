"""
Class Features

Name:          time_handler
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20251120'
Version:       '0.1.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import warnings
from dataclasses import dataclass, field
from typing import Iterable, Mapping, Any

import numpy as np
import pandas as pd
from tabulate import tabulate
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
@dataclass
class TimeConfig:
    """
    Helper for managing and deriving time information for HMC-like runs.

    Core fields
    -----------
    time_run        : reference run time (usually "now" or forecast base time)
    time_frequency  : pandas-like frequency string (e.g. 'H', '3H', 'D')
    time_period     : integer number of steps (can be derived from start/end)
    time_start      : simulation start time
    time_end        : simulation end time
    time_restart    : restart time (by default 1 step before start)

    Construction modes
    ------------------
    - Direct: pass times / period / frequency to __init__.
    - from_dict: consume raw dict with time_* keys.
    - from_config: consume a ConfigManager, using its LUT values.

    Autocomputation rules
    ---------------------
    If autocompute=True:

      1) time_start
         - if provided → kept as-is
         - else:
             * start_policy == 'from_run'
                 -> time_start = time_run
             * start_policy == 'from_midnight'
                 -> time_start = midnight of (time_run - start_offset_days)
             * start_policy == 'fixed'
                 -> time_start = start_fixed (must be provided)

      2) time_end
         - if time_end is None and time_period is not None:
               time_end = time_start + (time_period - 1) * frequency
         - if time_end is provided and time_period is None:
               time_period = (time_end - time_start) / frequency + 1
               (must be integer; otherwise a warning is raised and rounded)

      3) time_restart
         - if 'time_restart' is in needed and time_restart is None:
               time_restart = time_start + restart_offset_steps * frequency
           (default restart_offset_steps = -1 → one step before start)

    Optional fields
    ---------------
    Some fields may not be required for a given workflow. Use the `needed`
    argument to declare which fields are relevant. Non-needed fields are:

      - not stored in the internal dictionary
      - not computed (even if a rule exists)
      - not shown in view() or as_dict()

    Examples
    --------
    >>> tc = TimeConfig(
    ...     time_run=pd.Timestamp('2025-11-20 12:00'),
    ...     time_frequency='H',
    ...     time_period=6,
    ...     start_policy='from_run',
    ... )
    >>> tc.as_dict()['time_start']
    Timestamp('2025-11-20 12:00:00')
    >>> tc.as_dict()['time_restart']
    Timestamp('2025-11-20 11:00:00')
    """

    # ---- core values (all converted to pandas.Timestamp / int internally) ----
    time_run: Any
    time_frequency: Any = "H"
    time_period: Any | None = None
    time_start: Any | None = None
    time_end: Any | None = None
    time_restart: Any | None = None

    # ---- control over which fields are relevant ----
    needed: Iterable[str] | None = None

    # ---- autocompute behaviour ----
    autocompute: bool = True

    # policy for time_start
    start_policy: str = "from_run"     # 'from_run' | 'from_midnight' | 'fixed'
    start_offset_days: int = 0         # used for 'from_midnight'
    start_fixed: Any | None = None     # used for 'fixed'

    # policy for time_restart (expressed in units of frequency)
    restart_offset_steps: int = -1     # default: 1 step before time_start

    # internal storage (not passed directly by user)
    _values: dict = field(default_factory=dict, init=False, repr=False)

    # class-level definitions
    ALL_KEYS = ("time_run", "time_start", "time_end", "time_restart",
                "time_frequency", "time_period")

    # --------------------------------------------------------------
    def __post_init__(self):
        # normalize needed set
        if self.needed is None:
            self.needed = set(self.ALL_KEYS)
        else:
            self.needed = set(self.needed)

        # store values with normalization
        self._values = {}

        self._set_time("time_run", self.time_run, required=True)
        self._set_freq(self.time_frequency)
        self._set_int("time_period", self.time_period)
        self._set_time("time_start", self.time_start)
        self._set_time("time_end", self.time_end)
        self._set_time("time_restart", self.time_restart)

        if self.autocompute:
            self._autocompute_all()

    # --------------------------------------------------------------
    # basic setters / converters
    def _set_time(self, name: str, value: Any, required: bool = False):
        if name not in self.needed:
            return

        if value is None:
            if required:
                raise ValueError(f"{name} is required but None was provided.")
            self._values[name] = None
            return

        try:
            ts = pd.to_datetime(value)
        except Exception as exc:
            raise ValueError(f"Cannot parse {name}={value!r} as datetime.") from exc

        self._values[name] = ts

    def _set_int(self, name: str, value: Any | None):
        if name not in self.needed:
            return

        if value is None:
            self._values[name] = None
            return

        try:
            iv = int(value)
        except Exception as exc:
            raise ValueError(f"Cannot parse {name}={value!r} as int.") from exc

        self._values[name] = iv

    def _set_freq(self, value: Any):
        """
        Store frequency as a pandas offset-like string (e.g. 'H', '3H').
        """
        if "time_frequency" not in self.needed:
            return

        if value is None:
            raise ValueError("time_frequency cannot be None.")

        # Keep the original string, but validate it through to_offset
        try:
            offset = pd.tseries.frequencies.to_offset(value)
        except Exception as exc:
            raise ValueError(f"Invalid time_frequency={value!r}.") from exc

        # Store the normalized string representation
        self._values["time_frequency"] = str(offset.rule_code or offset.freqstr)

    # --------------------------------------------------------------
    # frequency helpers
    @property
    def freq_offset(self) -> pd.offsets.BaseOffset:
        """
        Return the pandas offset object corresponding to time_frequency.
        """
        f = self._values.get("time_frequency", None)
        if f is None:
            raise ValueError("time_frequency is not available.")
        return pd.tseries.frequencies.to_offset(f)

    # --------------------------------------------------------------
    # autocompute logic
    def _autocompute_all(self):
        """
        Apply all automatic derivations:
          - time_start (according to policy)
          - time_end / time_period
          - time_restart
        """
        self._autocompute_start()
        self._autocompute_end_period()
        self._autocompute_restart()

    def _autocompute_start(self):
        if "time_start" not in self.needed:
            return

        if self._values.get("time_start") is not None:
            return  # user provided

        run = self._values.get("time_run")
        if run is None:
            raise ValueError("time_run is required to derive time_start.")

        if self.start_policy == "from_run":
            self._values["time_start"] = run

        elif self.start_policy == "from_midnight":
            base = run - pd.Timedelta(days=int(self.start_offset_days))
            self._values["time_start"] = base.normalize()

        elif self.start_policy == "fixed":
            if self.start_fixed is None:
                raise ValueError(
                    "start_policy='fixed' requires start_fixed to be provided."
                )
            try:
                self._values["time_start"] = pd.to_datetime(self.start_fixed)
            except Exception as exc:
                raise ValueError(
                    f"Cannot parse start_fixed={self.start_fixed!r} as datetime."
                ) from exc
        else:
            raise ValueError(
                f"Unknown start_policy={self.start_policy!r}. "
                "Use 'from_run', 'from_midnight', or 'fixed'."
            )

    def _autocompute_end_period(self):
        """
        Ensure coherent time_end / time_period pair when possible.
        """
        if "time_start" not in self.needed:
            return

        start = self._values.get("time_start")
        if start is None:
            # cannot derive anything without time_start
            return

        freq = self.freq_offset
        period = self._values.get("time_period") if "time_period" in self.needed else None
        end = self._values.get("time_end") if "time_end" in self.needed else None

        # Case 1: period known, end missing -> derive end
        if period is not None and "time_end" in self.needed and end is None:
            self._values["time_end"] = start + (period - 1) * freq
            return

        # Case 2: end known, period missing -> derive period
        if end is not None and "time_period" in self.needed and period is None:
            delta = end - start
            # how many steps inclusive? (end = start + (n-1)*freq)
            try:
                n_steps_float = delta / freq + 1
            except Exception:
                # fallback: use seconds ratio
                seconds = delta.total_seconds()
                freq_seconds = freq.delta.total_seconds()
                n_steps_float = seconds / freq_seconds + 1

            n_steps_int = int(round(n_steps_float))

            if not np.isclose(n_steps_float, n_steps_int):
                warnings.warn(
                    "time_end - time_start not an exact multiple of frequency; "
                    f"derived time_period={n_steps_float:.3f}, rounded to {n_steps_int}",
                    UserWarning,
                )

            self._values["time_period"] = n_steps_int

    def _autocompute_restart(self):
        if "time_restart" not in self.needed:
            return

        if self._values.get("time_restart") is not None:
            return  # user provided

        start = self._values.get("time_start")
        if start is None:
            # cannot derive restart without a start time
            return

        freq = self.freq_offset
        steps = int(self.restart_offset_steps)
        self._values["time_restart"] = start + steps * freq

    # --------------------------------------------------------------
    # public helpers
    def as_dict(self) -> dict:
        """
        Return a plain dictionary with only the needed keys.
        """
        out = {}
        for k in self.ALL_KEYS:
            if k in self.needed:
                out[k] = self._values.get(k, None)
        return out

    # small convenience properties
    @property
    def time_run_val(self):
        return self._values.get("time_run")

    @property
    def time_start_val(self):
        return self._values.get("time_start")

    @property
    def time_end_val(self):
        return self._values.get("time_end")

    @property
    def time_restart_val(self):
        return self._values.get("time_restart")

    @property
    def time_period_val(self):
        return self._values.get("time_period")

    @property
    def time_frequency_val(self):
        return self._values.get("time_frequency")

    # --------------------------------------------------------------
    # view
    def view(
        self,
        table_format: str = "psql",
        table_print: bool = True,
        table_name: str = "time config",
    ) -> str:
        """
        Show the current time configuration as a table.
        """
        data = self.as_dict()
        df = pd.DataFrame(
            [{"key": k, "value": v} for k, v in data.items()],
        ).set_index("key")

        base = tabulate(
            df,
            headers=["key", "value"],
            tablefmt=table_format,
            showindex=True,
            missingval="N/A",
        )

        lines = base.split("\n")

        # find first border line
        border_idx = None
        for i, line in enumerate(lines):
            if line.startswith("+") and line.endswith("+"):
                border_idx = i
                border_line = line
                break

        if border_idx is None:
            title_line = f"view :: {table_name}"
            final = f"{title_line}\n{base}"
            if table_print:
                print(final)
            return final

        # full-width title row
        table_width = len(border_line)
        inner_width = table_width - 2
        title_text = f" view :: {table_name}"
        title_content = title_text.ljust(inner_width)[:inner_width]
        title_row = "|" + title_content + "|"

        insert_pos = border_idx + 1
        lines.insert(insert_pos, title_row)
        lines.insert(insert_pos + 1, border_line)

        final_table = "\n" + "\n".join(lines) + "\n"

        if table_print:
            print(final_table)

        return final_table

    # --------------------------------------------------------------
    # alternative constructors
    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        needed: Iterable[str] | None = None,
        autocompute: bool = True,
        start_policy: str = "from_run",
        start_offset_days: int = 0,
        start_fixed: Any | None = None,
        restart_offset_steps: int = -1,
    ) -> "TimeConfig":
        """
        Build a TimeConfig from a generic mapping (dict-like).

        Expected keys (if present):
            'time_run', 'time_start', 'time_end',
            'time_restart', 'time_period', 'time_frequency'

        Missing optional keys are left as None and may be derived according
        to the autocompute rules.
        """
        return cls(
            time_run=data.get("time_run"),
            time_frequency=data.get("time_frequency", "H"),
            time_period=data.get("time_period"),
            time_start=data.get("time_start"),
            time_end=data.get("time_end"),
            time_restart=data.get("time_restart"),
            needed=needed,
            autocompute=autocompute,
            start_policy=start_policy,
            start_offset_days=start_offset_days,
            start_fixed=start_fixed,
            restart_offset_steps=restart_offset_steps,
        )

    @classmethod
    def from_config(
        cls,
        cfg,
        *,
        lut_section: str | None = "lut",
        needed: Iterable[str] | None = None,
        autocompute: bool = True,
        start_policy: str = "from_run",
        start_offset_days: int = 0,
        start_fixed: Any | None = None,
        restart_offset_steps: int = -1,
    ) -> "TimeConfig":
        """
        Build a TimeConfig from a ConfigManager-like object.

        The object `cfg` is expected to expose:

            - cfg.get_section("lut")  -> dict with time_* values
              (or alternatively: have attribute `lut`)

        If format/template dictionaries are not available in the cfg object,
        a warning is emitted but default behaviour still applies.

        This method is intentionally lightweight: it simply reads the
        time-related keys from the LUT and delegates all logic to the
        base constructor.
        """

        # try to get LUT dict
        lut = None
        if lut_section is not None:
            try:
                lut = cfg.get_section(lut_section)
            except Exception:
                lut = None

        if lut is None and hasattr(cfg, "lut"):
            lut = getattr(cfg, "lut")

        if lut is None:
            raise ValueError(
                "TimeConfig.from_config could not find a LUT dictionary "
                "(no get_section('lut') result and no cfg.lut attribute)."
            )

        # extract raw values (may be None)
        data = {
            "time_run": lut.get("time_run"),
            "time_start": lut.get("time_start"),
            "time_end": lut.get("time_end"),
            "time_restart": lut.get("time_restart"),
            "time_period": lut.get("time_period"),
            "time_frequency": lut.get("time_frequency", "H"),
        }

        # format/template are optional; warn if not present but not critical
        has_format = hasattr(cfg, "format") or (
            hasattr(cfg, "variables")
            and isinstance(cfg.variables, dict)
            and "format" in cfg.variables
        )
        has_template = hasattr(cfg, "template") or (
            hasattr(cfg, "variables")
            and isinstance(cfg.variables, dict)
            and "template" in cfg.variables
        )

        if not has_format or not has_template:
            warnings.warn(
                "TimeConfig.from_config: 'format' and/or 'template' dictionaries "
                "are not available in the ConfigManager. Default assumptions are used.",
                UserWarning,
            )

        return cls.from_dict(
            data,
            needed=needed,
            autocompute=autocompute,
            start_policy=start_policy,
            start_offset_days=start_offset_days,
            start_fixed=start_fixed,
            restart_offset_steps=restart_offset_steps,
        )
# ----------------------------------------------------------------------------------------------------------------------
