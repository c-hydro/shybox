"""
Class Features

Name:          planning_handler
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20261112'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
from copy import deepcopy
from dataclasses import replace
from inspect import signature
from pathlib import Path
from string import Formatter
import os
import re

import pandas as pd

from shybox.runner_toolkit.datasets.lib_utils_datasets_time import create_time_period as CREATE_TIME_PERIOD_DEFAULT
from shybox.logging_toolkit.logging_handler import LoggingManager

# default settings
DEFAULT_TIME_TAGS = {
    "path_time_run": {"reference": "time_run", "format": "%Y%m%d%H%M",},
    "file_time_run": {"reference": "time_run","format": "%Y%m%d%H%M",},
    "path_time_destination": {"reference": "time","format": "%Y/%m/%d",},
    "file_time_destination": {"reference": "time","format": "%Y%m%d%H%M",},
    **{
        name: {"reference": "source_time", "format": fmt}
        for name, fmt in (
            ("path_time_observed", "%Y/%m/%d"),
            ("file_time_observed", "%Y%m%d%H%M"),
            ("path_time_forecast_nwp", "%Y/%m/%d"),
            ("file_time_forecast_nwp", "%Y%m%d%H%M"),
            ("path_time_forecast_rfarm", "%Y/%m"),
            ("file_time_forecast_rfarm", "%Y%m%d%H%M"),
        )
    },
}

DEFAULT_ENSEMBLE_TAGS = ("ensemble_member", "enseble_member", "member", "member_id", "ensemble_id")

# mandatory settings
VARIABLES_MANDATORY = {'rain', 'air_t', 'inc_rad', 'rel_hum', 'wind_speed'}
TIME_FIELDS_MANDATORY = {
    'time_run', 'time_rounding', 'time_frequency',
    'hour_reference_observed', 'hour_reference_forecast',
    'time_look_observed', 'time_horizon_observed',
    'time_look_forecast', 'time_horizon_forecast',
    'priority_observed', 'priority_forecast',
}
SOURCE_FIELDS_MANDATORY = {
    'path', 'file_name', 'x', 'y', 'time', 'dims', 'nodata', 'format', 'variable',
    'mode', 'ensemble', 'required', 'tolerance_steps', 'missing_policy', 'as_is',
}
DESTINATION_FIELDS_MANDATORY = {
    'path', 'file_name', 'format', 'type', 'dims'
}
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# class dataset planner
class DatasetPlanner:

    GROUPS = {"obs": "observed", "frc": "forecast","observed": "observed", "forecast": "forecast",}

    def __init__(
        self, application, time_tags=None, logger=None, *,
        time_run=None, time_factory=None, time_plan=None, options=None,
        ensemble_tags=None,):

        # set logger
        self.log = LoggingManager.get_logger(logger=logger, name="DatasetPlanner", set_as_current=False,)

        # set application
        self.application = deepcopy(application)
        # set options
        self.options = deepcopy(options) if options is not None else {}

        # set time info
        if time_run is not None:
            self.application["time"]["time_run"] = time_run
        self.time_tags = deepcopy(DEFAULT_TIME_TAGS if time_tags is None else time_tags)

        # set ensemble info
        if ensemble_tags is None:
            ensemble_tags = DEFAULT_ENSEMBLE_TAGS
        if isinstance(ensemble_tags, str):
            raise TypeError("ensemble_tags must be a collection of placeholder names.")
        self.ensemble_tags = frozenset(ensemble_tags)
        if not self.ensemble_tags or any(not isinstance(tag, str) or not tag.isidentifier() for tag in self.ensemble_tags):
            raise ValueError("ensemble_tags must contain nonempty placeholder identifiers.")

        # set class variable(s)
        self._time_factory = time_factory
        self._time_plan = None
        self._base_time_plan = None
        self._base_time_table = None
        self._time_options = {}
        self._file_plan = None
        self.check_count = 0

        # call method to organize source and destination files/variables
        self._discover()
        # call method to organize time(s)
        if time_plan is not None:
            self.configure_time(time_plan=time_plan)

    # instantiate class using an application resolved in shybox
    @classmethod
    def from_application(cls, application, **kwargs):
        return cls(application, **kwargs)

    # instantiate class using a configuration resolved in shybox
    @classmethod
    def from_config(cls,
            info, time, data_src_obs, data_src_frc,
            data_dst_obs, data_dst_frc, time_run=None,
            *,
            logger: LoggingManager | None = None,
            geo=None, time_tags=None, options=None, **kwargs):

        # iterate over needed object(s)
        for name, value in (('info', info), ('time', time),
                            ('data_src_obs', data_src_obs), ('data_src_frc', data_src_frc),
                            ('data_dst_obs', data_dst_obs), ('data_dst_frc', data_dst_frc)):
            # exit with errors (if one section is omitted)
            if not isinstance(value, dict):
                raise TypeError(f'DatasetPlanner {name} must be a dictionary.')

        # check missing time fields
        missing_fields_time = TIME_FIELDS_MANDATORY - time.keys()
        if missing_fields_time:
            raise ValueError(f'Missing dataset time fields: {sorted(missing_fields_time)}')

        # iterate over source obs/frc object(s)
        for group, data in (('observed', data_src_obs), ('forecast', data_src_frc)):
            for name, obj in data.items():
                if not isinstance(obj, dict):
                    raise TypeError(f'{group}.{name} must be a dictionary.')
                # check missing source fields
                missing_fields_src = SOURCE_FIELDS_MANDATORY - obj.keys()
                if missing_fields_src:
                    raise ValueError(f'{group}.{name}: missing source fields: {sorted(missing_fields_src)}')

        # iterate over destination obs/frc object(s)
        for group, data in (('observed', data_dst_obs), ('forecast', data_dst_frc)):
            for name, obj in data.items():
                if not isinstance(obj, dict):
                    raise TypeError(f'{group}.{name} must be a dictionary.')
                # check missing destination fields
                missing_fields_dst = DESTINATION_FIELDS_MANDATORY - obj.keys()
                if missing_fields_dst:
                    raise ValueError(f'{group}.{name}: missing destination fields: {sorted(missing_fields_dst)}')

        # define the application dataset
        application_settings = {
            'info': info, 'time': time,
            'data_source': {'observed': data_src_obs, 'forecast': data_src_frc},
            'data_destination': {'observed': data_dst_obs, 'forecast': data_dst_frc}}

        # add if available the geo object
        if geo is not None:
            application_settings['geo'] = geo if 'terrain' in geo else {'terrain': geo}

        return cls(
            application_settings, time_tags=time_tags, logger=logger,
            time_run=time_run, options=options, **kwargs,
        )

    # helper to create time_tags (template and format from luts)
    @staticmethod
    def create_time_tags(template, formats, *, references=None):

        # iterate over templates and formats
        for name, value in (('template', template), ('formats', formats)):
            # check value format (dict is requested)
            if not isinstance(value, dict):
                raise TypeError(f'{name} must be a dictionary.')

        # check reference format
        if references is None:
            references = {}
        if not isinstance(references, dict):
            raise TypeError('references must be a dictionary.')

        # check reference
        valid_references = {'time_run', 'forecast_run', 'time', 'source_time'}
        for name, reference in references.items():
            if not isinstance(reference, str) or reference not in valid_references:
                raise ValueError(f'{name}: unsupported time reference {reference!r}.')
            if formats.get(name) != 'time':
                raise ValueError(f'{name}: custom time tags must have format "time".')

        # manage templates formats and time for
        tags = {}
        for name, kind in formats.items():
            if kind != 'time' or not (name.startswith(('path_time_', 'file_time_')) or name in references):
                continue
            date_format = template.get(name)

            if not isinstance(date_format, str) or not date_format.strip():
                raise ValueError(f'{name}: a nonempty time template is required.')
            if name in ('path_time_run', 'file_time_run'):
                reference = 'time_run'
            elif name in ('path_time_destination', 'file_time_destination') or name.endswith('_valid'):
                reference = 'time'
            else:
                reference = 'source_time'

            # add time tags to common object
            tags[name] = {'reference': references.get(name, reference), 'format': date_format}

        return tags

    # method to discover variables and files
    def _discover(self):

        # set sources and destinations dictionary
        self._sources = {group: {} for group in ("observed", "forecast")}
        self._destinations = {group: {} for group in self._sources}

        # iterate over sources
        records, common_members = [], None
        for group, sources in self.application["data_source"].items():

            # search group in sources (if not raise an error)
            if group not in self._sources:
                raise ValueError(f"Unknown source group {group!r}.")

            # iterate over entries (variables)
            for entry, cfg in sources.items():

                # set label and mode (deterministic/probabilistic)
                label = f"{group}.{entry}"
                mode = cfg.get("mode", "deterministic")

                # check mode string
                if mode not in ("deterministic", "probabilistic", "probabilistic_ensemble"):
                    raise ValueError(f"{label}: unsupported mode {mode!r}.")

                # check probabilistic mode
                probabilistic = mode != "deterministic"
                # define probabilistic members
                members = (self._members(cfg, label) if probabilistic else ())

                # define file path
                path = os.path.join(cfg["path"], cfg["file_name"])
                # find file placeholders
                has_member = self._has_ensemble_tag(path)
                if has_member != probabilistic:
                    raise ValueError(f"{label}: an ensemble placeholder from {sorted(self.ensemble_tags)} must match probabilistic mode.")
                # check deterministic and ensemble declaration
                if not probabilistic and cfg.get("ensemble") is not None:
                    raise ValueError(f"{label}: deterministic source declares an ensemble.")
                # check members related to the members common (store the member if more of one variable is probabilistic)
                if members:
                    if common_members is not None and common_members != members:
                        raise ValueError("Probabilistic variables must use the same member range and format.")
                    common_members = members

                # get variable mapping
                mapping = cfg.get("variable")
                if not isinstance(mapping, dict) or not mapping:
                    raise ValueError(f"{label}: variable must be a nonempty mapping.")
                # iterate over variable(s), usually one variable for one dataset
                for file_variable, variable in mapping.items():
                    if variable in self._sources[group]:
                        raise ValueError(f"{group}: duplicate canonical variable {variable!r}.")
                    # add variables in sources
                    self._sources[group][variable] = (file_variable, cfg, members,)

                    # store information in records
                    records.append({
                        "group": group, "entry": entry,
                        "variable": variable, "source_name": file_variable,
                        "mode": mode, "ensemble_members": members,
                    })

        # get destinations (or collections or variables format)
        destinations = self.application["data_destination"]

        # support shared collections, as in your JSON, or sections
        # explicitly separated into observed and forecast.
        by_group = bool(set(destinations) & {"observed", "forecast"})
        # iterate over destination expected group(s)
        for group in self._destinations:
            # define destination collections (by collections or variables)
            collections = (destinations.get(group, {}) if by_group else destinations)

            if "file_name" in collections:
                collections = {"collection": collections}

            # iterate over entries (variables)
            for entry, cfg in collections.items():
                for variable, output_name in cfg["variable"].items():
                    if variable in self._destinations[group]:
                        raise ValueError(f"{group}: duplicate destination variable {variable!r}.")

                    # add variables in destinations
                    self._destinations[group][variable] = (entry, output_name,cfg,)

            # check missing variables sources/destinations
            missing = (self._sources[group].keys() - self._destinations[group].keys())
            if missing:
                raise ValueError(f"{group}: source variables without destinations: {sorted(missing)}")

        # store members, mode and records
        self.members = common_members or ()
        self.mode = ("probabilistic_ensemble" if self.members else "deterministic")
        self._variables = pd.DataFrame(records)

    # helper to generate members for probabilistic mode
    @staticmethod
    def _members(cfg, label):

        # get members configuration
        spec = cfg.get("ensemble")
        if not isinstance(spec, dict):
            raise ValueError(f"{label}: ensemble requires start, end and format.")

        # get members start, end and format
        start, end, fmt = spec.get("start"), spec.get("end"), spec.get("format")
        if (isinstance(start, bool) or isinstance(end, bool) or
                not isinstance(start, (int, str)) or not isinstance(end, (int, str))):
            raise ValueError(f"{label}: ensemble range must be inclusive nonnegative integers.")

        # convert start and end to integer
        try:
            start, end = int(start), int(end)
        except ValueError as error:
            raise ValueError(f"{label}: ensemble range must be inclusive nonnegative integers.") from error
        # check members range
        if start < 0 or end < start:
            raise ValueError(f"{label}: ensemble range must be inclusive nonnegative integers.")
        # check members format
        if not isinstance(fmt, str) or not fmt:
            raise ValueError(f"{label}: ensemble format is required, e.g. '03d'.")

        # generate members
        members = tuple(format(number, fmt) for number in range(start, end + 1))
        # check members
        if len(set(members)) != len(members) or any(not item.isdigit() for item in members):
            raise ValueError(f"{label}: ensemble format must produce unique numeric IDs.")

        return members

    # property variable(s)
    @property
    def variables(self):
        return self._variables.copy(deep=True)

    # helper to configure time
    def configure_time(self, time_run=None, *,time_plan=None, **options):

        """Reuse a TimePeriod or call the existing SHYBOX time factory."""
        if time_plan is not None and (time_run is not None or options):
            raise ValueError("Pass either a time_plan or time-generation options.")

        # create time plan if not available
        if time_plan is None:

            # define time factory
            factory = self._time_factory
            # use a different factory for times if not defined
            if factory is None:
                factory = CREATE_TIME_PERIOD_DEFAULT

            # get time info from application object
            cfg = deepcopy(self.application["time"])
            # update time info using options (if defined)
            cfg.update(options)

            # set time_run
            if time_run is not None:
                cfg["time_run"] = time_run

            # iterate over sources
            for group in self._sources:
                modes = {self._reference_mode(source[1]) for source in self._sources[group].values()}
                cfg.setdefault("reference_mode_" + group, "3d" if "3d" in modes else "step",)

            # configure of time info
            cfg = {key: value.strip() if isinstance(value, str) else value for key, value in cfg.items()}
            # configure params related to time factory used by the algorithm
            params = signature(factory).parameters
            accepts_kwargs = any(parameter.kind == parameter.VAR_KEYWORD for parameter in params.values())

            # check factory unkwnonw options
            unknown = options.keys() - params.keys()
            if unknown and not accepts_kwargs:
                raise ValueError(f"Unknown time options: {sorted(unknown)}")

            # define factory args
            arguments = {key: value for key, value in cfg.items() if accepts_kwargs or key in params}
            # create factory times
            time_plan = factory(**arguments)
            self._time_options = arguments
        else:
            self._time_options = {}

        # get time table from time planning (copy the object)
        table = time_plan.table.copy(deep=True)

        # check table index
        if not isinstance(table.index, pd.DatetimeIndex) or table.index.hasnans or not table.index.is_unique:
            raise ValueError("Time table requires a unique DatetimeIndex without missing times.")
        # check table utc
        if table.index.tz is not None:
            raise ValueError("Time table must use naive UTC timestamps.")
        # check table time_mode column
        if "time_mode" not in table or not table["time_mode"].isin(self.GROUPS).all():
            raise ValueError("Time table time_mode must contain obs/frc or observed/forecast.")

        # remap time mode to obtain the same observed/forecast tags as in other object
        table["time_mode"] = table["time_mode"].map(self.GROUPS)

        # set to common object(s)
        self._time_plan = time_plan
        self._time_table = table
        self._base_time_plan = deepcopy(time_plan)
        self._base_time_table = table.copy(deep=True)
        self._file_plan = None
        self.check_count = 0

        return self.time_table

    # property time table
    @property
    def time_table(self):
        if self._time_plan is None:
            self.configure_time()
        return self._time_table.copy(deep=True)

    # property time plan
    @property
    def time_plan(self):
        if self._time_plan is None:
            self.configure_time()
        return deepcopy(self._time_plan)

    # property time period
    @property
    def time_period(self):
        return self.time_table.index.copy()

    def _required_variables(self, group):
        return (VARIABLES_MANDATORY.intersection(self._destinations[group]) |
                {key for key, (_, cfg, _) in self._sources[group].items() if cfg.get("required", False)})

    @staticmethod
    def _required_available(files, stamp, required):
        at_time = files.loc[files.time == stamp]
        if at_time.empty:
            return False
        rows = at_time.loc[at_time.variable.isin(required)]
        return bool(required.issubset(set(rows.variable)) and
                    rows.source_exists.eq(True).fillna(False).all())

    # method to update time (to check if observed/forecast file are available or not)
    def update_time(
        self, n_step_to_check_obs=None, n_step_to_check_frc=None, *,
        flag_adjust_obs_frc=True, rules_to_fill_obs="fill_using_frc", rules_to_fill_frc=None,
        n_steps_observed=None, n_steps_forecast=None, exists=None,
        reset_history=True, flag_history=True, pickle_history="plans_file.workspace",):

        """Refresh availability, or reuse a matching local workspace.

        History is enabled by default. Reset recomputes and replaces the saved
        plan; without reset, a compatible workspace restores the time/file
        plans and their saved availability without probing dataset files.
        Missing, incompatible or unreadable history is rebuilt. Disable
        ``flag_history`` to avoid history I/O. Only load trusted pickle files.
        """
        import hashlib
        import pickle
        import tempfile

        for name, flag in (("reset_history", reset_history), ("flag_history", flag_history)):
            if not isinstance(flag, bool):
                raise TypeError(f"{name} must be a boolean.")

        # iterate over args
        limits = []
        for name, value, alias in (
            ("n_step_to_check_obs", n_step_to_check_obs, n_steps_observed),
            ("n_step_to_check_frc", n_step_to_check_frc, n_steps_forecast),
        ):
            # check limits format and values
            for limit in (value, alias):
                if limit is not None and (isinstance(limit, bool) or not isinstance(limit, int) or limit < 0):
                    raise ValueError(f"{name} must be a nonnegative integer or None.")
            if value is not None and alias is not None and value != alias:
                raise ValueError(f"Conflicting step limits for {name}.")
            limits.append(value if value is not None else alias)

        # define obs/frc limits steps to check by the procedure
        n_obs, n_frc = limits
        if not isinstance(flag_adjust_obs_frc, bool):
            raise TypeError("flag_adjust_obs_frc must be a boolean.")
        if rules_to_fill_obs not in (None, "fill_using_frc"):
            raise ValueError('rules_to_fill_obs must be "fill_using_frc" or None.')
        if rules_to_fill_frc is not None:
            raise ValueError("rules_to_fill_frc must be None.")

        # configure time (if time plan is not defined)
        if self._time_plan is None:
            self.configure_time()

        # get the plan and table time
        base = self._base_time_plan
        table = self._base_time_table.copy(deep=True)

        # History must match the configuration, base times and requested checks.
        history_path = Path(pickle_history).expanduser() if flag_history else None
        if history_path is not None:
            context = (
                self.application, self.options, self.time_tags, sorted(self.ensemble_tags),
                base, table, self._time_options, n_obs, n_frc, flag_adjust_obs_frc,
                rules_to_fill_obs, rules_to_fill_frc, str(Path.cwd()),
                os.path.expandvars(repr(self.application)),
            )
            history_key = hashlib.sha256(pickle.dumps(context, protocol=pickle.HIGHEST_PROTOCOL)).hexdigest()
            self.log.info(f"Time history: {history_path}; available={history_path.is_file()}, "
                          f"reset={reset_history}")
            if not reset_history and history_path.is_file():
                try:
                    with history_path.open("rb") as stream:
                        history = pickle.load(stream)
                    if history.get("version") == 1 and history.get("key") == history_key:
                        plan, files = history["time_plan"], history["file_plan"]
                        count = history["check_count"]
                        if (not isinstance(plan, type(base)) or not isinstance(files, pd.DataFrame)
                                or not isinstance(count, int) or count < 0
                                or not plan.table.index.equals(table.index)
                                or not {"time_checked", "time_available", "time_adjusted"} <= set(plan.table)
                                or not {"time", "file_source", "file_destination", "source_exists",
                                        "destination_exists", "time_checked", "time_available",
                                        "time_adjusted"} <= set(files)):
                            raise ValueError("Incomplete saved time/file plan.")
                        self._time_plan = plan
                        self._time_table = plan.table.copy(deep=True)
                        self._file_plan = files
                        self.check_count = count
                        self.log.info(f"Time history loaded from {history_path} "
                                      f"(saved {history['saved_at']}); reusing {len(files)} file-plan rows "
                                      "without new availability checks")
                        return self.time_plan
                    self.log.info("Time history does not match the current planning settings; rebuilding")
                except (OSError, pickle.PickleError, EOFError, AttributeError, ImportError,
                        ValueError, TypeError, KeyError) as exc:
                    self.log.warning(f"Cannot reuse time history {history_path}: {exc}; rebuilding")
        else:
            self.log.info("Time history disabled; updating availability")

        # pass to the property
        self._time_plan = deepcopy(base)
        self._time_table = table.copy(deep=True)

        # select steps
        steps = table.time_step

        # select the checked window
        window = pd.Series(False, index=table.index)
        if n_obs is not None:
            window |= steps.between(-n_obs, 0)
        if n_frc is not None:
            window |= steps.between(0, n_frc)

        # extract the checked times
        checked_times = table.index[window]
        # info about the checked time/steps
        self.log.info(f"Update time: observed window={n_obs}, forecast window={n_frc}; "
            f"checking {len(checked_times)} of {len(table)} steps")

        # One fresh cache covers original files, candidates and the final plan.
        probe = Path.is_file if exists is None else exists
        cache = {}

        # call cached file(s)
        def cached_probe(path):
            key = str(path)
            if key not in cache:
                cache[key] = bool(probe(path))
            return cache[key]

        def checked_files(times):
            self.plan_files()
            return self.check_availability(exists=cached_probe, check_times=times)

        # check availability files
        files = checked_files(checked_times)
        required = {group: self._required_variables(group) for group in self._sources}

        # try to adjust observed/forecast steps using a predefined rules
        replacements = []
        if flag_adjust_obs_frc and rules_to_fill_obs == "fill_using_frc" and self._sources["forecast"]:
            trailing = []
            observed_times = table.index[table.time_mode.eq("observed")]
            for stamp in observed_times[::-1]:
                if stamp not in checked_times or self._required_available(files, stamp, required["observed"]):
                    break
                trailing.append(stamp)

            # check for observed time files that are not available
            if trailing:
                candidate = table.copy(deep=True)
                candidate.loc[trailing, "time_mode"] = "forecast"
                candidate.loc[trailing, "time_reference_step"] = pd.DatetimeIndex(trailing)
                candidate.loc[trailing, "time_reference_3d"] = base.forecast_run
                forecast_mode = self._time_options.get(
                    "reference_mode_forecast", self.application["time"].get("reference_mode_forecast"))
                if forecast_mode is None:
                    forecast_mode = "3d" if any(
                        self._reference_mode(source[1]) == "3d" for source in self._sources["forecast"].values()
                    ) else "step"
                candidate.loc[trailing, "time_reference"] = candidate.loc[
                    trailing, "time_reference_" + forecast_mode]

                # update property time table using the candidate
                self._time_table = candidate

                # check availability candidates
                candidates = checked_files(trailing)

                # replace (if selected) the step using the candidates
                needed = required["observed"] | required["forecast"]
                replacements = [stamp for stamp in trailing if self._required_available(candidates, stamp, needed)]
                table.loc[replacements] = candidate.loc[replacements]

                # print the message for observed step replaced by the forecast
                for stamp in replacements:
                    self.log.info(f"Time step {steps.loc[stamp]:+d} at {stamp}: observed missing; using forecast")

        # update table
        table["time_checked"] = window
        table["time_adjusted"] = table.index.isin(replacements)
        table["time_available"] = pd.Series(pd.NA, index=table.index, dtype="boolean")

        # update the object related to the new table settings
        observed = table.index[table.time_mode.eq("observed")]
        forecast = table.index[table.time_mode.eq("forecast")]
        self._time_plan = replace(base, observed=observed, forecast=forecast, table=table)
        self._time_table = self._time_plan.table.copy(deep=True)

        # check availability updated
        refreshed = checked_files(checked_times)

        # set time_available column to the time_table
        for stamp in checked_times:
            group = self._time_table.loc[stamp, "time_mode"]
            self._time_table.loc[stamp, "time_available"] = self._required_available(refreshed, stamp, required[group])

        # update plan and table time
        self._time_plan = replace(self._time_plan, table=self._time_table)
        self._time_table = self._time_plan.table.copy(deep=True)
        # update file plan
        for column in ("time_checked", "time_available", "time_adjusted"):
            self._file_plan[column] = self._file_plan.time.map(self._time_table[column])

        # verify count and availability
        self.check_count = len(cache)
        available = int(self._time_table.time_available.fillna(False).sum())

        # print information
        self.log.info(
            f"Time update complete: checked={len(checked_times)}, available={available}, "
            f"unavailable={len(checked_times) - available}, forecast replacements={len(replacements)}, "
            f"unchecked={len(table) - len(checked_times)}, unique file checks={self.check_count}")

        # Publish a complete workspace atomically, keeping any previous file on failure.
        if history_path is not None:
            temporary_path = None
            try:
                history_path.parent.mkdir(parents=True, exist_ok=True)
                with tempfile.NamedTemporaryFile(mode="wb", dir=history_path.parent,
                                                 prefix=history_path.name + ".", delete=False) as stream:
                    temporary_path = Path(stream.name)
                    pickle.dump({"version": 1, "key": history_key,
                                 "saved_at": pd.Timestamp.now(tz="UTC").isoformat(),
                                 "time_plan": self._time_plan, "file_plan": self._file_plan,
                                 "check_count": self.check_count}, stream, protocol=pickle.HIGHEST_PROTOCOL)
                os.replace(temporary_path, history_path)
                self.log.info(f"Time history saved to {history_path}")
            except (OSError, pickle.PickleError, TypeError) as exc:
                self.log.warning(f"Cannot save time history {history_path}: {exc}")
            finally:
                if temporary_path is not None and temporary_path.exists():
                    temporary_path.unlink()

        # return
        return self.time_plan

    # method to set reference mode
    @staticmethod
    def _reference_mode(cfg):
        mode = cfg.get("reference_mode") or (
            "3d" if cfg.get("dims") == "3d" or (cfg.get("operations") or {}).get("split") else "step")
        if mode not in ("step", "3d"):
            raise ValueError(f"Unsupported reference_mode {mode!r}.")
        return mode

    # helper to search an ensemble tag (as defined in the ensemble names object)
    def _has_ensemble_tag(self, template):
        return any(field in self.ensemble_tags for _, field, _, _ in Formatter().parse(template))

    # method to fill file_name placeholder(s)
    def fill_file_name(self, cfg, time, *, ensemble_member=None, source=True,):

        if self._time_plan is None:
            self.configure_time()

        table = self._time_table
        stamp = pd.Timestamp(time)
        row = table.loc[stamp]

        if source:
            column = ("time_reference_" + self._reference_mode(cfg))
            if column not in table:
                raise ValueError(f"Time table must contain {column!r}; references are not recomputed.")

            reference = row[column]
            if pd.isna(reference):
                raise ValueError(f"Missing {column} at {stamp}.")
        else:
            reference = stamp

        tags = {
            **row.to_dict(),
            "time": stamp,
            "source_time": reference,
            "time_run": self._time_plan.time_run,
            "forecast_run": getattr(self._time_plan, "forecast_run", None),
            "ensemble_member": ensemble_member,
        }

        # iterate over time tags
        for name, declaration in self.time_tags.items():
            value = tags.get(declaration["reference"])

            if value is not None and not pd.isna(value):
                tags[name] = pd.Timestamp(value).strftime(declaration["format"])

        # manage tags
        tags.update({name: ensemble_member for name in self.ensemble_tags})

        # get rax path
        raw = os.path.expandvars(os.path.expanduser(os.path.join(cfg["path"], cfg["file_name"])))
        if re.search(r"\$(?:\w+|\{[^}]+\})", raw):
            raise ValueError(f"Unresolved environment variable in {raw!r}.")

        # check ensemble tags
        if self._has_ensemble_tag(raw) and ensemble_member is None:
            raise ValueError("An ensemble placeholder requires a member.")

        # try to return the path defined by tags
        try:
            return str(Path(raw.format_map(tags)).resolve())
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError(f"Cannot fill file template {raw!r} at {stamp}: {error}") from error

    # method to plan files for each step of time_period
    def plan_files(self, check_exists=False, *, check_times=None):

        # iterate for all rows in time_table
        rows = []
        for stamp, time_row in self.time_table.iterrows():
            group = self.GROUPS[time_row["time_mode"]]

            # iterate over destinations
            for variable, destination_info in (self._destinations[group].items()):

                collection, output_name, destination = (destination_info)
                source = self._sources[group].get(variable)

                source_name, cfg, members = (source if source else (None, None, ()))

                # define dims to select the right time stamp
                if cfg is not None and 'dims' not in cfg:
                    raise RuntimeError(f'Variable {variable} has not dimensions defined in planning files')
                # check dims values
                dims = cfg.get('dims') if cfg is not None else None
                if cfg is not None and dims not in ['2d', '3d']:
                    raise ValueError(f"Variable {variable} has not dimensions {dims} expected in planning files")

                for member in members or (None,):
                    path = (self.fill_file_name(cfg, stamp, ensemble_member=member,) if cfg else None)

                    reference = (time_row["time_reference_" + self._reference_mode(cfg)] if cfg else pd.NaT)
                    output_members = ((member,) if member is not None else self.members or (None,))
                    destination_template = os.path.join(destination["path"], destination["file_name"],)

                    if not self._has_ensemble_tag(destination_template):
                        output_members = (member,)

                    for output_member in output_members:
                        output = self.fill_file_name(destination, stamp, ensemble_member=output_member, source=False,)

                        # defined row information
                        rows.append({
                            "time": stamp,
                            "step": time_row.get("step", pd.NA),
                            "file_id": time_row.get("file_id", pd.NA),
                            "time_step": time_row.get("time_step", pd.NA),
                            "time_mode": time_row["time_mode"],
                            "time_checked": time_row.get("time_checked", False),
                            "time_available": time_row.get("time_available", pd.NA),
                            "time_adjusted": time_row.get("time_adjusted", False),
                            "group": group,
                            "time_reference": reference,
                            "variable": variable,
                            "source_name": source_name,
                            "output_name": output_name,
                            "mode": (cfg.get("mode", "deterministic") if cfg else None),
                            "ensemble_member": member,
                            "output_member": output_member,
                            "collection": collection,
                            "file_source": path,
                            "file_destination": output,
                        })

        columns = [
            "time",
            "step",
            "file_id",
            "time_step",
            "time_mode",
            "time_checked",
            "time_available",
            "time_adjusted",
            "group",
            "time_reference",
            "variable",
            "source_name",
            "output_name",
            "mode",
            "ensemble_member",
            "output_member",
            "collection",
            "file_source",
            "file_destination",
        ]

        self._file_plan = pd.DataFrame(rows, columns=columns)
        for kind in ("source", "destination"):
            self._file_plan[kind + "_exists"] = pd.Series(pd.NA,index=self._file_plan.index, dtype="boolean",)

        self.check_count = 0
        if check_exists:
            return self.check_availability(check_times=check_times)

        return self.file_plan

    @property
    def file_plan(self):
        if self._file_plan is None:
            self.plan_files()
        return self._file_plan.copy(deep=True)

    # method to check availability
    def check_availability(self, *, exists=None, check_times=None):
        """Check each unique absolute path once per scan.

        The cache spans variables, times, members and both source
        and destination files. Missing files are cached as well.

        A new call starts a fresh scan to detect arriving files.
        Only rows at check_times are scanned; other rows remain unknown.
        None checks every row, while an empty collection checks nothing.
        """
        if check_times is not None and not pd.api.types.is_list_like(check_times):
            raise TypeError(
                "check_times must be a collection of timestamps or None, not a scalar. "
                "Use update_time(n_step_to_check_obs=3, n_step_to_check_frc=1) "
                "to check a bounded window.")
        if self._file_plan is None:
            self.plan_files()

        probe = Path.is_file if exists is None else exists
        selected = (self._file_plan.time.isin(check_times) if check_times is not None
                    else pd.Series(True, index=self._file_plan.index))
        frame = self._file_plan.loc[selected]
        paths = pd.concat([frame.file_source, frame.file_destination]).dropna().unique()
        cache = {path: bool(probe(Path(path))) for path in paths}

        self.check_count = len(cache)
        self._file_plan["time_checked"] = selected
        for kind in ("source", "destination"):
            self._file_plan[kind + "_exists"] = pd.Series(pd.NA, index=self._file_plan.index, dtype="boolean")
            self._file_plan.loc[selected, kind + "_exists"] = (
                frame["file_" + kind]
                .map(
                    lambda path: (
                        cache[path] if path is not None else False
                    )
                )
                .astype("boolean")
            )

        return self.file_plan

    # method to creat file table (for each step/file/variable)
    def file_table(self, kind="source"):
        """Return one row per physical file and observed/forecast mode."""
        if kind not in ("source", "destination"):
            raise ValueError("kind must be source or destination.")

        frame = self.file_plan
        rows = []
        grouped = frame.dropna(subset=["file_" + kind]).groupby(["file_" + kind, "time_mode"], sort=False)

        name_column = ("source_name" if kind == "source" else "output_name")
        for (path, time_mode), group in grouped:
            known = group[kind + "_exists"].dropna()
            rows.append({
                "file_name": path,
                "time_mode": time_mode,
                "variables": tuple( group.variable.unique()),
                "file_variables": tuple(group[name_column].unique()),
                "times": tuple(group.time.unique()),
                "ensemble_members": tuple(group[
                    "ensemble_member" if kind == "source" else "output_member"
                ].dropna().unique()),
                "file_exists": known.iloc[0] if len(known) else pd.NA,
            })

        return pd.DataFrame(
            rows,
            columns=["file_name", "time_mode", "variables", "file_variables", "times",
                     "ensemble_members", "file_exists",],
        )

    # method to tag file as available
    def file_available(self, kind="source"):
        """Return checked physical files that exist; exclude unchecked files."""
        table = self.file_table(kind)
        return table.loc[table.file_exists.eq(True).fillna(False)].reset_index(drop=True)
    # method to tag file as not available
    def file_not_available(self, kind="source"):
        """Return checked missing physical files; exclude unchecked files."""
        table = self.file_table(kind)
        return table.loc[table.file_exists.eq(False).fillna(False)].reset_index(drop=True)

    # organize file by ensemble (if not ensemble tag to deterministic)
    def file_by_ensemble(self, kind="source"):

        if kind not in ("source", "destination"):
            raise ValueError("kind must be source or destination.")
        result = {}
        for row in self.file_plan.sort_values("time", kind="stable").itertuples(index=False):
            member = row.output_member if pd.notna(row.output_member) else row.ensemble_member
            members = (str(member),) if pd.notna(member) else self.members or ("deterministic",)
            stamp = str(pd.Timestamp(row.time))
            path = getattr(row, "file_" + kind)
            for member in members:
                variables = result.setdefault(member, {}).setdefault(stamp, {})
                if row.variable in variables and variables[row.variable] != path:
                    raise ValueError(f"{member}.{stamp}.{row.variable}: conflicting {kind} filenames.")
                variables[row.variable] = path
        return result
# ----------------------------------------------------------------------------------------------------------------------
