"""
Class Features

Name:          dataset_handler_base
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20251104'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
logging.getLogger('matplotlib.font_manager').setLevel(logging.WARNING)

from typing import Optional, Generator, Union, Tuple, Literal
import datetime as dt
import numpy as np
import pandas as pd
import xarray as xr

from types import SimpleNamespace
from pathlib import Path

from copy import deepcopy
from abc import ABC, ABCMeta, abstractmethod
import os
import re

from shybox.dataset_toolkit.dataset_handler_utils import make_namespaces
from shybox.logging_toolkit.logging_handler import LoggingManager
from shybox.default.lib_default_geo import crs_wkt as default_crs_wkt

from shybox.dataset_toolkit.lib_dataset_parse import substitute_string, extract_date_and_tags
from shybox.dataset_toolkit.lib_dataset_generic import (
    get_format_from_path, map_dims, map_coords, map_vars, flat_dims,
    straighten_data, straighten_time, straighten_dims, select_by_time, rename_da_by_template, select_da_by_mapping,
    set_type, check_data_format, select_variable)
from shybox.generic_toolkit.lib_utils_debug import plot_data
from shybox.logging_toolkit.lib_logging_utils import with_logger
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# ancillary decorators and classes
def with_cases(func):
    def wrapper(*args, **kwargs):
        if 'cases' in kwargs:
            cases = kwargs.pop('cases')
            if cases is not None:
                return [func(*args, **case['tags'], **kwargs) for case in cases]
        else:
            return func(*args, **kwargs)
    return wrapper

class DatasetMeta(ABCMeta):
    def __init__(cls, name, bases, attrs):
        super().__init__(name, bases, attrs)

        if not hasattr(cls, 'subclasses'):
            cls.subclasses = {}
        elif 'type' in attrs:
            cls.subclasses[attrs['type']] = cls
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# class dataset
class Dataset(ABC, metaclass=DatasetMeta):

    # default attributes
    _defaults = {
        'type': None, 'time_signature' : 'end', "workflow": 'undefined', "layout": 'undefined',
        'mode': 'local', 'format': 'tmp', 'variable': 'undefined'}

    def __init__(self, **kwargs):

        # ensure logging always works (to console)
        logger = kwargs.pop('logger', None)
        self.logger = logger or LoggingManager(name="Dataset")

        # ensure message (active or not)
        self.message = kwargs.pop('message', True)

        # info dataset initialization start
        if self.message:
            self.logger.info(f'Dataset obj {self.__str__()} ... ')

        # define loc_pattern BEFORE using it
        self.loc_pattern = kwargs.pop('loc_pattern', None)

        # substitute "now" with the current time
        if self.loc_pattern is not None:
            self.loc_pattern = substitute_string(self.loc_pattern, {'now': dt.datetime.now()})

        if 'file_name' in kwargs:
            self.file_name = kwargs.pop('file_name')

        self.file_format = self._defaults['format']
        if 'file_format' in kwargs:
            self.file_format = kwargs.pop('file_format')
        else:
            if self.loc_pattern is not None:
                self.file_format = get_format_from_path(self.loc_pattern)

        self.file_mode = self._defaults['mode']
        if 'file_mode' in kwargs:
            self.file_mode = kwargs.pop('file_mode')

        self.file_type = self._defaults['type']
        if 'file_type' in kwargs:
            self.file_type = kwargs.pop('file_type')

        self.file_variable = self._defaults['variable']
        if 'file_variable' in kwargs:
            self.file_variable = kwargs.pop('file_variable')

        self.time_signature = self._defaults['time_signature']
        if 'time_signature' in kwargs:
            self.time_signature = kwargs.pop('time_signature')

        self.nan_value = None
        if 'nan_value' in kwargs:
            self.nan_value = kwargs.pop('nan_value')

        self.data_layout = None
        if 'data_layout' in kwargs:
            self.data_layout = kwargs.pop('data_layout')

        self.variable_template = {}
        if 'variable_template' in kwargs:
            self.variable_template = kwargs.pop('variable_template')

        self.time_reference, self.time_freq, self.time_period, self.time_direction = None, None, None, None
        if 'time_reference' in kwargs:
            self.time_reference = kwargs.pop('time_reference')
        if 'time_freq' in kwargs:
            self.time_freq = kwargs.pop('time_freq')
        if 'time_direction' in kwargs:
            self.time_direction = kwargs.pop('time_direction')
        if 'time_period' in kwargs:
            self.time_period = kwargs.pop('time_period')
        self.time_format = '%Y-%m-%d'
        if 'time_format' in kwargs:
            self.time_format = kwargs.pop('time_format')
        self.time_normalize = False
        if 'time_normalize' in kwargs:
            self.time_normalize = kwargs.pop('time_normalize')

        self.file_workflow = self._defaults['workflow']
        if 'file_workflow' in kwargs:
            self.file_workflow = kwargs.pop('file_workflow')

        self.warnings_on_reading = True
        if 'warnings_on_reading' in kwargs:
            self.warnings_on_reading = kwargs.pop('warnings_on_reading')

        self.expected_time_steps = (
            self.time_reference, self.time_period, self.time_freq, self.time_direction, self.time_normalize)

        self._structure_template = {}
        self.options = kwargs
        self.tags = {}

        self.file_namespace = make_namespaces(variables=self.file_variable, workflows=self.file_workflow)

        self.memory_active = True
        self.memory_data = None

        # readability fields
        self.readable: bool = False
        self.status: str = "nio"   # "ok" or "nio"
        self.warnings: list = []

        # method to check readability
        self.readable_settings(active_warnings=self.message)
        # set debug state
        self.debug_state = True

        # info dataset initialization end
        if self.message:
            self.logger.info(f'Dataset obj {self.__str__()} ... INITIALIZED')

    # method to return information as a string representation
    def __repr__(self):
        return (f"{self.__class__.__name__}("
                f"{self.file_name!r}, {self.file_mode!r}, {self.data_layout!r}, {self.file_type!r}, "
                f"{self.file_format!r}, {self.file_variable!r}, {self.file_namespace!r},"
                f"status={self.status!r})")

    # method to extract information from string representation
    def __str__(self):

        file_name = getattr(self, "file_name", "<unknown>")
        file_type = getattr(self, "file_type", "unknown")
        file_format = getattr(self, "file_format", "unknown")
        file_mode = getattr(self, "file_mode", "n/a")
        file_variable = getattr(self, "file_variable", "n/a")
        file_namespace = getattr(self, "file_namespace", "n/a")
        status = getattr(self, "status", "n/a")

        return (f"{file_name} [{file_type}/{file_format}] "
                f"mode={file_mode}, var={file_variable}, namespace={file_namespace}, status={status}")

    # method to return a representation as dictionary
    def repr_to_dict(self):
        """Return a summary dictionary of key attributes."""
        return {
            "class": self.__class__.__name__,
            "file_name": self.file_name,
            "file_mode": self.file_mode,
            "file_type": self.file_type,
            "file_format": self.file_format,
            "file_variable": self.file_variable,
            "file_namespace": self.file_namespace,
            "status": self.status
        }

    # method to check readable settings
    def readable_settings(
            self,
            *,
            active_warnings: bool = True,
            require_type: bool = True,
            allowed_formats: Tuple[str, ...] = (
                    "grib", 'grib2',
                    "tiff", "tif", "geotiff", "gtiff",
                    "netcdf", "nc", "netcdf4", "nc4",
                    "csv", "json",
                    "txt", "ascii"),
            allowed_compressions: Tuple[str, ...] = ("gz",),
            temp_markers: Tuple[str, ...] = ("tmp", "temp"),
            min_size_bytes: int = 1,
            expected_mode: Optional[str] = "local",
    ) -> bool:

        self.warnings = []
        loc_str = str(self.loc_pattern)
        p = Path(loc_str)
        readable = True

        # 0) Template placeholders (year/month/day/hour/minute)
        # Supported tokens:
        #   Year   : YYYY | %Y  | {Y} / {YY} / {YYY} / {YYYY}
        #   Month  : MM   | %m  | {M} / {MM}
        #   Day    : DD   | %d  | {D} / {DD}
        #   Hour   : HH   | %H  | {H} / {HH}
        #   Minute : mm   | %M  | {m} / {mm}
        template_pattern = re.compile(
            r"(YYYY|%Y|\{Y{1,4}\}|"
            r"MM|%m|\{M{1,2}\}|"
            r"DD|%d|\{D{1,2}\}|"
            r"HH|%H|\{H{1,2}\}|"
            r"mm|%M|\{m{1,2}\})"
        )
        if template_pattern.search(loc_str):
            # Mark as a template path: not a concrete file to read yet.
            self.status = "template"
            self.readable = False
            if active_warnings:
                self.logger.warning("Path contains date/time placeholders; marking status='template'.")
            return False

        # 1) File existence
        if not p.exists():
            if active_warnings:
                self.logger.warning(f"File not found on disk: {p}")
            readable = False
        else:
            # 2) Size sanity
            try:
                if p.stat().st_size < min_size_bytes:
                    self.warnings.append(f"File is empty (<{min_size_bytes} bytes).")
                    readable = False
            except OSError as e:
                self.warnings.append(f"Could not stat file: {e}")
                readable = False

        # 3) Required type present
        if require_type and (self.file_type is None):
            self.warnings.append("file_type is None.")
            readable = False

        # 4) Temporary markers
        name_lower = p.name.lower()
        fmt_lower = (self.file_format or "").lower()
        if any(re.search(rf"(^|[^a-z]){re.escape(m)}([^a-z]|$)", name_lower)
               for m in temp_markers) or (fmt_lower in temp_markers):
            self.warnings.append("Temporary file detected (name/format contains tmp/temp).")
            readable = False

        # 5) Allowed format set
        if fmt_lower and (fmt_lower not in allowed_formats):
            self.warnings.append(f"Format '{self.file_format}' is not in allowed_formats={allowed_formats}.")

        # 6) Extension vs declared format
        ext = p.suffix.lower().lstrip(".")
        if ext and (ext not in allowed_formats):
            if ext not in allowed_compressions:
                self.warnings.append(f"Extension '.{ext}' is not in allowed_formats={allowed_formats}.")

        # 7) Expected mode
        if expected_mode and (self.file_mode != expected_mode):
            self.warnings.append(f"file_mode is '{self.file_mode}', expected '{expected_mode}'.")

        # 8) Variable sanity (optional, warn only)
        if self.file_variable in (None, "", "unknown"):
            self.warnings.append("file_variable is missing or generic.")

        # 9) check warnings
        if active_warnings:
            if self.warnings:
                self.logger.warning("\n".join(self.warnings))

        # define readable or not
        self.readable = bool(readable)
        # set the status
        self.status = "ok" if self.readable else "nio"

        return self.readable

    # method to get readable summary in human format
    def readable_summary(self) -> str:
        if self.readable:
            return "readable: yes"
        return "readable: no → " + "; ".join(self.warnings)

    # method to check if readable
    def is_readable(self) -> bool:
        """
        Check if this instance is readable.
        Returns True if status is 'ok', False otherwise.
        Automatically updates readability before returning.
        """
        self.readable_settings()
        return self.status == "ok"

    # method to update data object
    def update(self, in_place = False, **kwargs):

        new_file_name = substitute_string(self.file_name, kwargs)
        new_loc_pattern = substitute_string(self.loc_pattern, kwargs)

        if in_place:
            self.file_name = new_file_name
            self.loc_pattern = self.get_key(**kwargs)
            self.tags.update(kwargs)
            return self
        else:
            new_options = self.options.copy()
            new_options.update(
                {'loc_pattern': new_loc_pattern, 'file_name': new_file_name, 'logger': self.logger})

            new_dataset = self.__class__(**new_options)
            new_dataset.__dict__.update(self.__dict__)

            new_tags = self.tags.copy()
            new_tags.update(kwargs)
            new_dataset.tags = new_tags

            return new_dataset

    # method to copy dataset object
    def copy(self, template = False):
        new_dataset = self.update()
        if template:
            new_dataset._template = self._structure_template
        return new_dataset

    ## CLASS METHODS FOR FACTORY
    @classmethod
    def from_options(cls, options: dict, defaults: dict = None):
        defaults = defaults or {}
        new_options = defaults.copy()
        new_options.update(options)

        type = new_options.pop('type', None)
        type = cls.get_type(type)
        Subclass: 'Dataset' = cls.get_subclass(type)

        return Subclass(**new_options)

    @classmethod
    def get_subclass(cls, type: str):
        type = cls.get_type(type)
        Subclass: 'Dataset'|None = cls.subclasses.get(type.lower())
        if Subclass is None:
            raise ValueError(f"Invalid type of dataset: {type}")
        return Subclass
    
    @classmethod
    def get_type(cls, data_mode: Optional[str] = None):
        if data_mode is not None:
            return data_mode
        elif hasattr(cls, 'type'):
            return cls.data_mode
        else:
            return cls._defaults['type']
    
    ## PROPERTIES
    @property
    def file_format(self):
        return self._format
    @file_format.setter
    def file_format(self, value):
        self._format = value
    @property
    def file_mode(self):
        return self._mode
    @file_mode.setter
    def file_mode(self, value):
        self._mode = value
    @property
    def file_type(self):
        return self._type
    @file_type.setter
    def file_type(self, value):
        self._type = value
    @property
    def file_variable(self):
        return self._variable
    @file_variable.setter
    def file_variable(self, value):
        self._variable = value

    @property
    def has_version(self):
        return '{file_version}' in self.loc_pattern

    @property
    def loc_pattern(self):
        raise NotImplementedError
    @loc_pattern.setter
    def loc_pattern(self, value):
        raise NotImplementedError

    @property
    def expected_time_steps(self):
        return self._expected_time_steps
    @expected_time_steps.setter
    def expected_time_steps(self, value):
        self._expected_time_steps = self.get_expected_times(value[0], value[1], value[2], value[3], value[4])

    # method to get expected times
    def get_expected_times(
            self,
            time_reference: Union[pd.Timestamp, pd.DatetimeIndex, str, None],
            time_period: Optional[int] = None,
            time_freq: str = "h",
            time_direction: Literal["forward", "backward", "single"] = "forward",
            time_normalize: bool = False) -> Optional[pd.DatetimeIndex]:

        # Manage time_reference object
        if isinstance(time_reference, pd.DatetimeIndex):
            if len(time_reference) == 0:
                return None
            if len(time_reference) == 1:
                time_reference = time_reference[0]
            else:
                self.logger.error("Invalid time_reference: length greater than 1.")
                raise ValueError("time_reference must be a single timestamp or a length-1 DatetimeIndex.")
        elif isinstance(time_reference, str):
            try:
                time_reference = pd.to_datetime(time_reference)
            except Exception as e:
                self.logger.error(f"Invalid time_reference string: {e}")
                raise ValueError(f"Invalid time_reference string: {e}")

        # Early exits for missing params
        if time_reference is None or time_direction is None:
            return None
        if time_direction != "single" and time_period is None:
            return None

        # Validate time_period when used
        if time_direction != "single":
            if not isinstance(time_period, int) or time_period <= 0:
                self.logger.error("Invalid time_period: must be a positive integer.")
                raise ValueError("time_period must be a positive integer.")

        # Compute times
        if time_direction == "backward":
            time_obj = pd.date_range(
                end=time_reference, freq=time_freq, periods=time_period, normalize=time_normalize
            )
        elif time_direction == "forward":
            time_obj = pd.date_range(
                start=time_reference, freq=time_freq, periods=time_period, normalize=time_normalize
            )
        elif time_direction == "single":
            ts = time_reference.normalize() if time_normalize else time_reference
            time_obj = pd.DatetimeIndex([ts])
        else:
            self.logger.error("Invalid time_direction: must be 'forward', 'backward', or 'single'.")
            raise ValueError("Invalid time_direction. Use 'forward', 'backward', or 'single'.")

        return time_obj

    @property
    def available_keys(self):
        return self.get_available_keys()

    # method to get available keys
    def get_available_keys(self, time: (dt.datetime, pd.date_range) = None, **kwargs):
        
        prefix = self.get_prefix(time, **kwargs)
        if not self._check_data(prefix):
            return []
        if isinstance(time, dt.datetime):
            time = xr.date_range(time, time)

        loc_pattern = self.get_key(time = None, **kwargs)
        files = []
        for file in self._walk(prefix):
            try:
                this_time, _ = extract_date_and_tags(file, loc_pattern)
                if time is None or (time is not None and time.contains(this_time)) or not self.has_time:
                    files.append(file)
            except ValueError:
                pass
        
        return files

    def _walk(self, prefix: str) -> Generator[str, None, None]:
        raise NotImplementedError

    @property
    def is_static(self):
        return not '{' in self.loc_pattern and not self.has_time

    @property
    def has_time(self):
        """Return True if loc_pattern contains a time format placeholder like '%Y'."""
        return isinstance(self.loc_pattern, str) and '%' in self.loc_pattern

    @property
    def available_tags(self):
        return self.get_available_tags()

    def get_prefix(self, time: (dt.datetime, pd.date_range) = None, **kwargs):
        if not isinstance(time, pd.date_range):
            prefix = self.get_key(time = time, **kwargs)
        else:
            start = time.start
            end = time.end
            prefix = self.get_key(time = None, **kwargs)
            if start.year == end.year:
                prefix = prefix.replace('%Y', str(start.year))
                if start.month == end.month:
                    prefix = prefix.replace('%m', f'{start.month:02d}')
                    if start.day == end.day:
                        prefix = prefix.replace('%d', f'{start.day:02d}')

        prefix = os.path.dirname(prefix)
        while '%' in prefix or '{' in prefix:
            prefix = os.path.dirname(prefix)
        
        return prefix

    def get_available_tags(self, time: (dt.datetime, pd.date_range) = None, **kwargs):
        all_keys = self.get_available_keys(time, **kwargs)
        all_tags = {}
        all_dates = set()
        for key in all_keys:
            this_date, this_tags = extract_date_and_tags(key, self.loc_pattern)
            
            for tag in this_tags:
                if tag not in all_tags:
                    all_tags[tag] = set()
                all_tags[tag].add(this_tags[tag])
            all_dates.add(this_date)
        
        all_tags = {tag: list(all_tags[tag]) for tag in all_tags}
        all_tags['time'] = list(all_dates)

        return all_tags

    # method to get attribute
    def get_attribute(self, attribute: str) -> (str, int, float, dict, list, pd.Timestamp, None):
        if attribute in self.__dict__:
            return self.__dict__[attribute]
        else:
            return None

    # method to check if time step available in the datasets
    def check_time_step(self, time: (pd.Timestamp, None)) -> (int, bool):
        is_idx, is_valid = None, False
        if time is not None:
            if self._expected_time_steps is not None:
                if time in self._expected_time_steps:
                    is_idx = np.argwhere(self._expected_time_steps == time)[0][0]
                    is_valid = True
        return is_idx, is_valid
    
    def get_time_step(self, time_step: pd.Timestamp, time_type: str = 'start', **kwargs) -> (pd.Timestamp, int):
        
        if time_type == 'start':
            idx_select = 0
            time_select = self._expected_time_steps[idx_select]
        elif time_type == 'end':
            idx_select = len(self._expected_time_steps) - 1
            time_select = self._expected_time_steps[-1]
        elif time_type == 'current' or time_type == 'step':
            idx_select, is_valid = self.check_time_step(time_step)
            if is_valid:
                time_select = self._expected_time_steps[idx_select]
            else:
                time_select = None
        else:
            self.logger.error(f"Invalid time type: {time_type}")
            raise ValueError(f"Invalid time type: {time_type}")
        return time_select, idx_select
    
    def is_subdataset(self, other: 'Dataset') -> bool:
        key = self.get_key(time = dt.datetime(1900,1,1))
        try:
            extract_date_and_tags(key, other.loc_pattern)
            return True
        except ValueError:
            return False

    ## TIME-SIGNATURE MANAGEMENT
    @property
    def time_signature(self):
        if not hasattr(self, '_time_signature'):
            self._time_signature = self._defaults['time_signature']
        return self._time_signature
    @time_signature.setter
    def time_signature(self, value):
        if value not in ['period', 'step', 'current', 'constant', 'start', 'end', "unique", None]:
            self.logger.error(f'Invalid time signature: {value}')
            raise ValueError(f"Invalid time signature: {value}")
        self._time_signature = value

    def get_time_signature(self, timestep: (pd.Timestamp, dt.datetime)) -> (dt.datetime, None):
        if timestep is None:
            return None
        if isinstance(timestep, dt.datetime):
            time = timestep
            # calculating the length in this way is not perfect,
            # but should work given that timesteps are always requested in order
            if hasattr(self, 'previous_requested_time'):
                length = (time - self.previous_requested_time).days
            else:
                length = None
            self.previous_requested_time = time
        elif isinstance(timestep, list):

            time_signature = self.time_signature
            if time_signature == 'start':
                time = timestep[0]
            elif time_signature == 'end':
                time = timestep[-1]
            length = 1
            self.previous_requested_time = time

        else:

            time_signature = self.time_signature
            if time_signature == 'start':
                time = timestep.start
            elif time_signature == 'end':
                time = timestep.end
            elif time_signature == 'end+1':
                time = (timestep+1).start
            length = timestep.get_length()
            self.previous_requested_time = time

        key_without_tags = re.sub(r'\{[^}]*\}', '', self.loc_pattern)
        hasyear = '%Y' in key_without_tags

        # change the date to 28th of February if it is the 29th of February,
        # but only if no year is present in the path (i.e. this is a parameter)
        # and the length is greater than 1 (i.e. not a daily timestep)
        if not hasyear and time.month == 2 and time.day == 29:
            if length is not None and length > 1:
                time = time.replace(day = 28)
        
        # progressively remove the non-used tags from the time
        if '%s' not in key_without_tags:
            time = time.replace(second = 0)
            if '%M' not in key_without_tags:
                time = time.replace(minute = 0)
                if '%H' not in key_without_tags:
                    time = time.replace(hour = 0)
                    if '%d' not in key_without_tags:
                        time = time.replace(day = 1)
                        if '%m' not in key_without_tags:
                            time = time.replace(month = 1)

        return time

    def namespace(self, variable, workflow):
        # If both are lists, build a list of SimpleNamespace objects
        if isinstance(variable, list) and isinstance(workflow, list):
            # zip pairs up elements (stops at the shortest list)
            return [SimpleNamespace(variable=v, workflow=w) for v, w in zip(variable, workflow)]

        # Otherwise, make a single SimpleNamespace
        return SimpleNamespace(variable=variable, workflow=workflow)

    # method to get data
    def get_data(self, time: (dt.datetime, pd.Timestamp) = None, name: str = None,
                 as_is: bool = False, **kwargs):

        # mapping of dimensions and variables
        if 'dims_geo' in list(self.variable_template.keys()):
            dims_geo = self.variable_template['dims_geo']
        else:
            self.logger.warning("Variable template does not contain 'dims_geo' definition.", UserWarning)
        if 'vars_data' in list(self.variable_template.keys()):
            vars_data = self.variable_template['vars_data']
        else:
            self.logger.warning("Variable template does not contain 'vars_data' definition.", UserWarning)
        if 'coords_geo' in list(self.variable_template.keys()):
            coords_geo = self.variable_template['coords_geo']
        else:
            self.logger.warning("Variable template does not contain 'coords_geo' definition.", UserWarning)

        # get full location of file (based on time)
        full_location = self.get_key(time, **kwargs)

        # check memory active (if defined true or false)
        if 'memory_active' in kwargs:
            self.memory_active = kwargs.pop('memory_active')

        # check memory if active
        if self.memory_active:

            # check if data is already in memory
            if self.memory_data is not None:

                # info log start (memory case)
                log_data('start', name=name, time=time, from_memory=True)

                # get data from memory
                data = deepcopy(self.memory_data)

                # get variables
                variable = select_variable(data, **self.variable_template)

                # if there is no structure_template for the dataset, create it from the data
                structure_template = self.get_structure_template(make_it=False, **kwargs)
                if structure_template is None:
                    self.set_structure_template(data, template_key=variable, **kwargs)
                else:
                    # otherwise, update the data in the template
                    # (this will make sure there is no errors in the coordinates due to minor rounding)
                    attrs = data.attrs
                    data = self.set_data_to_structure_template(data, structure_template)
                    data.attrs.update(attrs)

                data.attrs.update({'source_location': full_location})

                # check data type
                if isinstance(data, xr.DataArray):
                    data.name = self.memory_data.name
                elif isinstance(data, xr.Dataset):
                    pass
                else:
                    self.logger.error('Invalid data type in memory.')
                    raise ValueError(f"Invalid data type: {type(data)}")

                # select by time
                data = select_by_time(data, when=time, tolerance='1H')

                # check data after selecting by time
                if data is None:
                    self.logger.warning(f'Data is defined by NoneType after selecting by time {time}.')
                    # info get data end
                    self.logger.info_down(f'Get data for time {time} ... SKIPPED. Time is not available')
                    return None

                # select data using a 'variable:workflow' mapping string (from reference or variable)
                reference, variable = kwargs.pop('reference', None), kwargs.pop('variable', None)
                if reference is not None or variable is not None:
                    for key in (reference, variable):
                        tmp = select_da_by_mapping(data, mapping_str=key)
                        if tmp is not None:
                            data = tmp
                            break

                # select by variables
                #data = select_by_vars(data, vars=self.variable_template)

                # check data after selecting by variables
                if data is None:
                    self.logger.warning(f'Data is defined by NoneType after selecting by variables {reference}.')
                    # info get data end
                    self.logger.info_down(f'Get data for time {time} ... SKIPPED. Variables are not available')
                    return None

                # info log start (memory case)
                log_data('end', name=name, time=time, from_memory=True)

                return data

            else:
                 pass

        # info log start (source case)
        log_data('start', name=name, time=time, from_memory=False)

        # file format specific reading
        if self.file_format in ['csv', 'json', 'txt', 'shp', 'ascii']:
            if self._check_data(full_location):

                # get data
                data = self._read_data(full_location)

                # return data as is (if specified)
                if as_is:
                    return data

                # check if data is xarray DataArray
                if isinstance(data, xr.DataArray):

                    # map the data to the template
                    data = map_dims(data, **self.variable_template)
                    # ensure that the data has descending latitudes
                    data = straighten_data(data)
                    # ensure that the data dimensions are flat
                    data = flat_dims(data)

                # info log end (source case)
                log_data('end', name=name, time=time, from_memory=False)

                return data

            else:
                self.logger.error(f'Could not resolve data from {full_location}.')
                raise ValueError(f'Could not resolve data from {full_location}.')

        # check if data is available
        if self.check_data(time, **kwargs):

            # get data
            data = self._read_data(full_location, **self.variable_template)

            # return data as is (if specified)
            if as_is:
                return data

            # ensure that the data dimensions are not empty
            data = straighten_dims(data)
            data = self._check_step(data, "straighten_dims")
            if data is None: return None
            # debug data
            if self.debug_state: plot_data(data, var_name='SnowMask')

            # map the data dimensions
            data = map_dims(data, **self.variable_template)
            data = self._check_step(data, "map_dims")
            if data is None: return None
            # debug data
            if self.debug_state: plot_data(data, var_name='SnowMask')

            # map the data coords
            data = map_coords(data, **self.variable_template)
            data = self._check_step(data, "map_coords")
            if data is None: return None
            # debug data
            if self.debug_state: plot_data(data, var_name='SnowMask')

            # map the data variables
            data = map_vars(data, **self.variable_template)
            data = self._check_step(data, "map_vars")
            if data is None: return None
            # debug data
            if self.debug_state: plot_data(data, var_name='SnowMask')

            # ensure that the data has descending latitudes
            data = straighten_data(data)
            data = self._check_step(data, "straighten_data")
            if data is None: return None

            # debug data
            if self.debug_state: plot_data(data, var_name='SnowMask')

            # ensure that the data dimensions are flat
            data = flat_dims(data)

            # ensure that the time info is correctly defined (if needed)
            data = straighten_time(
                data, time_file=self.time_reference, time_freq=self.time_freq, time_direction=self.time_direction)
            data = self._check_step(data, "straighten_time")
            if data is None: return None

            # make sure the nodata value is set to np.nan for floats and to the max int for integers
            data = set_type(data, self.nan_value)

            # get variables
            variable = select_variable(data, **self.variable_template)

            # store in memory (if active)
            if self.memory_active:
                self.memory_data = deepcopy(data)

        else:
            self.logger.error(f'Could not resolve data from {full_location}.')
            raise ValueError(f'Could not resolve data from {full_location}.')

        # if there is no template for the dataset, create it from the data
        structure_template = self.get_structure_template(make_it=False, **kwargs)
        if structure_template is None:
            if self.memory_data is not None:
                self.set_structure_template(template_array=self.memory_data, template_key=variable, **kwargs)
            else:
                self.set_structure_template(template_array=data, template_key=variable, **kwargs)
        else:
            # otherwise, update the data in the template
            # (this will make sure there is no errors in the coordinates due to minor rounding)
            attrs = data.attrs
            if self.memory_data is not None:
                data = self.set_data_to_structure_template(self.memory_data, template_dict)
            else:
                data = self.set_data_to_structure_template(data, template_dict)
            data.attrs.update(attrs)

        # set attributes
        data.attrs.update({'source_location': full_location})

        # select by time
        data = select_by_time(data, when=time, tolerance='1H')
        # check data after selecting by time
        if data is None:
            self.logger.warning(f'Data is defined by NoneType after selecting by time {time}.')
            # info get data end
            self.logger.info_down(f'Get data for time {time} ... SKIPPED. Time is not available')
            return None

        # select data using a 'variable:workflow' mapping string (from reference or variable)
        reference, variable = kwargs.pop('reference', None), kwargs.pop('variable', None)
        if reference is not None or variable is not None:
            for key in (reference, variable):
                tmp = select_da_by_mapping(data, mapping_str=key)
                if tmp is not None:
                    data = tmp
                    break

        #data = select_by_vars(data, **self.variable_template)
        # check data after selecting by variables
        if data is None:
            self.logger.warning(f'Data is defined by NoneType after selecting by variables {reference}.')
            # info get data end
            self.logger.info_down(f'Get data for time {time} ... SKIPPED. Variables are not available')
            return None

        # info get data end (source case)
        log_data('end', name=name, time=time, from_memory=False)

        return data

    def _check_step(self, data, step_name='n/a'):
        if data is None:
            self.logger.warning(f"Data became None after step: {step_name}")
            return None
        return data

    @staticmethod
    def select_dataarray_by_mapping(ds, mapping_str):
        """
        Select a DataArray from an xarray.Dataset using a 'name:var' mapping string.

        Format: '<new_name>:<var_name>'
        - If <var_name> is empty, use the first variable in the dataset.
        - Always returns a DataArray renamed to <new_name>.
        """
        # Split "Label:var"
        parts = mapping_str.split(':', 1)
        new_name = parts[0].strip() or None
        var_name = parts[1].strip() if len(parts) > 1 else ''

        # Pick the variable to extract
        if var_name:
            if var_name not in ds.data_vars:
                raise KeyError(f"Variable '{var_name}' not found in dataset variables: {list(ds.data_vars)}")
            da = ds[var_name]
        else:
            first_var = next(iter(ds.data_vars))
            da = ds[first_var]

        # Rename the DataArray (if name provided)
        if new_name:
            da = da.rename(new_name)

        return da

    @abstractmethod
    def _read_data(self, input_key:str):
        raise NotImplementedError

    # method to write date
    def write_data(self, data,
                   time: Union[dt.datetime, pd.Timestamp] = None,
                   time_format: str = '%Y-%m-%d',
                   metadata: dict = {}, variable: str = 'variable', separator: str = ';',
                   **kwargs):

        # check data (format and type)
        check_data_format(data, self.file_format)

        # define the output file
        out_file = self.get_key(time, **kwargs)
        out_path, out_name = os.path.dirname(out_file), os.path.basename(out_file)

        # case of file-like data
        if self.file_format == 'file':
            self._write_data(data, out_file)
            return

        # case of csv, json, txt, shp files
        if self.file_format in ['csv', 'json', 'txt', 'shp']:
            append = kwargs.pop('append', False)
            self._write_data(data, out_file, append = append)
            return

        # case of pandas DataFrame
        if isinstance(data, pd.DataFrame):

            if not data.empty:
                append = kwargs.pop('append', False)
                self._write_data(data, out_file, append=append)
                return
            else:
                self.logger.warning('Output DataFrame is empty and writing data is not activated.')

        # if data is a numpy array, ensure there is a structure template available
        if isinstance(data, np.ndarray):

            try:
                structure_template = self.get_structure_template(**kwargs)
            except Exception as exc: #PermissionError as permission_error:
                self.logger.error('Cannot write numpy array without a template.')
                raise ValueError('Cannot write numpy array without a template.')

        elif isinstance(data, xr.DataArray) or isinstance(data, xr.Dataset):

                self.set_structure_template(data, template_key=variable, **kwargs)
                structure_template = self.get_structure_template(**kwargs, make_it=False)

        else:
            self.logger.error(f'Invalid data type in writing method: {type(data)}')
            raise ValueError(f'Invalid data type in writing method: {type(data)}')

        # set the data to the structure template
        out_obj = self.set_data_to_structure_template(data, structure_template)
        out_obj = set_type(out_obj, self.nan_value)

        # adjust the data orientation
        if out_obj is not None:
            out_obj = straighten_data(out_obj)
        else:
            self.logger.warning('Output object is defined by NoneType before adjusting data orientation.')
        if out_obj is None:
            self.logger.warning('Output object is defined by NoneType after adjusting data orientation.')

        # map the data variables
        if out_obj is not None:
            out_obj = rename_da_by_template(out_obj, variable_template=self.variable_template)
            out_obj = map_vars(out_obj, **self.variable_template)
        else:
            self.logger.warning('Output object is defined by NoneType before mapping data variables.')
        if out_obj is None:
            self.logger.warning('Output object is defined by NoneType after mapping data variables.')

        # map the data dimensions
        if out_obj is not None:
            out_obj = map_dims(out_obj, **self.variable_template)
        else:
            self.logger.warning('Output object is defined by NoneType before mapping data dimensions.')
        if out_obj is None:
            self.logger.warning('Output object is defined by NoneType after mapping data dimensions.')

        # check if output object is defined
        if out_obj is not None:

            # set attributes
            out_obj.attrs['source_location'] = out_name

            # add the metadata
            data_attrs = data.attrs if hasattr(data, 'attrs') else {}
            out_attrs = out_obj.attrs
            data_attrs.update(out_attrs)
            out_obj.attrs = out_attrs

            # add the metadata
            metadata['file_name'] = out_name
            out_obj = self.set_metadata(out_obj, time, time_format, **metadata)

            # write the data
            out_opt = {'time' : time}
            out_opt = {'ref': kwargs['ref']} if 'ref' in kwargs else out_opt
            self._write_data(out_obj, out_file, **out_opt)

        else:
            self.logger.warning('Output object is defined by NoneType and writing data is not activated.')

    # method to copy data
    def copy_data(self, new_loc_pattern, time: Union[dt.datetime, pd.Timestamp] = None, **kwargs):
        data = self.get_data(time, **kwargs)
        timestamp = self.get_time_signature(time)
        if timestamp is None:
            new_key = substitute_string(new_loc_pattern, kwargs)
        else:
            new_key = timestamp.strftime(substitute_string(new_loc_pattern, kwargs))
        self._write_data(data, new_key)

    # method to remove data
    def rm_data(self, time: Union[dt.datetime, pd.Timestamp] = None, **kwargs):
        key = self.get_key(time, **kwargs)
        self._rm_data(key)

    # method to move data (and remove the original)
    def move_data(self, new_loc_pattern, time: Union[dt.datetime, pd.Timestamp] = None, **kwargs):
        self.copy_data(new_loc_pattern, time, **kwargs)
        self.rm_data(time, **kwargs)

    @abstractmethod
    def _write_data(self, output: xr.DataArray, output_key: str):
        raise NotImplementedError
    
    @abstractmethod
    def _rm_data(self, key: str):
        raise NotImplementedError

    # method to make data from parents
    def make_data(self, time: Union[dt.datetime, pd.Timestamp] = None, **kwargs):
        if not hasattr(self, 'parents') or self.parents is None:
            raise ValueError(f'No parents for {self.file_name}')
        
        parent_data = {name: parent.get_data(time, **kwargs) for name, parent in self.parents.items()}
        data = self.fn(**parent_data)
        self.write_data(data, time, **kwargs)
        return data

    ## METHODS TO CHECK DATA AVAILABILITY
    def _get_times(self, time_range: pd.date_range, **kwargs) -> Generator[dt.datetime, None, None]:
        all_times = self.get_available_tags(time_range, **kwargs)['time']
        all_times.sort()
        for time in all_times:
            if time_range.contains(time):
                yield time
        
        if hasattr(self, 'parents') and self.parents is not None:
            parent_times = [set(parent.get_times(time_range, **kwargs)) for parent in self.parents.values()]
            # get the intersection of all times
            parent_times = set.intersection(*parent_times)
            for time in parent_times:
                if time not in all_times and time_range.contains(time):
                    yield time

    @with_cases
    def get_times(self, time_range: pd.date_range, **kwargs) -> list[dt.datetime]:
        """
        Get a list of times between two dates.
        """
        return list(self._get_times(time_range, **kwargs))
        
    @with_cases
    def check_data(self, time: Union[pd.Timestamp, dt.datetime] = None, **kwargs) -> bool:
        """
        Check if data is available for a given time.
        """
        #if 'tile' in kwargs:
        full_key = self.get_key(time, **kwargs)
        if self._check_data(full_key):
            return True
        else:
            return False

    @abstractmethod
    def _check_data(self, data_key) -> bool:
        raise NotImplementedError

    # methods to manipulate the data
    def get_key(self, time: Union[pd.Timestamp, dt.datetime] = None, **kwargs):
        
        time = self.get_time_signature(time)
        raw_key = substitute_string(self.loc_pattern, kwargs)
        key = time.strftime(raw_key) if time is not None else raw_key
        return key

    # methods to organize the structure template
    def get_structure_template(self, make_it:bool = True, **kwargs):

        structure_key = kwargs.pop('structure_key', None)

        structure_template = self._structure_template.get(structure_key, None)
        if structure_template is None and make_it:
            if not self.has_time:
                data = self.get_data(as_is = True, **kwargs)
                self.set_structure_template(data, template_key = structure_key)

            else:
                time_start, idx_start = self.get_time_step(time_type='start', **kwargs)
                if time_start is not None:
                    data = self.get_data(time = time_start, as_is=True, **kwargs)
                else:
                    return None
            
            data = straighten_data(data)

            self.set_structure_template(data)
            structure_template = self.get_structure_template(make_it = False, **kwargs)
        
        return structure_template
    
    # method to set the structure template
    def set_structure_template(self, template_array: (xr.DataArray,xr.Dataset) = None, template_key: str = 'data', **kwargs):

        # ensure template key is a list (for iteration)
        if not isinstance(template_key, list):
            template_key = [template_key]

        # ensure crs object
        crs = template_array.attrs.get('crs', None)

        if crs is not None:
            crs_wkt = crs.to_wkt()
        elif hasattr(template_array, 'spatial_ref'):
            if hasattr(template_array.spatial_ref, 'crs_wkt'):
                crs_wkt = template_array.spatial_ref.crs_wkt
            else:
                crs_wkt = default_crs_wkt
        elif hasattr(template_array, 'crs'):
            crs_wkt = template_array.crs.crs_wkt
        else:
            crs_wkt = default_crs_wkt

        # iterate over all keys
        for step_key in template_key:

            if isinstance(template_array, xr.Dataset):
                self._structure_template[step_key] = {'crs': crs_wkt,
                                        '_FillValue' : template_array[step_key].attrs.get('_FillValue'),
                                        'dims_names' : template_array[step_key].dims,
                                        'spatial_dims' : (template_array[step_key].longitude.name, template_array[step_key].latitude.name),
                                        'dims_starts': {},
                                        'dims_ends': {},
                                        'dims_lengths': {}}
            elif isinstance(template_array, xr.DataArray):
                self._structure_template[step_key] = {
                    'crs': crs_wkt,
                    '_FillValue': template_array.attrs.get('_FillValue'),
                    'dims_names': template_array.dims,
                    'spatial_dims': (template_array.longitude.name,
                                     template_array.latitude.name),
                    'dims_starts': {},
                    'dims_ends': {},
                    'dims_lengths': {}
                }
            else:
                self.logger.error('Invalid template array type.')
                raise ValueError('Invalid template array type.')

        # ensure structure template for dataset case
        if isinstance(template_array, xr.Dataset):
            # copy the template structure for
            for step_key in template_key:
                self._structure_template[step_key]['variables'] = list(template_array.data_vars)

        # ensure dimensions info for all keys
        for step_key in template_key:
            for dim in template_array.dims:

                if isinstance(template_array, xr.DataArray):
                    this_dim_values = template_array[dim].data
                elif isinstance(template_array, xr.Dataset):
                    this_dim_values = template_array[step_key][dim].data
                else:
                    self.logger.error('Invalid template array type.')
                    raise ValueError('Invalid template array type.')

                start = this_dim_values[0]
                end = this_dim_values[-1]
                length = len(this_dim_values)
                self._structure_template[step_key]['dims_starts'][dim] = float(start)
                self._structure_template[step_key]['dims_ends'][dim] = float(end)
                self._structure_template[step_key]['dims_lengths'][dim] = length

    @staticmethod
    def build_structure_template_array(template_dict: dict, data = None) -> (xr.DataArray,xr.Dataset):
        """
        Build a template xarray.DataArray from a dictionary.
        """

        shape = [template_dict['dims_lengths'][dim] for dim in template_dict['dims_names']]
        if data is None:
            data = np.full(shape, template_dict['_FillValue'])
        else:
            data = data.reshape(shape)
        template = xr.DataArray(data, dims = template_dict['dims_names'])
        
        for dim in template_dict['dims_names']:
            start  = template_dict['dims_starts'][dim]
            end    = template_dict['dims_ends'][dim]
            length = template_dict['dims_lengths'][dim]
            template[dim] = np.linspace(start, end, length)

        template.attrs = {'crs': template_dict['crs'], '_FillValue': template_dict['_FillValue']}
        template = template.rio.set_spatial_dims(
            *template_dict['spatial_dims']).rio.write_crs(template_dict['crs']).rio.write_coordinate_system()

        if 'variables' in template_dict:
            template_ds = xr.Dataset({var: template.copy() for var in template_dict['variables']})
            return template_ds
        
        return template

    @staticmethod
    def set_data_to_structure_template(
            data: (np.ndarray, xr.DataArray, xr.Dataset),
            template_dict: dict) -> (xr.DataArray, xr.Dataset):

        if template_dict is None:
            return data

        if isinstance(data, xr.DataArray):
            data = Dataset.build_structure_template_array(template_dict, data.values)
        elif isinstance(data, np.ndarray):
            data = Dataset.build_structure_template_array(template_dict, data)
        elif isinstance(data, xr.Dataset):
            all_data = [Dataset.set_data_to_structure_template(
                data[var], template_dict) for var in template_dict['variables']]
            data = xr.merge(all_data)
        
        return data

    def set_metadata(self, data: (xr.DataArray, xr.Dataset),
                     time: Union[pd.Timestamp, dt.datetime] = None,
                     time_format: str = '%Y-%m-%d', **kwargs) -> xr.DataArray:
        """
        Set metadata for the data.
        """
     
        if hasattr(data, 'attrs'):
            if 'long_name' in data.attrs:
                data.attrs.pop('long_name')
            kwargs.update(data.attrs)
        
        metadata = kwargs.copy()
        metadata['time_produced'] = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        if time is not None:
            datatime = self.get_time_signature(time)
            metadata['time'] = datatime.strftime(time_format)

        file_name = metadata.get('file_name', self.file_name)
        if 'long_name' in metadata:
            metadata.pop('long_name')

        data.attrs.update(metadata)

        return data
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# helpers
# print logging message
@with_logger(var_name="logger_stream")
def log_data(stage, name=None, time=None, from_memory=False, reference=None):

    # determine tag
    tag = "memory" if from_memory else "source"
    # build the base message
    if name is not None:
        if time is not None:
            base = f'Get data for variable "{name}" at time "{time}"'
        else:
            base = f'Get data for variable "{name}" static'
    else:
        if time is not None:
            base = f'Get data at time "{time}"'
        else:
            base = f'Get data static'

    # Stage: START
    if stage == "start":
        logger_stream.info_up(f'[{tag}] {base} ...')

    # Stage: END
    elif stage == "end":
        logger_stream.info_down(f'[{tag}] {base} ... DONE')

    # Stage: SKIP
    elif stage == "skip":
        logger_stream.warning(
            f'Data is defined by NoneType after selecting by variables {reference}.'
        )
        logger_stream.info_down(
            f'[{tag}] {base} ... SKIPPED. Variables are not available'
        )
# ----------------------------------------------------------------------------------------------------------------------
