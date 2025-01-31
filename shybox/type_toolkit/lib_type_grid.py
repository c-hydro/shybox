from typing import Optional, Generator, Callable
import datetime as dt
import numpy as np
import xarray as xr
import pandas as pd

from abc import ABC, ABCMeta, abstractmethod
import os
import re

import tempfile

from shybox.generic_toolkit.lib_utils_time import is_date

class DataMeta(ABCMeta):
    def __init__(cls, name, bases, attrs):
        super().__init__(name, bases, attrs)
        if not hasattr(cls, 'subclasses'):
            cls.subclasses = {}
        elif 'type' in attrs:
            cls.subclasses[attrs['type']] = cls

class DataGrid(metaclass=DataMeta):

    _defaults = {'type': 'local',
                 'time_signature' : 'end'}

    def __init__(self, data, **kwargs):

        self.data = data

        self.file_format = 'NA'
        if 'file_format' in kwargs:
            self.file_format = kwargs.pop('file_format')

        self.file_type = 'NA'
        if 'file_type' in kwargs:
            self.file_type = kwargs.pop('file_type')

        if 'file_nans' in kwargs:
            self.file_nans = kwargs.pop('file_nans')
        else:
            self.file_nans = None

        self.map_dims, self.map_geo, self.map_data = {}, {}, {}
        if 'map_dims' in kwargs:
            self.map_dims = kwargs.pop('map_dims')
        if 'map_geo' in kwargs:
            self.map_geo = kwargs.pop('map_geo')
        if 'map_data' in kwargs:
            self.map_data = kwargs.pop('map_data')

        self._template = {}
        self.options = kwargs
        self.tags = {}

    def __repr__(self):
        return f"{self.__class__.__name__}({self.name})"

    # method to time period
    def time_period(self, time_dim: str = 'time', time_freq='h') -> xr.Dataset:

        data = self.data

        if time_dim in list(self.data.dims):
            time_values = self.data[time_dim].values
            time_check = is_date(time_values[0])
            if not time_check:
                if self.file_time is not None:
                    time_values = pd.date_range(start=self.file_time, periods=len(time_values), freq=time_freq)
                    obj_data['time'] = time_values
                else:
                    warnings.warn(f'Time values are not defined by dates. Time of the file is defined by NoneType.')
        else:
            if obj_data.ndim > 2:
                warnings.warn(f'Time dimension {time_dim} not found in dataset.')

        return obj_data

    # method to remap data
    def remap_data(self, obj_data: xr.Dataset) -> xr.Dataset:
        map_data = self.map_data
        if map_data is not None:
            if isinstance(obj_data, xr.Dataset):
                vars_data = list(obj_data.variables)
                remap_vars = {}
                for var_in, var_out in map_data.items():
                    if var_in in vars_data:
                        remap_vars[var_in] = var_out
                    else:
                        warnings.warn(f'Variable {var_in} not found in dataset.')
                obj_data = obj_data.rename(remap_vars)
        return obj_data


    def update(self, in_place=False, **kwargs):
        new_name = substitute_string(self.name, kwargs)
        new_key_pattern = substitute_string(self.key_pattern, kwargs)

        if in_place:
            self.name = new_name
            self.key_pattern = self.get_key(**kwargs)
            self.tags.update(kwargs)
            return self
        else:
            new_options = self.options.copy()
            new_options.update({'key_pattern': new_key_pattern, 'name': new_name})
            new_dataset = self.__class__(**new_options)

            new_dataset._template = self._template
            if hasattr(self, '_tile_names'):
                new_dataset._tile_names = self._tile_names

            new_dataset.time_signature = self.time_signature

            new_tags = self.tags.copy()
            new_tags.update(kwargs)
            new_dataset.tags = new_tags
            return new_dataset


    def copy(self, template=False):
        new_dataset = self.update()
        if template:
            new_dataset._template = self._template
        if hasattr(self, 'log_opts'):
            new_dataset.log_opts = self.log_opts
        if hasattr(self, 'thumb_opts'):
            new_dataset.thumb_opts = self.thumb_opts
        if hasattr(self, 'notif_opts'):
            new_dataset.notif_opts = self.notif_opts
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
        Subclass: 'Dataset' | None = cls.subclasses.get(type.lower())
        if Subclass is None:
            raise ValueError(f"Invalid type of dataset: {type}")
        return Subclass


    @classmethod
    def get_type(cls, type: Optional[str] = None):
        if type is not None:
            return type
        elif hasattr(cls, 'type'):
            return cls.type
        else:
            return cls._defaults['type']


    ## PROPERTIES
    @property
    def format(self):
        return self._format


    @format.setter
    def format(self, value):
        self._format = value


    @property
    def has_version(self):
        return '{file_version}' in self.key_pattern


    @property
    def has_tiles(self):
        return '{tile}' in self.key_pattern


    @property
    def tile_names(self):
        if not self.has_tiles:
            self._tile_names = ['__tile__']

        if not hasattr(self, '_tile_names') or self._tile_names is None:
            self._tile_names = self.available_tags.get('tile')

        return self._tile_names


    @tile_names.setter
    def tile_names(self, value):
        if isinstance(value, str):
            self._tile_names = self.get_tile_names_from_file(value)
        elif isinstance(value, list) or isinstance(value, tuple):
            self._tile_names = list(value)
        else:
            raise ValueError('Invalid tile names.')


    def get_tile_names_from_file(self, filename: str) -> list[str]:
        with open(filename, 'r') as f:
            return [l.strip() for l in f.readlines()]


    @property
    def ntiles(self):
        return len(self.tile_names)


    @property
    def key_pattern(self):
        raise NotImplementedError


    @key_pattern.setter
    def key_pattern(self, value):
        raise NotImplementedError


    @property
    def available_keys(self):
        return self.get_available_keys()


    def get_available_keys(self, time: (dt.datetime, pd.date_range) = None, **kwargs):
        prefix = self.get_prefix(time, **kwargs)
        if not self._check_data(prefix):
            return []
        if isinstance(time, dt.datetime):
            time = TimeRange(time, time)

        key_pattern = self.get_key(time=None, **kwargs)
        files = []
        for file in self._walk(prefix):
            try:
                this_time, _ = extract_date_and_tags(file, key_pattern)
                if time is None or (time is not None and time.contains(this_time)) or not self.has_time:
                    files.append(file)
            except ValueError:
                pass

        return files


    def _walk(self, prefix: str) -> Generator[str, None, None]:
        raise NotImplementedError


    @property
    def is_static(self):
        return not '{' in self.key_pattern and not self.has_time


    @property
    def has_time(self):
        return '%' in self.key_pattern


    @property
    def available_tags(self):
        return self.get_available_tags()


    def get_prefix(self, time: (str, pd.date_range) = None, **kwargs):
        if not isinstance(time, TimeRange):
            prefix = self.get_key(time=time, **kwargs)
        else:
            start = time.start
            end = time.end
            prefix = self.get_key(time=None, **kwargs)
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
            this_date, this_tags = extract_date_and_tags(key, self.key_pattern)

            for tag in this_tags:
                if tag not in all_tags:
                    all_tags[tag] = set()
                all_tags[tag].add(this_tags[tag])
            all_dates.add(this_date)

        all_tags = {tag: list(all_tags[tag]) for tag in all_tags}
        all_tags['time'] = list(all_dates)

        return all_tags


    def get_last_date(self, now=None, n=1, **kwargs) -> (dt.datetime, list[dt.datetime], None):
        if now is None:
            now = dt.datetime.now()

        # the most efficient way, I think is to search my month
        this_month = Month(now.year, now.month)
        last_date = []
        while len(last_date) < n:
            this_month_times = self.get_times(this_month, **kwargs)
            if len(this_month_times) > 0:
                valid_time = [t for t in this_month_times if t <= now]
                valid_time.sort(reverse=True)
                last_date.extend(valid_time)
            elif this_month.start.year < 1900:
                break

            this_month = this_month - 1

        if len(last_date) == 0:
            return None
        if n == 1:
            return last_date[0]
        else:
            return last_date


    def get_last_ts(self, **kwargs) -> pd.Timestamp:
        last_dates = self.get_last_date(n=15, **kwargs)
        if last_dates is None:
            return None

        timestep = estimate_timestep(last_dates)
        if timestep is None:
            return None

        if self.time_signature == 'end+1':
            return timestep.from_date(max(last_dates)) - 1
        else:
            return timestep.from_date(max(last_dates))


    def estimate_timestep(self) -> pd.Timestamp:
        last_dates = self.get_last_date(n=15)
        timestep = estimate_timestep(last_dates)
        return timestep


    def get_first_date(self, start=None, n=1, **kwargs) -> (dt.datetime, list[dt.datetime], None):
        if start is None:
            start = dt.datetime(1900, 1, 1)

        end = self.get_last_date(**kwargs)
        if end is None:
            return None

        start_month = Month(start.year, start.month)
        end_month = Month(end.year, end.month)

        # first look for a suitable time to start the search
        while True:
            midpoint = start_month.start + (end_month.end - start_month.start) / 2
            mid_month = Month(midpoint.year, midpoint.month)
            mid_month_times = self.get_times(mid_month, **kwargs)
            # if we do actually find some times in the month
            if len(mid_month_times) > 0:

                # end goes to midpoint
                end_month = mid_month
            # if we didn't find any times in the month
            else:
                # we start from the midpoint this time
                start_month = mid_month

            if start_month + 1 == end_month:
                break

        first_date = []
        while len(first_date) < n and start_month.end <= end:
            this_month_times = self.get_times(start_month, **kwargs)
            valid_time = [t for t in this_month_times if t >= start]
            valid_time.sort()
            first_date.extend(valid_time)

            start_month = start_month + 1

        if len(first_date) == 0:
            return None
        if n == 1:
            return first_date[0]
        else:
            return first_date


    def get_first_ts(self, **kwargs) -> pd.Timestamp:
        first_dates = self.get_first_date(n=15, **kwargs)
        if first_dates is None:
            return None

        timestep = estimate_timestep(first_dates)
        if timestep is None:
            return None

        if self.time_signature == 'end+1':
            return timestep.from_date(min(first_dates)) - 1
        else:
            return timestep.from_date(min(first_dates))


    def get_start(self, **kwargs) -> dt.datetime:
        """
        Get the start of the available data.
        """
        first_ts = self.get_first_ts(**kwargs)
        if first_ts is not None:
            return first_ts.start
        else:
            return self.get_first_date(**kwargs)


    def is_subdataset(self, other: 'Dataset') -> bool:
        key = self.get_key(time=dt.datetime(1900, 1, 1))
        try:
            extract_date_and_tags(key, other.key_pattern)
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
        if value not in ['start', 'end', 'end+1']:
            raise ValueError(f"Invalid time signature: {value}")
        self._time_signature = value