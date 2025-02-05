
import warnings
from typing import Optional, Generator, Callable, Type
import datetime as dt
import numpy as np
import xarray as xr
import pandas as pd

from copy import deepcopy
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
        self.options = deepcopy(kwargs)

        self.file_format = 'NA'
        if 'file_format' in kwargs:
            self.file_format = kwargs.pop('file_format')
        self.file_type = 'NA'
        if 'file_type' in kwargs:
            self.file_type = kwargs.pop('file_type')
        self.file_nans = None
        if 'file_nans' in kwargs:
            self.file_nans = kwargs.pop('file_nans')
        self.file_time = None
        if 'file_time' in kwargs:
            self.file_time = kwargs.pop('file_time')
        self.file_name = None
        if 'file_name' in kwargs:
            self.file_name = kwargs.pop('file_name')
        if 'file_freq' in kwargs:
            self.file_freq = kwargs.pop('file_freq')

        self.map_dims, self.map_geo, self.map_data = {}, {}, {}
        if 'map_dims' in kwargs:
            self.map_dims = kwargs.pop('map_dims')
        if 'map_geo' in kwargs:
            self.map_geo = kwargs.pop('map_geo')
        if 'map_data' in kwargs:
            self.map_data = kwargs.pop('map_data')

        self._template = {}
        self.tags = {}

    def __repr__(self):
        return f"{self.__class__.__name__}({self.file_name})"

    def _create_new_instance(self, data, options):
        return DataGrid(data, **options)

    def estimate_time_frequency(self, time_dim: str = 'time'):
        time_freq = None
        if self.file_time is not None:
            time_values = self.data[time_dim].values
            if len(time_values) >= 2:
                time_start, time_end = time_values[0], time_values[1]
                if is_date(time_start) and is_date(time_end):
                    time_freq = (time_end - time_start)
        return time_freq

    # method to add time period (if needed)
    def add_time_period(self, time_dim: str = 'time', time_freq: str = 'h' ) -> xr.Dataset:

        if time_freq is None:
            time_freq = self.file_freq

        from copy import deepcopy
        data = deepcopy(self.data)
        if time_dim in list(self.data.dims):
            time_values = self.data[time_dim].values
            time_check = is_date(time_values[0])
            if not time_check:
                if self.file_time is not None:
                    time_values = pd.date_range(start=self.file_time, periods=len(time_values), freq=time_freq)
                    data[time_dim] = time_values.values
                else:
                    warnings.warn(f'Time values are not defined by dates. Time of the file is defined by NoneType.')
        else:
            if data.ndim > 2:
                warnings.warn(f'Time dimension {time_dim} not found in dataset.')

        self.data = data

        return self._create_new_instance(data=data, options=self.options)

    def squeeze_data_by_time(self, time_step: (str, pd.Timestamp), time_dim: str = 'time', **kwargs):

        self.add_time_period(time_dim=time_dim)

        data_tmp = deepcopy(self.data)
        data_tmp = data_tmp.sel(time=time_step, method='nearest')

        self.options['file_time'] = time_step

        return self._create_new_instance(data=data_tmp, options=self.options)

    def squeeze_data_by_variable(self):
        print()

    @classmethod
    def get_type(cls, file_type: Optional[str] = None):
        if file_type is not None:
            return file_type
        elif hasattr(cls, 'file_type'):
            return cls.file_type
        else:
            return cls._defaults['file_type']

    ## PROPERTIES
    @property
    def file_format(self):
        return self._format
    @file_format.setter
    def file_format(self, value):
        self._format = value

    @property
    def has_version(self):
        return '{file_version}' in self.key_pattern

    @property
    def key_pattern(self):
        raise NotImplementedError
    @key_pattern.setter
    def key_pattern(self, value):
        raise NotImplementedError

    @property
    def has_time(self):
        return '%' in self.key_pattern
