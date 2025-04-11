from typing import Optional, Generator, Callable, Union
import datetime as dt
import numpy as np
import pandas as pd
import xarray as xr

from copy import deepcopy
from abc import ABC, ABCMeta, abstractmethod
import os
import re

from shybox.default.lib_default_geo import crs_wkt as default_crs_wkt
from shybox.generic_toolkit.lib_utils_time import convert_time_format

from shybox.dataset_toolkit.lib_dataset_parse import substitute_string, extract_date_and_tags
from shybox.dataset_toolkit.lib_dataset_generic import (
    get_format_from_path, map_dims, map_coords, map_vars, flat_dims,
    straighten_data, straighten_time, straighten_dims, select_by_time, select_by_vars,
    set_type, check_data_format, get_variable)

import matplotlib.pyplot as plt

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


class Dataset(ABC, metaclass=DatasetMeta):

    _defaults = {'type': 'local',
                 'time_signature' : 'end'}

    def __init__(self, **kwargs):

        # substitute "now" with the current time
        self.loc_pattern = substitute_string(self.loc_pattern, {'now': dt.datetime.now()})

        if 'file_name' in kwargs:
            self.file_name   = kwargs.pop('file_name')
        if 'file_format' in kwargs:
            self.file_format = kwargs.pop('file_format')
        else:
            self.file_format = get_format_from_path(self.loc_pattern)
        self.file_mode = None
        if 'file_mode' in kwargs:
            self.file_mode = kwargs.pop('file_mode')
        self.file_type = None
        if 'file_type' in kwargs:
            self.file_type = kwargs.pop('file_type')
        self.file_variable = 'default'
        if 'file_variable' in kwargs:
            self.file_variable = kwargs.pop('file_variable')

        self.time_signature = None
        if 'time_signature' in kwargs:
            self.time_signature = kwargs.pop('time_signature')

        self.nan_value = None
        if 'nan_value' in kwargs:
            self.nan_value = kwargs.pop('nan_value')

        self.file_template = {}
        if 'file_template' in kwargs:
            self.file_template = kwargs.pop('file_template')

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

        self.expected_time_steps = (
            self.time_reference, self.time_period, self.time_freq, self.time_direction, self.time_normalize)

        self._template = {}
        self.options = kwargs
        self.tags = {}

        self.memory_active = True
        self.memory_data = None

    def __repr__(self):
        return f"{self.__class__.__name__}({self.file_name}, {self.file_mode}, {self.file_type})"

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
            new_options.update({'loc_pattern': new_loc_pattern, 'file_name': new_file_name})

            new_dataset = self.__class__(**new_options)
            new_dataset.__dict__.update(self.__dict__)

            new_tags = self.tags.copy()
            new_tags.update(kwargs)
            new_dataset.tags = new_tags

            return new_dataset

    def copy(self, template = False):
        new_dataset = self.update()
        if template:
            new_dataset._template = self._template
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

    def get_expected_times(self, time_reference: (pd.Timestamp, None),
                           time_period: int = None, time_freq: str = 'h', time_direction: str = 'forward',
                           time_normalize: bool = False) -> pd.date_range:
        if time_reference is None or time_period is None:
            return None

        if time_direction == 'backward':
            time_obj = pd.date_range(end=time_reference, freq=time_freq, periods=time_period, normalize=time_normalize)
        elif time_direction == 'forward':
            time_obj = pd.date_range(start=time_reference, freq=time_freq, periods=time_period, normalize=time_normalize)
        else:
            raise RuntimeError('Invalid time direction.')
        return time_obj

    @property
    def available_keys(self):
        return self.get_available_keys()
    
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
        return '%' in self.loc_pattern

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
        if value not in ['period', 'step', 'current', 'start', 'end', None]:
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

    ## INPUT/OUTPUT METHODS
    def get_data(self, time: (dt.datetime, pd.Timestamp) = None, as_is = False, **kwargs):

        full_location = self.get_key(time, **kwargs)

        # check memory active (if defined true or false)
        if 'memory_active' in kwargs:
            self.memory_active = kwargs.pop('memory_active')

        mapping = None
        if 'map_in' in kwargs:
            mapping = kwargs.pop('map_in')

        if self.memory_active:
            if self.memory_data is not None:

                # get data from memory
                data = deepcopy(self.memory_data)

                # get variables
                variable = get_variable(data, **self.file_template)

                # if there is no template for the dataset, create it from the data
                template_dict = self.get_template_dict(make_it=False, **kwargs)
                if template_dict is None:
                    self.set_template(data, template_key=variable, **kwargs)
                else:
                    # otherwise, update the data in the template
                    # (this will make sure there is no errors in the coordinates due to minor rounding)
                    attrs = data.attrs
                    data = self.set_data_to_template(data, template_dict)
                    data.attrs.update(attrs)

                data.attrs.update({'source_location': full_location})

                if isinstance(data, xr.DataArray):
                    data.name = self.memory_data.name
                elif isinstance(data, xr.Dataset):
                    pass
                else:
                    raise ValueError(f"Invalid data type: {type(data)}")

                # select by time
                data = select_by_time(data, time=time, method='nearest')
                # select by variables
                data = select_by_vars(data, vars=mapping)

                return data

        if self.file_format in ['csv', 'json', 'txt', 'shp']:
            if self._check_data(full_location):

                data = self._read_data(full_location)

                if as_is:
                    return data

                if isinstance(data, xr.DataArray):
                    # map the data to the template
                    data = map_dims(data, **self.file_template)
                    # ensure that the data has descending latitudes
                    data = straighten_data(data)
                    # ensure that the data dimensions are flat
                    data = flat_dims(data)

                return data

            else:
                raise ValueError(f'Could not resolve data from {full_location}.')
            
        if self.check_data(time, **kwargs):

            data = self._read_data(full_location, input_mapping=mapping)

            if as_is:
                return data

            # ensure that the data dimensions are not empty
            data = straighten_dims(data)

            # map the data dimensions
            data = map_dims(data, **self.file_template)
            # map the data coords
            data = map_coords(data, **self.file_template)

            # map the data variables
            data = map_vars(data, **self.file_template)

            if not self.file_template:
                var_name = data.name
                if var_name is not None:
                    if var_name in list(mapping.keys()):
                        var_id = list(mapping.keys()).index(var_name)
                        var_upd = list(mapping.values())[var_id]
                        data.name = var_upd

            # ensure that the data has descending latitudes
            data = straighten_data(data)

            # ensure that the data dimensions are flat
            data = flat_dims(data)
            # ensure that the time info is correctly defined (if needed)
            data = straighten_time(
                data, time_file=self.time_reference, time_freq=self.time_freq, time_direction=self.time_direction)

            # check if time is valid
            idx, is_valid = self.check_time_step(time)

            # make sure the nodata value is set to np.nan for floats and to the max int for integers
            data = set_type(data, self.nan_value)
            # get variables
            variable = get_variable(data, **self.file_template)

            if self.memory_active:
                self.memory_data = deepcopy(data)

        else:
            raise ValueError(f'Could not resolve data from {full_location}.')

        # if there is no template for the dataset, create it from the data
        template_dict = self.get_template_dict(make_it=False, **kwargs)
        if template_dict is None:
            if self.memory_data is not None:
                self.set_template(template_array=self.memory_data, template_key=variable, **kwargs)
            else:
                self.set_template(template_array=data, template_key=variable, **kwargs)
        else:
            # otherwise, update the data in the template
            # (this will make sure there is no errors in the coordinates due to minor rounding)
            attrs = data.attrs
            if self.memory_data is not None:
                data = self.set_data_to_template(self.memory_data, template_dict)
            else:
                data = self.set_data_to_template(data, template_dict)
            data.attrs.update(attrs)

        # set attributes
        data.attrs.update({'source_location': full_location})

        # select by time
        data = select_by_time(data, time=time, method='nearest')
        # select by variables
        data = select_by_vars(data, vars=mapping)

        return data
    
    @abstractmethod
    def _read_data(self, input_key:str):
        raise NotImplementedError

    def write_data(self, data,
                   time: Union[dt.datetime, pd.Timestamp] = None,
                   time_format: str = '%Y-%m-%d',
                   metadata: dict = {}, variable: str = 'variable',
                   **kwargs):

        # check data (format and type)
        check_data_format(data, self.file_format)

        # define the output file
        out_file = self.get_key(time, **kwargs)
        out_path, out_name = os.path.dirname(out_file), os.path.basename(out_file)

        if self.file_format in ['csv', 'json', 'txt', 'shp']:
            append = kwargs.pop('append', False)
            self._write_data(data, out_file, append = append)
            return
        
        if self.file_format == 'file':
            self._write_data(data, out_file)
            return
        
        # if data is a numpy array, ensure there is a template available
        try:
            default_template = self.get_template_dict(**kwargs)
        except Exception as exc: #PermissionError as permission_error:
            default_template = None

        if default_template is None:
            if isinstance(data, xr.DataArray) or isinstance(data, xr.Dataset):
                self.set_template(data, template_key=variable, **kwargs)
                default_template = self.get_template_dict(**kwargs, make_it=False)
            else:
                raise ValueError('Cannot write numpy array without a template.')

        if isinstance(data, xr.Dataset):
            pass

        out_obj = self.set_data_to_template(data, default_template)
        out_obj = set_type(out_obj, self.nan_value)

        # adjust the data orientation
        out_obj = straighten_data(out_obj)
        # map the data variables
        out_obj = map_vars(out_obj, **self.file_template)
        # map the data dimensions
        out_obj = map_dims(out_obj, **self.file_template)

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
        

    def copy_data(self, new_loc_pattern, time: Union[dt.datetime, pd.Timestamp] = None, **kwargs):
        data = self.get_data(time, **kwargs)
        timestamp = self.get_time_signature(time)
        if timestamp is None:
            new_key = substitute_string(new_loc_pattern, kwargs)
        else:
            new_key = timestamp.strftime(substitute_string(new_loc_pattern, kwargs))
        self._write_data(data, new_key)

    def rm_data(self, time: Union[dt.datetime, pd.Timestamp] = None, **kwargs):
        key = self.get_key(time, **kwargs)
        self._rm_data(key)

    def move_data(self, new_loc_pattern, time: Union[dt.datetime, pd.Timestamp] = None, **kwargs):
        self.copy_data(new_loc_pattern, time, **kwargs)
        self.rm_data(time, **kwargs)

    @abstractmethod
    def _write_data(self, output: xr.DataArray, output_key: str):
        raise NotImplementedError
    
    @abstractmethod
    def _rm_data(self, key: str):
        raise NotImplementedError

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

    ## METHODS TO MANIPULATE THE DATASET
    def get_key(self, time: Union[pd.Timestamp, dt.datetime] = None, **kwargs):
        
        time = self.get_time_signature(time)
        raw_key = substitute_string(self.loc_pattern, kwargs)
        key = time.strftime(raw_key) if time is not None else raw_key
        return key

    ## METHODS TO MANIPULATE THE TEMPLATE
    def get_template_dict(self, make_it:bool = True, **kwargs):

        template_key = kwargs.pop('template_key', None)

        template_dict = self._template.get(template_key, None)
        if template_dict is None and make_it:
            if not self.has_time:
                data = self.get_data(as_is = True, **kwargs)
                self.set_template(data, template_key = template_key)

            else:
                time_start, idx_start = self.get_time_step(time_type='start', **kwargs)
                if time_start is not None:
                    data = self.get_data(time = time_start, as_is=True, **kwargs)
                else:
                    return None
            
            data = straighten_data(data)

            self.set_template(data)
            template_dict = self.get_template_dict(make_it = False, **kwargs)
        
        return template_dict
    
    
    def set_template(self, template_array: (xr.DataArray,xr.Dataset) = None,
                     template_key: str = 'data', **kwargs):
        # save in self._template the minimum that is needed to recreate the template
        # get the crs and the nodata value, these are the same for all tiles
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

        if not isinstance(template_key, list):
            template_key = [template_key]

        for step_key in template_key:
            if isinstance(template_array, xr.Dataset):
                self._template[step_key] = {'crs': crs_wkt,
                                        '_FillValue' : template_array[step_key].attrs.get('_FillValue'),
                                        'dims_names' : template_array[step_key].dims,
                                        'spatial_dims' : (template_array[step_key].longitude.name, template_array[step_key].latitude.name),
                                        'dims_starts': {},
                                        'dims_ends': {},
                                        'dims_lengths': {}}
            elif isinstance(template_array, xr.DataArray):
                self._template[step_key] = {'crs': crs_wkt,
                                            '_FillValue': template_array.attrs.get('_FillValue'),
                                            'dims_names': template_array.dims,
                                            'spatial_dims': (template_array.longitude.name,
                                                             template_array.latitude.name),
                                            'dims_starts': {},
                                            'dims_ends': {},
                                            'dims_lengths': {}}
            else:
                raise ValueError('Invalid template array type.')

        if isinstance(template_array, xr.Dataset):
            for step_key in template_key:
                self._template[step_key]['variables'] = list(template_array.data_vars)
            #self._template[template_key]['variables'] = list(template_array.data_vars)

        for step_key in template_key:
            for dim in template_array.dims:

                if isinstance(template_array, xr.DataArray):
                    this_dim_values = template_array[dim].data
                elif isinstance(template_array, xr.Dataset):
                    this_dim_values = template_array[step_key][dim].data
                else:
                    raise ValueError('Invalid template array type.')

                start = this_dim_values[0]
                end = this_dim_values[-1]
                length = len(this_dim_values)
                self._template[step_key]['dims_starts'][dim] = float(start)
                self._template[step_key]['dims_ends'][dim] = float(end)
                self._template[step_key]['dims_lengths'][dim] = length

    @staticmethod
    def build_template_array(template_dict: dict, data = None) -> (xr.DataArray,xr.Dataset):
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
    def set_data_to_template(data: (np.ndarray, xr.DataArray, xr.Dataset),
                             template_dict: dict) -> (xr.DataArray, xr.Dataset):

        if template_dict is None:
            return data

        if isinstance(data, xr.DataArray):
            data = Dataset.build_template_array(template_dict, data.values)
        elif isinstance(data, np.ndarray):
            data = Dataset.build_template_array(template_dict, data)
        elif isinstance(data, xr.Dataset):
            all_data = [Dataset.set_data_to_template(data[var], template_dict) for var in template_dict['variables']]
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
