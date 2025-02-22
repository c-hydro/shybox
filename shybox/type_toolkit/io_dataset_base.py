from typing import Optional, Generator, Callable, Union
import datetime as dt
import numpy as np
import pandas as pd
import xarray as xr

from abc import ABC, ABCMeta, abstractmethod
import os
import re

from shybox.default.lib_default_geo import crs_wkt as default_crs_wkt

from shybox.type_toolkit.parse_utils import substitute_string, extract_date_and_tags
from shybox.type_toolkit.io_utils import (get_format_from_path, map_dims, flat_dims,
                                          straighten_data, set_type, check_data_format)

# gestione path del file
# gestione dello zip del file
# gestione times del file
# gestione read and write generic fx

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

        self.time_signature = None
        if 'time_signature' in kwargs:
            self.time_signature = kwargs.pop('time_signature')

        if 'tile_names' in kwargs:
            self.tile_names = kwargs.pop('tile_names')

        self.nan_value = None
        if 'nan_value' in kwargs:
            self.nan_value = kwargs.pop('nan_value')

        self.file_template = {}
        if 'file_template' in kwargs:
            self.file_template = kwargs.pop('file_template')

        self._template = {}
        self.options = kwargs
        self.tags = {}

        self.data_store = None

    def __repr__(self):
        return f"{self.__class__.__name__}({self.file_name}, {self.file_mode})"

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
            new_options.update({'loc_pattern': new_loc_pattern, 'name': new_name})
            new_dataset = self.__class__(**new_options)

            new_dataset._template = self._template
            if hasattr(self, '_tile_names'):
                new_dataset._tile_names = self._tile_names

            new_dataset.time_signature = self.time_signature
            
            new_tags = self.tags.copy()
            new_tags.update(kwargs)
            new_dataset.tags = new_tags
            return new_dataset

    def copy(self, template = False):
        new_dataset = self.update()
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
    def file_mode(self):
        return self._mode

    @file_mode.setter
    def file_mode(self, value):
        self._mode = value

    @property
    def has_version(self):
        return '{file_version}' in self.loc_pattern

    @property
    def has_tiles (self):
        return '{tile}' in self.loc_pattern

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
    def loc_pattern(self):
        raise NotImplementedError

    @loc_pattern.setter
    def loc_pattern(self, value):
        raise NotImplementedError

    @property
    def available_keys(self):
        return self.get_available_keys()
    
    def get_available_keys(self, time: (dt.datetime,xr.date_range) = None, **kwargs):
        
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

    def get_last_date(self, now = None, n = 1, **kwargs) -> (dt.datetime, list[dt.datetime], None):
        if now is None:
            now = dt.datetime.now()
        
        # the most efficient way, I think is to search my month
        this_month = Month(now.year, now.month)
        last_date = []
        while len(last_date) < n:
            this_month_times = self.get_times(this_month, **kwargs)
            if len(this_month_times) > 0:
                valid_time = [t for t in this_month_times if t <= now]
                valid_time.sort(reverse = True)
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

    def get_last_ts(self, **kwargs) -> (pd.Timestamp, None):

        last_dates = self.get_last_date(n = 15, **kwargs)
        if last_dates is None:
            return None
        
        timestep = estimate_timestep(last_dates)
        if timestep is None:
            return None

        if self.time_signature == 'end+1':
            return timestep.from_date(max(last_dates)) -1
        else:
            return timestep.from_date(max(last_dates))

    def estimate_timestep(self) -> pd.Timestamp:
        last_dates = self.get_last_date(n = 15)
        timestep = estimate_timestep(last_dates)
        return timestep

    def get_first_date(self, start = None, n = 1, **kwargs) -> (dt.datetime, list[dt.datetime], None):
        if start is None:
            start = dt.datetime(1900, 1, 1)

        end = self.get_last_date(**kwargs)
        if end is None:
            return None
        
        start_month = Month(start.year, start.month)
        end_month   = Month(end.year, end.month)

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

        first_dates = self.get_first_date(n = 15, **kwargs)
        if first_dates is None:
            return None
        
        timestep = estimate_timestep(first_dates)
        if timestep is None:
            return None

        if self.time_signature == 'end+1':
            return timestep.from_date(min(first_dates)) -1
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
        if value not in ['start', 'end', 'end+1', None]:
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

        key_without_tags = re.sub(r'\{[^}]*\}', '', self.key_pattern)
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

        full_key = self.get_key(time, **kwargs)

        if self.file_format in ['csv', 'json', 'txt', 'shp']:
            if self._check_data(full_key):

                data = self._read_data(full_key)

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
            
        if self.check_data(time, **kwargs):

            if self.data_store is None:

                data = self._read_data(full_key)

                if as_is:
                    return data

                # map the data to the template
                data = map_dims(data, **self.file_template)
                # ensure that the data has descending latitudes
                data = straighten_data(data)
                # ensure that the data dimensions are flat
                data = flat_dims(data)

                # make sure the nodata value is set to np.nan for floats and to the max int for integers
                data = set_type(data, self.nan_value)

                self.data_store = data



        else:
            raise ValueError(f'Could not resolve data from {full_key}.')

        # if there is no template for the dataset, create it from the data
        template_dict = self.get_template_dict(make_it=False, **kwargs)
        if template_dict is None:
            #template = self.make_templatearray_from_data(data)
            self.set_template(data, **kwargs)
        else:
            # otherwise, update the data in the template
            # (this will make sure there is no errors in the coordinates due to minor rounding)
            attrs = data.attrs
            data = self.set_data_to_template(data, template_dict)
            data.attrs.update(attrs)
        
        data.attrs.update({'source_key': full_key})
        return data
    
    @abstractmethod
    def _read_data(self, input_key:str):
        raise NotImplementedError

    def write_data(self, data,
                   time: Union[dt.datetime, pd.Timestamp] = None,
                   time_format: str = '%Y-%m-%d',
                   metadata = {},
                   **kwargs):
        
        check_data_format(data, self.format)

        output_file = self.get_key(time, **kwargs)

        if self.format in ['csv', 'json', 'txt', 'shp']:
            append = kwargs.pop('append', False)
            self._write_data(data, output_file, append = append)
            return
        
        if self.format == 'file':
            self._write_data(data, output_file)
            return
        
        # if data is a numpy array, ensure there is a template available
        try:
            template_dict = self.get_template_dict(**kwargs)
        except PermissionError:
            template_dict = None

        if template_dict is None:
            if isinstance(data, xr.DataArray) or isinstance(data, xr.Dataset):
                #templatearray = self.make_templatearray_from_data(data)
                self.set_template(data, **kwargs)
                template_dict = self.get_template_dict(**kwargs, make_it=False)
            else:
                raise ValueError('Cannot write numpy array without a template.')
        
        output = self.set_data_to_template(data, template_dict)
        output = set_type(output, self.nan_value)
        output = straighten_data(output)
        output.attrs['source_key'] = output_file

        # if necessary generate the thubnail
        if 'parents' in metadata:
            parents = metadata.pop('parents')
        else:
            parents = {}


        # add the metadata
        old_attrs = data.attrs if hasattr(data, 'attrs') else {}
        new_attrs = output.attrs
        old_attrs.update(new_attrs)
        output.attrs = old_attrs
        
        name = substitute_string(self.name, kwargs)
        metadata['name'] = name
        output = self.set_metadata(output, time, time_format, **metadata)
        # write the data
        self._write_data(output, output_file)
        

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
        if 'tile' in kwargs:
            full_key = self.get_key(time, **kwargs)
            if self._check_data(full_key):
                return True
            else:
                return False

        for tile in self.tile_names:
            if not self.check_data(time, tile = tile, **kwargs):
                return False
        else:
            return True
    
    @with_cases
    def find_times(self, times: list[pd.date_range, dt.datetime],
                   id = False, rev = False, **kwargs) -> (list[pd.Timestamp], list[int]):
        """
        Find the times for which data is available.
        """
        all_ids = list(range(len(times)))

        time_signatures = [self.get_time_signature(t) for t in times]
        tr = pd.date_range(min(time_signatures), max(time_signatures))

        all_times = self.get_available_tags(tr, **kwargs).get('time', [])

        ids = [i for i in all_ids if time_signatures[i] in all_times] or []
        if rev:
            ids = [i for i in all_ids if i not in ids] or []

        if id:
            return ids
        else:
            return [times[i] for i in ids]

    @with_cases
    def find_tiles(self, time: Union[pd.Timestamp, dt.datetime] = None, rev = False, **kwargs) -> list[str]:
        """
        Find the tiles for which data is available.
        """
        all_tiles = self.tile_names
        available_tiles = self.get_available_tags(time, **kwargs).get('tile', [])
        
        if not rev:
            return [tile for tile in all_tiles if tile in available_tiles]
        else:
            return [tile for tile in all_tiles if tile not in available_tiles]

    @abstractmethod
    def _check_data(self, data_key) -> bool:
        raise NotImplementedError

    ## METHODS TO MANIPULATE THE DATASET
    def get_key(self, time: Union[pd.Timestamp, dt.datetime] = None, **kwargs):
        
        time = self.get_time_signature(time)
        raw_key = substitute_string(self.loc_pattern, kwargs)
        key = time.strftime(raw_key) if time is not None else raw_key
        return key

    def set_parents(self, parents:dict[str:'Dataset'], fn:Callable):
        self.parents = parents
        self.fn = fn

    ## METHODS TO MANIPULATE THE TEMPLATE
    def get_template_dict(self, make_it:bool = True, **kwargs):
        tile = kwargs.pop('tile', None)
        if tile is None:
            if self.has_tiles:
                template_dict = {}
                for tile in self.tile_names:
                    template_dict[tile] = self.get_template_dict(make_it = make_it, tile = tile, **kwargs)
                return template_dict
            else:
                tile = '__tile__'

        template_dict = self._template.get(tile, None)
        if template_dict is None and make_it:
            if not self.has_time:
                data = self.get_data(as_is = True, **kwargs)
                self.set_template(data, tile = tile)

            else:
                first_date = self.get_first_date(tile = tile, **kwargs)
                if first_date is not None:
                    data = self.get_data(time = first_date, tile = tile, as_is=True, **kwargs)
                else:
                    return None
            
            data = straighten_data(data)
            #templatearray = self.make_templatearray_from_data(start_data)
            self.set_template(data, tile = tile)
            template_dict = self.get_template_dict(make_it = False, tile = tile, **kwargs)
        
        return template_dict
    
    
    def set_template(self, template_array: (xr.DataArray,xr.Dataset), **kwargs):
        tile = kwargs.get('tile', '__tile__')
        # save in self._template the minimum that is needed to recreate the template
        # get the crs and the nodata value, these are the same for all tiles
        crs = template_array.attrs.get('crs', None)

        if crs is not None:
            crs_wkt = crs.to_wkt()
        elif hasattr(template_array, 'spatial_ref'):
            crs_wkt = template_array.spatial_ref.crs_wkt
        elif hasattr(template_array, 'crs'):
            crs_wkt = template_array.crs.crs_wkt
        else:
            crs_wkt = default_crs_wkt

        self._template[tile] = {'crs': crs_wkt,
                                '_FillValue' : template_array.attrs.get('_FillValue'),
                                'dims_names' : template_array.dims,
                                'spatial_dims' : (template_array.longitude, template_array.latitude),
                                'dims_starts': {},
                                'dims_ends': {},
                                'dims_lengths': {}}

        if isinstance(template_array, xr.Dataset):
            self._template[tile]['variables'] = list(template_array.data_vars)

        for dim in template_array.dims:
            this_dim_values = template_array[dim].data
            start = this_dim_values[0]
            end = this_dim_values[-1]
            length = len(this_dim_values)
            self._template[tile]['dims_starts'][dim] = float(start)
            self._template[tile]['dims_ends'][dim] = float(end)
            self._template[tile]['dims_lengths'][dim] = length

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
        template = template.rio.set_spatial_dims(*template_dict['spatial_dims']).rio.write_crs(template_dict['crs']).rio.write_coordinate_system()

        if 'variables' in template_dict:
            template_ds = xr.Dataset({var: template.copy() for var in template_dict['variables']})
            return template_ds
        
        return template

    @staticmethod
    def set_data_to_template(data: (np.ndarray, xr.DataArray, xr.Dataset),
                             template_dict: dict) -> (xr.DataArray, xr.Dataset):
        
        if isinstance(data, xr.DataArray):
            #data = straighten_data(data)
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

        name = metadata.get('name', self.name)
        if 'long_name' in metadata:
            metadata.pop('long_name')

        data.attrs.update(metadata)

        if isinstance(data, xr.DataArray):
            data.name = name

        return data
