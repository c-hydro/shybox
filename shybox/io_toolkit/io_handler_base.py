"""
Class Features

Name:          handler_io_base
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20250124'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import os
import warnings
from typing import Optional
from datetime import datetime
import numpy as np
import pandas as pd
import xarray as xr

from shybox.type_toolkit.lib_type_grid import DataGrid

from shybox.io_toolkit.lib_io_ascii_grid import get_file_grid as get_file_grid_ascii
from shybox.io_toolkit.lib_io_ascii_array import get_file_array as get_file_array_ascii
from shybox.io_toolkit.lib_io_tiff import get_file_grid as get_file_grid_tiff
from shybox.io_toolkit.lib_io_nc import get_file_grid as get_file_grid_nc

from shybox.generic_toolkit.lib_utils_time import is_date
from shybox.generic_toolkit.lib_default_args import logger_name, logger_arrow

import matplotlib.pylab as plt

# logging
logger_stream = logging.getLogger(logger_name)

# debugging
# import matplotlib.pylab as plt
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# class io base
class IOHandler:

    type_class = 'io_base'
    type_data_grid = {
        'ascii_raster': get_file_grid_ascii, 'tiff_raster': get_file_grid_tiff, 'netcdf_raster': get_file_grid_nc}
    type_data_array = {
        'ascii_array': get_file_array_ascii}
    type_data_point = {
        'ascii_time_series': None, 'csv_time_series': None,
    }

    def __init__(self, file_name: str, file_time: (str, pd.Timestamp) = None,
                 file_type: str = 'raster', file_format: Optional[str] = None,
                 map_dims: Optional[dict] = None, map_geo: Optional[dict] = None,
                 map_data: (list, dict) = None, **kwargs) -> None:

        if file_type is None:
            file_type = 'raster'

        self.file_name = file_name
        self.file_time = file_time
        self.file_type = file_type.lower()

        self.file_format = file_format if file_format is not None else self.file_name.split('.')[-1]
        if self.file_format.lower() in ['tif', 'tiff', 'geotiff']:
            self.file_format = 'tiff'
        elif self.file_format.lower() in ['txt', 'asc', 'ascii']:
            self.file_format = 'ascii'
        elif self.file_format.lower() in ['nc', 'netcdf', 'nc4']:
            self.file_format = 'netcdf'
        else:
            raise ValueError(f'Format {self.file_format} not supported.')

        if 'raster' in self.file_type:
            if self.file_format == 'ascii' or self.file_format == 'netcdf':

                if self.file_format not in self.file_type:
                    self.file_type = self.__compose_type(self.file_type, self.file_format)

                self.fx_data = self.type_data_grid.get(self.file_type, self.error_data)
            else:
                raise ValueError(f'Format {self.file_format} not supported for type {self.file_type}.')
        elif 'array' in self.file_type:
            if self.file_format == 'ascii':

                if self.file_format not in self.file_type:
                    self.file_type = self.__compose_type(self.file_type, self.file_format)

                self.fx_data = self.type_data_array.get(self.file_type, self.error_data)
            else:
                raise ValueError(f'Format {self.file_format} not supported for type {self.file_type}.')
        elif 'point' in self.file_type:
            if self.file_format == 'ascii':

                if self.file_format not in self.file_type:
                    self.file_type = self.__compose_type(self.file_type, self.file_format)

                self.fx_data = self.type_data_point.get(self.file_type, self.error_data)
            else:
                raise ValueError(f'Format {self.file_format} not supported for type {self.file_type}.')
        else:
            raise ValueError(f'Type {self.file_type} not supported.')

        if isinstance(map_data, list):
            map_data = {var_name: var_name for var_name in map_data}

        self.map_dims, self.map_geo, self.map_data = map_dims, map_geo, map_data

    @staticmethod
    def __compose_type(file_type: str, file_format: str) -> str:
        return f'{file_format}_{file_type}'

    @classmethod
    def from_path(cls, folder_name: str, file_name: str, file_format: Optional[None] = None, **kwargs):
        file_name = os.path.join(folder_name, file_name)
        return cls(file_name=file_name, file_format=file_format, **kwargs)

    # method to get data
    def get_data(self,
                 row_start: int = None, row_end: int = None,
                 col_start: int = None, col_end: int = None,
                 mandatory: bool = False, **kwargs) -> (xr.DataArray, xr.Dataset):
        """
        Get the data for a given time.
        """

        exist_flag = self.exist_data(file_name=self.file_name, mandatory=mandatory)
        if exist_flag:

            obj_data = self.fx_data(
                file_name=self.file_name,
                file_map_dims=self.map_dims, file_map_geo=self.map_geo, file_map_data=self.map_data)

            if row_start is not None and row_end is not None and col_start is not None and col_end is not None:
                obj_data = obj_data.isel(latitude=slice(row_start, row_end), longitude=slice(col_start, col_end))
        else:
            obj_data = None

        if isinstance(obj_data, xr.Dataset):
            obj_data = DataGrid(
                data=obj_data,
                file_format=self.file_format, file_type=self.file_type, file_name=self.file_name,
                file_time=self.file_time, map_dims=self.map_dims, map_geo=self.map_geo, map_data=self.map_data)
            obj_data = obj_data.add_time_period(time_dim='time', time_freq='h')

        return obj_data

    # method to select data
    def select_data(self, obj_data: xr.Dataset) -> xr.Dataset:
        map_data = self.map_data
        if map_data is not None:
            if isinstance(obj_data, xr.Dataset):
                vars_data = list(obj_data.variables)
                select_vars = []
                for var_in, var_out in list(map_data.items()):
                    if var_in in vars_data:
                        select_vars.append(var_in)
                    else:
                        warnings.warn(f'Variable {var_in} not found in dataset.')
                obj_data = obj_data[select_vars]

        return obj_data

    # method to remap time
    def remap_time(self, obj_data: xr.Dataset, time_dim: str = 'time', time_freq='h') -> xr.Dataset:

        if time_dim in list(obj_data.dims):
            time_values = obj_data[time_dim].values
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

    # method to fill data
    def fill_data(self, default_value: (int, float) = np.nan) -> xr.DataArray:
        """
        Fill the data for a given .
        """
        raise NotImplementedError

    # method to write data
    def write_data(self, data: xr.DataArray, time: Optional[datetime], tags: dict, **kwargs):
        """
        Write the data for a given time.
        """
        raise NotImplementedError

    # method to view data
    @staticmethod
    def view_data(obj_data: (xr.DataArray, xr.Dataset),
                  var_name: str = None,
                  var_data_min: (int, float) = None, var_data_max: (int, float) = None,
                  var_fill_data: (int, float) = np.nan, var_null_data: (int, float) = np.nan,
                  view_type: str = 'data_array', **kwargs) -> None:
        """
        View the data for a given time.
        """

        if isinstance(obj_data, xr.Dataset):
            if var_name is None:
                raise ValueError('Variable name not provided for dataset mode.')
            if var_name not in obj_data:
                raise ValueError(f'Variable {var_name} not found in dataset.')
            obj_data = obj_data[var_name]

        if var_data_min is not None and var_fill_data is not None:
            obj_data = obj_data.where(obj_data >= var_data_min, var_fill_data)
        if var_data_max is not None and var_fill_data is not None:
            obj_data = obj_data.where(obj_data <= var_data_max, var_fill_data)
        if var_fill_data is not None and var_null_data is not None:
            obj_data = obj_data.where(obj_data != var_null_data, var_null_data)

        if view_type == 'data_array':
            plt.figure()
            obj_data.plot()
            plt.colorbar()
        elif view_type == 'array':
            plt.figure()
            plt.imshow(obj_data.values)
            plt.colorbar()
        else:
            raise ValueError(f'View type {view_type} not supported.')

    # method to check data
    @staticmethod
    def exist_data(file_name : str, mandatory: bool = False, **kwargs) -> bool:
        """
        Check if data is available for a given time.
        """
        if os.path.exists(file_name):
            return True
        else:
            if mandatory:
                logger_stream.error(logger_arrow.error + 'File "' + file_name + '" does not exist')
                raise IOError(f'File {file_name} not found. Mandatory file is required to run the process')
            return False

    # raise error data for not implemented methods
    def error_data(self):
        """
        Error data.
        """
        raise NotImplementedError