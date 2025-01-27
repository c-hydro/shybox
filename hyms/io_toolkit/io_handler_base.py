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
import xarray as xr

from hyms.io_toolkit.lib_io_ascii_grid import get_file_grid as get_file_grid_ascii
from hyms.io_toolkit.lib_io_ascii_array import get_file_array as get_file_array_ascii
from hyms.io_toolkit.lib_io_tiff import get_file_grid as get_file_grid_tiff
from hyms.io_toolkit.lib_io_nc import get_file_grid as get_file_grid_nc

from hyms.generic_toolkit.lib_default_args import logger_name, logger_arrow

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

    def __init__(self, file_name: str, file_type: str = 'raster',
                 file_format: Optional[str] = None,
                 map_dims: Optional[dict] = None, map_vars: Optional[dict] = None, **kwargs) -> None:

        if file_type is None:
            file_type = 'raster'

        self.file_name = file_name
        self.file_type = file_type.lower()

        self.file_format = file_format if file_format is not None else self.file_name.split('.')[-1]
        if self.file_format.lower() in ['tif', 'tiff', 'geotiff']:
            self.file_format = 'tiff'
        elif self.file_format.lower() in ['txt', 'asc']:
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

        self.map_dims, self.map_vars = map_dims, map_vars

    @staticmethod
    def __compose_type(file_type: str, file_format: str) -> str:
        return f'{file_format}_{file_type}'

    @classmethod
    def from_path(cls, folder_name: str, file_name: str, file_format: Optional[None] = None, **kwargs):
        file_name = os.path.join(folder_name, file_name)
        return cls(file_name=file_name, file_format=file_format, **kwargs)

    def get_data(self,
                 row_start: int = None, row_end: int = None,
                 col_start: int = None, col_end: int = None,
                 mandatory: bool = False, **kwargs) -> (xr.DataArray, xr.Dataset):
        """
        Get the data for a given time.
        """

        obj_flag = self.check_data(file_name=self.file_name, mandatory=mandatory)
        if obj_flag:
            obj_data = self.fx_data(file_name=self.file_name)
            if row_start is not None and row_end is not None and col_start is not None and col_end is not None:
                obj_data = obj_data.isel(latitude=slice(row_start, row_end), longitude=slice(col_start, col_end))
        else:
            obj_data = None

        return obj_data

    def adjust_data(self, obj_data: xr.Dataset, type_data='netcdf_v1') -> xr.Dataset:
        obj_data = self.filter_data(obj_data)
        obj_data = self.map_data(obj_data)
        return

    def filter_data(self, obj_data: xr.Dataset) -> xr.Dataset:
        vars_list = self.vars_list
        if vars_list is not None:
            if isinstance(obj_data, xr.Dataset):
                vars_data = list(obj_data.variables)
                vars_found = []
                for var_name in vars_list:
                    if var_name in vars_data:
                        vars_found.append(var_name)
                    else:
                        warnings.warn(f'Variable {var_name} not found in dataset.')
                obj_data = obj_data[vars_found]
        return obj_data

    def map_data(self, obj_data: xr.Dataset) -> xr.Dataset:
        vars_mapping = self.vars_mapping
        if vars_mapping is not None:
            if isinstance(obj_data, xr.Dataset):
                vars_data = list(obj_data.variables)
                vars_found = {}
                for var_name_in, var_name_out in vars_mapping.items():
                    if var_name_in in vars_data:
                        vars_found[var_name_in] = var_name_out
                    else:
                        warnings.warn(f'Variable {var_name_in} not found in dataset.')
                obj_data = obj_data.rename(vars_found)
        return obj_data

    def fill_data(self, default_value: (int, float) = np.nan) -> xr.DataArray:
        """
        Fill the data for a given .
        """
        raise NotImplementedError

    def error_data(self):
        """
        Error data.
        """
        raise NotImplementedError
    
    def write_data(self, data: xr.DataArray, time: Optional[datetime], tags: dict, **kwargs):
        """
        Write the data for a given time.
        """
        raise NotImplementedError
    
    def view_data(self, obj_data: (xr.DataArray, xr.Dataset),
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

    def check_data(self, file_name : str, mandatory: bool = False, **kwargs) -> bool:
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
