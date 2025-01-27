"""
Class Features

Name:          driver_data_grid
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20241126'
Version:       '4.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import os
import pandas as pd
import xarray as xr

from typing import Optional
from copy import deepcopy

from hyms.dataset_toolkit.merge import handler_data_grid as handler_data

from hyms.generic_toolkit.lib_utils_time import convert_time_format
from hyms.generic_toolkit.lib_utils_string import fill_tags2string
from hyms.generic_toolkit.lib_utils_dict import create_dict_from_list
from hyms.generic_toolkit.lib_default_args import time_format_datasets, time_format_algorithm
from hyms.generic_toolkit.lib_default_args import logger_name, logger_arrow
from hyms.generic_toolkit.lib_default_args import collector_data

from hyms.io_toolkit import io_handler_base

from hyms.io_toolkit.lib_io_utils import substitute_string_by_date, substitute_string_by_tags
from hyms.io_toolkit.lib_io_variables import fill_var_generic, fill_var_air_pressure, fill_var_error
from hyms.io_toolkit.io_handler_base import IOHandler
from hyms.io_toolkit.zip_handler_base import ZipHandler

# logging
logger_stream = logging.getLogger(logger_name)

# debugging
# import matplotlib.pylab as plt
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# class to wrap zip handler
class ZipWrapper(ZipHandler):
    def __init__(self, file_name_compress: str, file_name_uncompress: str = None,
                 zip_extension: str = '.gz') -> None:
        super().__init__(file_name_compress, file_name_uncompress, zip_extension)

# class to wrap io handler
class IOWrapper(IOHandler):
    def __init__(self, file_name, file_format: str = None, **kwargs) -> None:
        super().__init__(file_name, file_format, **kwargs)

    def from_path(self, file_name: str, file_format: str = None, **kwargs):

        if file_format is None:
            file_format = file_name.split('.')[-1]
            logger_stream.warning(
                logger_arrow.warning +
                'File format not provided. Trying to infer it from file name. Select: "' + file_format + '"')

        return super().__init__(file_name=file_name, file_format=file_format, **kwargs)
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# class to configure data
class DrvData(ZipWrapper, IOWrapper):

    # ------------------------------------------------------------------------------------------------------------------
    # global variable(s)
    class_type = 'driver_data'
    file_handler = None
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # class initialization
    def __init__(self, file_name: str, file_time: pd.Timestamp = None,
                 file_type: str = 'raster', file_format: str = 'netcdf',
                 map_dims: {} = None, map_vars: dict = None, **kwargs)-> None:

        self.file_name = file_name
        self.file_time = file_time
        self.file_format = file_format
        self.file_type = file_type

        self.map_dims, self.map_vars = map_dims, map_vars

        extra_args = {'map_dims': self.map_dims, 'map_vars': self.map_vars}

        super().__init__(file_name_compress=self.file_name, file_name_uncompress=None,
                         zip_extension='.gz')

        if self.zip_check:
            super().from_path(self.file_name_uncompress, file_format=self.file_format, **extra_args)
            self.uncompress_file_name()
            self.file_name, self.file_tmp = self.file_name_uncompress, self.file_name_compress
        else:
            super().from_path(self.file_name_compress, **extra_args)

        self.file_handler = io_handler_base.IOHandler(
            file_name=self.file_name, file_type=self.file_type, file_format=self.file_format)

    @classmethod
    def by_file_generic(cls, file_name: (str, None) = 'hmc.forcing-grid.{file_datetime}.nc.gz',
                       file_time: (str, pd.Timestamp) = None, file_format='netcdf',
                       file_tags: dict = None,
                       file_mandatory: bool = True, file_template: dict = None,
                       map_dims: dict = None, map_vars: dict = None):

        file_time = convert_time_format(file_time, time_conversion='str_to_stamp')

        if file_tags is None:
            file_tags = {'file_datetime': file_time, 'file_sub_path': file_time}
        if file_template is None:
            file_template = {'file_datetime': '%Y%m%d%H00', 'file_sub_path': '%Y/%m/%d'}
        if map_dims is None:
            map_dims = {}
        if map_vars is None:
            map_vars = {}

        file_name = fill_tags2string(file_name, file_template, file_tags)[0]

        if file_mandatory:
            if not os.path.exists(file_name):
                logger_stream.error(logger_arrow.error + 'File "' + file_name + '" does not exist')
                raise FileNotFoundError(f'File {file_name} is mandatory and must defined to run the process')

        return cls(file_name, file_format=file_format, map_dims=map_dims, map_vars=map_vars)

    # ------------------------------------------------------------------------------------------------------------------


    # ------------------------------------------------------------------------------------------------------------------
    # get variable data
    def get_variable_data(self, row_start: int = None, row_end: int = None,
                                col_start: int = None, col_end: int = None) -> xr.Dataset:


        obj_data_domain = self.file_handler.get_data(
            row_start=row_start, row_end=row_end, col_start=col_start, col_end=col_end, mandatory=True)

        self.file_handler.view_data(obj_data=obj_data_domain,
                                   var_name='AirTemperature', var_data_min=0, var_data_max=None)

    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # method to organize data variables
    def organize_variable_data(self):
        pass
    # ------------------------------------------------------------------------------------------------------------------


    # ------------------------------------------------------------------------------------------------------------------
    # method to view data variable(s)
    def view_variable_data(self, data: dict = None, mode: bool = True) -> None:
        # info algorithm (start)
        logger_stream.info(logger_arrow.info(tag='info_method') + 'View "time_variables" ... ')
        if mode:
            self.data_handler.view(data)
        # info algorithm (start)
        logger_stream.info(logger_arrow.info(tag='info_method') + 'View "time_variables" ... DONE')
    # ------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
