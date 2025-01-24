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

from typing import Optional
from copy import deepcopy

from hyms.dataset_toolkit.merge import handler_data_grid as handler_data

from hyms.generic_toolkit.lib_utils_file import split_file_path
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
    def __init__(self, folder_name: str, file_name: str, file_format: Optional[str] = None, **kwargs) -> None:
        super().__init__(folder_name, file_name, file_format, **kwargs)

    def from_path(self, file_path: str, file_format: Optional[str] = None, **kwargs):
        folder_name, file_name = os.path.split(file_path)
        return super().__init__(folder_name, file_name, file_format, **kwargs)
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
                 file_vars: list = None, file_mapping: dict = None, **kwargs)-> None:

        self.file_name = file_name
        self.file_time = file_time
        self.file_format = file_format
        self.file_type = file_type

        self.file_handler = io_handler_base.IOHandler(
            file_name=self.file_name, file_type=self.file_type, file_format=self.file_format)

        self.file_vars = file_vars
        self.file_mapping = file_mapping

        extra_args = {'vars_list': self.file_vars, 'vars_mapping': self.file_mapping}

        super().__init__(file_name_compress=self.file_name, file_name_uncompress=None,
                         zip_extension='.gz')

        if self.zip_check:
            super().from_path(self.file_name_uncompress, **extra_args)
        else:
            super().from_path(self.file_name_compress, **extra_args)

    @classmethod
    def organize_file_data(cls, folder_name: str, file_name: str = 'hmc.forcing-grid.{datetime_dynamic_src_grid}.nc.gz',
                           file_time: pd.Timestamp = None,
                           file_tags: dict = None,
                           file_mandatory: bool = True, file_template: dict = None,
                           vars_list: dict = None, vars_tags: dict = None):

        if file_tags is None:
            file_tags = {}
        if file_template is None:
            file_template = {}
        if vars_list is None:
            vars_list = {}
        if vars_tags is None:
            vars_tags = {}

        vars_mapping = dict(zip(vars_list, vars_tags))

        folder_name = substitute_string_by_tags(folder_name, file_tags)
        folder_name = substitute_string_by_date(folder_name, file_time, file_template)
        file_name = substitute_string_by_tags(file_name, file_tags)
        file_name = substitute_string_by_date(file_name, file_time, file_template)

        if file_mandatory:
            if not os.path.exists(os.path.join(folder_name, file_name)):
                raise FileNotFoundError(f'File {file_name} does not exist in path {folder_name}.')

        return cls(folder_name, file_name, vars_list=vars_list, vars_mapping=vars_mapping)

    # ------------------------------------------------------------------------------------------------------------------


    # ------------------------------------------------------------------------------------------------------------------
    # configure data variable(s)
    def configure_variable_data(self):

        row_start, row_end, col_start, col_end = 0, 9, 3, 15


        obj_data_domain = obj_data_handler.get_data(
            row_start=row_start, row_end=row_end, col_start=col_start, col_end=col_end, mandatory=True)

        obj_data_handler.view_data(obj_data=obj_data_domain,
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
