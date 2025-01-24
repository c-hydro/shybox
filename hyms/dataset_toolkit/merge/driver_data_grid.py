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
import pandas as pd

from copy import deepcopy

from hyms.dataset_toolkit.merge import handler_data_grid as handler_data

from hyms.generic_toolkit.lib_utils_file import split_file_path
from hyms.generic_toolkit.lib_utils_dict import create_dict_from_list
from hyms.generic_toolkit.lib_default_args import time_format_datasets, time_format_algorithm
from hyms.generic_toolkit.lib_default_args import logger_name, logger_arrow
from hyms.generic_toolkit.lib_default_args import collector_data

from hyms.io_toolkit import io_handler_base

# logging
logger_stream = logging.getLogger(logger_name)

# debugging
# import matplotlib.pylab as plt
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# class to configure data
class DrvData(object):

    # ------------------------------------------------------------------------------------------------------------------
    # global variable(s)
    class_type = 'driver_data'
    data_handler = None
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # class initialization
    def __init__(self, file_name: str, file_time: pd.Timestamp = None,
                 file_type: str = 'grid', file_format: str = 'netcdf', **kwargs) -> None:

        self.file_name = file_name
        self.file_time = file_time
        self.file_format = file_format
        self.file_type = file_type

        self.folder_name, self.file_name = split_file_path(self.file_name)



    # ------------------------------------------------------------------------------------------------------------------


    # ------------------------------------------------------------------------------------------------------------------
    # configure data variable(s)
    def configure_variable_data(self):

        row_start, row_end, col_start, col_end = 0, 9, 3, 15

        obj_data_handler = io_handler_base.IOHandler(file_name=file_name, folder_name=folder_name)
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
