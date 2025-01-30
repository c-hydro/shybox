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

from shybox.dataset_toolkit.merge import handler_data_grid as handler_data

from shybox.generic_toolkit.lib_utils_dict import create_dict_from_list
from shybox.generic_toolkit.lib_default_args import time_format_datasets, time_format_algorithm
from shybox.generic_toolkit.lib_default_args import logger_name, logger_arrow
from shybox.generic_toolkit.lib_default_args import collector_data

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
                 file_format: str = 'netcdf', **kwargs) -> None:

        self.file_name = file_name
        self.file_time = file_time
        self.file_format = file_format


    # ------------------------------------------------------------------------------------------------------------------


    # ------------------------------------------------------------------------------------------------------------------
    # configure data variable(s)
    def configure_variable_data(self):
        pass

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
