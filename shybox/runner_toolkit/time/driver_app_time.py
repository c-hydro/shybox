"""
Class Features

Name:          driver_app_time
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20241126'
Version:       '4.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import pandas as pd

from copy import deepcopy

from shybox.runner_toolkit.time import handler_app_time as handler_time

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
# class to configure time
class DrvTime(object):

    # ------------------------------------------------------------------------------------------------------------------
    # global variable(s)
    class_type = 'driver_time'
    time_default_name = ['time_run', 'time_restart',
                         'time_start', 'time_end', 'time_period', 'time_frequency', 'time_rounding']
    time_default_nodata = [None, None, None, None, None, 'h', 'h']

    time_handler = None
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # class initialization
    def __init__(self, time_obj: dict, time_collectors: dict = None,
                 time_priority : str = 'user',
                 time_variables: list = None, time_nodata: list = None,
                 **kwargs) -> None:

        self.time_obj = time_obj

        if time_variables is None:
            self.time_variables = self.time_default_name
        else:
            self.time_variables = time_variables
        if time_nodata is None:
            self.time_nodata = self.time_default_nodata
        else:
            self.time_nodata = time_nodata

        self.time_default = create_dict_from_list(list_keys=self.time_variables, list_values=self.time_nodata)
        self.time_collector = time_collectors

        self.time_obj = self.__update_variables(self.time_collector)
        self.time_obj = self.__check_variables(self.time_default)

        self.time_run_file = self.time_obj['time_run']
        self.time_restart = self.time_obj['time_restart']
        self.time_period = self.time_obj['time_period']
        self.time_frequency = self.time_obj['time_frequency']
        self.time_rounding = self.time_obj['time_rounding']

        self.time_start = self.time_obj['time_start']
        if self.time_start is not None:
            self.time_start = pd.Timestamp(self.time_start)
        self.time_end = self.time_obj['time_end']
        if self.time_end is not None:
            self.time_end = pd.Timestamp(self.time_end)

        self.tag_root_priority, self.tag_root_flags = 'priority', 'flags'
        self.tag_root_variables, self.tag_root_configuration = 'variables', 'configuration'

    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # update time variables
    def __update_variables(self, time_info):

        time_obj = deepcopy(self.time_obj)
        for time_key, time_value in time_info.items():
            if time_key not in list(self.time_obj.keys()):
                logger_stream.warning(logger_arrow.warning + 'Time variable "' + time_key +
                                      '" is not defined. Collector value "' + str(time_value) + '" will be used')
                time_obj[time_key] = time_value
            else:
                new_value, old_value = time_value, self.time_obj[time_key]
                if new_value != old_value:
                    logger_stream.info(logger_arrow.info() + 'Time variable "' + time_key +
                                       '" is defined. Collector value "' + str(new_value) +
                                       '" will be used and override settings value "' + str(old_value) + '"')
                if old_value is None:
                    logger_stream.info(logger_arrow.info() + 'Time variable "' + time_key +
                                        '" is defined. Collector value "' + str(new_value) +
                                        '" will be used and override settings value defined by NoneType object')
                time_obj[time_key] = new_value

        return time_obj

    # ------------------------------------------------------------------------------------------------------------------


    # ------------------------------------------------------------------------------------------------------------------
    # check time variables (if needed)
    def __check_variables(self, time_info: dict = None) -> dict:
        time_obj = deepcopy(self.time_obj)
        for time_key, time_value in time_info.items():
            if time_key not in list(self.time_obj.keys()):
                logger_stream.warning(logger_arrow.warning + 'Time variable "' + time_key +
                                      '" is not defined. Default value "' + str(time_value) + '" will be used')
                time_obj[time_key] = time_info[time_key]

        return time_obj
    # ------------------------------------------------------------------------------------------------------------------


    # ------------------------------------------------------------------------------------------------------------------
    # method to view settings variable(s)
    def view_variable_time(self, data: dict = None, mode: bool = True) -> None:
        # info algorithm (start)
        logger_stream.info(logger_arrow.info(tag='info_method') + 'View "time_variables" ... ')
        if mode:
            self.time_handler.view(data)
        # info algorithm (start)
        logger_stream.info(logger_arrow.info(tag='info_method') + 'View "time_variables" ... DONE')
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # configure time variable(s)
    def configure_variable_time(self, time_run_cmd: (str, pd.Timestamp) = None) -> dict:

        # info algorithm (start)
        logger_stream.info(logger_arrow.info(tag='info_method') + 'Configure "time_variables" ... ')

        # define time handler class
        if time_run_cmd is not None or self.time_run_file is not None:
            self.time_handler = handler_time.TimeHandler.from_time_run(
                time_run_cmd=time_run_cmd, time_run_file=self.time_run_file,
                time_period=self.time_period, time_frequency=self.time_frequency, time_rounding=self.time_rounding)
        elif (self.time_start is not None) or (self.time_end is not None):
            self.time_handler = handler_time.TimeHandler.from_time_period(
                time_start=time_run_cmd, time_end=self.time_run_file,
                time_frequency=self.time_frequency, time_rounding=self.time_rounding)
        else:
            logger_stream.error(logger_arrow.error + 'Time run is not correctly defined')
            raise RuntimeError('Time run must be defined')

        # compute time restart
        self.time_handler.compute_time_restart(defined_by_user=None)

        # compute time range
        self.time_handler.compute_time_range()

        # freeze time object
        time_obj = self.time_handler.freeze()

        # # info algorithm (end)
        logger_stream.info(logger_arrow.info(tag='info_method') + 'Configure "time_variables" ... DONE')

        return time_obj

    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # method to organize time variables
    def organize_variable_time(self, time_obj: dict,
                               collector_obj: dict = None, collector_overwrite: bool = True) -> dict:

        # info algorithm (start)
        logger_stream.info(logger_arrow.info(tag='info_method') + 'Organize "time_variables" ... ')

        if collector_obj is None:
            collector_obj = {}

        common_obj = {}
        for time_key in list(self.time_default.keys()):
            if time_key in list(time_obj.keys()):
                common_obj[time_key] = time_obj[time_key]
            else:
                common_obj[time_key] = self.time_default[time_key]
            if time_key in list(collector_obj.keys()):
                collector_obj.pop(time_key, None)

        # merge collector and common object(s)
        common_obj = {**collector_obj, **common_obj}

        # collect singleton data object
        collector_data.collect(common_obj, overwrite=collector_overwrite)

        # info algorithm (start)
        logger_stream.info(logger_arrow.info(tag='info_method') + 'Organize "time_variables" ... DONE')

        return common_obj
    # ------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
