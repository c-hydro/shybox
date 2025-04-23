"""
Class Features

Name:          handler_hmc_time
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20241212'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import pandas as pd
from tabulate import tabulate

from shybox.generic_toolkit.lib_utils_dict import flat_dict_key
from shybox.generic_toolkit.lib_utils_time import (select_time_run, select_time_range, select_time_restart,
                                                   convert_time_frequency, convert_time_format)

from shybox.generic_toolkit.lib_default_args import logger_name, logger_arrow
from shybox.generic_toolkit.lib_default_args import collector_data

# logging
logger_stream = logging.getLogger(logger_name)
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# class time handler
class TimeHandler:

    class_type = 'time_handler'

    # initialize class
    def __init__(self, time_run: (str, pd.Timestamp) = None, time_restart: (str, pd.Timestamp) = None,
                 time_rounding: str = 'h', time_period: (int, None) = 1, time_shift: (int, None) = 1,
                 time_frequency: str = 'h', **kwargs) -> None:

        self.time_run = convert_time_format(time_run)
        self.time_period = time_period
        self.time_frequency = time_frequency
        self.time_rounding = time_rounding
        self.time_shift = time_shift
        self.time_restart = time_restart

        self.time_start, self.time_end = None, None

        self.time_reference, self.time_other = 'environment', 'user'

    @classmethod
    def from_time_run(cls, time_run_cmd: (str, pd.Timestamp), time_run_file: (str, pd.Timestamp) = None,
                      time_restart: (str, pd.Timestamp) = None,
                      time_period: (int, None) = 1, time_frequency: str = 'h', time_rounding: str = 'h'):

        time_run_cmd, time_run_file = convert_time_format(time_run_cmd), convert_time_format(time_run_file)
        time_run_select = select_time_run(time_run_cmd, time_run_file, time_rounding=time_rounding)

        time_restart = convert_time_format(time_restart)
        time_frequency = convert_time_frequency(time_frequency, frequency_conversion='int_to_str')

        return cls(time_run=time_run_select, time_restart=None,
                   time_rounding=time_rounding, time_period=time_period,
                   time_frequency=time_frequency)

    @classmethod
    def from_time_period(cls, time_start: (str, pd.Timestamp), time_end: (str, pd.Timestamp),
                         time_rounding: str = 'h', time_frequency: str = 'h'):

        time_start, time_end = convert_time_format(time_start), convert_time_format(time_end)

        time_start, time_end = time_start.round(time_rounding), time_end.round(time_rounding)
        time_range = select_time_range(time_start=time_start, time_end=time_end, time_frequency=time_frequency)
        time_frequency = convert_time_frequency(time_frequency, frequency_conversion='int_to_str')

        time_run, time_period = time_range[0], time_range.size

        return cls(time_run=time_run, time_restart=None,
                   time_rounding=time_rounding, time_period=time_period, time_frequency=time_frequency)

    # method to compute time restart
    def compute_time_restart(self, defined_by_user=None):

        if defined_by_user is None:
            self.time_restart = select_time_restart(
                time_run=self.time_run, time_frequency=self.time_frequency, time_shift=self.time_shift)
        else:
            time_restart = convert_time_format(defined_by_user)
            self.time_restart = time_restart

        return self.time_restart

    # method to compute time range
    def compute_time_range(self):

        time_run = self.time_run
        if self.time_period is None or self.time_period == 0:
            time_range = pd.DatetimeIndex([time_run])
        else:
            time_range = pd.date_range(start=time_run, periods=self.time_period, freq=self.time_frequency)
        self.time_start, self.time_end = time_range[0], time_range[-1]

        return self.time_start, self.time_end

    def select_time_priority(self, priority_obj: dict = None) -> (str, str):

        if priority_obj is not None:
            if 'reference' in list(priority_obj.keys()):
                self.time_reference = priority_obj['reference']
            else:
                logger_stream.warning(logger_arrow.warning + 'Reference tag is not defined. Use the default priority.')
            if 'other' in list(priority_obj.keys()):
                self.time_other = priority_obj['other']
            else:
                logger_stream.warning(logger_arrow.warning + 'Other tag is not defined. Use the default priority.')
        else:
            logger_stream.error(logger_arrow.error + 'Priority object not defined.')

        return self.time_reference, self.time_other

    # method to freeze data
    def freeze(self):

        self.time_frequency = convert_time_frequency(self.time_frequency)
        self.time_run = convert_time_format(self.time_run, time_conversion="stamp_to_str")
        self.time_restart = convert_time_format(self.time_restart, time_conversion="stamp_to_str")
        self.time_rounding = self.time_rounding.lower()

        if self.time_start is not None:
            self.time_start = convert_time_format(self.time_start, time_conversion="stamp_to_str")
        if self.time_end is not None:
            self.time_end = convert_time_format(self.time_end, time_conversion="stamp_to_str")

        return self.__dict__

    # method to error data
    def error(self):
        """
        Error time data.
        """
        raise NotImplementedError

    # method to write data
    def write(self):
        """
        Write the time data.
        """

        raise NotImplementedError

    # method to view data
    def view(self, table_data: dict = None,
             table_variable='variables', table_values='values', table_format='psql') -> None:
        """
        View the time data.
        """

        if table_data is None:
            table_data = self.__dict__

        table_dict = flat_dict_key(
            data=table_data, separator_key=":", separator_value=",", obj_dict={})
        table_dframe = pd.DataFrame.from_dict(table_dict, orient='index', columns=['value'])

        table_obj = tabulate(
            table_dframe,
            headers=[table_variable, table_values],
            floatfmt=".5f",
            showindex=True,
            tablefmt=table_format,
            missingval='N/A'
        )

        print(table_obj)

    # method to check data
    def check(self):
        """
        Check if time data is available.
        """
        raise NotImplementedError
# ----------------------------------------------------------------------------------------------------------------------
