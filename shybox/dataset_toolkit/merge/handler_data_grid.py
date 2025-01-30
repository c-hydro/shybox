"""
Class Features

Name:          handler_data_grid
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20250124'
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
# class data handler
class DataHandler:

    class_type = 'data_handler'

    # initialize class
    def __init__(self, file_name: str, file_time: (str, pd.Timestamp) = None,
                 file_format: str = None,  **kwargs) -> None:

        self.file_name = file_name
        self.file_time = file_time
        self.file_format = file_format

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


    def read(self):
        """
        Read the data.
        """
        raise NotImplementedError

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

    # method to get data
    def get_data(self):
        """
        Error time data.
        """
        raise NotImplementedError

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

        table_dict = flat_dict_key(data=table_data, separator=":", obj_dict={})
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
