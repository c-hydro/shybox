"""
Library Features:

Name:          lib_utils_time
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20241202'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import pandas as pd

from datetime import date

from shybox.generic_toolkit.lib_default_args import (logger_name, logger_arrow,
                                                     time_format_datasets as format_dset,
                                                     time_format_algorithm as format_alg)

# set logger
logger_stream = logging.getLogger(logger_name)
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# convert time format (from string to timestamp and vice versa)
def convert_time_format(time_in: (str, pd.Timestamp), time_conversion: str = 'str_to_stamp') -> (pd.Timestamp, str):

    if time_in is not None:
        if time_conversion == 'str_to_stamp':
            if isinstance(time_in, str):
                time_out = pd.Timestamp(time_in)
            elif isinstance(time_in, pd.Timestamp):
                time_out = time_in
            else:
                raise ValueError('Time input type is not recognized')
        elif time_conversion == 'stamp_to_str':
            if isinstance(time_in, pd.Timestamp):
                time_out = time_in.strftime(format_dset)
            elif isinstance(time_in, str):
                time_tmp = time_in
                time_tmp = pd.Timestamp(time_tmp)
                time_out = time_tmp.strftime(format_dset)
            else:
                raise ValueError('Time input type is not recognized')
        elif time_conversion == 'str_to_str':
            if isinstance(time_in, str):
                time_tmp = time_in
                time_tmp = pd.Timestamp(time_tmp)
                time_out = time_tmp.strftime(format_dset)
            elif isinstance(time_in, pd.Timestamp):
                time_out = time_in.strftime(format_dset)
            else:
                raise ValueError('Time input type is not recognized')
        else:
            raise ValueError('Time conversion not recognized')
    else:
        time_out = None

    return time_out
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to convert time frequency from string to seconds
def convert_time_frequency(time_frequency: (str, int), frequency_conversion: str = 'str_to_int') -> (int, str):
    if frequency_conversion == 'str_to_int':
        if isinstance(time_frequency, str):
            time_delta = pd.Timedelta(1, unit=time_frequency.lower())
            time_frequency = int(time_delta.total_seconds())
        elif isinstance(time_frequency, (int, float)):
            time_frequency = int(time_frequency)
        else:
            logger_stream.error(logger_arrow.error + 'Time frequency type is not expected')
            raise NotImplementedError('Only string or integer/float type are allowed')
    elif frequency_conversion == 'int_to_str':
        if isinstance(time_frequency, (int, float)):
            obj_timedelta = pd.Timedelta(time_frequency, unit='s')
            time_frequency = obj_timedelta.resolution_string
        elif isinstance(time_frequency, str):
            if time_frequency.isdigit():
                obj_timedelta = pd.Timedelta(int(time_frequency), unit='s')
                time_frequency = obj_timedelta.resolution_string
            elif time_frequency.isalpha():
                time_frequency = time_frequency.lower()
            else:
                logger_stream.error(logger_arrow.error + 'Time frequency type is not expected')
                raise NotImplementedError('Only string or integer/float type are allowed')
        else:
            logger_stream.error(logger_arrow.error + 'Time frequency type is not expected')
            raise NotImplementedError('Only string or integer/float type are allowed')
    else:
        logger_stream.error(logger_arrow.error + 'Time frequency conversion not recognized')
        raise NotImplementedError('Time frequency conversion not recognized')
    return time_frequency
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to check string has the date format
def is_date(date_obj: (str, pd.Timestamp), date_format: str = '%Y%m%d%H%M') -> bool:
    """
    Return whether the string can be interpreted as a date.
    :param date_obj: str, string to check for date
    :param date_format: str, format of the date
    """
    try:
        if isinstance(date_obj, str):
            pd.to_datetime(date_obj, format=date_format, errors='raise')
        elif isinstance(date_obj, pd.Timestamp):
            pd.to_datetime(date_obj.strftime(date_format), format=date_format, errors='raise')
        else:
            raise ValueError('Date type is not recognized')
        return True
    except ValueError:
        return False
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to select time run
def select_time_run(time_run_args: pd.Timestamp = None, time_run_file: pd.Timestamp = None,
                    time_rounding: str = 'H', **kwargs) -> (pd.Timestamp, None):

    # info algorithm (start)
    logger_stream.info(logger_arrow.info(tag='time_fx_lev1') + 'Select time run ... ')

    # case 1: time information defined by "time_now" argument
    if (time_run_args is not None) or (time_run_file is not None):

        # check time args and time file
        if time_run_args is not None:
            time_run = time_run_args
            logger_stream.info(
                logger_arrow.info(tag='time_fx_lev2') + 'Time "' + time_run.strftime(format_alg) + '" set by argument')
        elif (time_run_args is None) and (time_run_file is not None):
            time_run = time_run_file
            logger_stream.info(
                logger_arrow.info(tag='time_fx_lev2') + 'Time "' + time_run.strftime(format_alg) + '" set by user')
        elif (time_run_args is None) and (time_run_file is None):
            time_run = date.today()
            logger_stream.info(
                logger_arrow.info(tag='time_fx_lev2') + 'Time "' + time_run.strftime(format_alg) + '" set by system')
        else:
            logger_stream.info(logger_arrow.info(tag='time_fx_lev1') + 'Select time run ... FAILED')
            logger_stream.error(logger_arrow.error + 'Argument "time_run" is not correctly set')
            raise IOError('Time type or format is wrong')

        time_df = pd.DataFrame([{'time_now': pd.Timestamp(time_run)}])
        time_df['time_round'] = time_df['time_now'].dt.floor(time_rounding.lower())

        time_run = pd.Timestamp(time_df['time_round'].values[0])

        # info algorithm (end)
        logger_stream.info(logger_arrow.info(tag='time_fx_lev1') + 'Select time run ... DONE')

    else:
        # info algorithm (end)
        time_run = None
        logger_stream.info(logger_arrow.info(tag='time_fx_lev1') +
                           'Select time run ... SKIPPED. Variable is defined by NoneType')

    return time_run
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to select time restart
def select_time_restart(time_run: pd.Timestamp, time_shift: int = 1, time_frequency: str = 'h') -> pd.Timestamp:

    # info algorithm (start)
    logger_stream.info(logger_arrow.info(tag='time_fx_lev1') + 'Select time restart ... ')
    # compute time restart
    time_restart = time_run - pd.Timedelta(time_shift, unit=time_frequency.lower())
    # info algorithm (start)
    logger_stream.info(logger_arrow.info(tag='time_fx_lev1') + 'Select time restart ... DONE')

    return time_restart
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to select time range
def select_time_range(time_start: (str, pd.Timestamp) = None, time_end: (str, pd.Timestamp) = None,
                      time_period: int = 1,
                      time_frequency: str = 'h', time_rounding: str = 'h') -> (pd.date_range, None):

    if isinstance(time_start, str):
        time_start = time_start.strip("\"")
        time_start = pd.Timestamp(time_start)
    if isinstance(time_end, str):
        time_end = time_end.strip("\"")
        time_end = pd.Timestamp(time_end)

    time_rounding, time_frequency = time_rounding.lower(), time_frequency.lower()

    if (time_start is not None) and (time_end is not None):

        time_start, time_end = time_start.floor(time_rounding), time_end.floor(time_rounding)
        time_range = pd.date_range(start=time_start, end=time_end, freq=time_frequency)

    elif (time_start is None) and (time_end is not None):

        if time_period is None:
            raise ValueError('Time period is not defined')

        time_end = time_end.floor(time_rounding.lower())
        time_range = pd.date_range(end=time_end, periods=time_period, freq=time_frequency.lower())
    elif (time_start is not None) and (time_end is None):

        if time_period is None:
            raise ValueError('Time period is not defined')

        time_start = time_start.floor(time_rounding.lower())
        time_range = pd.date_range(start=time_start, periods=time_period, freq=time_frequency.lower())

    else:
        raise ValueError('Time range is not correctly defined')

    return time_range
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to select time format
def select_time_format(time_range: (pd.Timestamp, pd.DatetimeIndex), time_format: str ='%Y-%m-%d %H:%M'):

    if isinstance(time_range, pd.Timestamp):
        time_range = pd.DatetimeIndex([time_range])
    time_range_formatted = time_range.strftime(date_format=time_format)

    if len(time_range_formatted) == 1:
        time_range_formatted = time_range_formatted[0]
    return time_range_formatted
# ----------------------------------------------------------------------------------------------------------------------