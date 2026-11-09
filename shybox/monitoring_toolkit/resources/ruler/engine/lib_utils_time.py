"""
Library Features:

Name:          lib_utils_time
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20211208'
Version:       '1.0.0'
"""

#######################################################################################
# Libraries
import logging
import re
import pandas as pd

from datetime import date
from copy import deepcopy

from .lib_info_args import logger_name

# Logging
log_stream = logging.getLogger(logger_name)
#######################################################################################


# -------------------------------------------------------------------------------------
# Method to set time run
def set_time(time_run_args=None, time_run_file=None, time_format='%Y-%m-%d %H:$M',
             time_run_file_start=None, time_run_file_end=None,
             time_period=1, time_frequency='H', time_rounding='H', time_reverse=True):

    log_stream.info(' ----> Set time information ... ')
    if (time_run_file_start is None) and (time_run_file_end is None):

        log_stream.info(' -----> Time info defined by "time_run" argument ... ')

        if time_run_args is not None:
            time_run = time_run_args
            log_stream.info(' ------> Time ' + time_run + ' set by argument')
        elif (time_run_args is None) and (time_run_file is not None):
            time_run = time_run_file
            log_stream.info(' ------> Time ' + time_run + ' set by user')
        elif (time_run_args is None) and (time_run_file is None):
            time_now = date.today()
            time_run = time_now.strftime(time_format)
            log_stream.info(' ------> Time ' + time_run + ' set by system')
        else:
            log_stream.info(' ----> Set time period ... FAILED')
            log_stream.error(' ===> Argument "time_run" is not correctly set')
            raise IOError('Time type or format is wrong')

        time_tmp = pd.Timestamp(time_run)
        time_run = time_tmp.floor(time_rounding)

        if time_period > 0:
            time_range = pd.date_range(end=time_run, periods=time_period, freq=time_frequency)
        else:
            log_stream.warning(' ===> "time_period" must be greater then 0. "time_period" is set automatically to 1')
            time_range = pd.DatetimeIndex([time_run], freq=time_frequency)

        log_stream.info(' -----> Time info defined by "time_run" argument ... DONE')

    elif (time_run_file_start is not None) and (time_run_file_end is not None):

        log_stream.info(' -----> Time info defined by "time_start" and "time_end" arguments ... ')

        time_run_file_start = pd.Timestamp(time_run_file_start)
        time_run_file_start = time_run_file_start.floor(time_rounding)
        time_run_file_end = pd.Timestamp(time_run_file_end)
        time_run_file_end = time_run_file_end.floor(time_rounding)

        time_now = date.today()
        time_run = time_now.strftime(time_format)
        time_run = pd.Timestamp(time_run)
        time_run = time_run.floor(time_rounding)
        time_range = pd.date_range(start=time_run_file_start, end=time_run_file_end, freq=time_frequency)

        log_stream.info(' -----> Time info defined by "time_start" and "time_end" arguments ... DONE')

    else:
        log_stream.info(' ----> Set time information ... FAILED')
        log_stream.error(' ===> Arguments "time_start" and/or "time_end" is/are not correctly set')
        raise IOError('Time type or format is wrong')

    time_chunks = set_chunks(time_range)

    if time_reverse:
        time_range = time_range[::-1]
        time_tmp, time_chunks = deepcopy(time_chunks), {}
        for time_step in reversed(list(time_tmp.keys())):
            time_chunks[time_step] = time_tmp[time_step][::-1]

    log_stream.info(' ----> Set time information ... DONE')

    return time_run, time_range, time_chunks
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to set chunks
def set_chunks(time_range, time_delta='3H'):

    time_steps, time_freq = split_time_delta_parts(time_delta)

    if time_range.__len__() > time_steps:
        time_pvt_delta = deepcopy(time_delta)
        time_pvt_range = pd.date_range(start=time_range[0], end=time_range[-1], freq=time_pvt_delta)
    else:
        time_pvt_range = deepcopy(time_range)
        time_steps = 1
        time_freq = time_range.freqstr

    time_chunks = {}
    for time_pvt_step in reversed(time_pvt_range):
        time_prd_pvt_tmp = pd.date_range(start=time_pvt_step, periods=time_steps, freq=time_freq)
        time_prd_pvt_flt = time_prd_pvt_tmp[(time_prd_pvt_tmp >= time_range[0]) & (time_prd_pvt_tmp <= time_range[-1])]
        time_chunks[time_pvt_step] = time_prd_pvt_flt

    return time_chunks
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to convert time delta to seconds
def convert_time_delta_to_seconds(time_delta):
    seconds_delta = None
    if time_delta is not None:
        seconds_delta = pd.Timedelta(time_delta).total_seconds()
    return seconds_delta
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to fill time delta
def fill_time_delta_parts(string_delta):
    if isinstance(string_delta, str) and string_delta[0].isalpha():
        string_delta = '1' + string_delta
    return string_delta
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to check time delta limits
def check_time_delta_limits(time_delta_src, time_delta_min=None, time_delta_max=None):

    time_delta_dst = None
    if time_delta_src is None:
        if (time_delta_min is not None) and (time_delta_max is None):
            log_stream.warning(' ===> Object "time_delta_src" is defined by NoneType. '
                               'Set "time_delta_min" as reference "time_delta=' + time_delta_min + '"')
            time_delta_dst = deepcopy(time_delta_min)
        elif (time_delta_min is None) and (time_delta_max is not None):
            log_stream.warning(' ===> Object "time_delta_src" is defined by NoneType. '
                               'Set "time_delta_max" as reference "time_delta=' + time_delta_max + '"')
            time_delta_dst = deepcopy(time_delta_max)
        else:
            log_stream.error(' ===> Object "time_delta_min" and "time_delta_max" definitions are not supported '
                             'for time_delta_src defined by NoneType')
    else:

        if (time_delta_min is not None) and (time_delta_max is None):

            seconds_delta_src = pd.Timedelta(fill_time_delta_parts(time_delta_src)).total_seconds()
            seconds_delta_min = pd.Timedelta(fill_time_delta_parts(time_delta_min)).total_seconds()
            if seconds_delta_src < seconds_delta_min:
                time_delta_dst = deepcopy(time_delta_min)
                log_stream.warning(' ===> Object "time_delta_src=' + time_delta_src +
                                   '" is less than "time_delta_min". '
                                   'Set "time_delta_min" as reference "time_delta=' + time_delta_min + '"')
            else:
                time_delta_dst = deepcopy(time_delta_src)

        elif (time_delta_min is None) and (time_delta_max is not None):
            seconds_delta_src = pd.Timedelta(fill_time_delta_parts(time_delta_src)).total_seconds()
            seconds_delta_max = pd.Timedelta(fill_time_delta_parts(time_delta_max)).total_seconds()
            if seconds_delta_src > seconds_delta_max:
                time_delta_dst = deepcopy(time_delta_max)
                log_stream.warning(' ===> Object "time_delta_src=' + time_delta_src +
                                   '" is greater than "time_delta_max". '
                                   'Set "time_delta_max" as reference "time_delta=' + time_delta_max + '"')
            else:
                time_delta_dst = deepcopy(time_delta_src)
        else:
            log_stream.error(' ===> Object "time_delta_min" and "time_delta_max" definitions are not supported '
                             'for time_delta_src not defined by NoneType')
            raise NotImplemented('Case not implemented yet')

    if time_delta_dst is None:
        log_stream.error(' ===> Object "time_delta_dst" defined by NoneType is not supported')
        raise RuntimeError('Time delta object must be defined by a time delta string')

    return time_delta_dst

# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to split time delta
def split_time_delta_parts(time_delta):
    time_period = re.findall(r'\d+', time_delta)
    if time_period.__len__() > 0:
        time_period = int(time_period[0])
    else:
        time_period = 1
    time_frequency = re.findall("[a-zA-Z]+", time_delta)[0]
    return time_period, time_frequency
# -------------------------------------------------------------------------------------
