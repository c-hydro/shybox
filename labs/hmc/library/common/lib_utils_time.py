"""
Library Features:

Name:          lib_utils_time
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20210113'
Version:       '1.0.0'
"""
#######################################################################################
# Library
import logging
import pandas as pd

from library.common.lib_info_args import logger_name

# Logging
log_stream = logging.getLogger(logger_name)
#######################################################################################


# -------------------------------------------------------------------------------------
# Method to parser time from path list
def get_folders_time(folder_list, time_format_in='%Y%m%d_%H', time_format_out='%Y-%m-%d %H:00'):

    if isinstance(folder_list, str):
        folder_list = [folder_list]

    time_list = []
    for folder_step in folder_list:
        time_stamp_step = pd.to_datetime(folder_step, format=time_format_in)
        time_str_step = time_stamp_step.strftime(format=time_format_out)
        time_list.append(time_str_step)
    return time_list
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to validate time step
def validate_time_step(time_step, time_range):
    time_step = pd.Timestamp(time_step)
    if time_step in time_range:
        time_valid = True
    else:
        time_valid = False
    return time_valid
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to create data time range
def create_time_range(time_analysis=None, time_start=None, time_end=None,
                      time_analysis_left_period=1, time_analysis_left_freq='H',
                      time_analysis_right_period=0, time_analysis_right_freq='H',
                      time_format='%Y-%m-%d %H:00', time_reverse=True):

    if time_analysis and (time_start is None and time_end is None):
        if time_analysis_left_period is None or time_analysis_right_period is None:
            print(' ERROR ===> Time information are not enough')
            raise IOError('Time analysis must be considered with time_analysis_period')

    if time_analysis and (time_start is None and time_end is None):
        time_analysis = pd.Timestamp(time_analysis)
        time_analysis_right = time_analysis
        time_analysis_left = time_analysis + pd.Timedelta(1, unit=time_analysis_left_freq)

        time_analysis_left_range = pd.date_range(end=time_analysis_left,
                                                 periods=time_analysis_left_period, freq=time_analysis_left_freq)
        time_analysis_right_range = pd.date_range(start=time_analysis_right,
                                                  periods=time_analysis_right_period, freq=time_analysis_right_freq)

        time_range_stamp = time_analysis_left_range.union(time_analysis_right_range)

    elif (time_analysis is None) and (time_start and time_end):

        time_start, time_end = pd.Timestamp(time_start), pd.Timestamp(time_end)
        time_range_stamp = pd.date_range(start=time_start, end=time_end,
                                         periods=time_analysis_left_period, freq=time_analysis_right_freq)

    elif time_analysis and (time_start and time_end):
        time_start, time_end = pd.Timestamp(time_start), pd.Timestamp(time_end)
        time_range_stamp = pd.date_range(start=time_start, end=time_end,
                                         periods=time_analysis_left_period, freq=time_analysis_right_freq)
    else:
        print(' ERROR ===> Time definitions are not enough or are not consistent to define the time_range')
        raise NotImplementedError('Case not implemented yet')

    if time_reverse:
        time_range_stamp = time_range_stamp[::-1]

    time_range_str = []
    for time_step in time_range_stamp:
        time_str = time_step.strftime(format=time_format)
        time_range_str.append(time_str)

    print(' INFO --> Time period is defined from "' + time_range_str[0] + '" to "' + time_range_str[-1] + '"')

    return time_range_stamp, time_range_str

# -------------------------------------------------------------------------------------
