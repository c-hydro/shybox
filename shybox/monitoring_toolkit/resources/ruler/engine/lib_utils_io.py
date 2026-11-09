"""
Library Features:

Name:          lib_utils_io
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20211208'
Version:       '1.0.0'
"""

#######################################################################################
# Libraries
import logging
import pandas as pd

from copy import deepcopy

from .lib_analysis_utils import convert_human_unit, convert_string2num_value
from .lib_info_args import logger_name

# Logging
log_stream = logging.getLogger(logger_name)
#######################################################################################


# -------------------------------------------------------------------------------------
# Method to filter dataframe by column prefix
def filter_dframe_by_column(dframe_src, prefix_name=None, **kwargs):

    if dframe_src is not None:
        if prefix_name is not None:

            if not isinstance(prefix_name, list):
                prefix_name = [prefix_name]

            dframe_dst = None
            for prefix_step in prefix_name:
                dframe_step = dframe_src.loc[:, dframe_src.columns.str.startswith(prefix_step)]
                if dframe_dst is None:
                    dframe_dst = deepcopy(dframe_step)
                else:
                    dframe_dst = pd.concat([dframe_dst, dframe_step], axis=1)

        else:
            dframe_dst = deepcopy(dframe_src)

        # check dataframe empty or not
        if dframe_dst.empty:
            log_stream.warning(' ===> Destination filtered dataframe is defined by NoneType')
            dframe_dst = None
    else:
        log_stream.warning(' ===> Source and destination dataframes are defined by NoneType')
        dframe_dst = None
    return dframe_dst
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to convert string to float
def convert_dframe_string2num(dframe_src):
    column_list = list(dframe_src.columns)
    dframe_dst = pd.DataFrame()
    for column_name in column_list:
        dframe_dst[column_name] = dframe_src.apply(
            lambda x: convert_string2num_value(x[column_name]), axis=1)
    return dframe_dst
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to convert dframe human to bytes
def convert_dframe_units(dframe_src, dframe_units='G', dframe_format='{:0.3}', prefix_name=None, **kwargs):

    if isinstance(prefix_name, str):
        prefix_name = [prefix_name]
    elif isinstance(prefix_name, list):
        pass
    else:
        log_stream.error(' ===> Arguments "prefix_name" must be string or list')
        raise NotImplemented('Case not implemented yet')

    dframe_dst = None
    for prefix_step in prefix_name:
        column_list = list(dframe_src.columns[dframe_src.columns.str.startswith(prefix_step)])

        if column_list:
            for column_name in column_list:

                if dframe_dst is None:
                    dframe_dst = pd.DataFrame()

                # check column type
                dframe_value = dframe_src[column_name].iloc[0]

                if isinstance(dframe_value, str) and 'percent' not in column_name:

                    dframe_first, dframe_last = dframe_value[0], dframe_value[-1]
                    if dframe_first.isnumeric() and dframe_last.isalpha():
                        dframe_dst[column_name] = dframe_src.apply(
                            lambda x: convert_human_unit(
                                x[column_name], size_unit=dframe_units, size_format=dframe_format), axis=1)
                    else:
                        dframe_dst[column_name] = dframe_src[column_name]
                elif not isinstance(dframe_value, str) and 'percent' in column_name:
                    dframe_dst[column_name] = dframe_src[column_name]
                elif not isinstance(dframe_value, str) and 'percent' not in column_name:
                    dframe_dst[column_name] = dframe_src[column_name]
                else:
                    log_stream.error(' ===> Change units column for "' + column_name + '" is not supported')
                    raise NotImplemented('Case not implemented yet')

    return dframe_dst
# -------------------------------------------------------------------------------------
