"""
Library Features:

Name:          lib_data_io_json
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20210113'
Version:       '1.0.0'
"""
#######################################################################################
# Library
import logging
import os
import json

import pandas as pd

from library.common.lib_info_args import logger_name

# Logging
log_stream = logging.getLogger(logger_name)
#######################################################################################


# -------------------------------------------------------------------------------------
# Method to read file hydrograph
def read_file_ts_hydrograph(file_name, file_sep=',',
                            tag_time_in='time_period',
                            tag_discharge_obs_in='time_series_discharge_observed',
                            tag_discharge_sim_in='time_series_discharge_simulated',
                            tag_time_out='time',
                            tag_discharge_obs_out='discharge_observed',
                            tag_discharge_sim_out='discharge_simulated',
                            tag_index='time'):

    if os.path.exists(file_name):
        with open(file_name) as file_handle:
            file_data = json.load(file_handle)
    else:
        print(' ERROR ===> Error in reading hydrograph file "' + file_name + '"')
        raise IOError('File not found')

    variable_list_in = [tag_time_in, tag_discharge_obs_in, tag_discharge_sim_in]
    variable_list_out = [tag_time_out, tag_discharge_obs_out, tag_discharge_sim_out]

    file_data_attrs = {}
    file_data_dict = {}
    for file_key, file_value in file_data.items():
        if file_key in variable_list_in:
            var_idx = variable_list_in.index(file_key)
            var_name = variable_list_out[var_idx]
            file_data_dict[var_name] = file_value
        else:
            file_data_attrs[file_key] = file_value

    for file_key, file_value_tmp in file_data_dict.items():
        file_list_tmp = file_value_tmp.split(file_sep)
        if file_key == tag_time_out:
            file_list_converted = pd.DatetimeIndex(file_list_tmp)
        else:
            file_list_converted = list(map(float, file_list_tmp))
        file_data_dict[file_key] = file_list_converted

    file_data_df = pd.DataFrame(data=file_data_dict)
    if tag_index in list(file_data_df.columns):
        file_data_df.set_index(tag_index, inplace=True)

    return file_data_df, file_data_attrs
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to read file settings
def read_file_settings(file_name):
    if os.path.exists(file_name):
        with open(file_name) as file_handle:
            file_data = json.load(file_handle)
    else:
        print(' ERROR ===> Error in reading settings file ' + file_name)
        raise IOError('File not found')
    return file_data
# -------------------------------------------------------------------------------------
