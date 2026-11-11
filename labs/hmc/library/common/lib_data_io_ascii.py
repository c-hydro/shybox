"""
Library Features:

Name:          lib_data_io_ascii
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20210113'
Version:       '1.0.0'
"""
#######################################################################################
# Library
import logging
import os
import re

import xarray as xr
import pandas as pd
import numpy as np

from library.common.lib_utils_system import get_dict_values, fill_tags2string, make_folder
from library.common.lib_info_args import logger_name

# Logging
log_stream = logging.getLogger(logger_name)
#######################################################################################


# -------------------------------------------------------------------------------------
# Method to read ascii hydrograph file
def read_file_ascii_hydrograph(file_name, file_header=None, file_columns=None,
                               file_terminator='\n', file_separator='\t', file_names=None):

    if os.path.exists(file_name):

        if file_names is None:
            file_names = ['data']

        # Read ts
        file_data = pd.read_table(file_name, header=file_header, lineterminator=file_terminator, names=['data'])

        file_time_ws = []
        file_data_ws = []
        for id, ts_obj in enumerate(file_data['data']):

            ts_raw = ts_obj.split(file_separator)
            ts_parts = ts_raw[0].split()

            ts_time = pd.Timestamp(ts_parts[0])
            ts_values = ts_parts[1:]
            ts_values = [float(i) for i in ts_values]

            file_time_ws.append(ts_time)
            file_data_ws.append(ts_values)

        file_time_idx = pd.DatetimeIndex(file_time_ws)
        file_data_n = file_data_ws[0].__len__()

        if file_columns is None:
            file_columns = ["section_{:}".format(idx) for idx in range(1, file_data_n + 1)]

        file_dframe = pd.DataFrame(file_data_ws, columns=file_columns, index=file_time_idx)

        return file_dframe

    else:
        logging.warning(' ===> File ' + file_name + ' not found!')

# -------------------------------------------------------------------------------------
