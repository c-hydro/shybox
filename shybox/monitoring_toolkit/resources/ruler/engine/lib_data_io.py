"""
Library Features:

Name:          lib_data_io
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20210408'
Version:       '1.0.0'
"""
#######################################################################################
# Library
import logging
import os
import csv
import pickle
import json

import pandas as pd

from .lib_info_args import logger_name

# Logging
log_stream = logging.getLogger(logger_name)
#######################################################################################


# -------------------------------------------------------------------------------------
# Method to read file json
def read_json(file_name):
    if os.path.exists(file_name):
        with open(file_name) as file_handle:
            file_data = json.load(file_handle)
    else:
        log_stream.error(' ===> Error in reading file ' + file_name)
        raise IOError('File not found')
    return file_data
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to write csv file
def write_csv(file_name, file_dframe, file_separator=';'):
    if file_dframe is not None:
        file_dframe.to_csv(file_name, sep=file_separator)
    else:
        log_stream.warning(' ===> DataFrame is defined by NoneTye. The  "' + file_name + '" will be not saved')
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to read csv file
def read_csv(file_name, file_separator=';', index_name='time'):
    if os.path.exists(file_name):
        file_dframe = pd.read_csv(file_name, sep=file_separator)

        if index_name in list(file_dframe.columns):

            index_values = file_dframe[index_name].values
            file_dframe = file_dframe.reset_index()
            file_dframe.index = index_values
            file_dframe.index.name = index_name

            if index_name in list(file_dframe.columns):
                file_dframe = file_dframe.drop(columns=[index_name])
            if 'index' in list(file_dframe.columns):
                file_dframe = file_dframe.drop(columns=['index'])

        else:
            log_stream.warning(' ===> Expected file index "' +
                               index_name + '" was not found in the dataframe. Errors could happen in the algorith')

    else:
        log_stream.warning(' ===> File "' + file_name + '" not found. Data will be initialized by NoneType')
        file_dframe = None
    return file_dframe
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to read file pickle
def read_obj(file_name):
    if os.path.exists(file_name):
        with open(file_name, "rb") as stream:
            data = pickle.load(stream)
    else:
        log_stream.warning(' ===> File "' + file_name + '" not found. Data will be initialized by NoneType')
        data = None
    return data
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to write file pickle
def write_obj(file_name, data):
    if os.path.exists(file_name):
        os.remove(file_name)
    with open(file_name, 'wb') as handle:
        pickle.dump(data, handle, protocol=pickle.HIGHEST_PROTOCOL)
# -------------------------------------------------------------------------------------
