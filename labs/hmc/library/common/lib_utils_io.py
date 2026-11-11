"""
Library Features:

Name:          lib_utils_io
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20210113'
Version:       '1.0.0'
"""
#################################################################################
# Library
import logging
import os
import gzip
import json
import pickle

import numpy as np

from copy import deepcopy
from functools import reduce
from operator import getitem

from library.common.lib_info_args import logger_name

# Logging
log_stream = logging.getLogger(logger_name)
#################################################################################


# --------------------------------------------------------------------------------
# Method to update nested item
def update_nested_item(dict_data,
                       string_search='$HOME', string_replace='/home/fabio',
                       dict_keys_list=[], dict_data_fields=None):

    for dict_keys, dict_values in dict_data.items():

        dict_keys_list_step = deepcopy(dict_keys_list)

        if isinstance(dict_values, dict):
            dict_keys_list_step.append(dict_keys)
            dict_data_fields = update_nested_item(
                dict_values,
                string_search=string_search, string_replace=string_replace,
                dict_keys_list=dict_keys_list_step, dict_data_fields=dict_data_fields)
        elif isinstance(dict_values, str):
            if string_search in dict_values:
                dict_keys_list_step.append(dict_keys)
                dict_values_tmp = dict_values.replace(string_search, string_replace)

                dict_data_tmp = {dict_keys: dict_values_tmp}

                if dict_data_fields is None:
                    id_dict = 0
                    dict_data_fields = {id_dict: {}}
                else:
                    id_dict = np.max(np.array(list(dict_data_fields.keys())))
                    id_dict = id_dict + 1
                    dict_data_fields[id_dict] = {}

                dict_data_fields[id_dict]['map_keys'] = dict_keys_list_step
                dict_data_fields[id_dict]['map_field'] = dict_data_tmp

        elif isinstance(dict_values, list):

            dict_values_tmp = deepcopy(dict_values)

            update_flag = False
            for id_value, dict_value in enumerate(dict_values):
                if isinstance(dict_value, str):
                    if string_search in dict_value:
                        dict_keys_list_step.append(dict_keys)
                        dict_value_tmp = dict_value.replace(string_search, string_replace)

                        dict_values_tmp[id_value] = dict_value_tmp
                        update_flag = True

            if update_flag:
                dict_data_tmp = {dict_keys: dict_values_tmp}
                if dict_data_fields is None:
                    id_dict = 0
                    dict_data_fields = {id_dict: {}}
                else:
                    id_dict = np.max(np.array(list(dict_data_fields.keys())))
                    id_dict = id_dict + 1
                    dict_data_fields[id_dict] = {}

                dict_data_fields[id_dict]['map_keys'] = dict_keys_list_step
                dict_data_fields[id_dict]['map_field'] = dict_data_tmp

    return dict_data_fields
# --------------------------------------------------------------------------------


# --------------------------------------------------------------------------------
# Method to update nested item
def set_nested_item(dict_data, map_keys_list, map_value):
    """Set item in nested dictionary"""
    reduce(getitem, map_keys_list[:-1], dict_data)[map_keys_list[-1]] = map_value
    return dict_data
# --------------------------------------------------------------------------------


# --------------------------------------------------------------------------------
# Method to updatea json file
def update_json_file(file_name_in, file_name_out, update_dict=None):

    if update_dict is not None and isinstance(update_dict, dict):

        with open(file_name_in, 'r+') as json_file_in:
            json_data_in = json.load(json_file_in)

        json_data_out = deepcopy(json_data_in)
        for string_search_step, string_replace_step in update_dict.items():

            dict_data_fields = update_nested_item(
                json_data_out, string_search=string_search_step, string_replace=string_replace_step)

            if dict_data_fields is not None:
                for dict_id, dict_links in dict_data_fields.items():
                    map_keys = dict_links['map_keys']
                    map_value = list(dict_links['map_field'].values())[0]

                    json_data_out = set_nested_item(json_data_out, map_keys, map_value)

        json_coded_out = json.dumps(json_data_out, sort_keys=False, indent=4, separators=(',', ': '))
        with open(file_name_out, 'w+') as json_file_out:
            json_file_out.write(json_coded_out)

# --------------------------------------------------------------------------------


# --------------------------------------------------------------------------------
# Method to unzip file
def unzip_filename(file_name_zip, file_name_unzip):

    file_handle_zip = gzip.GzipFile(file_name_zip, "rb")
    file_handle_unzip = open(file_name_unzip, "wb")

    file_data_unzip = file_handle_zip.read()
    file_handle_unzip.write(file_data_unzip)

    file_handle_zip.close()
    file_handle_unzip.close()

# --------------------------------------------------------------------------------


# --------------------------------------------------------------------------------
# Method to zip file
def zip_filename(file_name_unzip, file_name_zip):

    file_handle_unzip = open(file_name_unzip, 'rb')
    file_handle_zip = gzip.open(file_name_zip, 'wb')

    file_handle_zip.writelines(file_handle_unzip)

    file_handle_zip.close()
    file_handle_unzip.close()
# --------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to read data obj
def read_obj(file_name):
    if os.path.exists(file_name):
        data = pickle.load(open(file_name, "rb"))
    else:
        data = None
    return data
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to write data obj
def write_obj(file_name, data):
    if os.path.exists(file_name):
        os.remove(file_name)
    with open(file_name, 'wb') as handle:
        pickle.dump(data, handle, protocol=pickle.HIGHEST_PROTOCOL)
# -------------------------------------------------------------------------------------
