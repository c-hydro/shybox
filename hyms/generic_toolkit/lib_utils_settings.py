"""
Library Features:

Name:          lib_info_settings
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20241202'
Version:       '1.0.0'
"""
# ----------------------------------------------------------------------------------------------------------------------
# libraries
import os
import json
import logging

from apps.generic_toolkit.lib_default_args import logger_name, logger_arrow, collector_data

# logging
logger_stream = logging.getLogger(logger_name)
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to get data settings
def get_data_settings(file_name: str, key_reference: str = 'default',
                      delete_keys: list = None):

    if delete_keys is None:
        delete_keys = []

    if os.path.exists(file_name):
        with open(file_name) as file_handle:
            data_settings = json.load(file_handle)
    else:
        logger_stream.error(logger_arrow.error + ' Error in reading settings file "' + file_name + '"')
        raise IOError('File not found')

    if key_reference in list(data_settings.keys()):
        data_reference = data_settings[key_reference]
        data_settings.pop(key_reference)
        data_settings = {**data_settings, **data_reference}

    comment_keys = ['comment', 'comments', '__comments__', '__comment__', '_comment_']
    delete_keys.extend(comment_keys)

    if delete_keys is not None:
        for delete_key in delete_keys:
            if delete_key in list(data_settings.keys()):
                data_settings.pop(delete_key)

    return data_settings
# ----------------------------------------------------------------------------------------------------------------------
