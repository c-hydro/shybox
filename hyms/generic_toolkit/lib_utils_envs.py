"""
Library Features:

Name:          lib_utils_envs
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20241209'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import os

import pandas as pd

from hyms.generic_toolkit.lib_default_args import logger_name, logger_arrow, time_format_datasets

# logging
logger_stream = logging.getLogger(logger_name)
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to get env variable(s)
def get_variable_envs(obj_variable_lut: dict) -> (dict, dict):

    env_system_obj = dict(os.environ)

    env_system_select, env_system_not_select = {}, {}
    for key_user, value_user in obj_variable_lut.items():
        if key_user in list(env_system_obj.keys()):
            env_system_select[value_user] = os.environ[key_user]
        else:
            env_system_not_select[value_user] = key_user

    return env_system_select, env_system_not_select
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to set env variable(s()
def set_variable_envs(obj_variable_data: dict, obj_variable_lut: dict, no_data: str = '') -> bool:

    # get environment variables
    obj_variable_env = dict(os.environ)

    # iterate over environment data
    for loc_key, loc_value in obj_variable_data.items():

        # check local key
        if loc_key in list(obj_variable_lut.keys()):

            # get environment key
            env_key = obj_variable_lut[loc_key]

            # check if loc_value is None
            if loc_value is None:
                loc_value = no_data
            # check if loc_value is a timestamp
            if isinstance(loc_value, pd.Timestamp):
                loc_value = loc_value.strftime(time_format_datasets)

            # check if environment key is in the lut
            if env_key not in list(obj_variable_env.keys()):
                os.environ[env_key] = loc_value
            else:
                os.environ[env_key] = loc_value
                logger_stream.warning(logger_arrow.warning +
                                      'Environment variable " ' + loc_key + '" set with value ' + str(loc_value) +
                                      ' ... DONE. VARIABLE "' + str(env_key) + '" IS UPDATED WITH NEW VALUE')

        else:
            logger_stream.warning(logger_arrow.warning +
                                  'Environment variable "' + loc_key + '" with value "' + str(loc_value) +
                                  '" ... SKIPPED. VARIABLE IS NOT IN THE EXPECTED LIST')

    return True
# ----------------------------------------------------------------------------------------------------------------------
