"""
Library Features:

Name:          lib_utils_test
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20241209'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging

from shybox.generic_toolkit.lib_utils_file import expand_file_path
from shybox.default.lib_default_args import logger_name, logger_arrow

# logging
logger_stream = logging.getLogger(logger_name)
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to configure home
def configure_home(variable_obj: dict) -> dict:
    for var_key, var_value in variable_obj.items():
        if isinstance(var_value, str):
            variable_obj[var_key] = expand_file_path(var_value)
    return variable_obj
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to extract object by list
def extract_obj_by_list(common_obj: dict, obj_list: list) -> dict:
    variable_obj = {}
    for obj_key in obj_list:
        if obj_key in common_obj:
            variable_obj[obj_key] = common_obj[obj_key]
    return variable_obj
# ----------------------------------------------------------------------------------------------------------------------
