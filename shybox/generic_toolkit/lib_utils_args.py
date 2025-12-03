"""
Library Features:

Name:          lib_utils_args
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20241218'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import os
import argparse

from shybox.default.lib_default_args import logger_name as logger_name_package
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to get script argument(s)
def get_args(settings_folder: str = None):

    # parser algorithm arg(s)
    parser_obj = argparse.ArgumentParser()
    parser_obj.add_argument('-settings_file', action="store", dest="settings_file")
    parser_obj.add_argument('-time', action="store", dest="settings_time")
    parser_value = parser_obj.parse_args()

    # set algorithm arg(s)
    settings_file, settings_time = 'configuration.json', None
    if parser_value.settings_file:
        settings_file = parser_value.settings_file
    if parser_value.settings_time:
        settings_time = parser_value.settings_time

    # if settings file is not absolute, set it as absolute using the current working directory
    if not os.path.isabs(settings_file):
        if settings_folder is None:
            settings_folder = os.path.dirname(os.path.realpath(__file__))
        settings_file = os.path.join(settings_folder, settings_file)

    return settings_file, settings_time

# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to get logger name
def get_logger_name(logger_name_mode='by_package', logger_name_default='script_logger') -> str:

    if logger_name_mode == 'by_env':
        logger_name = os.environ.get('LOGGER_NAME', logger_name_default)
    elif logger_name_mode == 'by_arg':
        logger_name = logger_name_default
    elif logger_name_mode == 'by_script':
        logger_name = os.path.basename(__file__)
    elif logger_name_mode == 'by_package':
        logger_name = logger_name_package
    else:
        logger_name = logger_name_default
    return logger_name
# ----------------------------------------------------------------------------------------------------------------------

