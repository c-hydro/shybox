#!/usr/bin/python3
"""
HMC-SUITE - TIME APP

__date__ = '20241209'
__version__ = '1.0.0'
__author__ =
    'Fabio Delogu (fabio.delogu@cimafoundation.org),
     Andrea Libertino (andrea.libertino@cimafoundation.org)'
__library__ = 'hmc-suite'

General command line:
python app_time_main.py -settings_file configuration.json -time "YYYY-MM-DD HH:MM"

Version(s):
20241209 (1.0.0) --> Beta release for hmc-suite package
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import os
import time

from hyms.generic_toolkit.lib_utils_args import get_args
from hyms.generic_toolkit.lib_utils_logging import set_logging_stream

from hyms.generic_toolkit.lib_default_args import logger_name, logger_format, logger_arrow
from hyms.generic_toolkit.lib_default_args import collector_data

from hyms.runner_toolkit.settings.driver_app_settings import DrvSettings
from hyms.runner_toolkit.time.driver_app_time import DrvTime

# set logger
logger_stream = logging.getLogger(logger_name)
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# algorithm information
project_name = 'hmc-suite'
alg_name = 'Application for time'
alg_type = 'Package'
alg_version = '1.0.0'
alg_release = '2024-12-09'
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# script main
def main(alg_collectors_settings: dict = None):

    # ------------------------------------------------------------------------------------------------------------------
    # get file settings
    alg_file_settings, alg_time_settings = get_args(settings_folder=os.path.dirname(os.path.realpath(__file__)))

    # method to initialize settings class
    driver_settings = DrvSettings(file_name=alg_file_settings, file_time=alg_time_settings,
                                  file_key='information', settings_collectors=alg_collectors_settings)
    # method to configure variable settings
    alg_variables_settings, alg_variables_collector, alg_variables_system = driver_settings.configure_variable_settings()
    # method to organize variable settings
    alg_variables_settings = driver_settings.organize_variable_settings(
        settings_obj=alg_variables_settings, collector_obj=alg_variables_collector)

    # get variables priority
    alg_variables_priority = driver_settings.get_variable_by_tag('priority')

    # method to view variable settings
    driver_settings.view_variable_settings(data=alg_variables_settings, mode=True)

    # collector data
    collector_data.view()

    # set logging stream
    set_logging_stream(
        logger_name=logger_name, logger_format=logger_format,
        logger_folder=alg_variables_settings['path_log'], logger_file=alg_variables_settings['file_log'])
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # info algorithm (start)
    logger_stream.info(logger_arrow.arrow_main_break)
    logger_stream.info(logger_arrow.main + alg_name + ' (Version: ' + alg_version + ' Release_Date: ' + alg_release + ')')
    logger_stream.info(logger_arrow.main + 'START ... ')
    logger_stream.info(logger_arrow.arrow_main_blank)

    # time algorithm
    start_time = time.time()
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # class to initialize the time class
    driver_time = DrvTime(time_obj=alg_variables_settings, time_collectors=alg_variables_collector)
    # method to configure time variables
    alg_variables_time = driver_time.configure_variable_time(time_run_cmd=alg_time_settings)
    # method to organize time variables
    alg_variables_time = driver_time.organize_variable_time(
        time_obj=alg_variables_time, collector_obj=alg_variables_collector)
    # method to view time variables
    driver_time.view_variable_time(data=alg_variables_time, mode=True)

    # collector data
    collector_data.view()
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # info algorithm (end)
    alg_time_elapsed = round(time.time() - start_time, 1)

    logger_stream.info(logger_arrow.arrow_main_blank)
    logger_stream.info(logger_arrow.main + alg_name + ' (Version: ' + alg_version + ' Release_Date: ' + alg_release + ')')
    logger_stream.info(logger_arrow.main + 'TIME ELAPSED: ' + str(alg_time_elapsed) + ' seconds')
    logger_stream.info(logger_arrow.main + '... END')
    logger_stream.info(logger_arrow.main + 'Bye, Bye')
    logger_stream.info(logger_arrow.arrow_main_break)
    # ------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# call script from external library
if __name__ == "__main__":

    collector_vars = {
        'time_run': '202501091400',
        'path_log': '$HOME/log', 'file_log': 'log.txt',
        'path_namelist': '$HOME/namelist', 'file_namelist': 'namelist.txt',
        'path_tmp': '$HOME/tmp/test/time', 'file_tmp': 'tmp.txt',
    }

    main(alg_collectors_settings=collector_vars)
# ----------------------------------------------------------------------------------------------------------------------
