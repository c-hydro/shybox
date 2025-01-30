#!/usr/bin/python3
"""
SHYBOX - Snow HYdro toolBOX - WORKFLOW RUNNER BASE

__date__ = '20250117'
__version__ = '1.0.0'
__author__ =
    'Fabio Delogu (fabio.delogu@cimafoundation.org),
     Andrea Libertino (andrea.libertino@cimafoundation.org)'
__library__ = 'shybox'

General command line:
python app_workflow_main.py -settings_file configuration.json -time "YYYY-MM-DD HH:MM"

Examples of environment variables declarations:
PATH_SRC=$HOME/run_base/;
PATH_DST=$HOME/run_base;
DOMAIN_NAME='marche'
PATH_LOG=$HOME/run_base/log/;
PATH_NAMELIST=$HOME/run_base/exec/;
PATH_EXEC=$HOME/run_base/exec/

Version(s):
20250117 (1.0.0) --> Beta release for hmc-suite package
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import os
import time

from shybox.generic_toolkit.lib_utils_args import get_args
from shybox.generic_toolkit.lib_utils_logging import set_logging_stream

from shybox.generic_toolkit.lib_default_args import logger_name, logger_format, logger_arrow
from shybox.generic_toolkit.lib_default_args import collector_data

from shybox.runner_toolkit.settings.driver_app_settings import DrvSettings
from shybox.runner_toolkit.time.driver_app_time import DrvTime
from shybox.runner_toolkit.namelist.driver_app_namelist import DrvNamelist
from shybox.runner_toolkit.execution.driver_app_execution import DrvExec

# set logger
logger_stream = logging.getLogger(logger_name)
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# algorithm information
project_name = 'shybox'
alg_name = 'Workflow for runner base configuration'
alg_type = 'Package'
alg_version = '1.0.0'
alg_release = '2025-01-15'
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# script main
def main(alg_collectors_settings: dict = None):

    # ------------------------------------------------------------------------------------------------------------------
    # get file settings
    alg_file_settings, alg_time_settings = get_args(settings_folder=os.path.dirname(os.path.realpath(__file__)))

    # method to initialize settings class
    driver_settings = DrvSettings(file_name=alg_file_settings, file_time=alg_time_settings,
                                  file_key='settings', settings_collectors=alg_collectors_settings)

    # method to configure variable settings
    (alg_variables_settings,
     alg_variables_collector, alg_variables_system) = driver_settings.configure_variable_by_settings()
    # method to organize variable settings
    alg_variables_settings = driver_settings.organize_variable_settings(
        alg_variables_settings, alg_variables_collector)
    # method to view variable settings
    driver_settings.view_variable_settings(data=alg_variables_settings, mode=True)

    # get variables namelist
    alg_variables_namelist = driver_settings.get_variable_by_tag('namelist')
    # get variables application
    alg_variables_application = driver_settings.get_variable_by_tag('application')
    alg_variables_application = driver_settings.fill_variable_by_dict(alg_variables_application, alg_variables_settings)

    # get variables flags
    alg_variables_flags = driver_settings.get_variable_by_tag('flags')

    # collector data
    collector_data.view(table_print=False)

    # set logging stream
    set_logging_stream(
        logger_name=logger_name, logger_format=logger_format,
        logger_folder=alg_variables_settings['path_log'], logger_file=alg_variables_settings['file_log'])
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # info algorithm (start)
    logger_stream.info(logger_arrow.arrow_main_break)
    logger_stream.info(
        logger_arrow.main + alg_name + ' (Version: ' + alg_version + ' Release_Date: ' + alg_release + ')')
    logger_stream.info(logger_arrow.main + 'START ... ')
    logger_stream.info(logger_arrow.arrow_main_blank)

    # time algorithm
    start_time = time.time()
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # class to initialize the hmc time
    driver_time = DrvTime(time_obj=alg_variables_settings, time_collectors=alg_variables_collector)
    # method to configure time variables
    alg_variables_time = driver_time.configure_variable_time(time_run_cmd=alg_time_settings)
    # method to organize time variables
    alg_variables_time = driver_time.organize_variable_time(
        time_obj=alg_variables_time, collector_obj=alg_variables_collector)
    # method to view time variables
    driver_time.view_variable_time(data=alg_variables_time, mode=True)

    # collector data
    collector_data.view(table_print=False)
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # driver namelist variable(s)
    driver_namelist = DrvNamelist(
        namelist_obj=alg_variables_namelist,
        time_obj=alg_variables_time,
        namelist_update=alg_variables_flags['update_namelist'])

    # method to define namelist file(s)
    alg_namelist_file = driver_namelist.define_file_namelist(settings_variables=alg_variables_settings)
    # method to get namelist structure
    alg_namelist_default = driver_namelist.get_structure_namelist()
    # method to define namelist variable(s)
    alg_namelist_by_value = driver_namelist.define_variable_namelist(settings_variables=alg_variables_settings)
    # method to combine namelist variable(s)
    alg_namelist_defined, alg_namelist_checked, alg_namelist_collections = driver_namelist.combine_variable_namelist(
        variables_namelist_default=alg_namelist_default, variables_namelist_by_value=alg_namelist_by_value)
    # method to dump namelist structure
    driver_namelist.dump_structure_namelist(alg_namelist_defined)

    # method to view namelist variable(s)
    driver_namelist.view_variable_namelist(data=alg_namelist_defined, mode=True)

    # collector data
    collector_data.view(table_print=False)
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # driver execution variable(s)
    driver_exec = DrvExec(
        execution_obj=alg_variables_application,
        time_obj=alg_variables_time,
        settings_obj=alg_variables_settings,
        execution_update=alg_variables_flags['update_execution'])

    # method to configure process executable
    driver_exec.configure_process_job()
    # method to execute process job
    driver_exec.execute_process_job()
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # info algorithm (end)
    alg_time_elapsed = round(time.time() - start_time, 1)

    logger_stream.info(logger_arrow.arrow_main_blank)
    logger_stream.info(
        logger_arrow.main + alg_name + ' (Version: ' + alg_version + ' Release_Date: ' + alg_release + ')')
    logger_stream.info(logger_arrow.main + 'TIME ELAPSED: ' + str(alg_time_elapsed) + ' seconds')
    logger_stream.info(logger_arrow.main + '... END')
    logger_stream.info(logger_arrow.main + 'Bye, Bye')
    logger_stream.info(logger_arrow.arrow_main_break)
    # ------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# call script from external library
if __name__ == "__main__":

    collector_vars = {}
    main(alg_collectors_settings=collector_vars)

# ----------------------------------------------------------------------------------------------------------------------
