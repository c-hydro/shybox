#!/usr/bin/python3
"""
SHYBOX - Snow HYdro toolBOX - WORKFLOW RUNNER BASE - HMC

__date__ = '20251128'
__version__ = '1.1.0'
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
20251128 (1.1.0) --> Update release for shybox package
20250117 (1.0.0) --> Beta release for shybox package
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
from shybox.runner_toolkit.namelist.old.driver_app_namelist import DrvNamelist
from shybox.runner_toolkit.execution.driver_app_execution import DrvExec

from shybox.logging_toolkit.logging_handler import LoggingManager
from shybox.config_toolkit.arguments_handler import ArgumentsManager
from shybox.config_toolkit.config_handler import ConfigManager
from shybox.time_toolkit.time_handler import TimeManager

from shybox.runner_toolkit.namelist.namelist_template_handler import NamelistTemplateManager
from shybox.runner_toolkit.namelist.namelist_structure_handler import NamelistStructureManager

from shybox.runner_toolkit.execution.execution_handler import ExecutionManager

# set logger
logger_stream = logging.getLogger(logger_name)
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# algorithm information
project_name = 'shybox'
alg_name = 'Workflow for runner base configuration'
alg_type = 'Package'
alg_version = '1.1.0'
alg_release = '2025-01-15'
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# script main
def main(view_table: bool = True):

    # ------------------------------------------------------------------------------------------------------------------
    ## SETTINGS MANAGEMENT
    # get file settings
    alg_args_obj = ArgumentsManager(settings_folder=os.path.dirname(os.path.realpath(__file__)))
    alg_args_file, alg_args_time = alg_args_obj.get()

    # crete configuration object
    alg_cfg_obj = ConfigManager.from_source(
        src=alg_args_file,
        root_key="configuration",
        application_key=None
    )

    # create time object
    alg_cfg_time = TimeManager.from_config(
        alg_cfg_obj, start_days_before=2,
        time_as_string=('time_frequency',), time_as_int=('time_period',))
    # update lut using time tags
    alg_cfg_obj.update_lut_using_extra_tags(extra_tags=alg_cfg_time.as_dict(), overwrite=True)
    # view time object
    alg_cfg_time.view(table_name='time', table_print=view_table)

    # get lut section
    alg_cfg_lut = alg_cfg_obj.get_section(section='lut')
    # view lut section
    alg_cfg_obj.view(section=alg_cfg_lut, table_name='lut', table_print=view_table)

    # get application execution
    alg_app_exec = alg_cfg_obj.get_application("application_execution", root_key=None)
    # fill application execution
    alg_app_exec = alg_app_exec.resolved(
        time_values=None,  # no fill_section_with_times
        when=None,  # no LUT time resolution
        strict=False,
        resolve_time_placeholders=False,  # do NOT turn time_* into strftime strings
        expand_env=True,  # BUT expand $HOME, $RUN, ...
        env_extra=None,  # or {"RUN": "base"} etc
        validate_result=False,  # or True + allow_placeholders=True if needed
        validate_allow_placeholders=True,
        validate_allow_none=False,
    )
    # view application execution section
    alg_cfg_obj.view(section=alg_app_exec, table_name='application [cfg application execution]', table_print=view_table)

    # get application namelist
    alg_app_nml = alg_cfg_obj.get_application("application_namelist", root_key=None)
    # fill application namelist
    alg_app_nml = alg_app_nml.resolved(
        time_values=None,  # no fill_section_with_times
        when=None,  # no LUT time resolution
        strict=False,
        resolve_time_placeholders=False,  # do NOT turn time_* into strftime strings
        expand_env=True,  # BUT expand $HOME, $RUN, ...
        time_keys=("time_start", "time_restart", "time_period"),  # <- keep these in LUT
        env_extra=None,  # or {"RUN": "base"} etc
        validate_result=False,  # or True + allow_placeholders=True if needed
        validate_allow_placeholders=True,
        validate_allow_none=False,
    )

    # view application namelist section
    alg_cfg_obj.view(section=alg_app_nml, table_name='application [cfg application namelist]', table_print=True)
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # 1. create the template manager (with all HMC/S3M templates)
    nml_template_obj = NamelistTemplateManager()

    # 2. (optional) if you just want to inspect the template fields:
    nml_template_fields = nml_template_obj.get(
        model=alg_app_nml['description']['type'],
        version=alg_app_nml['description']['version'],
    )

    # 3a. build the namelist text from your fields (flat or sectioned)
    nml_struct_text_by_config = NamelistStructureManager.from_dict(
        template_manager=nml_template_obj,
        model=alg_app_nml['description']['type'],
        version=alg_app_nml['description']['version'],
        values=alg_app_nml['fields'],  # e.g. {"by_value": {...}, "by_pattern": {...}}
        as_object=True,
    )
    # view namelist structure
    nml_struct_text_by_config.view(table_name='application [file application namelist]', table_print=True)
    # write namelist to file
    nml_struct_text_by_config.write_to_ascii(
        filename="/home/fabio/run_base_hmc/config/hmc_330_by_config.nml",
        overwrite = True, makedirs = True,)

    # 3b. build the namelist text from your fields (flat or sectioned)
    nml_struct_text_by_file = NamelistStructureManager.from_file(
        filename='hmc.template.info.v3.3.0.txt',
        template_manager=nml_template_obj,
        model=alg_app_nml['description']['type'],
        version=alg_app_nml['description']['version'],
        as_object=True,
    )
    # view namelist structure
    nml_struct_text_by_file.view(table_name='application [file application namelist]', table_print=True)
    # write namelist to file
    nml_struct_text_by_file.write_to_ascii(
        filename="/home/fabio/run_base_hmc/config/hmc_330_by_file.nml",
        overwrite = True, makedirs = True,)
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # create execution manager
    app_execution_manager = ExecutionManager(
        execution_obj=alg_app_exec,  # your config dict
        time_obj=alg_cfg_time.time_run,  # optional
        settings_obj={'RUN': 'exec_base'},  # or whatever you need for {RUN}
        execution_update=True,  # re-run or reuse .info
        stream_output=True,  # live Fortran logs
        timeout=None,  # or int seconds
    )
    # run execution
    execution_info = app_execution_manager.run()
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## LOGGING MANAGEMENT
    # set logging instance
    LoggingManager.setup(
        logger_folder=alg_cfg_obj['log']['path'],
        logger_file=alg_cfg_obj['log']['file_name'],
        logger_format="%(asctime)s %(name)-15s %(levelname)-8s %(message)-80s %(filename)-20s:[%(lineno)-6s - %(funcName)-20s()]",
        handlers=['file', 'stream'],
        force_reconfigure=True,
        arrow_base_len=3, arrow_prefix='-', arrow_suffix='>',
        warning_dynamic=False, error_dynamic=False, warning_fixed_prefix="===> ", error_fixed_prefix="===> ",
        level=10
    )

    # define logging instance
    logging_handle = LoggingManager(
        name="shybox_algorithm_converter_itwater_hmc_forcing",
        level=logging.INFO, use_arrows=True, arrow_dynamic=True, arrow_tag="algorithm",
        set_as_current=True)
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## INFO START
    # info algorithm (start)
    logging_handle.info_header(LoggingManager.rule_line("=", 78))
    logging_handle.info_header(alg_name + ' (Version: ' + alg_version + ' Release_Date: ' + alg_release + ')')
    logging_handle.info_header('START ... ', blank_after=True)

    # time algorithm
    start_time = time.time()
    # ------------------------------------------------------------------------------------------------------------------



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
    driver_time.view_variable_time(data=alg_variables_time, mode=False)

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

    main(view_table=False)

# ----------------------------------------------------------------------------------------------------------------------
