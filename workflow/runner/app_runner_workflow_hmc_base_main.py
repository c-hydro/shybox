#!/usr/bin/python3
"""
SHYBOX - Snow HYdro toolBOX - WORKFLOW RUNNER BASE - HMC

__date__ = '20251203'
__version__ = '1.2.0'
__author__ =
    'Fabio Delogu (fabio.delogu@cimafoundation.org),
     Andrea Libertino (andrea.libertino@cimafoundation.org)'
__library__ = 'shybox'

General command line:
python app_workflow_main.py -settings_file configuration.json -time "YYYY-MM-DD HH:MM"

Examples of environment variables declarations:
TIME_RUN="2021-11-27 01:23";
TIME_PERIOD=12;
PATH_LOG=$HOME/run_base/log/;
PATH_SRC=$HOME/run_base_hmc/;
PATH_DST=$HOME/run_base;
DOMAIN_NAME='marche';
PATH_APP=$HOME/run_base/exec/

Version(s):
20251203 (1.2.0) --> Refactor using class methods in shybox package
20251128 (1.1.0) --> Update release for shybox package
20250117 (1.0.0) --> Beta release for shybox package
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import os
import time

from shybox.logging_toolkit.logging_handler import LoggingManager
from shybox.config_toolkit.arguments_handler import ArgumentsManager
from shybox.config_toolkit.config_handler import ConfigManager
from shybox.time_toolkit.time_handler import TimeManager

from shybox.runner_toolkit.namelist.namelist_template_handler import NamelistTemplateManager
from shybox.runner_toolkit.namelist.namelist_structure_handler import NamelistStructureManager

from shybox.runner_toolkit.execution.execution_handler import ExecutionManager, ExecutionAnalyzer
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# algorithm information
project_name = 'shybox'
alg_name = 'Workflow for runner base configuration'
alg_type = 'Package'
alg_version = '1.2.0'
alg_release = '2025-12-03'
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# script main
def main(view_table: bool = False, dry_run : bool = False):

    # ------------------------------------------------------------------------------------------------------------------
    ## CONFIGURATION MANAGEMENT
    # get file settings
    alg_args_obj = ArgumentsManager(settings_folder=os.path.dirname(os.path.realpath(__file__)))
    alg_args_file, alg_args_time = alg_args_obj.get()

    # crete configuration object
    alg_cfg_obj = ConfigManager.from_source(
        src=alg_args_file,
        root_key="configuration",
        application_key=None
    )

    ## INFO MANAGEMENT
    # get application logging
    alg_app_info = alg_cfg_obj.get_application("info", root_key=None, convert_none_to_nan=False,)
    # fill application logging
    alg_app_info = alg_app_info.resolved(
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
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## LOGGING MANAGEMENT
    # get application logging
    alg_app_log = alg_cfg_obj.get_application("log", root_key=None)
    # fill application logging
    alg_app_log = alg_app_log.resolved(
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
    # view application logging
    alg_cfg_obj.view(section=alg_app_log, table_name='application [cfg application logging]', table_print=view_table)

    # set logging instance
    LoggingManager.setup(
        logger_folder=alg_app_log['path'], logger_file=alg_app_log['file_name'],
        logger_format="%(asctime)s %(name)-15s %(levelname)-8s %(message)-80s %(filename)-20s:[%(lineno)-6s - %(funcName)-20s()]",
        handlers=['stream'],
        force_reconfigure=True,
        arrow_base_len=3, arrow_prefix='-', arrow_suffix='>',
        warning_dynamic=False, error_dynamic=False, warning_fixed_prefix="===> ", error_fixed_prefix="===> ",
        level=10
    )

    # define logging instance
    logging_handle = LoggingManager(
        name="shybox_algorithm_runner_hmc",
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
    ## TIME MANAGEMENT
    # create time object
    alg_cfg_time = TimeManager.from_config(
        alg_cfg_obj, start_days_before=0,
        time_as_string=('time_frequency',), time_as_int=('time_period',))
    # update lut using time tags
    alg_cfg_obj.update_lut_using_extra_tags(extra_tags=alg_cfg_time.as_dict(), overwrite=True)
    # view time object
    alg_cfg_time.view(table_name='time', table_print=view_table)
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## VARIABLE MANAGEMENT
    # get lut section
    alg_cfg_lut = alg_cfg_obj.get_section(section='lut')
    # view lut section
    alg_cfg_obj.view(section=alg_cfg_lut, table_name='lut', table_print=view_table)
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## APPLICATION MANAGEMENT
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
    ## APPLICATION NAMELIST
    # 1. create the template manager (with all HMC/S3M templates)
    app_nml_obj = NamelistTemplateManager()

    # 2. (optional) if you just want to inspect the template fields:
    app_nml_fields = app_nml_obj.get(
        model=alg_app_nml['description']['type'],
        version=alg_app_nml['description']['version'],
    )

    # 3. build the namelist text from your fields (flat or sectioned)
    app_nml_struct = NamelistStructureManager.from_dict(
        template_manager=app_nml_obj,
        model=alg_app_nml['description']['type'],
        version=alg_app_nml['description']['version'],
        values=alg_app_nml['fields'],  # e.g. {"by_value": {...}, "by_pattern": {...}}
        as_object=True,
    )
    # view namelist structure
    app_nml_struct.view(table_name='application [file application namelist]', table_print=True)
    # write namelist to file
    app_nml_struct.write_to_ascii(
        filename=alg_app_nml['file']['location'],
        overwrite = True, makedirs = True,)
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## APPLICATION EXECUTION
    # create execution manager
    app_execution_obj = ExecutionManager(
        execution_obj=alg_app_exec,  # your config dict
        time_obj=alg_cfg_time.time_run,  # optional
        settings_obj={
            'MODE': alg_app_exec['description']['execution_mode'],
            'RUN': alg_app_exec['description']['execution_name']},  # or whatever you need for {RUN}
        execution_update=True,  # re-run or reuse .info
        stream_output=True,  # live Fortran logs
        timeout=None,  # or int seconds
    )
    # run execution obj
    app_execution_info = app_execution_obj.run(dry_run=dry_run)
    # view execution info
    app_execution_obj.view(table_name='execution_info', table_print=True)

    """
    # analyze execution obj
    app_analyzer_obj = app_execution_obj.analyze(app_execution_info)

    # view execution info
    app_analyzer_obj.view(table_name='execution_info', table_print=True)
    # dump execution info ['ascii', 'json', 'env']
    app_analyzer_obj.write_to(
        path=alg_app_info['path'], file_name=alg_app_info['file_name'],
        objects=alg_cfg_time, fmt=alg_app_info['format'])
    """
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## INFO END
    # info algorithm (end)
    alg_time_elapsed = round(time.time() - start_time, 1)

    logging_handle.info_header(alg_name + ' (Version: ' + alg_version + ' Release_Date: ' + alg_release + ')',
                               blank_before=True)
    logging_handle.info_header('TIME ELAPSED: ' + str(alg_time_elapsed) + ' seconds')
    logging_handle.info_header('... END')
    logging_handle.info_header('Bye, Bye')
    logging_handle.info_header(LoggingManager.rule_line("=", 78))
    # ------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# call script from external library
if __name__ == "__main__":
    main(view_table=True, dry_run=True)
# ----------------------------------------------------------------------------------------------------------------------
