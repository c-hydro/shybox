#!/usr/bin/env python3
"""
SHYBOX - Snow HYdro toolBOX - WORKFLOW RUNNER ORCHESTRATOR - HMC

__date__ = '20261027'
__version__ = '1.0.0'
__author__ =
    'Fabio Delogu (fabio.delogu@cimafoundation.org),
     Andrea Libertino (andrea.libertino@cimafoundation.org)'
__library__ = 'shybox'

General command line:
python app_workflow_main.py -settings_file configuration.json -time "YYYY-MM-DD HH:MM"

Examples of environment variables declarations:
TIME_RUN="2026-10-27 09:23";
TIME_PERIOD=8;
DOMAIN_NAME='marche';
PATH_APP=$HOME/run_base_hmc/exec/;
PATH_GEO=/home/fabio/Desktop/shybox/dset/case_study_destine/runner_hmc/probabilistic/geo/;
PATH_RESTART=$HOME/Desktop/shybox/dset/case_study_destine/runner_hmc/probabilistic/restart/;
PATH_SRC=$HOME/Desktop/shybox/dset/case_study_destine/runner_hmc/probabilistic/data/;
PATH_DST=$HOME/Desktop/shybox/exec/case_study_destine/runner_hmc/probabilistic/results/;
PATH_TMP=$HOME/Desktop/shybox/exec/case_study_destine/runner_hmc/probabilistic/tmp/;
PATH_LOG=$HOME/Desktop/shybox/exec/case_study_destine/runner_hmc/probabilistic/log/;
PATH_INFO=$HOME/Desktop/shybox/exec/case_study_destine/runner_hmc/probabilistic/info/;

Version(s):
20261027 (1.0.0) --> HMC runner based on orchestration (for prob/det runs)
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import copy
import logging
import os
import time

from copy import copy
from shybox.runner_toolkit.orchestration.lib_utils_orchestration_hmc import (
    configure_hmc_member, execute_hmc_member, configure_hmc_logger, debug_hmc_logger)
from shybox.runner_toolkit.orchestration import OrchestrationManager

from shybox.logging_toolkit.logging_handler import LoggingManager
from shybox.config_toolkit.arguments_handler import ArgumentsManager
from shybox.config_toolkit.config_handler import ConfigManager
from shybox.time_toolkit.time_handler import TimeManager
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# algorithm information
project_name = 'shybox'
alg_name = 'Workflow for runner orchestrator configuration'
alg_type = 'Package'
alg_version = '1.0.0'
alg_release = '2026-10-27'
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# script main
def main(view_table=False, dry_run=True):

    # ------------------------------------------------------------------------------------------------------------------
    ## CONFIGURATION MANAGEMENT
    # get file settings
    alg_args_obj = ArgumentsManager(settings_folder=os.path.dirname(os.path.realpath(__file__)))
    alg_args_file, alg_args_time = alg_args_obj.get()

    # crete configuration object
    alg_cfg_obj = ConfigManager.from_source(src=alg_args_file, root_key="configuration", application_key=None)

    ## VARIABLE MANAGEMENT [ORCHESTRATION, LUT]
    # get orchestration section
    alg_cfg_orchestration = alg_cfg_obj.get_section(section='orchestration')
    # view orchestration section
    alg_cfg_obj.view(section=alg_cfg_orchestration, table_name='orchestration', table_print=view_table)

    # get lut section
    alg_cfg_lut = alg_cfg_obj.get_section(section='lut')
    # view lut section
    alg_cfg_obj.view(section=alg_cfg_lut, table_name='lut', table_print=view_table)

    ## INFO MANAGEMENT
    # get application logging
    alg_app_info = alg_cfg_obj.get_application("info", root_key=None, convert_none_to_nan=False,)
    # fill application logging
    alg_app_info = alg_app_info.resolved(
        time_values=None,  # no fill_section_with_times
        when=None,  # no LUT time resolution
        strict=False,
        time_keys=("time_start", "time_restart", "time_period", "time_tag"),  # <- keep these in LUT
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
    alg_app_log_common = alg_cfg_obj.get_application("log", root_key=None)
    # fill application logging
    alg_app_log_common = alg_app_log_common.resolved(
        time_values=None,  # no fill_section_with_times
        when=None,  # no LUT time resolution
        strict=False,
        time_keys=("time_start", "time_restart", "time_period", "time_tag"),
        resolve_time_placeholders=False,  # do NOT turn time_* into strftime strings
        expand_env=True,  # BUT expand $HOME, $RUN, ...
        env_extra=None,  # or {"RUN": "base"} etc
        extra_tags={'ensemble_member': 'common'},
        validate_result=False,  # or True + allow_placeholders=True if needed
        validate_allow_placeholders=True,
        validate_allow_none=False,
    )
    # view application logging
    alg_cfg_obj.view(section=alg_app_log_common, table_name='application [cfg application logging]', table_print=view_table)

    # set logging instance
    LoggingManager.setup(
        logger_folder=alg_app_log_common['path'], logger_file=alg_app_log_common['file_name'],
        logger_format="%(asctime)s %(name)-15s %(levelname)-8s %(message)-80s %(filename)-20s:[%(lineno)-6s - %(funcName)-20s()]",
        handlers=alg_app_log_common.get('handlers', ['stream']),
        force_reconfigure=True,
        arrow_base_len=3, arrow_prefix='-', arrow_suffix='>',
        warning_dynamic=False, error_dynamic=False, warning_fixed_prefix="===> ", error_fixed_prefix="===> ",
        level=10
    )

    # define logging instance
    logging_handle_main = LoggingManager(
        name="shybox_algorithm_runner_probabilistic_hmc",
        level=logging.INFO, use_arrows=True, arrow_dynamic=True, arrow_tag="algorithm",
        set_as_current=True)
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## INFO START
    # info algorithm (start)
    logging_handle_main.info_header(LoggingManager.rule_line("=", 78))
    logging_handle_main.info_header(alg_name + ' (Version: ' + alg_version + ' Release_Date: ' + alg_release + ')')
    logging_handle_main.info_header('START ... ', blank_after=True)

    # time algorithm
    start_time = time.monotonic()
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## TIME MANAGEMENT
    # create time object
    alg_cfg_time = TimeManager.from_config(
        alg_cfg_obj, start_days_before=0, time_priority='run',
        time_as_string=('time_frequency',), time_as_int=('time_period',))
    # update lut using time tags
    alg_cfg_obj.update_lut_using_extra_tags(extra_tags=alg_cfg_time.as_dict(), overwrite=True)
    # view time object
    alg_cfg_time.view(table_name='time', table_print=view_table)
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## ORCHESTRATOR MANAGEMENT
    # method to instantiate the run(s)
    def callback(member, _config):

        # get members tags
        member_tags = member.tags
        # get time tags
        time_tags = alg_cfg_time.as_dict()

        # info hmc member start
        logging_handle_main.info(f"HMC orchestration member: {member_tags['ensemble_member']} ... ")

        # method to configure member log(s)
        member_log = configure_hmc_logger(copy(alg_cfg_obj), member_tags, time_tags, view_table=view_table)

        # method to configure application (per worker)
        member_cfg = configure_hmc_member(
            copy(alg_cfg_obj), member_log, member_tags, time_tags,
            view_table=view_table)

        # method to execute application (per worker)
        member_exec = execute_hmc_member(member_tags, member_cfg,
                                         time_tags=time_tags, dry_run=dry_run, view_table=view_table)

        # info hmc member end
        logging_handle_main.info(f"HMC orchestration member: {member_tags['ensemble_member']} ... DONE")

        return member_exec

    # method to organize orchestrator
    orchestrator_manager = OrchestrationManager(
        orchestration_obj=alg_cfg_orchestration,
        execution_callback=callback,
        logger_stream=logging_handle_main)

    # method to run orchestrator
    orchestrator_results = orchestrator_manager.run(config={})
    # method to summarize orchestrator
    orchestrator_summary = orchestrator_manager.summary()

    # info hmc summary status
    logging_handle_main.info_status('HMC orchestration summary: %s', orchestrator_summary)
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## INFO END
    # info algorithm (end)
    alg_time_elapsed = round(time.monotonic()  - start_time, 1)

    logging_handle_main.info_header(alg_name + ' (Version: ' + alg_version + ' Release_Date: ' + alg_release + ')',
                               blank_before=True)
    logging_handle_main.info_header('TIME ELAPSED: ' + str(alg_time_elapsed) + ' seconds')
    logging_handle_main.info_header('... END')
    logging_handle_main.info_header('Bye, Bye')
    logging_handle_main.info_header(LoggingManager.rule_line("=", 78))

    return orchestrator_results, orchestrator_summary
    # ------------------------------------------------------------------------------------------------------------------

#-----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# call script from external library
if __name__ == '__main__':
    main(view_table=True, dry_run=True)
# ----------------------------------------------------------------------------------------------------------------------
