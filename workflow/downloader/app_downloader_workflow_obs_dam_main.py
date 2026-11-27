#!/usr/bin/python3
"""
SHYBOX - Snow HYdro toolBOX - WORKFLOW DOWNLOADER OBSERVED DATASETS - HMC

__date__ = '20261125'
__version__ = '1.0.0'
__author__ =
    'Fabio Delogu (fabio.delogu@cimafoundation.org),
     Andrea Libertino (andrea.libertino@cimafoundation.org)
     Francesco Avanzi (francesco.avanzi@cimafoundation.org)'
     '
__library__ = 'shybox'

General command line:
python app_downloader_workflow_vda_obs_ws_main.py -settings_file app_downloader_workflow_obs_rain_local.json -time "YYYY-MM-DD HH:MM"

Use app_downloader_workflow_obs_rain_server.json on a server with direct access to PostgreSQL.
Set configuration.application.source.provider to select the observation provider.
The local and server examples both use vda with their respective connection settings.
Each destination selects an output writer with file_format, which defaults to csv.

Examples of environment variables declarations:
TIME_RUN="2026-11-14 12:00";
DOMAIN_NAME='vda';
PATH_LOG='/home/fabio/Desktop/shybox/exec/case_study_vda/downloader_obs/log/';
PATH_DATA='/home/fabio/Desktop/shybox/exec/case_study_vda/downloader_obs/data/';

Version(s):
20261125 (1.0.0) --> First release of shybox downloader
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import time
from pathlib import Path

from shybox.config_toolkit.arguments_handler import ArgumentsManager
from shybox.config_toolkit.config_handler import ConfigManager

from shybox.db_toolkit.db_handler_base import ConfigurationDB
from shybox.logging_toolkit.logging_handler import LoggingManager
from shybox.time_toolkit.time_handler import TimeManager
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# algorithm information
project_name = 'shybox'
alg_name = 'Workflow for downloading observed datasets (dams network)'
alg_type = 'Package'
alg_version = '1.0.0'
alg_release = '2026-11-25'
settings_folder = Path(__file__).resolve().parent
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# script main
def main(view_table: bool = False):

    # ------------------------------------------------------------------------------------------------------------------
    ## CONFIGURATION MANAGEMENT
    # get file settings
    alg_args_obj = ArgumentsManager(handlers=['stream'], settings_folder=str(settings_folder))
    alg_args_file, alg_args_time = alg_args_obj.get()

    # create configuration object
    alg_cfg_obj = ConfigManager.from_source(
        src=alg_args_file, root_key='configuration', convert_none_to_nan=False
    )
    if alg_args_time is not None:
        alg_cfg_obj.update_lut_using_extra_tags({'time_run': alg_args_time}, overwrite=True)

    ## APPLICATION MANAGEMENT
    # get application section
    alg_cfg_application = alg_cfg_obj.get_section(section='application')
    # fill application section
    alg_cfg_application = alg_cfg_obj.fill_obj_from_lut(
        section=alg_cfg_application,
        resolve_time_placeholders=False,
        time_keys=('time_run', 'time_start', 'time_end', 'time_period'),
        template_keys=None
    )
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## LOGGING MANAGEMENT
    # get logging section
    alg_cfg_log = alg_cfg_obj.get_section('log', raise_if_missing=True)
    # view logging section
    alg_cfg_obj.view(section=alg_cfg_log, table_name='application [cfg application logging]', table_print=view_table)

    # set logging instance
    LoggingManager.setup(
        logger_folder=alg_cfg_log['path'], logger_file=alg_cfg_log['file_name'],
        logger_format="%(asctime)s %(name)-15s %(levelname)-8s %(message)-80s %(filename)-20s:[%(lineno)-6s - %(funcName)-20s()]",
        handlers=alg_cfg_log.get('handlers', ['stream']),
        force_reconfigure=True,
        arrow_base_len=3, arrow_prefix='-', arrow_suffix='>',
        warning_dynamic=False, error_dynamic=False, warning_fixed_prefix="===> ", error_fixed_prefix="===> ",
        level=10
    )

    # define logging instance
    logging_handle = LoggingManager(
        name="shybox_algorithm_downloader_observed_datasets",
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
        alg_cfg_obj, start_days_before=None, time_priority='period',
        time_as_string=('time_frequency',), time_as_int=('time_period',))
    # update lut using time tags
    alg_cfg_obj.update_lut_using_extra_tags(extra_tags=alg_cfg_time.as_dict(), overwrite=True)
    # view time object
    alg_cfg_time.view(table_name='time', table_print=view_table)
    # get time_run and time_period
    alg_time_run, alg_time_range = alg_cfg_time.time_run, alg_cfg_time.time_range

    # view application section
    alg_cfg_obj.view(section=alg_cfg_application, table_name='application [time info]', table_print=True)
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## DATA MANAGEMENT
    configuration_db = ConfigurationDB(
        database=alg_cfg_application['source']['database'],
        variables=alg_cfg_application['source']['variables'],
        output=alg_cfg_application['destination'],
        backend=alg_cfg_application['source'].get('backend'),
        provider=alg_cfg_application['source'].get('provider', 'vda'), logger=logging_handle,
    )

    # iterate over time period
    for time_db in alg_time_range:

        # info message time start
        logging_handle.info_up(f'Time {time_db} ... ')

        # iterate over configuration variable(s)
        for var_db_name, var_db_settings in alg_cfg_application['destination'].items():

            # configure object by time
            var_db_object = alg_cfg_obj.fill_section_with_times(
                var_db_settings, time_values={'path_time_data': time_db, 'file_time_data': time_db,},
            )
            # configure obj by extra args
            var_db_object = alg_cfg_obj.fill_obj_from_lut(
                var_db_object, extra_tags={'variable': var_db_name}, strict=True
            )

            # get data
            var_db_data = configuration_db.get_data(var_db_name, time_db)
            # write data
            configuration_db.write_data(var_db_data, var_db_object, variable=var_db_name)

        # info message time end
        logging_handle.info_down(f'Time {time_db} ... DONE')
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
    main(view_table=True)
# ----------------------------------------------------------------------------------------------------------------------
