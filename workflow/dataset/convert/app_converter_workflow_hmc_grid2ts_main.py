#!/usr/bin/python3
"""
SHYBOX - Snow HYdro toolBOX - WORKFLOW CONVERTER GRID TO TIME-SERIES - HMC

__date__ = '20260825'
__version__ = '1.0.0'
__author__ =
    'Fabio Delogu (fabio.delogu@cimafoundation.org),
     Andrea Libertino (andrea.libertino@cimafoundation.org)'
__library__ = 'shybox'

General command line:
python app_workflow_main.py -settings_file configuration.json -time "YYYY-MM-DD HH:MM"

Examples of environment variables declarations:
DOMAIN_NAME='marche';
PATH_DST='/home/fabio/Desktop/shybox/dset/case_study_marche/converter_hmc/exec/forcing/time_series/';
PATH_GEO='/home/fabio/Desktop/shybox/dset/case_study_marche/converter_hmc/geo';
PATH_LOG=$HOME/Desktop/shybox/exec/case_study_marche/converter_hmc/log/;
PATH_SRC='/home/fabio/Desktop/shybox/dset/case_study_marche/converter_hmc/data/forcing/gridded/';
PATH_TMP=$HOME/Desktop/shybox/exec/case_study_marche/converter_hmc/tmp/;
TIME_END='2026-08-19 00:00';
TIME_START='2026-08-18 12:00'

# Set env variables to use proj 8.0.0 or proj 6.0.0
PROJ_DATA=/home/fabio/Documents/Work_Area/Code_Development/Workspace/shybox/conda/envs/shybox_geo_libraries/share/proj;
PROJ_LIB=

Version(s):
20260825 (1.0.0) --> Beta release for shybox package
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import os
import time
import pandas as pd

from shybox.logging_toolkit.logging_handler import LoggingManager
from shybox.config_toolkit.arguments_handler import ArgumentsManager
from shybox.config_toolkit.config_handler import ConfigManager
from shybox.time_toolkit.time_handler import TimeManager

# fx imported in the PROCESSES (will be used in the global variables PROCESSES) --> DO NOT REMOVE
from shybox.processing_ts_toolkit.lib_proc_compute import compute_average_over_mask
from shybox.processing_ts_toolkit.ts_adapter import exec_average_over_mask

from shybox.orchestrator_toolkit.orchestrator_handler_timeseries import OrchestratorTimeSeries as Orchestrator
from shybox.dataset_toolkit.dataset_handler_local import DataLocal
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# algorithm information
project_name = 'shybox'
alg_name = 'Workflow for converting data grid to time-series configuration'
alg_type = 'Package'
alg_version = '1.1.0'
alg_release = '2026-06-30'
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# script main
def main(view_table: bool = False):

    # ------------------------------------------------------------------------------------------------------------------
    ## CONFIGURATION MANAGEMENT
    # get file settings
    alg_args_obj = ArgumentsManager(
        handlers=['stream'],
        settings_folder=os.path.dirname(os.path.realpath(__file__)))
    alg_args_file, alg_args_time = alg_args_obj.get()

    # crete configuration object
    alg_cfg_obj = ConfigManager.from_source(
        src=alg_args_file,
        root_key="configuration",
        application_key=None
    )

    # get application section
    alg_cfg_application = alg_cfg_obj.get_section(section='application')
    # fill application section
    alg_cfg_application = alg_cfg_obj.fill_obj_from_lut(
        section=alg_cfg_application,
        resolve_time_placeholders=False, time_keys=('time_start', 'time_end', 'time_period'),
        template_keys=('path_time_source', 'file_time_source',
                       'path_time_destination', 'file_time_destination', )
    )
    # view application section
    alg_cfg_obj.view(section=alg_cfg_application, table_name='application [cfg info]', table_print=True)

    # get workflow section
    alg_cfg_workflow = alg_cfg_obj.get_section(section='workflow')
    # view workflow section
    alg_cfg_obj.view(section=alg_cfg_workflow, table_name='workflow', table_print=True)
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
        handlers=alg_app_log.get('handlers', ['stream']),
        force_reconfigure=True,
        arrow_base_len=3, arrow_prefix='-', arrow_suffix='>',
        warning_dynamic=False, error_dynamic=False, warning_fixed_prefix="===> ", error_fixed_prefix="===> ",
        level=10
    )

    # define logging instance
    logging_handle = LoggingManager(
        name="shybox_algorithm_converter_hmc_grid2ts",
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
    # create time object (time_priority: bounds or period)
    alg_cfg_time = TimeManager.from_config(
        alg_cfg_obj, start_days_before=1,
        time_priority='bounds', time_ref='time_run',
        time_as_string=('time_frequency',), time_as_int=('time_period',))

    # update lut using time tags
    alg_cfg_obj.update_lut_using_extra_tags(extra_tags=alg_cfg_time.as_dict(), overwrite=True)
    # view time object
    alg_cfg_time.view(table_name='time', table_print=view_table)
    # get reference time
    alg_reference_time = alg_cfg_time.time_run

    # update the applications obj
    alg_cfg_application = alg_cfg_obj.fill_section_with_times(
        alg_cfg_application,
        time_values={
            'path_dynamic_source': alg_reference_time, 'path_time_source': alg_reference_time,
            'path_dynamic_destination': alg_reference_time, 'path_time_destination': alg_reference_time}
    )

    # view application section
    alg_cfg_obj.view(section=alg_cfg_application, table_name='application [time info]', table_print=True)
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## STATIC DATASETS MANAGEMENT
    # registry sections
    registry_sections_data = DataLocal(
        path=alg_cfg_application['static_data']['registry_sections']['path'],
        file_name=alg_cfg_application['static_data']['registry_sections']['file_name'],
        data_layout='points',
        file_type='points_section_db', file_format='ascii', file_mode='local',
        file_args={
            'lut_map': alg_cfg_application['static_data']['registry_sections']['lut_map'],
            'lut_type': alg_cfg_application['static_data']['registry_sections']['lut_type'],
            'sep':',', 'col_datafrom': 'SEC_TAG'},
        file_variable='registry_db', file_io='input',
        variable_template={
            "dims_point": {"x": "fields", "y": "sections"},
            "vars_data": {"registry_db": "registry_db"}
        },
        time_signature=None, time_direction=None,
        logger=logging_handle, message=False
    )
    # registry hmc
    registry_sections_hmc = DataLocal(
        path=alg_cfg_application['static_data']['registry_hmc']['path'],
        file_name=alg_cfg_application['static_data']['registry_hmc']['file_name'],
        data_layout='points',
        file_type='points_section_hmc', file_format='ascii', file_mode='local',
        file_args={'delimiter':','},
        file_variable='registry_hmc', file_io='input',
        variable_template={
            "dims_point": {"x": "fields", "y": "sections"},
            "vars_data": {"registry_hmc": "registry_hmc"}
        },
        time_signature=None, time_direction=None,
        logger=logging_handle, message=False
    )
    # mask hmc
    mask_sections_hmc = DataLocal(
        path=alg_cfg_application['static_data']['mask']['path'],
        file_name=alg_cfg_application['static_data']['mask']['file_name'],
        data_layout='grid',
        file_type='grid', file_format='tiff', file_mode='local',
        file_args={'sections': registry_sections_hmc},
        file_variable='MASK', file_io='input',
        variable_template={
            "dims_geo": {"longitude": "longitude", "latitude": "latitude"},
            "vars_data": {"mask": "section_mask",}
        },
        time_signature=None, time_direction=None,
        logger=logging_handle, message=False
    )

    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## DYNAMIC DATASETS MANAGEMENT
    # source grid data handler
    source_data = DataLocal(
        path=alg_cfg_application['dynamic_data']['source']['path'],
        file_name=alg_cfg_application['dynamic_data']['source']['file_name'],
        file_type='forcing_hmc', file_format='netcdf', file_mode='local',
        file_variable=['AIR_T', 'RELATIVE_HUMIDITY'], file_io='input',
        file_deps=None, file_args={},
        variable_template={
            "dims_geo": {"west_east": "longitude", "south_north": "latitude", "time": "time"},
            "vars_data": {
                "AirTemperature": "air_temperature",
                "RelHumidity": "relative_humidity",
            }
        },
        time_signature='current',
        time_reference=alg_reference_time,
        time_period=alg_cfg_time.time_period, time_freq='h', time_direction='forward',
        logger=logging_handle
    )

    # destination time-series data handler
    destination_data = DataLocal(
        path=alg_cfg_application['dynamic_data']['destination']['path'],
        file_name=alg_cfg_application['dynamic_data']['destination']['file_name'],
        data_layout='time_series',
        file_type='time_series_hmc', file_format='ascii', file_mode='local',
        file_variable=['AIR_T', 'RELATIVE_HUMIDITY'], file_io='output',
        file_deps=None, file_args={}, file_placeholder={'var_name': ['default', 'default']},
        variable_template={
            "dims_data": {"time": "time", "sections": "n"},
            "coord_data": {"time": "time", "sections": "n"},
            "vars_data": {
                "air_temperature": "air_t",
                "relative_humidity": "rel_hum"
            }
        },
        time_signature='end', time_reference=alg_reference_time,
        time_period=1, time_format='%Y%m%d%H%M',
        logger=logging_handle, message=False
    )
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## ORCHESTRATOR MANAGEMENT
    # orchestrator settings
    orc_process = Orchestrator.time_series(
        data_package_in=source_data, data_package_out=destination_data,
        data_ref={
            'sections_hmc': registry_sections_hmc,
            'sections_db': registry_sections_data,
            'mask': mask_sections_hmc},
        priority=None,
        configuration=alg_cfg_workflow,
        logger=logging_handle
    )
    # orchestrator exec
    orc_process.run(time=pd.date_range(start=alg_cfg_time.time_start, end=alg_cfg_time.time_end, freq='h'),
                    group='by_time')
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
