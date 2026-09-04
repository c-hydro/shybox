#!/usr/bin/python3
"""
SHYBOX - Snow HYdro toolBOX - WORKFLOW CONVERTER POINT TO GRID - HMC

__date__ = '20260901'
__version__ = '1.0.0'
__author__ =
    'Fabio Delogu (fabio.delogu@cimafoundation.org),
     Andrea Libertino (andrea.libertino@cimafoundation.org)'
__library__ = 'shybox'

General command line:
python app_workflow_main.py -settings_file configuration.json -time "YYYY-MM-DD HH:MM"

Examples of environment variables declarations:
TIME_RUN="2026-07-14 01:34";
TIME_PERIOD=10;

TIME_START='2026-07-13 22:00';
TIME_END='2026-07-14 01:00';

DOMAIN_NAME='vda';
PATH_GEO='/home/fabio/Desktop/shybox/dset/case_study_vda/converter_hmc/geo/';
PATH_SRC='/home/fabio/Desktop/shybox/dset/case_study_vda/converter_hmc/data/weather_stations';
PATH_DST='/home/fabio/Desktop/shybox/dset/case_study_vda/converter_hmc/data/maps/';
PATH_TMP=$HOME/Desktop/shybox/exec/case_study_vda/converter_hmc/tmp/;
PATH_LOG=$HOME/Desktop/shybox/exec/case_study_vda/converter_hmc/log/;

# Set env variables to use proj 8.0.0 or proj 6.0.0
PROJ_DATA=/home/fabio/Documents/Work_Area/Code_Development/Workspace/shybox/conda/envs/shybox_geo_libraries/share/proj;
PROJ_LIB=

Version(s):
20260901 (1.0.0) --> Beta release for shybox package
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
from shybox.processing_points_toolkit.points_adapter import exec_interpolate_points

from shybox.orchestrator_toolkit.orchestrator_handler_points import OrchestratorPoints as Orchestrator
from shybox.dataset_toolkit.dataset_handler_local import DataLocal
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# algorithm information
project_name = 'shybox'
alg_name = 'Workflow for converting data points to grid configuration'
alg_type = 'Package'
alg_version = '1.0.0'
alg_release = '2026-09-01'
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
        name="shybox_algorithm_converter_hmc_points2grid",
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
    # create time object (time_priority: bounds, run or period)
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
    # geographical grid
    geo_grid = DataLocal(
        path=alg_cfg_application['static_data']['terrain']['path'],
        file_name=alg_cfg_application['static_data']['terrain']['file_name'],
        file_type='grid_2d', file_format='ascii', file_mode='local', file_variable='terrain', file_io='input',
        variable_template={
            "dims_geo": {"x": "longitude", "y": "latitude"},
            "vars_geo": {"x": "longitude", "y": "latitude"}
        },
        time_signature=None, time_direction=None,
        logger=logging_handle, message=False
    )

    # weather stations registry
    registry_ws = DataLocal(
        path=alg_cfg_application['static_data']['registry_weather_stations']['path'],
        file_name=alg_cfg_application['static_data']['registry_weather_stations']['file_name'],
        data_layout='points',
        file_type='points_registry', file_format='ascii', file_mode='local',
        file_args={
            'sep':',', 'col_reference': 'tag',
            'lut_map': alg_cfg_application['static_data']['registry_weather_stations']['lut_map'],
            'lut_type': alg_cfg_application['static_data']['registry_weather_stations']['lut_type']
        },
        file_variable='registry_ws', file_io='input',
        variable_template={
            "dims_point": {"x": "fields", "y": "sections"},
            "vars_point": {"registry_ws": "registry_ws"}
        },
        time_signature=None, time_direction=None,
        logger=logging_handle, message=False
    )
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## DYNAMIC DATASETS MANAGEMENT
    # source point codes handler
    source_point_code = DataLocal(
        path=alg_cfg_application['dynamic_data_src']['code']['path'],
        file_name=alg_cfg_application['dynamic_data_src']['code']['file_name'],
        data_layout='points', data_mandatory=False,
        file_deps={'registry': registry_ws},
        file_type='points_2d', file_format='ascii', file_mode='local',
        file_variable='AIR_T_DATA_C', file_workflow='AIR_T_WORKFLOW', file_io='input',
        file_args={'delimiter': ',', 'time_format': '%Y-%m-%d %H:%M'},
        variable_template={
            "dims_data": {"x": "data", "y": "weather_stations"},
            "vars_data": {"AIR_T_CODE": "air_temperature_code"}
        },
        time_signature='period',
        time_reference=alg_reference_time, time_period=alg_cfg_time.time_period, time_freq='h', time_direction='forward',
        logger=logging_handle, message=False
    )

    # source point data handler
    source_point_data = DataLocal(
        path=alg_cfg_application['dynamic_data_src']['data']['path'],
        file_name=alg_cfg_application['dynamic_data_src']['data']['file_name'],
        data_layout='points', data_mandatory=False,
        file_deps={'registry': registry_ws},
        file_type='points_2d', file_format='ascii', file_mode='local',
        file_variable='AIR_T_DATA_V', file_workflow='AIR_T_WORKFLOW', file_io='input',
        file_args={'delimiter': ',', 'time_format': '%Y-%m-%d %H:%M'},
        variable_template={
            "dims_data": {"x": "data", "y": "weather_stations"},
            "vars_data": {"AIR_T_VALUE": "air_temperature_value"}
        },
        time_signature='period',
        time_reference=alg_reference_time, time_period=alg_cfg_time.time_period, time_freq='h', time_direction='forward',
        logger=logging_handle, message=False
    )

    # destination grid data handler
    destination_grid_obj = DataLocal(
        path=alg_cfg_application['dynamic_data_dst']['path'],
        file_name=alg_cfg_application['dynamic_data_dst']['file_name'],
        data_layout='grid',
        file_type='grid_2d', file_format='tiff', file_mode='local',
        file_variable='AIR_T_GRID', file_workflow='AIR_T_WORKFLOW', file_io='output',
        variable_template={
            "dims_data": {"longitude": "longitude", "latitude": "longitude", "time": "time"},
            "coord_data": {"longitude": "longitude", "latitude": "latitude"},
            "vars_data": {
                "air_temperature": "air_t"
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
    orc_process = Orchestrator.points(
        data_package_in={
            'AIR_T_DATA_V': source_point_data,
            'AIR_T_DATA_C': source_point_code
        },
        data_package_out={'AIR_T_GRID': destination_grid_obj},
        data_ref={'terrain': geo_grid},
        priority=['AIR_T_WORKFLOW'],
        configuration=alg_cfg_workflow,
        logger=logging_handle
    )
    # orchestrator exec
    orc_process.run(time=pd.date_range(start=alg_cfg_time.time_start, end=alg_cfg_time.time_end, freq='h'),)
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
