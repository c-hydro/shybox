#!/usr/bin/python3
"""
SHYBOX PACKAGE - APP PROCESSING DATASET MAIN

__date__ = '20251118'
__version__ = '1.2.0'
__author__ =
    'Fabio Delogu (fabio.delogu@cimafoundation.org),
     Andrea Libertino (andrea.libertino@cimafoundation.org)'
__library__ = 'shybox'

General command line:
python app_converter_workflow_main.py -settings_file configuration.json -time "YYYY-MM-DD HH:MM"

Examples of environment variables declarations:
DOMAIN_NAME='marche';
TIME_START='2024-10-17 06:00';
TIME_END='2024-10-17 06:00';
PATH_GEO='/home/fabio/Desktop/shybox/dset/case_study_destine/data/data_static/gridded/';
PATH_SRC='/home/fabio/Desktop/shybox/dset/case_study_destine/data/data_dynamic/source/icon/';
PATH_DST='/home/fabio/Desktop/shybox/dset/case_study_destine/data/data_dynamic/destination/icon/';
PATH_LOG=/home/fabio/Desktop/shybox/dset/case_study_destine/log/;

Version(s):
20251116 (1.2.0) --> Release for shybox package (hmc datasets converter base configuration)
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import time
import os

import numpy as np
import pandas as pd

from shybox.generic_toolkit.lib_utils_args import get_args, get_logger_name
from shybox.generic_toolkit.lib_default_args import logger_name, logger_format, logger_arrow
from shybox.generic_toolkit.lib_default_args import collector_data
from shybox.generic_toolkit.lib_utils_time import (select_time_range, select_time_format,
                                                   get_time_length, get_time_bounds)
from shybox.generic_toolkit.lib_utils_string import fill_string

from shybox.processing_toolkit.lib_proc_merge import merge_data_by_ref
from shybox.processing_toolkit.lib_proc_mask import mask_data_by_ref, mask_data_by_limits
from shybox.processing_toolkit.lib_proc_interp import interpolate_data
from shybox.processing_toolkit.lib_proc_compute_humidity import compute_data_rh
from shybox.processing_toolkit.lib_proc_compute_wind import compute_data_wind_speed
from shybox.processing_toolkit.lib_proc_compute_radiation import compute_data_astronomic_radiation
from shybox.processing_toolkit.lib_proc_compute_temperature import convert_temperature_units

from shybox.runner_toolkit.settings.driver_app_settings import DrvSettings
from shybox.orchestrator_toolkit.orchestrator_handler_base import OrchestratorHandler as Orchestrator
from shybox.dataset_toolkit.dataset_handler_local import DataLocal
from shybox.logging_toolkit.logging_handler import LoggingManager
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# algorithm information
project_name = 'shybox'
alg_name = 'Application for processing datasets'
alg_type = 'Package'
alg_version = '1.2.0'
alg_release = '2025-11-18'
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# main function
def main(alg_collectors_settings: dict = None):

    # ------------------------------------------------------------------------------------------------------------------
    ## SETTINGS MANAGEMENT
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

    # get variables application
    alg_variables_application = driver_settings.get_variable_by_tag('application')
    alg_variables_application = driver_settings.fill_variable_by_dict(alg_variables_application, alg_variables_settings)

    # collector data
    collector_data.view(table_print=False)
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## LOGGING MANAGEMENT
    # set logging instance
    LoggingManager.setup(
        logger_folder=alg_variables_settings['path_log'],
        logger_file=alg_variables_settings['file_log'],
        logger_format="%(asctime)s %(name)-15s %(levelname)-8s %(message)-80s %(filename)-20s:[%(lineno)-6s - %(funcName)-20s()]",
        handlers=['file', 'stream'],
        force_reconfigure=True,
        arrow_base_len=3, arrow_prefix='-', arrow_suffix='>',
        warning_dynamic=False, error_dynamic=False, warning_fixed_prefix="===> ", error_fixed_prefix="===> ",
        level=10
    )

    # define logging instance
    logging_handle = LoggingManager(
        name="shybox_algorithm_converter_destine_icon",
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
    ## WORKFLOW MANAGEMENT
    # Orchestrator class for multi variable(s)
    configuration = {
        "WORKFLOW": {
            "options": {
                "intermediate_output": "Tmp",
                "tmp_dir": "tmp"
            },
            "process_list": {

                "AIR_T": [
                    {"function": "interpolate_data", "method":'nn', "max_distance": 25000, "neighbours": 7, "fill_value": np.nan},
                    {"function": "mask_data_by_ref", "ref_value": -9999, "mask_no_data": np.nan},
                    {"function": "convert_temperature_units", "to_celsius": True}
                ],

                "RAIN": [
                    {"function": "interpolate_data", "method": 'nn', "max_distance": 25000, "neighbours": 7,
                     "fill_value": np.nan},
                    {"function": "mask_data_by_ref", "ref_value": -9999, "mask_no_data": np.nan}
                ],

                "WIND_U": [], "WIND_V": [],
                "WIND_SPEED": [
                    {"function": "compute_data_wind_speed", "ref_value": -9999, "mask_no_data": np.nan,
                     "deps_vars": {'u': 'WIND_U', 'v': 'WIND_V'}},
                    {"function": "interpolate_data", "method": 'nn', "max_distance": 22000, "neighbours": 7,
                     "fill_value": np.nan},
                    {"function": "mask_data_by_ref", "ref_value": -9999, "mask_no_data": np.nan}
                ],

                "QW": [], "DEW_POINT": [],
                "RH": [
                    {"function": "compute_data_rh", "ref_value": -9999, "mask_no_data": np.nan,
                     "deps_vars": {'t': 'AIR_T', 'q': 'QV', 'td': 'DEW_POINT'}},
                    {"function": "interpolate_data", "method": 'nn', "max_distance": 22000, "neighbours": 7,
                     "fill_value": np.nan},
                    {"function": "mask_data_by_ref", "ref_value": -9999, "mask_no_data": np.nan}
                ],

                "INC_RAD": [
                    {"function": "compute_data_astronomic_radiation", "ref_value": -9999, "mask_no_data": np.nan,
                     "deps_vars": {'rain': 'RAIN'}},
                    {"function": "interpolate_data", "method": 'nn', "max_distance": 22000, "neighbours": 7,
                     "fill_value": np.nan},
                    {"function": "mask_data_by_ref", "ref_value": -9999, "mask_no_data": np.nan}
                ]

            }
        }
    }
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## TIME MANAGEMENT (GLOBAL)
    # Time generic configuration
    alg_reference_time = select_time_range(
        time_start=alg_variables_application['time']['start'],
        time_end=alg_variables_application['time']['end'],
        time_frequency=alg_variables_application['time']['frequency'], ensure_range=False)
    alg_reference_time = select_time_format(alg_reference_time, time_format=alg_variables_application['time']['format'])
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## GEO MANAGEMENT
    # geographic reference
    geo_data = DataLocal(
        path=alg_variables_application['geo']['terrain']['path'],
        file_name=alg_variables_application['geo']['terrain']['file_name'],
        file_type='grid_2d', file_format='ascii', file_mode='local', file_variable='terrain', file_io='input',
        variable_template={
            "dims_geo": {"x": "longitude", "y": "latitude"},
            "vars_geo": {"x": "longitude", "y": "latitude"}
        },
        time_signature=None, time_direction=None,
        logger=logging_handle, message=False
    )
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## TIME MANAGEMENT (STEP)
    # time analysis info
    time_anls_length = 24
    time_anls_period = select_time_range(
        time_start=alg_reference_time[0], time_period=time_anls_length, time_frequency='h')
    time_anls_start = select_time_format(time_anls_period[0], time_format='%Y-%m-%d %H:%M')
    time_anls_end = select_time_format(time_anls_period[-1], time_format='%Y-%m-%d %H:%M')
    # time data info
    time_data_length = 24
    time_data_period = select_time_range(
        time_start=alg_reference_time[0], time_period=time_data_length, time_frequency='h')
    time_data_start = select_time_format(time_data_period[0], time_format='%Y-%m-%d %H:%M')
    time_format_file = select_time_format(time_data_period[0], time_format='%Y%m%d%H%M')
    time_format_folder = select_time_format(time_data_period[0], time_format='%Y%m%d_%H%M')
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## DATASETS MANAGEMENT
    # Rain file and folder name
    file_name = fill_string(
        alg_variables_application['data_source']['rain']['file_name'],
        time_source=time_format_file)
    folder_name = fill_string(
        alg_variables_application['data_source']['rain']['path'],
        path_time_source=time_format_folder)
    # Rain handler
    rain_data = DataLocal(
        path=folder_name, file_name=file_name,
        file_type='grid_3d', file_format='grib', file_mode='local', file_variable='RAIN', file_io='input',
        variable_template={
            "dims_geo": {"longitude": "longitude", "latitude": "latitude", "step": "time"},
            "vars_data": {"tp": "rain"}
        },
        time_signature='period',
        time_reference=time_data_start, time_period=time_data_length, time_freq='h', time_direction='forward',
        logger=logging_handle, message=False
    )

    # Air Temperature file and folder names
    file_name = fill_string(
        alg_variables_application['data_source']['air_t']['file_name'],
        time_source=time_format_file)
    folder_name = fill_string(
        alg_variables_application['data_source']['air_t']['path'],
        path_time_source=time_format_folder)
    # Air Temperature handler
    airt_data = DataLocal(
        path=folder_name, file_name=file_name,
        file_type='grid_3d', file_format='grib', file_mode='local', file_variable='AIR_T', file_io='input',
        variable_template={
            "dims_geo": {"longitude": "longitude", "latitude": "latitude", "step": "time"},
            "vars_data": {"t2m": "air_temperature"}
        },
        time_signature='period',
        time_reference=time_data_start, time_period=time_data_length, time_freq='h', time_direction='forward',
        logger=logging_handle, message=False
    )

    # Specific humidity file and folder names
    file_name = fill_string(
        alg_variables_application['data_source']['specific_humidity']['file_name'],
        time_source=time_format_file)
    folder_name = fill_string(
        alg_variables_application['data_source']['specific_humidity']['path'],
        path_time_source=time_format_folder)
    # Specific humidity handler
    qv_data = DataLocal(
        path=folder_name, file_name=file_name,
        file_type='grid_3d', file_format='grib', file_mode='local', file_variable='QV', file_io='input',
        variable_template={
            "dims_geo": {"longitude": "longitude", "latitude": "latitude", "step": "time"},
            "vars_data": {"qv": "specific_humidity"}
        },
        time_signature='period',
        time_reference=time_data_start, time_period=time_data_length, time_freq='h', time_direction='forward',
        logger=logging_handle
    )

    # Dew Point Temperature file and folder names
    file_name = fill_string(
        alg_variables_application['data_source']['dew_point_temperature']['file_name'],
        time_source=time_format_file)
    folder_name = fill_string(
        alg_variables_application['data_source']['dew_point_temperature']['path'],
        path_time_source=time_format_folder)

    # Dew Point Temperature handler
    td_data = DataLocal(
        path=folder_name, file_name=file_name,
        file_type='grid_3d', file_format='grib', file_mode='local', file_variable='DEW_POINT', file_io='input',
        variable_template={
            "dims_geo": {"longitude": "longitude", "latitude": "latitude", "step": "time"},
            "vars_data": {"d2m": "dew_point_temperature"}
        },
        time_signature='period',
        time_reference=time_data_start, time_period=time_data_length, time_freq='h', time_direction='forward',
        logger=logging_handle
    )

    # Relative Humidity handler (derived)
    rh_data = DataLocal(
        path=None,
        file_name=None,
        file_type=None, file_format='tmp', file_mode='local', file_variable='RH', file_io='derived',
        file_deps=[airt_data, qv_data, td_data],
        variable_template={
            "dims_geo": {"longitude": "longitude", "latitude": "latitude", "step": "time"},
            "vars_data": {"rh": "relative_humidity"}
        },
        time_signature='period',
        time_reference=time_data_start, time_period=time_data_length, time_freq='h', time_direction='forward',
        logger=logging_handle
    )

    # Wind U component file and folder names
    file_name = fill_string(
        alg_variables_application['data_source']['wind_u']['file_name'],
        time_source=time_format_file)
    folder_name = fill_string(
        alg_variables_application['data_source']['wind_u']['path'],
        path_time_source=time_format_folder)

    # Wind U component handler
    wind_u_data = DataLocal(
        path=folder_name, file_name=file_name,
        file_type='grid_3d', file_format='grib', file_mode='local', file_variable='WIND_U', file_io='input',
        variable_template={
            "dims_geo": {"longitude": "longitude", "latitude": "latitude", "step": "time"},
            "vars_data": {"u10": "wind_u"}
        },
        time_signature='period',
        time_reference=time_data_start, time_period=time_data_length, time_freq='h', time_direction='forward',
        logger=logging_handle, message=False
    )

    # Wind V component file and folder names
    file_name = fill_string(
        alg_variables_application['data_source']['wind_v']['file_name'],
        time_source=time_format_file)
    folder_name = fill_string(
        alg_variables_application['data_source']['wind_v']['path'],
        path_time_source=time_format_folder)

    # Wind V component handler
    wind_v_data = DataLocal(
        path=folder_name, file_name=file_name,
        file_type='grid_3d', file_format='grib', file_mode='local', file_variable='WIND_V', file_io='input',
        variable_template={
            "dims_geo": {"longitude": "longitude", "latitude": "latitude", "step": "time"},
            "vars_data": {"v10": "wind_v"}
        },
        time_signature='period',
        time_reference=time_data_start, time_period=time_data_length, time_freq='h', time_direction='forward',
        logger=logging_handle, message=False
    )

    # Wind Speed handler (derived)
    wind_speed_data = DataLocal(
        path=None,
        file_name=None,
        file_type=None, file_format='tmp', file_mode='local', file_variable='WIND_SPEED', file_io='derived',
        file_deps=[wind_u_data, wind_v_data],
        variable_template={
            "dims_geo": {"longitude": "longitude", "latitude": "latitude", "step": "time"},
            "vars_data": {"ws": "wind_speed"}
        },
        time_signature='period',
        time_reference=time_data_start, time_period=time_data_length, time_freq='h', time_direction='forward',
        logger=logging_handle
    )

    # Incoming Radiation handler (derived)
    inc_rad_data = DataLocal(
        path=None,
        file_name=None,
        file_type=None, file_format='tmp', file_mode='local', file_variable='INC_RAD', file_io='derived',
        file_deps=[rain_data],
        variable_template={
            "dims_geo": {"longitude": "longitude", "latitude": "latitude", "step": "time"},
            "vars_data": {"inc_rad": "incoming_radiation"}
        },
        time_signature='period',
        time_reference=time_data_start, time_period=time_data_length, time_freq='h', time_direction='forward',
        logger=logging_handle
    )

    # Output data file and folder names
    file_name = fill_string(
        alg_variables_application['data_destination']['file_name'],
        time_destination="%Y%m%d%H%M")
    folder_name = fill_string(
        alg_variables_application['data_destination']['path'],
        path_time_destination="%Y/%m/%d/%H%M")

    # Output data handler
    output_data = DataLocal(
        path=folder_name, file_name=file_name,
        time_signature='step',
        file_format='netcdf', file_type='hmc', file_mode='local',
        file_variable=['RAIN', 'AIR_T', 'RH','INC_RAD','WIND_SPEED'], file_io='output',
        variable_template={
            "dims_geo": {"longitude": "west_east", "latitude": "south_north", "time": "time"},
            "coord_geo": {"Longitude": "longitude", "Latitude": "latitude"},
            "vars_data": {
                "rain": "Rain",
                "air_temperature": "AirTemperature",
                "relative_humidity": "RelHumidity",
                "incoming_radiation": "IncRadiation",
                "wind_speed": "Wind"}
        },
        time_period=1, time_format='%Y%m%d%H%M',
        logger=logging_handle, message=False
    )
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## ORCHESTRATOR MANAGEMENT
    # orchestrator settings
    orc_process = Orchestrator.multi_variable(
        data_package_in=[rain_data, airt_data, rh_data, inc_rad_data, wind_speed_data],
        data_package_out=output_data,
        data_ref=geo_data,
        priority=['RH'],
        configuration=configuration['WORKFLOW'],
        logger=logging_handle
    )
    # orchestrator exec
    orc_process.run(time=pd.date_range(start=time_anls_start, end=time_anls_end, freq='h'))
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## INFO END
    # info algorithm (end)
    alg_time_elapsed = round(time.time() - start_time, 1)

    logging_handle.info_header(alg_name + ' (Version: ' + alg_version + ' Release_Date: ' + alg_release + ')', blank_before=True)
    logging_handle.info_header('TIME ELAPSED: ' + str(alg_time_elapsed) + ' seconds')
    logging_handle.info_header('... END')
    logging_handle.info_header('Bye, Bye')
    logging_handle.info_header(LoggingManager.rule_line("=", 78))
    # ------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to update time reference
def update_time_reference(
        time_period_start, time_period_end, time_start_anls, time_end_anls):

    # Update period start
    if time_period_start != time_start_anls:
        new_start = max(time_period_start, time_start_anls)

        if new_start >= time_end_anls:
            raise ValueError(
                f"Invalid update: start {new_start} is not < analysis end {time_end_anls}"
            )
        time_period_start = new_start

    # Update period end
    if time_period_end != time_end_anls:
        new_end = min(time_period_end, time_end_anls)

        if new_end <= time_start_anls:
            raise ValueError(
                f"Invalid update: end {new_end} is not > analysis start {time_start_anls}"
            )
        time_period_end = new_end

    return time_period_start, time_period_end
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# call entrypoint
if __name__ == '__main__':
    main()
# ----------------------------------------------------------------------------------------------------------------------
