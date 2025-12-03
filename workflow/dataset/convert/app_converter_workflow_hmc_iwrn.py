#!/usr/bin/python3
"""
SHYBOX PACKAGE - APP PROCESSING DATASET MAIN

__date__ = '20251116'
__version__ = '1.1.1'
__author__ =
    'Fabio Delogu (fabio.delogu@cimafoundation.org),
     Andrea Libertino (andrea.libertino@cimafoundation.org)'
__library__ = 'shybox'

General command line:
python app_converter_workflow_main.py -settings_file configuration.json -time "YYYY-MM-DD HH:MM"

Examples of environment variables declarations:
DOMAIN_NAME='shebele';
TIME_START='2000-01-01';
TIME_END='2000-03-01';
PATH_GEO='/home/fabio/Desktop/shybox/dset/case_study_iwrn/static/gridded/';
PATH_SRC='/home/fabio/Desktop/shybox/dset/case_study_iwrn/dynamic/source/';
PATH_DST='/home/fabio/Desktop/shybox/dset/case_study_iwrn/dynamic/destination'
PATH_LOG=/home/fabio/Desktop/shybox/dset/case_study_iwrn/log/;

Version(s):
20251116 (1.1.0) --> Release for shybox package (hmc datasets converter base configuration)
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import time
import os

import numpy as np
import pandas as pd

from shybox.generic_toolkit.lib_utils_args import get_args
from shybox.default.lib_default_args import collector_data
from shybox.time_toolkit.lib_utils_time import (select_time_range, select_time_format,
                                                get_time_length, get_time_bounds)
from shybox.generic_toolkit.lib_utils_string import fill_string

from shybox.runner_toolkit.old.settings.driver_app_settings import DrvSettings

from shybox.orchestrator_toolkit.orchestrator_handler_base import OrchestratorHandler as Orchestrator

from shybox.dataset_toolkit.dataset_handler_local import DataLocal
from shybox.logging_toolkit.logging_handler import LoggingManager
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# algorithm information
project_name = 'shybox'
alg_name = 'Application for processing datasets'
alg_type = 'Package'
alg_version = '1.1.0'
alg_release = '2025-11-16'
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
        name="shybox_algorithm_converter_iwrn",
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
                "LAI": [
                    {"function": "interpolate_data", "method": 'nn', "max_distance": 25000, "neighbours": 7,
                     "fill_value": np.nan},
                    {"function": "mask_data_by_ref", "ref_value": -9999, "mask_no_data": np.nan}
                ],
                "AIR_T": [
                    {"function": "interpolate_data", "method":'nn', "max_distance": 25000, "neighbours": 7,
                     "fill_value": np.nan},
                    {"function": "mask_data_by_ref", "ref_value": -9999, "mask_no_data": np.nan}
                ],
                "RH": [
                    {"function": "interpolate_data", "method":'nn', "max_distance": 22000, "neighbours": 7,
                     "fill_value": np.nan},
                    {"function": "mask_data_by_ref", "ref_value": -9999, "mask_no_data": np.nan}
                ],
                "SR": [
                    {"function": "interpolate_data", "method":'nn', "max_distance": 22000, "neighbours": 7,
                     "fill_value": np.nan},
                    {"function": "mask_data_by_ref", "ref_value": -9999, "mask_no_data": np.nan}
                ],
                "TP": [
                    {"function": "interpolate_data", "method":'nn', "max_distance": 22000, "neighbours": 7,
                     "fill_value": np.nan},
                    {"function": "mask_data_by_ref", "ref_value": -9999, "mask_no_data": np.nan}
                ],
                "WIND": [
                    {"function": "interpolate_data", "method":'nn', "max_distance": 22000, "neighbours": 7,
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
    alg_generic_time = select_time_range(
        time_start=alg_variables_application['time']['start'],
        time_end=alg_variables_application['time']['end'],
        time_frequency=alg_variables_application['time']['frequency'], ensure_range=True)
    alg_time_generic = select_time_format(alg_generic_time, time_format=alg_variables_application['time']['format'])
    # Time orchestrator configuration
    alg_orchestrator_time = select_time_range(
        time_start=alg_variables_application['time']['start'],
        time_end=alg_variables_application['time']['end'],
        time_frequency='h', ensure_range=False)
    alg_time_orc = select_time_format(alg_orchestrator_time)
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
    # iterate over simulation time
    for time_step in alg_time_generic:

        # ------------------------------------------------------------------------------------------------------------------
        ## TIME MANAGEMENT (STEP)
        # time source data
        time_period_anls = select_time_range(time_start=time_step, time_period=24, time_frequency='h')
        time_start_anls = select_time_format(time_period_anls[0], time_format='%Y-%m-%d %H:%M')
        time_end_anls = select_time_format(time_period_anls[-1], time_format='%Y-%m-%d %H:%M')

        time_data_len = get_time_length(
            time_step, period_freq='MS', resolution='h', type=int)

        time_data_bounds = get_time_bounds(time_step, freq='MS')
        time_data_start = select_time_format(time_data_bounds[0], time_format='%Y-%m-%d %H:%M')
        time_data_end = select_time_format(time_data_bounds[-1], time_format='%Y-%m-%d %H:%M')
        time_file_vars = select_time_format(time_data_bounds[0], time_format='%m%Y')

        path_time_lai = select_time_format(time_period_anls[0], time_format='%m/%d')
        time_data_lai = select_time_format(time_period_anls[0], time_format='%Y-%m-%d 12:00')
        time_file_lai = select_time_format(time_period_anls[0], time_format='%m%d')

        time_start_orc, time_end_orc = alg_time_orc[0], alg_time_orc[-1]

        time_start_orc, time_end_orc = update_time_reference(
            time_start_orc, time_end_orc, time_start_anls, time_end_anls)
        # ------------------------------------------------------------------------------------------------------------------

        # ------------------------------------------------------------------------------------------------------------------
        ## DATASETS MANAGEMENT
        # LAI data source
        file_name = fill_string(
            alg_variables_application['data_source']['lai']['file_name'],
            time_lai=time_file_lai, path_lai=path_time_lai)

        path_name = fill_string(
            alg_variables_application['data_source']['lai']['path'],
            time_lai=time_file_lai, path_lai=path_time_lai)

        lai_data = DataLocal(
            path=path_name,
            file_name=file_name,
            file_type='grid_2d', file_format='tiff', file_mode='local', file_variable='LAI', file_io='input',
            variable_template={
                "dims_geo": {"x": "longitude", "y": "latitude"},
                "vars_data": {"lai": "leaf_area_index"}
            },
            time_signature='unique',
            time_reference=time_data_lai, time_period=1, time_freq='h', time_direction='single',
            logger=logging_handle, message=False
        )

        # Air Temperature
        file_name = fill_string(
            alg_variables_application['data_source']['air_t']['file_name'],
            time_source=time_file_vars)

        airt_data = DataLocal(
            path=alg_variables_application['data_source']['air_t']['path'],
            file_name=file_name,
            file_type='grid_3d', file_format='netcdf', file_mode='local', file_variable='AIR_T', file_io='input',
            variable_template={
                "dims_geo": {"x": "longitude", "y": "latitude", "time": "time"},
                "vars_data": {"temperature": "air_temperature"}
            },
            time_signature='period',
            time_reference=time_data_start, time_period=time_data_len, time_freq='h', time_direction='forward',
            logger=logging_handle, message=False
        )

        # Relative Humidity
        file_name = fill_string(
            alg_variables_application['data_source']['rh']['file_name'],
            time_source=time_file_vars)

        rh_data = DataLocal(
            path=alg_variables_application['data_source']['rh']['path'],
            file_name=file_name,
            file_type='grid_3d', file_format='netcdf', file_mode='local', file_variable='RH', file_io='input',
            variable_template={
                "dims_geo": {"x": "longitude", "y": "latitude", "time": "time"},
                "vars_data": {"relative_humidity": "relative_humidity"}
            },
            time_signature='period',
            time_reference=time_data_start, time_period=time_data_len, time_freq='h', time_direction='forward',
            logger=logging_handle, message=False
        )

        # Solar Radiation
        file_name = fill_string(
            alg_variables_application['data_source']['inc_rad']['file_name'],
            time_source=time_file_vars)

        sr_data = DataLocal(
            path=alg_variables_application['data_source']['inc_rad']['path'],
            file_name=file_name,
            file_type='grid_3d', file_format='netcdf', file_mode='local', file_variable='SR', file_io='input',
            variable_template={
                "dims_geo": {"x": "longitude", "y": "latitude", "time": "time"},
                "vars_data": {"solar_radiation": "solar_radiation"}
            },
            time_signature='period',
            time_reference=time_data_start, time_period=time_data_len, time_freq='h', time_direction='forward',
            logger=logging_handle, message=False
        )

        # Total Precipitation
        file_name = fill_string(
            alg_variables_application['data_source']['rain']['file_name'],
            time_source=time_file_vars)

        tp_data = DataLocal(
            path=alg_variables_application['data_source']['rain']['path'],
            file_name=file_name,
            file_type='grid_3d', file_format='netcdf', file_mode='local', file_variable='TP', file_io='input',
            variable_template={
                "dims_geo": {"x": "longitude", "y": "latitude", "time": "time"},
                "vars_data": {"total_precipitation": "total_precipitation"}
            },
            time_signature='period',
            time_reference=time_data_start, time_period=time_data_len, time_freq='h', time_direction='forward',
            logger=logging_handle, message=False
        )

        # Wind
        file_name = fill_string(
            alg_variables_application['data_source']['wind']['file_name'],
            time_source=time_file_vars)

        wind_data = DataLocal(
            path=alg_variables_application['data_source']['wind']['path'],
            file_name=file_name,
            file_type='grid_3d', file_format='netcdf', file_mode='local', file_variable='WIND', file_io='input',
            variable_template={
                "dims_geo": {"x": "longitude", "y": "latitude", "time": "time"},
                "vars_data": {"m10_wind": "wind"}
            },
            time_signature='period',
            time_reference=time_data_start, time_period=time_data_len, time_freq='h', time_direction='forward',
            logger=logging_handle, message=False
        )

        # Output data destination
        file_name = fill_string(
            alg_variables_application['data_destination']['file_name'],
            time_destination="%Y%m%d%H%M")

        output_data = DataLocal(
            path=alg_variables_application['data_destination']['path'],
            file_name=file_name,
            time_signature='step',
            file_format='netcdf', file_type='hmc', file_mode='local', 
            file_variable=['LAI', 'AIR_T', 'RH','SR','TP','WIND'], file_io='output',
            variable_template={
                "dims_geo": {"longitude": "west_east", "latitude": "south_north", "time": "time"},
                "coord_geo": {"Longitude": "longitude", "Latitude": "latitude"},
                "vars_data": {
                    "leaf_area_index": "LAI",
                    "air_temperature": "AirTemperature",
                    "relative_humidity": "RelHumidity",
                    "solar_radiation": "IncRadiation",
                    "total_precipitation": "Rain",
                    "wind": "Wind"}
            },
            time_period=1, time_format='%Y%m%d%H%M',
            logger=logging_handle, message=False
        )
        # ------------------------------------------------------------------------------------------------------------------

        # ------------------------------------------------------------------------------------------------------------------
        ## ORCHESTRATOR MANAGEMENT
        # orchestrator settings
        orc_process = Orchestrator.multi_variable(
            data_package_in=[lai_data, airt_data, rh_data, sr_data, tp_data, wind_data],
            data_package_out=output_data,
            data_ref=geo_data,
            priority=['leaf_area_index'],
            configuration=configuration['WORKFLOW'],
            logger=logging_handle
        )
        # orchestrator exec
        orc_process.run(time=pd.date_range(start=time_start_orc, end=time_end_orc, freq='h'))
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
