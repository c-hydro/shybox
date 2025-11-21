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
TIME_PERIOD=24;
TIME_START='2024-10-17 06:00';
TIME_END='2024-10-17 06:00';
PATH_GEO='/home/fabio/Desktop/shybox/dset/case_study_destine/data/data_static/gridded/';
PATH_SRC='/home/fabio/Desktop/shybox/dset/case_study_destine/data/data_dynamic/source/icon/';
PATH_DST='/home/fabio/Desktop/shybox/dset/case_study_destine/data/data_dynamic/destination/icon/';
PATH_LOG=/home/fabio/Desktop/shybox/dset/case_study_destine/log/

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

from shybox.config_toolkit.arguments_handler import ArgumentsManager
from shybox.config_toolkit.config_handler import ConfigManager

from shybox.processing_toolkit.lib_proc_merge import merge_data_by_ref
from shybox.processing_toolkit.lib_proc_mask import mask_data_by_ref, mask_data_by_limits
from shybox.processing_toolkit.lib_proc_interp import interpolate_data
from shybox.processing_toolkit.lib_proc_compute_humidity import compute_data_rh
from shybox.processing_toolkit.lib_proc_compute_wind import compute_data_wind_speed
from shybox.processing_toolkit.lib_proc_compute_radiation import compute_data_astronomic_radiation
from shybox.processing_toolkit.lib_proc_compute_temperature import convert_temperature_units

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
    alg_args_obj = ArgumentsManager(settings_folder=os.path.dirname(os.path.realpath(__file__)))
    alg_args_file, alg_args_time = alg_args_obj.get()

    # crete configuration object
    alg_cfg_obj = ConfigManager.from_source(
        alg_args_file,
        root_key="configuration",
        auto_validate=True, auto_fill_lut=True,
        flat_variables=True, flat_key_mode='value')
    # view lut section
    alg_cfg_lut = alg_cfg_obj.get_section(section='lut')
    alg_cfg_obj.view(section=alg_cfg_lut, table_name='lut', table_print=True)

    # get application section
    alg_cfg_application = alg_cfg_obj.get_section(section='application')
    # fill application section
    alg_cfg_application = alg_cfg_obj.fill_obj_from_lut(
        section=alg_cfg_application,
        resolve_time_placeholders=False, time_keys=('time_start', 'time_end', 'time_period'),
        template_keys=('file_time_destination',)
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
    # set logging instance
    LoggingManager.setup(
        logger_folder=alg_cfg_application['log']['path'],
        logger_file=alg_cfg_application['log']['file_name'],
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
    ## TIME MANAGEMENT (GLOBAL)
    # Time generic configuration
    alg_reference_time = select_time_range(
        time_start=alg_cfg_application['time']['start'],
        time_end=alg_cfg_application['time']['end'],
        time_frequency=alg_cfg_application['time']['frequency'],
        ensure_range=False, flat_if_single=True)
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## GEO MANAGEMENT
    # geographic reference
    geo_data = DataLocal(
        path=alg_cfg_application['geo']['terrain']['path'],
        file_name=alg_cfg_application['geo']['terrain']['file_name'],
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
    # time dataset info
    time_data_length = int(alg_cfg_application['time']['dataset'])
    time_data_start = select_time_format(alg_reference_time, time_format='%Y-%m-%d %H:%M')
    # time analysis info
    time_anls_length = int(alg_cfg_application['time']['dataset'])
    time_anls_period = select_time_range(
        time_start=alg_reference_time, time_period=time_anls_length, time_frequency='h')
    time_anls_start = select_time_format(time_anls_period[0], time_format='%Y-%m-%d %H:%M')
    time_anls_end = select_time_format(time_anls_period[-1], time_format='%Y-%m-%d %H:%M')

    # update the applications obj
    alg_cfg_application = alg_cfg_obj.fill_section_with_times(
        alg_cfg_application,
        time_values={
            'file_time_destination': "",
            'path_time_source': alg_reference_time, 'file_time_source': alg_reference_time,
            'path_time_destination': alg_reference_time, 'path_time_run': alg_reference_time}
    )

    # view application section
    alg_cfg_obj.view(section=alg_cfg_application, table_name='application [time info]', table_print=True)
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## DATASETS MANAGEMENT
    # Rain handler
    rain_data = DataLocal(
        path=alg_cfg_application['data_source']['RAIN']['path'],
        file_name=alg_cfg_application['data_source']['RAIN']['file_name'],
        file_type='grid_3d', file_format='grib', file_mode='local', file_variable='RAIN', file_io='input',
        variable_template={
            "dims_geo": {"longitude": "longitude", "latitude": "latitude", "step": "time"},
            "vars_data": {"tp": "rain"}
        },
        time_signature='period',
        time_reference=time_data_start, time_period=time_data_length, time_freq='h', time_direction='forward',
        logger=logging_handle, message=False
    )

    # Air Temperature handler
    airt_data = DataLocal(
        path=alg_cfg_application['data_source']['AIR_T']['path'],
        file_name=alg_cfg_application['data_source']['AIR_T']['file_name'],
        file_type='grid_3d', file_format='grib', file_mode='local', file_variable='AIR_T', file_io='input',
        variable_template={
            "dims_geo": {"longitude": "longitude", "latitude": "latitude", "step": "time"},
            "vars_data": {"t2m": "air_temperature"}
        },
        time_signature='period',
        time_reference=time_data_start, time_period=time_data_length, time_freq='h', time_direction='forward',
        logger=logging_handle, message=False
    )

    # Specific humidity handler
    qv_data = DataLocal(
        path=alg_cfg_application['data_source']['QV']['path'],
        file_name=alg_cfg_application['data_source']['QV']['file_name'],
        file_type='grid_3d', file_format='grib', file_mode='local', file_variable='QV', file_io='input',
        variable_template={
            "dims_geo": {"longitude": "longitude", "latitude": "latitude", "step": "time"},
            "vars_data": {"qv": "specific_humidity"}
        },
        time_signature='period',
        time_reference=time_data_start, time_period=time_data_length, time_freq='h', time_direction='forward',
        logger=logging_handle
    )

    # Dew Point Temperature handler
    td_data = DataLocal(
        path=alg_cfg_application['data_source']['DEW_POINT']['path'],
        file_name=alg_cfg_application['data_source']['DEW_POINT']['file_name'],
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

    # Wind U component handler
    wind_u_data = DataLocal(
        path=alg_cfg_application['data_source']['WIND_U']['path'],
        file_name=alg_cfg_application['data_source']['WIND_U']['file_name'],
        file_type='grid_3d', file_format='grib', file_mode='local', file_variable='WIND_U', file_io='input',
        variable_template={
            "dims_geo": {"longitude": "longitude", "latitude": "latitude", "step": "time"},
            "vars_data": {"u10": "wind_u"}
        },
        time_signature='period',
        time_reference=time_data_start, time_period=time_data_length, time_freq='h', time_direction='forward',
        logger=logging_handle, message=False
    )

    # Wind V component handler
    wind_v_data = DataLocal(
        path=alg_cfg_application['data_source']['WIND_V']['path'],
        file_name=alg_cfg_application['data_source']['WIND_V']['file_name'],
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

    # Output data handler
    output_data = DataLocal(
        path=alg_cfg_application['data_destination']['path'],
        file_name=alg_cfg_application['data_destination']['file_name'],
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
        priority=['WIND_SPEED'],
        configuration=alg_cfg_workflow,
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
