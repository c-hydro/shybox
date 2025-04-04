#!/usr/bin/python3
"""
SHYBOX - Snow HYdro toolBOX - WORKFLOW CONVERTER BASE

__date__ = '20250310'
__version__ = '1.0.0'
__author__ =
    'Fabio Delogu (fabio.delogu@cimafoundation.org),
     Francesco Avanzi (francesco.avanzi@cimafoundation.org)'
__library__ = 'shybox'

General command line:
python app_workflow_main.py -settings_file configuration.json -time "YYYY-MM-DD HH:MM"

Examples of environment variables declarations:
DOMAIN_NAME='marche';
TIME_START='1981-01-01';
TIME_END='1981-01-03';
PATH_SRC='/home/fabio/Desktop/shybox/dset/itwater';
PATH_DST='/home/fabio/Desktop/shybox/dset/itwater'
PATH_LOG=$HOME/dataset_base/log/;

Version(s):
20250310 (1.0.0) --> Beta release for shybox package
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import os
import time

import numpy as np
import pandas as pd

from shybox.generic_toolkit.lib_utils_args import get_args
from shybox.generic_toolkit.lib_utils_logging import set_logging_stream
from shybox.generic_toolkit.lib_utils_time import select_time_range, select_time_format
from shybox.generic_toolkit.lib_utils_string import fill_string

from shybox.generic_toolkit.lib_default_args import logger_name, logger_format, logger_arrow
from shybox.generic_toolkit.lib_default_args import collector_data

from shybox.runner_toolkit.settings.driver_app_settings import DrvSettings
from shybox.runner_toolkit.time.driver_app_time import DrvTime

from shybox.orchestrator_toolkit.orchestrator_handler_base import OrchestratorHandler as Orchestrator
from shybox.dataset_toolkit.dataset_handler_local import DataLocal

# fx imported in the PROCESSES (will be used in the global variables PROCESSES) --> DO NOT REMOVE
from shybox.processing_toolkit.lib_proc_mask import mask_data_by_ref, mask_data_by_limits
from shybox.processing_toolkit.lib_proc_interp import interpolate_data

# set logger
logger_stream = logging.getLogger(logger_name)
logger_stream.setLevel(logging.ERROR)
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# algorithm information
project_name = 'shybox'
alg_name = 'Workflow for datasets converter base configuration'
alg_type = 'Package'
alg_version = '1.0.0'
alg_release = '2025-03-10'
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

    # get variables application
    alg_variables_application = driver_settings.get_variable_by_tag('application')
    alg_variables_application = driver_settings.fill_variable_by_dict(alg_variables_application, alg_variables_settings)

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
    # configuration workflow
    configuration = {
        "WORKFLOW": {
            "options": {
                "intermediate_output": "Tmp",
                "tmp_dir": alg_variables_settings['path_tmp']
            },
            "process_list": {
                "rain": [
                    {"function": "interpolate_data", "method": 'nn', "max_distance": 25000, "neighbours": 7,
                     "fill_value": np.nan},
                    {"function": "mask_data_by_ref", "ref_value": -9999, "mask_no_data": np.nan}
                ],
                "air_t": [
                    {"function": "interpolate_data", "method":'nn', "max_distance": 25000, "neighbours": 7,
                     "fill_value": np.nan},
                    {"function": "mask_data_by_ref", "ref_value": -9999, "mask_no_data": np.nan}
                ],
                "rh": [
                    {"function": "interpolate_data", "method":'nn', "max_distance": 22000, "neighbours": 7,
                     "fill_value": np.nan},
                    {"function": "mask_data_by_ref", "ref_value": -9999, "mask_no_data": np.nan}
                ],
                "inc_rad": [
                    {"function": "interpolate_data", "method": 'nn', "max_distance": 22000, "neighbours": 7,
                     "fill_value": np.nan},
                    {"function": "mask_data_by_ref", "ref_value": -9999, "mask_no_data": np.nan}
                ]
            }
        }
    }
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # method to organize time information
    alg_sim_time = select_time_range(
        time_start=alg_variables_application['time']['start'],
        time_end=alg_variables_application['time']['end'],
        time_frequency=alg_variables_application['time']['frequency'])
    alg_sim_time = select_time_format(alg_sim_time, time_format=alg_variables_application['time']['format'])
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # define geo obj
    geo_data = DataLocal(
        path=alg_variables_application['geo']['terrain']['path'],
        file_name=alg_variables_application['geo']['terrain']['file_name'],
        file_mode='grid', file_variable='terrain',
        file_template={
            "dims_geo": {"x": "longitude", "y": "latitude"},
            "vars_geo": {"x": "longitude", "y": "latitude"}
        },
        time_signature=None
    )
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # time iteration(s)
    for sim_time in alg_sim_time:

        # time source data
        alg_data_time = select_time_range(
            time_start=sim_time,
            time_period=24,
            time_frequency='h')
        start_data_time, end_data_time = alg_data_time[0], alg_data_time[-1]
        period_data_time = len(alg_data_time)

        start_data_time = select_time_format(start_data_time, time_format='%Y-%m-%d %H:%M')
        end_data_time = select_time_format(end_data_time, time_format='%Y-%m-%d %H:%M')

        # precipitation source data
        file_name = fill_string(
            alg_variables_application['data_source']['rain']['file_name'],
            time_source=sim_time, domain_name=alg_variables_application['info']['domain_name'])

        rain_data = DataLocal(
            path=alg_variables_application['data_source']['rain']['path'],
            file_name=file_name,
            file_format=None, file_mode=None, file_variable='rain',
            file_template={
                "dims_geo": {"lon": "longitude", "lat": "latitude", "nt": "time"},
                "vars_data": {"Rain": "rain"}
            },
            time_signature='period',
            time_reference=start_data_time, time_period=period_data_time, time_freq='h', time_direction='forward',
        )

        # air temperature source data
        file_name = fill_string(
            alg_variables_application['data_source']['air_t']['file_name'],
            time_source=sim_time, domain_name=alg_variables_application['info']['domain_name'])

        airt_data = DataLocal(
            path=alg_variables_application['data_source']['air_t']['path'],
            file_name=file_name,
            file_format=None, file_mode=None, file_variable='air_t',
            file_template={
                "dims_geo": {"lon": "longitude", "lat": "latitude", "nt": "time"},
                "vars_data": {"Tair": "air_temperature"}
            },
            time_signature='period',
            time_reference=start_data_time, time_period=period_data_time, time_freq='h', time_direction='forward',
        )

        # relative humidity source data
        file_name = fill_string(
            alg_variables_application['data_source']['rh']['file_name'],
            time_source=sim_time, domain_name=alg_variables_application['info']['domain_name'])
        rh_data = DataLocal(
            path=alg_variables_application['data_source']['rh']['path'],
            file_name=file_name,
            file_format=None, file_mode=None, file_variable='rh',
            file_template={
                "dims_geo": {"lon": "longitude", "lat": "latitude", "nt": "time"},
                "vars_data": {"RH": "relative_humidity"}
            },
            time_signature='period',
            time_reference=start_data_time, time_period=period_data_time, time_freq='h', time_direction='forward',
        )

        # relative humidity source data
        file_name = fill_string(
            alg_variables_application['data_source']['inc_rad']['file_name'],
            time_source=sim_time, domain_name=alg_variables_application['info']['domain_name'])
        inc_rad_data = DataLocal(
            path=alg_variables_application['data_source']['inc_rad']['path'],
            file_name=file_name,
            file_format=None, file_mode=None, file_variable='inc_rad',
            file_template={
                "dims_geo": {"lon": "longitude", "lat": "latitude", "nt": "time"},
                "vars_data": {"Rad": "incoming_radiation"}
            },
            time_signature='period',
            time_reference=start_data_time, time_period=period_data_time, time_freq='h', time_direction='forward',
        )


        # destination data
        file_name = fill_string(
            alg_variables_application['data_destination']['file_name'],
            time_destination="%Y%m%d%H%M", domain_name=alg_variables_application['info']['domain_name'])

        output_data = DataLocal(
            path=alg_variables_application['data_destination']['path'],
            file_name=file_name,
            time_signature='step',
            file_format='netcdf', file_mode='grid', file_variable=['rain', 'air_t', 'rh', 'inc_rad'],
            file_type=alg_variables_application['data_destination']['type'],
            file_template={
                "dims_geo": {"longitude": "X", "latitude": "Y", "time": "time"},
                "vars_geo": {"longitude": "X", "latitude": "Y"},
                "vars_data": {
                    "rain": "Rain",
                    "air_temperature": "AirTemperature",
                    "relative_humidity": "RelHumidity",
                    "incoming_radiation": "IncRadiation"}
            },
            time_period=1, time_format='%Y%m%d%H%M')

        # orchestrator settings
        orc_process = Orchestrator.multi_variable(
            data_package_in=[rain_data, airt_data, rh_data, inc_rad_data], data_package_out=output_data,
            data_ref=geo_data,
            configuration=configuration['WORKFLOW']
        )
        # orchestrator exec
        orc_process.run(time=pd.date_range(start=start_data_time, end=end_data_time, freq='h'))

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
    # run script
    main()
# ----------------------------------------------------------------------------------------------------------------------
