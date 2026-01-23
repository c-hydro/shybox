#!/usr/bin/python3
"""
SHYBOX - Snow HYdro toolBOX - WORKFLOW MERGER BY TIME BASE

__date__ = '20250403'
__version__ = '1.0.0'
__author__ =
    'Fabio Delogu (fabio.delogu@cimafoundation.org),
     Francesco Avanzi (francesco.avanzi@cimafoundation.org)'
__library__ = 'shybox'

General command line:
python app_workflow_main.py -settings_file configuration.json -time "YYYY-MM-DD HH:MM"

Examples of environment variables declarations:
DOMAIN_NAME='italy';
TIME_START="'2025-01-24 00:00'";
TIME_END="'2025-01-24 05:00'";
PATH_SRC='/home/fabio/Desktop/shybox/dset/itwater';
PATH_DST='/home/fabio/Desktop/shybox/dset/itwater';
PATH_LOG=$HOME/dataset_base/log/;
PATH_TMP=$HOME/dataset_base/tmp/

Version(s):
20250403 (1.0.0) --> Beta release for shybox package
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries

# libraries
import logging
import os
import time

import pandas as pd

from shybox.orchestrator_toolkit.orchestrator_handler_grid import OrchestratorHandler as Orchestrator

from shybox.generic_toolkit.old.lib_utils_args_OLD import get_args
from shybox.generic_toolkit.old.lib_utils_logging_OLD import set_logging_stream
from shybox.time_toolkit.lib_utils_time import select_time_range, select_time_format

from shybox.default.lib_default_args_OLD import logger_name, logger_format, logger_arrow
from shybox.default.lib_default_args_OLD import collector_data

from shybox.runner_toolkit.old.settings.driver_app_settings import DrvSettings

from shybox.orchestrator_toolkit.orchestrator_handler_grid import OrchestratorGrid as Orchestrator
from shybox.dataset_toolkit.dataset_handler_local import DataLocal

# fx imported in the PROCESSES (will be used in the global variables PROCESSES) --> DO NOT REMOVE

# set logger
logger_stream = logging.getLogger(logger_name)
logger_stream.setLevel(logging.ERROR)
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# algorithm information
project_name = 'shybox'
alg_name = 'Workflow for datasets merger by time base configuration'
alg_type = 'Package'
alg_version = '1.0.0'
alg_release = '2025-04-03'
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
    configuration= {
        "WORKFLOW_DSET_01": {
            "options": {
                "intermediate_output": "Tmp",
                "tmp_dir": "tmp"
            },
            "process_list": {
                "rain_eff": [
                    {"function": "merge_data_by_time"}
                ]
            }
        },
        "WORKFLOW_DSET_02": {
            "options": {
                "intermediate_output": "Tmp",
                "tmp_dir": "tmp"
            },
            "process_list": {
                "snow_mask": [
                    {"function": "merge_data_by_time"}
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

        start_data_time = select_time_format(start_data_time, time_format='%Y-%m-%d %H:%M')
        end_data_time = select_time_format(end_data_time, time_format='%Y-%m-%d %H:%M')

        # get data source settings
        data_src_settings_01 = alg_variables_application['data_source']['dset_01']
        # organize data source obj
        data_src_obj_01 = DataLocal(
            path=data_src_settings_01['path'],
            file_name=data_src_settings_01['file_name'],
            file_format="geotiff", file_mode=None, file_variable=['rain_eff'],
            file_template={
                "dims_geo": {"X": "longitude", "Y": "latitude", "time": "time"},
                'coords_geo': {'Longitude': 'longitude', 'Latitude': 'latitude'},
                "vars_data": {"rain_eff": "rain_eff"}
            },
            time_signature='current',
            time_reference=start_data_time, time_period=1, time_freq='h', time_direction='forward',
        )

        # get data destination settings
        data_dst_settings_01 = alg_variables_application['data_destination']['dset_01']
        # organize data destination obj
        data_dst_obj_01 = DataLocal(
            path=data_dst_settings_01['path'],
            file_name=data_dst_settings_01['file_name'], time_signature='start',
            file_format='netcdf', file_type='itwater', file_mode='grid',
            file_variable=data_dst_settings_01['variable'],
            file_template={
                "dims_geo": {"longitude": "longitude", "latitude": "latitude"},
                "vars_geo": {"longitude": "longitude", "latitude": "longitude"},
                "vars_data": data_dst_settings_01['vars_data']
            },
            time_period=24, time_format='%Y%m%d%H%M')

        # get data source settings
        data_src_settings_02 = alg_variables_application['data_source']['dset_02']
        # organize data source obj
        data_src_obj_02 = DataLocal(
            path=data_src_settings_02['path'],
            file_name=data_src_settings_02['file_name'],
            file_format="geotiff", file_mode=None, file_variable=['snow_mask'],
            file_template={
                "dims_geo": {"X": "longitude", "Y": "latitude", "time": "time"},
                'coords_geo': {'Longitude': 'longitude', 'Latitude': 'latitude'},
                "vars_data": {"snow_mask": "snow_mask"}
            },
            time_signature='current',
            time_reference=start_data_time, time_period=1, time_freq='h', time_direction='forward',
        )

        # get data destination settings
        data_dst_settings_02 = alg_variables_application['data_destination']['dset_02']
        # organize data destination obj
        data_dst_obj_02 = DataLocal(
            path=data_dst_settings_02['path'],
            file_name=data_dst_settings_02['file_name'], time_signature='start',
            file_format='netcdf', file_type='itwater', file_mode='grid',
            file_variable=data_dst_settings_02['variable'],
            file_template={
                "dims_geo": {"longitude": "longitude", "latitude": "latitude"},
                "vars_geo": {"longitude": "longitude", "latitude": "longitude"},
                "vars_data": data_dst_settings_02['vars_data']
            },
            time_period=24, time_format='%Y%m%d%H%M')

        # orchestrator multi time(s) settings
        orc_process_01 = Orchestrator.multi_time(
            data_package_in=[data_src_obj_01], data_package_out=[data_dst_obj_01],
            data_ref=geo_data,
            configuration=configuration['WORKFLOW_DSET_01']
        )

        # orchestrator multi time(s) execution
        orc_process_01.run(time=pd.date_range(start_data_time, end_data_time, freq='h'),
                        group='by_time')

        # orchestrator multi time(s) settings
        orc_process_02 = Orchestrator.multi_time(
            data_package_in=[data_src_obj_02], data_package_out=[data_dst_obj_02],
            data_ref=geo_data,
            configuration=configuration['WORKFLOW_DSET_02']
        )

        # orchestrator multi time(s) execution
        orc_process_02.run(time=pd.date_range(start_data_time, end_data_time, freq='h'),
                        group='by_time')

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



