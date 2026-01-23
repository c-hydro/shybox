#!/usr/bin/python3
"""
SHYBOX PACKAGE - APP PROCESSING DATASET MAIN - MERGER BY DOMAIN

__date__ = '20260122'
__version__ = '1.1.0'
__author__ =
    'Fabio Delogu (fabio.delogu@cimafoundation.org),
     Francesco Avanzi (francesco.avanzi@cimafoundation.org)'
__library__ = 'shybox'

General command line:
python app_merger_workflow_main.py -settings_file configuration.json -time "YYYY-MM-DD HH:MM"

Examples of environment variables declarations:
DOMAIN_NAME='italy';
TIME_START="'2025-01-24 00:00'";
TIME_END="'2025-01-24 05:00'";
PATH_SRC='/home/fabio/Desktop/shybox/dset/itwater';
PATH_DST='/home/fabio/Desktop/shybox/dset/itwater';
PATH_LOG=$HOME/dataset_base/log/;
PATH_TMP=$HOME/dataset_base/tmp/

Version(s):
20260122 (1.1.0) --> Refactor using class methods in shybox package
20250403 (1.0.0) --> Beta release for shybox package
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import os
import time

import numpy as np
import pandas as pd

from shybox.config_toolkit.arguments_handler import ArgumentsManager
from shybox.config_toolkit.config_handler import ConfigManager

from shybox.orchestrator_toolkit.orchestrator_handler_grid import OrchestratorHandler as Orchestrator
from shybox.logging_toolkit.logging_handler import LoggingManager

from shybox.time_toolkit.lib_utils_time import select_time_range, select_time_format
from shybox.generic_toolkit.lib_utils_string import fill_string

#from shybox.default.lib_default_args import logger_name, logger_format, logger_arrow
#from shybox.default.lib_default_args import collector_data

#from shybox.runner_toolkit.old.settings.driver_app_settings import DrvSettings

from shybox.orchestrator_toolkit.orchestrator_handler_grid import OrchestratorGrid as Orchestrator
from shybox.dataset_toolkit.dataset_handler_local import DataLocal

# fx imported in the PROCESSES (will be used in the global variables PROCESSES) --> DO NOT REMOVE

# set logger
#logger_stream = logging.getLogger(logger_name)
#logger_stream.setLevel(logging.ERROR)
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# algorithm information
project_name = 'shybox'
alg_name = 'Application for processing datasets - Merger by Domain'
alg_type = 'Package'
alg_version = '1.1.0'
alg_release = '2026-01-22'
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# script main
def main(view_table: bool = False):

    # ------------------------------------------------------------------------------------------------------------------
    ## CONFIGURATION MANAGEMENT
    # get file settings
    alg_args_obj = ArgumentsManager(settings_folder=os.path.dirname(os.path.realpath(__file__)))
    alg_args_file, alg_args_time = alg_args_obj.get()

    # crete configuration object
    alg_cfg_obj = ConfigManager.from_source(src=alg_args_file, add_other_keys_to_mandatory=True,
                                            root_key=None, application_key=None)

    # view lut section
    alg_cfg_lut = alg_cfg_obj.get_section(section='lut', root_key=None, raise_if_missing=True)
    alg_cfg_obj.view(section=alg_cfg_lut, table_name='variables [cfg info]', table_print=view_table)

    # get application section
    alg_cfg_application = alg_cfg_obj.get_section(section='application', root_key=None, raise_if_missing=True)
    # fill application section
    alg_cfg_application = alg_cfg_obj.fill_obj_from_lut(
        section=alg_cfg_application,
        resolve_time_placeholders=False, time_keys=('time_start', 'time_end', 'time_period'),
        template_keys=('path_destination', 'time_destination')
    )
    # view application section
    alg_cfg_obj.view(section=alg_cfg_application, table_name='application [cfg info]', table_print=view_table)

    # get workflow section
    alg_cfg_workflow = alg_cfg_obj.get_section(section='workflow', root_key=None, raise_if_missing=True)
    # view workflow section
    alg_cfg_obj.view(section=alg_cfg_workflow, table_name='workflow [cfg info]', table_print=view_table)

    # get tmp section
    alg_cfg_tmp = alg_cfg_obj.get_section(section='tmp', root_key=None, raise_if_missing=True)
    # fill tmp section
    alg_cfg_tmp = alg_cfg_obj.fill_obj_from_lut(
        section=alg_cfg_tmp, lut=alg_cfg_lut,
        resolve_time_placeholders=False, time_keys=('time_start', 'time_end', 'time_period'),
        template_keys=()
    )

    # view tmp section
    alg_cfg_obj.view(section=alg_cfg_tmp, table_name='tmp [cfg info]', table_print=view_table)
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## LOGGING MANAGEMENT
    # get logging section
    alg_cfg_log = alg_cfg_obj.get_section("log", root_key=None, raise_if_missing=True)
    # fill logging section
    alg_cfg_log = alg_cfg_obj.fill_obj_from_lut(
        section=alg_cfg_log, lut=alg_cfg_lut,
        resolve_time_placeholders=False, time_keys=('time_start', 'time_end', 'time_period'),
        template_keys=()
    )
    # view log section
    alg_cfg_obj.view(section=alg_cfg_log, table_name='log [cfg info]', table_print=view_table)

    # set logging instance
    LoggingManager.setup(
        logger_folder=alg_cfg_log['path'], logger_file=alg_cfg_log['file_name'],
        logger_format="%(asctime)s %(name)-15s %(levelname)-8s %(message)-80s %(filename)-20s:[%(lineno)-6s - %(funcName)-20s()]",
        handlers=['file', 'stream'],
        force_reconfigure=True,
        arrow_base_len=3, arrow_prefix='-', arrow_suffix='>',
        warning_dynamic=False, error_dynamic=False, warning_fixed_prefix="===> ", error_fixed_prefix="===> ",
        level=10
    )

    # define logging instance
    logging_handle = LoggingManager(
        name="shybox_algorithm_merger_by_domain_hmc",
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
    alg_time_generic = select_time_range(
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
    ## CONFIGURATION MANAGEMENT
    configuration = {
        "WORKFLOW": {
            "options": {
                "intermediate_output": "Tmp",
                "tmp_dir": alg_cfg_tmp['path']
            },
            "process_list": {
                "snow_mask": [
                    {"function": "merge_data_by_ref", "method": 'nn', "max_distance": 25000, "neighbours": 7,
                     "fill_value": np.nan, "var_no_data": 0},
                    {"function": "mask_data_by_ref", "ref_value": -9999, "mask_no_data": np.nan}
                ],
                "rain_eff": [
                    {"function": "merge_data_by_ref", "method": 'nn', "max_distance": 25000, "neighbours": 7,
                     "fill_value": np.nan, "var_no_data": -9999},
                    {"function": "mask_data_by_ref", "ref_value": -9999, "mask_no_data": np.nan}
                ],
                "albedo": [
                    {"function": "merge_data_by_ref", "method": 'nn', "max_distance": 25000, "neighbours": 7,
                     "fill_value": np.nan, "var_no_data": -9999},
                    {"function": "mask_data_by_ref", "ref_value": -9999, "mask_no_data": np.nan}
                ]
            }
        }
    }
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # time iteration(s)
    for time_step in alg_time_generic:

        # ------------------------------------------------------------------------------------------------------------------
        ## TIME MANAGEMENT (STEP)
        # time data
        time_data_length = int(alg_cfg_application['time'].get('dataset', 24))
        time_data_reference = select_time_format(time_step, time_format=alg_cfg_application['time']['format'])

        # time analysis
        time_anls_length = int(alg_cfg_application['time'].get('dataset', 24))
        time_anls_period = select_time_range(time_start=time_step, time_period=time_anls_length, time_frequency='h')
        time_anls_start = select_time_format(time_anls_period[0], time_format='%Y-%m-%d %H:%M')
        time_anls_end = select_time_format(time_anls_period[-1], time_format='%Y-%m-%d %H:%M')
        # ------------------------------------------------------------------------------------------------------------------

        # ------------------------------------------------------------------------------------------------------------------
        ## SOURCE DATA MANAGEMENT
        alg_cfg_step = alg_cfg_obj.fill_obj_from_lut(
            resolve_time_placeholders=True, when=time_step,
            time_keys=('file_time_source', 'path_time_source'),
            extra_tags={
                'file_time_source': time_step, "path_time_source": time_step},
            section=alg_cfg_application, in_place=False,
            template_keys=('file_time_destination','path_time_destination')
        )
        # view application section
        alg_cfg_obj.view(section=alg_cfg_step, table_name='application [cfg step]', table_print=view_table)

        # iterate over src datasets
        dset_src_handler_list = []
        for alg_data_src_key, alg_data_src_settings in alg_cfg_step['data_source'].items():

            # dataset handler
            dset_src_handler_obj = DataLocal(
                path=alg_data_src_settings['path'],
                file_name=alg_data_src_settings['file_name'],
                file_type='grid_3d', file_format='netcdf', file_mode='local',
                file_variable='rain', file_io='input',
                variable_template={
                    "dims_geo": {"lon": "longitude", "lat": "latitude", "nt": "time"},
                    "vars_data": {"RAIN_EFF": "rain"}
                },
                time_signature='period',
                time_reference=time_data_reference, time_period=time_data_length,
                time_freq='h', time_direction='forward',
            )

            data_src_obj = create_src_dataset(
                file_name=data_src_settings['file_name'], file_path=data_src_settings['path'],
                file_time=time_step)

            dset_src_handler_list.append(dset_src_handler_obj)

        # iterate over dst datasets
        data_dst_list = []
        for data_dst_key, data_dst_settings in alg_variables_application['data_destination'].items():

            data_dst_obj = create_dst_dataset(
                file_name=data_dst_settings['file_name'], file_path=data_dst_settings['path'],
                file_time=sim_time, file_variable=data_dst_settings['variable'],
                vars_data=data_dst_settings['vars_data'],
                vars_geo=data_dst_settings['vars_geo'], dims_geo=data_dst_settings['dims_geo'])

            data_dst_list.append(data_dst_obj)

        # ------------------------------------------------------------------------------------------------------------------
        # ## SETTINGS MANAGEMENT (STEP)
        # fill application section
        step_cfg_application = alg_cfg_obj.fill_obj_from_lut(
            resolve_time_placeholders=True, when=time_step, time_keys=('time_source', 'path_source_time'),
            extra_tags={
                'time_source': time_step, "path_source_time": time_step},
            section=alg_cfg_application, in_place=False,
            template_keys=('file_time_destination',)
        )
        # view application section
        alg_cfg_obj.view(section=step_cfg_application, table_name='application [cfg step]', table_print=True)
        # ------------------------------------------------------------------------------------------------------------------

        # ------------------------------------------------------------------------------------------------------------------
        ## DATASETS MANAGEMENT
        # Rain handler
        rain_handler = DataLocal(
            path=step_cfg_application['data_source']['rain']['path'],
            file_name=step_cfg_application['data_source']['rain']['file_name'],
            file_type='grid_3d', file_format='netcdf', file_mode='local',
            file_variable='rain', file_io='input',
            variable_template={
                "dims_geo": {"lon": "longitude", "lat": "latitude", "nt": "time"},
                "vars_data": {"RAIN_EFF": "rain"}
            },
            time_signature='period',
            time_reference=time_data_reference, time_period=time_data_length,
            time_freq='h', time_direction='forward',
        )



        # iterate over src datasets
        data_src_list = []
        for data_src_key, data_src_settings in alg_variables_application['data_source'].items():

            data_src_obj = create_src_dataset(
                file_name=data_src_settings['file_name'], file_path=data_src_settings['path'],
                file_time=sim_time)

            data_src_list.append(data_src_obj)

        # iterate over dst datasets
        data_dst_list = []
        for data_dst_key, data_dst_settings in alg_variables_application['data_destination'].items():

            data_dst_obj = create_dst_dataset(
                file_name=data_dst_settings['file_name'], file_path=data_dst_settings['path'],
                file_time=sim_time, file_variable=data_dst_settings['variable'],
                vars_data=data_dst_settings['vars_data'],
                vars_geo=data_dst_settings['vars_geo'], dims_geo=data_dst_settings['dims_geo'])

            data_dst_list.append(data_dst_obj)

        # orchestrator multi variable settings
        orc_process = Orchestrator.multi_tile(
            data_package_in=data_src_list, data_package_out=data_dst_list,
            data_ref=geo_data,
            configuration=configuration['WORKFLOW']
        )

        # orchestrator multi variable execution
        orc_process.run(time=sim_time)

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
# method to define source dataset
def create_dst_dataset(file_name: str, file_path: str, file_time: pd.Timestamp,
                       file_variable: str,
                       vars_data: dict, vars_geo: dict, dims_geo: dict) -> DataLocal:

    # define file name
    file_name = fill_string(file_name, time_source=file_time, domain_name=None)

    data_obj = DataLocal(
        path=file_path,
        file_name=file_name, time_signature='step',
        file_format='geotiff', file_type=None, file_mode='grid', file_variable=[file_variable],
        file_template={
            "dims_geo": dims_geo, "vars_geo": vars_geo, "vars_data": vars_data
        },
        time_period=1, time_format='%Y%m%d%H%M')

    return data_obj
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to define source dataset
def create_src_dataset(file_name: str, file_path: str, file_time: pd.Timestamp) -> DataLocal:

    # define file name
    file_name = fill_string(file_name, time_source=file_time, domain_name=None)

    # define file obj
    data_obj = DataLocal(
        path=file_path,
        file_name=file_name,
        file_format="netcdf", file_mode=None, file_variable=['snow_mask', 'rain_eff', 'albedo'],
        file_template={
            "dims_geo": {"X": "longitude", "Y": "latitude", "time": "time"},
            'coords_geo': {'Longitude': 'longitude', 'Latitude': 'latitude'},
            "vars_data": {"SnowMask": "snow_mask", "REff": "rain_eff", "AlbedoS": "snow_albedo"}
        },
        time_signature='current',
        time_reference=file_time, time_period=1, time_freq='h', time_direction='forward',
    )

    return data_obj
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# call script from external library
if __name__ == "__main__":
    # run script
    main(view_table=True)
# ----------------------------------------------------------------------------------------------------------------------
