#!/usr/bin/python3
"""
SHYBOX PACKAGE - APP PROCESSING DATASET MAIN

__date__ = '20251126'
__version__ = '1.2.0'
__author__ =
    'Fabio Delogu (fabio.delogu@cimafoundation.org),
     Francesco Avanzi (francesco.avanzi@cimafoundation.org)'
__library__ = 'shybox'

General command line:
python app_converter_workflow_main.py -settings_file configuration.json -time "YYYY-MM-DD HH:MM"

Examples of environment variables declarations:
DOMAIN_NAME='marche';
TIME_START='2024-01-04';
TIME_END='2024-01-06';

PATH_SRC_BY_S3M='/home/fabio/Desktop/shybox/dset/case_study_itwater/case_study_merger_hmc/data_dynamic/src/s3m/';
PATH_SRC_BY_BIAS_CORRECTION='/home/fabio/Desktop/shybox/dset/case_study_itwater/case_study_merger_hmc/data_dynamic/src/cmcc';
PATH_DST='/home/fabio/Desktop/shybox/dset/case_study_itwater/case_study_merger_hmc/data_dynamic/src'
PATH_LOG=/home/fabio/Desktop/shybox/dset/case_study_itwater/case_study_merger_hmc/log/;

Version(s):
20251126 (1.2.0) --> Release for shybox package (hmc datasets converter base configuration)
20250716 (1.0.0) --> Beta release for shybox package (hmc datasets converter base configuration)
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

from shybox.orchestrator_toolkit.orchestrator_handler_base import OrchestratorHandler as Orchestrator
from shybox.dataset_toolkit.dataset_handler_local import DataLocal
from shybox.logging_toolkit.logging_handler import LoggingManager

# fx imported in the PROCESSES (will be used in the global variables PROCESSES) --> DO NOT REMOVE
from shybox.processing_toolkit.lib_proc_interp import interpolate_data
from shybox.processing_toolkit.lib_proc_mask import mask_data_by_ref

from shybox.time_toolkit.lib_utils_time import (select_time_range, select_time_format)

# set logger
logger_stream = logging.getLogger(logger_name)
logger_stream.setLevel(logging.ERROR)
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
# script main
def main(alg_collectors_settings: dict = None):

    # ------------------------------------------------------------------------------------------------------------------
    ## SETTINGS MANAGEMENT (GLOBAL)
    # get file settings
    alg_args_obj = ArgumentsManager(settings_folder=os.path.dirname(os.path.realpath(__file__)))
    alg_args_file, alg_args_time = alg_args_obj.get()

    # crete configuration object
    alg_cfg_obj = ConfigManager.from_source(
        alg_args_file,
        root_key="settings",
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
        template_keys=('path_destination_time', 'time_destination',)
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
        name="shybox_algorithm_converter_itwater_hmc_forcing",
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
    ## CONFIGURATION MANAGEMENT
    # set configuration instance
    configuration = {
        "WORKFLOW": {
            "options": {
                "intermediate_output": "Tmp",
                "tmp_dir": alg_cfg_application['tmp']['path']
            },
            "process_list": {
                "snow_mask": [
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
    alg_time_generic = select_time_range(
        time_start=alg_cfg_application['time']['start'],
        time_end=alg_cfg_application['time']['end'],
        time_frequency=alg_cfg_application['time']['frequency'],
        ensure_range=False, flat_if_single=True)
    alg_time_period = select_time_format(alg_time_generic, time_format=alg_cfg_application['time']['format'])
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
    # iterate over simulation time
    for time_step in alg_time_generic:

        # ------------------------------------------------------------------------------------------------------------------
        ## TIME MANAGEMENT (STEP)
        # time data
        time_data_length = int(alg_cfg_application['time'].get('dataset', 24))
        time_data_reference = select_time_format(time_step, time_format=alg_cfg_application['time']['format'])
        time_data_path = select_time_format(time_step, time_format='%Y')
        # time analysis
        time_anls_length = int(alg_cfg_application['time'].get('dataset', 24))
        time_anls_period = select_time_range(time_start=time_step, time_period=time_anls_length, time_frequency='h')
        time_anls_start = select_time_format(time_anls_period[0], time_format='%Y-%m-%d %H:%M')
        time_anls_end = select_time_format(time_anls_period[-1], time_format='%Y-%m-%d %H:%M')
        # ------------------------------------------------------------------------------------------------------------------

        # ------------------------------------------------------------------------------------------------------------------
        # ## SETTINGS MANAGEMENT (STEP)
        # fill application section
        step_cfg_application = alg_cfg_obj.fill_obj_from_lut(
            resolve_time_placeholders=True, when=time_step, time_keys=('time_source', 'path_source_time'),
            extra_tags={
                'time_source': time_data_reference, "path_source_time": time_data_path},
            section=alg_cfg_application, in_place=False,
            template_keys=('file_time_destination',)
        )
        # view application section
        alg_cfg_obj.view(section=step_cfg_application, table_name='application [cfg step]', table_print=True)
        # ------------------------------------------------------------------------------------------------------------------

        # ------------------------------------------------------------------------------------------------------------------
        ## DATASETS MANAGEMENT
        # Snow Mask handler
        snow_mask_handler = DataLocal(
            path=step_cfg_application['data_source']['snow_mask']['path'],
            file_name=step_cfg_application['data_source']['snow_mask']['file_name'],
            file_type='grid_3d', file_format='netcdf', file_mode='local',
            file_variable='snow_mask', file_io='input',
            variable_template={
                "dims_geo": {"lon": "longitude", "lat": "latitude", "nt": "time"},
                "vars_data": {"SNOW_MASK": "snow_mask"}
            },
            time_signature='period',
            time_reference=time_data_reference, time_period=time_data_length,
            time_freq='h', time_direction='forward',
        )

        # destination data
        output_handler = DataLocal(
            path=alg_cfg_application['data_destination']['path'],
            file_name=alg_cfg_application['data_destination']['file_name'],
            time_signature='step',
            file_format='netcdf', file_type='hmc', file_mode='local',
            file_variable=['snow_mask'], file_io='output',
            variable_template={
                "dims_geo": {"longitude": "west_east", "latitude": "south_north", "time": "time"},
                "coord_geo": {"Longitude": "longitude", "Latitude": "latitude"},
                "vars_data": {
                    "snow_mask": "SnowMask"
                }
            },
            time_period=1, time_format='%Y%m%d%H%M',
            logger=logging_handle, message=False
        )

        # orchestrator settings
        orc_process = Orchestrator.multi_variable(
            data_package_in=snow_mask_handler,
            data_package_out=output_handler,
            data_ref=geo_data,
            priority=['snow_mask'],
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
# call script from external library
if __name__ == "__main__":
    # run script
    main()
# ----------------------------------------------------------------------------------------------------------------------
