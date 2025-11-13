#!/usr/bin/python3
"""
SHYBOX PACKAGE - APP PROCESSING DATASET MAIN

__date__ = '20251105'
__version__ = '3.1.0'
__author__ =
    'Fabio Delogu (fabio.delogu@cimafoundation.org),
     Andrea Libertino (andrea.libertino@cimafoundation.org)'
__library__ = 'shybox'

General command line:
python test_dataset_main.py -settings_file configuration.json -time "YYYY-MM-DD HH:MM"

Version(s):
20251105 (3.1.0) --> Refactoring and improving shybox package structure and codebase
20250123 (3.0.0) --> Beta release for shybox package
20221013 (2.1.0) --> Add codes to manage tiff format and fix bug in output variable(s)
20220412 (2.0.3) --> Add codes to manage interpolating (nearest, linear, sample) and masking (watermark) method(s)
20220322 (2.0.2) --> Add codes and functions to merge hmc and s3m subdomains datasets
20211222 (2.0.1) --> Add no data filter
20211201 (2.0.0) --> Upgrade codes and routines
20211029 (1.0.0) --> First release
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import time

import numpy as np
import pandas as pd

from shybox.generic_toolkit.lib_utils_args import get_logger_name
from shybox.generic_toolkit.lib_default_args import logger_name, logger_format, logger_arrow
from shybox.generic_toolkit.lib_default_args import collector_data

from shybox.processing_toolkit.lib_proc_merge import merge_data_by_ref
from shybox.processing_toolkit.lib_proc_mask import mask_data_by_ref, mask_data_by_limits
from shybox.processing_toolkit.lib_proc_interp import interpolate_data

from shybox.orchestrator_toolkit.orchestrator_handler_base import OrchestratorHandler as Orchestrator

from shybox.dataset_toolkit.dataset_handler_local import DataLocal
from shybox.logging_toolkit.logging_handler import LoggingManager
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# algorithm information
project_name = 'shybox'
alg_name = 'Application for processing datasets'
alg_type = 'Package'
alg_version = '3.1.0'
alg_release = '2025-11-05'
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# main function
def main():

    # ------------------------------------------------------------------------------------------------------------------
    ## Logging setup
    # set logging instance
    LoggingManager.setup(
        logger_folder='log/',
        logger_file="shybox_variable_processing_iwrn.log",
        logger_format="%(asctime)s %(name)-15s %(levelname)-8s %(message)-80s %(filename)-20s:[%(lineno)-6s - %(funcName)-20s()]",
        handlers=['file', 'stream'],
        arrow_base_len=3, arrow_prefix='-', arrow_suffix='>',
        warning_dynamic=False, error_dynamic=False, warning_fixed_prefix="===> ", error_fixed_prefix="===> ",
        level=10
    )
    # define log main
    log_handle_main = LoggingManager(
        name='Main',
        level=logging.INFO, use_arrows=True, arrow_dynamic=True, arrow_tag="algorithm",
        set_as_current=True)
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## Info start
    # info algorithm (start)
    log_handle_main.info_header(LoggingManager.rule_line("=", 78))
    log_handle_main.info_header(alg_name + ' (Version: ' + alg_version + ' Release_Date: ' + alg_release + ')')
    log_handle_main.info_header('START ... ', blank_after=True)

    # time algorithm
    start_time = time.time()
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## Workflow configuration
    # Example of how to use the Orchestrator class for multi variable(s)
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
                ]
            }
        }
    }
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## Datasets
    # define log handle pdata
    log_handle_pdata = LoggingManager(
        name="ProcessingData",  level=10, use_arrows=True, arrow_dynamic=True,
        arrow_tag="algorithm", set_as_current=True)

    lai_data = DataLocal(
        path='/home/fabio/Desktop/shybox/dset/iwrn/dynamic/LAI/01/01/',
        file_name='CLIM_0101_LAI_clip_bbox_ETH.tif',
        file_type='grid_2d', file_format='tiff', file_mode='local', file_variable='LAI', file_io='input',
        variable_template={
            "dims_geo": {"x": "longitude", "y": "latitude"},
            "vars_data": {"lai": "leaf_area_index"}
        },
        time_signature='unique',
        time_reference='2000-01-01 12:00', time_period=1, time_freq='h', time_direction='single',
        logger=log_handle_pdata, message=False
    )

    airt_data = DataLocal(
        path='/home/fabio/Desktop/shybox/dset/iwrn/dynamic/',
        file_name='temperature_012000.nc',
        file_type='grid_3d', file_format='netcdf', file_mode='local', file_variable='AIR_T', file_io='input',
        variable_template={
            "dims_geo": {"x": "longitude", "y": "latitude", "time": "time"},
            "vars_data": {"temperature": "air_temperature"}
        },
        time_signature='period',
        time_reference='2000-01-01 00:00', time_period=744, time_freq='h', time_direction='forward',
        logger=log_handle_pdata, message=False
    )

    rh_data = DataLocal(
        path='/home/fabio/Desktop/shybox/dset/iwrn/dynamic/',
        file_name='relative_humidity_012000.nc',
        file_type='grid_3d', file_format='netcdf', file_mode='local', file_variable='RH', file_io='input',
        variable_template={
            "dims_geo": {"x": "longitude", "y": "latitude", "time": "time"},
            "vars_data": {"relative_humidity": "relative_humidity"}
        },
        time_signature='period',
        time_reference='2000-01-01 00:00', time_period=744, time_freq='h', time_direction='forward',
        logger=log_handle_pdata, message=False
    )

    geo_data = DataLocal(
        path='/home/fabio/Desktop/shybox/dset/iwrn/static/gridded/',
        file_name='shebele.dem.txt',
        file_type='grid_2d', file_format='ascii', file_mode='local', file_variable='terrain', file_io='input',
        variable_template={
            "dims_geo": {"x": "longitude", "y": "latitude"},
            "vars_geo": {"x": "longitude", "y": "latitude"}
        },
        time_signature=None, time_direction=None,
        logger=log_handle_pdata, message=False
    )

    output_data = DataLocal(
        path='/home/fabio/Desktop/shybox/dset/iwrn/dynamic/destination/',
        file_name='hmc.forcing.%Y%m%d%H%M.nc', time_signature='step',
        file_format='netcdf', file_type='hmc', file_mode='local', file_variable=['LAI', 'AIR_T', 'RH'], file_io='output',
        variable_template={
            "dims_geo": {"longitude": "west_east", "latitude": "south_north", "time": "time"},
            "coord_geo": {"longitude": "longitude", "latitude": "longitude"},
            "vars_data": {
                "leaf_area_index": "LEAF_AREA_INDEX",
                "air_temperature": "AIR_TEMPERATURE",
                "relative_humidity": "RELATIVE_HUMIDITY"}
        },
        time_period=1, time_format='%Y%m%d%H%M',
        logger=log_handle_pdata, message=False
    )
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## Orchestrator - Case 1: multi variable
    # define log handle orchestrator
    log_handle_orchestrator = LoggingManager(
        level=logging.INFO, use_arrows=True, arrow_dynamic=True,
        name='Orchestrator', arrow_tag="algorithm", set_as_current=True)

    # orchestrator settings
    orc_process = Orchestrator.multi_variable(
        data_package_in=[lai_data, airt_data, rh_data],
        data_package_out=output_data,
        data_ref=geo_data,
        priority=['lai'],
        configuration=configuration['WORKFLOW'],
        logger=log_handle_orchestrator
    )
    # orchestrator exec
    orc_process.run(time=pd.date_range(start='2000-01-01 11:00', end='2000-01-01 16:00', freq='h'))
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## Orchestrator - Case 2: single variable (example of how to use the Orchestrator class for single variable)
    # orchestrator settings
    orc_process = Orchestrator(
        data_in=airt_data, data_out=output_data,
        options={
            "intermediate_output": "Tmp",
            "tmp_dir": "/home/fabio/Desktop/shybox/dset/iwrn/tmp/"
        })

    # orchestrator processes (add to the orchestrator instance)
    orc_process.add_process(
        interpolate_data, ref=geo_data,
        method='nn', max_distance=25000, neighbours=7, fill_value=np.nan)
    orc_process.add_process(mask_data_by_ref, ref=geo_data, ref_value=-9999, mask_no_data=np.nan)

    # orchestrator exec
    orc_process.run(time=pd.date_range('2000-01-01 05:00', '2020-01-01 07:00', freq='H'))
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## Info end
    # info algorithm (end)
    alg_time_elapsed = round(time.time() - start_time, 1)

    log_handle_main.info_header(alg_name + ' (Version: ' + alg_version + ' Release_Date: ' + alg_release + ')', blank_before=True)
    log_handle_main.info_header('TIME ELAPSED: ' + str(alg_time_elapsed) + ' seconds')
    log_handle_main.info_header('... END')
    log_handle_main.info_header('Bye, Bye')
    log_handle_main.info_header(LoggingManager.rule_line("=", 78))
    # ------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# call entrypoint
if __name__ == '__main__':

    main()
# ----------------------------------------------------------------------------------------------------------------------
