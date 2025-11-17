#!/usr/bin/python3
"""
SHYBOX PACKAGE - TEST DATA GRID - MERGER BY DOMAIN

__date__ = '20251116'
__version__ = '1.1.0'
__author__ =
    'Fabio Delogu (fabio.delogu@cimafoundation.org),
     Andrea Libertino (andrea.libertino@cimafoundation.org)'
__library__ = 'shybox'

General command line:
python test_dataset_main.py -settings_file configuration.json -time "YYYY-MM-DD HH:MM"

Version(s):
20251116 (1.1.0) --> Refactoring and improving shybox package structure and codebase
20250123 (1.0.0) --> Beta release for shybox package
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
alg_name = 'Test for data grid - merger dy domain'
alg_type = 'Package'
alg_version = '1.1.0'
alg_release = '2025-11-16'
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# main function
def main():

    # ------------------------------------------------------------------------------------------------------------------
    ## Logging setup
    # set logging instance
    LoggingManager.setup(
        logger_folder='log/',
        logger_file="shybox_variable_merging_s3m_results.log",
        logger_format="%(asctime)s %(name)-15s %(levelname)-8s %(message)-80s %(filename)-20s:[%(lineno)-6s - %(funcName)-20s()]",
        handlers=['file', 'stream'],
        arrow_base_len=3, arrow_prefix='-', arrow_suffix='>',
        warning_dynamic=False, error_dynamic=False, warning_fixed_prefix="===> ", error_fixed_prefix="===> ",
        level=10
    )
    # define log main
    log_handle_algorithm = LoggingManager(
        name='shybox_algorithm_merger_s3m',
        level=logging.INFO, use_arrows=True, arrow_dynamic=True, arrow_tag="algorithm",
        set_as_current=True)
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## Info start
    # info algorithm (start)
    log_handle_algorithm.info_header(LoggingManager.rule_line("=", 78))
    log_handle_algorithm.info_header(alg_name + ' (Version: ' + alg_version + ' Release_Date: ' + alg_release + ')')
    log_handle_algorithm.info_header('START ... ', blank_after=True)

    # time algorithm
    start_time = time.time()
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## Workflow configuration
    configuration_tile = {
        "WORKFLOW": {
            "options": {
                "intermediate_output": "Tmp",
                "tmp_dir": "tmp"
            },
            "process_list": {
                "SWE": [
                    {"function": "merge_data_by_ref", "method": 'nn', "max_distance": 25000, "neighbours": 7,
                     "fill_value": np.nan, "var_no_data": -9999},
                    {"function": "mask_data_by_ref", "ref_value": -9999, "mask_no_data": np.nan}
                ],
                "Ice_Thickness": [
                    {"function": "merge_data_by_ref", "method": 'nn', "max_distance": 25000, "neighbours": 7,
                     "fill_value": -9999.0, "var_no_data": -9999.0},
                    {"function": "mask_data_by_ref", "ref_value": -9999, "mask_no_data": np.nan}
                ]
            }
        }
    }
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## Datasets
    s3m_data_01 = DataLocal(
        path='/home/fabio/Desktop/shybox/dset/case_study_itwater/case_study_merger/src/Valle_Aosta/2024/01/06/',
        file_name='S3M_202401060900.nc.gz',
        file_type='grid_2d', file_format='netcdf', file_mode='local', file_variable=['Ice_Thickness', 'SWE'], file_io='input',
        variable_template={
            "dims_geo": {"X": "longitude", "Y": "latitude", "time": "time"},
            'coords_geo': {'Longitude': 'longitude', 'Latitude': 'latitude'},
            "vars_data": {
                "Ice_Thickness": "ice_t",
                "SWE": "swe"
            }
        },
        time_signature='current',
        time_reference='2024-01-06 09:00', time_period=1, time_freq='h', time_direction='single',
        logger=log_handle_algorithm
    )

    s3m_data_02 = DataLocal(
        path='/home/fabio/Desktop/shybox/dset/case_study_itwater/case_study_merger/src/Piemonte/2024/01/06/',
        file_name='S3M_202401060900.nc.gz',
        file_type='grid_2d', file_format='netcdf', file_mode='local', file_variable=['Ice_Thickness', 'SWE'], file_io='input',
        variable_template={
            "dims_geo": {"X": "longitude", "Y": "latitude", "time": "time"},
            'coords_geo': {'Longitude': 'longitude', 'Latitude': 'latitude'},
            "vars_data": {
                "Ice_Thickness": "ice_t",
                "SWE": "swe"
            }
        },
        time_signature='current',
        time_reference='2024-01-06 09:00', time_period=1, time_freq='h', time_direction='single',
        logger=log_handle_algorithm
    )

    s3m_data_03 = DataLocal(
        path='/home/fabio/Desktop/shybox/dset/case_study_itwater/case_study_merger/src/Liguria/2024/01/06',
        file_name='S3M_202401060900.nc.gz', # 'S3M_%Y%m%d%H00.nc.gz',
        file_type='grid_2d', file_format='netcdf', file_mode='local', file_variable=['Ice_Thickness', 'SWE'], file_io='input',
        variable_template={
            "dims_geo": {"X": "longitude", "Y": "latitude", "time": "time"},
            'coords_geo': {'Longitude': 'longitude', 'Latitude': 'latitude'},
            "vars_data": {
                "Ice_Thickness": "ice_t",
                "SWE": "swe"
            }
        },
        time_signature='current',
        time_reference='2024-01-06 09:00', time_period=1, time_freq='h', time_direction='single',
        logger=log_handle_algorithm
    )

    geo_data = DataLocal(
        path='/home/fabio/Desktop/shybox/dset/case_study_itwater/case_study_merger/ref/',
        file_name='Mask_Italy_200m_WSG84geog.txt',
        file_type='grid_2d', file_format='ascii', file_mode='local', file_variable='terrain', file_io='input',
        variable_template={
            "dims_geo": {"x": "longitude", "y": "latitude"},
            "vars_geo": {"x": "longitude", "y": "latitude"}
        },
        time_signature=None, time_direction=None,
        logger=log_handle_algorithm, message=False
    )

    output_data_ice_thick = DataLocal(
        path='/home/fabio/Desktop/shybox/dset/case_study_itwater/case_study_merger/dst/%Y/%m/%d/',
        file_name='ice_t.%Y%m%d%H%M.tiff',
        file_format='geotiff', file_type=None, file_mode='local', file_variable=['Ice_Thickness'], file_io='output',
        variable_template={
            "dims_geo": {"longitude": "longitude", "latitude": "latitude"},
            "vars_geo": {"longitude": "longitude", "latitude": "longitude"},
            "vars_data": {"ice_t": "ICE_THICKNESS"}
        },
        time_signature='step', time_period=1, time_format='%Y%m%d%H%M',
        logger=log_handle_algorithm, message=False
    )

    output_data_swe = DataLocal(
        path='/home/fabio/Desktop/shybox/dset/case_study_itwater/case_study_merger/dst/%Y/%m/%d/',
        file_name='swe.%Y%m%d%H%M.tiff',
        file_format='geotiff', file_type=None, file_mode='local', file_variable=['SWE'], file_io='output',
        variable_template={
            "dims_geo": {"longitude": "longitude", "latitude": "latitude"},
            "vars_geo": {"longitude": "longitude", "latitude": "longitude"},
            "vars_data": {"swe": "SNOW_WATER_EQUIVALENT"}
        },
        time_signature='step', time_period=1, time_format='%Y%m%d%H%M',
        logger=log_handle_algorithm, message=False
    )
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## Orchestrator - multi tile
    # orchestrator settings
    orc_process = Orchestrator.multi_tile(
        data_package_in=[s3m_data_01, s3m_data_02, s3m_data_03],
        data_package_out=[output_data_ice_thick, output_data_swe],
        data_ref=geo_data,
        priority=['Ice_Thickness'],
        configuration=configuration_tile['WORKFLOW'],
        logger=log_handle_algorithm
    )
    # orchestrator exec
    orc_process.run(time=pd.date_range('2024-01-06 09:00', '2024-01-06 09:00', freq='h'))
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## Info end
    # info algorithm (end)
    alg_time_elapsed = round(time.time() - start_time, 1)

    log_handle_algorithm.info_header(alg_name + ' (Version: ' + alg_version + ' Release_Date: ' + alg_release + ')', blank_before=True)
    log_handle_algorithm.info_header('TIME ELAPSED: ' + str(alg_time_elapsed) + ' seconds')
    log_handle_algorithm.info_header('... END')
    log_handle_algorithm.info_header('Bye, Bye')
    log_handle_algorithm.info_header(LoggingManager.rule_line("=", 78))
    # ------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# call entrypoint
if __name__ == '__main__':
    main()
# ----------------------------------------------------------------------------------------------------------------------
