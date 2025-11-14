#!/usr/bin/python3
"""
SHYBOX PACKAGE - DATA GRID APP

__date__ = '20250123'
__version__ = '3.0.0'
__author__ =
    'Fabio Delogu (fabio.delogu@cimafoundation.org),
     Andrea Libertino (andrea.libertino@cimafoundation.org)'
__library__ = 'shybox'

General command line:
python test_dataset_main.py -settings_file configuration.json -time "YYYY-MM-DD HH:MM"

Version(s):
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
from shybox.processing_toolkit.lib_proc_compute import compute_data_rh

from shybox.orchestrator_toolkit.orchestrator_handler_base import OrchestratorHandler as Orchestrator

from shybox.dataset_toolkit.dataset_handler_local import DataLocal
from shybox.logging_toolkit.logging_handler import LoggingManager
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# algorithm information
project_name = 'shybox'
alg_name = 'Application for data grid'
alg_type = 'Package'
alg_version = '3.0.0'
alg_release = '2025-01-24'
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# main function
def main():

    # ------------------------------------------------------------------------------------------------------------------
    # set logging instance
    LoggingManager.setup(
        logger_folder='log/',
        logger_file="shybox_variable_processing_icon.log",
        logger_format="%(asctime)s %(name)-15s %(levelname)-8s %(message)-80s %(filename)-20s:[%(lineno)-6s - %(funcName)-20s()]",
        handlers=['file', 'stream'],
        arrow_base_len=3, arrow_prefix='-', arrow_suffix='>',
        warning_dynamic=False, error_dynamic=False, warning_fixed_prefix="===> ", error_fixed_prefix="===> ",
        level=10
    )
    # define logging instance
    logging_handle = LoggingManager(
        name="shybox_algorithm_converter_icon",
        level=logging.INFO, use_arrows=True, arrow_dynamic=True, arrow_tag="algorithm",
        set_as_current=True)
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## Info start
    # info algorithm (start)
    logging_handle.info_header(LoggingManager.rule_line("=", 78))
    logging_handle.info_header(alg_name + ' (Version: ' + alg_version + ' Release_Date: ' + alg_release + ')')
    logging_handle.info_header('START ... ', blank_after=True)

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
                "air_t": [
                    {"function": "interpolate_data", "method":'nn', "max_distance": 25000, "neighbours": 7, "fill_value": np.nan},
                    {"function": "mask_data_by_ref", "ref_value": -9999, "mask_no_data": np.nan}
                ],
                "qv": [
                    {"function": "interpolate_data", "method":'nn', "max_distance": 22000, "neighbours": 7, "fill_value": np.nan},
                    {"function": "mask_data_by_ref", "ref_value": -9999, "mask_no_data": np.nan}
                ],
                "td": [
                    {"function": "interpolate_data", "method": 'nn', "max_distance": 22000, "neighbours": 7,
                     "fill_value": np.nan},
                    {"function": "mask_data_by_ref", "ref_value": -9999, "mask_no_data": np.nan}
                ],
                "rh": [
                    {"function": "compute_data_rh", "ref_value": -9999, "mask_no_data": np.nan,
                     "deps_vars": {'t': 'air_t', 'q': 'qv', 'td': 'td'}},
                    {"function": "interpolate_data", "method": 'nn', "max_distance": 22000, "neighbours": 7,
                     "fill_value": np.nan},
                    {"function": "mask_data_by_ref", "ref_value": -9999, "mask_no_data": np.nan}
                ]
            }
        }
    }
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## Datasets
    airt_data = DataLocal(
        path='/home/fabio/Desktop/shybox/dset/case_study_destine/data/data_dynamic/source/icon/20241017_0600/',
        file_name='t2m_202410170600.grib',
        file_type='grid_3d', file_format='grib', file_mode='local', file_variable='air_t', file_io='input',
        variable_template={
            "dims_geo": {"longitude": "longitude", "latitude": "latitude", "step": "time"},
            "vars_data": {"t2m": "air_temperature"}
        },
        time_signature='period',
        time_reference='2024-10-17 06:00', time_period=24, time_freq='h', time_direction='forward',
        logger=logging_handle
    )

    qv_data = DataLocal(
        path='/home/fabio/Desktop/shybox/dset/case_study_destine/data/data_dynamic/source/icon/20241017_0600/',
        file_name='qv_202410170600.grib',
        file_type='grid_3d', file_format='grib', file_mode='local', file_variable='qv', file_io='input',
        variable_template={
            "dims_geo": {"longitude": "longitude", "latitude": "latitude", "step": "time"},
            "vars_data": {"qv": "specific_humidity"}
        },
        time_signature='period',
        time_reference='2024-10-17 06:00', time_period=24, time_freq='h', time_direction='forward',
        logger=logging_handle
    )

    td_data = DataLocal(
        path='/home/fabio/Desktop/shybox/dset/case_study_destine/data/data_dynamic/source/icon/20241017_0600/',
        file_name='td2m_202410170600.grib',
        file_type='grid_3d', file_format='grib', file_mode='local', file_variable='td', file_io='input',
        variable_template={
            "dims_geo": {"longitude": "longitude", "latitude": "latitude", "step": "time"},
            "vars_data": {"d2m": "dew_point_temperature"}
        },
        time_signature='period',
        time_reference='2024-10-17 06:00', time_period=24, time_freq='h', time_direction='forward',
        logger=logging_handle
    )

    rh_data = DataLocal(
        path=None,
        file_name=None,
        file_type=None, file_format='tmp', file_mode='local', file_variable='rh', file_io='derived',
        file_deps=[airt_data, qv_data, td_data],
        variable_template={
            "dims_geo": {"longitude": "longitude", "latitude": "latitude", "step": "time"},
            "vars_data": {"rh": "relative_humidity"}
        },
        time_signature='period',
        time_reference='2024-10-17 06:00', time_period=24, time_freq='h', time_direction='forward',
        logger=logging_handle
    )

    geo_data = DataLocal(
        path='/home/fabio/Desktop/shybox/dset/case_study_destine/data/data_static/gridded/',
        file_name='marche.dem.txt',
        file_type='grid_2d', file_format='ascii', file_mode='local',
        file_variable='terrain', file_io='input',
        variable_template={
            "dims_geo": {"x": "longitude", "y": "latitude"},
            "vars_geo": {"x": "longitude", "y": "latitude"}
        },
        time_signature=None,
        logger=logging_handle
    )

    output_data = DataLocal(
        path='/home/fabio/Desktop/shybox/dset/case_study_destine/data/data_dynamic/destination/nc_icon/',
        file_name='hmc.forcing.%Y%m%d%H%M.nc', time_signature='step',
        file_format='netcdf', file_type='grid_hmc', file_mode='local',
        file_variable=['air_t', 'rh', 'qv', 'td'], file_io='output',
        variable_template={
            "dims_geo": {"longitude": "west_east", "latitude": "south_north", "time": "time"},
            "vars_geo": {"longitude": "longitude", "latitude": "longitude"},
            "vars_data": {"air_temperature": "AIR_TEMPERATURE",
                          "relative_humidity": "RELATIVE_HUMIDITY",
                          "specific_humidity": "SPECIFIC_HUMIDITY",
                          'dew_point_temperature': "DEW_POINT_TEMPERATURE"}
        },
        time_period=1, time_format='%Y%m%d%H%M',
        logger=logging_handle
    )
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## Orchestrator - Case 1: multi variable
    # orchestrator settings
    orc_process = Orchestrator.multi_variable(
        data_package_in=[rh_data, airt_data, qv_data, td_data],
        data_package_out=output_data,
        data_ref=geo_data,
        priority=['rh'],
        configuration=configuration['WORKFLOW'],
        logger=logging_handle
    )
    # orchestrator exec
    orc_process.run(time=pd.date_range(start='2024-10-17 06:00', end='2024-10-17 15:00', freq='h'))
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## Orchestrator - Case 2: single variable (example of how to use the Orchestrator class for single variable)
    # orchestrator settings
    orc_process = Orchestrator(
        data_in=airt_data, data_out=output_data,
        options={
            "intermediate_output": "Tmp",
            "tmp_dir": "/home/fabio/Desktop/shybox/dset/case_study_destine/data/data_dynamic/tmp/"
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

    logging_handle.info_header(alg_name + ' (Version: ' + alg_version + ' Release_Date: ' + alg_release + ')', blank_before=True)
    logging_handle.info_header('TIME ELAPSED: ' + str(alg_time_elapsed) + ' seconds')
    logging_handle.info_header('... END')
    logging_handle.info_header('Bye, Bye')
    logging_handle.info_header(LoggingManager.rule_line("=", 78))
    # ------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# call entrypoint
if __name__ == '__main__':
    main()
# ----------------------------------------------------------------------------------------------------------------------
