#!/usr/bin/python3
"""
HYMS PACKAGE - DATA GRID APP

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

from shybox.processing_toolkit.lib_proc_merge import merge_data
from shybox.processing_toolkit.lib_proc_mask import mask_data_by_ref, mask_data_by_limits
from shybox.processing_toolkit.lib_proc_interp import interpolate_data

from shybox.orchestrator_toolkit.orchestrator_handler_base import OrchestratorHandler as Orchestrator

from shybox.dataset_toolkit.dataset_handler_local import DataLocal

# set logger
logger_stream = logging.getLogger(get_logger_name(logger_name_mode='by_script', logger_name_default=logger_name))
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
def main(alg_collectors_settings: dict = None):

    # ------------------------------------------------------------------------------------------------------------------
    # info algorithm (start)
    logger_stream.info(logger_arrow.arrow_main_break)
    logger_stream.info(logger_arrow.main + alg_name + ' (Version: ' + alg_version + ' Release_Date: ' + alg_release + ')')
    logger_stream.info(logger_arrow.main + 'START ... ')
    logger_stream.info(logger_arrow.arrow_main_blank)

    # time algorithm
    start_time = time.time()
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # Example of how to use the Orchestrator class for multi layers
    configuration_tile = {
        "WORKFLOW": {
            "options": {
                "intermediate_output": "Tmp",
                "tmp_dir": "tmp"
            },
            "process_list": {
                "age": [
                    {"function": "merge_data", "method": 'nn', "max_distance": 25000, "neighbours": 7, "fill_value": np.nan},
                    {"function": "mask_data_by_ref", "ref_value": -9999, "mask_no_data": np.nan}
                ],
                "albedo": [
                    {"function": "merge_data", "method": 'nn', "max_distance": 25000, "neighbours": 7,
                     "fill_value": np.nan},
                    {"function": "mask_data_by_ref", "ref_value": -9999, "mask_no_data": np.nan}
                ]
            }
        }
    }

    s3m_data_domain_1 = DataLocal(
        path='/home/fabio/Desktop/shybox/dset/data_source/s3m/marche/%Y/%m/%d',
        file_name='S3M_%Y%m%d%H00.nc.gz',
        file_format="netcdf", file_mode=None, file_variable=['age', 'albedo'],
        file_template={
            "dims_geo": {"X": "longitude", "Y": "latitude", "time": "time"},
            'coords_geo': {'Longitude': 'longitude', 'Latitude': 'latitude'},
            "vars_data": {"AgeS": "snow_age", "AlbedoS": "snow_albedo"}
        },
        time_signature='current',
        time_reference='2025-01-24 05:00', time_period=1, time_freq='h', time_direction='forward',
    )

    s3m_data_domain_2 = DataLocal(
        path='/home/fabio/Desktop/shybox/dset/data_source/s3m/emilia_romagna/%Y/%m/%d',
        file_name='S3M_%Y%m%d%H00.nc.gz',
        file_format='netcdf', file_mode=None, file_variable=['age', 'albedo'],
        file_template={
            "dims_geo": {"X": "longitude", "Y": "latitude", "time": "time"},
            'coords_geo': {'Longitude': 'longitude', 'Latitude': 'latitude'},
            "vars_data": {"AgeS": "snow_age", "AlbedoS": "snow_albedo"}
        },
        time_signature='current',
        time_reference='2025-01-24 05:00', time_period=1, time_freq='h', time_direction='forward',
    )


    geo_data = DataLocal(
        path='/home/fabio/Desktop/shybox/dset/data_static/gridded/',
        file_name='marche.dem.txt',
        file_mode='grid', file_variable='terrain',
        file_template={
            "dims_geo": {"x": "longitude", "y": "latitude"},
            "vars_geo": {"x": "longitude", "y": "latitude"}
        },
        time_signature=None
    )

    output_data_1 = DataLocal(
        path='/home/fabio/Desktop/shybox/dset/itwater/%Y/%m/%d/',
        file_name='age.%Y%m%d%H%M.tiff', time_signature='step',
        file_format='geotiff', file_type=None, file_mode='grid', file_variable=['age'],
        file_template={
            "dims_geo": {"longitude": "longitude", "latitude": "latitude"},
            "vars_geo": {"longitude": "longitude", "latitude": "longitude"},
            "vars_data": {"snow_age": "SNOW_AGE"}
        },
        time_period=1, time_format='%Y%m%d%H%M')

    output_data_2 = DataLocal(
        path='/home/fabio/Desktop/shybox/dset/itwater/%Y/%m/%d/',
        file_name='albedo.%Y%m%d%H%M.tiff', time_signature='step',
        file_format='geotiff', file_type=None, file_mode='grid', file_variable=['albedo'],
        file_template={
            "dims_geo": {"longitude": "longitude", "latitude": "latitude"},
            "vars_geo": {"longitude": "longitude", "latitude": "longitude"},
            "vars_data": {"snow_albedo": "SNOW_ALBEDO"}
        },
        time_period=1, time_format='%Y%m%d%H%M')

    # multi variable
    orc_process = Orchestrator.multi_tile(
        data_package_in=[s3m_data_domain_1, s3m_data_domain_2],
        data_package_out=[output_data_1, output_data_2],
        data_ref=geo_data,
        configuration=configuration_tile['WORKFLOW']
    )

    orc_process.run(time=pd.date_range('2025-01-24 03:00', '2025-01-24 03:00', freq='H'))
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
# call entrypoint
if __name__ == '__main__':

    main(alg_collectors_settings=None)
# ----------------------------------------------------------------------------------------------------------------------
