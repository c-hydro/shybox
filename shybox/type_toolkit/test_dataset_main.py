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
import os

import numpy as np
import pandas as pd

from shybox.generic_toolkit.lib_utils_args import get_logger_name
from shybox.generic_toolkit.lib_default_args import logger_name, logger_format, logger_arrow
from shybox.generic_toolkit.lib_default_args import collector_data

from shybox.dataset_toolkit.merge.driver_data_grid import DrvData, MultiData
from shybox.io_toolkit import io_handler_base

from shybox.processing_toolkit.lib_proc_mask import mask_data_by_ref, mask_data_by_limits
from shybox.processing_toolkit.lib_proc_interp import interpolate_data

from shybox.orchestrator_toolkit.orchestrator_handler_base import OrchestratorHandler as Orchestrator

from shybox.type_toolkit.io_dataset_grid import DataObj

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
    configuration = {
        "WORKFLOW": {
            "options": {
                "intermediate_output": "Tmp",
                "tmp_dir": "tmp"
            },
            "process_list": {
                "airt": [
                    {"function": "interpolate_data", "method":'nn', "max_distance": 25000, "neighbours": 7, "fill_value": np.nan},
                    {"function": "mask_data_by_ref", "ref_value": -9999, "mask_no_data": np.nan}
                ],
                "rh": [
                    {"function": "interpolate_data", "method":'nn', "max_distance": 25000, "neighbours": 7, "fill_value": np.nan},
                    {"function": "mask_data_by_ref", "ref_value": -9999, "mask_no_data": np.nan}
                ]
            }
        }
    }


    airt_data = DataObj(
        path='/home/fabio/Desktop/shybox/dset/itwater',
        file_name='T_19810101.nc',
        file_template={
            "format": "netcdf", "type": "grid",
            "dims_geo": {"lon": "longitude", "lat": "latitude", "nt": "time"},
            "vars_data": {"Tair": "air_temperature"}
        },
        time_signature='period',
        time_reference='1981-01-01 00:00', time_period=24, time_freq='h', time_direction='forward',
    )

    rh_data = DataObj(
        path='/home/fabio/Desktop/shybox/dset/itwater',
        file_name='U_19810101.nc',
        file_template={
            "format": "netcdf", "type": "grid",
            "dims_geo": {"lon": "longitude", "lat": "latitude", "nt": "time"},
            "vars_data": {"RH": "relative_humidity"}
        },
        time_signature='period',
        time_reference='1981-01-01 00:00', time_period=24, time_freq='h', time_direction='forward',
    )

    geo_data = DataObj(
        path='/home/fabio/Desktop/shybox/dset/data_static/gridded/',
        file_name='marche.dem.txt',
        file_mode='grid',
        file_template={
            "format": "ascii", "type": "grid",
            "dims_geo": {"x": "longitude", "y": "latitude"},
            "vars_geo": {"x": "longitude", "y": "latitude"}
        },
        time_signature=None
    )

    output_data = DataObj(
        path='/home/fabio/Desktop/shybox/dset/itwater',
        file_name='test_%Y%m%d%H%M.nc', time_signature='step',
        file_mode='grid',
        file_template={
            "format": "netcdf", "type": "grid",
            "dims_geo": {"longitude": "X", "latitude": "Y", "time": "time"},
            "vars_geo": {"longitude": "X", "latitude": "Y"},
            "vars_data": {"air_temperature": "AIR_TEMPERATURE"}
        },
        name='AirT')


    orc_process = Orchestrator.multi_variable(
        data_package={'airt':airt_data, 'rh': rh_data}, data_out=output_data,
        data_ref=geo_data,
        configuration=configuration['WORKFLOW']
    )

    orc_process.run(time=pd.date_range('1981-01-01 05:00', '1981-01-01 07:00', freq='H'))

    orc_process = Orchestrator(
        data_in=airt_data, data_out=output_data,
        options={
            "intermediate_output": "Tmp",
            "tmp_dir": "/home/fabio/Desktop/shybox/dset/itwater/tmp/"
        })

    orc_process.add_process(
        interpolate_data, ref=geo_data,
        method='nn', max_distance=25000, neighbours=7, fill_value=np.nan)
    #orc_process.add_process(mask_data_by_limits, mask_min=0, mask_max=10, mask_no_data=np.nan)
    orc_process.add_process(mask_data_by_ref, ref=geo_data, ref_value=-9999, mask_no_data=np.nan)

    orc_process.run(time=pd.date_range('1981-01-01 05:00', '1981-01-01 07:00', freq='H'))


    print()

    #row_start, row_end, col_start, col_end = 0, 9, 3, 15
    row_start, row_end, col_start, col_end = None, None, None, None

    driver_geo_data = DrvData.by_template(
        file_name=alg_collectors_settings.get('geo_name', None),
        file_time=alg_collectors_settings.get('geo_time', None),
        file_template=alg_collectors_settings.get('geo_template', None)
    )
    # get variable data
    geo_data = driver_geo_data.get_variable_data()

    driver_dyn_data = MultiData.by_iterable(
        file_iterable=alg_collectors_settings.get('file_name', None),
        file_time=alg_collectors_settings.get('file_time', None),
        file_template=alg_collectors_settings.get('file_template', None)
    )

    # get variable data
    dyn_data = driver_dyn_data.get_variable_data()


    orc_process = Orchestrator(
        data_in=dyn_data, data_ref=geo_data, data_out=None, options={})

    orc_process.add_process(
        interpolate_data, ref=geo_data,
        method='nn', max_distance=25000, neighbours=7, fill_value=np.nan)
    #orc_process.add_process(mask_data, ref=geo_data)

    orc_process.run(time=pd.date_range('1981-01-01 05:00', '1981-01-01 07:00', freq='H'))

    ## end test

    driver_data = DrvData.by_template(
        file_name=alg_collectors_settings.get('file_name', None),
        file_time=alg_collectors_settings.get('file_time', None),
        file_template=alg_collectors_settings.get('file_template', None)
    )

    # get variable data
    file_data = driver_data.get_variable_data()
    # select variable data
    file_data = driver_data.select_data(file_data)

    # ------------------------------------------------------------------------------------------------------------------
    # driver data
    driver_data = DrvData.by_file_generic(
        file_name=alg_collectors_settings.get('file_name', None),
        file_time=alg_collectors_settings.get('file_time', None),
        file_template=alg_collectors_settings.get('file_template', None)
    )

    # get variable data
    file_data = driver_data.get_variable_data()
    # select variable data
    file_data = driver_data.select_data(file_data)


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

    collector_vars = {
        "file_name": [
            '/home/fabio/Desktop/shybox/dset/itwater/T_19810101.nc',
            '/home/fabio/Desktop/shybox/dset/itwater/R_19810101.nc'
        ],
        "file_time": None,
        "file_template": "/home/fabio/Desktop/shybox/shybox/dataset_toolkit/template/tmpl_netcdf_gridded_itwater.json",
        "geo_name": "/home/fabio/Desktop/shybox/dset/data_static/gridded/marche.dem.txt",
        "geo_template": "/home/fabio/Desktop/shybox/shybox/dataset_toolkit/template/tmpl_ascii_gridded_geo.json",
        "geo_time": None,
        'path_log': '$HOME/log', 'file_log': 'log.txt'
    }

    collector_vars = {
        "file_name": '/home/fabio/Desktop/shybox/dset/itwater/T_19810101.nc',
        "file_time": None,
        "file_template": "/home/fabio/Desktop/shybox/shybox/dataset_toolkit/template/tmpl_netcdf_gridded_itwater.json",
        "geo_name": "/home/fabio/Desktop/shybox/dset/data_static/gridded/marche.dem.txt",
        "geo_template": "/home/fabio/Desktop/shybox/shybox/dataset_toolkit/template/tmpl_ascii_gridded_geo.json",
        "geo_time": None,
        'path_log': '$HOME/log', 'file_log': 'log.txt'
    }

    main(alg_collectors_settings=collector_vars)
# ----------------------------------------------------------------------------------------------------------------------
