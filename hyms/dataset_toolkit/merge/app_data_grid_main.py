#!/usr/bin/python3
"""
HYMS PACKAGE - DATA GRID APP

__date__ = '20250123'
__version__ = '3.0.0'
__author__ =
    'Fabio Delogu (fabio.delogu@cimafoundation.org),
     Andrea Libertino (andrea.libertino@cimafoundation.org)'
__library__ = 'hyms'

General command line:
python app_data_grid_main.py -settings_file configuration.json -time "YYYY-MM-DD HH:MM"

Version(s):
20250123 (3.0.0) --> Beta release for hyms package
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

from hyms.generic_toolkit.lib_utils_args import get_logger_name
from hyms.generic_toolkit.lib_default_args import logger_name, logger_format, logger_arrow
from hyms.generic_toolkit.lib_default_args import collector_data

from hyms.dataset_toolkit.merge.driver_data_grid import DrvData, MultiData
from hyms.io_toolkit import io_handler_base

# set logger
logger_stream = logging.getLogger(get_logger_name(logger_name_mode='by_script', logger_name_default=logger_name))
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# algorithm information
project_name = 'hyms'
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

    print()




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

    # case 1 - dynamic defined file name
    collector_vars = {
        'file_name': '/home/fabio/Desktop/hyms/dset/data_source/s3m/marche/2025/01/24/S3M_202501240400.nc.gz',
        'path_log': '$HOME/log', 'file_log': 'log.txt',
    }

    # case 2 - dynamic undefined file name
    collector_vars = {
        "file_name": '/home/fabio/Desktop/hyms/dset/data_source/s3m/marche/{file_sub_path}/S3M_{file_datetime}.nc.gz',
        "file_time": '202501240400',
        "file_template": "",
        'path_log': '$HOME/log', 'file_log': 'log.txt'
    }

    # case 3 - static defined file name
    collector_vars = {
        "file_name": '/home/fabio/Desktop/hyms/dset/data_static/gridded/marche.dem.txt',
        "file_time": None,
        "file_template": "/home/fabio/Desktop/hyms/hyms/dataset_toolkit/template/tmpl_ascii_gridded_geo.json",
        'path_log': '$HOME/log', 'file_log': 'log.txt',
    }

    # case 4 - static undefined file name
    collector_vars = {
        "file_name": '/home/fabio/Desktop/hyms/dset/data_static/gridded/{domain_name}.dem.txt',
        "file_time": None,
        "file_template": "/home/fabio/Desktop/hyms/hyms/dataset_toolkit/template/tmpl_ascii_gridded_geo.json",
        'path_log': '$HOME/log', 'file_log': 'log.txt',
    }

    # case 5 - static defined file name
    collector_vars = {
        "file_name": [
            '/home/fabio/Desktop/hyms/dset/merge_data/EntellaDomain.dem.txt',
            '/home/fabio/Desktop/hyms/dset/merge_data/LevanteGenoveseDomain.dem.txt',
            '/home/fabio/Desktop/hyms/dset/merge_data/PonenteGenoveseDomain.dem.txt'
        ],
        "file_time": None,
        "file_template": "/home/fabio/Desktop/hyms/hyms/dataset_toolkit/template/tmpl_ascii_gridded_geo.json",
        'path_log': '$HOME/log', 'file_log': 'log.txt',
    }

    collector_vars = {
        "file_name": [
            '/home/fabio/Desktop/hyms/dset/itwater/T_19810101.nc',
            '/home/fabio/Desktop/hyms/dset/itwater/R_19810101.nc'
        ],
        "file_time": None,
        "file_template": "/home/fabio/Desktop/hyms/hyms/dataset_toolkit/template/tmpl_netcdf_gridded_itwater.json",
        "geo_name": "/home/fabio/Desktop/hyms/dset/data_static/gridded/marche.dem.txt",
        "geo_template": "/home/fabio/Desktop/hyms/hyms/dataset_toolkit/template/tmpl_ascii_gridded_geo.json",
        "geo_time": None,
        'path_log': '$HOME/log', 'file_log': 'log.txt'
    }

    # /home/fabio/Desktop/hyms/dset/merge_data/geo_liguria.tiff
    # /home/fabio/Desktop/hyms/dset/merge_data/LiguriaDomain.dem.txt
    # testing tags

    main(alg_collectors_settings=collector_vars)
# ----------------------------------------------------------------------------------------------------------------------
