#!/usr/bin/python3

"""
RULER LIBRARY - RUntime anaLyzER Tool

__date__ = '20221227'
__version__ = '1.0.0'
__author__ = 'Fabio Delogu (fabio.delogu@cimafoundation.org)'
__library__ = 'ruler'

General command line:
python ruler_main.py -settings_file configuration.json -time "YYYY-MM-DD HH:MM"

Version(s):
20221227 (1.0.0) --> Beta release based on https://github.com/giampaolo/psutil
"""
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Complete library
import logging
import argparse
import time
import os

from .lib_info_args import logger_name, time_format_algorithm

from .lib_data_io import read_json
from .lib_utils_time import set_time
from .lib_utils_logging import set_logging_file

from .driver_analysis import DriverAnalysis

# Logging
log_stream = logging.getLogger(logger_name)
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Algorithm information
project_name = 'RULER'
alg_name = 'REPORT ANALYSIS'
alg_type = 'Package'
alg_version = '1.0.0'
alg_release = '2022-12-27'
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Script Main
def main():

    # -------------------------------------------------------------------------------------
    # Get algorithm settings
    alg_file_settings, alg_time = get_args()

    # Set algorithm settings
    alg_data_settings = read_json(alg_file_settings)

    # Set algorithm logging
    set_logging_file(
        logger_name=logger_name,
        logger_file=os.path.join(alg_data_settings['log']['folder_name'], alg_data_settings['log']['file_name']))
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Info algorithm
    log_stream.info(' ============================================================================ ')
    log_stream.info('[' + project_name + ' ' + alg_type + ' - ' + alg_name + ' (Version ' + alg_version +
                    ' - Release ' + alg_release + ')]')
    log_stream.info(' ==> START ... ')
    log_stream.info(' ')

    # Time algorithm information
    alg_time_start = time.time()
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Create time object
    alg_time_report, alg_time_range, alg_time_chunks = set_time(
        time_run_args=alg_time,
        time_run_file=alg_data_settings['time']['time_run'],
        time_format=time_format_algorithm,
        time_period=alg_data_settings['time']['time_period'],
        time_frequency=alg_data_settings['time']['time_frequency'],
        time_rounding=alg_data_settings['time']['time_rounding']
    )
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Create analysis object
    drv_report_analysis = DriverAnalysis(
        report_time=alg_time_report,
        dict_algorithm=alg_data_settings['algorithm'],
        dict_process=alg_data_settings['process'],
        dict_report=alg_data_settings['report'])
    # method to organize report analysis
    drv_report_analysis.organize()
    # method to dump report analysis
    drv_report_analysis.dump()
    # method to view report analysis
    drv_report_analysis.view()
    # method to view report analysis
    drv_report_analysis.kill()
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Info algorithm
    alg_time_elapsed = round(time.time() - alg_time_start, 1)

    log_stream.info(' ')
    log_stream.info('[' + project_name + ' ' + alg_type + ' - ' + alg_name + ' (Version ' + alg_version +
                    ' - Release ' + alg_release + ')]')
    log_stream.info(' ==> TIME ELAPSED: ' + str(alg_time_elapsed) + ' seconds')
    log_stream.info(' ==> ... END')
    log_stream.info(' ==> Bye, Bye')
    log_stream.info(' ============================================================================ ')
    # -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to get script argument(s)
def get_args():
    parser_handle = argparse.ArgumentParser()
    parser_handle.add_argument('-settings_file', action="store", dest="alg_settings")
    parser_handle.add_argument('-time', action="store", dest="alg_time")
    parser_values = parser_handle.parse_args()

    alg_settings, alg_time = 'configuration.json', None
    if parser_values.alg_settings:
        alg_settings = parser_values.alg_settings
    if parser_values.alg_time:
        alg_time = parser_values.alg_time

    return alg_settings, alg_time

# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Call script from external library
if __name__ == "__main__":
    main()
# -------------------------------------------------------------------------------------
