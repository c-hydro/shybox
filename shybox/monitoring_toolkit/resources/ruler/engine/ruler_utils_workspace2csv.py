#!/usr/bin/python3

"""
RULER LIBRARY - UTILS - Converter Workspace 2 CSV

__date__ = '20221227'
__version__ = '1.0.0'
__author__ = 'Fabio Delogu (fabio.delogu@cimafoundation.org)'
__library__ = 'ruler'

General command line:
python ruler_utils_workspace2csv.py -settings_file configuration.json -time "YYYY-MM-DD HH:MM"

Version(s):
20221227 (1.0.0) --> Beta release based on https://github.com/giampaolo/psutil
"""
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Complete library
import logging
import argparse
import time
import glob
import os
import pandas as pd

from copy import deepcopy

from .lib_info_args import logger_name
from .lib_data_io import read_json, read_obj, write_csv
from .lib_utils_system import fill_tags2string, make_folder
from .lib_utils_logging import set_logging_file

# Logging
log_stream = logging.getLogger(__name__)
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Algorithm information
project_name = 'RULER'
alg_name = 'WORKSPACE 2 CSV UTILS'
alg_type = 'Package'
alg_version = '1.0.0'
alg_release = '2022-12-27'
# Algorithm settings
report_file_delimiter = ';'
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Script Main
def main():

    # -------------------------------------------------------------------------------------
    # Get algorithm settings
    report_file_settings, report_time = get_args()

    # Set algorithm settings
    report_data_settings = read_json(report_file_settings)

    # Set algorithm logging
    set_logging_file(
        logger_name=logger_name,
        logger_file=os.path.join(report_data_settings['log']['folder_name'], report_data_settings['log']['file_name']))
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
    # get analysis information start
    log_stream.info(' ----> Get analysis information ... ')

    # Get report flags and template object(s)
    report_obj_flags = report_data_settings['algorithm']['flags']
    report_obj_template = report_data_settings['algorithm']['template']
    # Get report process object(s)
    report_obj_process = report_data_settings['process']
    # Get folder and file name(s) object(s)
    report_obj_src = report_data_settings['report']['source']
    report_obj_dst = report_data_settings['report']['destination']

    # get analysis information end
    log_stream.info(' ----> Get analysis information ... DONE')
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # define analysis file start
    log_stream.info(' ----> Define analysis file(s) ... ')

    # define source file
    report_file_path_src_raw = define_file_name(
        os.path.join(report_obj_src['folder_name'], report_obj_src['file_name']),
        report_template=report_obj_template, report_time=report_time)
    # define destination file
    report_file_path_dst = define_file_name(
        os.path.join(report_obj_dst['folder_name'], report_obj_dst['file_name']),
        report_template=report_obj_template, report_time=report_time,
        report_process_name=report_obj_process['destination_name'])

    # clean saved analysis file
    if report_obj_flags['clean_file_destination']:
        if os.path.exists(report_file_path_dst):
            os.remove(report_file_path_dst)

    # define analysis file end
    log_stream.info(' ----> Define analysis file(s) ... DONE')
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # get analysis file start
    log_stream.info(' ----> Get analysis file(s) ... ')

    # search and merge src filename(s)
    report_file_path_src_list = search_file_name(
        report_file_path_src_raw,
        report_template=report_obj_template, report_time=report_time,
        report_id='*', report_process_name=report_obj_process['source_name'])

    # check analysis file availability
    if report_file_path_src_list:
        if not os.path.exists(report_file_path_dst):

            # iterate on filename(s)
            report_analysis_collections = None
            for report_file_path_src_step in report_file_path_src_list:

                # get analysis file
                log_stream.info(' -----> File "' + report_file_path_src_step + '" ... ')
                # read ancillary analysis file
                if os.path.exists(report_file_path_src_step):

                    # read analysis file
                    report_analysis_step = read_obj(report_file_path_src_step)

                    # remove columns (problems in writing end file)
                    report_columns_to_drop = []
                    for report_column_name in list(report_analysis_step.columns):
                        if 'cpu_affinity' in report_column_name:
                            report_columns_to_drop.append(report_column_name)
                    report_analysis_step = report_analysis_step.drop(report_columns_to_drop, axis=1)

                    # merge analysis dframe
                    if report_analysis_collections is None:
                        report_analysis_collections = deepcopy(report_analysis_step)
                    else:
                        report_analysis_collections = pd.concat([report_analysis_collections, report_analysis_step], axis=0)

                    log_stream.info(' -----> File "' + report_file_path_src_step + '" ... DONE')
                else:
                    log_stream.warning(' ===> File not found')
                    log_stream.info(' -----> File "' + report_file_path_src_step + '" ... SKIPPED')

            # sort dataframe in ascending mode
            report_analysis_collections = report_analysis_collections.sort_index(ascending=True)

            # organize file end
            log_stream.info(' ----> Get analysis file(s) ... DONE')

        else:
            # organize file end
            report_analysis_collections = None
            log_stream.info(' ----> Get analysis file(s) ... SKIPPED. File(s) previously organized')
    else:
        # organize file end
        report_analysis_collections = None
        log_stream.info(' ----> Get analysis file(s) ... FAILED. DataFrame is not available')
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # dump analysis file start
    log_stream.info(' ----> Dump analysis file  ... ')

    # check analysis file availability
    if report_analysis_collections is not None:
        if not os.path.exists(report_file_path_dst):

            # dump analysis data
            log_stream.info(' -----> File "' + report_file_path_dst + '" ... ')

            report_folder_name_dst, report_file_name_dst = os.path.split(report_file_path_dst)
            make_folder(report_folder_name_dst)

            write_csv(report_file_path_dst, report_analysis_collections, file_separator=report_file_delimiter)
            log_stream.info(' -----> File "' + report_file_path_dst + '" ... DONE')

            # Dump analysis file end
            log_stream.info(' ----> Dump analysis file  ... DONE')

        else:
            # Dump analysis file end
            log_stream.info(' ----> Dump analysis file  ... SKIPPED. File previously dumped')
    else:
        # Dump analysis file end
        if not os.path.exists(report_file_path_dst):
            log_stream.info(' ----> Dump analysis file  ... FAILED. DataFrame is not available')
        else:
            log_stream.info(' ----> Dump analysis file  ... SKIPPED. File previously dumped')
            report_analysis_collections = pd.read_csv(report_file_path_dst, delimiter=report_file_delimiter)

            print('ciao')

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
# Method to search filename(s)
def search_file_name(file_path_raw, file_path_reverse=False, file_path_sorted=True, report_process_name=None, report_template=None,
                     report_time=None, report_type=None, report_id=None):

    file_path_def = define_file_name(
        file_path_raw, report_process_name=report_process_name, report_template=report_template,
        report_time=report_time, report_type=report_type, report_id=report_id)

    file_path_list = glob.glob(file_path_def)

    if file_path_sorted:
        file_path_list = sorted(file_path_list)

    if file_path_reverse:
        file_path_list = file_path_reverse[::-1]

    return file_path_list
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to define file name
def define_file_name(file_name_raw, report_process_name=None,
                     report_template=None, report_time=None, report_type=None, report_id=None):

    report_time_stamp = None
    if report_time is not None:
        report_time_stamp = pd.Timestamp(report_time)

    if report_id is not None:
        if isinstance(report_id, (int, float)):
            report_id = str(int(report_id))
        elif isinstance(report_id, str):
            pass
        else:
            log_stream.error(' ===> Report id format is not supported')
            raise NotImplemented('Case not implemented yet')

    report_template_keys = deepcopy(report_template)
    report_template_values = {
        'process_name': report_process_name,
        'report_type': report_type,
        "report_id": report_id,
        'report_sub_path': report_time_stamp,
        'report_datetime': report_time_stamp
    }

    file_name_def = fill_tags2string(file_name_raw, report_template_keys, report_template_values)[0]

    return file_name_def

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
