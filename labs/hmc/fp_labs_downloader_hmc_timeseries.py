"""
HMC - Downloader HMC time-series

__date__ = '20220926'
__version__ = '1.2.0'
__author__ = 'Fabio Delogu (fabio.delogu@cimafoundation.org'
__library__ = 'fp-hmc'

General command line:
python fp_labs_downloader_hmc_timeseries.py -settings_file configuration.json

Version(s):
20220926 (1.2.0) --> Training release
20211109 (1.0.0) --> Beta release
"""
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Complete library
import os
import time
import logging
from argparse import ArgumentParser

from library.common.lib_utils_logging import set_logging_file

from library.common.lib_data_io_json import read_file_settings
from library.common.lib_data_io_generic import define_file_path_downloader, define_file_obj, define_keys_list, \
    get_path_root, get_path_home

from library.common.lib_utils_ssh import info_machine, check_connection
from library.common.lib_data_organizer import collect_datasets

from library.common.lib_info_args import logger_name, logger_format

# Logging
log_stream = logging.getLogger(logger_name)
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Algorithm information
project_name = 'HMC'
alg_name = 'Time-Series Downloader Tool'
alg_type = 'Labs'
alg_version = '1.2.0'
alg_release = '2022-09-26'
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Script Main
def main(file_name_settings_default="fp_labs_downloader_hmc_timeseries.json"):

    # -------------------------------------------------------------------------------------
    # Select settings algorithm file
    file_name_settings_selected = get_args(file_name_settings_default)
    # Read data from settings algorithm file
    obj_settings = read_file_settings(file_name_settings_selected)

    # Set algorithm logging
    set_logging_file(
        logger_name=logger_name,
        logger_formatter=logger_format,
        logger_file=os.path.join(obj_settings['log']['folder_name'], obj_settings['log']['file_name']))
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Info algorithm
    print(' INFO ============================================================================ ')
    print(' INFO [' + project_name + ' ' + alg_type + ' - ' + alg_name + ' (Version ' + alg_version +
          ' - Release ' + alg_release + ')]')
    print(' INFO ==> START ... ')
    print(' ')

    # Time algorithm information
    alg_time_start = time.time()
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Define objects
    obj_generic = obj_settings['generic']
    obj_template_path = obj_settings['template']['path']
    obj_template_time = obj_settings['template']['time']
    obj_template_data = obj_settings['template']['data']
    obj_template_transfer = obj_settings['template']['transfer']
    obj_data_static_source = obj_settings['data']['static']['source']
    obj_data_static_destination = obj_settings['data']['static']['destination']
    obj_data_dynamic_source = obj_settings['data']['dynamic']['source']
    obj_data_dynamic_destination = obj_settings['data']['dynamic']['destination']

    info_analysis_name = obj_settings['generic']['analysis_name']
    info_transfer_method = obj_settings['generic']['transfer_methods']

    machine_address, machine_user, machine_ip, machine_type = info_machine(obj_settings['generic']['machine'])
    machine_connection = check_connection(machine_ip, machine_type)

    obj_machine = {'machine_address': machine_address, "machine_user": machine_user, "machine_ip": machine_ip,
                   'machine_type': machine_type, 'machine_connection': machine_connection}
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Define static and dynamic path root
    file_path_home = get_path_home(obj_generic['path_home'])
    file_path_root_static_source = get_path_root(
        obj_generic['path_root_data_static_source'], home_path_string=file_path_home)
    file_path_root_static_destination = get_path_root(
        obj_generic['path_root_data_static_destination'], home_path_string=file_path_home)
    file_path_root_dynamic_source = get_path_root(
        obj_generic['path_root_data_dynamic_source'], home_path_string=file_path_home)
    file_path_root_dynamic_destination = get_path_root(
        obj_generic['path_root_data_dynamic_destination'], home_path_string=file_path_home)
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Define file static path(s)
    obj_filled_path = dict.fromkeys(list(obj_template_path.keys()), file_path_root_static_source)
    file_path_static_source = define_file_path_downloader(
        obj_data_static_source,
        path_template_raw=obj_template_path, path_template_values=obj_filled_path, tag_file_time=None)[0]

    obj_filled_path = dict.fromkeys(list(obj_template_path.keys()), file_path_root_static_destination)
    file_path_static_destination = define_file_path_downloader(
        obj_data_static_destination,
        path_template_raw=obj_template_path, path_template_values=obj_filled_path, tag_file_time=None)[0]

    transfer_method_static_source = define_file_obj(obj_data_static_source, tag_file_obj='transfer_method')
    flag_active_static_source = define_file_obj(obj_data_static_source, tag_file_obj='flag_active')
    flag_update_static_source = define_file_obj(obj_data_static_source, tag_file_obj='flag_update')

    datasets_static_keys = define_keys_list(file_path_static_source, file_path_static_destination)
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Define file dynamic path(s)
    obj_filled_path = dict.fromkeys(list(obj_template_path.keys()), file_path_root_dynamic_source)
    file_path_dynamic_source = define_file_path_downloader(
        obj_data_dynamic_source,
        geo_template_raw=obj_template_path, geo_template_values=None,
        time_template_raw=None, time_template_values=None, tag_file_time=None,
        path_template_raw=obj_template_path, path_template_values=obj_filled_path)[0]

    obj_filled_path = dict.fromkeys(list(obj_template_path.keys()), file_path_root_dynamic_destination)
    file_path_dynamic_destination = define_file_path_downloader(
        obj_data_dynamic_destination,
        geo_template_raw=obj_template_path, geo_template_values=None,
        time_template_raw=None, time_template_values=None, tag_file_time=None,
        path_template_raw=obj_template_path, path_template_values=obj_filled_path)[0]

    time_reference_dynamic_source = define_file_obj(obj_data_dynamic_source, tag_file_obj='time_reference')
    time_period_dynamic_source = define_file_obj(obj_data_dynamic_source, tag_file_obj='time_period')
    time_frequency_dynamic_source = define_file_obj(obj_data_dynamic_source, tag_file_obj='time_frequency')
    transfer_method_dynamic_source = define_file_obj(obj_data_dynamic_source, tag_file_obj='transfer_method')
    flag_active_dynamic_source = define_file_obj(obj_data_dynamic_source, tag_file_obj='flag_active')
    flag_update_dynamic_source = define_file_obj(obj_data_dynamic_source, tag_file_obj='flag_update')
    ensemble_n_dynamic_source = define_file_obj(obj_data_dynamic_source, tag_file_obj='ensemble_n')

    datasets_dynamic_keys = define_keys_list(file_path_dynamic_source, file_path_dynamic_destination)
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Method to collect static file from source to destination
    collect_datasets(
        datasets_static_keys,
        file_path_static_source, file_path_static_destination,
        None, None, None,
        flag_active_static_source, flag_update_static_source, None,
        transfer_method_static_source, info_transfer_method,
        obj_template_time, obj_template_transfer, obj_machine)
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Method to collect dynamic file from source to destination
    collect_datasets(
        datasets_dynamic_keys,
        file_path_dynamic_source, file_path_dynamic_destination,
        time_reference_dynamic_source, time_period_dynamic_source, time_frequency_dynamic_source,
        flag_active_dynamic_source, flag_update_dynamic_source, ensemble_n_dynamic_source,
        transfer_method_dynamic_source, info_transfer_method,
        obj_template_time, obj_template_transfer, obj_machine)
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Info algorithm
    alg_time_elapsed = round(time.time() - alg_time_start, 1)

    print(' ')
    print(' INFO [' + project_name + ' ' + alg_type + ' - ' + alg_name + ' (Version ' + alg_version +
          ' - Release ' + alg_release + ')]')
    print(' INFO ==> TIME ELAPSED: ' + str(alg_time_elapsed) + ' seconds')
    print(' INFO ==> ... END')
    print(' INFO ==> Bye, Bye')
    print(' INFO ============================================================================ ')
    # -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to get script argument(s)
def get_args(file_name_default):

    parser_handle = ArgumentParser()
    parser_handle.add_argument('-settings_file', action="store", dest="file_name_args")
    parser_values = parser_handle.parse_args()

    if parser_values.file_name_args:
        file_name_selected = parser_values.file_name_args
    else:
        file_name_selected = file_name_default

    return file_name_selected

# -------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------
# Call script from external library
if __name__ == "__main__":
    file_name_settings_default = "fp_labs_downloader_hmc_timeseries.json"
    main(file_name_settings_default=file_name_settings_default)
# ----------------------------------------------------------------------------
