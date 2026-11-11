"""
FP-Labs - Analyzer HMC maps

__date__ = '20220926'
__version__ = '1.2.0'
__author__ = 'Fabio Delogu (fabio.delogu@cimafoundation.org'
__library__ = 'fp-labs'

General command line:
python fp_labs_analyzer_hmc_maps.py -settings_file configuration.json

Version(s):
20220926 (1.2.0) --> Update and refactor code(s)
20211115 (1.1.0) --> Update and refactor code(s) in according to the hmc flood proofs package v.3.1.5
20210113 (1.0.0) --> Beta release
"""
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Complete library
import os
import pandas as pd
import sys
from argparse import ArgumentParser

from library.common.lib_info_args import logger_name, logger_format
from library.common.lib_utils_logging import set_logging_file

from library.common.lib_data_io_json import read_file_settings
from library.common.lib_data_io_generic import define_file_path_analyzer, \
    define_file_time, define_file_map, define_file_variables, \
    create_darray_map, \
    get_path_root, get_path_home, set_info_mode, \
    convert_tstamp_2_tstr, define_folder_time

from library.common.lib_data_geo_ascii import read_data_grid
from library.common.lib_data_geo_shapefile import read_data_section

from library.common.lib_data_io_netcdf import read_file_map

from library.common.lib_graph_map import plot_map_var

from library.common.lib_utils_time import create_time_range
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Script Main
def main(file_name_settings="fp_labs_analyzer_hmc_maps.json"):

    # Select settings algorithm file
    file_name_settings, file_root_path = get_args(file_name_settings_default=file_name_settings)
    # Read data from settings algorithm file
    obj_settings = read_file_settings(file_name_settings)

    # Set algorithm logging
    set_logging_file(
        logger_name=logger_name,
        logger_formatter=logger_format,
        logger_file=os.path.join(obj_settings['log']['folder_name'], obj_settings['log']['file_name']))

    # Define objects
    obj_generic = obj_settings['generic']
    obj_variables_forcing = obj_settings['variables']['forcing']
    obj_variables_results = obj_settings['variables']['results']
    obj_template_path = obj_settings['template']['path']
    obj_template_geo = obj_settings['template']['geo']
    obj_template_var = obj_settings['template']['variable']
    obj_template_time = obj_settings['template']['time']
    obj_data_static = obj_settings['data']['static']
    obj_data_dynamic_source = obj_settings['data']['dynamic']['source']
    obj_data_dynamic_destination = obj_settings['data']['dynamic']['destination']

    info_mode, info_domain_name = set_info_mode(obj_settings['generic'], tag_type='maps')

    info_time_analysis = obj_settings['case_study']['time_analysis']
    info_time_start = obj_settings['case_study']['time_start']
    info_time_end = obj_settings['case_study']['time_end']
    info_time_period_left = obj_settings['case_study']['time_period_left']
    info_time_period_right = obj_settings['case_study']['time_period_right']
    info_description_analysis = obj_settings['case_study']['description']

    # Define analysis time range
    info_time_db_analysis_period, info_time_str_analysis_period = create_time_range(
        time_analysis=info_time_analysis, time_start=info_time_start, time_end=info_time_end,
        time_analysis_left_period=info_time_period_left, time_analysis_right_period=info_time_period_right)
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Define static and dynamic path root
    file_path_home = get_path_home(obj_generic['path_home'])
    file_path_root_static = get_path_root(obj_generic['path_root_data_static'], home_path_string=file_path_home)
    file_path_root_dynamic_source = get_path_root(
        obj_generic['path_root_data_dynamic_source'], home_path_string=file_path_home)
    file_path_root_dynamic_destination = get_path_root(
        obj_generic['path_root_data_dynamic_destination'], home_path_string=file_path_home)
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Define file static path(s)
    obj_filled_path = dict.fromkeys(list(obj_template_path.keys()), file_path_root_static)
    file_path_dset_static = define_file_path_analyzer(
        obj_data_static, path_template_raw=obj_template_path, path_template_values=obj_filled_path,
        tag_file_time=None)[0]

    # Read terrain datasets
    darray_terrain = read_data_grid(file_path_dset_static['terrain'], var_limit_min=0, var_limit_max=None)
    # Read river network datasets
    darray_river_network = read_data_grid(file_path_dset_static['river_network'], var_limit_min=0, var_limit_max=1)
    # Read sections shapefile
    dframe_section = read_data_section(file_path_dset_static['sections'])

    # Create geographical object
    obj_filled_geo = {'domain_name': info_domain_name}

    # Define variables
    run_variables_forcing = define_file_variables(obj_variables_forcing, obj_filled_geo)
    run_variables_results = define_file_variables(obj_variables_results, obj_filled_geo)
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Fill dynamic file path(s)
    obj_filled_path = dict.fromkeys(list(obj_template_path.keys()), file_path_root_dynamic_source)

    # Get time db and reference
    info_time_db_dynamic_source_generic, info_time_ref_dynamic_source_generic = define_file_time(
        obj_data_dynamic_source,
        time_format_path='%Y%m%d',
        geo_template_raw=obj_template_path, geo_template_values=obj_filled_geo,
        time_template_raw=obj_template_time, time_template_values=None,
        path_template_raw=obj_template_path, path_template_values=obj_filled_path)

    time_folder_list_forcing = convert_tstamp_2_tstr(info_time_db_dynamic_source_generic['maps_forcing'])
    time_folder_list_results = convert_tstamp_2_tstr(info_time_db_dynamic_source_generic['maps_results'])

    time_folder_select_forcing = time_folder_list_forcing[0]
    time_folder_select_results = time_folder_list_results[0]

    folder_name_forcing_generic = obj_data_dynamic_source['maps_forcing']['folder_name']
    obj_filled_time = {'maps_sub_path_forcing': pd.Timestamp(time_folder_select_forcing)}
    time_file_list_forcing = define_folder_time(folder_name_forcing_generic,
                                                obj_filled_time, obj_filled_path,
                                                obj_template_time, obj_template_path)

    folder_name_results_generic = obj_data_dynamic_source['maps_results']['folder_name']
    obj_filled_time = {'maps_sub_path_results': pd.Timestamp(time_folder_select_results)}
    time_file_list_results = define_folder_time(folder_name_results_generic,
                                                obj_filled_time, obj_filled_path,
                                                obj_template_time, obj_template_path)

    time_file_select_forcing = time_file_list_forcing[0]
    time_file_select_results = time_file_list_results[0]

    # Define dynamic source folder(s)
    obj_data_dynamic_source_forcing = {'maps_forcing': obj_data_dynamic_source['maps_forcing']}
    file_path_dynamic_source_forcing = define_file_path_analyzer(
        obj_data_dynamic_source_forcing, tag_file_time='time_reference',
        geo_template_raw=obj_template_path, geo_template_values=obj_filled_geo,
        time_template_raw=obj_template_time, time_template_values={'time_reference': time_file_select_forcing},
        path_template_raw=obj_template_path, path_template_values=obj_filled_path)[0]

    obj_data_dynamic_source_results = {'maps_results': obj_data_dynamic_source['maps_results']}
    file_path_dynamic_source_results = define_file_path_analyzer(
        obj_data_dynamic_source_results, tag_file_time='time_reference',
        geo_template_raw=obj_template_path, geo_template_values=obj_filled_geo,
        time_template_raw=obj_template_time, time_template_values={'time_reference': time_file_select_results},
        path_template_raw=obj_template_path, path_template_values=obj_filled_path)[0]

    # Define dynamic destination folder(s)
    obj_filled_path = dict.fromkeys(list(obj_template_path.keys()), file_path_root_dynamic_destination)

    obj_data_dynamic_destination_forcing = {'maps_forcing': obj_data_dynamic_destination['maps_forcing']}
    file_path_dset_dynamic_destination_forcing, file_act_dset_dynamic_destination_forcing = define_file_path_analyzer(
        obj_data_dynamic_destination_forcing, tag_file_time='time_reference',
        geo_template_raw=obj_template_geo, geo_template_values=obj_filled_geo,
        time_template_raw=obj_template_time, time_template_values={'time_reference': time_file_select_forcing},
        path_template_raw=obj_template_path, path_template_values=obj_filled_path)

    obj_data_dynamic_destination_results = {'maps_results': obj_data_dynamic_destination['maps_results']}
    file_path_dset_dynamic_destination_results, file_act_dset_dynamic_destination_results = define_file_path_analyzer(
        obj_data_dynamic_destination_results, tag_file_time='time_reference',
        geo_template_raw=obj_template_geo, geo_template_values=obj_filled_geo,
        time_template_raw=obj_template_time, time_template_values={'time_reference': time_file_select_results},
        path_template_raw=obj_template_path, path_template_values=obj_filled_path)
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Read forcing datasets map
    obj_maps_dynamic_forcing = read_file_map(
        file_path_dynamic_source_forcing['maps_forcing'], file_vars_expected=run_variables_forcing)
    # Read results datasets map
    obj_maps_dynamic_results = read_file_map(
        file_path_dynamic_source_results['maps_results'], file_vars_expected=run_variables_results)
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Extract variables from forcing datasets
    darray_map_rain, attrs_map_rain = create_darray_map(
        obj_maps_dynamic_forcing, var_name_in='Rain', var_name_out='rain')
    darray_map_airt, attrs_map_airt = create_darray_map(
        obj_maps_dynamic_forcing, var_name_in='Air_Temperature', var_name_out='air_temperature')
    darray_map_rh, attrs_map_rh = create_darray_map(
        obj_maps_dynamic_forcing, var_name_in='Relative_Humidity', var_name_out='relative_humidity')

    # Extract variables from results datasets
    darray_map_sm, attrs_map_sm = create_darray_map(
        obj_maps_dynamic_results,  var_name_in='SM', var_name_out='soil_moisture')
    darray_map_lst, attrs_map_lst = create_darray_map(
        obj_maps_dynamic_results, var_name_in='LST', var_name_out='land_surface_temperature')
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Plot map soil moisture
    file_name_map_sm = define_file_map(
        file_path_dset_dynamic_destination_results['maps_results'], var_name='soil_moisture')
    plot_map_var(file_name_map_sm, darray_map_sm, info_time_analysis,
                 var_name_data='soil_moisture', var_units='[-]',
                 var_name_geo_x='Longitude', var_name_geo_y='Latitude',
                 var_limit_min=0, var_limit_max=1)

    # Plot map air temperature
    file_name_map_airt = define_file_map(
        file_path_dset_dynamic_destination_forcing['maps_forcing'], var_name='air_temperature')

    plot_map_var(file_name_map_airt, darray_map_airt, info_time_analysis,
                 var_name_data='air_temperature', var_units='[C]',
                 var_name_geo_x='longitude', var_name_geo_y='latitude')
    # -------------------------------------------------------------------------------------

    print()

# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to get script argument(s)
def get_args(file_name_settings_default='fp_labs_analyzer_hmc_maps.json'):

    parser_handle = ArgumentParser()
    parser_handle.add_argument('-settings_file', action="store", dest="file_name_args")
    parser_values = parser_handle.parse_args()

    if parser_values.file_name_args:
        file_name_settings = parser_values.file_name_args
    else:
        file_name_settings = file_name_settings_default

    file_path_settings = os.path.dirname(os.path.realpath(sys.argv[0]))

    return file_name_settings, file_path_settings

# -------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------
# Call script from external library
if __name__ == "__main__":
    file_name_settings_default = "fp_labs_analyzer_hmc_maps.json"
    main(file_name_settings_default)
# ----------------------------------------------------------------------------
