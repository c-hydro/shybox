"""
FP-Labs - Analyzer HMC time-series

__date__ = '20220926'
__version__ = '1.2.0'
__author__ = 'Fabio Delogu (fabio.delogu@cimafoundation.org'
__library__ = 'fp_labs'

General command line:
python fp_labs_analyzer_hmc_timeseries.py -settings_file configuration.json

Version(s):
20220926 (1.2.0) --> Update and refactor code(s)
20211115 (1.1.0) --> Update and refactor code(s) in according to the hmc flood proofs package v.3.1.5
20210113 (1.0.0) --> Beta release
"""
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Complete library
import os
import sys
from argparse import ArgumentParser

from library.common.lib_info_args import logger_name, logger_format
from library.common.lib_utils_logging import set_logging_file

from library.common.lib_data_io_json import read_file_settings
from library.common.lib_data_io_generic import define_file_path_analyzer, \
    define_file_time, filter_file_time, select_file_time, \
    define_file_obj, define_file_variables, \
    get_path_root, get_path_home, set_info_mode

from library.common.lib_data_geo_ascii import read_data_grid
from library.common.lib_data_geo_shapefile import read_data_section, find_data_section, \
    create_tag_section, search_tag_section, validate_tag_section

from library.common.lib_data_io_netcdf import select_collections_var
from library.common.lib_data_organizer import wrap_ts, adjust_ts, wrap_file

from library.common.lib_graph_ts_utils import check_ts_workspace
from library.common.lib_graph_ts_obs import plot_ts_forcing_obs, plot_ts_discharge_obs
from library.common.lib_graph_ts_nwp import plot_ts_discharge_nwp_deterministic, plot_ts_discharge_nwp_probabilistic
from library.common.lib_graph_map import plot_map_terrain

from library.common.lib_utils_time import create_time_range
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Script Main
def main(file_name_settings="fp_labs_analyzer_hmc_timeseries.json"):

    # -------------------------------------------------------------------------------------
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
    obj_variables_section = obj_settings['variables']['section']
    obj_variables_domain = obj_settings['variables']['domain']
    obj_template_path = obj_settings['template']['path']
    obj_template_geo = obj_settings['template']['geo']
    obj_template_time = obj_settings['template']['time']
    obj_data_static = obj_settings['data']['static']
    obj_data_dynamic_source = obj_settings['data']['dynamic']['source']
    obj_data_dynamic_destination_workspace = obj_settings['data']['dynamic']['destination']
    obj_data_dynamic_destination_plot = obj_settings['data']['dynamic']['plot']

    info_mode, info_domain_name, info_basin_name, info_section_name = set_info_mode(obj_settings['generic'])

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
    file_path_root_dynamic_destination_workspace = get_path_root(
        obj_generic['path_root_data_dynamic_destination_workspace'], home_path_string=file_path_home)
    file_path_root_dynamic_destination_plot = get_path_root(
        obj_generic['path_root_data_dynamic_destination_plot'], home_path_string=file_path_home)
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

    # Create run section object(s)
    info_section_list, info_section_dict = create_tag_section(dframe_section)
    # Search run section id
    info_section_id = search_tag_section(info_section_list, basin_name=info_basin_name, section_name=info_section_name)

    # Validata section and basin info
    info_basin_name, info_section_name = validate_tag_section(
        info_section_list[info_section_id], basin_name=info_basin_name, section_name=info_section_name)
    # Get domain, section and time information
    info_section_attrs = find_data_section(
        dframe_section, section_name=info_section_name, basin_name=info_basin_name)
    # Create geographical object
    obj_filled_geo = {'section_name': info_section_name, 'basin_name': info_basin_name, 'domain_name': info_domain_name}

    # Define variables
    run_variables_section = define_file_variables(obj_variables_section, obj_filled_geo)
    run_variables_domain = define_file_variables(obj_variables_domain, obj_filled_geo)
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Fill dynamic file path(s)
    obj_filled_path = dict.fromkeys(list(obj_template_path.keys()), file_path_root_dynamic_source)

    # Get time db and reference
    info_time_db_dynamic_source_generic, info_time_ref_dynamic_source_generic = define_file_time(
        obj_data_dynamic_source,
        geo_template_raw=obj_template_path, geo_template_values=obj_filled_geo,
        time_template_raw=obj_template_time, time_template_values=None,
        path_template_raw=obj_template_path, path_template_values=obj_filled_path)

    # Filter time according to the analysis period (routine for interactive mode)
    info_time_db_dynamic_source_filtered, info_time_list_dynamic_source_filtered = filter_file_time(
        info_time_db_dynamic_source_generic, info_time_db_analysis_period)

    # Select time analysis (routine for cmd mode)
    info_time_dynamic_source_selected = select_file_time(
        info_time_db_dynamic_source_generic, info_time_ref_dynamic_source_generic,
        time_pivot='reference', time_value=None, time_random=True)

    # Define dynamic source folder(s)
    file_path_dynamic_source = define_file_path_analyzer(
        obj_data_dynamic_source, tag_file_time='time_dataset',
        geo_template_raw=obj_template_path, geo_template_values=obj_filled_geo,
        time_template_raw=obj_template_time, time_template_values={'time_dataset': info_time_dynamic_source_selected},
        path_template_raw=obj_template_path, path_template_values=obj_filled_path)[0]

    info_dynamic_source = define_file_obj(obj_data_dynamic_source, tag_file_obj='info')
    flags_dynamic_source = define_file_obj(obj_data_dynamic_source, tag_file_obj='flags')

    # Define dynamic destination folder(s)
    obj_filled_path = dict.fromkeys(list(obj_template_path.keys()), file_path_root_dynamic_destination_workspace)
    file_path_dset_dynamic_destination_workspace_section = define_file_path_analyzer(
        obj_data_dynamic_destination_workspace, tag_file_time='time_reference',
        geo_template_raw=obj_template_geo, geo_template_values=obj_filled_geo,
        time_template_raw=obj_template_time, time_template_values={'time_reference': info_time_analysis},
        path_template_raw=obj_template_path, path_template_values=obj_filled_path,
        tag_group='section_time_series')[0]

    file_path_dset_dynamic_destination_workspace_domain = define_file_path_analyzer(
        obj_data_dynamic_destination_workspace, tag_file_time='time_reference',
        geo_template_raw=obj_template_geo, geo_template_values=obj_filled_geo,
        time_template_raw=obj_template_time, time_template_values={'time_reference': info_time_analysis},
        path_template_raw=obj_template_path, path_template_values=obj_filled_path,
        tag_group='domain_time_series')[0]

    obj_filled_path = dict.fromkeys(list(obj_template_path.keys()), file_path_root_dynamic_destination_plot)
    file_path_dset_dynamic_destination_plot_section, file_act_dset_dynamic_destination_plot_section = define_file_path_analyzer(
        obj_data_dynamic_destination_plot, tag_file_time='time_reference',
        geo_template_raw=obj_template_geo, geo_template_values=obj_filled_geo,
        time_template_raw=obj_template_time, time_template_values={'time_reference': info_time_analysis},
        path_template_raw=obj_template_path, path_template_values=obj_filled_path,
        tag_group='section_time_series')
    file_path_dset_dynamic_destination_plot_domain, file_act_dset_dynamic_destination_plot_domain = define_file_path_analyzer(
        obj_data_dynamic_destination_plot, tag_file_time='time_reference',
        geo_template_raw=obj_template_geo, geo_template_values=obj_filled_geo,
        time_template_raw=obj_template_time, time_template_values={'time_reference': info_time_analysis},
        path_template_raw=obj_template_path, path_template_values=obj_filled_path,
        tag_group='domain_time_series')
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # DOMAIN FORCING
    run_tag = 'weather_stations'

    run_file_path_source = file_path_dynamic_source[run_tag]
    run_file_path_destination_workspace = file_path_dset_dynamic_destination_workspace_domain[run_tag]
    run_info = {**info_dynamic_source[run_tag]}
    run_flags = flags_dynamic_source[run_tag]

    run_file_path_source = wrap_file(
        run_file_path_source, file_mode=info_mode,
        file_root_default=file_root_path, file_folder_default='data')

    run_file_path_destination_workspace = wrap_file(
        run_file_path_destination_workspace, file_mode=info_mode,
        file_root_default=file_root_path, file_folder_default='workspace')

    dframe_domain_forcing_raw = wrap_ts(
        run_file_path_source, run_file_path_destination_workspace,
        file_run=run_tag, file_description='domain',
        dict_info=run_info, dict_flags=run_flags, dict_variables=run_variables_domain)
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # WEATHER STATIONS
    run_tag = 'weather_stations'

    run_file_path_source = file_path_dynamic_source[run_tag]
    run_file_path_destination_workspace = file_path_dset_dynamic_destination_workspace_section[run_tag]
    run_info = {**info_dynamic_source[run_tag], **info_section_attrs}
    run_flags = flags_dynamic_source[run_tag]

    run_file_path_source = wrap_file(
        run_file_path_source, file_mode=info_mode,
        file_root_default=file_root_path, file_folder_default='data')

    run_file_path_destination_workspace = wrap_file(
        run_file_path_destination_workspace, file_mode=info_mode,
        file_root_default=file_root_path, file_folder_default='workspace')

    dframe_ws_raw = wrap_ts(
        run_file_path_source, run_file_path_destination_workspace,
        file_run=run_tag, file_description='section',
        dict_info=run_info, dict_flags=run_flags, dict_variables=run_variables_section)
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # NWP LAMI-2I DETERMINISTIC
    run_tag = 'nwp_deterministic_lami2i'

    run_file_path_source = file_path_dynamic_source[run_tag]
    run_file_path_destination_workspace = file_path_dset_dynamic_destination_workspace_section[run_tag]
    run_info = {**info_dynamic_source[run_tag], **info_section_attrs}
    run_flags = flags_dynamic_source[run_tag]

    run_file_path_source = wrap_file(
        run_file_path_source, file_mode=info_mode,
        file_root_default=file_root_path, file_folder_default='data')

    run_file_path_destination_workspace = wrap_file(
        run_file_path_destination_workspace, file_mode=info_mode,
        file_root_default=file_root_path, file_folder_default='workspace')

    dframe_nwp_lami2i_det_raw = wrap_ts(
        run_file_path_source, run_file_path_destination_workspace,
        file_run=run_tag, file_description='section',
        dict_info=run_info, dict_flags=run_flags, dict_variables=run_variables_section)
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # NWP LAMI-2I PROBABILISTIC
    run_tag = 'nwp_probabilistic_lami2i'

    run_file_path_source = file_path_dynamic_source[run_tag]
    run_file_path_destination_workspace = file_path_dset_dynamic_destination_workspace_section[run_tag]
    run_info = {**info_dynamic_source[run_tag], **info_section_attrs}
    run_flags = flags_dynamic_source[run_tag]

    run_file_path_source = wrap_file(
        run_file_path_source, file_mode=info_mode,
        file_root_default=file_root_path, file_folder_default='data')

    run_file_path_destination_workspace = wrap_file(
        run_file_path_destination_workspace, file_mode=info_mode,
        file_root_default=file_root_path, file_folder_default='workspace')

    dframe_nwp_lami2i_prob_raw = wrap_ts(
        run_file_path_source, run_file_path_destination_workspace,
        file_run=run_tag, file_description='section',
        dict_info=run_info, dict_flags=run_flags, dict_variables=run_variables_section)
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # NWP ECMWF0100 DETERMINISTIC
    run_tag = 'nwp_deterministic_ecmwf0100'

    run_file_path_source = file_path_dynamic_source[run_tag]
    run_file_path_destination_workspace = file_path_dset_dynamic_destination_workspace_section[run_tag]
    run_info = {**info_dynamic_source[run_tag], **info_section_attrs}
    run_flags = flags_dynamic_source[run_tag]

    run_file_path_source = wrap_file(
        run_file_path_source, file_mode=info_mode,
        file_root_default=file_root_path, file_folder_default='data')

    run_file_path_destination_workspace = wrap_file(
        run_file_path_destination_workspace, file_mode=info_mode,
        file_root_default=file_root_path, file_folder_default='workspace')

    dframe_nwp_ecmwf0100_det_raw = wrap_ts(
        run_file_path_source, run_file_path_destination_workspace,
        file_run=run_tag, file_description='section',
        dict_info=run_info, dict_flags=run_flags, dict_variables=run_variables_section)
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # NWP ECMWF0100 PROBABILISTIC
    run_tag = 'nwp_probabilistic_ecmwf0100'

    run_file_path_source = file_path_dynamic_source[run_tag]
    run_file_path_destination_workspace = file_path_dset_dynamic_destination_workspace_section[run_tag]
    run_info = {**info_dynamic_source[run_tag], **info_section_attrs}
    run_flags = flags_dynamic_source[run_tag]

    run_file_path_source = wrap_file(
        run_file_path_source, file_mode=info_mode,
        file_root_default=file_root_path, file_folder_default='data')

    run_file_path_destination_workspace = wrap_file(
        run_file_path_destination_workspace, file_mode=info_mode,
        file_root_default=file_root_path, file_folder_default='workspace')

    dframe_nwp_ecmwf0100_prob_raw = wrap_ts(
        run_file_path_source, run_file_path_destination_workspace,
        file_run=run_tag, file_description='section',
        dict_info=run_info, dict_flags=run_flags, dict_variables=run_variables_section)
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Adjust time-series by different run
    dframe_nwp_lami2i_det_period, dframe_domain_forcing_period = adjust_ts(
        dframe_nwp_lami2i_det_raw, dframe_domain_forcing_raw,
        dframe_reference_name='nwp_lami2i_deterministic', dframe_other_name='domain_forcing')

    dframe_ws_period = adjust_ts(
        dframe_nwp_lami2i_det_raw, dframe_ws_raw,
        dframe_reference_name='nwp_lami2i_deterministic', dframe_other_name='weather_station')[1]

    dframe_nwp_lami2i_prob_period = adjust_ts(
        dframe_nwp_lami2i_det_raw, dframe_nwp_lami2i_prob_raw,
        dframe_reference_name='nwp_lami2i_deterministic', dframe_other_name='nwp_lami2i_probabilistic')[1]

    dframe_nwp_ecmwf0100_det_period = adjust_ts(
        dframe_nwp_lami2i_det_raw, dframe_nwp_ecmwf0100_det_raw,
        dframe_reference_name='nwp_lami2i_deterministic', dframe_other_name='nwp_ecmwf0100_deterministic')[1]

    dframe_nwp_ecmwf0100_prob_period = adjust_ts(
        dframe_nwp_lami2i_det_raw, dframe_nwp_ecmwf0100_prob_raw,
        dframe_reference_name='nwp_lami2i_deterministic', dframe_other_name='nwp_ecmwf0100_probabilistic')[1]
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Select variable(s)
    vars_domain_forcing_period = select_collections_var(
        dframe_domain_forcing_period,
        var_name=['rain', 'air_temperature',  'incoming_radiation', 'relative_humidity', 'wind_speed'],
        var_dataset='domain_forcing')
    ts_rain_forcing_period = vars_domain_forcing_period['rain']
    ts_air_temperature_forcing_period = vars_domain_forcing_period['rain']
    ts_incoming_radiation_forcing_period = vars_domain_forcing_period['incoming_radiation']
    ts_relative_humidity_forcing_period = vars_domain_forcing_period['relative_humidity']
    ts_wind_speed_forcing_period = vars_domain_forcing_period['wind_speed']

    vars_ws_period = select_collections_var(
        dframe_ws_period,
        var_name=['rain', 'discharge_simulated',  'discharge_observed', 'soil_moisture'],
        var_dataset='weather_station')
    ts_rain_ws_period = vars_ws_period['rain']
    ts_discharge_simulated_ws_period = vars_ws_period['discharge_simulated']
    ts_discharge_observed_ws_period = vars_ws_period['discharge_observed']
    ts_sm_ws_period = vars_ws_period['soil_moisture']

    vars_nwp_lami2i_det_period = select_collections_var(
        dframe_nwp_lami2i_det_period,
        var_name=['rain', 'discharge_simulated', 'soil_moisture'],
        var_dataset='nwp_lami2i_deterministic')
    ts_rain_nwp_lami2i_det_period = vars_nwp_lami2i_det_period['rain']
    ts_discharge_simulated_nwp_lami2i_det_period = vars_nwp_lami2i_det_period['discharge_simulated']
    ts_sm_nwp_lami2i_det_period = vars_nwp_lami2i_det_period['soil_moisture']

    vars_nwp_lami2i_prob_period = select_collections_var(
        dframe_nwp_lami2i_prob_period,
        var_name=['rain', 'discharge_simulated', 'soil_moisture'],
        var_dataset='nwp_lami2i_probabilistic')
    ts_rain_nwp_lami2i_prob_period = vars_nwp_lami2i_prob_period['rain']
    ts_discharge_simulated_nwp_lami2i_prob_period = vars_nwp_lami2i_prob_period['discharge_simulated']
    ts_sm_nwp_lami2i_prob_period = vars_nwp_lami2i_prob_period['soil_moisture']

    vars_nwp_ecmwf0100_det_period = select_collections_var(
        dframe_nwp_ecmwf0100_det_period,
        var_name=['rain', 'discharge_simulated', 'soil_moisture'],
        var_dataset='nwp_ecmwf0100_deterministic')
    ts_rain_nwp_ecmwf0100_det_period = vars_nwp_ecmwf0100_det_period['rain']
    ts_discharge_simulated_nwp_ecmwf0100_det_period = vars_nwp_ecmwf0100_det_period['discharge_simulated']
    ts_sm_nwp_ecmwf0100_det_period = vars_nwp_ecmwf0100_det_period['soil_moisture']

    vars_nwp_ecmwf0100_prob_period = select_collections_var(
        dframe_nwp_ecmwf0100_prob_period,
        var_name=['rain', 'discharge_simulated', 'soil_moisture'],
        var_dataset='nwp_ecmwf0100_probabilistic')
    ts_rain_nwp_ecmwf0100_prob_period = vars_nwp_ecmwf0100_prob_period['rain']
    ts_discharge_simulated_nwp_ecmwf0100_prob_period = vars_nwp_ecmwf0100_prob_period['discharge_simulated']
    ts_sm_nwp_ecmwf0100_prob_period = vars_nwp_ecmwf0100_prob_period['soil_moisture']
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # PLOT SECTION LOCATOR
    plot_map_terrain(
        None,
        darray_terrain, darray_river_network, info_section_attrs,
        mask_terrain=False
    )
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # PLOT FORCING DATASETS
    workspace_check = check_ts_workspace(
        [dframe_domain_forcing_period],
        ['attributes'], variable_dataset='domain_forcing')
    if workspace_check:

        run_file_path_destination_plot = file_path_dset_dynamic_destination_plot_domain['weather_stations']
        run_file_path_destination_plot = wrap_file(
            run_file_path_destination_plot, file_mode=info_mode,
            file_root_default=file_root_path, file_folder_default='plot', file_nullify=False)

        plot_ts_forcing_obs(
            run_file_path_destination_plot,
            dframe_domain_forcing_period.attrs,
            df_rain=ts_rain_forcing_period,
            df_airt=ts_air_temperature_forcing_period,
            df_incrad=ts_incoming_radiation_forcing_period,
            df_rh=ts_relative_humidity_forcing_period,
            df_winds=ts_wind_speed_forcing_period,
        )
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # PLOT WEATHER STATION OBS
    workspace_check = check_ts_workspace(
        [dframe_ws_period],
        ['attributes'], variable_dataset='weather_stations')
    if workspace_check:

        run_file_path_destination_plot = file_path_dset_dynamic_destination_plot_section['weather_stations']
        run_file_path_destination_plot = wrap_file(
            run_file_path_destination_plot, file_mode=info_mode,
            file_root_default=file_root_path, file_folder_default='plot', file_nullify=False)

        plot_ts_discharge_obs(
            run_file_path_destination_plot,
            dframe_ws_period.attrs,
            ts_rain_ws_period,
            ts_discharge_simulated_ws_period,
            ts_sm_ws_period,
            ts_discharge_observed_ws_period
        )
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # PLOT DETERMINISTIC LAMI-2I
    workspace_check = check_ts_workspace(
        [dframe_ws_period, dframe_nwp_lami2i_det_period],
        ['attributes', 'attributes'], variable_dataset='nwp_lami-2i_deterministic')
    if workspace_check:

        run_file_path_destination_plot = file_path_dset_dynamic_destination_plot_section['nwp_deterministic_lami2i']
        run_file_path_destination_plot = wrap_file(
            run_file_path_destination_plot, file_mode=info_mode,
            file_root_default=file_root_path, file_folder_default='plot', file_nullify=False)

        plot_ts_discharge_nwp_deterministic(
            run_file_path_destination_plot,
            dframe_ws_period.attrs,
            dframe_nwp_lami2i_det_period.attrs,
            ts_rain_ws_period,
            ts_rain_nwp_lami2i_det_period,
            ts_discharge_simulated_ws_period,
            ts_discharge_simulated_nwp_lami2i_det_period,
            ts_sm_ws_period,
            ts_sm_nwp_lami2i_det_period,
            ts_discharge_observed_ws_period
        )
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # PLOT PROBABILISTIC LAMI-2I
    workspace_check = check_ts_workspace(
        [dframe_ws_period, dframe_nwp_lami2i_det_period, dframe_nwp_lami2i_prob_period],
        ['attributes', 'attributes', 'attributes'], variable_dataset='nwp_lami-2i_probabilistic')
    if workspace_check:

        run_file_path_destination_plot = file_path_dset_dynamic_destination_plot_section['nwp_probabilistic_lami2i']
        run_file_path_destination_plot = wrap_file(
            run_file_path_destination_plot, file_mode=info_mode,
            file_root_default=file_root_path, file_folder_default='plot', file_nullify=False)

        plot_ts_discharge_nwp_probabilistic(
            run_file_path_destination_plot,
            dframe_ws_period.attrs,
            dframe_nwp_lami2i_det_period.attrs, dframe_nwp_lami2i_prob_period.attrs,
            ts_rain_ws_period,
            ts_rain_nwp_lami2i_det_period, ts_rain_nwp_lami2i_prob_period,
            ts_discharge_simulated_ws_period,
            ts_discharge_simulated_nwp_lami2i_det_period, ts_discharge_simulated_nwp_lami2i_prob_period,
            ts_sm_ws_period,
            ts_sm_nwp_lami2i_det_period, ts_sm_nwp_lami2i_prob_period,
            ts_discharge_observed_ws_period
        )
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # PLOT DETERMINISTIC ECMWF-0100
    workspace_check = check_ts_workspace(
        [dframe_ws_period, dframe_nwp_ecmwf0100_det_period],
        ['attributes', 'attributes'], variable_dataset='nwp_ecmwf0100_deterministic')
    if workspace_check:

        run_file_path_destination_plot = file_path_dset_dynamic_destination_plot_section['nwp_deterministic_ecmwf0100']
        run_file_path_destination_plot = wrap_file(
            run_file_path_destination_plot, file_mode=info_mode,
            file_root_default=file_root_path, file_folder_default='plot')

        plot_ts_discharge_nwp_deterministic(
            run_file_path_destination_plot,
            dframe_ws_period.attrs,
            dframe_nwp_ecmwf0100_det_period.attrs,
            ts_rain_ws_period,
            ts_rain_nwp_ecmwf0100_det_period,
            ts_discharge_simulated_ws_period,
            ts_discharge_simulated_nwp_ecmwf0100_det_period,
            ts_sm_ws_period,
            ts_sm_nwp_lami2i_det_period,
            ts_discharge_observed_ws_period
        )
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # PLOT PROBABILISTIC ECMWF-0100
    workspace_check = check_ts_workspace(
        [dframe_ws_period, dframe_nwp_ecmwf0100_det_period, dframe_nwp_ecmwf0100_prob_period],
        ['attributes', 'attributes', 'attributes'], variable_dataset='nwp_ecmwf0100_probabilistic')
    if workspace_check:

        run_file_path_destination_plot = file_path_dset_dynamic_destination_plot_section['nwp_probabilistic_ecmwf0100']
        run_file_path_destination_plot = wrap_file(
            run_file_path_destination_plot, file_mode=info_mode,
            file_root_default=file_root_path, file_folder_default='plot')

        plot_ts_discharge_nwp_probabilistic(
            run_file_path_destination_plot,
            dframe_ws_period.attrs,
            dframe_nwp_ecmwf0100_det_period.attrs, dframe_nwp_ecmwf0100_prob_period.attrs,
            ts_rain_ws_period,
            ts_rain_nwp_ecmwf0100_det_period, ts_rain_nwp_ecmwf0100_prob_period,
            ts_discharge_simulated_ws_period,
            ts_discharge_simulated_nwp_ecmwf0100_det_period, ts_discharge_simulated_nwp_ecmwf0100_prob_period,
            ts_sm_ws_period,
            ts_sm_nwp_ecmwf0100_det_period, ts_sm_nwp_ecmwf0100_prob_period,
            ts_discharge_observed_ws_period
        )
    # -------------------------------------------------------------------------------------

    print('')

# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to get script argument(s)
def get_args(file_name_settings_default='fp_labs_analyzer_hmc_timeseries.json'):

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
    file_name_settings_default = "fp_labs_analyzer_hmc_timeseries.json"
    main(file_name_settings_default)
# ----------------------------------------------------------------------------
