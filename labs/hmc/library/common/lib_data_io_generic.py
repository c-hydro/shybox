"""
Library Features:

Name:          lib_data_io_generic
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20210113'
Version:       '1.0.0'
"""
#######################################################################################
# Library
import logging
import os
import re
import random

from datetime import datetime
from copy import deepcopy

import xarray as xr
import pandas as pd
import numpy as np

from library.common.lib_utils_system import get_dict_values, fill_tags2string, make_folder
from library.common.lib_info_args import logger_name

# Logging
log_stream = logging.getLogger(logger_name)
#######################################################################################


# -------------------------------------------------------------------------------------
# Method to set the information mode
def set_info_mode(obj_data, tag_field_mode='mode', tag_type='time_series',
                  tag_field_domain_name='domain_name',
                  tag_field_basin_name='basin_name', tag_field_section_name='section_name'):

    if tag_field_mode in list(obj_data.keys()):
        field_mode = obj_data[tag_field_mode]
    else:
        print(' ERROR ===> The key mode "' + tag_field_mode + '" must be defined in the configuration file')
        raise IOError('The key mode must be defined to correctly run the notebook')

    if tag_type == 'time_series':
        if field_mode == 'cmd':
            field_domain_name = obj_data[tag_field_domain_name]
            field_basin_name = obj_data[tag_field_basin_name]
            field_section_name = obj_data[tag_field_section_name]
        elif field_mode == 'interactive_local':
            field_domain_name, field_basin_name, field_section_name = None, None, None
        elif field_mode == 'interactive_remote':
            field_domain_name, field_basin_name, field_section_name = None, None, None
        else:
            print(' ERROR ===> The field mode "' + field_mode + '" is not supported yet')
            raise RuntimeError(' ===> The field mode must be defined by '
                               '"cmd", "interactive_local" and "interactive_remote" value')

        return field_mode, field_domain_name, field_basin_name, field_section_name

    elif tag_type == 'maps':

        if field_mode == 'cmd':
            field_domain_name = obj_data[tag_field_domain_name]
        elif field_mode == 'interactive_local':
            field_domain_name = None
        elif field_mode == 'interactive_remote':
            field_domain_name = None
        else:
            print(' ERROR ===> The field mode "' + field_mode + '" is not supported yet')
            raise RuntimeError(' ===> The field mode must be defined by '
                               '"cmd", "interactive_local" and "interactive_remote" value')

        return field_mode, field_domain_name

    else:

        print(' ERROR ===> The field type "' + tag_type + '" is not supported yet')
        raise RuntimeError(' ===> The field mode must be defined by '
                           '"time_series" and "maps" value')

# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to organize file template
def organize_file_template(file_collections, file_dataset_tag='maps_forcing_obs_ws'):

    file_name_list = []
    file_time_list = []
    for file_time_step, file_dataset_step in file_collections.items():
        if file_dataset_tag in list(file_dataset_step.keys()):
            file_name_step = file_dataset_step[file_dataset_tag]
            file_name_list.append(file_name_step)
            file_time_list.append(file_time_step)
    return file_name_list, file_time_list
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to fill file ensemble
def fill_ensemble_template(file_name_raw, ensemble_n=None, ensemble_tag='ensemble_id', key_tag='KEY_TAG_ENS_{:}'):
    file_name_list = []
    if ensemble_n is not None:
        ensemble_list = list(np.arange(1, ensemble_n + 1))
        for key_id, ensemble_step in enumerate(ensemble_list):

            if ensemble_tag in file_name_raw:

                key_ensemble = '{' + ensemble_tag + '}'
                key_step = key_tag.format(str(key_id))
                file_name_step = file_name_raw.replace(key_ensemble, key_step)
                ensemble_step = '{:03d}'.format(ensemble_step)

                file_name_ens = file_name_step.replace(key_step, ensemble_step)

                file_name_list.append(file_name_ens)
    else:
        file_name_list.append(file_name_raw)

    return file_name_list
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to fill file path
def fill_file_template(file_obj, template_default=None, template_filled=None):

    if (template_filled is not None) and (template_default is not None):
        if isinstance(file_obj, dict):
            file_filled_obj = {}
            for file_path_key, file_path_raw in file_obj.items():
                file_path_def = fill_tags2string(file_path_raw, template_default, template_filled)
                file_filled_obj[file_path_key] = file_path_def
        elif isinstance(file_obj, list):
            file_filled_obj = []
            for file_path_step in file_obj:
                file_filled_step = fill_tags2string(file_path_step, template_default, template_filled)
                file_filled_obj.append(file_filled_step)
        elif isinstance(file_obj, str):
            file_filled_obj = fill_tags2string(file_obj, template_default, template_filled)
        else:
            print(' ERROR ===> Obj in a wrong format')
            raise NotImplementedError('Object in filling string must be dict, string or list')
    else:
        file_filled_obj = file_obj

    return file_filled_obj
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to define keys list source/destination
def define_keys_list(source_datasets=None, destination_datasets=None):

    source_keys = list(source_datasets.keys())
    destination_keys = list(destination_datasets.keys())

    selected_keys = []
    for key_step in source_keys:
        if key_step in destination_keys:
            selected_keys.append(key_step)
        else:
            print(' WARNING ===> Source key "' + key_step + '" not found in destination key(s)')

    return selected_keys
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to define file template
def define_file_template(time_run, time_analysis=None,
                         section_name=None, basin_name=None, domain_name=None, run_name=None,
                         template_default=None):

    if isinstance(time_run, str):
        time_run = pd.Timestamp(time_run)

    if time_analysis is None:
        time_analysis = time_run

    if isinstance(time_analysis, str):
        time_analysis = pd.Timestamp(time_analysis)

    template_filled = {}
    if template_default is not None:
        for template_key in list(template_default.keys()):
            if template_key == 'section_name':
                template_filled[template_key] = section_name
            elif template_key == 'basin_name':
                template_filled[template_key] = basin_name
            elif template_key == 'domain_name':
                template_filled[template_key] = domain_name
            elif template_key == 'run_datetime':
                template_filled[template_key] = time_run
            elif template_key == 'run_sub_path':
                template_filled[template_key] = time_run
            elif template_key == 'time_series_datetime':
                template_filled[template_key] = time_run
            elif template_key == 'time_series_sub_path':
                template_filled[template_key] = time_run
            elif template_key == 'maps_forcing_obs_ws_datetime':
                template_filled[template_key] = time_analysis
            elif template_key == 'maps_forcing_obs_ws_sub_path':
                template_filled[template_key] = time_run
            elif template_key == 'maps_outcome_datetime':
                template_filled[template_key] = time_analysis
            elif template_key == 'maps_outcome_sub_path':
                template_filled[template_key] = time_run
            elif template_key == 'plot_datetime':
                template_filled[template_key] = time_analysis
            elif template_key == 'plot_sub_path':
                template_filled[template_key] = time_analysis
            elif template_key == 'var_name':
                template_filled[template_key] = '{var_name}'
            elif template_key == 'group_name':
                template_filled[template_key] = '{group_name}'
            elif template_key == 'time_name':
                template_filled[template_key] = '{time_name}'
            elif template_key == 'run_name':
                template_filled[template_key] = run_name
            else:
                raise NameError('Template key ' + template_key + ' is not assigned by the procedure')
    else:
        template_filled = None

    return template_filled
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to list all subfolders in a root path
def get_path_folders(root_path):
    folder_list = os.listdir(root_path)
    return folder_list
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to search root of a generic path
def get_path_root(generic_path, home_path_string=None, home_path_env='$HOME'):

    if home_path_env in generic_path:
        generic_path = generic_path.replace(home_path_env, home_path_string)

    string_patterns = re.findall(r"\{([A-Za-z0-9_]+)\}", generic_path)

    dict_patterns = {}
    for string_pattern in string_patterns:
        dict_patterns[string_pattern] = ':::'

    tmp_path = generic_path.format(**dict_patterns)
    root_path = tmp_path.split(sep=':::')[0]

    return root_path
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to get home path
def get_path_home(home_path=None):
    if home_path is None:
        home_path = os.path.expanduser("~")
    if not home_path.endswith('/'):
        home_path = home_path + '/'
    return home_path
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to define file variable(s)
def define_file_variables(variables_data_generic, variables_data_template=None):
    variables_data_filled = {}
    for variable_key, variable_value_generic in variables_data_generic.items():
        variable_value_filled = variable_value_generic.format(**variables_data_template)
        variables_data_filled[variable_key] = variable_value_filled
    return variables_data_filled
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to define file obj
def define_file_obj(settings_data, tag_file_obj='info'):

    obj_fields = {}
    for settings_key, settings_fields in settings_data.items():
        if tag_file_obj in list(settings_fields.keys()):
            tmp_fields = settings_fields[tag_file_obj]
        else:
            tmp_fields = None

        obj_fields[settings_key] = tmp_fields

    return obj_fields
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to define file in folder
def define_folder_time(folder_name_raw,
                       obj_filled_time, obj_filled_path,
                       obj_template_time, obj_template_path, time_format_file='%Y%m%d%H%M'):

    obj_filled = {**obj_filled_path, **obj_filled_time}
    obj_template = {**obj_template_path, **obj_template_time}

    folder_name_def = fill_file_template(
        folder_name_raw, template_filled=obj_filled, template_default=obj_template)

    file_name_def = [f for f in os.listdir(folder_name_def) if os.path.isfile(os.path.join(folder_name_def, f))]

    file_time_list = []
    for file_name_step in file_name_def:

        file_time_tmp = re.search(r'\d{12}', file_name_step)
        file_time_str = str(pd.Timestamp(file_time_tmp.group()))
        file_time_list.append(file_time_str)

    file_time_list = sorted(file_time_list)[::-1]

    return file_time_list
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Method to select file time
def select_file_time(time_db_collections, time_ref_collections,
                     time_pivot='reference',
                     time_value=None, time_random=False, time_format='%Y-%m-%d %H:%M'):

    time_select_collections = {}
    for (time_db_key, time_db_list), (time_ref_key, time_ref_list) in zip(time_db_collections.items(), time_ref_collections.items()):

        if time_db_key == time_ref_key:

            if time_pivot == 'reference':
                time_list = time_ref_list
                if not time_random:
                    time_random = True
            elif time_pivot == 'db':
                time_list = time_db_list
            else:
                print(' ERROR ===> The "time_pivot" format is not supported')
                raise IOError('The "time_pivot" argument must be "db" or "reference"')

            if not isinstance(time_list, list):
                time_list = [time_list]

            if time_value is not None:

                if not isinstance(time_value, pd.Timestamp):
                    time_value = pd.to_datetime(time_value, format=time_format)

                if time_value in time_list:
                    time_select = time_value
                else:
                    if time_random:
                        time_select = random.choice(time_list)
                    else:
                        print(' ERROR ===> Time selection is not available for mismatching conditions')
                        raise RuntimeError('The arguments "time_value" is not available in the list '
                                           'and "time_random" is set to False; the time selection is None')
            else:
                if time_random:
                    time_select = random.choice(time_list)
                else:
                    print(' ERROR ===> Time selection is not available for mismatching conditions')
                    raise RuntimeError('The arguments "time_value" is None and "time_random" is set to False; '
                                       'the time selection is None')

            time_select_collections[time_db_key] = time_select

        else:
            print(' ERROR ===> The keys of the time variables are not the same.')
            raise RuntimeError('Time DB and Reference keys must be the same.')

    return time_select_collections
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to filter time db using a time window
def filter_file_time(info_time_db_general, info_datetime_idx_analysis, time_format='%Y-%m-%d %H:%M'):

    info_time_db_filter, info_time_list_filter = {},{}
    for info_key, info_times_step in info_time_db_general.items():

        info_datetime_idx_step = pd.DatetimeIndex(info_times_step)

        info_datetime_idx_filter = info_datetime_idx_analysis.intersection(info_datetime_idx_step)

        if info_datetime_idx_filter.empty:
            print(' ===> Time analysis and time datasets for "' + info_key + '" run does not have common time(s)')
            info_datetime_idx_filter = None

        if info_datetime_idx_filter is not None:
            info_time_filter, list_time_filter = [], []
            for info_datetime_idx_step in info_datetime_idx_filter:
                info_time_step = pd.to_datetime(info_datetime_idx_step, format=time_format)
                str_time_step = info_time_step.strftime(format=time_format)
                info_time_filter.append(info_time_step)
                list_time_filter.append(str_time_step)
        else:
            info_time_filter, list_time_filter = None, []

        info_time_db_filter[info_key] = info_time_filter
        info_time_list_filter[info_key] = list_time_filter

    return info_time_db_filter, info_time_list_filter

# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to define file time
def define_file_time(settings_data,
                     tag_folder_name='folder_name', tag_file_name='file_name', tag_file_time='time_reference',
                     time_format_path='%Y%m%d_%H', time_format_reference='%Y-%m-%d %H:%M', time_reverse=True,
                     tag_group=None,
                     geo_template_raw=None, geo_template_values=None,
                     time_template_raw=None, time_template_values=None,
                     path_template_raw=None, path_template_values=None):

    file_folder_time_db_collections, file_folder_time_ref_collections, file_folder_root_collections = {}, {}, {}
    for data_key, data_fields in settings_data.items():

        if tag_group is not None:
            if tag_group in list(data_fields.keys()):
                data_fields_selected = data_fields[tag_group]
            else:
                data_fields_selected = None
        else:
            data_fields_selected = deepcopy(data_fields)

        if data_fields_selected is not None:

            folder_name = get_dict_values(data_fields_selected, tag_folder_name, [])
            file_name = get_dict_values(data_fields_selected, tag_file_name, [])
            file_time_reference = get_dict_values(data_fields_selected, tag_file_time, [])

            if isinstance(folder_name, list):
                if folder_name.__len__() == 1:
                    folder_name = folder_name[0]
                else:
                    raise NotImplementedError(' ===> FolderName format is not allowed by the procedure')
            if isinstance(file_name, list):
                if file_name.__len__() == 1:
                    file_name = file_name[0]
                else:
                    raise NotImplementedError(' ===> FileName format is not allowed by the procedure')
            if file_time_reference and (isinstance(file_time_reference, list)):
                if file_time_reference.__len__() == 1:
                    file_time_reference = file_time_reference[0]
                    file_time_reference = pd.to_datetime(file_time_reference, format=time_format_reference)
                else:
                    raise NotImplementedError(' ===> FileTime format is not allowed by the procedure')

            generic_template_raw, generic_template_values = {}, {}
            if (geo_template_raw is not None) and (geo_template_values is not None):
                generic_template_raw = {**generic_template_raw, **geo_template_raw}
                generic_template_values = {**generic_template_values, **geo_template_values}
            if (path_template_raw is not None) and (path_template_values is not None):
                generic_template_raw = {**generic_template_raw, **path_template_raw}
                generic_template_values = {**generic_template_values, **path_template_values}

            # Nullify the time variable
            file_tstamp = '#'
            time_template_values_step = dict.fromkeys(list(time_template_raw.keys()), file_tstamp)

            generic_template_raw = {**generic_template_raw, **time_template_raw}
            generic_template_values = {**generic_template_values, **time_template_values_step}

            file_path_raw = os.path.join(folder_name, file_name)
            file_path_def = fill_file_template(
                file_path_raw, template_filled=generic_template_values, template_default=generic_template_raw)

            file_path_generic = file_path_def.split('#')[0]
            file_path_subs = [f.path for f in os.scandir(file_path_generic) if f.is_dir()]

            file_time_db = []
            for file_path_sub in file_path_subs:
                file_path_root, file_path_time = os.path.split(file_path_sub)
                file_path_time = pd.to_datetime(file_path_time, format=time_format_path)
                file_time_db.append(file_path_time)

            file_folder_root_collections[data_key] = file_path_generic
            if time_reverse:
                file_folder_time_db_collections[data_key] = sorted(file_time_db)[::-1]
            else:
                file_folder_time_db_collections[data_key] = sorted(file_time_db)
            file_folder_time_ref_collections[data_key] = file_time_reference

    return file_folder_time_db_collections, file_folder_time_ref_collections
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to define file path
def define_file_path_downloader(settings_data,
                              tag_folder_name='folder_name', tag_file_name='file_name',
                              tag_file_time='time_reference', tag_file_activate='activate',
                              tag_group=None,
                              geo_template_raw=None, geo_template_values=None,
                              time_template_raw=None, time_template_values=None,
                              path_template_raw=None, path_template_values=None):
    file_path_collections, file_activate_collections = {}, {}
    for data_key, data_fields in settings_data.items():

        if tag_group is not None:
            if tag_group in list(data_fields.keys()):
                data_fields_selected = data_fields[tag_group]
            else:
                data_fields_selected = None
        else:
            data_fields_selected = deepcopy(data_fields)

        if data_fields_selected is not None:
            folder_name = get_dict_values(data_fields_selected, tag_folder_name, [])
            file_name = get_dict_values(data_fields_selected, tag_file_name, [])
            file_activate = get_dict_values(data_fields_selected, tag_file_activate, [])

            if tag_file_time is not None:

                if isinstance(time_template_values, dict):
                    if tag_file_time not in list(time_template_values.keys()):
                        print(' ERROR ===> The key "tag_file_time" is not defined '
                              'in the "time_template_values" variable')
                        raise IOError('Check the "time_template_values" definitions')
                else:
                    print(' ERROR ===> The "time_template_values" is not defined by a dictionary')
                    raise NotImplementedError('Case not implemented yet')

                if tag_file_time == 'time_dataset':
                    time_dataset_values = time_template_values[tag_file_time]
                    if data_key in list(time_dataset_values.keys()):
                        file_time = time_dataset_values[data_key]
                    else:
                        print(' ERROR ===> The variable "file_time" is not defined by the "data_key"')
                        raise IOError('The "file_time" key must be in the related group of keys')

                elif tag_file_time == 'time_reference':
                    file_time = time_template_values[tag_file_time]
                else:
                    print(
                        ' ERROR ===> The variable "tag_file_time" must be equal to "time_dataset" or "time_reference"')
                    raise NotImplementedError('Case not implemented yet')

                if not file_time:
                    print(' ERROR ===> The variable "file_time" is not defined')
                    raise RuntimeError('The variable "file_time" must be defined')
                if not isinstance(file_time, pd.Timestamp):
                    file_time = pd.to_datetime(file_time)
            else:
                file_time = None

            if isinstance(folder_name, list):
                if folder_name.__len__() == 1:
                    folder_name = folder_name[0]
                else:
                    print(' ERROR ===> The variable "folder_name" format is not supported')
                    raise NotImplementedError('Case not implemented yet')
            if isinstance(file_name, list):
                if file_name.__len__() == 1:
                    file_name = file_name[0]
                else:
                    print(' ERROR ===> The variable "file_name" format is not supported')
                    raise NotImplementedError('Case not implemented yet')

            if isinstance(file_activate, list):
                if file_activate.__len__() == 1:
                    file_activate = file_activate[0]
                else:
                    file_activate = True

            if tag_file_time is not None:
                if file_time and (isinstance(file_time, list)):
                    if file_time.__len__() == 1:
                        file_time = file_time[0]
                    else:
                        print(' ERROR ===> The variable "time_reference" format is not supported')
                        raise NotImplementedError('Case not implemented yet')

            generic_template_raw, generic_template_values = {}, {}
            if (geo_template_raw is not None) and (geo_template_values is not None):
                generic_template_raw = {**generic_template_raw, **geo_template_raw}
                generic_template_values = {**generic_template_values, **geo_template_values}
            if (path_template_raw is not None) and (path_template_values is not None):
                generic_template_raw = {**generic_template_raw, **path_template_raw}
                generic_template_values = {**generic_template_values, **path_template_values}

            if tag_file_time is not None:
                if file_time:
                    file_tstamp = pd.Timestamp(file_time)

                    if time_template_raw is not None:
                        time_template_values_step = dict.fromkeys(list(time_template_raw.keys()), file_tstamp)
                    else:
                        time_template_values_step = None

                    if (path_template_raw is not None) and (time_template_values_step is not None):
                        generic_template_raw = {**generic_template_raw, **time_template_raw}
                        generic_template_values = {**generic_template_values, **time_template_values_step}
            else:
                if (time_template_raw is not None) and (time_template_values is not None):
                    generic_template_raw = {**generic_template_raw, **time_template_raw}
                    generic_template_values = {**generic_template_values, **time_template_values}

            file_path_raw = os.path.join(folder_name, file_name)
            file_path_def = fill_file_template(
                file_path_raw, template_filled=generic_template_values, template_default=generic_template_raw)

            file_path_collections[data_key] = file_path_def
            file_activate_collections[data_key] = file_activate

    return file_path_collections, file_activate_collections

# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to define file path
def define_file_path_analyzer(settings_data,
                     tag_folder_name='folder_name', tag_file_name='file_name',
                     tag_file_time='time_reference', tag_file_activate='activate',
                     tag_group=None,
                     geo_template_raw=None, geo_template_values=None,
                     time_template_raw=None, time_template_values=None,
                     path_template_raw=None, path_template_values=None):

    file_path_collections, file_activate_collections = {}, {}
    for data_key, data_fields in settings_data.items():

        if tag_group is not None:
            if tag_group in list(data_fields.keys()):
                data_fields_selected = data_fields[tag_group]
            else:
                data_fields_selected = None
        else:
            data_fields_selected = deepcopy(data_fields)

        if data_fields_selected is not None:
            folder_name = get_dict_values(data_fields_selected, tag_folder_name, [])
            file_name = get_dict_values(data_fields_selected, tag_file_name, [])
            file_activate = get_dict_values(data_fields_selected, tag_file_activate, [])

            if tag_file_time is not None:

                if isinstance(time_template_values, dict):
                    if tag_file_time not in list(time_template_values.keys()):
                        print(' ERROR ===> The key "tag_file_time" is not defined '
                              'in the "time_template_values" variable')
                        raise IOError('Check the "time_template_values" definitions')
                else:
                    print(' ERROR ===> The "time_template_values" is not defined by a dictionary')
                    raise NotImplementedError('Case not implemented yet')

                if tag_file_time == 'time_dataset':
                    time_dataset_values = time_template_values[tag_file_time]
                    if data_key in list(time_dataset_values.keys()):
                        file_time = time_dataset_values[data_key]
                    else:
                        print(' ERROR ===> The variable "file_time" is not defined by the "data_key"')
                        raise IOError('The "file_time" key must be in the related group of keys')

                elif tag_file_time == 'time_reference':
                    file_time = time_template_values[tag_file_time]
                else:
                    print(' ERROR ===> The variable "tag_file_time" must be equal to "time_dataset" or "time_reference"')
                    raise NotImplementedError('Case not implemented yet')

                if not file_time:
                    print(' ERROR ===> The variable "file_time" is not defined')
                    raise RuntimeError('The variable "file_time" must be defined')
                if not isinstance(file_time, pd.Timestamp):
                    file_time = pd.to_datetime(file_time)
            else:
                file_time = None

            if isinstance(folder_name, list):
                if folder_name.__len__() == 1:
                    folder_name = folder_name[0]
                else:
                    print(' ERROR ===> The variable "folder_name" format is not supported')
                    raise NotImplementedError('Case not implemented yet')
            if isinstance(file_name, list):
                if file_name.__len__() == 1:
                    file_name = file_name[0]
                else:
                    print(' ERROR ===> The variable "file_name" format is not supported')
                    raise NotImplementedError('Case not implemented yet')

            if isinstance(file_activate, list):
                if file_activate.__len__() == 1:
                    file_activate = file_activate[0]
                else:
                    file_activate = True

            if tag_file_time is not None:
                if file_time and (isinstance(file_time, list)):
                    if file_time.__len__() == 1:
                        file_time = file_time[0]
                    else:
                        print(' ERROR ===> The variable "time_reference" format is not supported')
                        raise NotImplementedError('Case not implemented yet')

            generic_template_raw, generic_template_values = {}, {}
            if (geo_template_raw is not None) and (geo_template_values is not None):
                generic_template_raw = {**generic_template_raw, **geo_template_raw}
                generic_template_values = {**generic_template_values, **geo_template_values}
            if (path_template_raw is not None) and (path_template_values is not None):
                generic_template_raw = {**generic_template_raw, **path_template_raw}
                generic_template_values = {**generic_template_values, **path_template_values}

            if tag_file_time is not None:
                if file_time:
                    file_tstamp = pd.Timestamp(file_time)
                    
                    if time_template_raw is not None:
                        time_template_values_step = dict.fromkeys(list(time_template_raw.keys()), file_tstamp)
                    else:
                        time_template_values_step = None

                    if (path_template_raw is not None) and (time_template_values_step is not None):
                        generic_template_raw = {**generic_template_raw, **time_template_raw}
                        generic_template_values = {**generic_template_values, **time_template_values_step}
            else:
                if (time_template_raw is not None) and (time_template_values is not None):
                    generic_template_raw = {**generic_template_raw, **time_template_raw}
                    generic_template_values = {**generic_template_values, **time_template_values}


            file_path_raw = os.path.join(folder_name, file_name)
            file_path_def = fill_file_template(
                file_path_raw, template_filled=generic_template_values, template_default=generic_template_raw)

            file_path_collections[data_key] = file_path_def
            file_activate_collections[data_key] = file_activate

    return file_path_collections, file_activate_collections
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to convert tstamp to tstr
def convert_tstamp_2_tstr(time_stamp_list):

    time_str_list = []
    for time_stamp_step in time_stamp_list:
        time_str_list.append(str(time_stamp_step))
    return time_str_list
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Method to define file name filling with variable name
def define_file_map(file_name_in, var_name, var_tag='var_name'):

    file_name_tmp = file_name_in.replace(var_tag, ':')
    file_path_out = file_name_tmp.format(var_name)
    folder_name_out, file_name_out = os.path.split(file_path_out)
    make_folder(folder_name_out)

    return file_path_out

# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to define file name filling with variable name
def define_file_var(file_name_collections, file_time, var_name, dataset_name, var_tag='var_name'):

    if file_time in list(file_name_collections.keys()):

        file_name = file_name_collections[file_time][dataset_name]

        file_name_tmp = file_name.replace(var_tag, ':')
        file_path_var = file_name_tmp.format(var_name)
        folder_name_var, file_name_var = os.path.split(file_path_var)
        make_folder(folder_name_var)
    else:
        raise IOError('Plot filename is not defined for time ' + file_time)

    return file_path_var

# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to create data array variable
def create_darray_map(dset_in, var_name_in='Air_Temperature', var_name_out='Air_T'):

    dset_vars_in = list(dset_in.data_vars)
    if var_name_in in dset_vars_in:

        dset_tmp = dset_in.rename({var_name_in: var_name_out})
        var_darray_out = dset_tmp[var_name_out]
        var_attrs_out = var_darray_out.attrs

        return var_darray_out, var_attrs_out
    else:
        raise IOError('Dataset variable does not exist')

# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Method to create data array variable
def create_darray_maps(dset_collections, var_time=None,
                       var_name_in='Air_Temperature', var_name_out='Air_T'):

    if var_time is not None:
        if var_time in list(dset_collections.keys()):

            dset_generic = dset_collections[var_time]
            dset_vars = list(dset_generic.data_vars)

            if var_name_in in dset_vars:

                dset_tmp = dset_generic.rename({var_name_in: var_name_out})
                var_darray_def = dset_tmp[var_name_out]
                var_attrs_def = var_darray_def.attrs

                return var_darray_def, var_attrs_def
            else:
                raise IOError('Dataset variable does not exist')
        else:
            raise IOError('Dataset time does not exist')
    else:
        raise IOError('Dataset time is undefined')
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to create time-series dataframe variable
def create_dframe_ts(df_generic,
                     var_name_in='discharge_obs', var_name_out='discharge_obs',
                     var_value_min=0, var_value_max=None,
                     index_name='time'):

    df_columns = list(df_generic.columns)

    var_dict = {}
    if var_name_in in df_columns:

        var_data = df_generic[var_name_in].values

        if var_value_min is not None:
            var_data[var_data < var_value_min] = np.nan
        if var_value_max is not None:
            var_data[var_data > var_value_max] = np.nan

        var_idx = df_generic.index
        var_dict[var_name_out] = var_data
        var_dict[index_name] = var_idx

        df_var = pd.DataFrame(data=var_dict)
        if index_name in list(df_var.columns):
            df_var.set_index(index_name, inplace=True)
        return df_var

    else:
        raise IOError('Dataframe variable does not exist')
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to create a data array
def create_darray_2d(data, geo_x, geo_y, geo_1d=True, time=None,
                     coord_name_x='west_east', coord_name_y='south_north', coord_name_time='time',
                     dim_name_x='west_east', dim_name_y='south_north', dim_name_time='time',
                     dims_order=None):

    if dims_order is None:
        dims_order = [dim_name_y, dim_name_x]
    if time is not None:
        dims_order = [dim_name_y, dim_name_x, dim_name_time]

    if geo_1d:
        if geo_x.shape.__len__() == 2:
            geo_x = geo_x[0, :]
        if geo_y.shape.__len__() == 2:
            geo_y = geo_y[:, 0]

        if time is None:
            data_da = xr.DataArray(data,
                                   dims=dims_order,
                                   coords={coord_name_x: (dim_name_x, geo_x),
                                           coord_name_y: (dim_name_y, geo_y)})
        elif isinstance(time, pd.DatetimeIndex):

            if data.shape.__len__() == 2:
                data = np.expand_dims(data, axis=-1)

            data_da = xr.DataArray(data,
                                   dims=dims_order,
                                   coords={coord_name_x: (dim_name_x, geo_x),
                                           coord_name_y: (dim_name_y, geo_y),
                                           coord_name_time: (dim_name_time, time)})
        else:
            log_stream.error(' ===> Time obj is in wrong format')
            raise IOError('Variable time format not valid')

    else:
        log_stream.error(' ===> Longitude and Latitude must be 1d')
        raise IOError('Variable shape is not valid')

    return data_da
# -------------------------------------------------------------------------------------
