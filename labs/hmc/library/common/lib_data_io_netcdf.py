"""
Library Features:

Name:          lib_data_io_netcdf
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20211025'
Version:       '1.5.0'
"""
# ------------------------------------------------------------------------------------
# Libraries
import os

from copy import deepcopy

import numpy as np
import pandas as pd
import xarray as xr

from library.common.lib_utils_io import unzip_filename
from library.common.lib_info_args import zip_extension, time_format_algorithm

# Settings
attrs_collections_excluded = ['dam_name', 'plant_name', 'dam_system_name',
                              'basin_name', 'section_name', 'outlet_name',
                              'time_length', 'time_format']
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to read netcdf maps file
def read_file_map(file_name_in, file_vars_expected=None):

    if os.path.exists(file_name_in):
        if file_name_in.endswith(zip_extension):
            file_name_zip = file_name_in
            file_name_unzip = file_name_in.replace(zip_extension, '')
            unzip_filename(file_name_zip, file_name_unzip)
        else:
            file_name_unzip = file_name_in
    else:
        print(' ERROR ===> File "' + file_name_in + '" is not available')
        raise FileNotFoundError('File not found!')

    file_handle = xr.open_dataset(file_name_unzip)
    file_vars_list_all = list(file_handle.data_vars)
    file_vars_list_expected = list(file_vars_expected.values())

    if file_vars_list_expected is None:
        file_vars_list_expected = file_vars_list_all

    file_vars_search = [var_step for var_step in file_vars_list_expected if var_step in file_vars_list_all]
    dset_vars_search = file_handle[file_vars_search]

    return dset_vars_search
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to read netcdf maps file
def read_file_maps(file_name, file_time, file_vars_list_select=None, format_time='%Y-%m-%d %H:00'):

    if isinstance(file_name, str):
        file_name_list = [file_name]
    else:
        file_name_list = file_name

    if isinstance(file_time, str):
        file_time_list = [file_time]
    else:
        file_time_list = file_time

    file_name_unzip_list = []
    file_time_unzip_list = []
    for file_time_step, file_name_step in zip(file_time_list, file_name_list):
        if os.path.exists(file_name_step):
            if file_name_step.endswith(zip_extension):
                file_name_step_zip = file_name_step
                file_name_step_unzip = file_name_step.replace(zip_extension, '')
                unzip_filename(file_name_step_zip, file_name_step_unzip)
            else:
                file_name_step_unzip = file_name_step

            file_name_unzip_list.append(file_name_step_unzip)
            file_time_unzip_list.append(file_time_step)
        else:
            print(' ERROR ===> File "' + file_name_step + '" is not available')
            raise FileNotFoundError('File not found!')

    file_data_collections = {}
    for file_time_unzip_step, file_name_unzip_step in zip(file_time_unzip_list, file_name_unzip_list):

        file_handle = xr.open_dataset(file_name_unzip_step)
        file_vars_list_all = list(file_handle.data_vars)

        if file_vars_list_select is None:
            file_vars_list_select = file_vars_list_all

        file_vars_search = [var_step for var_step in file_vars_list_select if var_step in file_vars_list_all]
        dset_vars_search = file_handle[file_vars_search]

        file_data_collections[file_time_unzip_step] = dset_vars_search

    return file_data_collections
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to select variable collections
def select_collections_var(dframe_collections, var_name=None, var_dataset='generic'):

    print(' INFO ---> Select "' + var_dataset + '" ... ')

    if var_name is None:
        var_name = ['variable']

    if not isinstance(var_name, list):
        var_name = list(var_name)

    vars_collections = {}
    for var_step in var_name:

        print(' INFO ----> Get "' + var_step + '" collections ... ')
        if dframe_collections is not None:
            dframe_var = dframe_collections.loc[:, [var_step in i for i in dframe_collections.columns]]
            print(' INFO ----> Get "' + var_step + '" collections ... DONE')
        else:
            dframe_var = None
            print(' WARNING ===> DataFrame is NoneType')
            print(' INFO ----> Get "' + var_step + '" collections ... FAILED')

        vars_collections[var_step] = dframe_var

    print(' INFO ---> Select "' + var_dataset + '" ... DONE')

    return vars_collections
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to join time series collections
def join_collections_ts(dframe_reference=None, dframe_other=None,
                        time_from=None, time_to=None, time_frequency='H'):

    print(' INFO ---> Join time series collections ... ')

    if (dframe_reference is not None) or (dframe_other is not None):

        if isinstance(dframe_reference, pd.DataFrame) and isinstance(dframe_other, pd.DataFrame):

            times_reference = dframe_reference.index
            times_other = dframe_other.index

            if (time_from is None) and (time_to is None) and (time_frequency is None):
                time_from = times_reference[0]
                time_to = times_reference[-1]
                time_freq = times_reference.inferred_freq
            elif (time_from is None) and (time_to is None) and (time_frequency is not None):
                time_from = times_reference[0]
                time_to = times_reference[-1]
                time_freq = time_frequency
            elif (time_from is not None) and (time_to is not None) and (time_frequency is not None):
                time_from = pd.Timestamp(time_from)
                time_to = pd.Timestamp(time_to)
                time_freq = time_frequency
            else:
                print(' ERROR ===> The format of time arguments is not expected.')
                raise NotImplementedError('Case not implemented yet')

            if time_freq is None:
                print(' WARNING ===> Time frequency is not initializes. Use default value')
                time_freq = time_frequency

            if time_to < time_from:
                print(' ERROR ===> TimeTo less than TimeFrom')
                raise IOError('TimeTo must be greater than TimeFrom')

            times_common = pd.date_range(start=time_from, end=time_to, freq=time_freq)
            dframe_common = pd.DataFrame(index=times_common)

            dframe_tmp = deepcopy(dframe_common)
            attrs_reference = dframe_reference.attrs
            dframe_reference_joined = dframe_tmp.join(dframe_reference)
            dframe_reference_joined.attrs = attrs_reference

            dframe_tmp = deepcopy(dframe_common)
            attrs_other = dframe_other.attrs
            dframe_other_joined = dframe_tmp.join(dframe_other)
            dframe_other_joined.attrs = attrs_other

            print(' INFO ---> Join time series collections ... DONE')
        else:
            print(' INFO ---> Join time series collections ... FAILED')
            print(' ERROR ===> Reference and other datasets must be dataframes')
            raise NotImplementedError('Case not implemented yet')
    else:
        print(' INFO ---> Join time series collections ... FAILED')
        print(' WARNING ===> Reference and other datasets must be not None')
        dframe_reference_joined, dframe_other_joined = None, None

    return dframe_reference_joined, dframe_other_joined
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to get time series collections for deterministic and probabilistic run
def organize_collections_ts(
        file_path_tmpl_collections='hmc.collections.{time_stamp}_{ensemble_id}.nc',
        file_dset_tmpl=None, file_vars_tmpl=None):

    print(' INFO ---> Organize time series collections ... ')

    if file_vars_tmpl is None:
        file_vars_tmpl = {
            'time': 'times',
            'rain': 'Rain:hmc_forcing_datasets:{basin_name}:{section_name}',
            'soil_moisture': 'SM:hmc_outcome_datasets:{basin_name}:{section_name}',
            'discharge_simulated': 'Discharge:section_discharge_sim:{basin_name}:{section_name}',
            'discharge_observed': 'Discharge:section_discharge_obs:{basin_name}:{section_name}'
        }

    if file_dset_tmpl:
        if 'ensemble_n' in list(file_dset_tmpl.keys()) or 'run_ensemble' in list(file_dset_tmpl.keys()):
            for ensemble_str in ['ensemble_n', 'run_ensemble']:
                if ensemble_str in list(file_dset_tmpl.keys()):
                    ensemble_n = file_dset_tmpl[ensemble_str]
                    break
        else:
            print(' ERROR ===> The "ensemble_n" key must be in the file dataset template')
            raise IOError('Key error')
        if 'time_reference' in list(file_dset_tmpl.keys()):
            time_reference = file_dset_tmpl['time_reference']
        else:
            print(' ERROR ===> The "time" key must be in the file dataset template')
            raise IOError('Key error')

    else:
        print(' ERROR ===> The "file_dset_template" must be defined by "section_name", '
              '"ensemble_n" and "time" keys.')
        raise IOError('Object file template not defined')

    if isinstance(time_reference, str):
        time_reference = pd.Timestamp(time_reference)
        time_reference = time_reference.strftime(format='%Y%m%d%H%M')
    else:
        print(' ERROR ===> Object "time" must be in string format')
        raise NotImplementedError('Case not implemented yet')

    if ensemble_n is None:
        run_group = ['deterministic']
    else:
        ensemble_range = list(np.arange(1, ensemble_n + 1, 1))
        run_group = ['{:03}'.format(ensemble_id) for ensemble_id in ensemble_range]
        np.arange(1, ensemble_n)

    folder_name_tmpl_collections, file_name_tmpl_collections = os.path.split(file_path_tmpl_collections)

    ts_time = None
    ts_dict = None
    attrs_dict = None
    for run_step in run_group:

        tags_obj = {'ensemble_id': run_step, 'time_reference': time_reference}

        folder_name_step = folder_name_tmpl_collections.format(**tags_obj)
        file_name_step = file_name_tmpl_collections.format(**tags_obj)
        file_path_step = os.path.join(folder_name_step, file_name_step)

        file_tag_step = os.path.split(file_path_step)[1]

        print(' INFO ----> Get collections file "' + file_tag_step + '" ... ')

        if os.path.exists(file_path_step):

            file_dset = xr.open_dataset(file_path_step)

            for var_key, var_name in file_vars_tmpl.items():

                if (var_key == 'time') and (ts_time is None):

                    var_name = file_vars_tmpl['time']
                    ts_time = file_dset[var_name].values
                    ts_time = pd.DatetimeIndex(ts_time)

                    if ts_dict is None:
                        ts_dict = {var_key: ts_time}
                    elif var_key not in list(ts_dict.keys()):
                        ts_dict[var_key] = ts_time

                elif var_key != 'time':

                    var_name = var_name.format(**tags_obj)
                    var_ts = file_dset[var_name].values
                    var_attrs = file_dset.attrs
                    var_ts[var_ts < 0.0] = np.nan

                    if ts_dict is None:
                        ts_dict = {var_key: var_ts}
                    elif var_key not in list(ts_dict.keys()):
                        ts_dict[var_key] = var_ts
                    else:
                        var_tmp = ts_dict[var_key]
                        var_tmp = np.vstack([var_tmp, var_ts])
                        ts_dict[var_key] = var_tmp

                    tmp_attrs = deepcopy(var_attrs)
                    for attr_key, attr_value in tmp_attrs.items():
                        if attr_key in attrs_collections_excluded:
                            var_attrs.pop(attr_key)

                    if attrs_dict is None:
                        attrs_dict = deepcopy(var_attrs)
                    else:
                        for attr_key, attr_value in var_attrs.items():
                            if attr_key in list(attrs_dict.keys()):
                                tmp_value = attrs_dict[attr_key]
                                if isinstance(tmp_value, str):
                                    if tmp_value != attr_value:
                                        tmp_list = [tmp_value, attr_value]
                                        attrs_dict[attr_key] = tmp_list
                                elif isinstance(tmp_value, list):
                                    if attr_value not in tmp_value:
                                        tmp_value.append(attr_value)
                                elif isinstance(tmp_value, (float, int, np.int64)):
                                    if tmp_value != attr_value:
                                        print(' WARNING ===> Attribute values is unexpected. '
                                              'Value differs with the stored one')
                                else:
                                    print(' WARNING ===> Attribute format is unexpected.')
                            else:
                                attrs_dict[attr_key] = attr_value

            print(' INFO ----> Get collections file "' + file_tag_step + '" ... DONE')
        else:
            print(' WARNING ===> File not found')
            print(' INFO ----> Get collections file "' + file_tag_step + '" ... FAILED')

    if (attrs_dict is not None) and (file_dset_tmpl is not None):
        attrs_dict = {**file_dset_tmpl, **attrs_dict}

        for attr_key, attr_value in attrs_dict.items():
            if isinstance(attr_value, str):
                try:
                    time_value = pd.Timestamp(attr_value)
                    time_string = time_value.strftime(format=time_format_algorithm)
                    attrs_dict[attr_key] = time_string
                except BaseException as base_exp:
                    pass
    else:
        print(' WARNING ===> Attribute object is not correctly defined.  Some fields could be not available')

    print(' INFO ----> Create collections dataframe ... ')
    if ts_dict is not None:
        ts_dframe_collections = None
        for ts_key, ts_values in ts_dict.items():
            if ts_key != 'time':

                if ts_values.ndim == 1:
                    ts_data = ts_values
                    ts_columns = [ts_key]
                else:
                    ts_data = np.transpose(ts_values)
                    ts_n = ts_data.shape[1]
                    ts_range = list(np.arange(1, ts_n + 1, 1))
                    ts_columns = ['{:}_{:03}'.format(ts_key, ts_id) for ts_id in ts_range]

                ts_dframe_tmp = pd.DataFrame(index=ts_time, data=ts_data, columns=ts_columns)

                if ts_dframe_collections is None:
                    ts_dframe_collections = deepcopy(ts_dframe_tmp)
                else:
                    ts_dframe_collections = ts_dframe_collections.join(ts_dframe_tmp)

        if attrs_dict is not None:
            ts_dframe_collections.attrs = attrs_dict

        print(' INFO ----> Create collections dataframe ... DONE')

    else:
        ts_dframe_collections = None
        print(' WARNING ===> Datasets is null')
        print(' INFO ----> Create collections dataframe ... FAILED')

    print(' INFO ---> Organize time series collections ... DONE')

    return ts_dframe_collections
# -------------------------------------------------------------------------------------
