"""
Library Features:

Name:          lib_data_organizer
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20211025'
Version:       '1.0.0'
"""
# ------------------------------------------------------------------------------------
# Libraries
import os
from copy import deepcopy

from library.common.lib_data_geo_shapefile import create_group_file_section
from library.common.lib_utils_io import write_obj, read_obj
from library.common.lib_data_io_generic import fill_file_template, fill_ensemble_template
from library.common.lib_utils_system import make_folder
from library.common.lib_utils_time import create_time_range
from library.common.lib_utils_process import fill_command_line_template, exec_process
from library.common.lib_utils_ssh import exists_remote

from library.common.lib_data_io_netcdf import organize_collections_ts, join_collections_ts

from library.common.lib_info_args import time_format_algorithm
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to download the datasets
def collect_datasets(
        datasets_keys,
        obj_file_path_source, obj_file_path_destination,
        obj_time_reference=None, obj_time_period=None, obj_time_frequency=None,
        obj_flag_active=True, obj_flag_update=True, obj_ensemble_n=None,
        obj_transfer_method=None, settings_transfer_method=None,
        obj_template_time=None, obj_template_transfer=None,
        obj_machine=None):

    if 'machine_address' in list(obj_machine.keys()):
        machine_address = obj_machine['machine_address']
    else:
        print(' ERROR ===> The field "machine_address" must be defined.')
        raise IOError('Check your settings to correctly set the field')

    if 'machine_type' in list(obj_machine.keys()):
        machine_type = obj_machine['machine_type']
    else:
        print(' ERROR ===> The field "machine_type" must be defined.')
        raise IOError('Check your settings to correctly set the field')

    if obj_template_transfer is None:
        obj_template_transfer = {
            'machine_address': None,
            'path_source': None,
            'path_destination': None,
        }

    for datasets_key in datasets_keys:

        print(' INFO ---> Collect datasets for "' + datasets_key + '" type ... ')

        if obj_time_reference is not None:
            time_reference = obj_time_reference[datasets_key]
        else:
            time_reference = None
        if obj_time_period is not None:
            time_period = obj_time_period[datasets_key]
        else:
            time_period = None
        if obj_time_frequency is not None:
            time_frequency = obj_time_frequency[datasets_key]
        else:
            time_frequency = None

        flag_active = obj_flag_active[datasets_key]
        flag_update = obj_flag_update[datasets_key]

        file_path_source_raw = obj_file_path_source[datasets_key]
        file_path_destination_raw = obj_file_path_destination[datasets_key]

        if obj_ensemble_n is not None:
            ensemble_n = obj_ensemble_n[datasets_key]
        else:
            ensemble_n = None

        transfer_method_tag = obj_transfer_method[datasets_key]
        if transfer_method_tag in list(settings_transfer_method.keys()):
            transfer_method_dict = settings_transfer_method[transfer_method_tag]
        else:
            transfer_method_dict = None

        if flag_active:

            if transfer_method_dict is not None:

                transfer_method_raw = transfer_method_dict['command_line']

                if (time_reference is not None) and (time_period is not None) and (time_frequency is not None):
                    file_time_idx, file_time_str = create_time_range(
                        time_reference,
                        time_analysis_left_period=time_period, time_analysis_left_freq = time_frequency,
                        time_analysis_right_period=0, time_analysis_right_freq='H')
                else:
                    file_time_idx, file_time_str = None, None

                if (file_time_idx is not None) and (file_time_str is not None):
                    file_obj_source, file_obj_destination = {}, {}
                    for step_time_idx in file_time_idx:

                        print(' INFO ----> Search dynamic datasets at time "' +
                              step_time_idx.strftime(time_format_algorithm) + '" ... ')

                        obj_filled_time = dict.fromkeys(list(obj_template_time.keys()), step_time_idx)

                        file_path_source_tmp = fill_ensemble_template(
                            file_path_source_raw,  ensemble_n=ensemble_n)
                        file_path_source_def = fill_file_template(
                            file_path_source_tmp,
                            template_filled=obj_filled_time, template_default=obj_template_time)

                        file_path_destination_tmp = fill_ensemble_template(
                            file_path_destination_raw, ensemble_n=ensemble_n)
                        file_path_destination_def = fill_file_template(
                            file_path_destination_tmp,
                            template_filled=obj_filled_time, template_default=obj_template_time)

                        file_obj_source[step_time_idx] = file_path_source_def
                        file_obj_destination[step_time_idx] = file_path_destination_def

                        print(' INFO ----> Search dynamic datasets at time "' +
                              step_time_idx.strftime(time_format_algorithm) + '" ... DONE')

                elif (file_time_idx is None) and (file_time_str is None):

                    print(' INFO ----> Search static datasets ... ')
                    if datasets_key == 'sections':
                        file_path_source_def = create_group_file_section(file_path_source_raw)
                        file_path_destination_def = create_group_file_section(file_path_destination_raw)
                    else:
                        file_path_source_def = [file_path_source_raw]
                        file_path_destination_def = [file_path_destination_raw]

                    file_obj_source = {datasets_key: file_path_source_def}
                    file_obj_destination = {datasets_key: file_path_destination_def}
                    print(' INFO ----> Search static datasets ... DONE')

                else:
                    print(' ERROR ===> Time conditions are not defined as expected.')
                    raise RuntimeError('Object are not correctly created')

                for file_path_source_list, file_path_destination_list in zip(
                        file_obj_source.values(), file_obj_destination.values()):

                    flag_exists = None
                    for file_path_source_step, file_path_destination_step in zip(
                            file_path_source_list, file_path_destination_list):

                        folder_name_source_step, file_name_source_step = os.path.split(
                            file_path_source_step)
                        folder_name_destination_step, file_name_destination_step = os.path.split(
                            file_path_destination_step)

                        if flag_update:
                            if os.path.exists(file_path_destination_step):
                                os.remove(file_path_destination_step)

                        print(' INFO ----> Get file "' + file_name_source_step + '" ... ')

                        if not os.path.exists(file_path_destination_step):

                            obj_filled_transfer = {
                                'machine_address': machine_address,
                                'path_source': file_path_source_step,
                                'path_destination': file_path_destination_step,
                            }

                            if flag_exists is None:
                                if machine_type == 'local':
                                    flag_exists_first = True if os.path.exists(file_path_source_list[0]) else False
                                    flag_exists_last = True if os.path.exists(file_path_source_list[-1]) else False
                                    if flag_exists_first and flag_exists_last:
                                        flag_exists = True
                                    else:
                                        flag_exists = False
                                elif machine_type == 'remote':
                                    flag_exists_first = exists_remote(machine_address, file_path_source_list[0])
                                    flag_exists_last = exists_remote(machine_address, file_path_source_list[-1])
                                    if flag_exists_first and flag_exists_last:
                                        flag_exists = True
                                    else:
                                        flag_exists = False
                                else:
                                    print(' ERROR ===> The machine type is not allowed.')
                                    raise NotImplementedError('Case not implemented yet')

                            if flag_exists:

                                transfer_method_def = fill_command_line_template(
                                    transfer_method_raw,
                                    template_filled=obj_filled_transfer, template_raw=obj_template_transfer)

                                make_folder(folder_name_destination_step)
                                exec_process(transfer_method_def)

                                print(' INFO ----> Get file "' + file_name_source_step + '" ... DONE')

                            else:
                                print(' INFO ----> Get file "' + file_name_source_step +
                                      '" ... SKIPPED. Datasets are not available in the source ')

                        else:
                            print(' INFO ----> Get file "' + file_name_source_step +
                                  '" ... SKIPPED. Datasets are previously collected')

                print(' INFO ---> Collect datasets for "' + datasets_key +
                      '" type ... DONE')
            else:
                print(' INFO ---> Collect datasets for "' + datasets_key +
                      '" type ... FAILED. Transfer method is not defined')
        else:
            print(' INFO ---> Collect datasets for "' + datasets_key +
                  '" type ... SKIPPED. Datasets is not activated')

# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to adjust the time series
def adjust_ts(dframe_reference_raw=None, dframe_other_raw=None,
              dframe_reference_name='dataframe_reference', dframe_other_name='dataframe_other'):

    print(' INFO --> Adjust the dataframe "' + dframe_other_name +
          '" with the reference dataframe "' + dframe_reference_name + '" ... ')

    if (dframe_reference_raw is not None) and (dframe_other_raw is not None):
        dframe_reference_period, dframe_other_subperiod = join_collections_ts(
            dframe_reference=dframe_reference_raw, dframe_other=dframe_other_raw)

        print(' INFO --> Adjust the dataframe "' + dframe_other_name +
              '" with the reference dataframe "' + dframe_reference_name + '" ... DONE')
    else:
        print(' WARNING ===> Reference and other dataframe must be defined. DataFrame(s) will be NoneType')
        dframe_reference_period, dframe_other_subperiod = None, None

        print(' INFO --> Adjust the dataframe "' + dframe_other_name +
              '" with the reference dataframe "' + dframe_reference_name + '" ... FAILED')

    return dframe_reference_period, dframe_other_subperiod
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to wrap the time series
def wrap_ts(file_path_source, file_path_destination,
            file_run='hmc', file_description='time_series',
            dict_info=None, dict_flags=None, dict_variables=None):

    print(' INFO --> Wrap "' + file_run + '" datasets of "' + file_description + '" ... ')

    ts_collections = None
    if dict_flags['update']:
        if os.path.exists(file_path_destination):
            os.remove(file_path_destination)

    if dict_flags['activate']:

        if not os.path.exists(file_path_destination):
            ts_collections = organize_collections_ts(
                file_path_tmpl_collections=file_path_source,
                file_dset_tmpl=dict_info, file_vars_tmpl=dict_variables)

            if ts_collections is not None:
                folder_name_destination, file_name_destination = os.path.split(file_path_destination)
                make_folder(folder_name_destination)
                write_obj(file_path_destination, ts_collections)
                print(' INFO --> Wrap "' + file_run + '" datasets of "' + file_description + '" ... DONE')
            else:
                print(' WARNING ===> "' + file_run + '" datasets of "' + file_description +
                      '" not found. TimeSeries is NoneType.')
                print(' INFO --> Wrap "' + file_run + '" datasets of "' + file_description + '" ... FAILED')
        else:
            ts_collections = read_obj(file_path_destination)
            print(' INFO --> Wrap "' + file_run + '" datasets of "' + file_description + '" ... PREVIOUSLY DONE')

            if ts_collections is None:
                print(' WARNING ===> The time_series previously stored is defined by NoneType. '
                      'Check the datasets because this condition is not expected. '
                      'Try to update the datasets using the condition in the configuration file')

    return ts_collections
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to wrap the file
def wrap_file(file_path_in, file_mode='cmd', file_root_default=None, file_folder_default='data', file_nullify=False):

    if file_mode == 'interactive_remote':

        file_folder_tmp, file_name_out = os.path.split(file_path_in)
        file_folder_out = os.path.join(file_root_default, file_folder_default)

        make_folder(file_folder_out)
        file_path_out = os.path.join([file_folder_out, file_name_out])
    elif (file_mode == 'cmd') or (file_mode == 'interactive_local'):
        file_path_out = deepcopy(file_path_in)
    else:
        print(' ERROR ===> File mode tag "' + file_mode + '" is not supported')
        raise IOError('File mode tag must be "cmd", "interactive_local" or "interactive_remote')

    if file_nullify:
        file_path_out = None

    return file_path_out
# -------------------------------------------------------------------------------------
