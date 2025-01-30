"""
Class Features

Name:          handler_hmc_settings
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20241212'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import os
import re
import pandas as pd

from tabulate import tabulate
from copy import deepcopy

from shybox.generic_toolkit.lib_utils_file import expand_file_path
from shybox.generic_toolkit.lib_utils_time import convert_time_format
from shybox.generic_toolkit.lib_utils_settings import get_data_settings

from shybox.generic_toolkit.lib_utils_string import replace_string, fill_tags2string
from shybox.generic_toolkit.lib_utils_dict import (get_dict_value_by_key, swap_keys_values, filter_dict_by_keys,
                                                   add_dict_key, flat_dict_key)

from shybox.generic_toolkit.lib_default_args import logger_name, logger_arrow
from shybox.generic_toolkit.lib_default_args import collector_data

# logging
logger_stream = logging.getLogger(logger_name)
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# class settings handler
class SettingsHandler:

    class_type = 'settings_handler'
    excluded_keys = ['__comment__', '_comment_', '__comment', '_comment']

    # initialize class
    def __init__(self, settings_obj: dict = None, system_obj: dict = None, collector_obj: dict = None,
                 settings_time: (str, pd.Timestamp) = None, settings_key: (str, None) = 'info_settings',
                 **kwargs) -> None:

        self.settings_obj = settings_obj
        self.settings_time = settings_time
        self.system_obj = system_obj
        self.collector_obj = collector_obj
        self.variables_obj = {}

        if self.settings_obj is None:
            if settings_key is not None:
                self.settings_obj = {settings_key: None}
            else:
                self.settings_obj = {}

        if settings_key is not None:
            self.generic_obj = deepcopy(self.settings_obj)
            if settings_key in list(self.settings_obj.keys()):
                settings_tmp = self.settings_obj[settings_key]
                self.settings_obj.pop(settings_key)
                self.settings_obj = {**self.settings_obj, **settings_tmp}
            else:
                logger_stream.error(logger_arrow.error + 'Key "' + settings_key + '" not found in settings object.')
                raise KeyError('Key must be defined.')
        else:
            self.settings_obj, self.generic_obj = self.settings_obj, self.settings_obj

        if self.system_obj is None:
            self.system_obj = dict(os.environ)
        if self.collector_obj is None:
            self.collector_obj = {}

        self.variables_reference, self.variables_other = 'environment', 'user'

    @classmethod
    def from_file(cls, file_name: (str, None), file_key: (str, None) = 'info_time',
                  file_time: (str, pd.Timestamp) = None, **kwargs):

        if file_name is not None:
            if os.path.exists(file_name):
                file_settings = get_data_settings(file_name)
            else:
                logger_stream.error(logger_arrow.error + 'File "' + file_name + '" does not exists.')
                raise FileNotFoundError('File must be defined.')
        else:
            file_settings = None

        return cls(settings_obj=file_settings, settings_key=file_key, settings_time=file_time)

    @classmethod
    def from_file_and_collector(cls, file_name: (str, None), file_key: (str, None) = 'info_time',
                                file_time: (str, pd.Timestamp) = None,
                                collector_vars: dict = None, collector_overwrite: bool = True, **kwargs):

        if file_name is not None:
            if os.path.exists(file_name):
                file_settings = get_data_settings(file_name)
            else:
                logger_stream.error(logger_arrow.error + 'File "' + file_name + '" does not exists.')
                raise FileNotFoundError('File must be defined.')
        else:
            file_settings = None

        if file_settings is not None:
            if collector_vars is not None:
                for collectors_key, collectors_value in collector_vars.items():
                    if collectors_key in list(file_settings.keys()):
                        tmp_value = file_settings[collectors_key]
                        if tmp_value is None:
                            file_settings[collectors_key] = collectors_value
        if collector_vars is None:
            collector_vars = {}
        if file_settings is None:
            file_settings = {}

        if file_time is not None:
            file_time = convert_time_format(file_time, time_conversion='str_to_str')

        return cls(settings_obj=file_settings, settings_key=file_key, settings_time=file_time,
                   collector_obj=collector_vars)

    @classmethod
    def from_collector(cls, file_time: (str, pd.Timestamp) = None,
                       collector_vars: dict = None,  **kwargs):

        if collector_vars is None or not collector_vars:
            logger_stream.error(logger_arrow.error +
                                'Collector variables are not defined. All variables are set to NoneType.')
            raise RuntimeError('Collector variables must be defined in this configuration mode.')

        file_settings = None

        if file_settings is None:
            file_settings = {}

        if file_time is not None:
            file_time = convert_time_format(file_time, time_conversion='str_to_str')

        return cls(settings_obj=file_settings, settings_key=None, settings_time=file_time,
                   collector_obj=collector_vars)


    def select_variable_priority(self, priority_obj: dict = None) -> (str, str):

        if priority_obj is not None:
            if 'reference' in list(priority_obj.keys()):
                self.variables_reference = priority_obj['reference']
            else:
                logger_stream.warning(logger_arrow.warning + 'Reference tag is not defined. Use the default priority.')
            if 'other' in list(priority_obj.keys()):
                self.variables_other = priority_obj['other']
            else:
                logger_stream.warning(logger_arrow.warning + 'Other tag is not defined. Use the default priority.')
        else:
            logger_stream.error(logger_arrow.error + 'Priority object not defined.')

        return self.variables_reference, self.variables_other

    def select_variable_algorithm(self, settings_obj: dict = None, variable_obj: dict = None,
                                  tag_name: (str, None) = 'default',
                                  tag_sep: str = ':', tag_part: str = 'last', tag_save: bool = False) -> (dict, None):

        if tag_name is None:
            tag_name = 'not_defined'
        if tag_sep in tag_name:
            if tag_part == 'last':
                tag_name = tag_name.split(tag_sep)[-1]
            elif tag_part == 'first':
                tag_name = tag_name.split(tag_sep)[0]
            else:
                logger_stream.error(logger_arrow.error + 'Tag part "' + tag_part + '" not expected.')
                raise NotImplementedError('Case not implemented yet.')

        if settings_obj is None:
            settings_obj = self.settings_obj
        if variable_obj is None:
            variable_obj = self.variables_obj

        if tag_name is not None:
            if tag_name not in list(variable_obj.keys()):
                variable_args = get_dict_value_by_key(settings_obj, tag_name)

                if variable_args is None:
                    variable_obj, variable_key = None, None
                else:
                    variable_obj, variable_key = variable_args[0], variable_args[1]

                if variable_obj is not None:
                    if tag_save:
                        self.variables_obj[tag_name] = variable_obj
                else:
                    logger_stream.warning(
                        logger_arrow.warning + 'Tag "' + tag_name + '" does not exist in the variable object.')
                    return None
            else:
                logger_stream.error(
                    logger_arrow.error + 'Tag "' + tag_name + '" already exists in the variables object.')
                raise KeyError('Tag string cannot overwrite saved keys.')
        else:
            if tag_name not in list(self.variables_obj.keys()):
                variable_obj = deepcopy(settings_obj)
                if tag_save:
                    self.variables_obj[tag_name] = variable_obj
            else:
                logger_stream.error(
                    logger_arrow.error + 'Tag "' + tag_name + '" already exists in the variables object.')
                raise KeyError('Tag string cannot overwrite saved keys.')

        return variable_obj

    def update_settings(self, lut_by_user: dict = None, format_by_user: dict = None, template_by_user: dict = None):

        if (lut_by_user is not None) and (template_by_user is not None):

            settings_flatten = flat_dict_key(self.settings_obj, separator=":", obj_dict={})

            settings_filled = {}
            for data_key, tmp_value in settings_flatten.items():
                if isinstance(tmp_value, str):

                    data_value = fill_tags2string(tmp_value, template_by_user, lut_by_user)[0]

                    tmp_tag = replace_string(tmp_value, string_replace={'{':'', '}': ''})
                    if tmp_tag in list(template_by_user.keys()):
                        tmp_format = template_by_user[tmp_tag]
                        if tmp_format == 'int':
                            if data_value.isnumeric():
                                data_value = int(data_value)
                        if tmp_format == 'float':
                            if data_value.isnumeric():
                                data_value = float(data_value)
                else:
                    data_value = tmp_value

                tmp_key = data_key.split(':')[-1]

                if tmp_key in list(template_by_user.keys()):
                    data_format = template_by_user[tmp_key]
                    if data_value is not None:
                        if data_format == 'int':
                            if isinstance(data_value, str):
                                if data_value.isnumeric():
                                    data_value = int(data_value)
                            if isinstance(data_value, (float, int)):
                                data_value = int(data_value)
                        if data_format == 'float':
                            if isinstance(data_value, str):
                                if data_value.isnumeric():
                                    data_value = float(data_value)
                            if isinstance(data_value, (float, int)):
                                data_value = float(data_value)

                settings_filled[data_key] = data_value

            settings_update = {}
            for tmp_key, data_value in settings_filled.items():
                list_key = tmp_key.split(':')
                if list_key[-1] not in self.excluded_keys:
                    add_dict_key(settings_update, list_key, data_value)
                else:
                    logger_stream.warning(logger_arrow.warning + 'Settings "' + str(list_key) +
                                          '" removed from settings object. Excluded key found.')

            self.settings_obj = settings_update

        return self.settings_obj

    @staticmethod
    def update_variable_by_configuration(variable_obj: dict = None, configuration_obj: dict = None,
                                         format_obj: dict = None, template_obj: dict = None,
                                         default_value: {int, float, bool} = None) -> dict:

        if configuration_obj is None:
            configuration_obj = deepcopy(variable_obj)

        for tag_key, configuration_value in configuration_obj.items():

            format_key, template_key = None, None
            if tag_key in list(format_obj.keys()):
                format_key = format_obj[tag_key]
            if tag_key in list(template_obj.keys()):
                template_key = template_obj[tag_key]

            if configuration_value is not None:
                configuration_key = None
                if isinstance(configuration_value, str):
                    if any(re.findall(r'{|}', configuration_value, re.IGNORECASE)):
                        configuration_key = replace_string(configuration_value, string_replace={'{': '', '}': ''})

                if configuration_key in list(variable_obj.keys()):
                    variable_value = variable_obj[configuration_key]
                else:
                    variable_value = deepcopy(configuration_value)

                variable_select = deepcopy(variable_value)

            else:
                if tag_key in list(variable_obj.keys()):
                    variable_select = variable_obj[tag_key]
                else:
                    variable_select = None

            if variable_select is not None:
                apply_format = True
                if isinstance(variable_select, str):
                    if any(re.findall(r'{|}', variable_select, re.IGNORECASE)):
                        apply_format = False
                if apply_format:
                    value_defined = format_variable(
                        var_value=variable_select, var_format=format_key, var_template=template_key,
                        default_value=default_value)
                else:
                    value_defined = variable_select
            else:
                value_defined = default_value
                logger_stream.warning(logger_arrow.warning + 'Value for variable "' +
                                      str(tag_key) + '" is not defined. Default value will be used.')

            configuration_obj[tag_key] = value_defined

        return configuration_obj

    @staticmethod
    def update_variable_by_dict(variable_obj: dict,
                                selection_obj: dict = None, selection_force: bool = False,
                                tag_sep: str =':', tag_part: str = 'last') -> dict:
        if selection_obj is None:
            selection_obj = {}
        for var_key, var_value in variable_obj.items():
            if var_value is None:
                if var_key in list(selection_obj.keys()):
                    variable_obj[var_key] = selection_obj[var_key]
            elif isinstance(var_value, str):
                if any(re.findall(r'{|}', var_value, re.IGNORECASE)):

                    if (tag_sep is not None) and (tag_sep in var_key):
                        if tag_part == 'last':
                            var_tag = var_key.split(tag_sep)[-1]
                        elif tag_part == 'first':
                            var_tag = var_key.split(tag_sep)[0]
                        else:
                            logger_stream.error(logger_arrow.error + 'Variable part "' + tag_part + '" not expected.')
                            raise NotImplementedError('Case not implemented yet.')
                    else:
                        var_tag = var_key

                    if var_tag in list(selection_obj.keys()):
                        variable_obj[var_key] = selection_obj[var_tag]

        if selection_force:
            for sel_key, sel_value in selection_obj.items():
                if sel_key not in list(variable_obj.keys()):
                    variable_obj[sel_key] = sel_value
                    logger_stream.warning(
                        logger_arrow.warning + 'Variable "' + sel_key +
                        '" not found in the dictionary but forced by definition in the selection object ')

        return variable_obj

    @staticmethod
    def update_variable_by_self(variable_obj: dict = None) -> dict:

        if variable_obj is None or not variable_obj:
            variable_obj = {}

        variable_self, variable_tmp = deepcopy(variable_obj), deepcopy(variable_obj)

        for var_key, var_value in variable_self.items():

            if var_value is not None:
                if isinstance(var_value, str):

                    variable_self[var_key] = expand_file_path(var_value)

                    if any(re.findall(r'{|}', var_value, re.IGNORECASE)):
                        list_tag = re.findall("\{(.*?)\}", var_value)
                        for var_tag in list_tag:
                            if var_tag in list(variable_tmp.keys()):
                                tmp_value = variable_tmp[var_tag]

                                template_by_var, lut_by_var = {var_tag: 'string'}, {var_tag: tmp_value}
                                var_value = fill_tags2string(var_value, template_by_var, lut_by_var)[0]

                                variable_self[var_key] = var_value

                                logger_stream.warning(
                                    logger_arrow.warning +
                                    'Variable "' + var_key + '" updated by variable "' + str(var_value) +
                                    '" defined in the dictionary to autocomplete the key defined by NoneType')

        return variable_self

    def select_variable_user(self, lut_obj_in: dict = None,
                             format_obj: dict = None, template_obj: dict = None,
                             default_value: {int, float, bool} = None) -> dict:

        lut_obj_out = deepcopy(lut_obj_in)
        if lut_obj_in is not None:

            lut_obj_out = {}
            for lut_key, lut_value in lut_obj_in.items():

                if lut_value is not None:
                    lut_format, lut_tmpl = None, None
                    if lut_key in list(format_obj.keys()):
                        lut_format = format_obj[lut_key]
                    if lut_key in list(template_obj.keys()):
                        lut_tmpl = template_obj[lut_key]

                    apply_format = True
                    if isinstance(lut_value, str):
                        if any(re.findall(r'{|}', lut_value, re.IGNORECASE)):
                            apply_format = False
                    if apply_format:
                        value_def = format_variable(
                            var_value=lut_value, var_format=lut_format, var_template=lut_tmpl,
                            default_value=default_value)
                    else:
                        value_def = lut_value
                else:
                    value_def = default_value
                    logger_stream.warning(logger_arrow.warning + 'Value for variable user "' +
                                          str(lut_key) + '" is not defined. Default value will be used.')

                lut_obj_out[lut_key] = value_def

        return lut_obj_out

    def select_variable_env(self, lut_obj_in: dict = None,
                            format_obj: dict = None, template_obj: dict = None,
                            lut_swap: bool = False, default_value: {int, float, bool} = None,
                            settings_priority: bool = True) -> dict:

        lut_obj_out = deepcopy(lut_obj_in)
        if lut_obj_in is not None:
            if lut_swap:
                lut_collection = swap_keys_values(lut_obj_in)
            else:
                lut_collection = lut_obj_in
            self.system_obj = filter_dict_by_keys(self.system_obj, list(lut_collection.keys()))

            lut_obj_out = {}
            for lut_key, lut_value in lut_collection.items():

                if lut_swap:
                    lut_tag_user, lut_tag_sys = lut_value, lut_key
                else:
                    lut_tag_user, lut_tag_sys = lut_key, lut_value

                lut_format = None
                if lut_tag_user in list(format_obj.keys()):
                    lut_format = format_obj[lut_tag_user]
                lut_tmpl = None
                if lut_tag_user in list(template_obj.keys()):
                    lut_tmpl = template_obj[lut_tag_user]
                if lut_tag_sys in list(self.system_obj.keys()):

                    tmp_system = self.system_obj[lut_tag_sys]
                    value_def = format_variable(
                        var_value=tmp_system, var_format=lut_format, var_template=lut_tmpl,
                        default_value=default_value)

                    if lut_tag_user in list(self.collector_obj.keys()):
                        if settings_priority:
                            tmp_settings = self.collector_obj[lut_tag_user]
                            value_settings = format_variable(
                                var_value=tmp_settings, var_format=lut_format, var_template=lut_tmpl,
                                default_value=default_value)
                            if value_settings is not None:
                                value_def = value_settings

                    lut_obj_out[lut_tag_user] = value_def

        return lut_obj_out


    def merge_variable_settings(self, variables_env: dict = None, variables_user: dict = None) -> dict:

        var_ref, var_other = self.variables_reference, self.variables_other
        if var_ref == 'environment' and var_other == 'user':
            variables_ref = variables_env
            variables_other = variables_user
        elif var_ref == 'user' and var_other == 'environment':
            variables_ref = variables_user
            variables_other = variables_env
        else:
            logger_stream.error(logger_arrow.error + 'Reference "' + var_ref + '" and other "' + var_other +
                                '" tags not expected or in wrong order or case.')
            raise NotImplementedError('Case not implemented yet.')

        if variables_ref is not None and variables_other is not None:

            variables_common = deepcopy(variables_other)
            for var_key, var_value in variables_ref.items():

                if var_key not in list(variables_common.keys()):
                    variables_common[var_key] = var_value
                else:
                    if var_value is not None:
                        variables_common[var_key] = var_value
                    else:
                        logger_stream.warning(
                            logger_arrow.warning + 'Variable "' + str(var_key) +
                            '" already exists in the variables common object. '
                            'Value will be not overwritten because in the reference object is defined by NoneType.')

        elif variables_ref is not None:
            variables_common = variables_ref
        elif variables_other is not None:
            variables_common = variables_other
        else:
            variables_common = {}

        return variables_common

    # method to freeze data
    def freeze(self):
        """
        Freeze the data.
        """
        raise NotImplementedError

    # method to error data
    def error(self):
        """
        Error time data.
        """
        raise NotImplementedError

    # method to write data
    def write(self):
        """
        Write the data.
        """

        raise NotImplementedError

    # method to view data
    def view(self, table_data: dict = None,
             table_variable='variables', table_values='values', table_format='psql') -> None:
        """
        View the data.
        """

        if table_data is None:
            table_data = self.settings_obj

        table_dict = flat_dict_key(data=table_data, separator=":", obj_dict={})
        table_dframe = pd.DataFrame.from_dict(table_dict, orient='index', columns=['value'])

        table_obj = tabulate(
            table_dframe,
            headers=[table_variable, table_values],
            floatfmt=".5f",
            showindex=True,
            tablefmt=table_format,
            missingval='N/A'
        )

        print(table_obj)

    # method to check data
    def check(self):
        """
        Check if time data is available.
        """
        raise NotImplementedError
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to format variable
def format_variable(var_value: (int, float, str, bool, pd.Timestamp) = None,
                    var_format: str = 'string', var_template: str = None,
                    default_value: (int, float, str, bool, pd.Timestamp) = None) -> (int, float, str, bool, pd.Timestamp):

    if var_value is not None:
        if var_format is not None:
            if var_format == 'string':
                var_value = str(var_value)
                if "$HOME" in var_value:
                    var_value = var_value.replace("$HOME", os.path.expanduser("~"))
            elif var_format == 'int':
                var_value = int(var_value)
            elif var_format == 'float':
                var_value = float(var_value)
            elif var_format == 'bool':
                var_value = bool(var_value)
            elif var_format == 'timestamp':
                if var_template is not None:
                    var_value = pd.Timestamp(var_value).strftime(var_template)
                else:
                    logger_stream.error(logger_arrow.error + 'Template "' + str(var_template) +
                                     '" not defined for format "' + str(var_format) + '".')
                    raise KeyError('Template must be defined.')
            else:
                logger_stream.error(logger_arrow.error + 'Format "' + str(var_format) +
                                    '" not expected.')
                raise NotImplementedError('Case not implemented yet.')
        else:
            logger_stream.warning(logger_arrow.warning + 'Variable "'
                                  + str(var_format) + '" format is not defined.')
    else:
        logger_stream.warning(logger_arrow.warning + 'Variable value is defined by default value "' +
                              str(default_value) + '"')

        var_value = default_value

    # expand file path
    if isinstance(var_value, str):
        var_value = expand_file_path(var_value)

    return var_value
# ----------------------------------------------------------------------------------------------------------------------
