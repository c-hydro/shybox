"""
Class Features

Name:          driver_app_settings
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20241212'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import os
from copy import deepcopy

from apps.runner_toolkit.settings import handler_app_settings as handler_settings

from apps.generic_toolkit.lib_utils_file import expand_file_path
from apps.generic_toolkit.lib_utils_dict import check_keys_of_dict, flat_dict_key, add_dict_key
from apps.generic_toolkit.lib_default_args import logger_name, logger_arrow
from apps.generic_toolkit.lib_default_args import collector_data

# logging
logger_stream = logging.getLogger(logger_name)

# debugging
# import matplotlib.pylab as plt
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# class to configure settings
class DrvSettings:

    # ------------------------------------------------------------------------------------------------------------------
    # global variable(s)
    class_type = 'driver_settings'
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # class initialization
    def __init__(self,  file_name: str = None,
                 file_key: str = None, file_time: str = None,
                 settings_collectors: dict = None, settings_system: dict = None,
                 **kwargs) -> None:

        self.file_name = file_name
        self.file_key = file_key
        self.file_time = file_time

        self.settings_collectors = settings_collectors
        self.settings_system = settings_system

        self.tag_root_priority, self.tag_root_flags = 'priority', 'flags'
        self.tag_root_variables, self.tag_root_configuration = 'variables', 'configuration'

        self.tag_var_lut_env, self.tag_var_lut_user = 'lut:environment', 'lut:user'
        self.tag_var_format, self.tag_var_template = 'format', 'template'

        self.variables_settings, self.variables_collector = None, None
        self.variables_env, self.variables_user, self.variables_system = None, None, None

        # get driver settings
        if (self.file_name is not None) and (self.settings_collectors is None) and (self.settings_system is None):
            if os.path.exists(self.file_name):
                self.settings_handler = handler_settings.SettingsHandler.from_file(
                    file_name=self.file_name, file_key=self.file_key, file_time=self.file_time)
            else:
                logger_stream.error(logger_arrow.error + ' Error in reading settings file "' + self.file_name + '"')
                raise ValueError('File "' + self.file_name + '" does not exists.')
        elif (self.file_name is None) and (self.settings_collectors is None) and (self.settings_system is not None):
            self.settings_handler = handler_settings.SettingsHandler(
                settings_obj=self.settings_system)
        elif (self.file_name is not None) and (self.settings_collectors is not None) and (self.settings_system is None):
            self.settings_handler = handler_settings.SettingsHandler.from_file_and_collector(
                file_name=self.file_name, file_key=self.file_key, file_time=self.file_time,
                collector_vars=self.settings_collectors, collector_overwrite=True)
        elif (self.file_name is None) and (self.settings_collectors is not None) and (self.settings_system is None):
            self.settings_handler = handler_settings.SettingsHandler.from_collector(
                file_key=self.file_key, file_time=self.file_time,
                collector_vars=self.settings_collectors, collector_overwrite=True)
        else:
            logger_stream.error(logger_arrow.error + ' Error in defining driver settings')
            raise RuntimeError('Check your algorithm configuration')

    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # method to view settings variable(s)
    def view_variable_settings(self, data: dict = None, mode: bool = True) -> None:
        # info algorithm (start)
        logger_stream.info(logger_arrow.info(tag='info_method') + 'View "settings_variables" ... ')
        if mode:
            self.settings_handler.view(data)
        # info algorithm (start)
        logger_stream.info(logger_arrow.info(tag='info_method') + 'View "settings_variables" ... DONE')
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # method to organize settings variable(s)
    @staticmethod
    def organize_variable_settings(settings_obj: dict = None, collector_obj: dict = None) -> dict:

        # info algorithm (start)
        logger_stream.info(logger_arrow.info(tag='info_method') + 'Organize "settings_variables" ... ')

        if collector_obj is None:
            collector_obj = deepcopy(settings_obj)

        # get settings variables
        common_obj = {}
        for var_key, var_value in settings_obj.items():
            if var_key in list(collector_obj.keys()):
                logger_stream.warning(logger_arrow.warning + 'Variable "' + var_key +
                                      '" found in the collector object. Variable will be overwritten')
                common_obj[var_key] = collector_obj[var_key]
            else:
                common_obj[var_key] = var_value

        # collect singleton data object
        collector_data.collect(common_obj)

        # info algorithm (start)
        logger_stream.info(logger_arrow.info(tag='info_method') + 'Organize "settings_variables" ... DONE')

        return common_obj

    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # method to configure settings variable(s)
    def configure_variable_by_settings(self, collector_settings: dict = None, system_settings: dict = None,
                                       **kwargs) -> (dict, dict, dict):

        # info algorithm (start)
        logging.info(logger_arrow.info(tag='info_method') + 'Configure "variables_by_settings" ... ')

        # get application settings
        settings_flags = self.settings_handler.select_variable_algorithm(
            tag_name=self.tag_root_flags, tag_save=False)
        app_priority = self.settings_handler.select_variable_algorithm(
            tag_name=self.tag_root_priority, tag_save=False)
        app_variables = self.settings_handler.select_variable_algorithm(
            tag_name=self.tag_root_variables, tag_save=True)
        app_configuration = self.settings_handler.select_variable_algorithm(
            tag_name=self.tag_root_configuration, tag_save=True)

        # set variables priority
        variables_ref, variables_other = self.settings_handler.select_variable_priority(app_priority)

        # get variables settings
        variables_lut_env = self.settings_handler.select_variable_algorithm(
            settings_obj=app_variables, tag_name=self.tag_var_lut_env, tag_sep=':', tag_part='last', tag_save=False)
        variables_lut_user = self.settings_handler.select_variable_algorithm(
            settings_obj=app_variables, tag_name=self.tag_var_lut_user, tag_sep=':', tag_part='last', tag_save=False)
        variables_format = self.settings_handler.select_variable_algorithm(
            settings_obj=app_variables, tag_name=self.tag_var_format, tag_save=False)
        variables_tmpl = self.settings_handler.select_variable_algorithm(
            settings_obj=app_variables,  tag_name=self.tag_var_template, tag_save=False)
        # check variables consistency
        variables_check_format = check_keys_of_dict(
            d1={**variables_lut_env, **variables_lut_user}, d2=variables_format, name1='lut', name2=self.tag_var_format)
        variables_check_tmpl = check_keys_of_dict(
            d1={**variables_lut_env, **variables_lut_user}, d2=variables_tmpl, name1='lut', name2=self.tag_var_template)

        # select variable environment
        self.variables_env = self.settings_handler.select_variable_env(
            lut_obj_in=variables_lut_env, format_obj=variables_format, template_obj=variables_tmpl,
            lut_swap=True, settings_priority=True)
        # select variable user
        self.variables_user = self.settings_handler.select_variable_user(
            lut_obj_in=variables_lut_user, format_obj=variables_format, template_obj=variables_tmpl)

        # merge variables system
        self.variables_system = self.settings_handler.merge_variable_settings(
            variables_env=self.variables_env, variables_user=self.variables_user)

        # update variables settings (using configuration)
        self.variables_settings = self.settings_handler.update_variable_by_configuration(
            variable_obj=self.variables_system, configuration_obj=app_configuration,
            format_obj=variables_format, template_obj=variables_tmpl)

        # update variables settings (using itself)
        self.variables_settings = self.settings_handler.update_variable_by_self(
            variable_obj=self.variables_settings)

        # update variables settings (using dictionary)
        self.variables_settings = self.settings_handler.update_variable_by_dict(
            variable_obj=self.variables_settings,
            selection_obj={'time_run': self.settings_handler.settings_time, 'time_restart': None},
            selection_force=True)

        # update variables collector (using configuration)
        self.variables_collector = self.settings_handler.update_variable_by_configuration(
            variable_obj=self.variables_settings, configuration_obj=self.settings_collectors,
            format_obj=variables_format, template_obj=variables_tmpl)

        # info algorithm (end)
        logging.info(logger_arrow.info(tag='info_method') + 'Configure "variables_by_settings" ... DONE')

        return self.variables_settings, self.variables_collector, self.variables_system

    # ------------------------------------------------------------------------------------------------------------------


    # ------------------------------------------------------------------------------------------------------------------
    # method to configure settings variable(s) by other settings
    def configure_variable_by_dict(self, other_settings: dict = None) -> (dict, dict, dict):

        # info algorithm (start)
        logging.info(logger_arrow.info(tag='info_method') + 'Configure "variables_by_dictionary" ... ')

        # update variables settings (using dictionary)
        self.variables_settings = {**self.settings_handler.collector_obj, **other_settings}
        # update variables settings (using itself)
        self.variables_settings = self.settings_handler.update_variable_by_self(
            variable_obj=self.variables_settings)

        self.variables_collector, self.variables_system = {}, {}

        # info algorithm (end)
        logging.info(logger_arrow.info(tag='info_method') + 'Configure "variables_by_dictionary" ... DONE')

        return self.variables_settings, self.variables_collector, self.variables_system

    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # method to get variable obj
    def get_variable_by_tag(self, tag_name: str = 'namelist') -> dict:
        # get structure object
        generic_obj = self.settings_handler.generic_obj
        variable_obj = self.settings_handler.select_variable_algorithm(
            settings_obj=generic_obj, tag_name=tag_name, tag_save=False)
        return variable_obj
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # method to fill variable by dictionary
    def fill_variable_by_dict(self, variable_obj: dict = None, info_obj: dict = None) -> dict:

        # flat dictionary of variable(s)
        tmp_obj = flat_dict_key(variable_obj)

        for tmp_key, tmp_value in tmp_obj.items():
            if isinstance(tmp_value, str):
                tmp_value = expand_file_path(tmp_value)
            elif isinstance(tmp_value, list):
                tmp_value = [expand_file_path(tmp_value_i) for tmp_value_i in tmp_value]
            else:
                pass

            tmp_obj[tmp_key] = tmp_value

        # fill variable object
        common_obj = {**tmp_obj, **info_obj}
        filled_obj = self.settings_handler.update_variable_by_self(common_obj)

        # select dictionary of variable(s)
        select_obj = {}
        for var_key, var_value in filled_obj.items():
            if var_key in list(tmp_obj.keys()):
                select_obj[var_key] = filled_obj[var_key]

        compose_obj = {}
        for tmp_key, var_value in select_obj.items():
            list_key = tmp_key.split(':')
            add_dict_key(compose_obj, list_key, var_value)

        return compose_obj

    # ------------------------------------------------------------------------------------------------------------------

# ------------------------------------------------------------------------------------------------------------------
