"""
Class Features

Name:          drv_app_namelist
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20241126'
Version:       '4.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import os
import re

from copy import deepcopy

from shybox.generic_toolkit.lib_utils_string import replace_string, fill_tags2string
from shybox.generic_toolkit.lib_utils_file import split_file_path, join_file_path, check_file_path
from shybox.generic_toolkit.lib_utils_namelist import select_namelist_type_hmc, select_namelist_type_s3m
from shybox.generic_toolkit.lib_utils_time import convert_time_frequency
from shybox.generic_toolkit.lib_default_args import logger_name, logger_arrow
from shybox.generic_toolkit.lib_default_args import collector_data

from shybox.runner_toolkit.namelist.handler_app_namelist import NamelistHandler

# logging
logger_stream = logging.getLogger(logger_name)

# debugging
# import matplotlib.pylab as plt
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# class to configure namelist
class DrvNamelist:

    # ------------------------------------------------------------------------------------------------------------------
    # global variable(s)
    class_type = 'namelist_driver'
    select_namelist = {
        'hmc:3.1.6': select_namelist_type_hmc,
        'hmc:3.2.0': select_namelist_type_hmc,
        's3m': select_namelist_type_s3m
    }
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # Method class initialization
    def __init__(self, time_obj: dict, namelist_obj: dict,
                 namelist_version: str = '3.1.6', namelist_type: str = 'hmc',
                 namelist_template: str = 'template.namelist', namelist_application: str = 'application.namelist',
                 namelist_update: bool = True, **kwargs) -> None:

        self.time_obj = time_obj
        self.namelist_obj = namelist_obj

        # get namelist fields
        if 'fields' in list(self.namelist_obj.keys()):
            self.namelist_fields_by_value = self.namelist_obj['fields']['by_value']
            self.namelist_fields_by_pattern = self.namelist_obj['fields']['by_pattern']
        else:
            logger_stream.error(logger_arrow.error + 'Namelist fields are not available')
            raise RuntimeError('Namelist fields must be defined')
        # get namelist description
        if 'version' in list(self.namelist_obj.keys()):
            self.namelist_version = self.namelist_obj['description']['version']
        else:
            self.namelist_version = namelist_version
        if 'type' in list(self.namelist_obj.keys()):
            self.namelist_type = self.namelist_obj['description']['type']
        else:
            self.namelist_type = namelist_type

        # get namelist file (template and application)
        if 'file' in list(self.namelist_obj.keys()):
            path_tmpl, path_app = self.namelist_obj['file']['template'], self.namelist_obj['file']['project']
        else:
            path_tmpl, path_app = namelist_template, namelist_application

        self.file_tmpl, self.folder_tmpl = split_file_path(path_tmpl)
        self.file_app, self.folder_app = split_file_path(path_app)

        self.path_tmpl = join_file_path(folder_name=self.folder_tmpl, file_name=self.file_tmpl)
        self.path_app = join_file_path(folder_name=self.folder_app, file_name=self.file_app)

        # get namelist variables
        self.namelist_file = {'template' : self.path_tmpl, 'application': self.path_app}

        self.time_frequency = convert_time_frequency(
            self.time_obj['time_frequency'], frequency_conversion='str_to_int')

        self.namelist_update = namelist_update
        self.line_indent = 4 * ' '

        namelist_structure_obj = self.select_namelist.get(
            namelist_type + ':' + namelist_version, self.error_variable_namelist)
        self.namelist_type_default, self.namelist_structure_default = namelist_structure_obj()

        self.driver_namelist_in, self.driver_namelist_out = None, None

    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # method to dump namelist structure
    def dump_structure_namelist(self, variables_namelist: dict = None) -> None:

        # info algorithm (start)
        logger_stream.info(logger_arrow.info(tag='info_method') + 'Dump "namelist_structure" ... ')

        # get namelist file path
        file_path_app = self.namelist_file['application']
        # check namelist file path
        check_file_path(file_path_app)

        # check namelist update (if needed)
        if self.namelist_update:
            if os.path.exists(file_path_app):
                os.remove(file_path_app)

        # check namelist variables
        if variables_namelist is None or not variables_namelist:
            logger_stream.error(logger_arrow.error + 'Namelist variables are not defined')
            raise RuntimeError('Namelist variables must be defined')

        # check file namelist availability
        if not os.path.exists(file_path_app):

            # info write namelist (start)
            logger_stream.info(logger_arrow.info(tag='info_lev_1') + 'Write namelist file ... ')

            # create folder (if needed)
            folder_name, file_name = os.path.split(file_path_app)
            if (folder_name is not None) and (folder_name != ''):
                os.makedirs(folder_name, exist_ok=True)

            # write namelist file
            self.driver_namelist_out = NamelistHandler(file_path_app, file_method='w')
            self.driver_namelist_out.write(variables_namelist)

            # info write namelist (end)
            logger_stream.info(logger_arrow.info(tag='info_lev_1') + 'Write namelist file ... DONE')

        else:

            # info write namelist (end)
            logger_stream.info(logger_arrow.info(tag='info_lev_1') +
                               'Write namelist file ... SKIPPED. File already exists')

        # info algorithm (end)
        logger_stream.info(logger_arrow.info(tag='info_method') + 'Dump "namelist_structure" ... DONE')

    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # method to select namelist structure
    def get_structure_namelist(self, file_template: str = None) -> dict:

        # info algorithm (start)
        logger_stream.info(logger_arrow.info(tag='info_method') + 'Get "namelist_structure" ... ')

        # get namelist file path
        if file_template is None:
            file_template = self.namelist_file['template']
        # check file namelist availability
        if os.path.exists(file_template):

            # info read template file (start)
            logger_stream.info(logger_arrow.info(tag='info_lev_1') + 'Read namelist file ... ')

            # read namelist file
            self.driver_namelist_in = NamelistHandler(file_template, file_method='r')
            # get namelist data (from default file)
            tmp_variables = self.driver_namelist_in.get()
            # update namelist default
            file_variables = self.__update_namelist_default(tmp_variables, self.namelist_type_default)

            # info read template file (end)
            logger_stream.info(logger_arrow.info(tag='info_lev_1') + 'Read namelist file ... DONE')

        else:

            # info read template obj (start)
            logger_stream.info(logger_arrow.info(tag='info_lev_1') + 'Read namelist default ... ')

            # get namelist data (from default obj)
            tmp_variables = deepcopy(self.namelist_structure_default)
            # update namelist default
            file_variables = self.__update_namelist_default(tmp_variables, self.namelist_type_default)

            # info read template obj (end)
            logger_stream.info(logger_arrow.info(tag='info_lev_1') + 'Read namelist default ... DONE')

        # info algorithm (start)
        logger_stream.info(logger_arrow.info(tag='info_method') + 'Get "namelist_structure" ... DONE')

        return file_variables

    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # method to update namelist default
    @staticmethod
    def __update_namelist_default(namelist_variables_default, namelist_type_default):

        # info algorithm (start)
        logger_stream.info(logger_arrow.info(tag='info_upd_nl_main') + 'Update namelist field(s) ... ')

        # iterate over namelist groups
        namelist_variables_update, namelist_variable_mandatory = {}, []
        for file_group, file_fields in namelist_variables_default.items():

            # info check group (start)
            logger_stream.info(logger_arrow.info(tag='info_upd_nl_1') + 'Group "' + file_group + '" ... ')

            # check group fields in the default namelist
            if file_group in list(namelist_type_default.keys()):
                # get default namelist fields
                format_fields = namelist_type_default[file_group]

                # iterate over namelist fields
                namelist_variables_update[file_group] = {}
                for file_key, file_value in file_fields.items():

                    # check key fields in the default namelist
                    if file_key in list(format_fields.keys()):
                        # get default namelist value
                        format_value = format_fields[file_key]

                        # check namelist value and type
                        namelist_variables_update[file_group][file_key] = {}
                        if format_value == 'mandatory':
                            namelist_variables_update[file_group][file_key] = None
                            namelist_variable_tag = file_group + ':' + file_key
                            namelist_variable_mandatory.append(namelist_variable_tag)
                        elif format_value == 'default' or format_value == 'ancillary':
                            namelist_variables_update[file_group][file_key] = file_value
                        else:
                            logger_stream.warning(logger_arrow.warning + 'Namelist format value "' +
                                                  str(format_value) + '" is not expected')

                    else:
                        # warning message
                        logger_stream.warning(logger_arrow.warning + 'Namelist key "' + file_key +
                                              '" is not available in the default namelist')
            else:
                # error message
                logger_stream.error(logger_arrow.error + 'Namelist group "' + file_group +
                                    '" is not available in the default namelist')
                raise TypeError('Group "' + file_group + '" is not defined')
            # info check group (end)
            logger_stream.info(logger_arrow.info(tag='info_upd_nl_1') + 'Group "' + file_group + '" ... DONE')

        # info mandatory variable(s)
        logger_stream.info(logger_arrow.info(tag='info_upd_nl_1') + 'Variable defined by mandatory flag ... ')
        for variable_n, variable_step in enumerate(namelist_variable_mandatory):
            logger_stream.info(logger_arrow.info(tag='info_upd_nl_2') + 'Variable "' + variable_step + '"')
        logger_stream.info(logger_arrow.info(tag='info_upd_nl_1') + 'Variable defined by mandatory flag ... DONE')

        # info algorithm (end)
        logger_stream.info(logger_arrow.info(tag='info_upd_nl_main') + 'Update namelist field(s) ... DONE')

        return namelist_variables_update

    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # method to define namelist variable
    def define_variable_namelist(self, settings_variables: dict) -> dict:

        # info algorithm (start)
        logger_stream.info(logger_arrow.info(tag='info_method') + 'Define "namelist_variables" ... ')

        # check settings variables
        if settings_variables is None or not settings_variables:
            settings_variables = {}
            logger_stream.warning(logger_arrow.warning + 'Settings variables are defined by null dictionary')

        # update settings variables with the time variables
        time_variables = self.time_obj
        for time_key, time_value in time_variables.items():
            if time_key in list(settings_variables.keys()):
                settings_variables[time_key] = time_value

        # get namelist variables
        namelist_variables = self.namelist_fields_by_value

        # fill namelist variables using settings variables
        for settings_key, settings_value in settings_variables.items():

            for namelist_key, namelist_value in namelist_variables.items():

                if isinstance(namelist_value, str):
                    if any(re.findall(r'{|}', namelist_value, re.IGNORECASE)):

                        namelist_tag = replace_string(namelist_value, string_replace={'{': '', '}': ''})

                        if settings_key == namelist_tag:
                            namelist_variables[namelist_key] = settings_value
                        elif settings_key in namelist_tag:
                            if isinstance(namelist_value, str):
                                template_keys = {settings_key: 'string'}
                                template_values = {settings_key: settings_value}
                                namelist_value = fill_tags2string(namelist_value, tags_format=template_keys,
                                                                 tags_filling=template_values)[0]
                                namelist_variables[namelist_key] = namelist_value
                            else:
                                namelist_variables[namelist_key] = namelist_value

                        else:
                            pass
        # update namelist variables using updated variables
        self.namelist_fields_by_value = namelist_variables

        # info algorithm (start)
        logger_stream.info(logger_arrow.info(tag='info_method') + 'Define "namelist_variables" ... DONE')

        return namelist_variables

    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # method to define namelist file
    def define_file_namelist(self, settings_variables: dict,
                             app_path_def: str = None, app_path_force: bool = False) -> dict:

        # info algorithm (start)
        logger_stream.info(logger_arrow.info(tag='info_method') + 'Define "namelist_files" ... ')

        # check settings variables
        if settings_variables is None or not settings_variables:
            settings_variables = {}
            logger_stream.warning(logger_arrow.warning + 'Settings variables are defined by null dictionary')

        # get namelist file
        namelist_file = self.namelist_file

        # fill namelist variables using settings variables
        for settings_key, settings_value in settings_variables.items():

            for namelist_key, namelist_value in namelist_file.items():

                if isinstance(namelist_value, str):
                    if any(re.findall(r'{|}', namelist_value, re.IGNORECASE)):

                        namelist_tag = replace_string(namelist_value, string_replace={'{': '', '}': ''})

                        if settings_key == namelist_tag:
                            namelist_file[namelist_key] = settings_value
                        elif settings_key in namelist_tag:
                            if isinstance(namelist_value, str):
                                template_keys = {settings_key: 'string'}
                                template_values = {settings_key: settings_value}
                                namelist_value = fill_tags2string(namelist_value, tags_format=template_keys,
                                                                 tags_filling=template_values)[0]
                                namelist_file[namelist_key] = namelist_value
                            else:
                                namelist_file[namelist_key] = namelist_value

                        else:
                            pass

        for namelist_key, namelist_path in namelist_file.items():
            if namelist_key == 'application':
                file_tmp, folder_tmp = os.path.basename(namelist_path), os.path.dirname(namelist_path)
                if folder_tmp is None or folder_tmp == '':
                    if app_path_force:
                        namelist_path = os.path.join(app_path_def, file_tmp)
                        namelist_file[namelist_key] = namelist_path

        # update namelist file using updated variables
        self.namelist_file = namelist_file

        # info algorithm (start)
        logger_stream.info(logger_arrow.info(tag='info_method') + 'Define "namelist_files" ... DONE')

        return deepcopy(self.namelist_file)

    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # method to combine namelist variables
    def combine_variable_namelist(self, variables_namelist_default: dict,
                                  variables_namelist_by_value: dict) -> (dict, dict):

        # info algorithm (start)
        logger_stream.info(logger_arrow.info(tag='info_method') + 'Combine "namelist_variables" ... ')

        # get namelist default type(s)
        type_namelist_filled = self.namelist_type_default
        # get autocomplete fields and flags
        namelist_variables_by_pattern = self.namelist_fields_by_pattern
        # define namelist collections
        variables_namelist_collections = deepcopy(variables_namelist_by_value)

        # iterate over namelist groups
        variables_namelist_defined, variables_namelist_checked = {}, {}
        for nml_group, nml_fields in variables_namelist_default.items():

            # info check group (start)
            logger_stream.info(logger_arrow.info(tag='info_lev_1') + 'Group "' + nml_group + '" ... ')

            # check group fields in the default namelist
            if nml_group in list(type_namelist_filled.keys()):
                # get default namelist fields
                format_fields = type_namelist_filled[nml_group]

                # iterate over namelist fields
                variables_namelist_defined[nml_group], variables_namelist_checked[nml_group] = {}, {}
                for nml_key, nml_value in nml_fields.items():

                    # check key fields in the default namelist
                    if nml_key in list(format_fields.keys()):
                        # get default namelist value
                        nml_format = format_fields[nml_key]

                        # get data from namelist by value
                        defined_value = None
                        if nml_key in list(variables_namelist_by_value.keys()):
                            defined_value = variables_namelist_by_value[nml_key]
                        if defined_value is not None:
                            nml_value = defined_value

                        # check namelist value and type
                        variables_namelist_defined[nml_group][nml_key] = {}
                        variables_namelist_checked[nml_group][nml_key] = {}
                        if nml_format == 'mandatory':
                            if nml_value is None:

                                auto_value = None
                                for pat_key, pat_fields in namelist_variables_by_pattern.items():

                                    pat_active = pat_fields['active']
                                    pat_tmpl, pat_value = pat_fields['template'], pat_fields['value']

                                    if pat_tmpl in nml_key:
                                        if pat_active:
                                            auto_value = deepcopy(pat_value)
                                            logger_stream.warning(
                                                logger_arrow.warning + 'Namelist variable "' +
                                                nml_key + '" auto completed by value "' + str(auto_value) + '"')

                                if auto_value is None:
                                    logger_stream.error(logger_arrow.error + 'Namelist key "' + nml_key +
                                                        '" is not defined')
                                    raise TypeError('Field mandatory value "' + nml_key +
                                                    '" is not defined or defined by NoneType')
                                variables_namelist_collections[nml_key] = auto_value
                                variables_namelist_defined[nml_group][nml_key] = auto_value
                            else:
                                variables_namelist_checked[nml_group][nml_key] = True
                                variables_namelist_defined[nml_group][nml_key] = nml_value
                                variables_namelist_collections[nml_key] = nml_value
                        elif nml_format == 'default' or nml_format == 'ancillary':
                            variables_namelist_checked[nml_group][nml_key] = True
                            variables_namelist_defined[nml_group][nml_key] = nml_value
                            variables_namelist_collections[nml_key] = nml_value
                        else:
                            logger_stream.warning(logger_arrow.warning + 'Namelist format value "' +
                                                  nml_format + '" is not expected')

                    else:
                        # warning message
                        logger_stream.warning(logger_arrow.warning + 'Namelist key "' +
                                              nml_key + '" is not available in the default namelist')

            else:
                # error message
                logger_stream.error(logger_arrow.error + 'Namelist group "' +
                                    nml_group + '" is not available in the default namelist')
                raise TypeError('Group "' + nml_group + '" is not defined')

            # info check group (end)
            logger_stream.info(logger_arrow.info(tag='info_lev_1') + 'Group "' + nml_group + '" ... DONE')

        # info algorithm (end)
        logger_stream.info(logger_arrow.info(tag='info_method') + 'Combine "namelist_variables" ... DONE')

        return variables_namelist_defined, variables_namelist_checked, variables_namelist_collections

    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # method to organize namelist variable(s)
    @staticmethod
    def organize_variable_namelist(namelist_obj: dict, collector_obj: dict = None) -> dict:

        # info algorithm (start)
        logger_stream.info(logger_arrow.info(tag='info_method') + 'Organize "namelist_variables" ... ')

        if collector_obj is None:
            collector_obj = deepcopy(namelist_obj)

        # get settings variables
        common_obj = {}
        for var_key, var_value in namelist_obj.items():
            if var_key in list(collector_obj.keys()):
                logger_stream.warning(logger_arrow.warning + 'Variable "' + var_key +
                                      '" found in the collector object. Variable will be overwritten')
                common_obj[var_key] = collector_obj[var_key]
            else:
                common_obj[var_key] = var_value

        # collect singleton data object
        collector_data.collect(common_obj)

        # info algorithm (start)
        logger_stream.info(logger_arrow.info(tag='info_method') + 'Organize "namelist_variables" ... DONE')

        return common_obj

    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # method to view namelist variable(s)
    def view_variable_namelist(self, data: dict = None, mode: bool = True) -> None:
        # info algorithm (start)
        logger_stream.info(logger_arrow.info(tag='info_method') + 'View "namelist_variables" ... ')
        if mode:
            self.driver_namelist_out.view(data)
        # info algorithm (start)
        logger_stream.info(logger_arrow.info(tag='info_method') + 'View "namelist_variables" ... DONE')
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # method to return error in selecting namelist variable(s)
    def error_variable_namelist(self):
        logger_stream.error(' ===> Namelist type is not available')
        raise RuntimeError('Namelist type must be expected in the version dictionary to correctly set the variables')
    # ------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
