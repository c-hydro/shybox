"""
Class Features

Name:          drv_app_execution
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20250115'
Version:       '4.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import os
import re

from hyms.generic_toolkit.lib_utils_dict import create_dict_from_list
from hyms.generic_toolkit.lib_utils_string import replace_string, fill_tags2string, convert_bytes2string
from hyms.generic_toolkit.lib_utils_file import (split_file_path, join_file_path,
                                                 sanitize_file_path, expand_file_path, copy_file)
from hyms.generic_toolkit.lib_utils_time import convert_time_frequency
from hyms.generic_toolkit.lib_utils_debug import read_workspace_obj, write_workspace_obj
from hyms.generic_toolkit.lib_default_args import logger_name, logger_arrow

from hyms.runner_toolkit.execution.handler_app_execution import ExecutionHandler

# logging
logger_stream = logging.getLogger(logger_name)

# debugging
# import matplotlib.pylab as plt
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# class to configure execution
class DrvExec:

    # ------------------------------------------------------------------------------------------------------------------
    # global variable(s)
    class_type = 'execution_driver'

    execution_std_error_valid_flag = ['IEEE_INVALID_FLAG', 'IEEE_OVERFLOW_FLAG', 'IEEE_UNDERFLOW_FLAG']
    execution_handler = None
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # Method class initialization
    def __init__(self, execution_obj : dict, time_obj : dict = None, settings_obj : dict = None,
                 execution_name: str = 'default', execution_mode: str = 'default',
                 execution_update: bool = True, **kwargs) -> None:

        # define object(s)
        self.execution_obj = execution_obj
        self.time_obj = time_obj
        self.settings_obj = settings_obj

        # get execution name and mode
        if 'execution_name' in list(self.execution_obj['description'].keys()):
            self.execution_name = self.execution_obj['description']['execution_name']
        else:
            self.execution_name = execution_name
        if 'execution_mode' in list(self.execution_obj['description'].keys()):
            self.execution_mode = self.execution_obj['description']['execution_mode']
        else:
            self.execution_mode = execution_mode

        if 'executable' in list(self.execution_obj.keys()):
            self.execution_args = self.execution_obj['executable']['arguments']
            self.execution_location = self.execution_obj['executable']['location']
        else:
            logger_stream.error(logger_arrow.error + 'Executable information is not available')
            raise RuntimeError('Executable information must be defined')

        self.file_exec, self.folder_exec = split_file_path(self.execution_location)
        self.path_exec = join_file_path(folder_name=self.folder_exec, file_name=self.file_exec)

        if 'library' in list(self.execution_obj.keys()):
            self.library_location = self.execution_obj['library']['location']
            self.library_deps = self.execution_obj['library']['dependencies']
        else:
            logger_stream.error(logger_arrow.error + 'Library information is not available')
            raise RuntimeError('Library information must be defined')

        self.file_libs, self.folder_libs = split_file_path(self.library_location)
        self.path_libs = join_file_path(folder_name=self.folder_libs, file_name=self.file_libs)

        if 'info' in list(self.execution_obj.keys()):
            self.info_location = self.execution_obj['info']['location']
        else:
            self.info_location = os.path.join(self.folder_exec, 'execution.info')

        self.file_info, self.folder_info = split_file_path(self.info_location)
        self.path_info = join_file_path(folder_name=self.folder_info, file_name=self.file_info)

        # get namelist variables
        self.execution_file = {'executable' : self.path_exec, 'library': self.path_libs, 'info': self.path_info}
        self.execution_libs = create_dict_from_list(default_key='deps_{:}', list_values=self.library_deps)
        self.execution_collections = {**self.execution_file, **self.execution_libs}

        self.time_frequency = convert_time_frequency(
            self.time_obj['time_frequency'], frequency_conversion='str_to_int')

        self.execution_update = execution_update
        self.execution_options = self.__set_process_options(settings_variables=self.settings_obj)

    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # method to set process options
    def __set_process_options(self, settings_variables: dict = None) -> dict:

        # check settings variables
        if settings_variables is None or not settings_variables:
            settings_variables = {}
            logger_stream.warning(logger_arrow.warning + 'Settings variables are defined by null dictionary')

        # get execution file
        exec_collections = self.execution_collections

        # fill namelist variables using settings variables
        for settings_key, settings_value in settings_variables.items():

            for exec_key, exec_value in exec_collections.items():

                if isinstance(exec_value, str):
                    if any(re.findall(r'{|}', exec_value, re.IGNORECASE)):

                        exec_tag = replace_string(exec_value, string_replace={'{': '', '}': ''})

                        if settings_key == exec_tag:
                            exec_collections[exec_key] = settings_value
                        elif settings_key in exec_tag:
                            if isinstance(exec_value, str):
                                template_keys = {settings_key: 'string'}
                                template_values = {settings_key: settings_value}
                                exec_value = fill_tags2string(
                                    exec_value, tags_format=template_keys, tags_filling=template_values)[0]
                                exec_collections[exec_key] = exec_value
                            else:
                                exec_collections[exec_key] = exec_value

                        else:
                            pass

        # expand and sanitize file path (if needed)
        for exec_key, exec_path in exec_collections.items():
            tmp_path = expand_file_path(exec_path)
            exec_collections[exec_key] = sanitize_file_path(tmp_path)

        return exec_collections

    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # method to configure process job
    def configure_process_job(self, file_exec: str = None, file_library: str = None, file_deps: list = None) -> bool:

        # info algorithm (start)
        logger_stream.info(logger_arrow.info(tag='process_main') + 'Set "process_job" ... ')

        # get executable and library file
        if 'executable' in list(self.execution_options.keys()):
            file_exec = self.execution_options['executable']
        if file_exec is None:
            logger_stream.error(logger_arrow.error + 'Process executable file is not available')
            raise RuntimeError('Process executable file must be defined')
        if 'library' in list(self.execution_options.keys()):
            file_library = self.execution_options['library']
        if file_library is None:
            logger_stream.error(logger_arrow.error + 'Process library file is not available')
            raise RuntimeError('Process library file must be defined')

        # manage of executable folder
        folder_exec = split_file_path(file_exec)[1]
        if self.execution_update:
            if os.path.exists(file_exec):
                os.remove(file_exec)
            #if os.path.exists(folder_exec): # TODO: check if it is necessary
            #    shutil.rmtree(folder_exec, ignore_errors=True)
        if not os.path.exists(folder_exec):
            os.makedirs(folder_exec, exist_ok=True)

        # manage of executable file
        try:
            if not os.path.exists(file_exec):
                if os.path.exists(file_library):
                    copy_file(file_library, file_exec)
                else:
                    logger_stream.error(logger_arrow.error + 'Process library file ' + file_library + ' not found')
                    raise RuntimeError('Process library file is unavailable. Exit.')

        except RuntimeError as run_error:
            logger_stream.error(logger_arrow.error + 'Process executable is not set! ' + (str(run_error)))
            raise RuntimeError('Process executable is corrupted. Exit.')

        # info algorithm (end)
        logger_stream.info(logger_arrow.info(tag='process_main') + 'Set "process_job" ... DONE')

        return True
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # method to execute process
    def execute_process_job(self, exec_cmd: str = None, exec_update: bool = True,
                            exec_auxiliary: str = None, tag_auxiliary: str = 'exec_process') -> (str, str, int):

        # algorithm information (start)
        logger_stream.info(logger_arrow.info(tag='process_main') + 'Execute "process_job" ... ')

        if exec_cmd is None:
            exec_cmd = self.path_exec
        if exec_update is None:
            exec_update = self.execution_update
        if exec_auxiliary is None:
            exec_auxiliary = self.info_location

        if exec_update:
            if os.path.exists(exec_auxiliary):
                os.remove(exec_auxiliary)

        if not os.path.exists(exec_auxiliary):

            if isinstance(self.execution_args, str):
                self.exec_handler = ExecutionHandler(
                    exec_app=exec_cmd, exec_args=self.execution_args,
                    exec_tag=self.execution_name, exec_mode=self.execution_mode,
                    exec_deps=self.library_deps)
            elif isinstance(self.execution_args, list):
                self.exec_handler = ExecutionHandler.exec_with_multiple_args(
                    exec_app=exec_cmd, exec_args=self.execution_args,
                    exec_tag=self.execution_name, exec_mode=self.execution_mode,
                    exec_deps=self.library_deps)
            else:
                logger_stream.error(logger_arrow.error + 'Executable arguments format is not expected')
                raise RuntimeError('Executable arguments format must be a string or a list')

            # Execution_response
            # example of:
            # execution_response = [[None, None, 0], [None, None, 0], [None, None, 0]]
            # execution_response = [[None, b'STOP Stopped\n', 0]]
            exec_response = self.exec_handler.worker_base()

            if exec_response[0] is not None:
                exec_response[0] = str(exec_response[0])
            if exec_response[1] is not None:
                exec_response[1] = convert_bytes2string(exec_response[1])
            if exec_response[2] is not None:
                exec_response[2] = str(exec_response[2])

            # Check of the standard stream(s) for each run
            logger_stream.info(logger_arrow.info(tag='exec_process_sub_anls_run') +
                               'Analyze run "' + self.execution_name + '" ... ')

            # print standard output and error
            logger_stream.info(logger_arrow.info(tag='exec_process_sub_type_std') +
                               'StdOut: ' + str(exec_response[0]))
            logger_stream.info(logger_arrow.info(tag='exec_process_sub_type_std') +
                               'StdErr: ' + str(exec_response[1]))

            # Check for StdErr valid flag(s)
            if exec_response[1] is not None:
                logger_stream.info(logger_arrow.info(tag='exec_process_sub_check_std') +
                                   'Check StdErr for finding error derived by valid StdErr flags ... ')
                for valid_flag_step in self.execution_std_error_valid_flag:
                    logger_stream.info(
                        logger_arrow.info(tag='exec_process_sub_check_flag') +
                        'Control the ' + valid_flag_step + ' return code in the StdErr ... ')
                    check_response = exec_response[1].find(valid_flag_step)
                    if check_response >= 0:
                        exec_response[1] = None
                        logger_stream.info(
                            logger_arrow.info(tag='exec_process_sub_check_flag') +
                            'Control the ' + valid_flag_step + ' return code in the StdErr ... FOUND;'
                                                               ' StdError for run is set to "None"')
                        break
                    else:
                        logger_stream.info(
                            logger_arrow.info(tag='exec_process_sub_check_flag') + 'Control the ' + valid_flag_step +
                            ' return code in the StdErr ... NOT FOUND')

                logger_stream.info(logger_arrow.info(tag='exec_process_sub_check_std') +
                                   'Check StdErr for finding error derived by valid StdErr flags ... DONE')
            # print standard exit
            logger_stream.info(logger_arrow.info(tag='exec_process_sub_type_std') +
                               'StdExit: ' + str(exec_response[2]))

            # Check of standard error messages
            if exec_response[1] is not None:
                logger_stream.info(logger_arrow.info(tag='exec_process_sub_anls_run') +
                                   'Analyze run "' + self.execution_name + '" ... FAILED')
                logger_stream.info(' #### Configure execution ... FAILED')
                logger_stream.error(logger_arrow.error + 'Execution failed with "' + str(exec_response[1]) + '" message.')
                raise RuntimeError('Error found in running application. Check your settings and datasets!')
            else:
                logger_stream.info(logger_arrow.info(tag='exec_process_sub_anls_run') +
                                   'Analyze run "' + self.execution_name + '" ... DONE')

            # organize execution info
            execution_other = {'exec_time' : self.time_obj, 'exec_response': exec_response}
            # Freeze execution(s) info
            execution_info = self.execution_handler.freeze(other_settings=execution_other)

            # dump execution info
            folder_auxiliary, file_auxiliary = os.path.split(exec_auxiliary)
            os.makedirs(folder_auxiliary, exist_ok=True)

            write_workspace_obj(exec_auxiliary, execution_info)

            # algorithm information (end - done)
            logger_stream.info(logger_arrow.info(tag='process_main') + 'Execute "process_job" ... DONE')

        elif os.path.exists(exec_auxiliary):

            # collect execution info
            execution_info = read_workspace_obj(exec_auxiliary)

            # algorithm information (end - skipped )
            logger_stream.info(logger_arrow.info(tag='process_main') +
                               'Execute "process_job" ... SKIPPED. Process information stored in ' +
                               exec_auxiliary + '"')

        else:
            # algorithm information (end - error)
            logger_stream.info(logger_arrow.info(tag='process_main') + 'Execute "process_job" ... FAILED.')
            logger_stream.error(logger_arrow.error + 'Process execution or information is/are not correctly set.')
            raise RuntimeError('Bad definition of ancillary dynamic file')

        return execution_info

    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # method to view settings variable(s)
    def view_process_options(self, data: dict = None, mode: bool = True) -> None:
        # info algorithm (start)
        logger_stream.info(logger_arrow.info(tag='info_method') + 'View "namelist_variables" ... ')
        if mode:
            self.driver_namelist_out.view(data)
        # info algorithm (start)
        logger_stream.info(logger_arrow.info(tag='info_method') + 'View "namelist_variables" ... ')
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # method to return error in selecting variables templates
    def error_variable_information(self):
        logger_stream.error(logger_arrow.error + 'Information type is not available')
        raise RuntimeError('Namelist type must be expected in the version dictionary to correctly set the variables')
    # ------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
