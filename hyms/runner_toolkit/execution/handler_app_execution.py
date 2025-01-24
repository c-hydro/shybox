"""
Class Features

Name:          handler_app_execution
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20250116'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import os

import pandas as pd

from multiprocessing import Pool, cpu_count
from tabulate import tabulate


from hyms.generic_toolkit.lib_utils_process import exec_process
from hyms.generic_toolkit.lib_utils_dict import  flat_dict_key

from hyms.generic_toolkit.lib_default_args import logger_name, logger_arrow
from hyms.generic_toolkit.lib_default_args import collector_data

# logging
logger_stream = logging.getLogger(logger_name)
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# class execution handler
class ExecutionHandler:

    class_type = 'execution_handler'
    std_error_valid_flag = ['IEEE_INVALID_FLAG', 'IEEE_OVERFLOW_FLAG', 'IEEE_UNDERFLOW_FLAG']

    # initialize class
    def __init__(self, exec_app: str, exec_args: str = None,
                 exec_tag: str = 'default_tag', exec_mode: str = 'default_mode',
                 exec_deps: list = None, **kwargs) -> None:

        self.exec_app = exec_app
        self.exec_args = exec_args
        self.exec_tag, self.exec_mode = exec_tag, exec_mode

        self.exec_deps = self.configure_process_deps(exec_deps)
        self.exec_cmd = self.configure_process_cmd(exec_app=exec_app, exec_args=exec_args)

        self.exec_settings = None

    @classmethod
    def exec_with_multiple_args(
            cls, exec_app: str, exec_args: list = None, sep_args: str = ' ',
            exec_tag: str = 'default', exec_mode: str = 'default',
            exec_deps: list = None, **kwargs):

        if isinstance(exec_args, list):
            exec_args = sep_args.join(exec_args)

        return cls(exec_app=exec_app, exec_args=exec_args,
                   exec_tag=exec_tag, exec_mode=exec_mode, exec_deps=exec_deps)

    def configure_process_deps(self, exec_deps : list = None) -> list:

        if exec_deps is None:
            exec_deps = self.exec_deps

        if exec_deps is not None:
            if not isinstance(exec_deps, list):
                exec_deps = [exec_deps]

            exec_deps = list(set(exec_deps))
            for exec_path in exec_deps:
                if isinstance(exec_path, str):
                    os.environ['LD_LIBRARY_PATH'] = 'LD_LIBRARY_PATH:' + str(exec_path)
                else:
                    logger_stream.error(logger_arrow.error + ' Execution dependency path is wrong defined')
                    raise RuntimeError('Execution dependency path is not defined')

        self.exec_deps = exec_deps

        return self.exec_deps

    def configure_process_cmd(self, exec_app: str = None, exec_args: str = None) -> str:
        if exec_app is None:
            exec_app = self.exec_app
        if exec_args is None:
            exec_args = self.exec_args
        exec_cmd = exec_app + ' ' + exec_args
        return exec_cmd

    # method to run application in base mode
    def worker_base(self, exec_cmd: str = None) -> list:
        if exec_cmd is None:
            exec_cmd = self.exec_cmd
        else:
            self.exec_cmd = exec_cmd
        exec_response = exec_process(exec_cmd)
        return exec_response

    # method to run application in sequential mode
    def worker_seq(self, exec_cmd: list = None) -> list:
        exec_response = []
        for step_cmd in exec_cmd:
            step_response = self.worker_base(exec_cmd=step_cmd)
            exec_response.append(step_response)
        return exec_response

    # method to run application in multiprocess mode
    def worker_mp(self, exec_cmd: list, process_n: int = 2):

        list_cmd = []
        for run_key, step_cmd in self.command_line_info.items():
            list_cmd.append(step_cmd)

        logger_stream.info(logger_arrow.info(tag='worker_main_mp') + 'Pool requests ... ')
        list_response = []
        if list_cmd:
            with Pool(processes=process_n, maxtasksperchild=1) as process_pool:
                exec_response = process_pool.map(exec_process, list_cmd, chunksize=1)
                process_pool.close()
                process_pool.join()
                list_response.append(exec_response)
            logger_stream.info(logger_arrow.info(tag='worker_main_mp') + 'Pool requests ... DONE')
        else:
            logger_stream.info(logger_arrow.info(tag='worker_main_mp') + 'Pool requests ... SKIPPED. PREVIOUSLY DONE')

        return list_response

    # method to freeze data
    def freeze(self, other_settings: dict = None) -> dict:

        self.exec_settings = {
            'exec_cmd': self.exec_cmd,
            'exec_app': self.exec_app, 'exec_args': self.exec_args,
            'exec_tag': self.exec_tag, 'exec_mode': self.exec_mode,
            'exec_deps': self.exec_deps}

        if other_settings is not None:
            self.exec_settings = {**self.exec_settings, **other_settings}

        return self.exec_settings

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
            table_data = self.exec_settings

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
        Check if data is available.
        """
        raise NotImplementedError
# ----------------------------------------------------------------------------------------------------------------------
