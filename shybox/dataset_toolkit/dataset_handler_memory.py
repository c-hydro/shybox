"""
Class Features

Name:          dataset_handler_memory
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20260929'
Version:       '2.1.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
from copy import deepcopy
from typing import Any, Dict, Optional, Union, Sequence

import xarray as xr
import pandas as pd

from shybox.dataset_toolkit.dataset_handler_base import Dataset
from shybox.logging_toolkit.logging_handler import LoggingManager
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# class to handle memory dataset
class DataMemory(Dataset):

    type = 'memory_dataset'

    def __init__(
            self,
            data: Any = None,
            key: Optional[Union[str, Sequence[str]]] = None,
            file_variable: Optional[Union[str, Sequence[str]]] = None,
            file_workflow: Optional[Union[str, Sequence[str]]] = None,
            file_io: str = 'memory',
            data_role: str = 'data',
            data_layout: str = 'geo',
            variable_template: Optional[dict] = None,
            keep_after_reading: bool = True,
            copy_on_read: bool = False,
            logger: Optional[LoggingManager] = None,
            message: bool = True,
            **kwargs):

        # -------------------------------------------------------------------------------------
        # define logger
        self.logger = logger or LoggingManager(name='DataMemory')

        # -------------------------------------------------------------------------------------
        # define memory behaviour
        self.keep_after_reading = keep_after_reading
        self.copy_on_read = copy_on_read

        # actual memory storage
        self.data_dict: Dict[str, Any] = {}

        # define file io type (to simulate the behaviour of local data)
        self.file_io = file_io

        # -------------------------------------------------------------------------------------
        # normalize input data
        if isinstance(data, dict):
            data_dict = data.copy()

        elif data is None:
            data_dict = {}

        else:
            # single object
            data_dict = {'data': data}

        # -------------------------------------------------------------------------------------
        # normalize file variables
        file_variable = self._normalize_file_variables(
            file_variable=file_variable,
            data_dict=data_dict,
        )

        # -------------------------------------------------------------------------------------
        # normalize memory keys
        memory_keys = self._normalize_memory_keys(
            key=key,
            file_variable=file_variable,
            data_dict=data_dict,
        )

        # -------------------------------------------------------------------------------------
        # reorganize memory dictionary
        #
        # input dictionary keys are not necessarily the final memory keys.
        # Values are associated by insertion order.
        if data_dict:

            data_values = list(data_dict.values())

            self.data_dict = {
                memory_key: data_value
                for memory_key, data_value in zip(memory_keys, data_values)
            }

        # default key
        self.memory_key = (
            memory_keys[0]
            if len(memory_keys) == 1
            else memory_keys
        )

        # -------------------------------------------------------------------------------------
        # normalize workflow
        file_workflow = self._normalize_file_workflow(
            file_workflow=file_workflow,
            file_variable=file_variable,
        )

        # -------------------------------------------------------------------------------------
        # define variable template
        variable_template = variable_template or {}

        if 'vars_data' not in variable_template:
            variable_template['vars_data'] = {
                variable: variable
                for variable in file_variable
            }

        # -------------------------------------------------------------------------------------
        # dataset logical information
        #
        # use a logical memory identifier instead of a physical file path
        loc_pattern = (
            memory_keys[0]
            if len(memory_keys) == 1
            else 'memory'
        )

        file_name = loc_pattern

        kwargs.update({
            'loc_pattern': loc_pattern,
            'file_name': file_name,

            'file_format': 'memory',
            'file_mode': 'memory',
            'file_type': self.type,
            'file_io': 'memory',

            'file_variable': (
                file_variable[0]
                if len(file_variable) == 1
                else file_variable
            ),

            'file_workflow': file_workflow,

            'data_role': data_role,
            'data_layout': data_layout,

            'variable_template': variable_template,

            'logger': self.logger,
            'message': message,
        })

        # -------------------------------------------------------------------------------------
        # initialize base class
        super().__init__(**kwargs)

        # DataMemory is already a memory backend
        self.memory_active = False
        self.memory_data = None

        # update readability
        self.readable_settings(active_warnings=False)

    # ------------------------------------------------------------------------------------------------------------------
    # NORMALIZATION METHODS

    @staticmethod
    def _normalize_file_variables(
            file_variable,
            data_dict: Dict[str, Any]) -> list:

        n_data = len(data_dict)

        # no data available yet
        if n_data == 0:

            if file_variable is None:
                return ['data']

            if isinstance(file_variable, str):
                return [file_variable]

            if isinstance(file_variable, (list, tuple)):
                return list(file_variable)

            raise TypeError(
                'file_variable must be a string, list or tuple.'
            )

        # -------------------------------------------------------------------------------------
        # infer from dictionary keys
        if file_variable is None:

            return list(data_dict.keys())

        # -------------------------------------------------------------------------------------
        # string
        if isinstance(file_variable, str):

            file_variable = [file_variable]

        # -------------------------------------------------------------------------------------
        # list / tuple
        elif isinstance(file_variable, (list, tuple)):

            file_variable = list(file_variable)

        else:

            raise TypeError(
                'file_variable must be a string, list or tuple.'
            )

        # -------------------------------------------------------------------------------------
        # check number of variables
        if len(file_variable) != n_data:

            raise ValueError(
                f'file_variable has {len(file_variable)} elements, '
                f'but data contains {n_data} elements.'
            )

        return file_variable

    @staticmethod
    def _normalize_memory_keys(
            key,
            file_variable: list,
            data_dict: Dict[str, Any]) -> list:

        n_data = len(data_dict)

        # no data yet
        if n_data == 0:

            if key is None:
                return list(file_variable)

            if isinstance(key, str):
                return [key]

            if isinstance(key, (list, tuple)):
                return list(key)

            raise TypeError(
                'key must be a string, list or tuple.'
            )

        # -------------------------------------------------------------------------------------
        # default keys = file variables
        if key is None:

            return list(file_variable)

        # -------------------------------------------------------------------------------------
        # string
        if isinstance(key, str):

            key = [key]

        elif isinstance(key, (list, tuple)):

            key = list(key)

        else:

            raise TypeError(
                'key must be a string, list or tuple.'
            )

        # -------------------------------------------------------------------------------------
        # check key length
        if len(key) != n_data:

            raise ValueError(
                f'key has {len(key)} elements, '
                f'but data contains {n_data} elements.'
            )

        return key

    @staticmethod
    def _normalize_file_workflow(
            file_workflow,
            file_variable: list):

        n_variable = len(file_variable)

        if file_workflow is None:

            file_workflow = ['memory'] * n_variable

        elif isinstance(file_workflow, str):

            file_workflow = [file_workflow] * n_variable

        elif isinstance(file_workflow, (list, tuple)):

            file_workflow = list(file_workflow)

            if len(file_workflow) != n_variable:

                raise ValueError(
                    f'file_workflow has {len(file_workflow)} elements, '
                    f'but file_variable contains {n_variable} elements.'
                )

        else:

            raise TypeError(
                'file_workflow must be a string, list or tuple.'
            )

        if len(file_workflow) == 1:
            return file_workflow[0]

        return file_workflow

    # ------------------------------------------------------------------------------------------------------------------
    # LOCATION MANAGEMENT

    @property
    def loc_pattern(self):
        return self._loc_pattern

    @loc_pattern.setter
    def loc_pattern(self, value):
        self._loc_pattern = value

    # ------------------------------------------------------------------------------------------------------------------
    # READABILITY MANAGEMENT

    def readable_settings(
            self,
            *,
            active_warnings: bool = True,
            **kwargs) -> bool:

        self.warnings = []

        if self.data_dict:

            self.readable = True
            self.status = 'ok'

        else:

            self.readable = False
            self.status = 'nio'

            warning_message = (
                'Memory dataset does not contain data.'
            )

            self.warnings.append(warning_message)

            if active_warnings:
                self.logger.warning(warning_message)

        return self.readable

    def is_readable(self):

        self.readable_settings()

        return self.status, self.readable

    # ------------------------------------------------------------------------------------------------------------------
    # KEY MANAGEMENT

    def get_key(
            self,
            time=None,
            variable: Optional[str] = None,
            key: Optional[str] = None,
            **kwargs):

        # explicit key
        if key is not None:
            return key

        # variable identifies the memory object
        if variable is not None:
            return variable

        # single memory key
        if isinstance(self.memory_key, str):
            return self.memory_key

        # multiple variables require explicit selection
        if len(self.memory_key) == 1:
            return self.memory_key[0]

        raise ValueError(
            'DataMemory contains multiple variables. '
            'Specify "variable" or "key".'
        )

    # ------------------------------------------------------------------------------------------------------------------
    # INPUT / OUTPUT METHODS

    def _read_data(
            self,
            input_key: str,
            **kwargs):

        if input_key not in self.data_dict:

            raise KeyError(
                f'Memory key "{input_key}" is not available. '
                f'Available keys: {list(self.data_dict.keys())}'
            )

        if self.keep_after_reading:

            data = self.data_dict[input_key]

        else:

            data = self.data_dict.pop(input_key)

        if self.copy_on_read:
            data = deepcopy(data)

        if not self.keep_after_reading:
            self.readable_settings(active_warnings=False)

        return data

    def _write_data(
            self,
            output: Any,
            output_key: str,
            **kwargs):

        self.data_dict[output_key] = output

        self.readable = True
        self.status = 'ok'

    def _rm_data(
            self,
            key: str):

        if key in self.data_dict:
            self.data_dict.pop(key)

        self.readable_settings(
            active_warnings=False
        )

    def _check_data(
            self,
            data_key: str) -> bool:

        return data_key in self.data_dict

    def _walk(
            self,
            prefix: Optional[str] = None):

        if prefix is None:

            yield from self.data_dict.keys()
            return

        for key in self.data_dict.keys():

            if key.startswith(prefix):
                yield key

    # ------------------------------------------------------------------------------------------------------------------
    # DATA CREATION

    def _create_data(
            self,
            input_obj=None,
            **kwargs):

        return input_obj

    # ------------------------------------------------------------------------------------------------------------------
    # PUBLIC METHODS

    def get_data(
            self,
            time=None,
            name: Optional[str] = None,
            variable: Optional[str] = None,
            key: Optional[str] = None,
            as_is: bool = False,
            **kwargs):

        # if no selector and multiple variables,
        # return the complete memory dictionary
        if (
                variable is None and
                key is None and
                len(self.data_dict) > 1
        ):

            if self.copy_on_read:
                return deepcopy(self.data_dict)

            return self.data_dict

        # define requested key
        data_key = self.get_key(
            time=time,
            variable=variable,
            key=key,
            **kwargs
        )

        return self._read_data(
            data_key
        )

    def write_data(
            self,
            data,
            time=None,
            variable: Optional[Union[str, Sequence[str]]] = None,
            key: Optional[Union[str, Sequence[str]]] = None,
            **kwargs):

        # -------------------------------------------------------------------------------------
        # dictionary case
        if isinstance(data, dict):

            data_values = list(data.values())
            n_data = len(data_values)

            # normalize variables
            if variable is None:
                variables = list(data.keys())

            elif isinstance(variable, str):
                variables = [variable]

            elif isinstance(variable, (list, tuple)):
                variables = list(variable)

            else:
                raise TypeError(
                    'variable must be a string, list or tuple.'
                )

            if len(variables) != n_data:
                raise ValueError(
                    f'variable has {len(variables)} elements, '
                    f'but data contains {n_data} elements.'
                )

            # normalize keys
            if key is None:
                keys = variables

            elif isinstance(key, str):
                keys = [key]

            elif isinstance(key, (list, tuple)):
                keys = list(key)

            else:
                raise TypeError(
                    'key must be a string, list or tuple.'
                )

            if len(keys) != n_data:
                raise ValueError(
                    f'key has {len(keys)} elements, '
                    f'but data contains {n_data} elements.'
                )

            # write all data
            for output_key, output_data in zip(
                    keys,
                    data_values):

                self._write_data(
                    output_data,
                    output_key,
                    **kwargs
                )

            return

        # -------------------------------------------------------------------------------------
        # single object
        if isinstance(variable, (list, tuple)):

            if len(variable) != 1:
                raise ValueError(
                    'Multiple variables cannot be used '
                    'with a single data object.'
                )

            variable = variable[0]

        if isinstance(key, (list, tuple)):

            if len(key) != 1:
                raise ValueError(
                    'Multiple keys cannot be used '
                    'with a single data object.'
                )

            key = key[0]

        output_key = self.get_key(
            time=time,
            variable=variable,
            key=key,
            **kwargs
        )

        self._write_data(
            data,
            output_key,
            **kwargs
        )

    def set_data(
            self,
            data,
            key: Optional[Union[str, Sequence[str]]] = None):

        # dictionary
        if isinstance(data, dict):

            if key is None:
                keys = list(data.keys())

            elif isinstance(key, str):
                keys = [key]

            else:
                keys = list(key)

            if len(keys) != len(data):

                raise ValueError(
                    f'key has {len(keys)} elements, '
                    f'but data contains {len(data)} elements.'
                )

            for this_key, this_data in zip(
                    keys,
                    data.values()):

                self._write_data(
                    this_data,
                    this_key
                )

            return

        # single object
        if key is None:

            if isinstance(self.memory_key, list):

                if len(self.memory_key) != 1:
                    raise ValueError(
                        'DataMemory contains multiple keys. '
                        'Specify the target key.'
                    )

                key = self.memory_key[0]

            else:

                key = self.memory_key

        self._write_data(
            data,
            key
        )

    def get(
            self,
            key: Optional[str] = None,
            default=None):

        if key is None:

            if len(self.data_dict) == 1:
                key = next(iter(self.data_dict))

            else:
                return self.data_dict

        return self.data_dict.get(
            key,
            default
        )

    def get_all(
            self,
            copy: bool = False):

        if copy:
            return deepcopy(self.data_dict)

        return self.data_dict

    @property
    def keys(self):

        return list(self.data_dict.keys())

    def __len__(self):

        return len(self.data_dict)

    def __contains__(
            self,
            key):

        return key in self.data_dict

    def clear(self):

        self.data_dict.clear()

        self.readable_settings(
            active_warnings=False
        )

    # ------------------------------------------------------------------------------------------------------------------
    # FACTORY

    @classmethod
    def from_dict(
            cls,
            data: Dict[str, Any],
            file_variable: Optional[Union[str, Sequence[str]]] = None,
            **kwargs):

        if not isinstance(data, dict):

            raise TypeError(
                f'Expected dictionary, found '
                f'"{type(data).__name__}".'
            )

        return cls(
            data=data,
            file_variable=file_variable,
            **kwargs
        )

# ----------------------------------------------------------------------------------------------------------------------