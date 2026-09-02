"""
Library Features:

Name:          lib_orchestrator_utils_processes_upd
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20260831'
Version:       '1.0.0'
"""
# ----------------------------------------------------------------------------------------------------------------------
# libraries
import functools
import warnings
import inspect

from typing import Union, Optional

import pandas as pd

try:
    from osgeo import gdal  # optional
except Exception:  # pragma: no cover
    gdal = None

from shybox.orchestrator_toolkit.lib_orchestrator_utils_adapters_fx_single import (
    adapter_source_obj, adapter_destination_obj)
from shybox.orchestrator_toolkit.lib_orchestrator_utils_adapters_registry import ADAPTERS_TYPE, ADAPTERS_FX_TS
from shybox.logging_toolkit.lib_logging_utils import with_logger
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# globals variables
global PROCESSES
PROCESSES = {}

# map the declared output_type to a sensible file extension
_ext_map = {
    'tif': 'tif', 'tiff': 'tif', 'gdal': 'tif', 'xarray': 'tif', 'file': 'tif',
    'table': 'csv', 'csv': 'csv', 'pandas': 'csv',
    'shape': 'json', 'dict': 'json', 'geojson': 'json',
    'text': 'txt', 'txt': 'txt'
}

# decoretor method
def as_process(input_type=None, adapter_type=None, output_type=None, **process_attrs):

    adapter_type = adapter_type or {}

    def decorator(func):

        @functools.wraps(func)
        @with_logger(var_name='logger_stream')
        def wrapper(data, *args, **kwargs):

            # get data time
            data_time = []
            if 'time' in kwargs:
                data_time = kwargs.pop('time', None)
            # get data keys
            data_keys = []
            if 'keys' in kwargs:
                data_keys = kwargs['keys']
            # get data types
            data_types = []
            if 'types' in kwargs:
                data_types = kwargs['types']
            # get mapping vars
            mapping_fx_vars = {}
            if 'mapping_fx_vars' in kwargs:
                mapping_fx_vars = kwargs['mapping_fx_vars']
            # get mapping args
            mapping_fx_args = {}
            if 'mapping_fx_args' in kwargs:
                mapping_fx_args = kwargs['mapping_fx_args']

            # normalize fx datasets
            fx_data = _normalize_input(data, input_type=input_type, **process_attrs)
            # organize fx arguments
            fx_args = _organize_args(
                obj_data=fx_data, keys_data=data_keys, time_data=data_time,
                mapping_fx_vars=mapping_fx_vars, mapping_fx_args=mapping_fx_args
            )
            # check fx arguments
            fx_args = _check_args(
                func=func, fx_args=fx_args,
                args=args, kwargs=kwargs,
                lazy_undefined_args=process_attrs.get('lazy_undefined_args', True),
                lazy_undefined_value=process_attrs.get('lazy_undefined_value', None)
            )
            # convert fx arguments (if needed)
            fx_args = _apply_adapters(fx_args=fx_args, fx_time=data_time, mapping_adapters=adapter_type)

            # execute fx
            result = func(**fx_args, **kwargs)

            # normalize fx results
            result = _normalize_output(result, output_type=output_type)

            return result

        # set output extension from library
        wrapper.output_ext = _ext_map.get(output_type, 'txt')

        # set wrapper attributes
        for key, value in process_attrs.items():
            setattr(wrapper, key, value)
        # update processes global
        PROCESSES[func.__name__] = wrapper

        return wrapper

    return decorator
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to apply adapters to function arguments
def _apply_adapters(
        fx_args: dict,
        fx_time: Union[str, pd.Timestamp, pd.Timestamp, list, None] = None,
        mapping_adapters: Union[dict, None] = None):

    # if adapters are not defined. Nothing to do
    if not mapping_adapters:
        return fx_args

    # standard object -> adapters based on argument names cannot be applied
    if not isinstance(fx_args, dict):
        return fx_args

    # iterate over variable and adapters
    fx_args = dict(fx_args)
    for arg_name, adapter_name in mapping_adapters.items():

        # argument not available
        if arg_name not in fx_args:

            logger_stream.warning(
                f'Argument "{arg_name}" required by adapter '
                f'"{adapter_name}" is not available. '
                f'Adapter will be skipped.'
            )

            continue

        # adapter not registered
        if adapter_name not in ADAPTERS_TYPE:
            raise KeyError(f'Adapter "{adapter_name}" is not registered.')

        # get adapter function
        adapter_fx = ADAPTERS_TYPE[adapter_name]

        # prepare adapter kwargs
        adapter_kwargs = {}
        adapter_signature = inspect.signature(adapter_fx)

        # set time args (if defined in the adapters fx)
        if "time" in adapter_signature.parameters:
            adapter_kwargs["time"] = fx_time

        # apply adapter
        fx_args[arg_name] = adapter_fx(fx_args[arg_name], **adapter_kwargs)

    return fx_args
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to check function arguments
def _check_args(
        func,fx_args,
        args=None, kwargs=None,
        lazy_undefined_args=False, lazy_undefined_value=None):

    # normalize inputs
    args = args or ()
    kwargs = kwargs or {}

    # standard object -> nothing to check here
    if not isinstance(fx_args, dict):
        return fx_args

    # get function signature
    signature = inspect.signature(func)
    # arguments already provided by caller
    bound = signature.bind_partial(*args, **kwargs)

    # copy function arguments
    fx_args_checked = dict(fx_args)

    # collect missing required arguments
    missing_args = []
    for param_name, param in signature.parameters.items():

        # skip *args and **kwargs
        if param.kind in (
                inspect.Parameter.VAR_POSITIONAL,
                inspect.Parameter.VAR_KEYWORD):
            continue

        # already provided by positional/keyword caller arguments
        if param_name in bound.arguments:
            continue

        # available in organized function arguments
        if param_name in fx_args_checked:
            continue

        # parameter has default value -> optional
        if param.default is not inspect.Parameter.empty:
            continue

        # required parameter is missing
        missing_args.append(param_name)

    # manage missing required arguments
    if missing_args:

        if not lazy_undefined_args:
            raise TypeError(
                f"Function '{func.__name__}' is missing required "
                f"arguments: {missing_args}"
            )

        # fill missing arguments with undefined value
        for param_name in missing_args:
            fx_args_checked[param_name] = lazy_undefined_value

    return fx_args_checked
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to organize function arguments using variable mapping
def _organize_args(
        obj_data: Union[dict, list, str],
        keys_data: list, time_data: Union[str, pd.Timestamp, pd.DatetimeIndex, list, None] = None,
        mapping_fx_vars: Optional[dict] = None,
        mapping_fx_args: Optional[dict] = None):

    # mappings not defined -> standard object
    if not mapping_fx_vars or not mapping_fx_args:
        return obj_data

    # normalize references
    keys_ref = set(keys_data)
    vars_ref = set(mapping_fx_vars.values())
    args_ref = set(mapping_fx_args.values())

    # arguments variables must be available both
    # in data keys and in workflow variable mapping
    missing_in_keys = args_ref - keys_ref
    missing_in_vars = args_ref - vars_ref

    if missing_in_keys or missing_in_vars:
        logger_stream.warning(
            'Function argument mapping is not compatible with available data. '
            f'Missing in data keys: {missing_in_keys}; '
            f'Missing in variable mapping: {missing_in_vars}. '
            'Standard object will be used.'
        )

        return obj_data

    # organize mapped arguments
    fx_args = {}
    for arg_name, variable_name in mapping_fx_args.items():

        # CASE 1: obj_data already organized as dictionary
        if isinstance(obj_data, dict):
            if arg_name in obj_data:
                fx_args[arg_name] = obj_data[arg_name]
                continue
            if variable_name in obj_data:
                fx_args[arg_name] = obj_data[variable_name]
                continue
            logger_stream.warning(f'Argument "{arg_name}" mapped to variable "{variable_name}" is not available in input dictionary.')
            continue

        # CASE 2: obj_data is a list / tuple associated with keys_data
        if isinstance(obj_data, (list, tuple)):

            values = [
                data_step
                for data_key, data_step in zip(keys_data, obj_data)
                if data_key == variable_name
            ]

            if not values:
                logger_stream.warning(f'Variable "{variable_name}" mapped to argument "{arg_name}" has no associated data.')
                continue

            # single value
            if len(values) == 1:
                fx_args[arg_name] = values[0]

            # multiple values for the same variable
            else:
                fx_args[arg_name] = values

            continue

        # CASE 3: unsupported structure
        logger_stream.warning(f'Cannot organize argument "{arg_name}" from object type "{type(obj_data).__name__}".')

    # add time information (if needed)
    if 'time' not in fx_args:
        fx_args['time'] = time_data

    return fx_args
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to normalize input data
def _normalize_input(data, input_type=None, lazy_undefined_value=None):

    # dictionary
    if isinstance(data, dict):
        return {key: adapter_source_obj(
            value, obj_type=input_type, obj_undefined=lazy_undefined_value) for key, value in data.items()}

    # list / tuple
    elif isinstance(data, (list, tuple)):
        return [adapter_source_obj(value, obj_type=input_type, obj_undefined=lazy_undefined_value) for value in data]

    # single object
    else:
        return adapter_source_obj(data, obj_type=input_type, obj_undefined=lazy_undefined_value)
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to normalize process output
def _normalize_output(result, output_type=None):
    # single object
    dst_obi = adapter_destination_obj(obj_results=result, obj_type=output_type)
    return dst_obi
# ----------------------------------------------------------------------------------------------------------------------
