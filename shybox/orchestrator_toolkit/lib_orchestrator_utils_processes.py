"""
Library Features:

Name:          lib_orchestrator_utils_processes
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20260831'
Version:       '1.0.0'
"""
# ----------------------------------------------------------------------------------------------------------------------
# libraries
import functools
import warnings
import inspect
import pandas as pd

from shybox.orchestrator_toolkit.lib_orchestrator_utils_checker import check_data_type
from shybox.orchestrator_toolkit.lib_orchestrator_utils_adapters_registry import (
    ADAPTERS_TYPE, ADAPTERS_FX_TS, ADAPTERS_FX_MAPS)
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

# manage adapters
ADAPTERS_COLLECTIONS = {**ADAPTERS_FX_TS, **ADAPTERS_FX_MAPS, **ADAPTERS_TYPE}
# --------------------------------------------------------------------------------------------------------------

# --------------------------------------------------------------------------------------------------------------
# decoretor method
def as_process(
        input_type=None, input_adapter=None,
        output_type=None, output_adapter=None,
        args_adapter=None,
        **process_attrs):

    # initialize adapters
    input_adapter = input_adapter or {}
    output_adapter = output_adapter or {}
    args_adapter = args_adapter or {}
    # --------------------------------------------------------------------------------------------------------------

    # --------------------------------------------------------------------------------------------------------------
    # set decoretor
    def decorator(func):

        # --------------------------------------------------------------------------------------------------------------
        # get function signature
        func_signature = inspect.signature(func)
        func_parameters = func_signature.parameters

        # get explicit function arguments
        func_args = {
            param_name for param_name, param_obj in func_parameters.items()
            if param_obj.kind in (
                inspect.Parameter.POSITIONAL_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                inspect.Parameter.KEYWORD_ONLY,
            )
        }

        # check if function accepts **kwargs
        func_has_kwargs = any(param_obj.kind == inspect.Parameter.VAR_KEYWORD for param_obj in func_parameters.values())

        # validate input adapters
        for variable_name, adapter_obj in input_adapter.items():

            # check variable against function signature
            if variable_name not in func_args and not func_has_kwargs:
                raise RuntimeError(
                    f'Input adapter variable "{variable_name}" is not available in function "{func.__name__}" signature.')

            # configure adapter pipeline
            adapter_configs = __configure_adapter(adapter_obj)
            # validate each adapter in the pipeline
            for adapter_config in adapter_configs:

                # get adapter information
                adapter_name = adapter_config.get('method', None)
                adapter_kwargs = adapter_config.get('kwargs', {}) or {}

                # adapter method is mandatory
                if adapter_name is None:
                    raise RuntimeError(f'Input adapter for variable "{variable_name}" does not define a method.')
                # check adapter in registry
                if adapter_name not in ADAPTERS_COLLECTIONS:
                    raise RuntimeError(f'Adapter "{adapter_name}" is not available for variable "{variable_name}".')

        # validate args adapters
        for arg_name, adapter_obj in args_adapter.items():

            # check argument against function signature
            if arg_name not in func_args and not func_has_kwargs:
                raise RuntimeError(
                    f'Argument adapter "{arg_name}" is not available in function "{func.__name__}" signature.')

            # configure adapter pipeline
            adapter_configs = __configure_adapter(adapter_obj)
            # validate each adapter in the pipeline
            for adapter_config in adapter_configs:

                # get adapter information
                adapter_name = adapter_config.get('method', None)
                adapter_kwargs = adapter_config.get('kwargs', {}) or {}

                # adapter method is mandatory
                if adapter_name is None:
                    raise RuntimeError(f'Argument adapter for "{arg_name}" does not define a method.')
                # check adapter in registry
                if adapter_name not in ADAPTERS_COLLECTIONS:
                    raise RuntimeError(
                        f'Adapter "{adapter_name}" is not available for argument "{arg_name}".')

        # --------------------------------------------------------------------------------------------------------------

        # --------------------------------------------------------------------------------------------------------------
        # wrapper to decorate function
        @functools.wraps(func)
        @with_logger(var_name='logger_stream')
        def wrapper(data, *args, **kwargs):

            # ----------------------------------------------------------------------------------------------------------
            # get map and behaviour in
            variable_in = kwargs.pop('variable_in', None)
            in_map, in_behaviour = __get_variables_object(variable_in)
            in_kwargs = {'mapping': in_map, 'behaviour': in_behaviour}
            # get map and behaviour out
            variable_out = kwargs.pop('variable_out', None)
            out_map, out_behaviour = __get_variables_object(variable_out)
            out_kwargs = {'mapping': out_map, 'behaviour': out_behaviour}
            # ----------------------------------------------------------------------------------------------------------

            # ----------------------------------------------------------------------------------------------------------
            # check data format (in)
            if not isinstance(data, dict):
                data = __try_to_adjust_data(data, in_map)

            # check input (if expected or not)
            input_check = check_data_type(obj_data=data, string_data=input_type)
            # warn input
            if not input_check:
                logger_stream.warning(f'Decorator "as_process": data type mismatch. '
                                      f'Expected "{input_type}", got "{type(data).__name__}".')
            # ----------------------------------------------------------------------------------------------------------

            # ----------------------------------------------------------------------------------------------------------
            # apply adapters to data variables
            for variable_name, adapter_obj in input_adapter.items():

                # check variable against function signature
                if variable_name not in func_args and not func_has_kwargs:
                    continue

                # configure adapters
                adapter_configs = __configure_adapter(adapter_obj)

                # apply adapters sequentially
                data_tmp = data
                for adapter_config in adapter_configs:

                    # get adapter information
                    adapter_name = adapter_config.get('method', None)
                    adapter_kwargs = adapter_config.get('kwargs', {}) or {}
                    # add in kwargs
                    adapter_kwargs = {**adapter_kwargs, **in_kwargs}

                    # check if adapter name is defined
                    if adapter_name is None:
                        continue

                    # check adapter in the registry
                    if adapter_name not in ADAPTERS_COLLECTIONS:
                        raise RuntimeError(f'Adapter "{adapter_name}" is not available for variable "{variable_name}".')

                    # get adapter
                    adapter_fx = ADAPTERS_COLLECTIONS[adapter_name]
                    # apply adapter
                    data_tmp = adapter_fx(data_tmp, **adapter_kwargs)

                # update adapted data
                data = data_tmp
            # ----------------------------------------------------------------------------------------------------------

            # ----------------------------------------------------------------------------------------------------------
            # apply adapters to function args
            for arg_name, adapter_obj in args_adapter.items():

                # skip unavailable argument
                if arg_name not in kwargs:
                    continue

                # normalize adapter configuration
                adapter_configs = __configure_adapter(adapter_obj)

                # get current argument value
                arg_value = kwargs[arg_name]
                # apply adapters sequentially
                for adapter_config in adapter_configs:

                    # get adapter object(s)
                    adapter_name, adapter_kwargs = adapter_config['method'], adapter_config['kwargs']

                    # check adapter availability
                    if adapter_name not in ADAPTERS_COLLECTIONS:
                        raise RuntimeError(f'Adapter "{adapter_name}" is not available for argument "{arg_name}".')

                    # get adapter function
                    adapter_fx = ADAPTERS_COLLECTIONS[adapter_name]
                    # apply adapter
                    arg_value = adapter_fx(arg_value, **adapter_kwargs)

                # update argument
                kwargs[arg_name] = arg_value

            # ----------------------------------------------------------------------------------------------------------

            # ----------------------------------------------------------------------------------------------------------
            # get time
            time = kwargs.pop('time', None)
            # add time
            data['time'] = time

            # execute fx
            result = func(**data, **kwargs)
            # ----------------------------------------------------------------------------------------------------------

            # ----------------------------------------------------------------------------------------------------------
            # apply adapters to result variables
            for variable_name, adapter_obj in output_adapter.items():

                # configure adapters
                adapter_configs = __configure_adapter(adapter_obj)

                # apply adapters sequentially
                result_tmp = result
                for adapter_config in adapter_configs:

                    # get adapter information
                    adapter_name = adapter_config.get('method', None)
                    adapter_kwargs = adapter_config.get('kwargs', {}) or {}
                    # add in kwargs
                    adapter_kwargs = {**adapter_kwargs, **out_kwargs}

                    # check if adapter name is defined
                    if adapter_name is None:
                        continue

                    # check adapter in the registry
                    if adapter_name not in ADAPTERS_COLLECTIONS:
                        raise RuntimeError(f'Adapter "{adapter_name}" is not available for variable "{variable_name}".')

                    # get adapter
                    adapter_fx = ADAPTERS_COLLECTIONS[adapter_name]
                    # apply adapter
                    result_tmp = adapter_fx(result_tmp, **adapter_kwargs)

                # update adapted data
                result = result_tmp
            # ----------------------------------------------------------------------------------------------------------

            # ----------------------------------------------------------------------------------------------------------
            # check output format
            if not isinstance(result, dict):
                result = __try_to_adjust_result(result, out_map)

            # check output (if expected or not)
            output_check = check_data_type(obj_data=result, string_data=output_type)

            # warn output
            if not output_check:
                logger_stream.warning(f'Decorator "as_process": result type mismatch. '
                                      f'Expected "{output_type}", got "{type(result).__name__}".')

            # return result
            return result
            # ----------------------------------------------------------------------------------------------------------

        # --------------------------------------------------------------------------------------------------------------
        # set output extension
        wrapper.output_ext = _ext_map.get( output_type,'txt')

        # set wrapper attributes
        wrapper.input_type = input_type
        wrapper.output_type = output_type
        wrapper.input_adapter = input_adapter
        wrapper.output_adapter = output_adapter
        wrapper.args_adapter = args_adapter

        # set process attributes
        for key, value in process_attrs.items():
            setattr(wrapper, key, value)

        # update processes global
        PROCESSES[func.__name__] = wrapper

        return wrapper
        # --------------------------------------------------------------------------------------------------------------

    return decorator
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# HELPERS
# helper to configure adapter(s)
def __configure_adapter(adapter_obj):

    # list of adapters
    if isinstance(adapter_obj, list):
        adapter_list = adapter_obj
    # single adapter
    else:
        adapter_list = [adapter_obj]

    # iterate over configuration(s)
    adapter_configs = []
    for adapter_item in adapter_list:
        # simple form
        if isinstance(adapter_item, str):
            adapter_name = adapter_item
            adapter_kwargs = {}
        # extended form
        elif isinstance(adapter_item, dict):
            adapter_name = adapter_item.get('method')
            adapter_kwargs = adapter_item.get('kwargs', {}) or {}
        else:
            raise TypeError(
                f'Adapter configuration must be a string, dictionary or list. Found: {type(adapter_item).__name__}')

        if adapter_name is None:
            raise ValueError('Adapter configuration requires a "method".')

        # save configuration template
        adapter_configs.append({'method': adapter_name,'kwargs': adapter_kwargs})

    return adapter_configs

# helper to get variables objects
def __get_variables_object(variable_mapping: dict) -> tuple[dict, dict]:

    # initialize mapping and behaviour collections
    mapping, behaviour = {}, {}
    # iterate over groups
    for group_name, group_info in variable_mapping.items():
        # get group mapping
        mapping[group_name] = group_info.get('mapping', {})
        # get group behaviour
        behaviour[group_name] = group_info.get('behaviour', None)

    return mapping, behaviour

# helper to try to adjust data object(s)
@with_logger(var_name='logger_stream')
def __try_to_adjust_data(data, mapping):

    # info warning
    logger_stream.warning(
        'Method as_process expected "data" in dictionary format. '
        'Trying to normalize "data" using output mapping. Method not implemented yet.'
    )

    return data

# helper to try to adjust result object(s)
@with_logger(var_name='logger_stream')
def __try_to_adjust_result(result, mapping):

    # info warning
    logger_stream.warning(
        'Method as_process expected "result" in dictionary format. '
        'Trying to normalize "result" using output mapping.'
    )

    # check mapping
    if not isinstance(mapping, dict) or not mapping:
        logger_stream.error('Output mapping is not defined or is not a dictionary')
        raise RuntimeError('Unable to normalize result without a valid output mapping')

    # normalize result to list
    if isinstance(result, (list, tuple)):
        result_values = list(result)
    else:
        result_values = [result]

    # get final mapped output variables
    result_keys = [
        mapped_value
        for mapping_obj in mapping.values()
        if isinstance(mapping_obj, dict)
        for mapped_value in mapping_obj.values()
    ]

    # check number of returned objects
    if len(result_values) != len(result_keys):
        logger_stream.error(
            f'Unable to normalize process result: '
            f'{len(result_values)} returned object(s), '
            f'{len(result_keys)} expected output variable(s). '
            f'Expected variables: {result_keys}'
        )

        raise RuntimeError('Process result size does not match expected output variables')

    # organize result
    result = dict(zip(result_keys, result_values))

    return result
# ----------------------------------------------------------------------------------------------------------------------
