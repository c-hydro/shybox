"""
Library Features:

Name:          lib_orchestrator_utils_processes
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20260123'
Version:       '1.1.0'
"""
# ----------------------------------------------------------------------------------------------------------------------
# libraries
import functools
import warnings
import inspect
import pandas as pd
import xarray as xr

import rioxarray as rxr
import tempfile
import os

try:
    from osgeo import gdal  # optional
except Exception:  # pragma: no cover
    gdal = None

from osgeo import gdal
from typing import Iterable

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
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to decorate processing functions
def as_process(input_type: str = 'xarray', output_type: str = 'xarray', **decorator_attrs):
    """
    Decorate a processing function that has signature like:
        func(data, *args, **kwargs)

    Conventions:
      - input_type:  'pandas' | 'xarray' | 'gdal' | 'file'
      - output_type: 'pandas' | 'xarray' | 'gdal' | 'file' | 'tif' | 'tiff' |
                     'table' | 'csv' | 'pandas' | 'shape' | 'dict' | 'geojson' |
                     'text' | 'txt'
    """
    def decorator(func):

        @functools.wraps(func)
        @with_logger(var_name='logger_stream')
        def wrapper(data, *args, **kwargs):
            created_temp_paths = []

            # normalize and convert input data
            def _to_gdal(obj):
                # Convert DataArray/Dataset → GDAL (or path to file that GDAL can open)
                if isinstance(obj, (xr.DataArray, xr.Dataset)):
                    gd = xarray_to_gdal(obj)
                    return gd
                return obj  # assume already GDAL or path

            def _from_file(obj):
                # Convert file path → xarray
                return file_to_xarray(obj)

            def _to_file(obj):
                # Convert xarray → file path; track for later cleanup if desired
                path = xarray_to_file(obj)  # returns a temp path (assumed)
                created_temp_paths.append(path)
                return path

            # Convert the incoming `data` according to input_type
            if input_type not in ('pandas', 'xarray', 'gdal', 'file'):
                warnings.warn(f"Unknown input_type '{input_type}', leaving data as-is.")

            def _convert_single(obj):
                if input_type == 'gdal':
                    return _to_gdal(obj)
                elif input_type == 'file':
                    # If caller passed a file path, we want xarray obj here:
                    return _from_file(obj)
                else:
                    return obj  # 'xarray' passthrough

            # handle different data structures: dict, list/tuple, or single object
            if isinstance(data, dict):

                # Defer conversion of individual values to the function (dict can contain multiple fields)
                normalized_data = {k: _convert_single(v) for k, v in data.items()}

            elif isinstance(data, (list, tuple)):

                # iterate over items, convert each, and flatten if needed (e.g. list of lists)
                normalized_data = []
                for v in data:

                    # manage value (keep if not None, skip if None unless lazy_undefined_value is set)
                    value = _convert_single(v)
                    if value is None:
                        if 'lazy_undefined_value' in decorator_attrs:
                            pass
                        else:
                            continue
                    # values is a list/tuple
                    if isinstance(value, (list, tuple)):
                        for item in value:

                            # manage None values in list/tuple: skip if None or keep if lazy_undefined_value is set
                            if item is not None:
                                normalized_data.append(item)
                            else:
                                if 'lazy_undefined_value' in decorator_attrs:
                                    normalized_data.append(decorator_attrs['lazy_undefined_value'])
                                else:
                                    pass

                    else:
                        normalized_data.append(value)

            else:
                normalized_data = _convert_single(data)

            # call the wrapped function with normalized data
            try:
                if isinstance(normalized_data, dict):

                    # dict: merge with kwargs (dict takes precedence)
                    merged_kwargs = {**kwargs, **normalized_data}
                    result = func(*args, **merged_kwargs)

                elif isinstance(normalized_data, (list, tuple)):

                    # get the function signature to determine parameter names
                    signature = inspect.signature(func)

                    # Debug: show function and its parameters
                    logger_stream.info(f"Function: {func.__name__}")
                    logger_stream.info(f"Signature: {signature}")
                    logger_stream.info(f"Parameters: {list(signature.parameters.keys())}")

                    # get optional keys used to build the dynamic dictionary input
                    file_keys = kwargs.get("keys", [])

                    # Find required positional parameters that have not been provided
                    params = []
                    for index, param in enumerate(signature.parameters.values()):

                        is_required = param.default is inspect.Parameter.empty

                        if param.default is None:
                            if param.name in file_keys:
                                is_required = True

                        is_positional = param.kind in (
                            inspect.Parameter.POSITIONAL_ONLY,
                            inspect.Parameter.POSITIONAL_OR_KEYWORD,
                        )

                        is_provided = index < len(args) or param.name in kwargs

                        if is_positional and is_required and not is_provided:
                            params.append(param)

                    # Debug: show parameters found
                    logger_stream.info(f"Missing required parameters: {[param.name for param in params]}")

                    # order based on file_keys
                    order = {key: i for i, key in enumerate(file_keys)}
                    params.sort(key=lambda param: order.get(param.name, len(order)))

                    # map normalized data to function parameters
                    if len(params) == 1:

                        param_name = params[0].name
                        if isinstance(normalized_data, (list, tuple)):

                            if file_keys is not None:
                                sel_keys = (
                                    list(file_keys)
                                    if isinstance(file_keys, (list, tuple))
                                    else [file_keys]
                                )

                                if len(sel_keys) != len(normalized_data):
                                    count_data = {}
                                    for step_data in normalized_data:
                                        name_data = step_data.name
                                        if name_data not in count_data:
                                            count_data[name_data] = 1
                                        else:
                                            count_data[name_data] += 1
                                    for key_data, count_data in count_data.items():
                                        count_keys = sel_keys.count(key_data)
                                        if count_keys == count_data:
                                            sel_keys = sel_keys[0:count_keys]
                                        else:
                                            raise ValueError(
                                                f"Cannot build '{param_name}' dictionary: "
                                                f"{len(sel_keys)} keys for "
                                                f"{len(normalized_data)} normalized objects."
                                            )

                                if len(set(sel_keys)) == 1:
                                    param_value = {sel_keys[0]: normalized_data}
                                else:
                                    param_value = dict(zip(sel_keys, normalized_data))

                            else:
                                param_value = {
                                    f"obj_{index + 1}": obj
                                    for index, obj in enumerate(normalized_data)
                                }

                        else:
                            param_value = normalized_data

                        normalized_dict = {param_name: param_value}

                    else:
                        param_names = [param.name for param in params]
                        normalized_dict = dict(zip(param_names, normalized_data))

                    lazy_undefined_args = False
                    if 'lazy_undefined_args' in decorator_attrs and decorator_attrs['lazy_undefined_args']:
                        lazy_undefined_args = decorator_attrs['lazy_undefined_args']

                    # check for missing required parameters (those that are not in normalized_dict and have no default)
                    missing = [p.name for p in params if p.name not in normalized_dict and p.default is inspect._empty]
                    if missing:
                        if not lazy_undefined_args:
                            raise TypeError(f"Function '{func.__name__}' missing required arguments: {missing}")
                        else:
                            lazy_undefined_value = None
                            if 'lazy_undefined_value' in decorator_attrs:
                                lazy_undefined_value = decorator_attrs['lazy_undefined_value']
                            lazy_kwargs = {k: lazy_undefined_value for k in missing}
                    else:
                        lazy_kwargs = {}

                    # organize the call kwargs: normalized_dict takes precedence over kwargs
                    extended_kwargs = {**normalized_dict, **kwargs, **lazy_kwargs}
                    result = func(*args, **extended_kwargs)

                elif isinstance(normalized_data, pd.DataFrame):
                    result = func(normalized_data, *args, **kwargs)

                elif isinstance(normalized_data, pd.Series):
                    result = func(normalized_data, *args, **kwargs)

                elif (isinstance(normalized_data, (xr.DataArray, xr.Dataset))
                      or (gdal and isinstance(normalized_data, gdal.Dataset))):
                    result = func(normalized_data, *args, **kwargs)
                else:
                    raise TypeError(f'Unsupported data type: {type(normalized_data)}')
            finally:
                # If we created temp files for input ('file' path generation etc.), decide if you want to keep or remove.
                # In your original code you removed the input when input_type == 'file', but that was ambiguous.
                # Here we only remove paths we KNOW we created.
                for p in created_temp_paths:
                    try:
                        remove(p)
                    except Exception:
                        pass  # best-effort cleanup

            # convert the result according to output_type
            # Keep naming consistent: we interpret output_type as the format you WANT to return.
            if output_type == 'xarray':
                # If result is GDAL/path, bring it back to xarray
                if gdal and isinstance(result, gdal.Dataset):
                    result = gdal_to_xarray(result)
                elif isinstance(result, (str, bytes)):  # path-like
                    result = gdal_to_xarray(result)
                # else assume it is already xarray
            elif output_type in ('gdal', 'tif', 'tiff'):
                # If result is xarray, convert to GDAL-compatible (or a tif path)
                if isinstance(result, (xr.DataArray, xr.Dataset)):
                    result = xarray_to_gdal(result)
                # else assume it is already GDAL/path
            elif output_type in ('file',):
                # If result is xarray, write to a file and return path
                if isinstance(result, (xr.DataArray, xr.Dataset)):
                    result = xarray_to_file(result)  # returns path
            elif output_type in ('table', 'csv', 'pandas', 'shape', 'dict', 'geojson', 'text', 'txt'):
                # leave as-is; your concrete functions should return the correct objects
                pass
            else:
                warnings.warn(f"Unknown output_type '{output_type}', returning result unchanged.")

            return result

        # add the output_ext attribute
        setattr(wrapper, 'output_ext', _ext_map.get(output_type, 'txt'))

        # attach extra attributes
        for key, value in decorator_attrs.items():
            setattr(wrapper, key, value)

        # register the process
        PROCESSES[func.__name__] = wrapper

        return wrapper

    return decorator
# ----------------------------------------------------------------------------------------------------------------------
from functools import wraps
# ----------------------------------------------------------------------------------------------------------------------
def with_dict_input(func):
    """
    Recursively walk through nested dictionaries.

    Supported leaves:
        - list[pd.DataFrame | pd.Series | None]

    Examples:
        list
        dict -> list
        dict -> dict -> list
        dict -> dict -> dict -> list

    The dictionary structure is preserved in the output.
    """

    @wraps(func)
    def wrapper(data, *args, **kwargs):

        def process(obj, path="data"):

            # CASE 1: dictionary -> recursively process every key
            if isinstance(obj, dict):
                result = {}
                for key, value in obj.items():
                    result[key] = process(value,path=f"{path}.{key}")
                return result

            # CASE 2: list -> expected time series
            if isinstance(obj, (list, tuple)):

                # empty list
                if len(obj) == 0:
                    return func(obj,*args,**kwargs)

                # validate elements
                valid_types = (pd.DataFrame, pd.Series, type(None),)
                if all(isinstance(item, valid_types) for item in obj):
                    return func(obj, *args, **kwargs)
                raise TypeError(f"Unsupported list content at '{path}'. Expected DataFrame, Series or None.")

            # CASE 3: unsupported leaf
            raise TypeError(f"Unsupported data type at '{path}': {type(obj).__name__}")

        return process(data)

    return wrapper
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# methods to decorate other methods
def with_list_input(func):
    def wrapper(data, *args, **kwargs):
        if isinstance(data, Iterable) and not isinstance(data, str) and not isinstance(data, xr.DataArray):
            return [func(i, *args, **kwargs) for i in data]
        else:
            return func(data, *args, **kwargs)
    return wrapper
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to remove file
@with_list_input
def remove(filename: str):
    if os.path.exists(filename): os.remove(filename)
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to dump data to file
@with_list_input
def xarray_to_file(data_array: xr.DataArray) -> str:
    # Create a temporary file
    temp_file = tempfile.NamedTemporaryFile(suffix='.tif', delete=False)
    temp_file.close()

    # Save the DataArray to the temporary file
    data_array.rio.to_raster(temp_file.name, compress='LZW')

    # Move the temporary file to the desired filename
    return temp_file.name
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to read data from file to xarray
@with_list_input
def file_to_xarray(file_path: str) -> xr.DataArray:
    # Open the raster as xarray DataArray
    da = rxr.open_rasterio(file_path)

    # If it's single-band, squeeze the band dimension
    if "band" in da.dims and da.sizes.get("band", 1) == 1:
        da = da.squeeze("band", drop=True)

    return da
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to read data from file (xarray)
@with_list_input
def xarray_to_gdal(data_array: xr.DataArray) -> gdal.Dataset:
    temp_file = xarray_to_file(data_array)

    # Open the temporary file with GDAL
    gdal_dataset = gdal.Open(temp_file)
    # Optionally, delete the temporary file after opening it with GDAL
    os.remove(temp_file)

    return gdal_dataset
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to convert data to file (xarray)
@with_list_input
def gdal_to_xarray(dataset: gdal.Dataset) -> xr.DataArray:

    # Create a temporary file
    temp_file = tempfile.NamedTemporaryFile(suffix='.tif', delete=False)
    temp_file.close()

    # Save the Dataset to the temporary file
    driver = gdal.GetDriverByName('GTiff')
    driver.CreateCopy(temp_file.name, dataset, options=['COMPRESS=LZW'])

    # Open the temporary file with xarray
    data_array = rxr.open_rasterio(temp_file.name)

    # Optionally, delete the temporary file after opening it with GDAL
    os.remove(temp_file.name)

    return data_array
# ----------------------------------------------------------------------------------------------------------------------
