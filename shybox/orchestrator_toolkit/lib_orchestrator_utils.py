"""
Library Features:

Name:          lib_orchestrator_utils
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20250127'
Version:       '1.0.0'
"""
# ----------------------------------------------------------------------------------------------------------------------
# libraries
import xarray as xr
import functools
import warnings
import xarray as xr

import rioxarray as rxr
import tempfile
import os

from osgeo import gdal
from typing import Iterable

#PROCESSES = globals().setdefault("PROCESSES", {})
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# globals variables
global PROCESSES
PROCESSES = {}
# ----------------------------------------------------------------------------------------------------------------------


try:
    from osgeo import gdal  # optional
except Exception:  # pragma: no cover
    gdal = None

# --- these helpers are assumed to exist in your codebase ---------------
# xarray_to_gdal(xr_obj) -> gdal.Dataset or path
# file_to_xarray(path) -> xr.DataArray or xr.Dataset
# gdal_to_xarray(gdal_ds_or_path) -> xr.DataArray or xr.Dataset
# xarray_to_file(xr_obj, path=None) -> path
# remove(path) -> None



def as_process(input_type: str = 'xarray', output_type: str = 'xarray', **decorator_attrs):
    """
    Decorate a processing function that has signature like:
        func(data, *args, **kwargs)

    Conventions:
      - input_type:  'xarray' | 'gdal' | 'file'
      - output_type: 'xarray' | 'gdal' | 'file' | 'tif' | 'tiff' |
                     'table' | 'csv' | 'pandas' | 'shape' | 'dict' | 'geojson' |
                     'text' | 'txt'
    """
    def decorator(func):

        @functools.wraps(func)
        def wrapper(data, *args, **kwargs):
            created_temp_paths = []

            # ------------- normalize & convert INPUT ------------------------
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
            if input_type not in ('xarray', 'gdal', 'file'):
                warnings.warn(f"Unknown input_type '{input_type}', leaving data as-is.")

            def _convert_single(obj):
                if input_type == 'gdal':
                    return _to_gdal(obj)
                elif input_type == 'file':
                    # If caller passed a file path, we want xarray obj here:
                    return _from_file(obj)
                else:
                    return obj  # 'xarray' passthrough

            if isinstance(data, dict):
                # Defer conversion of individual values to the function (dict can contain multiple fields)
                normalized_data = {k: _convert_single(v) for k, v in data.items()}
            elif isinstance(data, (list, tuple)):
                normalized_data = type(data)(_convert_single(v) for v in data)
            else:
                normalized_data = _convert_single(data)

            # ------------- CALL the wrapped function ------------------------
            try:
                if isinstance(normalized_data, dict):
                    # dict: merge with kwargs (dict takes precedence)
                    merged_kwargs = {**kwargs, **normalized_data}
                    result = func(*args, **merged_kwargs)
                elif isinstance(normalized_data, (list, tuple)):
                    # IMPORTANT: pass list/tuple as ONE arg (do not splat)
                    result = func(normalized_data, *args, **kwargs)
                elif (
                    isinstance(normalized_data, (xr.DataArray, xr.Dataset))
                    or (gdal and isinstance(normalized_data, gdal.Dataset))
                ):
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

            # ------------- convert OUTPUT to the requested type -------------
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

        # ------------- output_ext attribute ---------------------------------
        # Map the declared output_type to a sensible file extension
        _ext_map = {
            'tif': 'tif', 'tiff': 'tif', 'gdal': 'tif', 'xarray': 'tif', 'file': 'tif',
            'table': 'csv', 'csv': 'csv', 'pandas': 'csv',
            'shape': 'json', 'dict': 'json', 'geojson': 'json',
            'text': 'txt', 'txt': 'txt'
        }
        setattr(wrapper, 'output_ext', _ext_map.get(output_type, 'txt'))

        # attach extra attributes
        for key, value in decorator_attrs.items():
            setattr(wrapper, key, value)

        # register the process
        PROCESSES[func.__name__] = wrapper
        return wrapper

    return decorator


# ----------------------------------------------------------------------------------------------------------------------
# method to decorate processing algorithm
def as_process_OLD(input_type: str = 'xarray', output_type: str = 'xarray', **kwargs):
    def decorator(func):
        def wrapper(data, *args, **kwargs):
            # Ensure the input is in the correct format
            if input_type == 'gdal':
                # convert xr.DataArray to gdal.Dataset
                data = xarray_to_gdal(data)
            elif input_type == 'file':
                # convert filename to xr.DataArray
                data = xarray_to_file(data)

            # Call the original function
            if isinstance(data, dict):
                result = func(*args, **data, **kwargs)
            elif isinstance(data, list):
                result = func(*data, *args, **kwargs)
            elif isinstance(data, xr.DataArray) or isinstance(data, gdal.Dataset):
                result = func(data, *args, **kwargs)
            else:
                raise TypeError('Unsupported data type')

            # remove the temporary file if input is a file
            if input_type == 'file':
                remove(data)

            # Ensure the output is in the correct format
            if output_type == 'gdal':
                # Add your output validation logic here
                result = gdal_to_xarray(result)
            elif output_type == 'file':
                # Add your output validation logic here
                result = file_to_xarray(result)
            return result

        # define the output extension based on the output type (attribute of the output object)
        if output_type in ['tif', 'tiff', 'gdal', 'xarray', 'file']:
            setattr(wrapper, 'output_ext', 'tif')
        elif output_type in ['table', 'csv', 'pandas']:
            setattr(wrapper, 'output_ext', 'csv')
        elif output_type in ['shape', 'dict', 'geojson']:
            setattr(wrapper, 'output_ext', 'json')
        elif output_type in ['text', 'txt']:
            setattr(wrapper, 'output_ext', 'txt')

        wrapper.__name__ = func.__name__
        for key, value in kwargs.items():
            setattr(wrapper, key, value)

        # Add the wrapped function to the global list of processes
        PROCESSES[func.__name__] = wrapper

        return wrapper

    return decorator
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
    os.remove(filename)
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
