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

            if isinstance(data, dict):
                # Defer conversion of individual values to the function (dict can contain multiple fields)
                normalized_data = {k: _convert_single(v) for k, v in data.items()}
            elif isinstance(data, (list, tuple)):
                normalized_data = type(data)(_convert_single(v) for v in data)
            else:
                normalized_data = _convert_single(data)

            # call the wrapped function with normalized data
            try:
                if isinstance(normalized_data, dict):

                    # dict: merge with kwargs (dict takes precedence)
                    merged_kwargs = {**kwargs, **normalized_data}
                    result = func(*args, **merged_kwargs)

                elif isinstance(normalized_data, (list, tuple)):
                    # IMPORTANT: pass list/tuple as ONE arg (do not splat)
                    result = func(normalized_data, *args, **kwargs)

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
