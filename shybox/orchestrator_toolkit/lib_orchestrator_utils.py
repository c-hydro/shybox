"""
Library Features:

Name:          lib_orch_utils
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20250127'
Version:       '1.0.0'
"""
# ----------------------------------------------------------------------------------------------------------------------
# libraries
import xarray as xr
#import rioxarray as rxr
from osgeo import gdal
import tempfile
import os

from typing import Iterable

from shybox.type_toolkit.lib_type_grid import DataGrid

# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# globals variables
global PROCESSES
PROCESSES = {}
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to decorate processing algorithm
def as_process(input_type: str = 'xarray', output_type: str = 'xarray', **kwargs):
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
            result = func(data, *args, **kwargs)

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

@with_list_input
def remove(filename: str):
    os.remove(filename)

@with_list_input
def grid_to_xarray(data: DataGrid) -> (xr.DataArray, xr.Dataset):
    data = data.data
    return data


@with_list_input
def xarray_to_file(data_array: xr.DataArray) -> str:
    # Create a temporary file
    temp_file = tempfile.NamedTemporaryFile(suffix='.tif', delete=False)
    temp_file.close()

    # Save the DataArray to the temporary file
    data_array.rio.to_raster(temp_file.name, compress='LZW')

    # Move the temporary file to the desired filename
    return temp_file.name


'''
@with_list_input
def file_to_xarray(filename: str) -> xr.DataArray:
    # Open the file with xarray
    return rxr.open_rasterio(filename)
'''

@with_list_input
def xarray_to_gdal(data_array: xr.DataArray) -> gdal.Dataset:
    temp_file = xarray_to_file(data_array)

    # Open the temporary file with GDAL
    gdal_dataset = gdal.Open(temp_file)
    # Optionally, delete the temporary file after opening it with GDAL
    os.remove(temp_file)

    return gdal_dataset


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