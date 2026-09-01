# ----------------------------------------------------------------------------------------------------------------------
"""
Library Features:

Name:          lib_orchestrator_utils_fx_single
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20260123'
Version:       '1.1.0'
"""
# ----------------------------------------------------------------------------------------------------------------------
# libraries
import warnings
import xarray as xr

import rioxarray as rxr
import tempfile
import os

try:
    from osgeo import gdal  # optional
except Exception:  # pragma: no cover
    gdal = None

from osgeo import gdal

from shybox.logging_toolkit.lib_logging_utils import with_logger
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# adapter to convert source obj to data function
@with_logger(var_name='logger_stream')
def adapter_source_obj(obj_data, obj_type=None, obj_undefined=None):

    # check if obj is defined or not
    if obj_data is None:
        logger_stream.warning("The 'obj_data' is defined by NoneType. Object will be returned defined by NoneType.")
        return obj_undefined

    # check obj type
    if obj_type == 'file':
        return file_to_xarray(obj_data)
    elif obj_type == 'gdal':
        return xarray_to_gdal(obj_data)
    elif obj_type in ('pandas', 'xarray', None):
        return obj_data
    elif obj_type == 'as_is':
        return obj_data
    elif obj_type is None:
        logger_stream.warning("The 'obj_type' is defined by NoneType. Object will be returned unchanged.")
        return obj_data
    else:
        logger_stream.warning(f"Unknown 'obj_type' '{obj_type}'. Object will be returned unchanged.")
        return obj_data
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# adapter to results function to data destination
@with_logger(var_name='logger_stream')
def adapter_destination_obj(obj_results, obj_type=None, obj_undefined=None):

    # check if obj is defined or not
    if obj_results is None:
        logger_stream.warning("The 'obj_results' is defined by NoneType. Object will be returned defined by NoneType.")
        return obj_undefined

    # check obj type
    if obj_type == 'xarray':

        if gdal and isinstance(obj_results, gdal.Dataset):
            return gdal_to_xarray(obj_results)
        elif isinstance(obj_results, (str, bytes)):
            return file_to_xarray(obj_results)

    elif obj_type in ('gdal', 'tif', 'tiff'):

        if isinstance(obj_results, (xr.DataArray, xr.Dataset)):
            return xarray_to_gdal(obj_results)

    elif obj_type == 'file':

        if isinstance(obj_results, (xr.DataArray, xr.Dataset)):
            return xarray_to_file(obj_results)

    elif obj_type in ('table', 'csv', 'pandas', 'shape', 'dict', 'geojson', 'text', 'txt', None):
        return obj_results
    elif obj_type == 'as_is':
        return obj_results
    else:
        logger_stream.warning(f"Unknown output_type '{obj_type}'. Result will be returned unchanged.")
        return obj_results

# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to dump data to file
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
