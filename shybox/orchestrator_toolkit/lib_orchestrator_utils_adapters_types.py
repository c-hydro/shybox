# ----------------------------------------------------------------------------------------------------------------------
"""
Library Features:

Name:          lib_orchestrator_utils_adapters_types
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20261014'
Version:       '1.2.0'
"""
# ----------------------------------------------------------------------------------------------------------------------
# libraries
from typing import Union
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
# adapter to convert fx output to dictionary
def fx_to_dict(
        obj_data, obj_keys=None, obj_separator='|',
        logger_stream=None, **kwargs):

    # normalize input object to list of values
    if isinstance(obj_data, dict):
        obj_values = list(obj_data.values())
    elif isinstance(obj_data, (list, tuple)):
        obj_values = list(obj_data)
    else:
        obj_values = [obj_data]

    # check mapping (if exists)
    if obj_keys is None:

        if 'mapping' in kwargs:
            mapping = kwargs['mapping']

            # get mapping values
            obj_keys = []
            for map_key, map_value in mapping.items():

                # use sub-dictionary values
                if isinstance(map_value, dict):
                    obj_keys.extend(map_value.values())
                # use root value
                else:
                    obj_keys.append(map_value)

        else:
            # create default keys
            obj_keys = [str(idx + 1) for idx in range(len(obj_values))]

    # normalize key defined by string
    elif isinstance(obj_keys, str):
        obj_keys = [obj_keys]
    # normalize key defined by list or tuple
    elif isinstance(obj_keys, (list, tuple)):
        obj_keys = list(obj_keys)

    # unsupported key format
    else:
        raise TypeError(f"`obj_keys` must be None, str, list or tuple. Got {type(obj_keys)}")

    # check if number of values is greater than number of keys
    if len(obj_values) > len(obj_keys):

        msg = (
            f"Number of objects ({len(obj_values)}) is greater than "
            f"number of keys ({len(obj_keys)}). "
            f"Only the first {len(obj_keys)} objects will be added."
        )

        if logger_stream is not None:
            logger_stream.warning(msg)

    # create output dictionary
    if len(obj_values) == 1 and obj_keys:

        # get root keys
        root_keys = {key.split(obj_separator, 1)[0] for key in obj_keys}

        # use root only if all keys share the same root
        if len(root_keys) == 1:
            obj_dict = {next(iter(root_keys)): obj_values[0]}

        # use complete keys
        else:
            obj_dict = {key: value for key, value in zip(obj_keys, obj_values)}

    else:

        # map values to keys
        obj_dict = {key: value for key, value in zip(obj_keys, obj_values)}

    return obj_dict
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# adapter to convert dict to fx input
def dict_to_fx(obj_data, flat_list_one_element=True, **kwargs):

    # if not dictinary (for example key:value run in the previous method)
    if not isinstance(obj_data, dict):
        return obj_data

    # check if dictionary contains nested dictionaries
    has_subdict = False
    if isinstance(obj_data, dict):
        has_subdict = any(isinstance(root_value, dict)for root_value in obj_data.values())

    # case of flat dictionary
    if not has_subdict:
        # get dictionary values
        obj = list(obj_data.values())
        # flatten one-element list
        if flat_list_one_element and len(obj) == 1:
            obj = obj[0]

    # case of nested dictionary
    else:

        # initialize output
        obj = {}
        # iterate over root dictionary
        for root_key, root_value in obj_data.items():
            # nested dictionary
            if isinstance(root_value, dict):
                # get nested values
                values = list(root_value.values())
                # flatten one-element list
                if flat_list_one_element and len(values) == 1:
                    values = values[0]
                # keep root key
                obj[root_key] = values
            else:
                # keep standard value
                obj[root_key] = root_value

    return obj
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# helper to normalize xarray object to dictionary
@with_logger(var_name='logger_stream')
def xarray_to_dict(obj: Union[None, xr.DataArray, xr.Dataset]) -> Union[None, dict]:
    # check obj format
    if obj is None:
        return None
    elif isinstance(obj, xr.DataArray):
        name = obj.name or 'var'
        return {name: obj}
    elif isinstance(obj, xr.Dataset):
        return {var_name: obj[var_name] for var_name in obj.data_vars}
    else:
        logger_stream.error(f"Unsupported element in data: {type(obj)}")
        raise TypeError(f"Unsupported element in data: {type(obj)}")
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# helper to normalize `data` to dataset
@with_logger(var_name='logger_stream')
def xarray_to_dataset(obj: Union[None, xr.DataArray, xr.Dataset]) -> Union[None, xr.Dataset]:
    # check obj format
    if obj is None:
        return None
    elif isinstance(obj, xr.Dataset):
        return obj
    elif isinstance(obj, xr.DataArray):
        name = obj.name or 'var'
        return obj.rename(name).to_dataset(name=name)
    else:
        logger_stream.error(f"Unsupported element in data: {type(obj)}")
        raise TypeError(f"Unsupported element in data: {type(obj)}")
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# helper to normalize `data` to DataArray
@with_logger(var_name='logger_stream')
def xarray_to_dataarray(obj: Union[None, xr.DataArray, xr.Dataset]) -> Union[None, xr.DataArray]:

    # check obj format
    if obj is None:
        return None
    elif isinstance(obj, xr.DataArray):
        return obj
    elif isinstance(obj, xr.Dataset):
        # check number of data variables
        if len(obj.data_vars) != 1:
            logger_stream.error(
                f"Dataset must contain exactly one data variable. "
                f"Found {len(obj.data_vars)} variables: {list(obj.data_vars)}"
            )
            raise ValueError(
                f"Dataset must contain exactly one data variable. "
                f"Found {len(obj.data_vars)} variables: {list(obj.data_vars)}"
            )

        # get the only data variable
        var_name = next(iter(obj.data_vars))

        # convert Dataset to DataArray
        obj = obj[var_name]
        # set name
        obj.name = var_name

        return obj

    else:
        logger_stream.error(f"Unsupported element in data: {type(obj)}")
        raise TypeError(f"Unsupported element in data: {type(obj)}")
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
