
import warnings

#import rioxarray as rxr
import xarray as xr
import numpy as np
import os
import json
import datetime as dt

import pandas as pd
import rasterio as rio
import rioxarray as rxr

try:
    import geopandas as gpd
except ImportError:
    pass

from typing import Optional

from shybox.io_toolkit.lib_io_nc import write_file_nc_hmc
from shybox.generic_toolkit.lib_utils_time import is_date, convert_time_format

import matplotlib.pyplot as plt

def check_data_format(data, file_format: str) -> None:
    """"
    Ensures that the data is compatible with the format of the dataset.
    """
    # add possibility to write a geopandas dataframe to a geojson or a shapefile
    if isinstance(data, np.ndarray) or isinstance(data, xr.DataArray):
        if not file_format in ['geotiff', 'netcdf']:
            raise ValueError(f'Cannot write matrix data to a {file_format} file.')

    elif isinstance(data, xr.Dataset):
        if file_format not in ['netcdf']:
            raise ValueError(f'Cannot write a dataset to a {file_format} file.')
        
    elif isinstance(data, str):
        if file_format not in ['txt', 'file']:
            raise ValueError(f'Cannot write a string to a {file_format} file.')
        
    elif isinstance(data, dict):
        if file_format not in ['json']:
            raise ValueError(f'Cannot write a dictionary to a {file_format} file.')
        
    elif 'gpd' in globals() and isinstance(data, gpd.GeoDataFrame):
        if file_format not in ['shp', 'json']:
            raise ValueError(f'Cannot write a geopandas dataframe to a {file_format} file.')
                
    elif 'pd' in globals() and isinstance(data, pd.DataFrame):
        if file_format not in ['csv']:
            raise ValueError(f'Cannot write a pandas dataframe to a {file_format} file.')
    
    elif format not in 'file':
        raise ValueError(f'Cannot write a {type(data)} to a {file_format} file.')

def get_zip_from_path(path: str) -> (str, None):
    # get the zip extension
    extension = path.split('.')[-1]

    # check if the file is a ggip
    if extension == 'gz':
        return 'gzip'
    else:
        return None

def get_format_from_path(path: str) -> str:
    # get the file extension
    extension = path.split('.')[-1]

    # check if the file is a csv
    if extension == 'csv':
        return 'csv'

    # check if the file is a geotiff
    elif extension == 'tif' or extension == 'tiff':
        return 'geotiff'

    # check if the file is a netcdf
    elif extension == 'nc':
        return 'netcdf'
    
    elif extension in ['json', 'geojson']:
        return 'json'
    
    elif extension == 'txt':
        return 'txt'
    
    elif extension == 'shp':
        return 'shp'
    
    elif extension in ['png', 'pdf']:
        return 'file'

    raise ValueError(f'File format not supported: {extension}')

def read_from_file(
        path, file_format: Optional[str] = None, file_mode: Optional[str] = None) \
        -> (xr.DataArray, xr.Dataset, pd.DataFrame):

    if file_format is None:
        file_format = get_format_from_path(path)

    # read the data from a csv
    if file_format == 'csv':
        data = pd.read_csv(path)

    # read the data from a json
    elif file_format == 'json':
        with open(path, 'r') as f:
            data = json.load(f)
            # understand if the data is actually in a geodataframe format
            if isinstance(data, dict) and 'features' in data.keys():
                data = gpd.read_file(path)

    # read the data from a txt file
    elif file_format == 'txt':
        if file_mode == 'grid':
            data = rxr.open_rasterio(path)

            squeeze_dims = [dim for dim in data.dims if data[dim].size == 1]
            if len(squeeze_dims) > 0:
                data = data.squeeze(squeeze_dims)

        elif file_mode == 'time_series':
            with open(path, 'r') as f:
                data = f.readlines()
        else:
            with open(path, 'r') as f:
                data = f.readlines()

    # read the data from a shapefile
    elif file_format == 'shp':
        data:gpd.GeoDataFrame = gpd.read_file(path)

    # read the data from a geotiff
    elif file_format == 'geotiff':
        data = rxr.open_rasterio(path)

        if 'band' in list(data.dims):
            if data.band.size == 1:
                data = data.squeeze('band', drop = True)

    # read the data from a netcdf
    elif file_format == 'netcdf':
        data = xr.open_dataset(path)
        # check if there is a single variable in the dataset
        if len(data.data_vars) == 1:
            data = data[list(data.data_vars)[0]]

    # read the data from a png or pdf
    elif file_format == 'file':
        data = path

    else:
        raise ValueError(f'File format not supported: {file_format}')

    return data

def write_to_file(data, path, file_format: Optional[str] = None,
                  file_mode: Optional[str] = None, file_type: Optional[str] = None,
                  append = False, **kwargs) -> None:

    time = None
    if 'time' in kwargs:
        time = kwargs.pop('time')

    if file_format is None:
        file_format = get_format_from_path(path)

    dir = os.path.dirname(path)
    if len(dir) > 0:
        os.makedirs(os.path.dirname(path), exist_ok = True)
    if not os.path.exists(path):
        append = False

    # write the data to a csv
    if file_format == 'csv':
        if append:
            data.to_csv(path, mode = 'a', header = False)
        else:
            data.to_csv(path)

    # write the data to a json
    elif file_format == 'json':
        # write a dictionary to a json
        if isinstance(data, dict):
            for key in data.keys():
                if isinstance(data[key], np.ndarray):
                    data[key] = data[key].tolist
                elif isinstance(data[key], dt.datetime):
                    data[key] = data[key].isoformat()
            if append:
                with open(path, 'r') as f:
                    old_data = json.load(f)
                old_data = [old_data] if not isinstance(old_data, list) else old_data
                old_data.append(data)
                data = old_data
            with open(path, 'w') as f:
                json.dump(data, f, indent = 4)
        # write a geodataframe to a json
        elif isinstance(data, gpd.GeoDataFrame):
            data.to_file(path, driver = 'GeoJSON')

    # write a geo dataframe to a shapefile
    elif file_format == 'shp':
        data.to_file(path)

    elif file_format == 'txt':
        if append:
            with open(path, 'a') as f:
                f.writelines(data)
        else:
            with open(path, 'w') as f:
                f.writelines(data)

    # write the data to a geotiff
    elif file_format == 'geotiff':
        data.rio.to_raster(path, compress = 'LZW')

    # write the data to a netcdf
    elif file_format == 'netcdf':

        if file_type == 'hmc':
            write_file_nc_hmc(path=path, data=data, time=time, attrs_data=None, **kwargs)
        elif file_type == 's3m':
            data.to_netcdf(path, format = 'NETCDF4', engine = 'netcdf4')
        else:
            data.to_netcdf(path, format = 'NETCDF4', engine = 'netcdf4')

    # write the data to a png or pdf (i.e. move the file)
    elif file_format == 'file':
        os.rename(data, path)

def rm_file(path) -> None:
    os.remove(path)

# DECORATOR TO MAKE THE FUNCTION BELOW WORK WITH XR.DATASET
def withxrds(func):
    def wrapper(*args, **kwargs):
        if isinstance(args[0], xr.Dataset):
            obj_fx = xr.Dataset()
            for var in args[0]:
                tmp_fx = func(args[0][var], **kwargs)
                if hasattr(tmp_fx, 'name'):
                    var = tmp_fx.name
                obj_fx[var] = tmp_fx
            return obj_fx
        else:
            return func(*args, **kwargs)
    return wrapper

def with_dict(func):
    def wrapper(*args, **kwargs):
        if isinstance(args[0], dict):
            return dict({key: func(data, **kwargs) for key, data in args[0].items})
        else:
            return func(*args, **kwargs)
    return wrapper

## FUNCTIONS TO CLEAN DATA
# flat dims
@withxrds
def flat_dims(data: xr.DataArray, dim_x: str = 'longitude', dim_y: str = 'latitude'):

    x, y = data[dim_x].values, data[dim_y].values
    if x.ndim == 2:
        x = x[0, :]
        data = data.drop_vars([dim_x])
        data[dim_x] = xr.DataArray(x, dims=dim_x)
    if y.ndim == 2:
        y = y[:, 0]
        data = data.drop_vars([dim_y])
        data[dim_y] = xr.DataArray(y, dims=dim_y)

    return data

# method to map dims
@withxrds
def map_dims(data: xr.DataArray, dims_geo: dict= None, **kwargs) -> xr.DataArray:
    if dims_geo is not None:
        for dim_in, dim_out in dims_geo.items():
            if dim_in in list(data.dims):
                data = data.rename({dim_in: 'var_tmp'})
                data = data.rename({'var_tmp': dim_out})
    return data

# method to map vars
@withxrds
def map_vars(data: xr.DataArray, vars_data: dict = None,  **kwargs) -> xr.DataArray:
    if vars_data is not None:
        for var_in, var_out in vars_data.items():
            if var_in == data.name:
                data.name = var_out
            elif var_out == data.name:
                data.name = var_in
    return data

@withxrds
def select_by_time(data: xr.DataArray, time: (str, pd.Timestamp, None) = None,
                   method='nearest', **kwargs) -> xr.DataArray:
    if time is not None:
        if isinstance(time, pd.Timestamp):
            time = convert_time_format(time, time_conversion='stamp_to_str')

        if 'time' in list(data.dims):
            data = data.sel(time = time, method=method)
    return data

# ensure that time are defined by dates and not by numbers
@withxrds
def straighten_time(data: xr.DataArray, time_file: pd.Timestamp = None,
                    time_direction: str = 'left',
                    time_dim: str = 'time', time_freq: str ='h') -> xr.DataArray:

    if time_dim in list(data.dims):
        time_values = data[time_dim].values
        time_check = is_date(time_values[0])
        if not time_check:
            if time_file is not None:

                if time_direction == 'forward':
                    time_values = pd.date_range(start=time_file, periods=len(time_values), freq=time_freq)
                elif time_direction == 'backward':
                    time_values = pd.date_range(end=time_file, periods=len(time_values), freq=time_freq)
                else:
                    raise ValueError(f'Time direction {time_direction} not recognized.')

                data[time_dim] = time_values
            else:
                warnings.warn(f'Time values are not defined by dates. Time of the file is defined by NoneType.')

    return data

@withxrds
def straighten_data(data: xr.DataArray, dim_x: str = 'longitude', dim_y: str = 'latitude') -> xr.DataArray:
    """
    Ensure that the data has descending latitudes.
    """

    data_dims = list(data.dims)
    if 'x' in data_dims:
        tmp_x = 'x'
    else:
        tmp_x = dim_x
    if 'y' in data_dims:
        tmp_y = 'y'
    else:
        tmp_y = dim_y

    x_idx, y_idx = data_dims.index(tmp_x), data_dims.index(tmp_y)
    if x_idx < y_idx:
        if len(data_dims) == 3:
            tmp_z = data_dims[0]
            data = data.transpose(tmp_z, tmp_y, tmp_x)
        elif len(data_dims) == 2:
            data = data.transpose(tmp_y, tmp_x)
        else:
            raise ValueError(f'Cannot transpose data with {len(data_dims)} dimensions.')

    # adjust lats
    if data[tmp_y].data[0] < data[tmp_y].data[-1]:
        data = data.sortby(tmp_y, ascending = False)
    # adjust lons
    x_max = np.nanmax(data.coords[tmp_x])
    if x_max > 180:
        data.coords[tmp_x] = (data.coords[tmp_x] + 180) % 360 - 180
        data = data.sortby(data[tmp_x])

    if tmp_x != dim_x:
        data = data.rename({tmp_x: dim_x})
    if tmp_y != dim_y:
        data = data.rename({tmp_y: dim_y})

    return data

@withxrds
def reset_nan(data: xr.DataArray, nan_value = None) -> xr.DataArray:
    """
    Make sure that the nodata value is set to np.nan for floats and to the maximum integer for integers.
    """

    fill_value = data.attrs.get('_FillValue', None)
    if nan_value is None:
        data_type = data.dtype
        new_fill_value = np.nan if np.issubdtype(data_type, np.floating) else np.iinfo(data_type).max
    else:
        new_fill_value = nan_value

    if fill_value is None:
        data.attrs['_FillValue'] = new_fill_value
    elif not np.isclose(fill_value, new_fill_value, equal_nan = True):
        data = data.where(~np.isclose(data, fill_value, equal_nan = True), new_fill_value)
        data.attrs['_FillValue'] = new_fill_value

    return data

@withxrds
def set_type(data: xr.DataArray, nan_value = None) -> xr.DataArray:
    """
    Make sure that the data is the smallest possible.
    """

    max_value = data.max()
    min_value = data.min()

    # check if output contains floats or integers
    if np.issubdtype(data.dtype, np.floating):
        if max_value < 2**31 and min_value > -2**31:
            data = data.astype(np.float32)
        else:
            data = data.astype(np.float64)
    elif np.issubdtype(data.dtype, np.integer):
        
        if min_value >= 0:
            if max_value <= 255:
                data = data.astype(np.uint8)
            elif max_value <= 65535:
                data = data.astype(np.uint16)
            elif max_value < 2**31:
                data = data.astype(np.uint32)
            else:
                data = data.astype(np.uint64)
                
            if nan_value is not None and not np.issubdtype(data.dtype, np.unsignedinteger):
                nan_value = None
        else:
            if max_value <= 127 and min_value >= -128:
                data = data.astype(np.int8)
            elif max_value <= 32767 and min_value >= -32768:
                data = data.astype(np.int16)
            elif max_value < 2**31 and min_value > -2**31:
                data = data.astype(np.int32)
            else:
                data = data.astype(np.int64)

            if nan_value is not None and not np.issubdtype(data.dtype, np.integer):
                nan_value = None

    return reset_nan(data, nan_value)
