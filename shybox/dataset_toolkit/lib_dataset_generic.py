"""
Library Features:

Name:          lib_dataset_generic
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20251029'
Version:       '1.1.0'
"""
# ----------------------------------------------------------------------------------------------------------------------
# libraries
from __future__ import annotations

import logging
logging.getLogger("findlibs").setLevel(logging.WARNING)
logging.getLogger("gribapi.bindings").setLevel(logging.WARNING)
logging.getLogger("cfgrib").setLevel(logging.ERROR)

import warnings
import os
import json
import datetime as dt

import xarray as xr
import numpy as np
import pandas as pd

import rasterio as rio
import rioxarray as rxr

try:
    import geopandas as gpd
except ImportError:
    pass

from decimal import Decimal
from typing import Optional, Dict

from shybox.io_toolkit.lib_io_ascii_hmc import read_sections_db, read_sections_data, read_sections_registry
from shybox.io_toolkit.lib_io_gzip import uncompress_and_remove
from shybox.io_toolkit.lib_io_nc_s3m import write_dataset_s3m
from shybox.io_toolkit.lib_io_nc_hmc import write_dataset_hmc, write_ts_hmc
from shybox.io_toolkit.lib_io_nc_other import write_dataset_itwater
from shybox.generic_toolkit.lib_utils_file import has_compression_extension
from shybox.time_toolkit.lib_utils_time import is_date
from shybox.logging_toolkit.lib_logging_utils import with_logger

# manage logger
try:
    from shybox.logging_toolkit.lib_logging_utils import with_logger
except Exception as e:
    from shybox.default.lib_default_log import logger_default
    logger_stream = logger_default(__name__)
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to check data format
def check_data_format(data, file_format: str) -> None:
    """"
    Ensures that the data is compatible with the format of the dataset.
    """
    # add possibility to write a geopandas dataframe to a geojson or a shapefile
    if isinstance(data, np.ndarray) or isinstance(data, xr.DataArray):
        if not file_format in ['geotiff', 'netcdf']:
            raise ValueError(f'Cannot write matrix data to a {file_format} file.')

    elif isinstance(data, xr.Dataset):
        if file_format not in ['netcdf', 'geotiff']:
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
        if file_format not in ['csv', 'netcdf']:
            raise ValueError(f'Cannot write a pandas dataframe to a {file_format} file.')
    
    elif format not in 'file':
        raise ValueError(f'Cannot write a {type(data)} to a {file_format} file.')
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to get zip from path
def get_zip_from_path(path: str) -> (str, None):
    # get the zip extension
    extension = path.split('.')[-1]

    # check if the file is a ggip
    if extension == 'gz':
        return 'gzip'
    else:
        return None
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to get format from path
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
    elif extension in ['nc', 'nc4', 'netcdf']:
        return 'netcdf'

    # check if the file is a grib
    elif extension in ['grib', 'grb', 'grb2']:
        return 'grib'
    
    elif extension in ['json', 'geojson']:
        return 'json'
    
    elif extension == 'txt':
        return 'txt'
    
    elif extension == 'shp':
        return 'shp'
    
    elif extension in ['png', 'pdf']:
        return 'file'

    elif extension in ['tmp', 'temp']:
        return 'tmp'

    raise ValueError(f'File format not supported: {extension}')
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to read from file
def read_from_file(
        path, file_format: Optional[str] = None,
        file_type: Optional[str] = None, file_variable: (str, list) = 'na') \
        -> (xr.DataArray, xr.Dataset, pd.DataFrame):

    # add suppress warnings
    rxr_logger = logging.getLogger('rioxarray')
    rxr_logger.setLevel(logging.ERROR)
    rio_logger = logging.getLogger('rasterio')
    rio_logger.setLevel(logging.ERROR)

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
    elif file_format in ['txt', 'ascii']:
        if file_type in ['grid', 'grid_2d']:

            # get header
            geo_attrs = {}
            with open(path, 'r') as file:
                # Read the first six lines
                geo_lines = [next(file) for _ in range(6)]

            # Parse the header lines
            for line in geo_lines:
                if line.startswith('xllcorner'):
                    geo_attrs['xllcorner'] = Decimal(line.split()[1])
                elif line.startswith('yllcorner'):
                    geo_attrs['yllcorner'] = Decimal(line.split()[1])
                elif line.startswith('cellsize'):
                    geo_attrs['cellsize'] = Decimal(line.split()[1])
                elif line.startswith('NODATA_value'):
                    geo_attrs['NODATA_value'] = float(line.split()[1])
                elif line.startswith('ncols'):
                    geo_attrs['ncols']  = int(line.split()[1])
                elif line.startswith('nrows'):
                    geo_attrs['nrows']  = int(line.split()[1])

            # get data
            data = rxr.open_rasterio(path)
            generic_attrs = data.attrs

            # store attributes
            data.attrs = {**generic_attrs, **geo_attrs}

            squeeze_dims = [dim for dim in data.dims if data[dim].size == 1]
            if len(squeeze_dims) > 0:
                data = data.squeeze(squeeze_dims)

        elif file_type == 'points_section_db':

            # read section database (flood-proofs format)
            data = read_sections_db(file_path=path)

        elif file_type == 'points_section_hmc':

            # read section registry (hmc format)
            data = read_sections_registry(filepath=path)

        elif file_type == 'time_series_hmc':

            # read time series data (hmc format)
            data = read_sections_data(path=path)

        elif file_type == 'points' or file_type == 'points_generic':

            # read generic points file
            with open(path, 'r') as f:
                data = f.readlines()

        else:
            with open(path, 'r') as f:
                data = f.readlines()

    # read the data from a shapefile
    elif file_format == 'shp':
        data:gpd.GeoDataFrame = gpd.read_file(path)

    # read the data from a geotiff (and similar formats)
    elif file_format in ['tiff', 'geotiff', 'tif']:

        if file_variable is not None:
            if not file_variable:
                file_variable = 'variable'

        # data = rxr.open_rasterio(path) # if no epsg are provided ... we have to set a geo template
        with rio.open(path) as src:
            # Read band with mask
            tmp = src.read(1, masked=True)

            # Extract transform
            transform = src.transform

            # Build coordinate arrays correctly
            # Note: transform * (col, row) gives (x, y)
            # But you can also compute linearly using transform coefficients:
            x = transform[2] + np.arange(src.width) * transform[0]
            y = transform[5] + np.arange(src.height) * transform[4]

            # Flip y if descending (common in rasters)
            if y[1] < y[0]:
                y = y[::-1]
                tmp = tmp[::-1, :]

            # If masked, convert to np.nan (optional)
            if np.ma.isMaskedArray(tmp):
                data_values = tmp.filled(np.nan)
            else:
                data_values = tmp

            # Create DataArray
            data = xr.DataArray(
                data_values,
                dims=("latitude", "longitude"),
                coords={"latitude": y, "longitude": x},
                name=file_variable
            )

        if 'band' in list(data.dims):
            if data.band.size == 1:
                data = data.squeeze('band', drop = True)

        if isinstance(data, xr.DataArray):
            if file_variable is not None:
                if isinstance(file_variable, list):
                    file_variable = file_variable[0]
                data.name = file_variable

    # read the data from a netcdf (and similar formats)
    elif file_format in ['netcdf', 'nc', 'nc4']:

        has_compression = has_compression_extension(path)

        if has_compression:
            file = uncompress_and_remove(path)
        else:
            file = path

        data = xr.open_dataset(file)
        # check if there is a single variable in the dataset
        if len(data.data_vars) == 1:
            data = data[list(data.data_vars)[0]]

        if has_compression:
            if os.path.exists(file):
                os.remove(file)

    elif file_format == 'grib':

        has_compression = has_compression_extension(path)

        if has_compression:
            file = uncompress_and_remove(path)
        else:
            file = path

        # read the grib file
        data = xr.open_dataset(file, engine="cfgrib")

        # 1) Preserve the original reference time
        #    (rename 'time' -> 'time_start' to keep it)
        if "time" not in data.coords:
            raise ValueError("Dataset has no 'time' coordinate.")
        data = data.rename({"time": "time_start"})

        # 2) Sanity check
        if "step" not in data.coords and "step" not in data.dims:
            raise ValueError("Dataset has no 'step' coordinate/dimension from cfgrib.")

        # 3) Compute valid timestamps and assign them back onto the 'step' coordinate
        #    (this keeps the dimension name 'step' but makes its values datetimes)
        valid_time = data["time_start"] + data["step"]
        data = data.assign_coords(step=valid_time)
        # 4) Sort by the new datetime 'step' (handy if steps weren't monotonic)
        data = data.sortby("step")

        # 5) Optional: prove it's now a DatetimeIndex
        assert np.issubdtype(data["step"].dtype, np.datetime64), "step is not datetime64 after conversion"

        # check if there is a single variable in the dataset
        if len(data.data_vars) == 1:
            data = data[list(data.data_vars)[0]]

        if has_compression:
            if os.path.exists(file):
                os.remove(file)

    # read the data from a png or pdf
    elif file_format == 'file':
        data = path

    elif file_format == 'tmp':
        data = path

    else:
        raise ValueError(f'File format not supported: {file_format}')

    return data
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to write to file
def write_to_file(data, path,
                  file_format: Optional[str] = None, file_type: Optional[str] = None,
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

        if file_type in ['grid_hmc', 'updating_hmc', 'forcing_hmc']:
            write_dataset_hmc(path=path, data=data, time=time, attrs_data=None, **kwargs)
        elif file_type in ['time_series_hmc', 'ts_hmc']:
            write_ts_hmc(file_name=path, ts=data, time=time, attrs_data=None, **kwargs)
        elif file_type in ['grid_s3m', 'forcing_s3m']:
            write_dataset_s3m(path=path, data=data, time=time, attrs_data=None, **kwargs)
        elif file_type in ['itwater', 'it_water']:
            write_dataset_itwater(path=path, data=data, time=time, attrs_data=None, **kwargs)
        else:
            data.to_netcdf(path, format = 'NETCDF4', engine = 'netcdf4')

    # write the data to a png or pdf (i.e. move the file)
    elif file_format == 'file':
        os.rename(data, path)

def rm_file(path) -> None:
    os.remove(path)
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# DECORATOR TO MAKE THE FUNCTION BELOW WORK WITH XR.DATASET
def withxrds(func):
    def wrapper(*args, **kwargs):
        if isinstance(args[0], xr.Dataset):
            obj_fx = None
            for var in args[0]:
                tmp_fx = func(args[0][var], **kwargs)

                if isinstance(tmp_fx, xr.DataArray):
                    if obj_fx is None:
                        obj_fx = xr.Dataset()
                    if hasattr(tmp_fx, 'name'):
                        var = tmp_fx.name
                elif isinstance(tmp_fx, str):
                    if obj_fx is None:
                        obj_fx = []
                elif tmp_fx is None:
                    pass

                else:
                    logger_stream.error(f'Expected tmp_fx is not a xr.DataArray or a str. Got {type(tmp_fx)} instead.')
                    raise ValueError(f'Expected tmp_fx is not a xr.DataArray or a str. Got {type(tmp_fx)} instead.')

                if isinstance(obj_fx, xr.Dataset):
                    if tmp_fx is not None:
                        obj_fx[var] = tmp_fx
                elif isinstance(obj_fx, list):
                    if tmp_fx is not None:
                        obj_fx.append(tmp_fx)
                elif obj_fx is None:
                    pass
                else:
                    logger_stream.error('Expected obj_fx is not a xr.Dataset or a str.')
                    raise ValueError(f'Expected obj_fx is not a xr.Dataset or a str. Got {type(obj_fx)} instead.')
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
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
## FUNCTIONS TO CLEAN DATA
@withxrds
def straighten_dims(data: xr.DataArray) -> (xr.DataArray, None):
    if data.dims == ():
        return None
    else:
        return data

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

# method to map coords
@withxrds
def map_coords(data: xr.DataArray, coords_geo: dict = None, **kwargs) -> xr.DataArray:

    if coords_geo is not None:
        for coords_in, coords_out in coords_geo.items():
            if coords_in in list(data.coords):
                data = data.rename({coords_in: 'var_tmp'})
                crds = data['var_tmp'].values

                if crds.ndim == 2:

                    crds_ax1, crds_ax2 = crds[0, :], crds[:, 0]
                    check_ax1, check_ax2 = np.unique(crds_ax1).size == 1, np.unique(crds_ax2).size == 1

                    if not check_ax1 and check_ax2:
                        crds = crds_ax1
                    elif check_ax1 and not check_ax2:
                        crds = crds_ax2
                    else:
                        raise ValueError(f'Cannot map coordinates {coords_in} to {coords_out}.')

                    data['var_tmp'] = xr.DataArray(crds, dims=coords_out)

                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    data = data.rename({'var_tmp': coords_out})

    return data

# method to map vars
@withxrds
def map_vars(
    data: xr.DataArray,
    vars_data: Optional[Dict[str, str]] = None,
    **kwargs
) -> xr.DataArray | None:
    """
    Rename DataArray.name based on vars_data.

    Behavior (matches original intent):
    - If vars_data is None -> return data unchanged.
    - If data.name is None -> set to the first value in vars_data (insertion order) and return data.
    - If data.name matches a key in vars_data -> rename to its value and return data.
    - If data.name matches a value in vars_data -> rename back to its key and return data.
    - Otherwise -> return None.

    Note: mutation is in-place (like the original).
    """
    if vars_data is None:
        return data

    # If name is missing, set it to the first mapping value (if any)
    if data.name is None:
        try:
            data.name = next(iter(vars_data.values()))
            return data
        except StopIteration:
            # Empty dict: nothing to do
            return None

    name = data.name

    # Forward mapping: var_in -> var_out
    if name in vars_data:
        data.name = vars_data[name]
        return data

    # Reverse mapping: var_out -> var_in
    for var_in, var_out in vars_data.items():
        if var_out == name:
            data.name = var_in
            return data

    # No rule matched
    return None


@withxrds
def select_by_vars(data: xr.DataArray, vars_data: dict = None, **kwargs) -> (xr.DataArray, None):
    if vars_data is not None:
        for var_in, var_out in vars_data.items():
            var_file = data.name
            if var_file is not None:
                if var_in == var_file or var_out == var_file:
                    return data
                else:
                    return None
            else:
                return data
    return data

# method to select time
@withxrds
def select_by_time(
    da: xr.DataArray,
    when,
    *,
    tolerance: str | pd.Timedelta = "1H",
    active: bool = False
):
    """
    Select a time from a DataArray with optional tolerance logic.

    Parameters
    ----------
    da : xr.DataArray
        Must have a 'time' coordinate.
    when : str | pandas.Timestamp | numpy.datetime64
        Target timestamp.
    tolerance : str | pandas.Timedelta, default "1H"
        Max time difference allowed when no exact match is found.
    active : bool, default False
        If False: choose the previous time within tolerance (default behaviour).
        If True: choose the nearest time within tolerance (any side).

    Returns
    -------
    xr.DataArray | None
        The selected data (0D) or None if no valid time found.
    """

    # if no time selection is required
    if when is None:
        return da
    # if coords time is not in the data array
    if "time" not in da.coords:
        raise ValueError("DataArray must have a 'time' coordinate.")

    ts = pd.to_datetime(when)
    times = pd.to_datetime(da.time.values)
    tol = pd.to_timedelta(tolerance)

    # if exact match exists
    if ts in times:
        return da.sel(time=ts)

    # find candidate times within tolerance
    diffs = times - ts
    diffs_abs = abs(diffs)

    if active:
        # select nearest time (before or after) within tolerance
        idx = diffs_abs.argmin()
        if diffs_abs[idx] <= tol:
            return da.isel(time=idx)
    else:
        # select previous time (before only)
        mask_prev = diffs <= pd.Timedelta(0)
        if not mask_prev.any():
            return None
        diffs_prev = diffs_abs.where(mask_prev, pd.NaT)
        idx = diffs_prev.argmin()
        if pd.notna(diffs_prev[idx]) and diffs_prev[idx] <= tol:
            return da.isel(time=idx)

    return None
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
def straighten_data(
    data: xr.DataArray,
    dim_x: str = "longitude", dim_y: str = "latitude",
    coord_x: str = "longitude", coord_y: str = "latitude"
) -> xr.DataArray:

    da = data

    # Pick spatial dims if generic 'x'/'y' exist
    data_dims = list(da.dims)
    tmp_x = "x" if "x" in data_dims else dim_x
    tmp_y = "y" if "y" in data_dims else dim_y

    # If auxiliary coord names are present, map them onto the spatial dims
    if coord_x in da.coords and tmp_x in da.dims:
        da = da.assign_coords({tmp_x: da.coords[coord_x]})
    if coord_y in da.coords and tmp_y in da.dims:
        da = da.assign_coords({tmp_y: da.coords[coord_y]})

    # Ensure the x/y coordinates exist; if not, create simple index coords
    if tmp_x in da.dims and tmp_x not in da.coords:
        da = da.assign_coords({tmp_x: xr.DataArray(np.arange(da.sizes[tmp_x]), dims=tmp_x)})
    if tmp_y in da.dims and tmp_y not in da.coords:
        da = da.assign_coords({tmp_y: xr.DataArray(np.arange(da.sizes[tmp_y]), dims=tmp_y)})

    # Collapse 2-D rectilinear coords to 1-D when possible
    if tmp_x in da.coords:
        x_coord = da.coords[tmp_x]
        if x_coord.ndim == 2:
            # assume rectilinear: rows identical
            da = da.assign_coords({tmp_x: xr.DataArray(x_coord.isel({tmp_y: 0}).drop_vars([v for v in x_coord.coords]) if hasattr(x_coord, "coords") else x_coord[0, :], dims=tmp_x)})
    if tmp_y in da.coords:
        y_coord = da.coords[tmp_y]
        if y_coord.ndim == 2:
            # assume rectilinear: cols identical
            da = da.assign_coords({tmp_y: xr.DataArray(y_coord.isel({tmp_x: 0}).drop_vars([v for v in y_coord.coords]) if hasattr(y_coord, "coords") else y_coord[:, 0], dims=tmp_y)})

    # Reorder dims so that ... , tmp_y, tmp_x (keep other dims order)
    dims = list(da.dims)
    if tmp_x in dims and tmp_y in dims:
        others = [d for d in dims if d not in (tmp_y, tmp_x)]
        da = da.transpose(*others, tmp_y, tmp_x)

    # Ensure latitude (tmp_y) is descending
    if tmp_y in da.coords:
        yvals = da.coords[tmp_y].values
        if yvals[0] < yvals[-1]:
            da = da.sortby(tmp_y, ascending=False)

    # Normalize longitudes to [-180, 180] if they look like [0, 360]
    if tmp_x in da.coords:
        xvals = da.coords[tmp_x].values
        x_min = np.nanmin(xvals)
        x_max = np.nanmax(xvals)
        # Heuristic: if mostly in [0, 360] and spans > 180°, shift
        if (x_max > 180) and (x_min >= 0) and (x_max - x_min >= 180):
            new_x = (da.coords[tmp_x] + 180) % 360 - 180
            da = da.assign_coords({tmp_x: new_x})
            da = da.sortby(tmp_x)

    # Finally, rename dims back to user-requested names if we used 'x'/'y'
    if "x" in data_dims and dim_x != "x":
        da = da.rename({"x": dim_x})
    if "y" in data_dims and dim_y != "y":
        da = da.rename({"y": dim_y})

    return da

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

# method to select variable
@withxrds
def select_variable(data: xr.DataArray, vars_data: dict = None, **kwargs) -> (str, None):

    if vars_data is not None:
        var_select = None
        for var_in, var_out in vars_data.items():
            var_data = data.name
            if var_data == var_in or var_data == var_out:
                var_select = var_data
                break
    else:
        var_select = data.name

    return var_select

# method to select time
@withxrds
def select_by_time(
    da: xr.DataArray,
    when,
    *,
    tolerance: str | pd.Timedelta = "1H",
    active: bool = False
):
    """
    Select a time from a DataArray with optional tolerance logic.

    Parameters
    ----------
    da : xr.DataArray
        Must have a 'time' coordinate.
    when : str | pandas.Timestamp | numpy.datetime64
        Target timestamp.
    tolerance : str | pandas.Timedelta, default "1H"
        Max time difference allowed when no exact match is found.
    active : bool, default False
        If False: choose the previous time within tolerance (default behaviour).
        If True: choose the nearest time within tolerance (any side).

    Returns
    -------
    xr.DataArray | None
        The selected data (0D) or None if no valid time found.
    """
    # if no time selection is required
    if when is None:
        return da
    # return the data if are 2d
    if len(da.dims) == 2 and when is not None:
        return da

    # if variable time is not in the data array
    if "time" not in da.coords:
        raise ValueError("DataArray must have a 'time' coordinate.")

    ts = pd.to_datetime(when)
    times = pd.to_datetime(da.time.values)
    tol = pd.to_timedelta(tolerance.lower())

    # if exact match exists
    if ts in times:
        return da.sel(time=ts)

    # find candidate times within tolerance
    diffs = times - ts
    diffs_abs = abs(diffs)

    if active:
        # select nearest time (before or after) within tolerance
        idx = diffs_abs.argmin()
        if diffs_abs[idx] <= tol:
            return da.isel(time=idx)
    else:
        # select previous time (before only)
        mask_prev = diffs <= pd.Timedelta(0)
        if not mask_prev.any():
            return None
        diffs_prev = diffs_abs.where(mask_prev, pd.NaT)
        idx = diffs_prev.argmin()
        if pd.notna(diffs_prev[idx]) and diffs_prev[idx] <= tol:
            return da.isel(time=idx)

    return None

# method to select da by mapping (mapping_str = variable:workflow)
@with_logger(var_name="logger_stream")
@withxrds
def select_da_by_mapping(data: xr.DataArray,
    mapping_str: str = None, mapping_use: str ="value", mapping_delimiter: str =":"
):

    # No mapping provided → return unchanged
    if mapping_str is None:
        return data

    # Parse mapping string into key/value
    if mapping_delimiter in mapping_str:
        key, value = mapping_str.split(mapping_delimiter, 1)
    else:
        key, value = mapping_str, ""

    # Normalize key/value to stripped strings or None
    key = key.strip() if isinstance(key, str) and key.strip() else None
    value = value.strip() if isinstance(value, str) and value.strip() else None

    orig_name = data.name  # may be None

    def _no_mapping():
        """Handle the 'mapping does not apply' case."""
        if orig_name is not None:
            # DataArray already has a name → signal "no match" by returning None
            return None
        # DataArray has no name → keep it but warn
        logger_stream.warning(
            f"DataArray has no name; mapping (key={key!r}, value={value!r}) "
            f"does not apply. Returning original DataArray unchanged."
        )
        return data

    # mapping_use == "key": use key as target name, but only if it matches
    # or DataArray is unnamed.
    if mapping_use == "key":
        if not key:
            return _no_mapping()

        # If DA already has a different name, mapping does not apply
        if orig_name is not None and orig_name != key:
            return _no_mapping()

        # Either unnamed or already matching → rename to key (idempotent if equal)
        return data.rename(key)

    # mapping_use == "value": same logic, but with 'value'
    elif mapping_use == "value":
        if not value:
            return _no_mapping()

        if orig_name is not None and orig_name != value:
            return _no_mapping()

        return data.rename(value)

    elif mapping_use == "auto":
        # Prefer value
        if value:
            if orig_name is None or orig_name == value:
                return data.rename(value)
        # Fall back to key
        if key:
            if orig_name is None or orig_name == key:
                return data.rename(key)
        # Neither key nor value applicable
        return _no_mapping()

    else:
        logger_stream.error(f'Invalid mapping_use: {mapping_use!r}')
        raise ValueError(f"Invalid mapping_use: {mapping_use!r}")

# method to rename da variables use variable template
@with_logger(var_name="logger_stream")
@withxrds
def rename_da_by_template(
    da: xr.DataArray,
    variable_template: dict = None,
    sep: str =":",
    use_value: bool =False,       # False => rename to KEY (default), True => rename to VALUE
    case_sensitive: bool =True,
    allow_tagless: bool = True,  **kwargs  # if no sep, treat whole name as the tag
):
    """
    Rename a single xarray.DataArray based on template['vars_data'] and a '<prefix><sep><tag>' name.

    Matching:
      - Extract <tag> as the substring after the last `sep`. If no sep and allow_tagless=True, use the full name.
      - Look up <tag> in template['vars_data'] keys (case-sensitive or not).
      - If matched:
          - new name = matched KEY (default) or VALUE if use_value=True.
        If not matched:
          - return unchanged, with status='missing'.

    Returns
    -------
    da_new : xarray.DataArray
    info : dict  # details for logging/debug
    """
    vars_data = dict(variable_template.get("vars_data", {}))
    if not isinstance(getattr(da, "name", None), (str, type(None))):
        logger_stream.error("DataArray must have a string (or None) name.")
        raise TypeError("DataArray must have a string (or None) name.")

    # Build normalized lookup (preserve canonical key for output)
    def norm(s): return s if case_sensitive else s.lower()
    key_index = {norm(k): k for k in vars_data.keys()}     # normalized -> canonical key
    val_index = {norm(k): v for k, v in vars_data.items()} # normalized -> value

    old_name = da.name or ""
    # derive tag
    if sep in old_name:
        tag = old_name.rsplit(sep, 1)[-1]
    else:
        if not allow_tagless:
            return da, {"status": "missing_sep", "old_name": old_name, "tag": None}
        tag = old_name

    ntag = norm(tag)
    if ntag not in key_index:
        return da, {"status": "missing", "old_name": old_name, "tag": tag}

    canonical_key = key_index[ntag]
    target_name = val_index[ntag] if use_value else canonical_key

    if target_name == old_name:
        info = {"status": "no_op", "old_name": old_name, "new_name": target_name}
        return da

    da_new = da.rename(target_name)
    info_new = {
        "status": "renamed", "old_name": old_name, "tag": tag,
        "matched_key": canonical_key, "new_name": target_name,
        "mode": "value" if use_value else "key",
        "case_sensitive": case_sensitive}

    return da_new

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
# ----------------------------------------------------------------------------------------------------------------------
