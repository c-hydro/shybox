"""
Library Features:

Name:          lib_io_nc_generic
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20260113'
Version:       '1.1.0'
"""
# ----------------------------------------------------------------------------------------------------------------------
# libraries
import rasterio
import xarray as xr
import numpy as np

from rasterio.crs import CRS

from shybox.logging_toolkit.lib_logging_utils import with_logger
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to read netcdf file
@with_logger(var_name='logger_stream')
def get_file_grid(file_name: str,
                  file_epsg: str = 'EPSG:4326', file_crs: CRS = None,
                  file_transform: rasterio.transform = None,
                  file_map_dims: dict = None, file_map_geo: dict = None, file_map_data: dict = None,
                  **kwargs):

    file_obj = xr.open_dataset(file_name)

    if file_map_geo is not None:
        file_obj = __adjust_geo_naming(file_obj, file_map_geo=file_map_geo)

    file_x_values, file_y_values = file_obj['longitude'].values, file_obj['latitude'].values
    file_obj = file_obj.drop_vars(['longitude', 'latitude'])
    if file_x_values.ndim == 2:
        file_x_values = file_x_values[0, :]
    elif file_x_values.ndim == 1:
        pass
    else:
        logger_stream.error('Geographical dimensions of "longitude" must be 1D or 2D')
        raise NotImplementedError('Geographical dimensions of "longitude" must be 1D or 2D')

    if file_y_values.ndim == 2:
        file_y_values = file_y_values[:, 0]
    elif file_y_values.ndim == 1:
        pass
    else:
        logger_stream.error('Geographical dimensions of "latitude" must be 1D or 2D')
        raise NotImplementedError('Geographical dimensions of "latitude" must be 1D or 2D')

    if file_map_dims is not None:
        file_obj = __adjust_dims_naming(file_obj, file_map_dims=file_map_dims)

    file_obj['longitude'] = xr.DataArray(file_x_values, dims='longitude')
    file_obj['latitude'] = xr.DataArray(file_y_values, dims='latitude')

    file_height, file_width = file_obj['latitude'].shape[0], file_obj['longitude'].shape[0]

    file_x_left = np.min(np.min(file_x_values))
    file_x_right = np.max(np.max(file_x_values))
    file_y_bottom = np.min(np.min(file_y_values))
    file_y_top = np.max(np.max(file_y_values))

    file_x_res = (file_x_right - file_x_left) / file_width
    file_y_res = (file_y_top - file_y_bottom) / file_height

    if file_epsg is None:
        file_epsg = proj_epsg

    if file_crs is None:
        file_crs = CRS.from_string(file_epsg)

    if file_transform is None:
        # TO DO: fix the 1/2 pixel of resolution in x and y ... using resolution/2
        file_transform = rasterio.transform.from_bounds(
            file_x_left, file_y_bottom, file_x_right, file_y_top,
            file_width, file_height)

    file_attrs = {'transform': file_transform, 'crs': file_crs, 'epsg': file_epsg,
                  'bbox': [file_x_left, file_y_bottom, file_x_right, file_y_top],
                  'bb_left': file_x_left, 'bb_right': file_x_right,
                  'bb_top': file_y_top, 'bb_bottom': file_y_bottom,
                  'res_x': file_x_res, 'res_y': file_y_res,
                  'high': file_height, 'wide': file_width}

    file_obj.attrs = file_attrs

    return file_obj
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to adjust dimensions naming
@with_logger(var_name='logger_stream')
def get_dims_by_object(obj):
    if isinstance(obj, xr.Dataset):
        # Already a dict
        return dict(obj.sizes)
    elif isinstance(obj, xr.DataArray):
        # Need to zip names with sizes
        return dict(zip(obj.dims, obj.shape))
    else:
        logger_stream.error("Input must be an xarray.Dataset or xarray.DataArray")
        raise TypeError("Input must be an xarray.Dataset or xarray.DataArray")
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to convert dataarray to dataset
@with_logger(var_name='logger_stream')
def da_to_dset(da: xr.DataArray) -> xr.Dataset:
    """
    Convert a DataArray to a Dataset, preserving its name.
    If the DataArray has no name, assign a default one.
    """
    var_name = da.name if da.name is not None else "variable"
    return da.to_dataset(name=var_name)
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to adjust object type to netcdf dtype
@with_logger(var_name='logger_stream')
def to_nc_dtype(t):

    # If it's already a real dtype/type, return it
    if t in (str, int, float, np.float32, np.float64, np.int32, np.int64, np.uint8):
        return t
    if isinstance(t, type):  # e.g., <class 'int'>
        # map builtins to explicit numpy types (recommended)
        if t is int:
            return np.int32
        if t is float:
            return np.float32
        if t is str:
            return str
        return t

    # If it's a string like "<class 'str'>" / "<class 'numpy.float64'>"
    if isinstance(t, str):
        s = t.strip()
        mapping = {
            "<class 'str'>": str,
            "<class 'int'>": np.int32,
            "<class 'float'>": np.float32,
            "<class 'numpy.float64'>": np.float64,
            "<class 'numpy.float32'>": np.float32,
            "<class 'numpy.int64'>": np.int64,
            "<class 'numpy.int32'>": np.int32,
        }
        if s in mapping:
            return mapping[s]
        # also accept netcdf typecodes if you ever use them
        if s in ("f4","f8","i4","i8","u1","S1"):
            return s

    logger_stream.error(f"Unsupported dtype spec: {t!r}")
    raise TypeError(f"Unsupported dtype spec: {t!r}")
# ----------------------------------------------------------------------------------------------------------------------
