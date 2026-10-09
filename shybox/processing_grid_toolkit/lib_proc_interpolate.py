"""
Library Features:

Name:          lib_proc_interpolate
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20260925'
Version:       '1.1.0'
"""
# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import numpy as np
import xarray as xr

from pyresample.geometry import GridDefinition, SwathDefinition
from pyresample.kd_tree import resample_nearest, resample_gauss, resample_custom
from repurpose.resample import resample_to_grid

from shybox.io_toolkit.lib_io_utils import create_darray

# common decorators
from shybox import with_logger, deprecated
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to interpolate data
@with_logger(var_name='logger_stream')
def interpolate_data(data: xr.DataArray, ref: xr.DataArray,
                     method='nn', radius_of_influence=18000, sigmas=25000, neighbours=8,
                     fill_value=np.nan, use_only_finite=True,
                     var_name_data='NA',
                     var_name_geo_x='longitude', var_name_geo_y='latitude',
                     coord_name_x='longitude', coord_name_y='latitude', dim_name_x='longitude', dim_name_y='latitude',
                     logger_stream=logging.Logger,
                     **kwargs) -> xr.DataArray:

    # check data format
    if not isinstance(data, xr.DataArray):
        logger_stream.error('Data variable format must be DataArray')
        raise NotImplementedError('Data variable format not allowed')
    # check data format
    if not isinstance(ref, xr.DataArray):
        logger_stream.error('Data reference format must be DataArray')
        raise NotImplementedError('Data reference format not allowed')

    # get data
    data_values = np.asarray(data.values, dtype=float)
    data_x_arr = np.asarray(data[var_name_geo_x].values, dtype=float)
    data_y_arr = np.asarray(data[var_name_geo_y].values, dtype=float)
    data_x_grid, data_y_grid = np.meshgrid(data_x_arr, data_y_arr)
    data_attrs = data.attrs

    # check data
    if data_values.ndim != 2:
        raise ValueError("`data_in` must be 2D")
    if data_x_grid.shape != data_values.shape or data_y_grid.shape != data_values.shape:
        raise ValueError("`data_in`, `geo_x_2d`, and `geo_y_2d` must have the same shape")

    # get reference
    ref_x_arr = ref[var_name_geo_x].values
    ref_y_arr = ref[var_name_geo_y].values
    ref_x_grid, ref_y_grid = np.meshgrid(ref_x_arr, ref_y_arr)

    # get finite values
    data_finite = np.isfinite(data_values)
    if not np.any(data_finite):
        data_out = np.full(data_values.shape, fill_value, dtype=float)

        output = create_darray(
            data_out, ref_x_grid[0, :], ref_y_grid[:, 0], name=var_name,
            coord_name_x=coord_name_x, coord_name_y=coord_name_y,
            dim_name_x=dim_name_x, dim_name_y=dim_name_y)

        output.attrs = data_attrs

        return da_out

    # define data object
    if use_only_finite:
        finite_x_grid, finite_y_grid = data_x_grid[data_finite], data_y_grid[data_finite]
        geo_grid_data = SwathDefinition(lons=finite_x_grid, lats=finite_y_grid)
        data_values = data_values[data_finite]
    else:
        finite_x_grid, finite_y_grid = data_x_grid, data_y_grid
        geo_grid_data = GridDefinition(lons=data_x_grid, lats=data_y_grid)

    # define reference object
    geo_grid_ref = GridDefinition(lons=ref_x_grid, lats=ref_y_grid)

    # apply interpolation method
    if method == 'nn':
        data_out = resample_nearest(
            source_geo_def=geo_grid_data,
            data=data_values,
            target_geo_def=geo_grid_ref,
            radius_of_influence=radius_of_influence,
            fill_value=fill_value
        )

    elif method == "gauss":
        data_out = resample_gauss(
            source_geo_def=geo_grid_data,
            data=data_values,
            target_geo_def=geo_grid_ref,
            radius_of_influence=radius_of_influence,
            sigmas=sigmas,
            neighbours=neighbours,
            fill_value=fill_value
        )

    elif method == "custom":
        data_out = resample_to_grid(
            {"data": data_values},
            finite_x_grid, finite_y_grid,
            ref_x_grid, ref_y_grid,
            search_rad=radius_of_influence,
            neighbours=neighbours,
            fill_values=fill_value
        )["data"]

    elif method == 'idw':
        weight_fx = lambda r: 1 / r ** 2
        data_out = resample_custom(
            geo_grid_data, data_values, geo_grid_ref,
            radius_of_influence=radius_of_influence, neighbours=neighbours,
            weight_funcs=weight_fx,
            fill_value=fill_value)

    else:
        logger_stream.error(f' ===> Interpolating method {method} is not available')
        raise NotImplementedError(f'Interpolation method {method} not implemented yet')

    output = create_darray(
        data_out, ref_x_grid[0, :], ref_y_grid[:, 0], name=var_name_data,
        coord_name_x=coord_name_x, coord_name_y=coord_name_y,
        dim_name_x=dim_name_x, dim_name_y=dim_name_y)
    output.attrs = data_attrs

    return output

# ----------------------------------------------------------------------------------------------------------------------
