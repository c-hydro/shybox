"""
Library Features:

Name:          lib_proc_interp
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20250127'
Version:       '1.0.0'
"""
# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import numpy as np
import xarray as xr

from pyresample.geometry import GridDefinition
from pyresample.kd_tree import resample_nearest, resample_gauss, resample_custom#
from repurpose.resample import resample_to_grid

from shybox.io_toolkit.lib_io_utils import create_darray
from shybox.orchestrator_toolkit.lib_orchestrator_utils_processes import as_process

from shybox.logging_toolkit.lib_logging_utils import with_logger

import matplotlib.pyplot as plt
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to interpolate data
@as_process(input_type='xarray', output_type='xarray')
def interpolate_data(data: xr.DataArray, ref: xr.DataArray,
                     method='nn', max_distance=18000, neighbours=8, fill_value=np.nan,
                     var_name_geo_x='longitude', var_name_geo_y='latitude',
                     coord_name_x='longitude', coord_name_y='latitude', dim_name_x='longitude', dim_name_y='latitude',
                     **kwargs) -> (xr.Dataset, xr.DataArray):

    # get geo data
    data_x_arr = data[var_name_geo_x].values
    data_y_arr = data[var_name_geo_y].values
    data_x_grid, data_y_grid = np.meshgrid(data_x_arr, data_y_arr)

    # get geo reference
    ref_x_arr = ref[var_name_geo_x].values
    ref_y_arr = ref[var_name_geo_y].values
    ref_x_grid, ref_y_grid = np.meshgrid(ref_x_arr, ref_y_arr)

    geo_grid_data = GridDefinition(lons=data_x_grid, lats=data_y_grid)
    geo_grid_ref = GridDefinition(lons=ref_x_grid, lats=ref_y_grid)

    if isinstance(data, xr.DataArray):
        var_data_in = data.values
        var_name = data.name
        var_attrs = data.attrs
    else:
        logging.error(' ===> Data format in interpolation method not allowed')
        raise NotImplementedError('Data format not allowed')

    if method == 'nn':
        var_data_out = resample_nearest(
            geo_grid_data, var_data_in, geo_grid_ref,
            radius_of_influence=max_distance,
            fill_value=fill_value)

    elif method == 'gauss':
        var_data_out = resample_gauss(
            geo_grid_data, var_data_in, geo_grid_ref,
            radius_of_influence=max_distance,
            neighbours=neighbours, sigmas=250000,
            fill_value=fill_value)

    elif method == 'idw':
        weight_fx = lambda r: 1 / r ** 2
        var_data_out = resample_custom(
            geo_grid_data, var_data_in, geo_grid_ref,
            radius_of_influence=max_distance, neighbours=neighbours,
            weight_funcs=weight_fx,
            fill_value=fill_value)
    else:
        logging.error(' ===> Interpolating method "' + method + '" is not available')
        raise NotImplemented('Interpolation method "' + method + '" not implemented yet')

    output = create_darray(
        var_data_out, ref_x_grid[0, :], ref_y_grid[:, 0], name=var_name,
        coord_name_x=coord_name_x, coord_name_y=coord_name_y,
        dim_name_x=dim_name_x, dim_name_y=dim_name_y)
    output.attrs = var_attrs

    return output

# ----------------------------------------------------------------------------------------------------------------------
