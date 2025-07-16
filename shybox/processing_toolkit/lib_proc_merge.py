"""
Library Features:

Name:          lib_proc_merge
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20250203'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import warnings
import os.path

from osgeo import gdal
import xarray as xr
import numpy as np
import pandas as pd
from collections import defaultdict
from typing import Optional, Generator

from pyresample.geometry import GridDefinition
from pyresample.kd_tree import resample_nearest, resample_gauss, resample_custom#
from repurpose.resample import resample_to_grid

from shybox.io_toolkit.lib_io_utils import create_darray
from shybox.orchestrator_toolkit.lib_orchestrator_utils import as_process

#import matplotlib
#matplotlib.use('TkAgg')
#import matplotlib.pyplot as plt
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to merge data by time
@as_process(input_type='xarray', output_type='xarray')
def merge_data_by_time(
        data: (xr.DataArray, list), ref: xr.DataArray,
        time: pd.DatetimeIndex = None,
        ref_add_data: bool = False, ref_no_data: (float, int) = -9999.0, ref_name: str = 'terrain',
        var_no_data: (float, int) = -9999.0,
        var_geo_x='longitude', var_geo_y='latitude', var_time='time',
        coord_name_x: str = 'longitude', coord_name_y: str = 'latitude', coord_name_time: str = 'time',
        dim_name_x: str = 'longitude', dim_name_y: str = 'latitude', dim_name_time: str = 'time',
        **kwargs):

    geo_data = ref.values
    geo_x, geo_y = ref[var_geo_x].values, ref[var_geo_y].values

    if geo_x.ndim == 2 and geo_y.ndim == 2:
        geo_x = geo_x[0, :]
        geo_y = geo_y[:, 0]

    step_n = len(data)
    var_name = None
    var_obj = np.zeros((step_n, geo_data.shape[0], geo_data.shape[1]), dtype=geo_data.dtype)
    for step_id, step_obj in enumerate(data):
        var_data = step_obj.values

        ''' debug plot
        plt.figure()
        plt.imshow(var_data)
        plt.colorbar()
        plt.show(block=True)
        '''
        var_data[np.isnan(var_data)] = var_no_data

        if var_name is None: var_name = step_obj.name
        var_obj[step_id, :, :] = var_data


    dset_obj = xr.Dataset(coords={coord_name_time: ([dim_name_time], time)})
    dset_obj.coords[coord_name_time] = dset_obj.coords[coord_name_time].astype('datetime64[ns]')

    if ref_add_data:
        geo_data[np.isnan(geo_data)] = ref_no_data
        var_da_terrain = xr.DataArray(geo_data,  name=ref_name,
                                      dims=[dim_name_y, dim_name_x],
                                      coords={coord_name_x: ([dim_name_x], geo_x),
                                              coord_name_y: ([dim_name_y], geo_y)})
        dset_obj[ref_name] = var_da_terrain


    if var_obj.ndim == 2:
        da_obj = xr.DataArray(var_obj, name=var_name,
                                   dims=[dim_name_y, dim_name_x],
                                   coords={coord_name_x: ([dim_name_x], geo_x),
                                           coord_name_y: ([dim_name_y], geo_y)})
    elif var_obj.ndim == 3:
        da_obj = xr.DataArray(var_obj, name=var_name,
                                   dims=[dim_name_time, dim_name_y, dim_name_x],
                                   coords={coord_name_time: ([dim_name_time], time),
                                           coord_name_x: ([dim_name_x], geo_x),
                                           coord_name_y: ([dim_name_y], geo_y)})
    else:
        raise NotImplemented

    dset_obj[var_name] = da_obj

    ''' debug test
    file_test = 'test.nc'
    if os.path.exists(file_test):
        os.remove(file_test)
    dset_obj.to_netcdf('test.nc',
                       mode='w', format='NETCDF4', engine='netcdf4',
                       encoding={var_name: {'zlib': True, 'complevel': 5}})
    '''
    return dset_obj

# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to merge data
@as_process(input_type='xarray', output_type='xarray')
def merge_data_by_ref(
        data: (xr.DataArray, list),
        ref: xr.DataArray, ref_no_data: (float, int) = -9999.0, var_no_data: (float, int) = -9999.0,
        var_geo_x='longitude', var_geo_y='latitude',
        coord_name_x: str = 'longitude', coord_name_y: str = 'latitude',
        dim_name_x: str = 'longitude', dim_name_y: str = 'latitude',
        method='nn', max_distance = 18000, neighbours = 8, fill_value = np.nan,
        **kwargs):

    var_list = None
    for da_id, da_obj in enumerate(data):
        if var_list is None:
            var_list = list(da_obj.data_vars)
            break

    ref_data = ref.values

    dset_no_data = var_no_data
    if hasattr(ref, "NODATA_value"):
        dset_no_data = ref.NODATA_value

    if dset_no_data != ref_no_data:
        warnings.warn(
            f"Reference DataArray NODATA value ({dset_no_data}) does not match the provided ref_no_data ({ref_no_data}). "
            "This may lead to unexpected results. Proceed with caution. Algorithm will use the provided dset_no_data value."
        )
        ref_no_data = dset_no_data

    ref_data = ref_data.astype(np.float64)
    ref_data[ref_data == ref_no_data] = np.nan
    ref_x_1d, ref_y_1d = ref[var_geo_x].values, ref[var_geo_y].values
    ref_x_2d, ref_y_2d = np.meshgrid(ref_x_1d, ref_y_1d)
    ref_grid = GridDefinition(lons=ref_x_2d, lats=ref_y_2d)

    # Merge the DataArray objects based on the reference DataArray
    var_obj = []
    for var_name in var_list:

        var_attrs, var_merge = None, np.zeros_like(ref_data)
        for da_id, da_obj in enumerate(data):
            var_data = da_obj[var_name].values

            if var_attrs is None:
                var_attrs = da_obj[var_name].attrs

            var_x_1d, var_y_1d = da_obj[var_geo_x].values, da_obj[var_geo_y].values
            var_x_2d, var_y_2d = np.meshgrid(var_x_1d, var_y_1d)
            var_grid = GridDefinition(lons=var_x_2d, lats=var_y_2d)

            if method == 'nn':
                var_resample = resample_nearest(
                    var_grid, var_data, ref_grid,
                    radius_of_influence=max_distance,
                    fill_value=fill_value)

            elif method == 'gauss':
                var_resample = resample_gauss(
                    var_grid, var_data, ref_grid,
                    radius_of_influence=max_distance, neighbours=neighbours, sigmas=250000,
                    fill_value=fill_value)

            elif method == 'idw':
                weight_fx = lambda r: 1 / r ** 2
                var_resample = resample_custom(
                    var_grid, var_data, ref_grid,
                    radius_of_influence=max_distance, neighbours=neighbours,
                    weight_funcs=weight_fx,
                    fill_value=fill_value)
            else:
                raise NotImplemented('Resampling method "' + method + '" is not available')

            var_data[var_data == var_no_data] = np.nan
            var_resample[ref_data == ref_no_data] = np.nan
            var_resample[var_resample == var_no_data] = np.nan

            var_merge[np.isfinite(var_resample)] = var_resample[np.isfinite(var_resample)]
            var_merge[np.isnan(ref_data)] = np.nan

        var_da = create_darray(
            var_merge, ref_x_2d[0, :], ref_y_2d[:, 0], name=var_name,
            coord_name_x=coord_name_x, coord_name_y=coord_name_y,
            dim_name_x=dim_name_x, dim_name_y=dim_name_y)
        var_da.attrs = var_attrs

        var_obj.append(var_da)

        ''' debug plot
        import matplotlib.pyplot as plt
        plt.figure()
        plt.imshow(var_data)
        plt.colorbar()
        plt.figure()
        plt.imshow(var_resample)
        plt.colorbar()

        plt.figure()
        plt.imshow(var_merge)
        plt.colorbar()
        plt.show(block=True)
        plt.interactive(False)
        '''

    if len(var_obj) == 1:
        var_obj = var_obj[0]

    return var_obj
# ----------------------------------------------------------------------------------------------------------------------
