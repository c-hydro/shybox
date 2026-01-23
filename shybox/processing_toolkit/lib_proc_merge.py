"""
Library Features:

Name:          lib_proc_merge
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20260123'
Version:       '1.2.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import os.path

import xarray as xr
import numpy as np
import pandas as pd

from pyresample.geometry import GridDefinition
from pyresample.kd_tree import resample_nearest, resample_gauss, resample_custom#
from repurpose.resample import resample_to_grid

from shybox.io_toolkit.lib_io_utils import create_darray
from shybox.orchestrator_toolkit.lib_orchestrator_utils import as_process
from shybox.logging_toolkit.lib_logging_utils import with_logger
from shybox.generic_toolkit.lib_utils_debug import plot_data, dump_data2nc
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to merge data by time
@as_process(input_type='xarray', output_type='xarray')
@with_logger(var_name='logger_stream')
def merge_data_by_time(
        data: (xr.DataArray, list), ref: xr.DataArray,
        time: pd.DatetimeIndex = None,
        ref_add_data: bool = False, ref_no_data: (float, int) = -9999.0, ref_name: str = 'terrain',
        var_no_data: (float, int) = -9999.0,
        var_geo_x='longitude', var_geo_y='latitude', var_time='time',
        coord_name_x: str = 'longitude', coord_name_y: str = 'latitude', coord_name_time: str = 'time',
        dim_name_x: str = 'longitude', dim_name_y: str = 'latitude', dim_name_time: str = 'time',
        debug: bool = False,
        **kwargs):

    # get geographical data from reference
    geo_data = ref.values
    geo_x, geo_y = ref[var_geo_x].values, ref[var_geo_y].values
    # reshape geo_x and geo_y if 2D
    if geo_x.ndim == 2 and geo_y.ndim == 2:
        geo_x = geo_x[0, :]
        geo_y = geo_y[:, 0]

    # iterate over data and create 3D variable object
    step_n = len(data)
    var_name = None
    var_obj = np.zeros((step_n, geo_data.shape[0], geo_data.shape[1]), dtype=geo_data.dtype)
    for step_id, step_obj in enumerate(data):
        var_data = step_obj.values

        # debug (plot before resample)
        if debug:
            plot_data(var_data, var_name=step_obj.name, title=f'Step {step_id} - before resample')

        var_data[np.isnan(var_data)] = var_no_data

        if var_name is None: var_name = step_obj.name
        var_obj[step_id, :, :] = var_data

    # create Dataset object
    dset_obj = xr.Dataset(coords={coord_name_time: ([dim_name_time], time)})
    dset_obj.coords[coord_name_time] = dset_obj.coords[coord_name_time].astype('datetime64[ns]')

    # add reference dataarray
    if ref_add_data:
        geo_data[np.isnan(geo_data)] = ref_no_data
        var_da_terrain = xr.DataArray(geo_data,  name=ref_name,
                                      dims=[dim_name_y, dim_name_x],
                                      coords={coord_name_x: ([dim_name_x], geo_x),
                                              coord_name_y: ([dim_name_y], geo_y)})
        dset_obj[ref_name] = var_da_terrain


    # create DataArray (2D or 3D)
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
        logger_stream.error('Data obj dimensions not supported.')
        raise NotImplemented

    dset_obj[var_name] = da_obj

    # debug (dump to netcdf)
    if debug:
        file_nc = 'merger_time.nc'
        if os.path.exists(file_nc): os.remove(file_nc)
        dump_data2nc(dset_obj, path='test.nc', var_name=var_name)

    return dset_obj

# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to merge data
@as_process(input_type='xarray', output_type='xarray')
@with_logger(var_name='logger_stream')
def merge_data_by_ref(
        data,                                     # xr.DataArray | xr.Dataset | list/tuple of them
        ref: xr.DataArray,
        ref_no_data: (float, int) = -9999.0,
        var_no_data: (float, int) = -9999.0,
        var_geo_x: str = 'longitude',
        var_geo_y: str = 'latitude',
        coord_name_x: str = 'longitude',
        coord_name_y: str = 'latitude',
        dim_name_x: str = 'longitude',
        dim_name_y: str = 'latitude',
        method: str = 'nn',
        max_distance: int = 18000,
        neighbours: int = 8,
        fill_value = np.nan,
        debug: bool = True,
        **kwargs):

    # check data type and normalize to list of Datasets
    if isinstance(data, (xr.DataArray, xr.Dataset)):
        ds_list = [_to_dataset(data)]
    elif isinstance(data, (list, tuple)):
        ds_list = [_to_dataset(it) for it in data]
    else:
        logger_stream.error("`data` must be a DataArray, Dataset, or list/tuple of them.")
        raise TypeError(f"`data` must be a DataArray, Dataset, or list/tuple of them. Got {type(data)}")

    # gather union of variable names across datasets
    var_list = sorted({vn for ds in ds_list for vn in ds.data_vars})

    # reference grid & mask
    dset_no_data = getattr(ref, "NODATA_value", var_no_data)
    if dset_no_data != ref_no_data:
        logger_stream.warning(
            f"Reference DataArray NODATA value ({dset_no_data}) does not match the provided ref_no_data ({ref_no_data}). "
            "This may lead to unexpected results. Proceed with caution. Using the reference NODATA value."
        )
        ref_no_data = dset_no_data

    # organize reference data & mask
    ref_data = ref.values.astype(np.float64)
    ref_data[ref_data == ref_no_data] = np.nan
    ref_nan = np.isnan(ref_data)

    ref_x_1d, ref_y_1d = ref[var_geo_x].values, ref[var_geo_y].values
    ref_x_2d, ref_y_2d = np.meshgrid(ref_x_1d, ref_y_1d)
    ref_grid = GridDefinition(lons=ref_x_2d, lats=ref_y_2d)

    # merge/resample per variable
    var_obj = []
    for var_name in var_list:

        # initialize merge array
        var_attrs = None
        var_merge = np.full_like(ref_data, np.nan, dtype=np.float64)

        # iterate over datasets to resample and merge
        for ds in ds_list:
            if var_name not in ds.data_vars:
                continue
            # get dataarray
            da_in = ds[var_name]
            if var_attrs is None:
                var_attrs = dict(da_in.attrs)

            # prepare data for resampling
            var_data = da_in.values.astype(np.float64)
            var_data[var_data == var_no_data] = np.nan  # mask source nodata before resampling

            var_x_1d = ds[var_geo_x].values
            var_y_1d = ds[var_geo_y].values
            var_x_2d, var_y_2d = np.meshgrid(var_x_1d, var_y_1d)
            var_grid = GridDefinition(lons=var_x_2d, lats=var_y_2d)

            # resample data to reference grid
            if method == 'nn':
                var_resample = resample_nearest(
                    var_grid, var_data, ref_grid,
                    radius_of_influence=max_distance,
                    fill_value=fill_value
                )

            elif method == 'gauss':
                var_resample = resample_gauss(
                    var_grid, var_data, ref_grid,
                    radius_of_influence=max_distance,
                    neighbours=neighbours,
                    sigmas=250000,
                    fill_value=fill_value
                )
            elif method == 'idw':
                weight_fx = lambda r: 1 / (r ** 2)
                var_resample = resample_custom(
                    var_grid, var_data, ref_grid,
                    radius_of_influence=max_distance,
                    neighbours=neighbours,
                    weight_funcs=weight_fx,
                    fill_value=fill_value
                )
            else:
                logger_stream.error(f'Resampling method "{method}" is not available')
                raise NotImplementedError(f'Resampling method "{method}" is not available')

            # sanitize resampled values and honor reference mask
            if np.isfinite(var_no_data):
                var_resample[var_resample == var_no_data] = np.nan
            var_resample[ref_nan] = np.nan

            # merge: overwrite where finite
            mask_finite = np.isfinite(var_resample)
            var_merge[mask_finite] = var_resample[mask_finite]

            # debug plot (merge data)
            if debug:
                plot_data(var_data, title=f'Input data: {var_name}')
                plot_data(var_resample, title=f'Resampled data: {var_name}')
                plot_data(var_merge, title=f'Merged data: {var_name}')

        # keep ref NaNs as NaN
        var_merge[ref_nan] = np.nan

        # debug plot (merge data)
        if debug:
            plot_data(var_merge, title=f'Merged data: {var_name}')

        # build output DataArray aligned to ref grid
        var_da = create_darray(
            var_merge, ref_x_2d[0, :], ref_y_2d[:, 0], name=var_name,
            coord_name_x=coord_name_x, coord_name_y=coord_name_y,
            dim_name_x=dim_name_x, dim_name_y=dim_name_y
        )
        # add attrinutes if defined
        if var_attrs:
            var_da.attrs = var_attrs
        # store variable DataArray
        var_obj.append(var_da)

    # keep your original return style: single DA or list of DAs
    if len(var_obj) == 1:
        return var_obj[0]
    else:
        return var_obj

# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# HELPERS
# normalize `data` to a list of Datasets
@with_logger(var_name='logger_stream')
def _to_dataset(obj: xr.DataArray | xr.Dataset) -> xr.Dataset:
    if isinstance(obj, xr.Dataset):
        return obj
    if isinstance(obj, xr.DataArray):
        name = obj.name or 'var'
        return obj.rename(name).to_dataset(name=name)
    logger_stream.error(f"Unsupported element in data: {type(obj)}")
    raise TypeError(f"Unsupported element in data: {type(obj)}")
# ----------------------------------------------------------------------------------------------------------------------

