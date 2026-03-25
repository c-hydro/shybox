"""
Library Features:

Name:          lib_proc_compute_rt
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20260123'
Version:       '1.2.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import xarray as xr
import numpy as np
from scipy.stats import genextreme
from decimal import Decimal
import pandas as pd

from pyresample.geometry import GridDefinition
from pyresample.kd_tree import resample_nearest, resample_gauss, resample_custom#
from repurpose.resample import resample_to_grid

from shybox.io_toolkit.lib_io_utils import create_darray
from shybox.orchestrator_toolkit.lib_orchestrator_utils_processes import as_process
from shybox.logging_toolkit.lib_logging_utils import with_logger
from shybox.generic_toolkit.lib_utils_debug import plot_data, dump_data2nc
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# helper to merge data by watermark
def _map_1d_to_ref_indices(ref_1d, sub_1d):
    """
    ref_1d must be monotonic. Returns indices into ref_1d for each sub_1d (nearest).
    """
    ref_1d = np.asarray(ref_1d)
    sub_1d = np.asarray(sub_1d)

    asc = ref_1d[0] < ref_1d[-1]
    if not asc:
        ref_1d = ref_1d[::-1]

    pos = np.searchsorted(ref_1d, sub_1d)
    pos = np.clip(pos, 1, ref_1d.size - 1)

    left = ref_1d[pos - 1]
    right = ref_1d[pos]
    choose_right = (np.abs(right - sub_1d) < np.abs(sub_1d - left))
    idx = pos.copy()
    idx[~choose_right] = pos[~choose_right] - 1

    if not asc:
        idx = (ref_1d.size - 1) - idx
    return idx


def burn_sub_on_ref_interp(da_sub, ref_x_1d, ref_y_1d, ref_nan=None,
                           var_no_data=None, method="nearest"):
    """
    da_sub: DataArray with 1D coords (latitude, longitude) (+ optional time length 1)
    ref_x_1d/ref_y_1d: reference 1D lon/lat
    ref_nan: 2D boolean mask on ref (True = do not write)
    method: "nearest" or "linear"
    """

    # ---- take 2D slice if time exists ----
    if "time" in da_sub.dims:
        if da_sub.sizes["time"] != 1:
            raise NotImplementedError("Only time length = 1 supported.")
        da2 = da_sub.isel(time=0)
    else:
        da2 = da_sub

    # ---- clean nodata ----
    if var_no_data is not None:
        da2 = da2.where(da2 != var_no_data)

    # ---- bbox mask: keep only where ref coords are within sub extent ----
    sub_x = da2["longitude"].values
    sub_y = da2["latitude"].values

    x_min, x_max = float(np.min(sub_x)), float(np.max(sub_x))
    y_min, y_max = float(np.min(sub_y)), float(np.max(sub_y))

    # ---- interpolate sub -> ref grid ----
    da_on_ref = da2.interp(
        longitude=xr.DataArray(ref_x_1d, dims=("longitude",)),
        latitude=xr.DataArray(ref_y_1d, dims=("latitude",)),
        method=method
    )

    # ---- apply bbox mask on ref grid (avoid extrapolated fill outside sub box) ----
    inside_x = (ref_x_1d >= x_min) & (ref_x_1d <= x_max)
    inside_y = (ref_y_1d >= y_min) & (ref_y_1d <= y_max)
    inside_2d = inside_y[:, None] & inside_x[None, :]

    out = da_on_ref.values.astype(np.float64)
    out[~inside_2d] = np.nan

    # ---- apply ref nodata mask ----
    if ref_nan is not None:
        out[np.asarray(ref_nan, dtype=bool)] = np.nan

    return out

# method to merge data
@as_process(input_type='xarray', output_type='xarray',
            lazy_undefined_args=True, lazy_undefined_value=None)
@with_logger(var_name='logger_stream')
def compute_return_time(
        data,
        ref: xr.DataArray,
        area: (list, xr.DataArray) = None,
        watermark: (list, xr.DataArray) = None,
        metrics: (list, xr.DataArray) = None,
        var_name_in: str = 'discharge', var_name_out: str = 'return_time',
        ref_no_data=-9999.0, var_no_data=-9999.0,
        coord_name_x='longitude', coord_name_y='latitude',
        dim_name_x='longitude', dim_name_y='latitude',
        cellsize_name='cellsize',
        interpolation_mode: bool = False, interpolation_method: str= 'nearest',
        thr_discharge_min_flag: bool = True, thr_discharge_max_flag: bool = False,
        thr_discharge_min_value: float = 8, thr_discharge_max_value: float = 10000,
        thr_rt_min_flag: bool = False, thr_rt_max_flag: bool = True,
        thr_rt_min_value: float = 0, thr_rt_max_value: float = 100, bins_n_rt: int = 5,
        debug_box: bool = False, debug_geo_steps: bool = False,  debug_data_steps: bool = False,
        debug_data_out: bool = False,  **kwargs):

    # algorithm info start
    logger_stream.info_up("Compute return time ... ")

    # normalize to list of Datasets
    if isinstance(data, (xr.DataArray, xr.Dataset)):
        ds_list = [_to_dataset(data)]
    elif isinstance(data, (list, tuple)):
        ds_list = [_to_dataset(it) for it in data]
    else:
        raise TypeError(f"`data` must be a DataArray, Dataset, or list/tuple. Got {type(data)}")

    # normalize watermark to list aligned to ds_list (KEEP list behavior)
    if watermark is None:
        wm_list = [None] * len(ds_list)
    elif isinstance(watermark, (xr.DataArray, xr.Dataset)):
        # same watermark for all datasets (your previous behavior)
        wm_list = [watermark] * len(ds_list)
    elif isinstance(watermark, (list, tuple)):
        if len(watermark) != len(ds_list):
            raise ValueError("If `watermark` is a list/tuple, it must have the same length as `data`.")
        wm_list = list(watermark)
    else:
        raise TypeError(f"`watermark` must be DataArray/Dataset or list/tuple. Got {type(watermark)}")

    # normalize watermark to list aligned to ds_list (KEEP list behavior)
    if metrics is None:
        metrics_list = [None] * len(ds_list)
    elif isinstance(watermark, (xr.DataArray, xr.Dataset)):
        # same watermark for all datasets (your previous behavior)
        metrics_list = [metrics] * len(ds_list)
    elif isinstance(watermark, (list, tuple)):
        if len(watermark) != len(ds_list):
            raise ValueError("If `metrics` is a list/tuple, it must have the same length as `data`.")
        metrics_list = list(metrics)
    else:
        raise TypeError(f"`metrics` must be DataArray/Dataset or list/tuple. Got {type(watermark)}")

    # normalize watermark to list aligned to ds_list (KEEP list behavior)
    if area is None:
        area_list = [None] * len(ds_list)
    elif isinstance(watermark, (xr.DataArray, xr.Dataset)):
        # same watermark for all datasets (your previous behavior)
        area_list = [area] * len(ds_list)
    elif isinstance(watermark, (list, tuple)):
        if len(watermark) != len(ds_list):
            raise ValueError("If `area` is a list/tuple, it must have the same length as `data`.")
        area_list = list(area)
    else:
        raise TypeError(f"`area` must be DataArray/Dataset or list/tuple. Got {type(watermark)}")

    # define the variable list
    var_list = sorted({vn for ds in ds_list for vn in ds.data_vars})
    if len(var_list) == 1:
        logger_stream.info(f"Variable datasets: {var_list[0]} "
                           f"-- Variable src: {var_name_in} -- Variable dst {var_name_out}")
        var_name_found = var_list[0]
    else:
        logger_stream.error('Multiple variables found in datasets. Please specify variable names explicitly.')
        raise ValueError('Multiple variables found in datasets. Please specify variable names explicitly.')

    # prepare reference mask
    ref_data = ref.values.astype(np.float64)
    ref_data[ref_data == ref_no_data] = np.nan
    ref_nan = np.isnan(ref_data)

    # reference coords
    ref_x_1d = ref[coord_name_x].values
    ref_y_1d = ref[coord_name_y].values
    ref_x_2d, ref_y_2d = np.meshgrid(ref_x_1d, ref_y_1d)
    nrows_ref, ncols_ref = ref_data.shape

    # info start variable
    logger_stream.info_up(f"Variable {var_name_found} ... ")

    # initialize merge rt objects
    attrs_rt = {}
    merge_rt = np.full((nrows_ref, ncols_ref), np.nan, dtype=np.float64)
    # iterate over datasets to merge
    for ds_id, (ds_vars, da_wm, da_metrics, da_area) in enumerate(zip(ds_list, wm_list, metrics_list, area_list)):

        # check variable presence (redundant if we enforce single variable, but good for safety)
        if var_name_found not in ds_vars.data_vars:
            logger_stream.error(f"Variable {var_name_found} not found in dataset {ds_id}. Skipping this dataset.")
            raise ValueError(f"Variable {var_name_found} not found in dataset {ds_id}. Skipping this dataset.")

        # info start dataset
        logger_stream.info_up(f"Dataset {ds_id} ... ")

        # get data
        da_in = ds_vars[var_name_found]
        if not attrs_rt:
            attrs_rt = dict(da_in.attrs)
        # mask nodata
        da_in = da_in.where(da_in != var_no_data, np.nan)
        values_q_in = da_in.values

        # get area values
        values_area = None
        area_x_1d, area_y_1d = None, None
        if da_area is not None:
            values_area_px = da_area.values
            area_x_1d = da_area[coord_name_x].values
            area_y_1d = da_area[coord_name_y].values

            # get cell size in degrees from area attributes
            cell_size_deg = da_area.attrs[cellsize_name]

            # ensure float
            if isinstance(cell_size_deg, Decimal):
                cell_size_deg = float(cell_size_deg)
            else:
                cell_size_deg = float(cell_size_deg)

            # approximate conversion deg -> km
            lat_mean = float(np.nanmean(area_y_1d))

            km_per_deg_lat = 111.32
            km_per_deg_lon = 111.32 * np.cos(np.deg2rad(lat_mean))

            dx_km = cell_size_deg * km_per_deg_lon
            dy_km = cell_size_deg * km_per_deg_lat

            values_area_km2 = values_area_px * (dx_km * dy_km)

            area_x_2d, area_y_2d = np.meshgrid(area_x_1d, area_y_1d)
        else:
            # exit if area not provided
            logger_stream.warning('Area dataset not provided. Skipping return time computation.')
            # algorithm end start
            logger_stream.info_down(f"Dataset {ds_id} ... some variables missing. Skipping return time computation. ")
            logger_stream.info_down(f"Variable {var_name_found} ... SKIPPED")
            logger_stream.info_down("Compute return time ... SKIPPED. ")
            return None

        # get watermark values
        values_wm = None
        wm_x_1d, wm_y_1d = None, None
        if da_wm is not None:
            values_wm = da_wm.values
            da_in = da_in.where(values_wm <= 0, np.nan)
            wm_x_1d = da_wm[coord_name_x].values
            wm_y_1d = da_wm[coord_name_y].values

        # initialize return time arrays
        values_p = np.full_like(values_q_in, np.nan, dtype=float)
        values_kt = np.zeros_like(values_q_in, dtype=float)
        if da_metrics is not None:

            # get value from metrics if available
            values_a = da_metrics['a'].values if 'a' in da_metrics.data_vars else None
            values_q_idx = da_metrics['qindex'].values if 'qindex' in da_metrics.data_vars else None
            params_gev = da_metrics['params_gev'].values if 'params_gev' in da_metrics.data_vars else None
            # check if all required variables are present
            if any(v is None for v in (values_a, values_q_idx, params_gev)):
                # exit if metrics do not contain required variables
                logger_stream.warning(
                    'Metrics dataset does not contain required variables (a, q_index, params_gev). Skipping return time computation.')
                # algorithm end start
                logger_stream.info_down(
                    f"Dataset {ds_id} ... some variables missing. Skipping return time computation. ")
                logger_stream.info_down(f"Variable {var_name_found} ... SKIPPED")
                logger_stream.info_down("Compute return time ... SKIPPED. ")

                return None

            # apply discharge thresholds on area classes if flags are set
            if thr_discharge_min_flag:
                values_a[0] = thr_discharge_min_value
            if thr_discharge_max_flag:
                values_a[-1] = thr_discharge_max_value

            # check results
            if debug_geo_steps:
                plot_data(values_q_in, title=f"rt - discharge values", plot_block=True)
                plot_data(values_area_km2, title=f"rt - cell area", plot_block=True)
                plot_data(values_wm, title=f"rt - watermark", plot_block=True)
            if debug_data_steps:
                plot_data(values_q_idx, title=f"rt - discharge index", plot_block=True)

            # iterate over area classes
            a_num = len(values_a) - 1
            for a_id, a_step in enumerate(range(a_num)):

                # mask for current area class
                a_1, a_2 = values_a[a_step], values_a[a_step + 1]

                # mask for current area class
                mask_area = (values_area_km2 > a_1) & (values_area_km2 < a_2)
                mask_count = np.sum(mask_area)

                # check results
                if debug_geo_steps:
                    plot_data(mask_area, title=f"rt - mask area")

                # info start variable
                logger_stream.info_up(
                    f"Area class ::  id {a_id} -- bounds_area [{a_1}, {a_2}] -- values_area: {mask_count} ... ")

                # get parameters for current area class
                gev_k, gev_sigma, gev_mu = params_gev[a_step, :]
                # compute kt for current area class
                with np.errstate(divide='ignore', invalid='ignore'):
                    values_kt[mask_area] = values_q_in[mask_area] / values_q_idx[mask_area]

                # MATLAB: gevcdf(x, k, sigma, mu)
                # SciPy: genextreme.cdf(x, c=-k, loc=mu, scale=sigma)
                values_p[mask_area] = genextreme.cdf(values_kt[mask_area], c=-gev_k, loc=gev_mu, scale=gev_sigma)
                # set negative probabilities to NaN
                values_p[values_p < 0] = np.nan

                # info end variable
                logger_stream.info_down(
                    f"Area class ::  id {a_id} -- bounds_area [{a_1}, {a_2}] -- values_area: {mask_count} ... DONE")

            # return time computation
            with np.errstate(divide="ignore", invalid="ignore"):
                values_rt = 1.0 / (1.0 - values_p)
            # return time post-processing
            values_rt = _nullify_bounds(values_rt)

            # apply return time thresholds if flags are set
            if thr_rt_min_flag:
                values_rt[values_rt < thr_rt_min_value] = thr_rt_min_value
            if thr_rt_max_flag:
                values_rt[values_rt > thr_rt_max_value] = thr_rt_max_value

            # analyze return time distribution and create classes
            min_rt, max_rt = np.nanmin(values_rt), np.nanmax(values_rt)
            bins_spatial_rt = np.floor(np.linspace(min_rt, max_rt, int(bins_n_rt) + 1)).astype(int)
            classes_rt = np.digitize(values_rt, bins_spatial_rt)

            logger_stream.info("rt bins: %s", bins_spatial_rt)
            for bin_i in range(1, len(bins_spatial_rt)):
                classes_n = np.sum(classes_rt == bin_i)
                logger_stream.info(f"rt bin {bin_i}: {bins_spatial_rt[bin_i - 1]:.2f} - {bins_spatial_rt[bin_i]:.2f} -> n = {classes_n}")

            # compute return time mask (1 where valid, NaN where invalid)
            mask_rt = values_rt.copy()
            mask_rt[mask_rt >= 0] = 1

            # filter areas outside the watermark (if watermark provided)
            values_rt[values_wm == 1] = np.nan

            # check results
            if debug_data_steps:
                plot_data(values_rt, title=f"rt - values", plot_block=True)
                plot_data(mask_rt, title=f"rt - mask", plot_block=True)
                plot_data(values_wm, title=f"rt - watermark", plot_block=True)

        else:
            # exit if metrics not provided
            logger_stream.warning('Metrics dataset not provided. Skipping return time computation.')

            # algorithm end start
            logger_stream.info_down(f"Dataset {ds_id} ... some variables missing. Skipping return time computation. ")
            logger_stream.info_down(f"Variable {var_name_found} ... SKIPPED")
            logger_stream.info_down("Compute return time ... SKIPPED. ")
            return None

        # define return time DataArray for current dataset (for debugging and interpolation)
        da_rt = create_darray(
            values_rt, area_x_2d[0, :], area_y_2d[:, 0],
            name=var_name_out,
            coord_name_x=coord_name_x, coord_name_y=coord_name_y,
            dim_name_x=dim_name_x, dim_name_y=dim_name_y
        )

        # clean sub data
        sub_x_1d,  sub_y_1d  = da_rt[coord_name_x].values, da_rt[coord_name_y].values

        # check ref/sub grid compatibility
        dx_ref = float(np.abs(np.median(np.diff(ref_x_1d))))
        dy_ref = float(np.abs(np.median(np.diff(ref_y_1d))))

        dx_sub = float(np.abs(np.median(np.diff(sub_x_1d))))
        dy_sub = float(np.abs(np.median(np.diff(sub_y_1d))))

        same_dx = np.isclose(dx_ref, dx_sub, atol=1e-10)
        same_dy = np.isclose(dy_ref, dy_sub, atol=1e-10)

        # comment about grids
        logger_stream.info(
            f"Grid check dataset {ds_id} :: "
            f"ref_dx={dx_ref:.12f}, ref_dy={dy_ref:.12f}, "
            f"sub_dx={dx_sub:.12f}, sub_dy={dy_sub:.12f}"
        )

        # if grid spacing is different, use area geometry directly and force interpolation
        # area geometry is the authoritative subgrid geometry
        if not (same_dx and same_dy):

            logger_stream.warning(
                f"Grid resolution mismatch in dataset {ds_id}. "
                f"Using area geometry directly and interpolating to reference grid."
            )

            # rebuild subgrid coordinates from area geometry
            # da_area is assumed to carry the correct native geometry
            area_x_native = da_area[coord_name_x].values
            area_y_native = da_area[coord_name_y].values

            # enforce RT field on area native coordinates
            da_rt = create_darray(
                values_rt, area_x_native, area_y_native,
                name=var_name_out,
                coord_name_x=coord_name_x, coord_name_y=coord_name_y,
                dim_name_x=dim_name_x, dim_name_y=dim_name_y
            )

        # activate interpolation mode if sub grid is not perfectly aligned with ref grid
        if interpolation_mode:
            sub_out = burn_sub_on_ref_interp(
                da_sub=da_rt,  # DataArray of your variable in subdomain
                ref_x_1d=ref_x_1d,
                ref_y_1d=ref_y_1d,
                ref_nan=ref_nan,
                var_no_data=var_no_data,
                method=interpolation_method # "nearest" or "linear"
            )

            # now update merge (if you want overwrite only where sub has values)
            mask_finite = ~np.isnan(sub_out)
            merge_rt[mask_finite] = sub_out[mask_finite]

        else:

            # get sub data
            sub_data = da_rt.values.astype(np.float64)
            sub_data[sub_data == var_no_data] = np.nan

            # indices of each sub coord in ref grid
            i_ref = _map_1d_to_ref_indices(ref_x_1d, sub_x_1d)  # length 559
            j_ref = _map_1d_to_ref_indices(ref_y_1d, sub_y_1d)  # length 167

            dx_ref = np.abs(np.median(np.diff(ref_x_1d)))
            dx_sub = np.abs(np.median(np.diff(sub_x_1d)))
            dy_ref = np.abs(np.median(np.diff(ref_y_1d)))
            dy_sub = np.abs(np.median(np.diff(sub_y_1d)))

            x_phase = (sub_x_1d[0] - ref_x_1d[0]) / dx_ref
            y_phase = (ref_y_1d[0] - sub_y_1d[0]) / dy_ref

            if debug_box:
                logger_stream.info("ref x start: %s", ref_x_1d[0])
                logger_stream.info("sub x start: %s", sub_x_1d[0])
                logger_stream.info("ref y start: %s", ref_y_1d[0])
                logger_stream.info("sub y start: %s", sub_y_1d[0])

                logger_stream.info("dx_ref: %s", dx_ref)
                logger_stream.info("dx_sub: %s", dx_sub)
                logger_stream.info("dy_ref: %s", dy_ref)
                logger_stream.info("dy_sub: %s", dy_sub)

                logger_stream.info("x_phase: %s", x_phase)
                logger_stream.info("y_phase: %s", y_phase)
                logger_stream.info("x_phase nearest int diff: %s", x_phase - round(x_phase))
                logger_stream.info("y_phase nearest int diff: %s", y_phase - round(y_phase))

            # build 2D index grids
            jj_ref, ii_ref = np.meshgrid(j_ref, i_ref, indexing="ij")  # shape (167, 559)
            # update only where sub valid and ref is valid
            mask_finite = (~np.isnan(sub_data)) & (~ref_nan[jj_ref, ii_ref])
            merge_rt[jj_ref[mask_finite], ii_ref[mask_finite]] = sub_data[mask_finite]

        # debug
        if debug_data_steps:
            plot_data(da_rt.values, title=f"return time - data step")
            plot_data(merge_rt, title=f"return time - merge step")

        # info end dataset
        logger_stream.info_down(f"Dataset {ds_id} ... DONE")

    # keep ref NaNs
    merge_rt[ref_nan] = np.nan

    # check results
    if debug_data_out:
        plot_data(merge_rt, title=f"return time - merge step - final", plot_block=True)

    # define output DataArray
    da_merge = create_darray(
        merge_rt, ref_x_2d[0, :], ref_y_2d[:, 0],
        name=var_name_out,
        coord_name_x=coord_name_x, coord_name_y=coord_name_y,
        dim_name_x=dim_name_x, dim_name_y=dim_name_y
    )
    if attrs_rt:
        da_merge.attrs = attrs_rt

    # info start variable
    logger_stream.info_down(f"Variable {var_name_found} ... DONE")

    # algorithm end
    logger_stream.info_down("Compute return time ... DONE")

    return da_merge

# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# HELPERS
# helper to remove boundaries (set to NaN)
def _nullify_bounds(arr, no_data=np.nan):
    arr[0, :] = no_data
    arr[-1, :] = no_data
    arr[:, 0] = no_data
    arr[:, -1] = no_data
    return arr

# helper to normalize `data` to a list of Datasets
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

