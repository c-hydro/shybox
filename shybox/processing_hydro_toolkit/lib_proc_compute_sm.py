"""
Library Features:

Name:          lib_proc_compute_sm
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20260925'
Version:       '1.4.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import xarray as xr
import numpy as np

from typing import List, Optional

from scipy.ndimage import convolve, distance_transform_edt

from shybox.io_toolkit.lib_io_utils import create_darray
from shybox.geo_toolkit.lib_geo_utils import is_regular_grid, map_1d_to_ref_indices
from shybox.processing_grid_toolkit.lib_proc_interpolate import interpolate_data

from shybox.logging_toolkit.lib_logging_utils import with_logger
from shybox.generic_toolkit.lib_utils_debug import plot_data, dump_data2nc
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to compute soil moisture using domain mask, gaps filling and weighted mean
@with_logger(var_name='logger_stream')
def merge_soil_moisture(
        data: List[xr.DataArray],
        watermark: Optional[List[xr.DataArray]] = None,
        ref: Optional[xr.DataArray] = None,
        var_name_in: str = 'soil_moisture', var_name_out: str = 'soil_moisture',
        ref_no_data=-9999.0, value_no_data=-9999.0,
        coord_name_x='longitude', coord_name_y='latitude',
        dim_name_x='longitude', dim_name_y='latitude',
        thr_sm_min_flag: bool = False, thr_sm_max_flag: bool = False,
        thr_sm_min_value: float = 0.0, thr_sm_max_value: float = 1.0,
        debug_box: bool = False, debug_geo: bool = False, debug_data_raw: bool = False,
        debug_data_by_watermark: bool = False, debug_data_by_river_network: bool = False,
        debug_data_by_resampling: bool = False,
        debug_data_merge_domain_partial: bool = False, debug_data_merge_domain_gaps: bool = False,
        debug_data_merge_domain_merged: bool = False, debug_data_merge_domain_weighted: bool = False,
        fill_gaps_flag: bool = True, fill_gaps_poi: int = 2,
        interpolate_flag: bool = False,
        interpolate_method: str = "nn", interpolate_roi: float = 20000, interpolate_neighbours: int =  7,
        weight_flag: bool = True,
        weight_radius: int = 1, weight_poi: int = 5, weight_method: str ='distance',
        **kwargs):

    # algorithm info start
    logger_stream.info_up(f"Compute variable {var_name_in} ... ")

    # prepare reference mask
    ref_data = ref.values.astype(np.float64)
    ref_data[ref_data == ref_no_data] = np.nan
    ref_nan = np.isnan(ref_data)

    # reference coords
    ref_x_1d = ref[coord_name_x].values
    ref_y_1d = ref[coord_name_y].values
    ref_x_2d, ref_y_2d = np.meshgrid(ref_x_1d, ref_y_1d)
    nrows_ref, ncols_ref = ref_data.shape

    # initialize merge rt objects
    attrs_out = {}
    merge_out_raw = np.full((nrows_ref, ncols_ref), np.nan, dtype=np.float64)
    # iterate over datasets to merge
    for da_id, (da_in, da_wm) in enumerate(zip(data, watermark)):

        # info start data
        logger_stream.info_up(f" Analyze data {da_id} ... ")

        if not attrs_out:
            attrs_out = dict(da_in.attrs)
        # mask nodata
        da_in = da_in.where(da_in != value_no_data, np.nan)
        in_x_1d = da_in[coord_name_x].values
        in_y_1d = da_in[coord_name_y].values
        nrows_in, ncols_in = da_in.shape

        # get watermark values
        if da_wm is not None:
            values_wm = da_wm.values
            wm_x_1d = da_wm[coord_name_x].values
            wm_y_1d = da_wm[coord_name_y].values
        else:
            values_wm = np.full((nrows_in, ncols_in), -9999.0, dtype=np.float64)
            wm_x_1d, wm_y_1d = None, None

        # check geo coordinates of data input
        check_x_1d = is_regular_grid(in_x_1d)
        if not check_x_1d:
            logger_stream.warning(f"Input x coordinate in dataset {da_id} is not regular. Interpolation mode will be activated.")
            if wm_x_1d is not None:
                logger_stream.warning(f"Using watermark x coordinate for dataset {da_id} as input grid.")
                in_x_1d = wm_x_1d
            else:
                logger_stream.warning(f"No watermark x coordinate available for dataset {da_id}. Dataset will be skipped.")
        check_y_1d = is_regular_grid(in_y_1d)
        if not check_y_1d:
            logger_stream.warning(f"Input y coordinate in dataset {da_id} is not regular. Interpolation mode will be activated.")
            if wm_y_1d is not None:
                logger_stream.warning(f"Using watermark y coordinate for dataset {da_id} as input grid.")
                in_y_1d = wm_y_1d
            else:
                logger_stream.warning(f'No watermark y coordinate available for dataset {da_id}. Dataset will be skipped.')

        # filter the input data by watermark (keep only where watermark <= 0, set to NaN otherwise)
        if values_wm is not None:
            da_tmp = da_in.where(values_wm != 1, np.nan)
            values_tmp = da_tmp.values
        else:
            values_tmp = da_in.values
        tmp_x_2d, tmp_y_2d = np.meshgrid(in_x_1d, in_y_1d)

        # apply soil moisture thresholds (keep only where sm is between min and max, set to NaN otherwise)
        if thr_sm_min_flag:
            values_tmp = np.where(values_tmp < thr_sm_min_value, np.nan, values_tmp)
        if thr_sm_max_flag:
            values_tmp = np.where(values_tmp > thr_sm_max_value, np.nan, values_tmp)

        # check results
        if debug_geo:
            plot_data(values_wm, title=f"sm - watermark", plot_block=True)
        if debug_data_raw:
            plot_data(da_in.values, title=f"sm - data raw", plot_block=True)
        if debug_data_by_watermark:
            plot_data(values_tmp, title=f"sm - data filtered by wm", plot_block=True)

        # define return time DataArray for current dataset (for debugging and interpolation)
        da_tmp = create_darray(
            values_tmp, tmp_x_2d[0, :], tmp_y_2d[:, 0],
            name=var_name_out,
            coord_name_x=coord_name_x, coord_name_y=coord_name_y,
            dim_name_x=dim_name_x, dim_name_y=dim_name_y
        )

        # clean sub data
        sub_x_1d,  sub_y_1d  = da_tmp[coord_name_x].values, da_tmp[coord_name_y].values

        # check ref/sub grid compatibility
        dx_ref = float(np.abs(np.median(np.diff(ref_x_1d))))
        dy_ref = float(np.abs(np.median(np.diff(ref_y_1d))))

        dx_sub = float(np.abs(np.median(np.diff(sub_x_1d))))
        dy_sub = float(np.abs(np.median(np.diff(sub_y_1d))))

        same_dx = np.isclose(dx_ref, dx_sub, atol=1e-10)
        same_dy = np.isclose(dy_ref, dy_sub, atol=1e-10)

        # comment about grids
        logger_stream.info(
            f"Grid check dataset {da_id} :: "
            f"ref_dx={dx_ref:.12f}, ref_dy={dy_ref:.12f}, "
            f"sub_dx={dx_sub:.12f}, sub_dy={dy_sub:.12f}"
        )

        # if grid spacing is different, use wt directly and interpolate to ref grid (if watermark is present)
        if not (same_dx and same_dy):

            # check if watermark is present
            if da_wm is not None:

                # message about grid mismatch and interpolation
                logger_stream.warning(
                    f"Grid resolution mismatch in dataset {da_id}. "
                    f"Using watermark geometry directly and interpolating to reference grid."
                )

                # rebuild subgrid coordinates from area geometry
                wm_x_native = da_wm[coord_name_x].values
                wm_y_native = da_wm[coord_name_y].values

                # enforce tmp field on area native coordinates
                da_tmp = create_darray(
                    values_tmp, wm_x_native, wm_y_native,
                    name=var_name_out,
                    coord_name_x=coord_name_x, coord_name_y=coord_name_y,
                    dim_name_x=dim_name_x, dim_name_y=dim_name_y
                )

            else:

                # message about grid mismatch and interpolation
                logger_stream.warning(
                    f"Grid resolution mismatch in dataset {da_id}. "
                    f"Watermark not present, but interpolation needed to merge. "
                )

        # activate interpolation mode if sub grid is not perfectly aligned with ref grid
        if interpolate_flag:

            da_sub_out = interpolate_data(
                data=da_tmp, ref=ref,
                method=interp_method,
                neighbours=interpolate_neighbours,
                radius_of_influence=interpolate_roi, fill_value=np.nan,
                var_name_geo_x='longitude', var_name_geo_y='latitude',
                coord_name_x='longitude', coord_name_y='latitude', dim_name_x='longitude',
                dim_name_y='latitude',
            )
            # extract values from da
            arr_sub_out = da_sub_out.values

            # now update merge (if you want overwrite only where sub has values)
            mask_finite = ~np.isnan(arr_sub_out)
            merge_out_raw[mask_finite] = arr_sub_out[mask_finite]

        else:

            # get sub data
            sub_tmp = da_tmp.values.astype(np.float64)
            sub_tmp[sub_tmp == value_no_data] = np.nan

            # indices of each sub coord in ref grid
            i_ref = map_1d_to_ref_indices(ref_x_1d, sub_x_1d)  # length 559
            j_ref = map_1d_to_ref_indices(ref_y_1d, sub_y_1d)  # length 167

            dx_ref = np.abs(np.median(np.diff(ref_x_1d)))
            dx_sub = np.abs(np.median(np.diff(sub_x_1d)))
            dy_ref = np.abs(np.median(np.diff(ref_y_1d)))
            dy_sub = np.abs(np.median(np.diff(sub_y_1d)))

            x_phase = (sub_x_1d[0] - ref_x_1d[0]) / dx_ref
            y_phase = (ref_y_1d[0] - sub_y_1d[0]) / dy_ref

            # debug about grid and phase
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
            mask_finite = (~np.isnan(sub_tmp)) & (~ref_nan[jj_ref, ii_ref])
            merge_out_raw[jj_ref[mask_finite], ii_ref[mask_finite]] = sub_tmp[mask_finite]

        # check results
        if debug_data_by_resampling:
            plot_data(da_tmp.values, title=f"sm - data resample/interp method", plot_block=True)
        if debug_data_merge_domain_partial:
            plot_data(merge_out_raw, title=f"sm - merge domain partial", plot_block=True)

        # define output DataArray
        da_merge_raw = create_darray(
            merge_out_raw, ref_x_2d[0, :], ref_y_2d[:, 0],
            name=var_name_out,
            coord_name_x=coord_name_x, coord_name_y=coord_name_y,
            dim_name_x=dim_name_x, dim_name_y=dim_name_y
        )

        # info end data
        logger_stream.info_down(f" Analyze data {da_id} ... DONE")

    # apply fill gaps if requested
    if fill_gaps_flag:

        # find the gaps in the merged output (NaN regions close to data are gaps, others are extra)
        merge_out_gaps = _classify_gaps(merge_out_raw, pixel_poi=fill_gaps_poi)

        # check results
        if debug_data_merge_domain_gaps:
            plot_data(merge_out_gaps, title=f"sm - merge domain gaps", plot_block=True)
            plot_data(merge_out_raw, title=f"sm - merge domain out - before filling gaps", plot_block=True)

        da_merge_interp = interpolate_data(
            data=da_merge_raw, ref=ref,
            method=interpolate_method,
            neighbours=interpolate_neighbours,
            radius_of_influence=interpolate_roi, fill_value=np.nan,
            var_name_geo_x='longitude', var_name_geo_y='latitude',
            coord_name_x='longitude', coord_name_y='latitude', dim_name_x='longitude',
            dim_name_y='latitude',
        )

        # get values from raw and interp merge data
        merge_out_raw = da_merge_raw.values
        merge_out_interp = da_merge_interp.values

        # create mask no-finite data and gaps == 1
        merge_mask_gaps = (~np.isfinite(merge_out_raw)) & (merge_out_gaps == 1)
        # apply interpolated data only where mask is defined by 1
        merge_out_def = _apply_mask_filter(merge_out_raw, merge_mask_gaps, merge_out_interp)

        # check results
        if debug_data_merge_domain_gaps:
            plot_data(merge_out_def, title=f"sm - merge domain out - after filling gaps", plot_block=True)

    else:
        merge_out_def = merge_out_raw

    # keep ref NaNs
    merge_out_def[ref_nan] = np.nan

    # check results
    if debug_data_merge_domain_merged:
        plot_data(merge_out_def, title=f"sm - merge domain out", plot_block=True)

    # apply weighted mean before DataArray creation
    if weight_flag:
        merge_out_wg = _apply_mean_weighted(
            merge_out_def,
            radius=weight_radius, max_distance=weight_poi,
            weight_mode=weight_method
        )
        # restore original valid mask to avoid filling sea / outside-domain cells
        merge_out_wg[np.isnan(merge_out_def)] = np.nan
    else:
        merge_out_wg = merge_out_def.copy()

    # check results
    if debug_data_merge_domain_weighted:
        plot_data(merge_out_wg, title=f"sm - weighted domain out", plot_block=True)

    # define output DataArray
    da_out = create_darray(
        merge_out_wg, ref_x_2d[0, :], ref_y_2d[:, 0],
        name=var_name_out,
        coord_name_x=coord_name_x, coord_name_y=coord_name_y,
        dim_name_x=dim_name_x, dim_name_y=dim_name_y
    )

    if attrs_out:
        da_out.attrs = attrs_out

    # algorithm info end
    logger_stream.info_down(f"Compute variable {var_name_in} ... ")

    return da_out

# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# HELPERS
# helper to apply a NaN-aware weighted mean on a 2D array using a square moving window
@with_logger(var_name='logger_stream')
def _apply_mean_weighted(data, radius=1, weight_mode='distance',
                         max_distance=None):
    """
    Weighted mean on a 2D array using a square moving window,
    with optional max distance from valid data.

    Parameters
    ----------
    data : 2D np.ndarray
        Input raster.
    radius : int
        Number of pixel rings around the center:
        radius=1 -> 3x3
        radius=2 -> 5x5
    weight_mode : str
        'uniform'  -> all pixels same weight
        'distance' -> center gets larger weight
    max_distance : float or None
        Maximum allowed distance (in pixels) from valid data.
        Pixels farther than this remain NaN.

    Returns
    -------
    data_out : 2D np.ndarray
        Smoothed raster with NaN-aware weighted mean
        and distance constraint.
    """
    size = 2 * radius + 1

    # --- weights ---
    if weight_mode == 'uniform':
        weights = np.ones((size, size), dtype=float)

    elif weight_mode == 'distance':
        yy, xx = np.mgrid[-radius:radius+1, -radius:radius+1]
        dist = np.sqrt(xx**2 + yy**2)
        weights = 1.0 / (1.0 + dist)
    else:
        raise ValueError(f"Unsupported weight_mode: {weight_mode}")

    # --- NaN handling ---
    valid_mask = np.isfinite(data)
    data_filled = np.where(valid_mask, data, 0.0)

    num = convolve(data_filled, weights, mode='nearest')
    den = convolve(valid_mask.astype(float), weights, mode='nearest')

    with np.errstate(invalid='ignore'):
        data_out = np.where(den > 0, num / den, np.nan)

    # --- distance constraint ---
    if max_distance is not None:
        # distance from nearest valid pixel
        dist_map = distance_transform_edt(~valid_mask)

        # mask pixels too far from valid data
        data_out[dist_map > max_distance] = np.nan

    return data_out

# helper to apply mask to insert interpolated values only where mask is True
def _apply_mask_filter(data_raw, mask, data_to_apply_using_mask):

    data_raw = np.asarray(data_raw, dtype=float)
    mask = np.asarray(mask, dtype=bool)
    data_to_apply_using_mask = np.asarray(data_to_apply_using_mask, dtype=float)

    if data_raw.shape != mask.shape or data_raw.shape != data_to_apply_using_mask.shape:
        raise ValueError("data_raw, mask, and data_to_apply_using_mask must have the same shape")

    data_out = data_raw.copy()

    mask_apply = (~np.isfinite(data_raw)) & mask & np.isfinite(data_to_apply_using_mask)
    data_out[mask_apply] = data_to_apply_using_mask[mask_apply]

    return data_out

# helper to classify gaps in a 2D array based on NaN regions and distance to finite data (1=gap, 2=data, 3=extra)
@with_logger(var_name='logger_stream')
def _classify_gaps(arr, pixel_poi=3, return_poi=False):
    """
    Pixelwise gap classification.

      1 = gap   -> NaN pixel within `pixel_distance` of finite data
      2 = data  -> finite pixel
      3 = extra -> NaN pixel farther than `pixel_distance` from finite data

    Parameters
    ----------
    arr : 2D array-like
        Input array with NaNs/non-finite values.
    pixel_distance : int or float, default 3
        Max distance in pixels from finite data for a NaN pixel
        to be considered an internal gap.
    return_distance : bool, default False
        If True, also return distance-to-data.

    Returns
    -------
    cls : 2D uint8 array
        Classification mask.
    dist_to_data : 2D float array, optional
        Distance to nearest finite pixel.
    """
    a = np.asarray(arr, dtype=float)

    if a.ndim != 2:
        raise ValueError("`arr` must be a 2D array")

    data_mask = np.isfinite(a)
    nan_mask = ~data_mask

    # For each NaN pixel: distance to nearest finite pixel
    dist_to_data = distance_transform_edt(~data_mask)

    cls = np.full(a.shape, 3, dtype=np.uint8)
    cls[data_mask] = 2
    cls[nan_mask & (dist_to_data <= pixel_poi)] = 1

    if return_poi:
        return cls, dist_to_data
    return cls
# ----------------------------------------------------------------------------------------------------------------------

