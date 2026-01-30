"""
Library Features:

Name:          lib_utils_geo
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20260130'
Version:       '1.1.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import numpy as np
import xarray as xr

from shybox.logging_toolkit.lib_logging_utils import with_logger
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to create rasterio-style grid from lower-left corner
@with_logger(var_name="logger_stream")
def create_grid_from_corners(
        y_ll, x_ll, rows, cols,res, grid_value: (float, int, np.nan) = 0.0,
        grid_check: bool=True, grid_format: str ='data_array', grid_name="grid"):

    """
    Create a rasterio-style grid from LOWER-LEFT corner.

    Returns:
    --------
    - If grid_format == 'data_array'  -> xarray.DataArray
    - If grid_format == 'dictionary'  -> structured dict with lon/lat/x/y arrays
    """
    # check grid value (if is defined by NoneType)
    if grid_value is None:
        grid_value = np.nan

    # build 1D coordinates (cell centers)
    # Longitude: west → east
    x_coords = x_ll + (np.arange(cols) + 0.5) * res

    # latitude: south → north, then flipped (north at top)
    y_coords_sn = y_ll + (np.arange(rows) + 0.5) * res
    y_coords = y_coords_sn[::-1]

    # build 2D meshgrid
    lon2d, lat2d = np.meshgrid(x_coords, y_coords)

    # check geographical information
    if grid_check:

        # check info start
        logger_stream.info("================ GRID CHECKS ================")

        # Grid shape
        logger_stream.info(f"Grid shape: {lon2d.shape}")
        # Corner coordinates
        logger_stream.info(f"Corner coordinates (lon, lat):")
        logger_stream.info(f" Top-left     : {lon2d[0, 0]} {lat2d[0, 0]}")
        logger_stream.info(f" Top-right    : {lon2d[0, -1], lat2d[0, -1]}")
        logger_stream.info(f" Bottom-left  : {lon2d[-1, 0], lat2d[-1, 0]}")
        logger_stream.info(f" Bottom-right : {lon2d[-1, -1], lat2d[-1, -1]}")

        # Orientation checks
        lat_ok = lat2d[0, 0] > lat2d[-1, 0]
        lon_ok = lon2d[0, 0] < lon2d[0, -1]

        logger_stream.info(f"Orientation:")
        logger_stream.info(f" Latitude decreases downward? {lat_ok}")
        logger_stream.info(f" Longitude increases rightward? {lon_ok}")

        # exit on orientation mismatch
        if not lat_ok:
            logger_stream.error("Latitude orientation incorrect")
            raise ValueError("Latitude orientation incorrect")
        if not lon_ok:
            logger_stream.error("Longitude orientation incorrect")
            raise ValueError("Longitude orientation incorrect")

        # Resolution checks
        dx = lon2d[0, 1] - lon2d[0, 0]
        dy = lat2d[0, 0] - lat2d[1, 0]

        logger_stream.info(f"Resolution:")
        logger_stream.info(f" dx {dx} expected {res}")
        logger_stream.info(f" dy {dy} expected {res}")

        # exit on resolution mismatch
        if not np.isclose(dx, res, atol=1e-8):
            logger_stream.error("Longitude resolution mismatch")
            raise ValueError("Longitude resolution mismatch")
        if not np.isclose(dy, res, atol=1e-8):
            logger_stream.error("Latitude resolution mismatch")
            raise ValueError("Latitude resolution mismatch")

        logger_stream.info(f"Grid checks passed successfully.")
        logger_stream.info(f"===========================================")

    # Return DataArray (default)
    if grid_format == 'data_array':

        # define empty data array
        data = np.full((rows, cols), grid_value)
        # create data array
        da = xr.DataArray(
            data,
            dims=("latitude", "longitude"),
            coords={
                "longitude": (("latitude", "longitude"), lon2d),
                "latitude": (("latitude", "longitude"), lat2d),
            },
            name=grid_name
        )

        return da

    # Return structured dictionary
    elif grid_format == 'dictionary':

        return {
            "lon2d": lon2d,
            "lat2d": lat2d,
            "x_coords": x_coords,
            "y_coords": y_coords,
            "shape": lon2d.shape,
            "resolution": res,
            "lower_left": (x_ll, y_ll),
        }

    else:
        # exit with error
        logger_stream.error(f"Unsupported format in grid creation from corners'{format}'.")
        raise ValueError(f"Unsupported format in grid creation from corner '{format}'.")

# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to help geo coordinates decreasing/increasing check
def is_increasing_1d(a) -> bool:
    da = np.diff(a)
    da = da[np.isfinite(da)]
    return bool(da.size == 0 or np.all(da > 0))
def is_decreasing_1d(a) -> bool:
    da = np.diff(a)
    da = da[np.isfinite(da)]
    return bool(da.size == 0 or np.all(da < 0))

# method to help in checking longitude in [-180, 180]
def lon_in_minus180_180(a) -> bool:
    aa = a[np.isfinite(a)]
    if aa.size == 0:
        return True
    return bool((aa.min() >= -180.0) and (aa.max() <= 180.0))
# method to help in converting longitude to [-180, 180)
def convert_lon_to_minus180_180(a):
    """Convert any longitude degrees array to [-180, 180)."""
    return ((a + 180.0) % 360.0) - 180.0
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to help geo coordinates name detection
@with_logger(var_name="logger_stream")
def find_geo_names(
    ds: xr.Dataset,
    lon_candidates=("lon", "longitude", "Longitude", "nav_lon", "X"),
    lat_candidates=("lat", "latitude", "Latitude", "nav_lat", "Y"),
) -> tuple[str, str]:
    """Return (lonv, latv) as they exist in the dataset (original names)."""

    def _find_var(cands):
        for c in cands:
            if c in ds.variables:
                return c
        low = {v.lower(): v for v in ds.variables}
        for c in cands:
            if c.lower() in low:
                return low[c.lower()]
        return None

    lonv = _find_var(lon_candidates)
    latv = _find_var(lat_candidates)

    # error messages
    if lonv is None or latv is None:
        logger_stream.error('Cannot find lon/lat variables.')
        raise KeyError(
            f"Cannot find lon/lat variables.\n"
            f"lon candidates: {list(lon_candidates)}\n"
            f"lat candidates: {list(lat_candidates)}\n"
            f"available variables: {sorted(list(ds.variables))}"
        )

    return lonv, latv
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to check geo coordinates to a reference DataArray
@with_logger(var_name="logger_stream")
def check_geo_orientation(ds: xr.Dataset, lonv: str, latv: str) -> dict:
    lon = ds[lonv]
    lat = ds[latv]

    report = {
        "lon_name": lonv,
        "lat_name": latv,
        "lon_dims": lon.dims,
        "lat_dims": lat.dims,
        "lon_ndim": int(lon.ndim),
        "lat_ndim": int(lat.ndim),
        "lat_south_to_north": None,
        "lon_west_to_east": None,
        "lon_in_-180_180": None,
    }

    # 1D case
    if lon.ndim == 1 and lat.ndim == 1:
        lon_vals = lon.values.astype(float)
        lat_vals = lat.values.astype(float)

        report["lat_south_to_north"] = is_increasing_1d(lat_vals)
        report["lon_west_to_east"] = is_increasing_1d(lon_vals)
        report["lon_in_-180_180"] = lon_in_minus180_180(lon_vals)

        return report

    # 2D case
    if lon.ndim == 2 and lat.ndim == 2:
        lon_vals = lon.values.astype(float)
        lat_vals = lat.values.astype(float)

        # latitude orientation by row-mean (along X)
        row_mean_lat = np.nanmean(lat_vals, axis=1)
        report["lat_south_to_north"] = is_increasing_1d(row_mean_lat)

        # longitude orientation by col-mean (along Y)
        col_mean_lon = np.nanmean(lon_vals, axis=0)
        report["lon_west_to_east"] = is_increasing_1d(col_mean_lon)

        report["lon_in_-180_180"] = lon_in_minus180_180(lon_vals.ravel())

        return report

    # error messages
    logger_stream.error(f"Unsupported lon/lat shapes: lon.ndim={lon.ndim}, lat.ndim={lat.ndim}. ")
    raise ValueError(
        f"Unsupported lon/lat shapes: lon.ndim={lon.ndim}, lat.ndim={lat.ndim}. "
        f"Expected both 1D or both 2D."
    )
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to fix geo coordinates to standard orientation
@with_logger(var_name="logger_stream")
def fix_geo_orientation(ds: xr.Dataset, lonv: str, latv: str) -> tuple[xr.Dataset, list[str]]:
    """
    Fix lon/lat orientation keeping the same names.
    Returns (ds_fixed, actions)
    """
    actions = []
    ds_out = ds

    lon = ds_out[lonv]
    lat = ds_out[latv]

    # case 1D lon/lat
    if lon.ndim == 1 and lat.ndim == 1:
        xdim = lon.dims[0]
        ydim = lat.dims[0]

        # flip latitude if descending
        lat_vals = ds_out[latv].values.astype(float)
        if is_decreasing_1d(lat_vals) and not is_increasing_1d(lat_vals):
            ds_out = ds_out.isel({ydim: slice(None, None, -1)})
            actions.append(f"flipped '{latv}' along dim '{ydim}' (S->N)")

        # convert lon range to [-180,180]
        lon_vals = ds_out[lonv].values.astype(float)
        if not lon_in_minus180_180(lon_vals):
            new_lon = convert_lon_to_minus180_180(lon_vals)
            ds_out = ds_out.assign_coords({lonv: (xdim, new_lon)})
            actions.append(f"converted '{lonv}' to [-180,180]")

            # sort longitudes increasing and reorder data accordingly
            order = np.argsort(ds_out[lonv].values)
            ds_out = ds_out.isel({xdim: order})
            actions.append(f"sorted '{lonv}' increasing along dim '{xdim}' (W->E)")

        # flip longitude if still decreasing
        lon_vals = ds_out[lonv].values.astype(float)
        if is_decreasing_1d(lon_vals) and not is_increasing_1d(lon_vals):
            ds_out = ds_out.isel({xdim: slice(None, None, -1)})
            actions.append(f"flipped '{lonv}' along dim '{xdim}' (W->E)")

        return ds_out, actions

    # case 2D lon/lat
    if lon.ndim == 2 and lat.ndim == 2:
        ydim, xdim = lat.dims

        # flip Y if mean latitude is descending
        row_mean = np.nanmean(ds_out[latv].values.astype(float), axis=1)
        if is_decreasing_1d(row_mean) and not is_increasing_1d(row_mean):
            ds_out = ds_out.isel({ydim: slice(None, None, -1)})
            actions.append(f"flipped '{latv}/{lonv}' along dim '{ydim}' (S->N)")

        # convert lon to [-180,180] (values only)
        lon_vals = ds_out[lonv].values.astype(float)
        if not lon_in_minus180_180(lon_vals.ravel()):
            new_lon = convert_lon_to_minus180_180(lon_vals)
            ds_out[lonv] = xr.DataArray(new_lon, dims=ds_out[lonv].dims, attrs=ds_out[lonv].attrs)
            actions.append(f"converted '{lonv}' values to [-180,180]")

        # flip X if mean lon is decreasing
        col_mean = np.nanmean(ds_out[lonv].values.astype(float), axis=0)
        if is_decreasing_1d(col_mean) and not is_increasing_1d(col_mean):
            ds_out = ds_out.isel({xdim: slice(None, None, -1)})
            actions.append(f"flipped '{latv}/{lonv}' along dim '{xdim}' (W->E)")

        return ds_out, actions

    # error messages
    logger_stream.error(f"Unsupported lon/lat shapes: lon.ndim={lon.ndim}, lat.ndim={lat.ndim}. ")
    raise ValueError(f"Unsupported lon/lat shapes: lon.ndim={lon.ndim}, lat.ndim={lat.ndim}. "
                    f"Expected both 1D or both 2D.")
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to match coordinates to a reference DataArray
@with_logger(var_name="logger_stream")
def match_coords_to_reference(
    da: xr.DataArray, ref: xr.DataArray,
    lat_name: str = "latitude", lon_name: str = "longitude",
    raise_on_bbox: bool = False,) -> xr.DataArray:
    """
    Return a copy of `da` whose latitude/longitude coordinates are replaced
    by those from `ref` (no interpolation, just relabelling) **only if**:

      - lat/lon sizes match
      - bbox (min/max lat/lon) matches within `bbox_tol`

    If coords are already equal (within tight tolerance), `da` is returned unchanged.

    If bbox matches but coords differ within tolerance, coords are snapped
    to `ref` and a WARNING is logged.

    If checks fail:
      - if raise_on_mismatch=True  -> ValueError
      - if raise_on_mismatch=False -> original `da` is returned unchanged
    """

    da = da.copy()

    # Extract coords from DataArrays
    if lat_name not in da.coords or lon_name not in da.coords:
        logger_stream.error(f"`da` has no coords '{lat_name}'/'{lon_name}'")
        raise ValueError(f"`da` has no coords '{lat_name}'/'{lon_name}'")

    if lat_name not in ref.coords or lon_name not in ref.coords:
        logger_stream.error(f"`ref` has no coords '{lat_name}'/'{lon_name}'")
        raise ValueError(f"`ref` has no coords '{lat_name}'/'{lon_name}'")

    da_lat = da.coords[lat_name].values
    da_lon = da.coords[lon_name].values
    ref_lat = ref.coords[lat_name].values
    ref_lon = ref.coords[lon_name].values

    # 1. Shape check
    if da_lat.size != ref_lat.size or da_lon.size != ref_lon.size:
        logger_stream.warning("=== Shape mismatch report ===")
        logger_stream.warning("Shape mismatch:")
        logger_stream.warning(f"  da:  ({lat_name}={da_lat.size}, {lon_name}={da_lon.size})")
        logger_stream.warning(f"  ref: ({lat_name}={ref_lat.size}, {lon_name}={ref_lon.size})")
        logger_stream.warning("============================")

        logger_stream.error('Shape mismatch between data and reference coordinates. Raising error.')
        raise ValueError('Shape mismatch between data and reference coordinates.\n' + msg)

    # 2. BBOX check
    da_bbox = (
        float(np.nanmin(da_lon)), float(np.nanmax(da_lon)),
        float(np.nanmin(da_lat)), float(np.nanmax(da_lat)),
    )
    ref_bbox = (
        float(np.nanmin(ref_lon)), float(np.nanmax(ref_lon)),
        float(np.nanmin(ref_lat)), float(np.nanmax(ref_lat)),
    )

    # Compute tolerance from reference grid spacing
    res_lon = np.nanmedian(np.abs(np.diff(ref_lon)))
    res_lat = np.nanmedian(np.abs(np.diff(ref_lat)))
    bbox_tol = max(res_lon, res_lat)  # half-cell tolerance

    # check bbox match within tolerance
    bbox_match = np.allclose(da_bbox, ref_bbox, rtol=0.0, atol=bbox_tol)
    # check bbox match
    if not bbox_match:

        logger_stream.warning("=== BBOX mismatch report ===")
        logger_stream.warning(f"BBOX mismatch (tol={bbox_tol}):")
        logger_stream.warning(f" da_bbox:  {da_bbox}")
        logger_stream.warning(f" ref_bbox: {ref_bbox}")
        logger_stream.warning(f" Snapping coords could made errors in geographical projections")
        logger_stream.warning("===========================")
        if raise_on_bbox:
            logger_stream.error('Data BBox mismatch. Tolerance exceeded. Raising error as requested.')
            raise ValueError('Data Bounding Box mismatch. Tolerance exceeded.')
        else:
            logger_stream.warning('Data BBox mismatch. Tolerance exceeded. Returning original data.')

    # 3. Coordinate equality check
    # use tighter tolerance than bbox (same grid)
    lat_equal = np.allclose(da_lat, ref_lat, rtol=0.0, atol=bbox_tol / 10.0)
    lon_equal = np.allclose(da_lon, ref_lon, rtol=0.0, atol=bbox_tol / 10.0)

    if lat_equal and lon_equal:
        # already aligned
        return da

    # 4. Snap coords to reference grid, with warning
    max_lat_diff = float(np.nanmax(np.abs(da_lat - ref_lat)))
    max_lon_diff = float(np.nanmax(np.abs(da_lon - ref_lon)))

    # info about the snapping
    logger_stream.warning("=== Lon/Lat alignment warning ===")
    logger_stream.warning("Coords not aligned but BBOX matches -> forcing alignment to reference grid")
    logger_stream.warning(f" max |delta_lat| = {max_lat_diff:.6g}")
    logger_stream.warning(f" max |delta_lon| = {max_lon_diff:.6g}")
    logger_stream.warning(f" bbox_tol       = {bbox_tol:.6g}")
    logger_stream.warning("================================")

    # assign reference coords
    da = da.assign_coords({
        lat_name: ref.coords[lat_name],
        lon_name: ref.coords[lon_name],
    })

    return da

# ----------------------------------------------------------------------------------------------------------------------
