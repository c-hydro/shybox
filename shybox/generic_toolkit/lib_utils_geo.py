"""
Library Features:

Name:          lib_utils_geo
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20251120'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import numpy as np
import xarray as xr

from shybox.logging_toolkit.lib_logging_utils import with_logger
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
        msg = (
            f"[match_coords_to_reference] Shape mismatch:\n"
            f"  da:  ({lat_name}={da_lat.size}, {lon_name}={da_lon.size})\n"
            f"  ref: ({lat_name}={ref_lat.size}, {lon_name}={ref_lon.size})"
        )
        logger_stream.error(msg)
        raise ValueError(msg)

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

    bbox_match = np.allclose(da_bbox, ref_bbox, rtol=0.0, atol=bbox_tol)

    if not bbox_match:
        msg = (
            f"[match_coords_to_reference] BBOX mismatch (tol={bbox_tol}):\n"
            f"  da_bbox:  {da_bbox}\n"
            f"  ref_bbox: {ref_bbox}\n"
            "  -> Snapping coords could made errors in geographical projections"
        )
        if raise_on_bbox:
            logger_stream.error(msg)
            raise ValueError(msg)
        else:
            logger_stream.warning(msg)

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

    logger_stream.warning(
        "[match_coords_to_reference] Coords not aligned but BBOX matches; "
        "forcing alignment to reference grid.\n"
        f"  max |Δlat| = {max_lat_diff:.6g}, max |Δlon| = {max_lon_diff:.6g}\n"
        f"  bbox_tol = {bbox_tol:.6g}"
    )

    da = da.assign_coords({
        lat_name: ref.coords[lat_name],
        lon_name: ref.coords[lon_name],
    })

    return da

# ----------------------------------------------------------------------------------------------------------------------
