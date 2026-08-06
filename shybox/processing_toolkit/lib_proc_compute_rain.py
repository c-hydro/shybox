"""
Library Features:

Name:          lib_proc_compute_rain
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20260624'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import xarray as xr

from shybox.orchestrator_toolkit.lib_orchestrator_utils_processes import as_process
from shybox.logging_toolkit.lib_logging_utils import with_logger
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# constants
M_TO_MM = 1000.0
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to convert rain units
@as_process(input_type='xarray', output_type='xarray')
@with_logger(var_name='logger_stream')
def convert_rain_units(
        da: xr.DataArray,
        to_mm: bool = True,
        units_attr: str = "units",
        **kwargs
) -> xr.DataArray:

    da = da.copy()
    units = (da.attrs.get(units_attr, "") or "").lower()

    # stats before conversion
    min_before = float(da.min(skipna=True).values)
    max_before = float(da.max(skipna=True).values)

    # info data start
    logger_stream.info_up(
        f'Rain values BEFORE conversion :: '
        f'min={min_before:.6f}, max={max_before:.6f}, units="{units}"'
    )

    if to_mm and units in ("m", "meter", "metre", "meters", "metres", ""):

        da.values = da.values * 1000.0
        da.attrs[units_attr] = "mm"

    elif not to_mm and units in (
            "mm", "millimeter", "millimetre",
            "millimeters", "millimetres"):

        da.values = da.values / 1000.0
        da.attrs[units_attr] = "m"

    else:
        logger_stream.warning(
            f"Rain units conversion skipped. "
            f"Current units='{units}', to_mm={to_mm}"
        )

    # stats after conversion
    min_after = float(da.min(skipna=True).values)
    max_after = float(da.max(skipna=True).values)

    # info data end
    logger_stream.info_down(
        f'Rain values AFTER conversion :: '
        f'min={min_after:.6f}, max={max_after:.6f}, '
        f'units="{da.attrs.get(units_attr, "unknown")}"'
    )

    # add attributes after applying method
    da.attrs["processing"] = (
        "Interval accumulation in millimeters"
        if to_mm else
        "Interval accumulation in meters"
    )

    return da
# ----------------------------------------------------------------------------------------------------------------------