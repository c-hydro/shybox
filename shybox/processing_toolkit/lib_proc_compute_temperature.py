"""
Library Features:

Name:          lib_proc_compute_temperature
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20251118'
Version:       '1.1.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import xarray as xr

from shybox.orchestrator_toolkit.lib_orchestrator_utils_processes import as_process
from shybox.logging_toolkit.lib_logging_utils import with_logger

import matplotlib
import matplotlib.pyplot as plt

# define constants
KELVIN_OFFSET = 273.15
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to convert air temperature units
@as_process(input_type='xarray', output_type='xarray')
@with_logger(var_name='logger_stream')
def convert_temperature_units(da: xr.DataArray,
                              to_celsius: bool = True, units_attr: str = "units",
                              **kwargs) -> xr.DataArray:
    da = da.copy()
    units = (da.attrs.get(units_attr, "") or "").lower()

    if to_celsius and units in ("k", "kelvin", ""):
        da.values = da.values - KELVIN_OFFSET
        da.attrs[units_attr] = "C"
    elif not to_celsius and units in ("c", "degc", "celsius", ""):
        da.values = da.values + KELVIN_OFFSET
        da.attrs[units_attr] = "K"

    """ debug plot
    plt.figure()
    plt.imshow(da.values, cmap='viridis')
    plt.colorbar(label='Masked Data')
    plt.show(block=True)
    """

    return da
# ----------------------------------------------------------------------------------------------------------------------
