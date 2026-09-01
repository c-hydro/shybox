"""
Library Features:

Name:          lib_proc_compute
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20260827'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import numpy as np
import pandas as pd
import xarray as xr

from shybox.logging_toolkit.lib_logging_utils import with_logger
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# Method to compute average values over a spatial mask
def compute_average_over_mask(
        data: xr.DataArray, mask: xr.DataArray, mask_value: int = 1,
        time_dim: str ="time", y_dim: str ="latitude", x_dim: str ="longitude",
        var_name: str ='NA', **kwargs):

    # check input
    if not isinstance(data, xr.DataArray):
        raise TypeError(f"Object 'data' must be an xarray.DataArray. Got {type(data)}")

    # get variable name
    if var_name is None:
        var_name = data.name or "var"

    # get mask values
    mask_values = (mask.values if isinstance(mask, xr.DataArray) else np.asarray(mask))

    # check mask dimensions
    if mask_values.ndim != 2:
        raise ValueError(f"'mask' must be 2D. Found shape: {mask_values.shape}")

    # check spatial shape
    data_shape = (data.sizes[y_dim],data.sizes[x_dim],)
    # check mask value shape
    if mask_values.shape != data_shape:
        raise ValueError(f"Mask shape {mask_values.shape} does not match data spatial shape {data_shape}")

    # create valid mask
    mask_valid = (np.isfinite(mask_values) & (mask_values == mask_value))
    # create mask DataArray
    mask_da = xr.DataArray(mask_valid, dims=(y_dim, x_dim))

    # apply mask and compute spatial average
    data_avg = data.where(mask_da).mean(dim=(y_dim, x_dim),skipna=True)

    # 3D case: time series
    if time_dim in data_avg.dims:
        df_avg = pd.DataFrame({time_dim: data_avg[time_dim].values, var_name: data_avg.values})
    # 2D case: single average value
    else:
        df_avg = pd.DataFrame({var_name: [data_avg.item()]})

    return df_avg
# ----------------------------------------------------------------------------------------------------------------------
