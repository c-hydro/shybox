"""
Library Features:

Name:          lib_proc_compute_wind
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20251118'
Version:       '1.1.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging

import numpy as np
import xarray as xr

from shybox.orchestrator_toolkit.lib_orchestrator_utils_processes import as_process
from shybox.logging_toolkit.lib_logging_utils import with_logger

import matplotlib
import matplotlib.pyplot as plt
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to compute Wind Speed from u, v wind components
@as_process(input_type='xarray', output_type='xarray')
def compute_data_wind_speed(
        u: xr.DataArray,
        v: xr.DataArray,
        mask_format: str = 'float',
        mask_no_data: (float, int) = -9999.0,
        clip: bool = True,
        return_diag: bool = False,
        name: (str, None) = 'wind_speed',
        **kwargs
    ):
    """
    Compute wind speed (m s-1) from u, v wind components, following the same
    structure and masking/dtype logic as `compute_data_rh`.

    Parameters
    ----------
    u : xr.DataArray
        Zonal wind component [m s-1], positive toward east.
    v : xr.DataArray
        Meridional wind component [m s-1], positive toward north.
    mask_format : {'integer','float',None}, optional
        Output dtype control, same behavior as your other processes.
        - 'integer' -> cast to int
        - 'float'   -> cast to float
        - None      -> leave as float
    mask_no_data : float|int, optional
        Fill value for masked cells (currently only checked for consistency
        when mask_format='integer'.
    clip : bool, optional
        If True, clip wind speed to be >= 0. (No upper bound.)
    return_diag : bool, optional
        If True, also return wind direction (mathematical convention) [deg]:
        0° = east, 90° = north, counter-clockwise positive.

    Returns
    -------
    ws : xr.DataArray
        Wind speed [m s-1].
    direction : xr.DataArray, optional
        Wind direction TO which the wind blows [deg] if return_diag=True,
        mathematical convention (0°=east, 90°=north, CCW positive).
    """

    # ---- Input checks mirroring your structure ----
    if mask_format is not None:
        if mask_format == 'integer':
            if mask_no_data == np.nan:
                raise RuntimeError('Change the no_data value or the grid format')

    # Coerce dtypes per mask_format
    if mask_format is not None:
        if mask_format == 'integer':
            out_dtype = int
        elif mask_format == 'float':
            out_dtype = float
        else:
            raise NotImplementedError(f'Mask type "{mask_format}" not implemented yet')
    else:
        out_dtype = float

    # ---- Core computation (vectorized with xarray) ----
    # Wind speed magnitude
    ws = xr.ufuncs.sqrt(u ** 2 + v ** 2)

    if clip:
        ws = ws.clip(min=0.0)

    # Optional diagnostic: wind direction (TO, math convention)
    direction = None
    if return_diag:
        # atan2(v, u): angle from +x (east) toward +y (north), CCW positive
        theta_rad = xr.ufuncs.atan2(v, u)
        theta_deg = theta_rad * 180.0 / np.pi

        # Map to [0, 360)
        direction = (theta_deg + 360.0) % 360.0
        direction.name = 'wind_direction_to'
        direction.attrs.update({
            'units': 'degrees',
            'long_name': 'Wind direction (to, 0°=east, 90°=north, CCW positive)'
        })

    # Cast to desired dtype
    ws = ws.astype(out_dtype)
    if direction is not None:
        direction = direction.astype(out_dtype)

    # Add metadata
    if name is not None:
        ws.name = name
    else:
        ws.name = 'wind_speed'
    ws.attrs.update({'units': 'm s-1', 'long_name': 'Wind speed'})

    """ debug plot
    plt.figure()
    plt.imshow(ws.values, cmap='viridis')
    plt.colorbar(label='Wind speed [m s-1]')
    plt.show(block=True)
    """

    return (ws, direction) if return_diag else ws
# ----------------------------------------------------------------------------------------------------------------------
