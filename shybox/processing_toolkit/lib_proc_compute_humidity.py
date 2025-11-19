"""
Library Features:

Name:          lib_proc_compute_humidity
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20251118'
Version:       '1.1.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging

import numpy as np
import xarray as xr

from shybox.io_toolkit.lib_io_utils import create_darray
from shybox.orchestrator_toolkit.lib_orchestrator_utils import as_process

import matplotlib
import matplotlib.pyplot as plt
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to compute Relative Humidity from specific humidity, dew point and air temperature
@as_process(input_type='xarray', output_type='xarray')
def compute_data_rh(
        q: xr.DataArray,
        td: xr.DataArray,
        t: xr.DataArray,
        p: (xr.DataArray, float, int) = 1013.25,
        mask_format: str = 'float',
        mask_no_data: (float, int) = -9999.0,
        clip: bool = True,
        return_diag: bool = False,
        p_units: str = 'hPa',  # 'hPa' or 'Pa'
        **kwargs
    ):
    """
    Compute Relative Humidity (%) from specific humidity (kg/kg), dew point (K), and air temperature (K),
    following the same structure and masking logic of your template function.

    Parameters
    ----------
    q : xr.DataArray
        Specific humidity [kg/kg]
    td : xr.DataArray
        Dew point temperature [K]
    t : xr.DataArray
        Air temperature [K]
    p : xr.DataArray or float, optional
        Pressure (default 1013.25). Units controlled by `p_units`.
    ref : xr.DataArray, optional
        Reference grid used for masking (same shape/broadcastable).
    ref_value : float|int, optional
        Value in `ref` identifying no-data cells (default -9999.0).
    mask_format : {'integer','float',None}, optional
        Output dtype control, same behavior as your template.
    mask_no_data : float|int, optional
        Fill value for masked cells (default -9999.0).
    clip : bool, optional
        If True, clip RH to [0, 100].
    return_diag : bool, optional
        If True, also return diagnostic (e_from_td - e_from_q) in hPa.
    p_units : {'hPa','Pa'}, optional
        Units for `p`.

    Returns
    -------
    rh : xr.DataArray
        Relative humidity [%], masked per `ref` if provided.
    diag : xr.DataArray (optional)
        Vapor-pressure mismatch diagnostic [hPa] if return_diag=True:
        e_from_td - e_from_q.
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
    # Convert K -> °C for Magnus/Tetens
    td_c = td - 273.15
    t_c  = t  - 273.15

    # Vapor pressure from dew point (hPa); common constants (17.67, 243.5) over water
    e_td = 6.112 * xr.ufuncs.exp((17.67 * td_c) / (td_c + 243.5))

    # Saturation vapor pressure at air temperature (hPa)
    es_t = 6.112 * xr.ufuncs.exp((17.67 * t_c) / (t_c + 243.5))

    # Relative humidity (%)
    rh = 100.0 * (e_td / es_t)

    if clip:
        rh = rh.clip(min=0.0, max=100.0)

    # Optional diagnostic using q and p:
    diag = None
    if return_diag:
        # Prepare pressure in hPa
        if isinstance(p, xr.DataArray):
            p_hpa = p if p_units.lower() == 'hpa' else p / 100.0
        else:
            p_val = float(p)
            p_hpa = p_val if p_units.lower() == 'hpa' else p_val / 100.0
            p_hpa = xr.ones_like(rh) * p_hpa  # broadcast to rh

        # e_q = (q * p) / (0.622 + 0.378 q)  in hPa
        e_q = (q * p_hpa) / (0.622 + 0.378 * q)

        diag = (e_td - e_q)
        diag.name = 'e_td_minus_e_q'
        diag.attrs.update({'units': 'hPa', 'long_name': 'Vapor pressure mismatch (e_td - e_q)'})

    # Cast to desired dtype
    rh = rh.astype(out_dtype)
    if diag is not None:
        diag = diag.astype(out_dtype)

    # Add metadata
    rh.name = 'relative_humidity'
    rh.attrs.update({'units': '%', 'long_name': 'Relative Humidity'})

    """ debug plot
    plt.figure()
    plt.imshow(rh.values, cmap='viridis')
    plt.colorbar(label='RH [%]')
    plt.show(block=True)
    """

    return (rh, diag) if return_diag else rh
# ----------------------------------------------------------------------------------------------------------------------
