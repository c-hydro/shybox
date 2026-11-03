"""
Library Features:

Name:          lib_proc_compute_radiation
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20251121'
Version:       '1.2.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging

import pandas as pd
import numpy as np
import xarray as xr

from shybox.io_toolkit.lib_io_utils import create_darray
from shybox.orchestrator_toolkit.lib_orchestrator_utils_processes import as_process
from shybox.logging_toolkit.lib_logging_utils import with_logger

import matplotlib
import matplotlib.pyplot as plt
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# default lookup table for cloud factor against rain values
lookup_table_cf_default = {
    'CF_L1': {'Rain': [0, 1],       'CloudFactor': [0.95]},
    'CF_L2': {'Rain': [1, 3],       'CloudFactor': [0.75]},
    'CF_L3': {'Rain': [3, 5],       'CloudFactor': [0.65]},
    'CF_L4': {'Rain': [5, 10],      'CloudFactor': [0.50]},
    'CF_L5': {'Rain': [10, None],   'CloudFactor': [0.15]}
}
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to compute incoming radiation (from accumulated ssrd/strd)
@as_process(input_type='xarray', output_type='xarray')
def compute_data_incoming_radiation(
    ssrd: xr.DataArray | None = None,
    strd: xr.DataArray | None = None,
    time: pd.Timestamp | str | None = None,
    time_delta: str = "1h",
    time_dim: str = "time",
    kind: str = "k",
    midpoint: bool = True,
    clip_sw_min: float = 0.0,
    var_name_k: str = "incoming_radiation",
    var_name_l: str = "incoming_longwave_radiation",
    var_name_r: str = "incoming_total_radiation",
    **kwargs,
):
    """
    Convert a single accumulated IFS/ERA5 ssrd/strd field (J m-2) to incoming
    radiation fluxes (W m-2) for a *single interval* (2D case).

    This version only supports 2D inputs WITHOUT a time dimension:
      - input: lat x lon (or similar)
      - output: same dims, with a scalar time coordinate (not a dimension)

    Parameters
    ----------
    ssrd : xr.DataArray or None
        Accumulated surface solar radiation downwards [J m-2].
        Required for kind in {"k", "r", "w", "all"}.
        Must NOT have dimension `time_dim`.
    strd : xr.DataArray or None
        Accumulated surface thermal radiation downwards [J m-2].
        Required for kind in {"l", "r", "w", "all"}.
        Must NOT have dimension `time_dim`.
    time : pd.Timestamp or str
        Time of the *end* of the accumulation interval.
        Required (no default from data, since there is no time dimension).
    time_delta : str
        Length of the accumulation interval, e.g. "1h".
    time_dim : str
        Name of the time coordinate in the output (scalar coordinate).
    kind : {"k", "l", "r", "w", "all"}
        Which flux to return:
        - "k": shortwave incoming (K_in_sw)
        - "l": longwave incoming (L_in_lw)
        - "r" or "w": total incoming (R_in = K + L)
        - "all": dict with all three DataArrays
    midpoint : bool
        If True: output time = time - 0.5 * time_delta (center of interval)
        If False: output time = time (end of interval)
    clip_sw_min : float
        Minimum value for shortwave (to remove tiny negatives at night).
        If None, no clipping is applied.
    var_name_k, var_name_l, var_name_r : str
        Output variable names.

    Returns
    -------
    xr.DataArray or dict[str, xr.DataArray]
        Depending on `kind`. All outputs have:
        - same dimensions as input (lat, lon, ...)
        - a scalar coordinate with name `time_dim`.
    """
    if kind not in {"k", "l", "r", "w", "all"}:
        raise ValueError("kind must be one of {'k', 'l', 'r', 'w', 'all'}")

    # choose reference array to inspect shape/dims
    ref_da = ssrd if ssrd is not None else strd
    if ref_da is None:
        raise ValueError("At least one of ssrd or strd must be provided.")

    # This function is ONLY for 2D-like data: must NOT have time_dim as a dimension
    if time_dim in ref_da.dims:
        raise ValueError(
            f"This 2D version does not support a '{time_dim}' dimension. "
            "Provide a single accumulated field without time dimension."
        )

    if time is None:
        raise ValueError(
            "For 2D inputs (no time dimension), 'time' must be provided "
            "as the end of the accumulation interval."
        )

    td = pd.to_timedelta(time_delta)
    dt_seconds = float(td.total_seconds())

    t_end = pd.to_datetime(time)
    t_out = t_end - td / 2 if midpoint else t_end

    out: dict[str, xr.DataArray] = {}

    # Shortwave
    if kind in {"k", "r", "w", "all"}:
        if ssrd is None:
            raise ValueError("ssrd must be provided for kind 'k', 'r', 'w', or 'all'.")

        K_in_sw = (ssrd / dt_seconds).rename(var_name_k)
        # Attach time as a scalar coordinate (NOT a dimension)
        K_in_sw = K_in_sw.assign_coords({time_dim: t_out})

        if clip_sw_min is not None:
            K_in_sw = K_in_sw.clip(min=clip_sw_min)

        out[var_name_k] = K_in_sw

    # Longwave
    if kind in {"l", "r", "w", "all"}:
        if strd is None:
            raise ValueError("strd must be provided for kind 'l', 'r', 'w', or 'all'.")

        L_in_lw = (strd / dt_seconds).rename(var_name_l)
        L_in_lw = L_in_lw.assign_coords({time_dim: t_out})

        out[var_name_l] = L_in_lw

    # Total
    if kind in {"r", "w", "all"}:
        if (var_name_k not in out) or (var_name_l not in out):
            raise ValueError(
                "Both ssrd and strd must be provided for kind 'r', 'w', or 'all'."
            )
        R_in = (out[var_name_k] + out[var_name_l]).rename(var_name_r)
        R_in = R_in.assign_coords({time_dim: t_out})
        out[var_name_r] = R_in

    # Return according to `kind`
    if kind == "k":
        return out[var_name_k]
    elif kind == "l":
        return out[var_name_l]
    elif kind in {"r", "w"}:
        return out[var_name_r]
    else:
        return out

# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to compute look-up table for a single 2D rain field
def apply_cloud_factor_lut(var_data_in,
                           lookup_table=None,
                           var_in='Rain',
                           var_out='CloudFactor'):
    """
    Apply a rain→cloud-factor LUT to a 2D field.

    Parameters
    ----------
    var_data_in : 2D ndarray (y, x)
        Rain field for a single timestep.
    lookup_table : dict, optional
        LUT with ranges + output values. If None, use lookup_table_cf_default.
    var_in : str
        Key name of input variable in the LUT.
    var_out : str
        Key name of output variable in the LUT.

    Returns
    -------
    var_data_out : 2D ndarray (y, x)
        Cloud factor for the given timestep.
    """
    if lookup_table is None:
        lookup_table = lookup_table_cf_default

    var_data_in = np.float32(var_data_in)
    var_idx_nan = np.argwhere(np.isnan(var_data_in))

    var_data_out = np.zeros(var_data_in.shape, dtype=np.float32)

    for lu_table_key, lu_table_value in lookup_table.items():
        lu_var_in = lu_table_value[var_in]
        lu_var_out = lu_table_value[var_out]

        if len(lu_var_in) == 2 and len(lu_var_out) == 1:
            lu_var_in_min = lu_var_in[0]
            lu_var_in_max = lu_var_in[1]
            lu_value_out = lu_var_out[0]

            if (lu_var_in_min is not None) and (lu_var_in_max is not None):
                mask = ((var_data_in >= lu_var_in_min) &
                        (var_data_in <  lu_var_in_max))
            elif (lu_var_in_min is not None) and (lu_var_in_max is None):
                mask = (var_data_in >= lu_var_in_min)
            elif (lu_var_in_min is None) and (lu_var_in_max is not None):
                mask = (var_data_in <= lu_var_in_max)
            else:
                log_stream.error(' ===> LookUp condition not available')
                raise NotImplementedError('LookUp condition not available')

            var_data_out[mask] = lu_value_out
        else:
            log_stream.error(' ===> LookUp variable range not available')
            raise NotImplementedError('LookUp variable range not available')

    # restore NaNs
    if var_idx_nan.size > 0:
        var_data_out[var_idx_nan[:, 0], var_idx_nan[:, 1]] = np.nan

    return var_data_out
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to compute cloud attenuation factor using time series of rain
def compute_cloud_factor(data_rain, lookup_table_cf=None):
    """
    Compute cloud attenuation factor from rain.

    Parameters
    ----------
    data_rain : 3D ndarray (time, y, x)
        Rain field.
    lookup_table_cf : dict, optional
        LUT; if None, use lookup_table_cf_default.

    Returns
    -------
    data_cf : 3D ndarray (time, y, x)
        Cloud factor (dimensionless).
    """
    if lookup_table_cf is None:
        lookup_table_cf = lookup_table_cf_default

    ny, nx = data_rain.shape

    data_cf = np.full((ny, nx), np.nan, dtype=np.float32)
    data_cf = apply_cloud_factor_lut(
        data_rain, lookup_table_cf, 'Rain', 'CloudFactor'
    )

    return data_cf
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to define parameters for astronomic radiation computation
def define_parameters(geo_x, geo_y):
    """
    Define FAO-like astronomic radiation parameters.

    Parameters
    ----------
    geo_x : ndarray
        Longitude [deg east].
    geo_y : ndarray
        Latitude [deg north].

    Returns
    -------
    geo_lz, geo_lm, geo_phi, arad_param_gsc, arad_param_as, arad_param_bs
    """
    # degree to rad factor
    tor = np.pi / 180.0

    # gsc solar constant = MJ m-2 day-1
    gsc = 118.08
    # gsc solar constant = MJ m-2 min-1
    arad_param_gsc = gsc / (60.0 * 24.0)

    # longitude of the centre of the local time zone (deg east)
    geo_lz = np.round(geo_x / 15.0) * 15.0

    # longitude of the measurement site [degrees west of Greenwich]
    geo_lm = 360.0 - geo_x

    # latitude [rad]
    geo_phi = geo_y * tor

    # astronomic parameters
    arad_param_as = 0.65
    # NOTE: this keeps your original value: 2.0 * 10e-5 = 2e-4
    arad_param_bs = 2.0 * 10e-5

    return geo_lz, geo_lm, geo_phi, arad_param_gsc, arad_param_as, arad_param_bs
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to compute astronomic radiation (by FAO algorithm)
def exec_astronomic_radiation(
    var_data_cf,
    time,          # pd.Timestamp
    geo_lz, geo_lm, geo_phi,
    param_gsc, param_as, param_bs=None,
    geo_z=None, time_delta=pd.Timedelta('1h'),
    return_mode="both"  # "both", "ar", "k"
):
    """
    FAO astronomic radiation for a single time interval.
    return_mode:
        "both" -> return (ar, k)
        "ar"   -> return only extraterrestrial radiation
        "k"    -> return only clear-sky radiation
    """

    if isinstance(time_delta, str):
        time_delta = pd.Timedelta(time_delta)

    if param_bs is None:
        param_bs = 0.0005  # FAO-56 default

    seconds_delta = time_delta.total_seconds()
    dt_mid_hours = seconds_delta / 3600.0
    minutes_input_step = seconds_delta / 60.0

    # midpoint of interval
    time_mid = time - time_delta / 2
    hour_mid = (
        time_mid.hour +
        time_mid.minute / 60.0 +
        time_mid.second / 3600.0
    )
    doy_mid = time_mid.dayofyear

    # Earth–Sun distance factor
    ird = 1.0 + 0.033 * np.cos(2 * np.pi / 365.0 * doy_mid)

    b = 2 * np.pi * (doy_mid - 81) / 364.0

    solar_corr = (
        0.1645 * np.sin(2 * b)
        - 0.1255 * np.cos(b)
        - 0.025  * np.sin(b)
    )

    solar_decl = 0.4093 * np.sin(2 * np.pi / 365.0 * doy_mid - 1.405)

    # solar time angle
    omega_mid = np.pi / 12.0 * (
        hour_mid + 0.06667 * (geo_lz - geo_lm) + solar_corr - 12.0
    )

    # start & end of interval
    omega_start = omega_mid - np.pi * dt_mid_hours / 24.0
    omega_end   = omega_mid + np.pi * dt_mid_hours / 24.0

    # extraterrestrial radiation (Duffie & Beckman)
    var_model_ar = (
        12.0 * minutes_input_step / np.pi * param_gsc * ird *
        (
            (omega_end - omega_start) * np.sin(geo_phi) * np.sin(solar_decl)
            + np.cos(geo_phi) * np.cos(solar_decl)
            * (np.sin(omega_end) - np.sin(omega_start))
        )
    )

    # MJ/m2 → W/m2
    var_model_ar = var_model_ar * 1e6 / seconds_delta
    var_model_ar[var_model_ar <= 0] = 0
    var_model_ar[np.isnan(var_data_cf)] = np.nan
    var_model_ar = var_model_ar.astype(np.float32)

    # -----------------------------------------
    # Clear-sky radiation
    # -----------------------------------------
    if geo_z is None:
        cs_factor = param_as
    else:
        cs_factor = param_as + param_bs * geo_z

    var_model_k = (var_data_cf * cs_factor * var_model_ar).astype(np.float32)

    # -----------------------------------------
    # Output selector
    # -----------------------------------------
    return_mode = return_mode.lower()

    if return_mode == "ar":
        return var_model_ar
    elif return_mode == "k":
        return var_model_k
    elif return_mode == "both":
        return var_model_ar, var_model_k
    else:
        raise ValueError("return_mode must be 'both', 'ar', or 'k'")
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# XARRAY PROCESS WRAPPER (same style as your other compute_data_* processes)
@as_process(input_type='xarray', output_type='xarray')
def compute_data_astronomic_radiation(
        rain: xr.DataArray,
        geo_z: xr.DataArray = None,
        geo_x: xr.DataArray = None,
        geo_y: xr.DataArray = None,
        time: pd.Timestamp = None,
        time_delta: str = '1h',
        lookup_table_cf: dict = None,
        mask_format: str = 'float',
        mask_no_data: (float, int) = -9999.0,
        clip: bool = True,
        return_diag: bool = False,
        return_vars='k',
        var_name_k='incoming_radiation',
        var_name_ar='astronomic_radiation',
        var_name_x='longitude',
        var_name_y='latitude',
        **kwargs
):
    """
    Compute clear-sky shortwave radiation (k) and astronomic radiation (ar),
    optionally also returning cloud factor (cf).

    Parameters
    ----------
    rain : xr.DataArray
        Rainfall field used to derive cloud factor.
    geo_z, geo_x, geo_y : xr.DataArray, optional
        Elevation and coordinates. If geo_x/geo_y are None, they
        are taken from rain[var_name_x], rain[var_name_y].
    time : pd.Timestamp
        Reference time.
    time_delta : str
        Time step (e.g. '1h').
    lookup_table_cf : dict, optional
        Lookup table for cloud factor from rain.
    mask_format : {'float', 'integer', None}
        Output dtype logic.
    mask_no_data : float or int
        No-data value (used only for consistency with other routines).
    clip : bool
        If True, clip k >= 0.
    return_diag : bool
        Backward-compatible: if True and return_vars is None,
        return (k, ar).
    return_vars : None, str, or sequence of str
        If None:
            - if return_diag is False: return k
            - if return_diag is True : return (k, ar)
        If not None:
            - One variable  -> return a single DataArray
            - Many variables -> return list of DataArray

        Allowed variable names: 'k', 'ar', 'cf'.

    var_name_k, var_name_ar : str or None
        Names for the output DataArrays.
    var_name_x, var_name_y : str
        Spatial dimension / coord names.

    Returns
    -------
    xr.DataArray or list[xr.DataArray] or tuple[xr.DataArray]
        Depending on return_vars / return_diag.
    """

    # ---- mask_format logic, same style as your other processes ----
    if mask_format is not None:
        if mask_format == 'integer':
            if mask_no_data == np.nan:
                raise RuntimeError('Change the no_data value or the grid format')

    if mask_format is not None:
        if mask_format == 'integer':
            out_dtype = int
        elif mask_format == 'float':
            out_dtype = float
        else:
            raise NotImplementedError(f'Mask type "{mask_format}" not implemented yet')
    else:
        out_dtype = float

    # 1) Cloud factor from rain (all numpy, shape (y, x))
    rain_np = np.asarray(rain.values, dtype=np.float32)
    data_cf = compute_cloud_factor(rain_np, lookup_table_cf=lookup_table_cf)

    cf = xr.DataArray(
        data_cf,
        coords=rain.coords,
        dims=rain.dims,
        name='cloud_factor'
    )
    cf.attrs.update({'units': '-', 'long_name': 'Cloud attenuation factor'})

    # 2) Coordinates handling
    if geo_x is None:
        if var_name_x in rain.dims:
            geo_x_values = rain[var_name_x].values
        else:
            raise RuntimeError('Geo coordinates x not found in rain DataArray')
    else:
        geo_x_values = geo_x.values

    if geo_y is None:
        if var_name_y in rain.dims:
            geo_y_values = rain[var_name_y].values
        else:
            raise RuntimeError('Geo coordinates y not found in rain DataArray')
    else:
        geo_y_values = geo_y.values

    if geo_z is None:
        geo_z_values = None
    else:
        geo_z_values = geo_z.values

    # Ensure coordinates are 2D
    # Case 1: both are 1D → meshgrid
    if geo_x_values.ndim == 1 and geo_y_values.ndim == 1:
        geo_x_values, geo_y_values = np.meshgrid(geo_x_values, geo_y_values)

    # Case 2: one is 2D and the other is 1D → error (inconsistent)
    elif geo_x_values.ndim != geo_y_values.ndim:
        raise RuntimeError(
            f"Inconsistent coordinate dimensions: "
            f"longitude ndim={geo_x_values.ndim}, latitude ndim={geo_y_values.ndim}"
        )

    # Case 3: both are already 2D → leave as is
    elif geo_x_values.ndim == 2 and geo_y_values.ndim == 2:
        pass

    # Any other unexpected shapes
    else:
        raise RuntimeError(
            f"Unexpected coordinate shapes: "
            f"longitude.shape={geo_x_values.shape}, latitude.shape={geo_y_values.shape}"
        )

    # 3) Define FAO parameters
    geo_lz, geo_lm, geo_phi, param_gsc, param_as, param_bs = define_parameters(
        geo_x_values, geo_y_values
    )

    # 4) FAO astronomic radiation core
    var_model_ar, var_model_k = exec_astronomic_radiation(
        var_data_cf=data_cf,  # (y, x)
        geo_z=geo_z_values,  # (y, x)
        time=time,
        time_delta=time_delta,
        geo_lz=geo_lz,
        geo_lm=geo_lm,
        geo_phi=geo_phi,
        param_gsc=param_gsc,
        param_as=param_as,
        param_bs=param_bs
    )

    # 5) Wrap back to xarray
    if var_name_ar is None:
        var_name_ar = 'astronomic_radiation'

    ar = xr.DataArray(
        var_model_ar,
        coords=rain.coords,
        dims=rain.dims,
        name=var_name_ar
    )
    ar.attrs.update({
        'units': 'W m-2',
        'long_name': 'Astronomic (extraterrestrial) shortwave radiation at TOA'
    })

    if var_name_k is None:
        var_name_k = 'shortwave_radiation_clear_sky'

    k = xr.DataArray(
        var_model_k,
        coords=rain.coords,
        dims=rain.dims,
        name=var_name_k
    )
    k.attrs.update({
        'units': 'W m-2',
        'long_name': 'Clear-sky shortwave radiation at surface'
    })

    if clip:
        k = k.clip(min=0.0)

    # 6) Final dtype casting
    k = k.astype(out_dtype)
    ar = ar.astype(out_dtype)
    cf = cf.astype(out_dtype)

    # 7) Decide what to return
    # ------------------------------------------------------------------
    # Behaviour:
    #   - If return_vars is None:
    #         - old behaviour: return k or (k, ar)
    #   - If return_vars is not None:
    #         - collect requested variables into a list
    #         - if len(list) == 1 -> return list[0]
    #         - else              -> return list
    # ------------------------------------------------------------------
    if return_vars is None:
        # Backward-compatible behaviour
        if return_diag:
            # Keep original (k, ar) behaviour here
            return k, ar
        else:
            return k

    # Normalise return_vars to iterable
    if isinstance(return_vars, str):
        return_vars = (return_vars,)

    outputs = []
    for name in return_vars:
        if name == 'k':
            outputs.append(k)
        elif name == 'ar':
            outputs.append(ar)
        elif name == 'cf':
            outputs.append(cf)
        else:
            raise ValueError(
                f'Unknown variable "{name}". Allowed: "k", "ar", "cf".'
            )

    # If only one variable requested → return DataArray, not list
    if len(outputs) == 1:
        return outputs[0]

    # Many variables → return list in the same order as return_vars
    return outputs

# ----------------------------------------------------------------------------------------------------------------------
