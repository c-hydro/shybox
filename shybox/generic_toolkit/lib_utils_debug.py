"""
Library Features:

Name:          lib_utils_debug
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20251110'
Version:       '1.0.0'
"""
# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
logging.getLogger('matplotlib.font_manager').setLevel(logging.WARNING)
logging.getLogger('matplotlib.colorbar').setLevel(logging.WARNING)

import os
import numpy as np
import xarray as xr
import matplotlib.pyplot as plt

from typing import Optional, Dict, Any, Union

from shybox.logging_toolkit.lib_logging_utils import with_logger
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to plot xarray.Dataset variables for debugging
@with_logger(var_name="logger_stream")
def plot_data(data, var_name=None, cmap="viridis", title=None):
    # --- Handle NumPy ndarray case ---
    if isinstance(data, np.ndarray):
        if data.ndim != 2:
            logger_stream.error("NumPy array must be 2D.")
            raise TypeError(f"Unsupported NumPy array shape: {data.shape} (expect 2D)")

        var_label = var_name or "Array"
        plt.figure()
        plt.title(f"{title} - {var_label}" if title else var_label)
        im = plt.imshow(data, cmap=cmap)
        cbar = plt.colorbar(im)
        cbar.set_label(var_label)
        plt.show(block=True)
        return

    # --- Handle xarray.DataArray case ---
    if isinstance(data, xr.DataArray):
        var_label = data.name or "DataArray"
        plt.figure()
        plt.title(f"{title} - {var_label}" if title else var_label)
        im = plt.imshow(data.values, cmap=cmap)
        cbar = plt.colorbar(im)
        cbar.set_label(var_label)
        plt.show(block=True)
        return

    # --- Handle xarray.Dataset or dict-like case ---
    if not hasattr(data, "items"):
        logger_stream.error("Data must be an xarray.Dataset, xarray.DataArray, dict-like, or a 2D NumPy array.")
        raise TypeError(f"Unsupported data type: {type(data)}")

    # Select which variables to plot
    if var_name:
        if var_name not in data:
            logger_stream.info(f"Variable '{var_name}' not found in dataset.")
            raise KeyError(f"Variable '{var_name}' not found in dataset.")
        vars_to_plot = [var_name]
    else:
        vars_to_plot = [
            name for name, da in data.items()
            if hasattr(da, "ndim") and da.ndim == 2
        ]
        if not vars_to_plot:
            logger_stream.warning("No 2D variables found to plot.")
            return

    # Plot each selected variable
    for name in vars_to_plot:
        da = data[name]
        plt.figure()
        plt.title(f"{title} - {name}" if title else name)
        im = plt.imshow(da.values, cmap=cmap)
        cbar = plt.colorbar(im)
        cbar.set_label(name)
        plt.show(block=True)

# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to dump xarray Dataset/DataArray to NetCDF with debug reporting
@with_logger(var_name="logger_stream")
def dump_data2nc(
    obj: Union[xr.Dataset, xr.DataArray],
    path: str = "test.nc",
    *,
    var_name: Optional[str] = None,
    overwrite: bool = True,
    compression: bool = True,
    complevel: int = 5,
    engine: str = "netcdf4",
    nc_format: str = "NETCDF4",
    extra_encoding: Optional[Dict[str, Dict[str, Any]]] = None,
    readback: bool = True,
    show_report: bool = True,
) -> Optional[xr.Dataset]:
    """
    Dump a Dataset/DataArray to NetCDF with debug reporting.

    Parameters
    ----------
    obj : xr.Dataset | xr.DataArray
        The object to write.
    path : str
        Output NetCDF path.
    var_name : str | None
        If obj is a DataArray, the name to use when converting to Dataset.
        If None, uses obj.name or 'data'.
    overwrite : bool
        Remove existing file at 'path' before writing.
    compression : bool
        Apply zlib compression to data variables.
    complevel : int
        Compression level (0–9) if compression is True.
    engine : str
        NetCDF engine ('netcdf4', 'h5netcdf', or 'scipy' for classic).
    nc_format : str
        NetCDF format passed to xarray ('NETCDF4', 'NETCDF4_CLASSIC', etc.).
    extra_encoding : dict | None
        Per-variable encoding overrides, e.g. {'var': {'_FillValue': -9999}}.
    readback : bool
        Re-open the file after writing to verify.
    show_report : bool
        Print a human-readable summary.

    Returns
    -------
    xr.Dataset | None
        The re-opened dataset if readback=True, else None.
    """
    # Normalize to a Dataset
    if isinstance(obj, xr.DataArray):
        name = var_name or (obj.name if obj.name is not None else "data")
        ds = obj.to_dataset(name=name)
    elif isinstance(obj, xr.Dataset):
        ds = obj
    else:
        logger_stream.error("obj must be an xarray.Dataset or xarray.DataArray.")
        raise TypeError("obj must be an xarray.Dataset or xarray.DataArray")

    # Prepare encodings
    enc: Dict[str, Dict[str, Any]] = {}

    # Start from user-provided overrides (if any)
    if extra_encoding:
        for k, v in extra_encoding.items():
            enc[k] = dict(v)

    # Add compression defaults where not explicitly given
    if compression:
        for v in ds.data_vars:
            enc.setdefault(v, {})
            enc[v].setdefault("zlib", True)
            enc[v].setdefault("complevel", complevel)

    # Remove existing file
    if overwrite and os.path.exists(path):
        os.remove(path)

    # Write to disk
    ds.to_netcdf(path, mode="w", format=nc_format, engine=engine, encoding=enc if enc else None)

    reopened = None
    if readback:
        # Open and decode normally; ensure resources are closed later
        reopened = xr.open_dataset(path, engine=engine)

        if show_report:
            # Build a concise report
            lines = []
            lines.append("=== NetCDF Debug Report ===")
            lines.append(f"File: {path}")
            lines.append(f"Engine: {engine} | Format: {nc_format}")
            lines.append("--- Dimensions ---")
            for d, n in reopened.dims.items():
                lines.append(f"  {d}: {n}")

            if reopened.coords:
                lines.append("--- Coordinates ---")
                for c in reopened.coords:
                    da = reopened.coords[c]
                    lines.append(f"  {c}: dtype={da.dtype}, size={da.size}")

            lines.append("--- Data Variables ---")
            for v in reopened.data_vars:
                da = reopened[v]
                # chunk info (if dask-chunked)
                chunk_str = None
                if hasattr(da.data, "chunks") and da.data.chunks is not None:
                    chunk_str = "x".join(
                        ["(" + ",".join(str(x) for x in ch) + ")" for ch in da.data.chunks]
                    )
                # encoding info as stored/read by xarray
                enc_kv = []
                for key in ("zlib", "complevel", "chunksizes", "_FillValue", "shuffle"):
                    if key in da.encoding:
                        enc_kv.append(f"{key}={da.encoding[key]}")
                enc_str = ", ".join(enc_kv) if enc_kv else "n/a"

                lines.append(
                    f"  {v}: dtype={da.dtype}, shape={tuple(da.shape)}, "
                    f"chunks={chunk_str or 'n/a'}, encoding[{enc_str}]"
                )

            # Print it once
            print("\n".join(lines))

    return reopened
# ----------------------------------------------------------------------------------------------------------------------
