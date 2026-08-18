import xarray as xr
import logging

logger = logging.getLogger(__name__)

def clean_attrs(ds: xr.Dataset) -> xr.Dataset:
    """
    Remove non-serializable metadata from xarray Dataset and variables.

    This is necessary before saving to NetCDF, because `earthkit-data` may attach
    Python objects or bytes to `.attrs`, which `xarray.to_netcdf()` cannot serialize.
    """
    ds.attrs = {k: v for k, v in ds.attrs.items()
                if not isinstance(v, (dict, bytes)) and k != "_earthkit"}
    for obj in list(ds.data_vars.values()) + list(ds.coords.values()):
        obj.attrs = {k: v for k, v in obj.attrs.items()
                    if not isinstance(v, (dict, bytes)) and k != "_earthkit"}
    return ds

def process_accumulated_variable(ds: xr.Dataset, param_name: str) -> xr.Dataset:
    """
    Convert cumulative accumulations to interval accumulations.

    Input: step i holds accumulation from 0..i  
    Output: step i holds accumulation over (i-1..i)
    """
    logger.info(f"Processing accumulated variable: {param_name}")
    var_name = list(ds.data_vars)[0]
    first = ds[var_name].isel(step=0).copy()
    ds[var_name] = ds[var_name].diff(dim="step", label="upper")
    ds[var_name][0] = first
    ds[var_name].attrs["processing"] = "Interval accumulation (consecutive step differences)"
    return ds