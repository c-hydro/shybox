import os
import logging
import xarray as xr
from earthkit.data import from_source
from processing import process_accumulated_variable, clean_attrs

logger = logging.getLogger(__name__)

def download_to_xarray(request: dict, address: str) -> xr.Dataset:
    data = from_source("polytope", "destination-earth", request, address=address, stream=False)
    return data.to_xarray()

def download_and_process(name: str,
                         request: dict,
                         output_dir: str,
                         address: str,
                         var_type: str,
                         convert_cumulative: bool = True) -> str:
    logger.info(f"Downloading: {name} — req keys: {list(request.keys())}")
    ds = download_to_xarray(request, address=address)

    if var_type == "accum":
        if convert_cumulative:
            logger.info(f"Converting cumulative to interval accumulation for: {name}")
            ds = process_accumulated_variable(ds, name)
        else:
            logger.info(f"Keeping cumulative values for: {name} (no post-processing)")
    else:
        logger.info(f"No post-processing needed for instantaneous variable: {name}")

    ds = clean_attrs(ds)

    os.makedirs(output_dir, exist_ok=True)
    out_nc = os.path.join(output_dir, f"{name}.nc")
    ds.to_netcdf(out_nc)
    logger.info(f"Saved: {out_nc}")
    return out_nc