"""
Library Features:

Name:           lib_io_nc
Author(s):      Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:           '20260626'
Version:        '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import xarray as xr

from config_info import LOGGER_NAME

# set logger
logger = logging.getLogger(LOGGER_NAME)
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# helper to read hydrograph netcdf
def read_hydrograph_nc(nc_file):

    logger.info(f" -----> Read NetCDF file: {nc_file} ... ")

    ds = xr.open_dataset(nc_file)

    logger.info(" ::: Dimensions:")
    for name, size in ds.sizes.items():
        logger.info(f"  - {name}: {size}")

    logger.info(" ::: Coordinates:")
    for coord in ds.coords:
        logger.info(f"  - {coord}")

    logger.info(" ::: Variables:")
    for var in ds.data_vars:
        logger.info(f"  - {var}")

    logger.info(" ::: Attributes:")
    if ds.attrs:
        for key, value in ds.attrs.items():
            logger.info(f"  - {key}: {value}")
    else:
        logger.info("  - None")

    logger.info(f" -----> Read NetCDF file: {nc_file} ... DONE")

    return ds
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# helper to get metadata from nc dataset
def get_data_from_nc(ds: xr.Dataset, fields: dict) -> dict:
    """
    Parameters
    ----------
    ds : xr.Dataset
        Opened NetCDF dataset.
    fields : dict
        Mapping {output_key: nc_variable_or_attribute}

    Returns
    -------
    dict
        Dictionary with the requested fields.
    """

    results = {}
    for out_key, field_name in fields.items():

        if field_name in ds:
            values = ds[field_name].values

            if values.ndim == 0:
                results[out_key] = str(values.item())
            else:
                results[out_key] = [str(v) for v in values.flatten()]

        elif field_name in ds.attrs:
            value = ds.attrs[field_name]

            if isinstance(value, (list, tuple)):
                results[out_key] = [str(v) for v in value]
            else:
                results[out_key] = str(value)

        else:
            raise RuntimeError(
                f"Field '{field_name}' not found in dataset variables or attributes."
            )

    return results
# ----------------------------------------------------------------------------------------------------------------------
