# ----------------------------------------------------------------------------------------------------------------------
"""
Library Features:

Name:          lib_orchestrator_checker
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20260928'
Version:       '1.0.0'
"""
# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging

import pandas as pd
import xarray as xr
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to check pandas
def check_pandas(obj_data) -> bool:
    """Check if all objects are pandas objects."""

    if not isinstance(obj_data, dict):
        return False

    def _check(value):
        if isinstance(value, (pd.DataFrame, pd.Series)):
            return True
        if isinstance(value, (list, tuple)):
            return all(isinstance(item, (pd.DataFrame, pd.Series)) for item in value)
        return False

    return all(_check(value)for value in obj_data.values())

# method to check xarray
def check_xarray(obj_data) -> bool:
    """Check if all objects are xarray objects."""

    if not isinstance(obj_data, dict):
        return False

    def _check(value):
        if isinstance(value, (xr.DataArray, xr.Dataset)):
            return True
        if isinstance(value, (list, tuple)):
            return all(isinstance(item, (xr.DataArray, xr.Dataset)) for item in value)
        return False

    return all(_check(value) for value in obj_data.values())
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to check data type
def check_data_type(obj_data, string_data: str = 'pandas', logger=None) -> bool:

    logger = logger or logging.getLogger(__name__)

    if string_data == 'pandas':
        is_data = check_pandas(obj_data)
    elif string_data == 'xarray':
        is_data = check_xarray(obj_data)
    elif string_data == 'tiff':
        is_data = True
    else:
        logger.warning('Data type is not defined in the expected ones')
        is_data = False

    return is_data
# ----------------------------------------------------------------------------------------------------------------------
