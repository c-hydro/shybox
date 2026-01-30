"""
Library Features:

Name:          lib_dataset_ondemand
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20260130'
Version:       '1.0.0'
"""
# ----------------------------------------------------------------------------------------------------------------------
# libraries
import xarray as xr
import pandas as pd

from typing import Optional

from shybox.generic_toolkit.lib_utils_geo import create_grid_from_corners
from shybox.logging_toolkit.lib_logging_utils import with_logger
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to read from file
@with_logger(var_name="logger_stream")
def create_object(
        info, obj_format: Optional[str] = None,
        obj_type: Optional[str] = None, obj_variable: (str, list) = 'object', obj_mandatory: bool = True) \
        -> (xr.DataArray, xr.Dataset, pd.DataFrame):

    # check object type
    if obj_type in ['grid', 'grid_2d']:

        # required keys
        required_keys = ["y_ll", "x_ll", "rows", "cols", "res", "value"]

        # check missing keys
        missing = [k for k in required_keys if k not in info]
        # show missing + required args
        if missing:
            # log error and raise exception
            if obj_mandatory:
                logger_stream.error(
                    "Missing required keys in create object.\n"
                    f"  Required: {required_keys}\n"
                    f"  Missing:  {missing}"
                )
                raise ValueError("Missing required keys in create object.")
            else:
                logger_stream.warning(
                    "Missing required keys in create object.\n"
                    f"  Required: {required_keys}\n"
                    f"  Missing:  {missing}"
                )
                return None

        # unpack info
        y_ll, x_ll, rows, cols, res = info["y_ll"], info["x_ll"], info["rows"], info["cols"], info["res"]
        value = info["value"]
        # create grid from corners
        data = create_grid_from_corners(
            y_ll, x_ll, rows, cols, res, grid_value=value,
            grid_check=True, grid_format='data_array', grid_name=obj_variable)

    else:

        logger_stream.error(f'Object type not supported: {obj_type}')
        raise ValueError(f'Object type not supported: {obj_type}')


    return data
# ----------------------------------------------------------------------------------------------------------------------
