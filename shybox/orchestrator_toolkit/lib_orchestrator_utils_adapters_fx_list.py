"""
Library Features:

Name:          lib_orchestrator_utils_fx_list
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20260309'
Version:       '1.1.0'
"""


# ----------------------------------------------------------------------------------------------------------------------
# libraries
import xarray as xr
import pandas as pd

from typing import Union

from shybox.logging_toolkit.lib_logging_utils import with_logger
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# adapter to convert list of xarray objects to xarray object
@with_logger(var_name='logger_stream')
def adapter_list_to_xarray(
        data: Union[list, None] = None,
        time: Union[list, pd.DatetimeIndex, pd.Timestamp, str, None] = None,
        dim: str = "time",
        dim_values: Union[int, None] = None,
        no_data: float = -9999.0):

    # check input
    if data is None:
        logger_stream.warning('Object data is defined by None. Return None')
        return None

    # normalize input to list
    if not isinstance(data, (list, tuple)):
        data = [data]

    # get first valid object as reference
    obj_ref = next((obj for obj in data if obj is not None), None)

    # no valid reference available
    if obj_ref is None:
        logger_stream.warning('Object reference is defined by None. Return None')
        return None

    # check reference type
    if not isinstance(obj_ref, (xr.DataArray, xr.Dataset)):
        raise TypeError(f"Expected xr.DataArray or xr.Dataset. Found: {type(obj_ref).__name__}")

    # initialize undefined objects using reference
    data = [obj_ref.full_like(no_data) if obj is None else obj for obj in data]

    # check homogeneous xarray type
    obj_type = type(obj_ref)
    if not all(isinstance(obj, obj_type) for obj in data):
        raise TypeError("All objects must have the same xarray type: all xr.DataArray or all xr.Dataset.")

    # define time coordinates
    if time is not None:

        if isinstance(time, pd.DatetimeIndex):
            dim_values = tim

        elif isinstance(time, (pd.Timestamp, str)):
            dim_values = pd.DatetimeIndex([pd.Timestamp(time)])

        elif isinstance(time, (list, tuple)):
            dim_values = pd.DatetimeIndex([pd.Timestamp(time_step) for time_step in time])
        else:
            logger_stream.error('Time object is not in expected format. Exit.')
            raise TypeError(f"Unsupported time type: {type(time).__name__}")

    # use explicitly defined dimension values
    elif dim_values is not None:

        if isinstance(dim_values, pd.DatetimeIndex):
            pass
        elif isinstance(dim_values, pd.Timestamp):
            dim_values = pd.DatetimeIndex([dim_values])
        else:
            dim_values = list(dim_values)

    # fallback to sequential index
    else:
        dim_values = list(range(len(data)))

    # check dimension size
    if len(dim_values) != len(data):
        raise ValueError(f"Dimension '{dim}' has {len(dim_values)} values, but {len(data)} objects were provided.")

    # concatenate along dimension
    data_out = xr.concat(data, dim=xr.IndexVariable(dim, dim_values))

    return data_out
# ----------------------------------------------------------------------------------------------------------------------
