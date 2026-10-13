"""
Library Features:

Name:          lib_orchestrator_utils_adapters_fx
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
# method to adapt vars to arg (datasets
@with_logger(var_name='logger_stream')
def adapter_variables_to_group(data: dict, separator: str = '|', **kwargs) -> dict:

    # initialize organized data
    data_organized = {}
    # iterate over input data
    for data_key, data_value in data.items():

        # split group and variable
        try:
            group_name, variable_name = data_key.split(separator, 1)
        except ValueError:
            logger_stream.warning(f'Key "{data_key}" must have format group{separator}variable".')
            return data

        # initialize group
        if group_name not in data_organized:
            data_organized[group_name] = {}

        # store variable
        data_organized[group_name][variable_name] = data_value

    return data_organized
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# adapter to convert list of xarray objects to xarray object
@with_logger(var_name='logger_stream')
def adapter_obj_to_xarray(
        data: Union[dict, list, tuple, xr.DataArray, xr.Dataset, None] = None,
        time: Union[list, pd.DatetimeIndex, pd.Timestamp, str, None] = None,
        dim: str = "time",
        dim_values=None,
        no_data: float = -9999.0,
        **kwargs):
    """Concatenate xarray objects along a new dimension, supporting dict inputs."""

    # check input
    if data is None:
        logger_stream.warning('Object data is defined by None. Return None')
        return None

    # manage dictionary inputs
    if isinstance(data, dict):

        data_out = {}
        for data_key, data_value in data.items():

            # recursively concatenate each dictionary value
            data_out[data_key] = adapter_obj_to_xarray(
                data=data_value,
                time=time,dim=dim,
                dim_values=dim_values, no_data=no_data,
                **kwargs)

        return data_out

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
    data = [xr.full_like(obj_ref, no_data) if obj is None else obj for obj in data]

    # check homogeneous xarray type
    obj_type = type(obj_ref)
    if not all(type(obj) is obj_type for obj in data):
        raise TypeError("All objects must have the same xarray type: all xr.DataArray or all xr.Dataset.")

    # define dimension coordinates
    if time is not None:
        if isinstance(time, pd.DatetimeIndex):
            dim_values = time
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
        elif isinstance(dim_values, (int, float)):
            dim_values = [dim_values]
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
