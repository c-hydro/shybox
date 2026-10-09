"""
Library Features:

Name:          grids_adapter
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20261002'
Version:       '1.0.0'
"""
# ----------------------------------------------------------------------------------------------------------------------
# libraries
import numpy as np
import pandas as pd
import xarray as xr
import rasterio

from typing import Union, Optional

from shybox.orchestrator_toolkit.lib_orchestrator_utils_processes import as_process
from shybox.io_toolkit.lib_io_utils import create_darray
from shybox.logging_toolkit.lib_logging_utils import with_logger

from shybox.processing_grid_toolkit.lib_utils_interpolate_hs import _updating_evaluate_Quality
from shybox.processing_grid_toolkit.lib_proc_interpolate import interpolate_data

ARGS_DEFAULT = {
    'min_r2': 0.7,
    'min_n_updating': 20}
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# helper to interpolate data
@as_process(input_type='pandas',
            input_adapter=None,
            args_adapter=None,
            output_adapter={
                'data': {"method": "fx_to_dict", "kwargs": {}},
            },
            output_type='xarray')
@with_logger(var_name='logger_stream')
def exec_interpolate_data_generic(data, ref, **kwargs):

    # check data is defined or not
    if data is None:
        logger_stream.warning(f'Input DataArray has defined by NoneType. Return None.')
        return None

    # check input data type
    if not isinstance(data, xr.DataArray):
        logger_stream.error(f'Interpolation data must be an xr.DataArray, got "{type(data).__name__}".')
        raise TypeError(f'Expected xr.DataArray, got {type(data).__name__}')

    # get variable name or assign default
    var_name = data.name
    if not isinstance(var_name, str) or not var_name.strip():
        var_name = 'DATA'
        logger_stream.warning(f'Input DataArray has no valid variable name. Using default name "{var_name}".')
        data = data.rename(var_name)

    da = interpolate_data(data, ref, var_name_data=var_name, **kwargs)

    # check output data type
    if not isinstance(da, xr.DataArray):
        logger_stream.error(f'Interpolation result must be an xr.DataArray, got "{type(da).__name__}".')
        raise TypeError(f'Expected xr.DataArray, got {type(da).__name__}')

    return da
# ----------------------------------------------------------------------------------------------------------------------
