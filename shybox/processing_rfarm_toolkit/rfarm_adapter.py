"""
Library Features:

Name:          rfarm_adapter
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20261102'
Version:       '1.0.0'
"""
# ----------------------------------------------------------------------------------------------------------------------
# libraries
import numpy as np
import pandas as pd
import xarray as xr

from typing import Union

from shybox.io_toolkit.lib_io_utils import create_darray
from shybox.processing_points_toolkit.lib_proc_interpolate_rain import interpolate_griso

from shybox.generic_toolkit.lib_utils_debug import plot_data

# common decorators
from shybox import with_logger, deprecated, as_process
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# helper to disaggregate datasets using rfarm
@as_process(input_type='xarray', input_adapter=None,
            args_adapter=None, output_adapter=None, output_type='xarray')
@with_logger(var_name='logger_stream')
def exec_rfarm_nwp(
        data: xr.DataArray, ref: xr.DataArray = None,
        debug_data: bool = False, debug_results: bool = False,
        **kwargs):

    # Info method start
    logger_stream.info_up('Execute RainFARM NWP ...')

    # Check input data
    if data is None:
        logger_stream.error('RainFARM input data are not defined. Disaggregation cannot proceed.')
        raise ValueError('RainFARM input data are None.')

    # Check input data type
    if not isinstance(data, xr.DataArray):
        logger_stream.error('RainFARM input data format is not expected. Disaggregation cannot proceed.')
        raise TypeError(f'RainFARM input must be an xarray DataArray. Got {type(data).__name__}.')

    # Check if input data are empty
    if data.size == 0:
        logger_stream.warning('RainFARM input DataArray is empty. Disaggregation skipped.')
        logger_stream.info_down('Execute RainFARM NWP ... SKIPPED')
        return None

    # Check if all input values are missing
    if bool(data.isnull().all().item()):
        logger_stream.warning('RainFARM input DataArray contains only NaN values. Disaggregation skipped.')
        logger_stream.info_down('Execute RainFARM NWP ... SKIPPED')
        return None

    ### da fare
    # Validate reference grid if required
    # Configure RainFARM parameters
    # Execute spatial and temporal disaggregation
    # Organize disaggregated DataArray

    logger_stream.info_down('Execute RainFARM NWP ... DONE')

    return data
# ----------------------------------------------------------------------------------------------------------------------
