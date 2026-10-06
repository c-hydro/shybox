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

from typing import Union

from shybox.orchestrator_toolkit.lib_orchestrator_utils_processes import as_process
from shybox.io_toolkit.lib_io_utils import create_darray
from shybox.logging_toolkit.lib_logging_utils import with_logger

from shybox.processing_grid_toolkit.lib_proc_interpolate import interpolate_data
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
def exec_interpolate_data(data, ref, **kwargs):

    da = interpolate_data(data, ref, **kwargs)

    return da
# ----------------------------------------------------------------------------------------------------------------------
