"""
Library Features:

Name:          lib_proc_mask
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20250203'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging

import numpy as np
import xarray as xr

from shybox.io_toolkit.lib_io_utils import create_darray
from shybox.orchestrator_toolkit.lib_orchestrator_utils import as_process

import matplotlib
import matplotlib.pyplot as plt
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to mask data
@as_process(input_type='xarray', output_type='xarray')
def mask_data_by_ref(
        data: xr.DataArray,
        ref: xr.DataArray, ref_value: (float, int) = -9999.0,
        mask_format: str = 'float', mask_no_data: (float, int) = -9999.0, **kwargs):

    if mask_format is not None:
        if mask_format == 'integer':
            if mask_no_data == np.nan:
                raise RuntimeError('Change the no_data value or the grid format')

    if mask_format is not None:
        if mask_format == 'integer':
            data = data.astype(int)
        elif mask_format == 'float':
            data = data.astype(float)
        else:
            raise NotImplemented('Mask type "' + mask_format + '"not implemented yet')

    data = xr.where(ref.values == ref_value, mask_no_data, data)

    return data
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to mask data
@as_process(input_type='xarray', output_type='xarray')
def mask_data_by_limits(
        data: xr.DataArray,
        mask_min: (float, int) = None, mask_max: (float, int) = None,
        mask_format: str = None, mask_no_data: (float, int) = -9999.0, **kwargs):

    if mask_min is not None:
        data = xr.where(data >= mask_min, data, mask_no_data)
    if mask_max is not None:
        data = xr.where(data <= mask_max, data, mask_no_data)

    if mask_format is not None:
        if mask_format == 'integer':
            if mask_no_data == np.nan:
                raise RuntimeError('Change the no_data value or the grid format')

    if mask_format is not None:
        if mask_format == 'integer':
            data = data.astype(int)
        elif mask_format == 'float':
            data = data.astype(float)
        else:
            raise NotImplemented('Mask type "' + mask_format + '"not implemented yet')

    return data
# ----------------------------------------------------------------------------------------------------------------------

