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
from shybox.orchestrator_toolkit.lib_orchestrator_utils_processes import as_process
from shybox.logging_toolkit.lib_logging_utils import with_logger

import matplotlib
import matplotlib.pyplot as plt
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to mask data
@as_process(input_type='xarray', output_type='xarray')
@with_logger(var_name='logger_stream')
def mask_data_by_ref(
        data: xr.DataArray,
        ref: xr.DataArray, ref_value: (float, int) = -9999.0,
        mask_format: str = 'float', mask_no_data: (float, int) = -9999.0, mask_mode='lazy', **kwargs):

    if mask_format is not None:
        if mask_format == 'integer':
            if mask_no_data == np.nan:
                logger_stream.error('Change the no_data value or the grid format')
                raise RuntimeError('Change the no_data value or the grid format')

    if mask_format is not None:
        if mask_format == 'integer':
            data = data.astype(int)
        elif mask_format == 'float':
            data = data.astype(float)
        else:
            logger_stream.error('Change the mask_format value or the grid format')
            raise NotImplemented('Mask type "' + mask_format + '"not implemented yet')

    # array to check
    ref_arr = ref.values

    # Check if ref_value exists in the array (handle NaN safely)
    if np.isnan(ref_value):
        # ref_value itself is NaN → directly use isnan
        mask = np.isnan(ref_arr)
    else:
        count = np.count_nonzero(ref_arr == ref_value)

        if count > 0:
            # Normal case: ref_value is present
            mask = ref_arr == ref_value
        else:
            # Fallback: ref_value not present → try using NaN mask
            if mask_mode == 'lazy':
                logger_stream.warning(
                    'Reference value "' + str(ref_value) + '" not found in reference array; using NaN mask instead.')
                mask = np.isnan(ref_arr)
            elif mask_mode == 'strict':
                logger_stream.error(
                    'Reference value "' + str(ref_value) + '" not found in reference array; cannot proceed in strict mode.')
                raise RuntimeError(
                    'Reference value "' + str(ref_value) + '" not found in reference array; cannot proceed in strict mode.')
            else:
                logger_stream.error('Unknown mask_mode "' + str(mask_mode) + '" specified.')
                raise NotImplemented('Unknown mask_mode "' + str(mask_mode) + '" specified.')

    # Apply mask
    data = xr.where(mask, mask_no_data, data)

    """ debug plot
    plt.figure()
    plt.imshow(data.values, cmap='viridis')
    plt.colorbar(label='Masked Data')
    plt.show(block=True)
    """

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

