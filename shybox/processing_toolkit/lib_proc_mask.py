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
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to mask data
def mask_data(data: xr.DataArray, ref: xr.DataArray,
              mask_data: (float, int) = -9999.0, mask_min: (float, int) = None, mask_max: (float, int) = None,
              mask_format: str = 'integer',
              mask_no_data: (float, int) =-9999.0):

    if grid_mask_min is not None:
        grid_da = xr.where(grid_da >= grid_mask_min, grid_mask_data, grid_mask_no_data)
    if grid_mask_max is not None:
        grid_da = xr.where(grid_da <= grid_mask_max, grid_mask_data, grid_mask_no_data)

    if grid_mask_format == 'integer':
        if grid_mask_no_data == np.nan:
            logging.error(' ===> Grid no_data equal to NaN is not supported in integer datasets')
            raise RuntimeError('Change the no_data value or the grid format in the settings file ')

    if grid_mask_format is not None:
        if grid_mask_format == 'integer':
            grid_da = grid_da.astype(int)
        elif grid_mask_format == 'float':
            grid_da = grid_da.astype(float)
        else:
            logging.error(' ===> Mask format "' + grid_mask_format + '" is not supported')
            raise NotImplemented('Case not implemented yet')

    return grid_da
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to execute mask
def compute_grid_mask(grid_da, grid_mask_min=0, grid_mask_max=None, grid_mask_data=1, grid_mask_format='integer',
                      grid_mask_no_data=-9999.0, grid_mask_active=True):
    if grid_mask_active:
        if grid_mask_min is not None:
            grid_da = xr.where(grid_da >= grid_mask_min, grid_mask_data, grid_mask_no_data)
        if grid_mask_max is not None:
            grid_da = xr.where(grid_da <= grid_mask_max, grid_mask_data, grid_mask_no_data)

        if grid_mask_format == 'integer':
            if grid_mask_no_data == np.nan:
                logging.error(' ===> Grid no_data equal to NaN is not supported in integer datasets')
                raise RuntimeError('Change the no_data value or the grid format in the settings file ')

        if grid_mask_format is not None:
            if grid_mask_format == 'integer':
                grid_da = grid_da.astype(int)
            elif grid_mask_format == 'float':
                grid_da = grid_da.astype(float)
            else:
                logging.error(' ===> Mask format "' + grid_mask_format + '" is not supported')
                raise NotImplemented('Case not implemented yet')
    else:
        logging.warning(' ===> Grid masking is not activated')
    return grid_da

# ----------------------------------------------------------------------------------------------------------------------

