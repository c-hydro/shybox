"""
Library Features:

Name:          lib_proc_merge
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20250203'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging

from osgeo import gdal
import xarray as xr
import numpy as np
from collections import defaultdict
from typing import Optional, Generator

from shybox.io_toolkit.lib_io_utils import create_darray
from shybox.orchestrator_toolkit.lib_orchestrator_utils import as_process

import matplotlib
import matplotlib.pyplot as plt
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to merge data
@as_process(input_type='xarray', output_type='xarray')
def merge_data_by_ref(
        data: xr.DataArray,
        ref: xr.DataArray, ref_value: (float, int) = -9999.0,
        mask_format: str = 'integer', mask_no_data: (float, int) = -9999.0, **kwargs):

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


@as_DAM_process(input_type = 'file', output_type = 'gdal', input_tiles = True)
def combine_tiles(inputs: list[str|gdal.Dataset],
                  num_cpus: Optional[int] = None
                  )-> gdal.Dataset:
    """
    Mosaic a set of input rasters.
    """

    if num_cpus is None:
        num_cpus = 'ALL_CPUS'

    out_ds = gdal.Warp('', inputs, format = 'MEM', options=[f'NUM_THREADS={num_cpus}'], multithread=True)

    return out_ds