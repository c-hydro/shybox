
"""
Library Features:

Name:          lib_proc_warp
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20230727'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import os
import shutil

import numpy as np
import xarray as xr

import gdal
from osgeo import gdal, gdalconst

from lib_data_io_tiff import write_file_tiff
from lib_utils_generic import create_filename_tmp, make_folder
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to warp grid data
def warp_grid_data(file_name_src, file_name_dst,
                   grid_warp_method='near', grid_warp_resolution=None,
                   grid_warp_format='GTiff', grid_warp_active=True, grid_warp_copy=True):

    if grid_warp_active:
        if grid_warp_resolution is not None:
            grid_warp_options = {
                'format': grid_warp_format,
                'xRes': grid_warp_resolution, 'yRes': grid_warp_resolution,
                'resampleAlg': grid_warp_method}

            file_ds = gdal.Warp(file_name_dst, file_name_src, **grid_warp_options)
            del file_ds
        else:
            logging.error(' ===> Warping grid is not possible due to the resolution defined by NoneType')
            raise RuntimeError('Set the resolution in the settings file')
    else:
        logging.warning(' ===> Grid warping is not activated')
        if grid_warp_copy:
            shutil.copy(file_name_src, file_name_dst)

# ----------------------------------------------------------------------------------------------------------------------