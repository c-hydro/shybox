"""
Class Features

Name:          lib_data_geo_ascii
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20210113'
Version:       '1.0.0'
"""

#######################################################################################
# Libraries
import logging
import rasterio

import numpy as np

from library.common.lib_data_io_generic import create_darray_2d
from library.common.lib_info_args import logger_name

# Logging
logging.getLogger("fiona").setLevel(logging.WARNING)
log_stream = logging.getLogger(logger_name)

# Debug
import matplotlib.pylab as plt
#######################################################################################


# -------------------------------------------------------------------------------------
# Method to load an ascii grid file
def read_data_grid(file_name, var_limit_min=0, var_limit_max=None):

    try:
        dset = rasterio.open(file_name)
        bounds = dset.bounds
        res = dset.res
        transform = dset.transform
        data = dset.read()
        values = np.float32(data[0, :, :])

        if var_limit_min is not None:
            values[values < var_limit_min] = np.nan
        if var_limit_max is not None:
            values[values > var_limit_max] = np.nan

        decimal_round = 7

        center_right = bounds.right - (res[0] / 2)
        center_left = bounds.left + (res[0] / 2)
        center_top = bounds.top - (res[1] / 2)
        center_bottom = bounds.bottom + (res[1] / 2)

        lon = np.arange(center_left, center_right + np.abs(res[0] / 2), np.abs(res[0]), float)
        lat = np.arange(center_bottom, center_top + np.abs(res[0] / 2), np.abs(res[1]), float)
        lons, lats = np.meshgrid(lon, lat)

        min_lon_round = round(np.min(lons), decimal_round)
        max_lon_round = round(np.max(lons), decimal_round)
        min_lat_round = round(np.min(lats), decimal_round)
        max_lat_round = round(np.max(lats), decimal_round)

        center_right_round = round(center_right, decimal_round)
        center_left_round = round(center_left, decimal_round)
        center_bottom_round = round(center_bottom, decimal_round)
        center_top_round = round(center_top, decimal_round)

        assert min_lon_round == center_left_round
        assert max_lon_round == center_right_round
        assert min_lat_round == center_bottom_round
        assert max_lat_round == center_top_round

        lats = np.flipud(lats)

        da_frame = create_darray_2d(values, lons, lats,
                                    coord_name_x='west_east', coord_name_y='south_north',
                                    dim_name_x='west_east', dim_name_y='south_north')
    except IOError as io_error:
        raise io_error('File ' + file_name + ' not found')

    return da_frame
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to load an ascii vector file
def read_data_vector(file_name):

    file_handle = open(file_name, 'r')
    file_lines = file_handle.readlines()
    file_handle.close()

    vector_frame = [float(elem.strip('\n')) for elem in file_lines]

    return vector_frame

# -------------------------------------------------------------------------------------
