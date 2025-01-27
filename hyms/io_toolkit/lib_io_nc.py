"""
Library Features:

Name:          lib_io_nc
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20250127'
Version:       '1.0.0'
"""
# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import rasterio
import xarray as xr
import numpy as np
from rasterio.crs import CRS

from hyms.default.lib_default_geo import proj_epsg, proj_wkt
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to adjust dimensions naming
def __adjust_dims_naming(file_obj: xr.Dataset, file_map_geo: dict) -> xr.Dataset:
    for dim_in, dim_out in file_map_geo.items():
        if dim_in in file_obj.dims:
            file_obj = file_obj.rename({dim_in: 'dim_tmp'})
            file_obj = file_obj.rename_dims({'dim_tmp': dim_out})
    return file_obj
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to read netcdf file
def get_file_grid(file_name: str,
                  file_epsg: str = 'EPSG:4326', file_crs: CRS = None, file_dims=
                  file_transform: rasterio.transform = None, file_no_data: float = -9999.0,
                  **kwargs):

    file_obj = xr.open_dataset(file_name)

    file_obj = __adjust_dims_naming(file_obj, file_map_geo={'X': 'longitude', 'Y': 'latitude'})

    file_x_values, file_y_values = file_obj['longitude'].values, file_obj['latitude'].values
    file_height, file_width = file_y_values.shape[0], file_x_values.shape[1]

    if 'xllcorner' in list(file_obj.attrs.keys()):
        file_x_left_check = file_obj.attrs['xllcorner']
    if 'yllcorner' in list(file_obj.attrs.keys()):
        file_y_bottom_check = file_obj.attrs['yllcorner']

    file_x_left = np.min(np.min(file_x_values))
    file_x_right = np.max(np.max(file_x_values))
    file_y_bottom = np.min(np.min(file_y_values))
    file_y_top = np.max(np.max(file_y_values))

    if 'cellsize' in list(file_obj.attrs.keys()):
        file_x_res, file_y_res = file_obj.attrs['cellsize'], file_obj.attrs['cellsize']
    else:
        file_x_res = (file_x_right - file_x_left) / file_height
        file_y_res = (file_y_top - file_y_bottom) / file_width

    if file_epsg is None:
        file_epsg = proj_epsg

    if file_crs is None:
        file_crs = CRS.from_string(file_epsg)

    if file_transform is None:
        # TO DO: fix the 1/2 pixel of resolution in x and y ... using resolution/2
        file_transform = rasterio.transform.from_bounds(
            file_x_left, file_y_bottom, file_x_right, file_y_top,
            file_width, file_height)

    file_attrs = {'transform': file_transform, 'crs': file_crs, 'epsg': file_epsg,
                  'bbox': [file_x_left, file_y_bottom, file_x_right, file_y_top],
                  'bb_left': file_x_left, 'bb_right': file_x_right,
                  'bb_top': file_y_top, 'bb_bottom': file_y_bottom,
                  'res_x': file_x_res, 'res_y': file_y_res,
                  'high': file_height, 'wide': file_width}

    file_obj.attrs = file_attrs

    return file_obj
# ----------------------------------------------------------------------------------------------------------------------
