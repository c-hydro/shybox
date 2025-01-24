import logging
import rasterio
import xarray as xr
import numpy as np
from rasterio.crs import CRS

from hyms.default.lib_default_geo import proj_epsg, proj_wkt


# ----------------------------------------------------------------------------------------------------------------------
# method to read netcdf file
def get_file_grid(file_name: str,
                  file_epsg: str = 'EPSG:4326', file_crs: CRS = None,
                  file_transform: rasterio.transform = None, file_no_data: float = -9999.0):

    file_obj = xr.open_dataset(file_name)

    if 'south_north' in file_obj.dims and 'west_east' in file_obj.dims:
        file_obj = file_obj.rename({'longitude': 'lon', 'latitude': 'lat'})
        file_obj = file_obj.rename_dims({'west_east': 'longitude', 'south_north': 'latitude'})
        file_obj = file_obj.rename({'lon': 'longitude', 'lat': 'latitude'})

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
