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

#from shybox.dataset_toolkit.merge.app_data_grid_main import logger_name, logger_arrow
from shybox.default.lib_default_geo import crs_epsg, crs_wkt

# logging
#logger_stream = logging.getLogger(logger_name)
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to read netcdf file
def get_file_grid(file_name: str,
                  file_epsg: str = 'EPSG:4326', file_crs: CRS = None,
                  file_transform: rasterio.transform = None,
                  file_map_dims: dict = None, file_map_geo: dict = None, file_map_data: dict = None,
                  **kwargs):

    file_obj = xr.open_dataset(file_name)

    if file_map_geo is not None:
        file_obj = __adjust_geo_naming(file_obj, file_map_geo=file_map_geo)

    file_x_values, file_y_values = file_obj['longitude'].values, file_obj['latitude'].values
    file_obj = file_obj.drop_vars(['longitude', 'latitude'])
    if file_x_values.ndim == 2:
        file_x_values = file_x_values[0, :]
    elif file_x_values.ndim == 1:
        pass
    else:
        #logger_stream.error(logger_arrow.error + 'Geographical dimensions of "longitude" is not expected')
        assert RuntimeError('Geographical dimensions of "longitude" must be 1D or 2D')
    if file_y_values.ndim == 2:
        file_y_values = file_y_values[:, 0]
    elif file_y_values.ndim == 1:
        pass
    else:
        #logger_stream.error(logger_arrow.error + 'Geographical dimensions of "latitude" is not expected')
        assert RuntimeError('Geographical dimensions of "latitude" must be 1D or 2D')

    if file_map_dims is not None:
        file_obj = __adjust_dims_naming(file_obj, file_map_dims=file_map_dims)

    file_obj['longitude'] = xr.DataArray(file_x_values, dims='longitude')
    file_obj['latitude'] = xr.DataArray(file_y_values, dims='latitude')

    file_height, file_width = file_obj['latitude'].shape[0], file_obj['longitude'].shape[0]

    file_x_left = np.min(np.min(file_x_values))
    file_x_right = np.max(np.max(file_x_values))
    file_y_bottom = np.min(np.min(file_y_values))
    file_y_top = np.max(np.max(file_y_values))

    file_x_res = (file_x_right - file_x_left) / file_width
    file_y_res = (file_y_top - file_y_bottom) / file_height

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
