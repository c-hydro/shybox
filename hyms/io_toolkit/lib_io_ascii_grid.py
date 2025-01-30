# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import numpy as np
import xarray as xr
import rasterio as rio
from rasterio.crs import CRS

from hyms.io_toolkit.lib_io_utils import create_darray
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to read grid data
def get_file_grid(file_name: str, file_epsg: str = 'EPSG:4326', file_dtype: str = 'float32',
                  var_limit_min: (int, float) = None, var_limit_max: (int, float) = None,
                  var_null_data: (int, float) = np.nan,
                  coord_name_x: str = 'longitude', coord_name_y: str = 'latitude',
                  dim_name_x: str = 'longitude', dim_name_y: str = 'latitude', **kwargs) -> xr.DataArray:

    dset = rio.open(file_name)
    bounds, res, transform = dset.bounds, dset.res, dset.transform
    data = dset.read()

    if dset.crs is None:
        file_crs = CRS.from_string(file_epsg)
    else:
        file_crs = dset.crs

    if file_dtype == 'float32':
        values = np.float32(data[0, :, :])
    else:
        logging.error(' ===> Data type is not allowed.')
        raise NotImplementedError('Case not implemented yet')

    height, width = values.shape

    if var_limit_min is not None:
        var_limit_min = np.float32(var_limit_min)
        values[values < var_limit_min] = var_null_data
    if var_limit_max is not None:
        var_limit_max = np.float32(var_limit_max)
        values[values > var_limit_max] = var_null_data

    decimal_round_geo = 7

    center_right = bounds.right - (res[0] / 2)
    center_left = bounds.left + (res[0] / 2)
    center_top = bounds.top - (res[1] / 2)
    center_bottom = bounds.bottom + (res[1] / 2)

    if center_bottom > center_top:
        logging.warning(' ===> Coords "center_bottom": ' + str(center_bottom) + ' is greater than "center_top": '
                        + str(center_top) + '. Try to inverse the bottom and top coords. ')
        center_tmp = center_top
        center_top = center_bottom
        center_bottom = center_tmp

    lon = np.arange(center_left, center_right + np.abs(res[0] / 2), np.abs(res[0]), float)
    lat = np.flip(np.arange(center_bottom, center_top + np.abs(res[1] / 2), np.abs(res[1]), float), axis=0)
    lons, lats = np.meshgrid(lon, lat)

    lat_upper = lats[0, 0]
    lat_lower = lats[-1, 0]
    if lat_lower > lat_upper:
        lats = np.flipud(lats)
        values = np.flipud(values)

    min_lon_round = round(np.min(lons), decimal_round_geo)
    max_lon_round = round(np.max(lons), decimal_round_geo)
    min_lat_round = round(np.min(lats), decimal_round_geo)
    max_lat_round = round(np.max(lats), decimal_round_geo)

    center_right_round = round(center_right, decimal_round_geo)
    center_left_round = round(center_left, decimal_round_geo)
    center_bottom_round = round(center_bottom, decimal_round_geo)
    center_top_round = round(center_top, decimal_round_geo)

    assert min_lon_round == center_left_round
    assert max_lon_round == center_right_round
    assert min_lat_round == center_bottom_round
    assert max_lat_round == center_top_round

    data_attrs = {'transform': transform, 'crs': file_crs, 'epsg': file_epsg,
                  'bbox': [bounds.left, bounds.bottom, bounds.right, bounds.top],
                  'bb_left': bounds.left, 'bb_right': bounds.right,
                  'bb_top': bounds.top, 'bb_bottom': bounds.bottom,
                  'res_x': res[0], 'res_y': res[1],
                  'high': height, 'wide': width}

    data_obj = create_darray(
        values, lons[0, :], lats[:, 0],
        coord_name_x=coord_name_x, coord_name_y=coord_name_y,
        dim_name_x=dim_name_x, dim_name_y=dim_name_y)

    data_obj.attrs = data_attrs

    return data_obj

# ----------------------------------------------------------------------------------------------------------------------
