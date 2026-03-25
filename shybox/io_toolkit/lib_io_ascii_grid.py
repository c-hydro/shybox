"""
Library Features:

Name:          lib_io_ascii_grid
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20260324'
Version:       '1.1.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import numpy as np
import xarray as xr
import rasterio as rio
from rasterio.crs import CRS
from decimal import Decimal

from shybox.io_toolkit.lib_io_utils import create_darray
from shybox.logging_toolkit.lib_logging_utils import with_logger
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to read grid data
@with_logger(var_name='logger_stream')
def read_grid(file_name: str, file_epsg: str = 'EPSG:4326', file_dtype: str = 'float32',
                  var_limit_min: (int, float) = None, var_limit_max: (int, float) = None,
                  var_null_data: (int, float) = np.nan,
                  coord_name_x: str = 'longitude', coord_name_y: str = 'latitude',
                  dim_name_x: str = 'longitude', dim_name_y: str = 'latitude', **kwargs) -> xr.DataArray:

    geo_attrs = {}
    is_ascii_grid = False

    # try to read ASCII-grid header
    try:
        with open(file_name, 'r') as file:
            geo_lines = [next(file) for _ in range(6)]

        for line in geo_lines:
            if line.startswith('xllcorner'):
                geo_attrs['xllcorner'] = Decimal(line.split()[1])
            elif line.startswith('yllcorner'):
                geo_attrs['yllcorner'] = Decimal(line.split()[1])
            elif line.startswith('cellsize'):
                geo_attrs['cellsize'] = Decimal(line.split()[1])
            elif line.startswith('NODATA_value'):
                geo_attrs['NODATA_value'] = float(line.split()[1])
            elif line.startswith('ncols'):
                geo_attrs['ncols'] = int(line.split()[1])
            elif line.startswith('nrows'):
                geo_attrs['nrows'] = int(line.split()[1])

        required_geo = ['xllcorner', 'yllcorner', 'cellsize', 'NODATA_value', 'ncols', 'nrows']
        is_ascii_grid = all(field in geo_attrs for field in required_geo)

    except Exception:
        geo_attrs = {}
        is_ascii_grid = False

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

    if is_ascii_grid:

        x_ll = float(geo_attrs['xllcorner'])
        y_ll = float(geo_attrs['yllcorner'])
        cellsize = float(geo_attrs['cellsize'])
        ncols = int(geo_attrs['ncols'])
        nrows = int(geo_attrs['nrows'])
        nodata_value = float(geo_attrs['NODATA_value'])

        if values.shape != (nrows, ncols):
            logging.error(
                f' ===> Grid shape mismatch. Header says ({nrows}, {ncols}) '
                f'but raster values have shape {values.shape}.'
            )
            raise ValueError('Grid shape mismatch between ASCII header and raster values.')

        values[values == nodata_value] = var_null_data

        center_left = x_ll + (cellsize / 2)
        center_right = x_ll + (ncols * cellsize) - (cellsize / 2)
        center_bottom = y_ll + (cellsize / 2)
        center_top = y_ll + (nrows * cellsize) - (cellsize / 2)

        lon = x_ll + (np.arange(ncols) + 0.5) * cellsize
        lat = np.flip(y_ll + (np.arange(nrows) + 0.5) * cellsize, axis=0)

        lons, lats = np.meshgrid(lon, lat)

        transform_out = transform
        res_x = cellsize
        res_y = cellsize
        bbox_left = x_ll
        bbox_bottom = y_ll
        bbox_right = x_ll + ncols * cellsize
        bbox_top = y_ll + nrows * cellsize

    else:

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

        transform_out = transform
        res_x = res[0]
        res_y = res[1]
        bbox_left = bounds.left
        bbox_bottom = bounds.bottom
        bbox_right = bounds.right
        bbox_top = bounds.top

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

    data_attrs = {'transform': transform_out, 'crs': file_crs, 'epsg': file_epsg,
                  'bbox': [bbox_left, bbox_bottom, bbox_right, bbox_top],
                  'bb_left': bbox_left, 'bb_right': bbox_right,
                  'bb_top': bbox_top, 'bb_bottom': bbox_bottom,
                  'res_x': res_x, 'res_y': res_y,
                  'cellsize': cellsize if is_ascii_grid else None,
                  'high': height, 'wide': width}

    if is_ascii_grid:
        data_attrs = {**data_attrs, **geo_attrs}

    data_obj = create_darray(
        values, lons[0, :], lats[:, 0],
        coord_name_x=coord_name_x, coord_name_y=coord_name_y,
        dim_name_x=dim_name_x, dim_name_y=dim_name_y)

    data_obj.attrs = data_attrs

    return data_obj
# ----------------------------------------------------------------------------------------------------------------------

def get_grid_file():
    logging.error(' ===> Method "get_grid_file" is deprecated. Use "read_grid" instead.')
    raise NotImplementedError('Method "get_grid_file" is deprecated. Use "read_grid" instead.')

# ----------------------------------------------------------------------------------------------------------------------
# method to read grid data
def get_file_grid_OLD(file_name: str, file_epsg: str = 'EPSG:4326', file_dtype: str = 'float32',
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
