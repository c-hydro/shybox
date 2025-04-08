"""
Library Features:

Name:          lib_io_utils
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20240801'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import re
from datetime import datetime
import dateutil.parser as dparser

import numpy as np
import pandas as pd
import xarray as xr
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to extract time from string
def extract_time_from_string(string: str, time_format: str = None):
    time_tmp = dparser.parse(string, fuzzy=True)
    if time_format is not None:
        time_str = time_tmp.strftime(time_format)
        time_stamp = pd.Timestamp(time_str)
    else:
        time_stamp = pd.Timestamp(time_tmp)
    return time_stamp
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to substitute string by tags
def substitute_string_by_tags(string: str, tags: dict = None) -> str:
    if tags is None:
        tags = {}
    for key, value in tags.items():
        key = '{' + key + '}'
        string = string.replace(key, value)
    return string
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to substitute string by date
def substitute_string_by_date(string: str, date: pd.Timestamp, tags: dict = None) -> str:
    if tags is None:
        tags = {}
    if date is not None:
        for key, value in tags.items():
            key = '{' + key + '}'
            if key in string:
                date_str = date.strftime(value)
                string = string.replace(key, date_str)
    return string
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to create a data array
def create_darray(data, geo_x, geo_y, time=None, name=None,
                  coord_name_x='longitude', coord_name_y='latitude', coord_name_time='time',
                  dim_name_x='longitude', dim_name_y='latitude', dim_name_time='time',
                  dims_order=None):

    if dims_order is None:
        dims_order = [dim_name_y, dim_name_x]
    if time is not None:
        dims_order = [dim_name_y, dim_name_x, dim_name_time]

    if geo_x.shape.__len__() == 2:
        geo_x = geo_x[0, :]
    if geo_y.shape.__len__() == 2:
        geo_y = geo_y[:, 0]

    if time is None:

        data_da = xr.DataArray(data,
                               dims=dims_order,
                               coords={coord_name_x: (dim_name_x, geo_x),
                                       coord_name_y: (dim_name_y, geo_y)})

    elif isinstance(time, pd.DatetimeIndex):

        if data.shape.__len__() == 2:
            data = np.expand_dims(data, axis=-1)

        data_da = xr.DataArray(data,
                               dims=dims_order,
                               coords={coord_name_x: (dim_name_x, geo_x),
                                       coord_name_y: (dim_name_y, geo_y),
                                       coord_name_time: (dim_name_time, time)})
    else:
        logging.error(' ===> Time obj is in wrong format')
        raise IOError('Variable time format not valid')

    if name is not None:
        data_da.name = name

    return data_da
# ----------------------------------------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# method to create dataset
def create_dset(var_data_values,
                var_geo_values, var_geo_x, var_geo_y,
                var_data_time=None,
                var_data_name='variable', var_geo_name='terrain', var_data_attrs=None, var_geo_attrs=None,
                var_geo_1d=False,
                file_attributes=None,
                coord_name_x='longitude', coord_name_y='latitude', coord_name_time='time',
                dim_name_x='west_east', dim_name_y='south_north', dim_name_time='time',
                dims_order_2d=None, dims_order_3d=None):

    var_geo_x_tmp = var_geo_x
    var_geo_y_tmp = var_geo_y
    if var_geo_1d:
        if (var_geo_x.shape.__len__() == 2) and (var_geo_y.shape.__len__() == 2):
            var_geo_x_tmp = var_geo_x[0, :]
            var_geo_y_tmp = var_geo_y[:, 0]
    else:
        if (var_geo_x.shape.__len__() == 1) and (var_geo_y.shape.__len__() == 1):
            var_geo_x_tmp, var_geo_y_tmp = np.meshgrid(var_geo_x, var_geo_y)

    if dims_order_2d is None:
        dims_order_2d = [dim_name_y, dim_name_x]
    if dims_order_3d is None:
        dims_order_3d = [dim_name_y, dim_name_x, dim_name_time]

    if isinstance(var_data_time, pd.Timestamp):
        var_data_time = pd.DatetimeIndex([var_data_time])
    elif isinstance(var_data_time, pd.DatetimeIndex):
        pass
    else:
        raise NotImplemented('Case not implemented yet')

    var_dset = xr.Dataset(coords={coord_name_time: ([dim_name_time], var_data_time)})
    var_dset.coords[coord_name_time] = var_dset.coords[coord_name_time].astype('datetime64[ns]')

    if file_attributes:
        if isinstance(file_attributes, dict):
            var_dset.attrs = file_attributes

    var_da_terrain = xr.DataArray(np.flipud(var_geo_values),  name=var_geo_name,
                                  dims=dims_order_2d,
                                  coords={coord_name_x: ([dim_name_y, dim_name_x], var_geo_x_tmp),
                                          coord_name_y: ([dim_name_y, dim_name_x], np.flipud(var_geo_y_tmp))})
    var_dset[var_geo_name] = var_da_terrain
    var_geo_attrs_select = select_attrs(var_geo_attrs)

    if var_geo_attrs_select is not None:
        var_dset[var_geo_name].attrs = var_geo_attrs_select

    if var_data_values.shape.__len__() == 2:
        var_da_data = xr.DataArray(np.flipud(var_data_values), name=var_data_name,
                                   dims=dims_order_2d,
                                   coords={coord_name_x: ([dim_name_y, dim_name_x], var_geo_x_tmp),
                                           coord_name_y: ([dim_name_y, dim_name_x], np.flipud(var_geo_y_tmp))})
    elif var_data_values.shape.__len__() == 3:
        var_da_data = xr.DataArray(np.flipud(var_data_values), name=var_data_name,
                                   dims=dims_order_3d,
                                   coords={coord_name_time: ([dim_name_time], var_data_time),
                                           coord_name_x: ([dim_name_y, dim_name_x], var_geo_x_tmp),
                                           coord_name_y: ([dim_name_y, dim_name_x], np.flipud(var_geo_y_tmp))})
    else:
        raise NotImplemented

    if var_data_attrs is not None:
        if attr_valid_range in list(var_data_attrs.keys()):
            valid_range = var_data_attrs[attr_valid_range]
            var_da_data = clip_map(var_da_data, valid_range)

        if attr_missing_value in list(var_data_attrs.keys()):
            missing_value = var_data_attrs[attr_missing_value]
            var_da_data = var_da_data.where(var_da_terrain > 0, other=missing_value)

    var_dset[var_data_name] = var_da_data
    if var_data_attrs is not None:
        var_data_attrs_select = select_attrs(var_data_attrs)
    else:
        var_data_attrs_select = None

    if var_data_attrs_select is not None:
        var_dset[var_data_name].attrs = var_data_attrs_select

    return var_dset

# -------------------------------------------------------------------------------------