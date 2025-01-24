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

import pandas as pd
import xarray as xr
# ----------------------------------------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to parse complex row to string
def parse_row2str(row_obj, row_delimiter='#'):

    # Check if line starts with {number}######
    if row_obj.count(row_delimiter) > 1:
        pattern = r'[0-9]'
        row_obj = re.sub(pattern, '', row_obj)

    row_string = row_obj.split(row_delimiter)[0]

    # Check delimiter character (in intake file info there are both '#' and '%')
    if ('#' not in row_obj) and ('%' in row_string):
        row_string = row_obj.split('%')[0]

    row_string = row_string.strip()

    return row_string
# -------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to substitute string by tags
def substitute_string_by_tags(string: str, tags: dict = {}) -> str:
    for key, value in tags.items():
        key = '{' + key + '}'
        string = string.replace(key, value)
    return string
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to substitute string by date
def substitute_string_by_date(string, date: pd.Timestamp, tags_template: dict = {}):
    if date is not None:
        for tag_key, tag_value in tags_template.items():
            tag_key = '{' + tag_key + '}'
            if tag_key in string:
                date_str = date.strftime(tag_value)
                string = string.replace(tag_key, date_str)
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
