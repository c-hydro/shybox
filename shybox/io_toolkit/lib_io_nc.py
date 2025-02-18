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
import pandas as pd
import numpy as np

from datetime import datetime
from netCDF4 import Dataset, date2num, num2date
from rasterio.crs import CRS
from copy import deepcopy

from shybox.generic_toolkit.lib_default_args import file_conventions, file_title, file_institution, file_source, \
    file_history, file_references, file_comment, file_email, file_web_site, file_project_info, file_algorithm
from shybox.generic_toolkit.lib_default_args import time_units, time_calendar

#from shybox.dataset_toolkit.merge.app_data_grid_main import logger_name, logger_arrow
from shybox.default.lib_default_geo import crs_epsg, crs_wkt

# logging
#logger_stream = logging.getLogger(logger_name)
# ----------------------------------------------------------------------------------------------------------------------



# ----------------------------------------------------------------------------------------------------------------------
# method to write nc file (netcdf library)
def write_file_nc_hmc(
        path, data, time: (pd.DatetimeIndex, pd.Timestamp) = None,
        attrs_data: dict = None, attrs_system: dict = None, attrs_x: dict = None, attrs_y: dict = None,
        file_format: str = 'NETCDF4', time_format: str ='%Y%m%d%H%M',
        compression_flag: bool =True, compression_level: int =5,
        var_system: str ='crs',
        var_time: str = 'time', var_x: str = 'longitude', var_y: str = 'latitude',
        dim_time: str = 'time', dim_x: str = 'west_east', dim_y: str = 'south_north',
        type_time: str = 'float64', type_x: str = 'float64', type_y: str = 'float64'):

    # get dimensions
    dset_dims = data.dims
    if dim_time in list(dset_dims.keys()):
        n_cols, n_rows, n_time = dset_dims[dim_x], dset_dims[dim_y], dset_dims[dim_time]
    else:
        n_cols, n_rows, n_time = dset_dims[dim_x], dset_dims[dim_y], 1
    # get geographical coordinates
    x, y = deepcopy(data[var_x]), deepcopy(data[var_y])

    # Define time dimension
    if time is not None:
        if not isinstance(time, pd.DatetimeIndex):
            time = pd.DataFrame(time)

    # define time reference
    time_period = np.array(time)
    time_labels = []
    for time_step in time_period:
        time_tmp = pd.to_datetime(str(time_step[0])).strftime(time_format)
        time_labels.append(time_tmp)
    time_start = time_labels[0]
    time_end = time_labels[-1]

    # Generate datetime from time(s)
    date_list = []
    for i, time_step in enumerate(time_period):

        date_stamp = pd.to_datetime(str(time_step[0]))
        date_str = date_stamp.strftime(time_format)

        date_list.append(datetime(int(date_str[0:4]), int(date_str[4:6]),
                                  int(date_str[6:8]), int(date_str[8:10]), int(date_str[10:12])))

    # open file
    handle = Dataset(path, 'w', format=file_format)

    # create dimensions
    handle.createDimension('west_east', n_cols)
    handle.createDimension('south_north', n_rows)
    handle.createDimension('time', n_time)
    handle.createDimension('nsim', 1)
    handle.createDimension('ntime', 2)
    handle.createDimension('nens', 1)

    # add file attributes
    handle.filedate = 'Created ' + time.ctime(time.time())
    handle.Conventions = file_conventions
    handle.title = file_title
    handle.institution = file_institution
    handle.source = file_source
    handle.history = file_history
    handle.references = file_references
    handle.comment = file_comment
    handle.email = file_email
    handle.web_site = file_web_site
    handle.project_info = file_project_info
    handle.algorithm = file_algorithm

    # variable time
    variable_time = handle.createVariable(
        varname=var_time, dimensions=(dim_time,), datatype=type_time)
    variable_time.calendar = time_calendar
    variable_time.units = time_units
    variable_time.time_date = time_labels
    variable_time.time_start = time_start
    variable_time.time_end = time_end
    variable_time.time_steps = time_steps
    variable_time.axis = 'T'
    variable_time[:] = date2num(date_list, units=variable_time.units, calendar=variable_time.calendar)

    # debug
    # date_check = num2date(variable_time[:], units=variable_time.units, calendar=variable_time.calendar)
    # print(date_check)

    # variable geo x
    variable_x = handle.createVariable(
        varname='longitude', dimensions=(dim_y, dim_x), datatype=type_x,
        zlib=compression_flag, complevel=compression_level)
    if attrs_x is not None:
        for attr_key, attr_value in attrs_x.items():
            if attr_key == 'fill_value':
                fill_value = attr_value
                variable_x.setncattr(attr_key.lower(), float(attr_value))
            elif attr_key == 'scale_factor':
                scale_factor = attr_value
                variable_x.setncattr(attr_key.lower(), float(attr_value))
            elif attr_key == 'units':
                variable_x.setncattr(attr_key.lower(), str(attr_value))
            elif attr_key == 'format':
                variable_x.setncattr(attr_key.lower(), str(attr_value))
            else:
                variable_x.setncattr(attr_key.lower(), str(attr_value).lower())
    variable_x[:, :] = np.transpose(np.rot90(x, -1))

    # variable geo y
    variable_y = handle.createVariable(
        varname='latitude', dimensions=(dim_y, dim_x), datatype=type_y,
        zlib=compression_flag, complevel=compression_level)
    if attrs_y is not None:
        for attr_key, attr_value in attrs_y.items():
            if attr_key == 'fill_value':
                fill_value = attr_value
                variable_y.setncattr(attr_key.lower(), float(attr_value))
            elif attr_key == 'scale_factor':
                scale_factor = attr_value
                variable_y.setncattr(attr_key.lower(), float(attr_value))
            elif attr_key == 'units':
                variable_y.setncattr(attr_key.lower(), str(attr_value))
            elif attr_key == 'format':
                variable_y.setncattr(attr_key.lower(), str(attr_value))
            else:
                variable_y.setncattr(attr_key.lower(), str(attr_value).lower())
    variable_y[:, :] = np.transpose(np.rot90(y, -1))

    # variable geo system
    variable_system = handle.createVariable(var_name_geo_system, 'i')
    if attrs_system is not None:
        for attr_key, attr_value in geo_system_attrs.items():
            if attr_key == 'fill_value':
                fill_value = attr_value
                variable_system.setncattr(attr_key.lower(), float(attr_value))
            elif attr_key == 'scale_factor':
                scale_factor = attr_value
                variable_system.setncattr(attr_key.lower(), float(attr_value))
            elif attr_key == 'units':
                variable_system.setncattr(attr_key.lower(), str(attr_value))
            elif attr_key == 'format':
                variable_system.setncattr(attr_key.lower(), str(attr_value))
            else:
                variable_system.setncattr(attr_key.lower(), str(attr_value).lower())

    # iterate over variables
    for variable_name in data.data_vars:

        variable_data = data[variable_name].values
        variable_dims = variable_data.ndim

        attrs_variable = {}
        if variable_name in list(attrs_data.keys()):
            attrs_variable = attrs_data[variable_name]

        variable_format = 'f4'
        if 'format' in list(attrs_data.keys()):
            variable_format = attrs_data['format']

        fill_value = None
        if 'fill_value' in list(attrs_data.keys()):
            fill_value = attrs_data['fill_value']
        scale_factor = 1
        if 'scale_factor' in list(attrs_data.keys()):
            scale_factor = attrs_data['scale_factor']

        if variable_dims == 3:
            var_handle = handle.createVariable(
                varname=variable_name, datatype=variable_format, fill_value=fill_value,
                dimensions=(dim_time, dim_y, dim_x),
                zlib=compression_flag, complevel=compression_level)
        elif variable_dims == 2:
            var_handle = handle.createVariable(
                varname=variable_name, datatype=variable_format, fill_value=fill_value,
                dimensions=(dim_y, dim_x),
                zlib=compression_flag, complevel=compression_level)
        else:
            raise NotImplementedError('Case not implemented yet')

        fill_value, scale_factor = -9999.0, 1
        for attr_key, attr_value in attrs_variable.items():
            if attr_key not in ['add_offset']:
                if attr_key == 'fill_value':
                    fill_value = attr_value
                    var_handle.setncattr(attr_key.lower(), float(attr_value))
                elif attr_key == 'scale_factor':
                    scale_factor = attr_value
                    var_handle.setncattr(attr_key.lower(), float(attr_value))
                elif attr_key == 'units':
                    var_handle.setncattr(attr_key.lower(), str(attr_value))
                elif attr_key == 'format':
                    var_handle.setncattr(attr_key.lower(), str(attr_value))
                else:
                    var_handle.setncattr(attr_key.lower(), str(attr_value).lower())

        if variable_dims == 3:

            variable_tmp = np.zeros((n_time, n_rows, n_cols))
            for i, t in enumerate(time.values):
                tmp_data = variable_data[i, :, :]
                tmp_data = np.transpose(np.rot90(tmp_data, -1))
                variable_tmp[i, :, :] = tmp_data

                '''
                import matplotlib
                matplotlib.use('TkAgg')
                plt.figure()
                plt.imshow(dset_data[variable_name].values[i, :, :])
                plt.colorbar(); plt.clim(2, 20)
                plt.figure()
                plt.imshow(variable_data[i, :, :])
                plt.colorbar(); plt.clim(2, 20)
                plt.figure()
                plt.imshow(tmp_data)
                plt.colorbar(); plt.clim(2, 20)
                plt.figure()
                plt.imshow(variable_tmp[i, :, :])
                plt.colorbar(); plt.clim(2, 20)
                plt.show()
                '''

            variable_tmp[np.isnan(variable_tmp)] = fill_value
            var_handle[:, :, :] = variable_tmp

        elif variable_dims == 2:
            variable_tmp = np.transpose(np.rot90(variable_data, -1))
            variable_tmp[np.isnan(variable_tmp)] = fill_value
            var_handle[:, :] = variable_tmp
        else:
            raise NotImplementedError('Case not implemented yet')

    # close file
    handle.close()
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


