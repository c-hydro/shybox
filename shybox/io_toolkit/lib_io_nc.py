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
import os.path
import time as tm
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
from shybox.io_toolkit.lib_io_gzip import define_compress_filename, compress_and_remove

#from shybox.dataset_toolkit.merge.app_data_grid_main import logger_name, logger_arrow
from shybox.default.lib_default_geo import crs_epsg, crs_wkt

# logging
#logger_stream = logging.getLogger(logger_name)
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to write nc file (xarray library for itwater dataset)
def write_file_nc_itwater(path: str, data: xr.Dataset, time: pd.DatetimeIndex, attrs_data: dict = None,
                          var_time: str = 'nt', var_x: str = 'lon', var_y: str = 'lat',
                          dim_time: str = 'nt', dim_x: str = 'lon', dim_y: str = 'lat',
                          dset_mode: str ='w', dset_engine: str = 'netcdf4',
                          dset_compression: int =5, dset_format: str ='NETCDF4',
                          no_data = -9999, **kwargs) -> None:

    if os.path.exists(path):
        os.remove(path)

    data = data.rename({"longitude": dim_x, "latitude": dim_y, "time": dim_time})
    encoded = dict(zlib=True, complevel=dset_compression)

    encoding = {}
    for var_name in data.data_vars:

        if isinstance(var_name, bytes):
            tmp_name = var_name.decode("utf-8")
            data.rename({var_name: tmp_name})
            var_name = deepcopy(tmp_name)

        var_data = data[var_name]
        if len(var_data.dims) > 0:
            encoding[var_name] = deepcopy(encoded)

        #if '_FillValue' not in list(dset_encoding[var_name].keys()):
        #    dset_encoding[var_name]['_FillValue'] = no_data

    if dim_time in list(data.coords):
        encoding[dim_time] = {'calendar': 'gregorian'}

    data = data.transpose(dim_time, dim_x, dim_y)

    data.to_netcdf(path=path, format=dset_format, mode=dset_mode, engine=dset_engine, encoding=encoding)

# ----------------------------------------------------------------------------------------------------------------------



# ----------------------------------------------------------------------------------------------------------------------
# method to write nc file (netcdf library for s3m dataset)
def write_file_nc_s3m(
        path, data, time: (pd.DatetimeIndex, pd.Timestamp) = None,
        attrs_data: dict = None, attrs_system: dict = None, attrs_x: dict = None, attrs_y: dict = None,
        file_format: str = 'NETCDF4', time_format: str ='%Y%m%d%H%M',
        file_compression: bool =True, file_update: bool = True,
        compression_flag: bool =True, compression_level: int =5,
        var_system: str ='crs',
        var_time: str = 'time', var_x: str = 'X', var_y: str = 'Y',
        dim_time: str = 'time', dim_x: str = 'X', dim_y: str = 'Y',
        type_time: str = 'float64',
        type_terrain: str = 'float64', type_x: str = 'float64', type_y: str = 'float64', **kwargs):

    # manage file path
    path_unzip = path
    path_zip = define_compress_filename(path, remove_ext=False, uncompress_ext='.nc', compress_ext='.gz')
    if file_update:
        if os.path.exists(path_zip):
            os.remove(path_zip)
        if os.path.exists(path_unzip):
            os.remove(path_unzip)

    # managing attributes objects
    if attrs_data is None: attrs_data = {}
    if attrs_system is None: attrs_system = {}
    if attrs_x is None: attrs_x = {}
    if attrs_y is None: attrs_y = {}
    # managing reference object
    ref = None
    if 'ref' in kwargs: ref = kwargs['ref']

    # get dimensions
    dset_dims = data.dims
    if dim_time in list(dset_dims.keys()):
        n_cols, n_rows, n_time = dset_dims[dim_x], dset_dims[dim_y], dset_dims[dim_time]
    else:
        n_cols, n_rows, n_time = dset_dims[dim_x], dset_dims[dim_y], 1
    # get geographical coordinates
    try:
        x, y = data[var_x].values, data[var_y].values
    except KeyError:
        x, y = data[dim_x].values, data[dim_y].values

    if len(x.shape) == 1 and len(y.shape) == 1:
        x, y = np.meshgrid(x, y)
    elif len(x.shape) == 2 and len(y.shape) == 2:
        pass
    else:
        raise NotImplementedError('Case not implemented yet')

    # Define time dimension
    if time is not None:
        # define time reference
        time_period = [np.array(time)]
        time_labels = []
        for time_step in time_period:
            time_tmp = pd.to_datetime(str(time_step)).strftime(time_format)
            time_labels.append(time_tmp)
        time_start = time_labels[0]
        time_end = time_labels[-1]
        time_steps = len(time_period)

        # Generate datetime from time(s)
        date_list = []
        for i, time_step in enumerate(time_period):
            date_stamp = pd.to_datetime(str(time_step))
            date_str = date_stamp.strftime(time_format)

            date_list.append(datetime(int(date_str[0:4]), int(date_str[4:6]),
                                      int(date_str[6:8]), int(date_str[8:10]), int(date_str[10:12])))

    else:
        # case with time not defined
        time_start, time_end = 'NaD', 'NaD'
        time_steps, time_period, time_labels = 0, 0, 'NA'
        date_list = []

    # open file
    handle = Dataset(path_unzip, 'w', format=file_format)

    # create dimensions
    handle.createDimension('X', n_cols)
    handle.createDimension('Y', n_rows)
    handle.createDimension('time', n_time)
    handle.createDimension('nsim', 1)
    handle.createDimension('ntime', 2)
    handle.createDimension('nens', 1)

    # add file attributes
    handle.filedate = 'Created ' + tm.ctime(tm.time())
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

    terrain = None
    if ref is not None:

        terrain = ref.values

        key_to_search = 'xllcorner'
        if hasattr(ref, key_to_search):
            handle.xllcorner = float(ref.xllcorner)
        key_to_search = 'yllcorner'
        if hasattr(ref, key_to_search):
            handle.yllcorner = float(ref.yllcorner)
        key_to_search = 'cellsize'
        if hasattr(ref, key_to_search):
            handle.cellsize = float(ref.cellsize)
        key_to_search = 'ncols'
        if hasattr(ref, key_to_search):
            handle.ncols = int(ref.ncols)
        key_to_search = 'nrows'
        if hasattr(ref, key_to_search):
            handle.nrows = int(ref.nrows)
        key_to_search = 'NODATA_value'
        if hasattr(ref, key_to_search):
            handle.nodata_value = float(ref.NODATA_value)

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
        zlib=compression_flag, complevel=compression_level, fill_value=-9999.0)

    set_scale_factor = False, False
    if attrs_x is not None:
        for attr_key, attr_value in attrs_x.items():
            if attr_key == 'scale_factor':
                variable_x.setncattr('scale_factor', float(attr_value))
                set_scale_factor = True
            elif attr_key == 'units':
                variable_x.setncattr(attr_key.lower(), str(attr_value))
            elif attr_key == 'format':
                variable_x.setncattr(attr_key.lower(), str(attr_value))
            else:
                variable_x.setncattr(attr_key.lower(), str(attr_value).lower())
    variable_x[:, :] = np.transpose(np.rot90(x, -1))

    if not set_scale_factor:
        variable_x.setncattr('scale_factor', 1)

    # variable geo y
    variable_y = handle.createVariable(
        varname='latitude', dimensions=(dim_y, dim_x), datatype=type_y,
        zlib=compression_flag, complevel=compression_level)

    set_scale_factor = False
    if attrs_y is not None:
        for attr_key, attr_value in attrs_y.items():
            if attr_key == 'scale_factor':
                variable_y.setncattr('scale_factor', float(attr_value))
                set_scale_factor = True
            elif attr_key == 'units':
                variable_y.setncattr(attr_key.lower(), str(attr_value))
            elif attr_key == 'format':
                variable_y.setncattr(attr_key.lower(), str(attr_value))
            else:
                variable_y.setncattr(attr_key.lower(), str(attr_value).lower())
    variable_y[:, :] = np.transpose(np.rot90(y, -1))

    if not set_scale_factor:
        variable_y.setncattr('scale_factor', 1)

    # variable terrain
    if terrain is not None:
        variable_terrain = handle.createVariable(
            varname='terrain', dimensions=(dim_y, dim_x), datatype=type_terrain,
            zlib=compression_flag, complevel=compression_level, fill_value=-9999.0)

        set_scale_factor = False
        if attrs_x is not None:
            for attr_key, attr_value in attrs_x.items():
                if attr_key == 'scale_factor':
                    variable_terrain.setncattr('scale_factor', float(attr_value))
                    set_scale_factor = True
                elif attr_key == 'units':
                    variable_terrain.setncattr(attr_key.lower(), str(attr_value))
                elif attr_key == 'format':
                    variable_terrain.setncattr(attr_key.lower(), str(attr_value))
                else:
                    variable_terrain.setncattr(attr_key.lower(), str(attr_value).lower())
        variable_terrain[:, :] = np.transpose(np.rot90(terrain, -1))

        if not set_scale_factor:
            variable_terrain.setncattr('scale_factor', 1)

    # variable geo system
    variable_system = handle.createVariable(var_system, 'i')

    if attrs_system is not None:
        for attr_key, attr_value in attrs_system.items():
            if attr_key == 'units':
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

        fill_value = -9999.0
        if 'fill_value' in list(attrs_data.keys()):
            fill_value = attrs_data['fill_value']
        scale_factor = 1
        if 'scale_factor' in list(attrs_data.keys()):
            scale_factor = attrs_data['scale_factor']

        variable_data[np.isnan(variable_data)] = fill_value
        variable_data[variable_data <= fill_value] = fill_value

        '''
        import matplotlib
        matplotlib.use('TkAgg')
        import matplotlib.pylab as plt
        plt.figure()
        plt.imshow(variable_data); plt.colorbar()
        plt.show()
        '''

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

        set_scale_factor = False
        for attr_key, attr_value in attrs_variable.items():
            if attr_key == 'scale_factor':
                var_handle.setncattr('scale_factor', float(attr_value))
                set_scale_factor = True
            elif attr_key == 'units':
                var_handle.setncattr(attr_key.lower(), str(attr_value))
            elif attr_key == 'format':
                var_handle.setncattr(attr_key.lower(), str(attr_value))
            else:
                var_handle.setncattr(attr_key.lower(), str(attr_value).lower())

        if not set_scale_factor:
            if scale_factor is not None:
                var_handle.setncattr('scale_factor', scale_factor)

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

    # if needed compress the file
    if file_compression:
        compress_and_remove(path_unzip, path_zip, remove_original=True)

# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to write nc file (netcdf library for hmc dataset)
def write_file_nc_hmc(
        path, data, time: (pd.DatetimeIndex, pd.Timestamp) = None,
        attrs_data: dict = None, attrs_system: dict = None, attrs_x: dict = None, attrs_y: dict = None,
        file_format: str = 'NETCDF4', time_format: str ='%Y%m%d%H%M',
        compression_flag: bool =True, compression_level: int = 5,
        file_compression: bool =True, file_update: bool = True,
        var_system: str ='crs',
        var_time: str = 'time', var_x: str = 'longitude', var_y: str = 'latitude',
        dim_time: str = 'time', dim_x: str = 'west_east', dim_y: str = 'south_north',
        type_time: str = 'float64', type_x: str = 'float64', type_y: str = 'float64', **kwargs):

    # manage file path
    path_unzip = path
    path_zip = define_compress_filename(path, remove_ext=False, uncompress_ext='.nc', compress_ext='.gz')
    if file_update:
        if os.path.exists(path_zip):
            os.remove(path_zip)
        if os.path.exists(path_unzip):
            os.remove(path_unzip)

    # managing attributes objects
    if attrs_data is None: attrs_data = {}
    if attrs_system is None: attrs_system = {}
    if attrs_x is None: attrs_x = {}
    if attrs_y is None: attrs_y = {}
    # managing reference object
    ref = None
    if 'ref' in kwargs: ref = kwargs['ref']

    # adapt data object
    if isinstance(data, xr.DataArray):
        data = da_to_dset(data)
    elif isinstance(data, xr.Dataset):
        pass
    else:
        raise NotImplementedError('Case not implemented yet')

    # get dimensions (from xr.Dataset and xr.DataArray objects)
    dset_dims = get_dims_by_object(data)

    # select temporary dimension names
    tmp_x = dim_x
    if 'X' in list(dset_dims.keys()):
        tmp_x = 'X'
    tmp_y = dim_y
    if 'Y' in list(dset_dims.keys()):
        tmp_y = 'Y'

    if dim_time in list(dset_dims.keys()):
        n_cols, n_rows, n_time = dset_dims[tmp_x], dset_dims[tmp_y], dset_dims[dim_time]
    else:
        n_cols, n_rows, n_time = dset_dims[tmp_x], dset_dims[tmp_y], 1

    # get geographical coordinates
    try:
        x, y = data[var_x].values, data[var_y].values
    except KeyError:
        x, y = data[tmp_x].values, data[tmp_y].values

    if len(x.shape) == 1 and len(y.shape) == 1:
        x, y = np.meshgrid(x, y)
    elif len(x.shape) == 2 and len(y.shape) == 2:
        pass
    else:
        raise NotImplementedError('Case not implemented yet')

    # Define time dimension
    if time is not None:
        # define time reference
        time_period = [np.array(time)]
        time_labels = []
        for time_step in time_period:
            time_tmp = pd.to_datetime(str(time_step)).strftime(time_format)
            time_labels.append(time_tmp)
        time_start = time_labels[0]
        time_end = time_labels[-1]
        time_steps = len(time_period)

        # Generate datetime from time(s)
        date_list = []
        for i, time_step in enumerate(time_period):
            date_stamp = pd.to_datetime(str(time_step))
            date_str = date_stamp.strftime(time_format)

            date_list.append(datetime(int(date_str[0:4]), int(date_str[4:6]),
                                      int(date_str[6:8]), int(date_str[8:10]), int(date_str[10:12])))

    else:
        # case with time not defined
        time_start, time_end = 'NaD', 'NaD'
        time_steps, time_period, time_labels = 0, 0, 'NA'
        date_list = []

    # open file
    handle = Dataset(path_unzip, 'w', format=file_format)

    # create dimensions
    handle.createDimension('west_east', n_cols)
    handle.createDimension('south_north', n_rows)
    handle.createDimension('time', n_time)
    handle.createDimension('nsim', 1)
    handle.createDimension('ntime', 2)
    handle.createDimension('nens', 1)

    # add file attributes
    handle.filedate = 'Created ' + tm.ctime(tm.time())
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

    if ref is not None:
        key_to_search = 'xllcorner'
        if hasattr(ref, key_to_search):
            handle.xllcorner = float(ref.xllcorner)
        key_to_search = 'yllcorner'
        if hasattr(ref, key_to_search):
            handle.yllcorner = float(ref.yllcorner)
        key_to_search = 'cellsize'
        if hasattr(ref, key_to_search):
            handle.cellsize = float(ref.cellsize)
        key_to_search = 'ncols'
        if hasattr(ref, key_to_search):
            handle.ncols = int(ref.ncols)
        key_to_search = 'nrows'
        if hasattr(ref, key_to_search):
            handle.nrows = int(ref.nrows)
        key_to_search = 'NODATA_value'
        if hasattr(ref, key_to_search):
            handle.nodata_value = float(ref.NODATA_value)

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
    set_scale_factor = False
    variable_x = handle.createVariable(
        varname='longitude', dimensions=(dim_y, dim_x), datatype=type_x,
        zlib=compression_flag, complevel=compression_level)
    if attrs_x is not None:
        for attr_key, attr_value in attrs_x.items():
            if attr_key == 'scale_factor':
                variable_x.setncattr('scale_factor', float(attr_value))
                set_scale_factor = True
            elif attr_key == 'units':
                variable_x.setncattr(attr_key.lower(), str(attr_value))
            elif attr_key == 'format':
                variable_x.setncattr(attr_key.lower(), str(attr_value))
            else:
                variable_x.setncattr(attr_key.lower(), str(attr_value).lower())

    variable_x[:, :] = np.transpose(np.rot90(x, -1))

    if not set_scale_factor:
        variable_x.setncattr('scale_factor', 1)

    # variable geo y
    set_scale_factor = False
    variable_y = handle.createVariable(
        varname='latitude', dimensions=(dim_y, dim_x), datatype=type_y,
        zlib=compression_flag, complevel=compression_level)
    if attrs_y is not None:
        for attr_key, attr_value in attrs_y.items():
            if attr_key == 'scale_factor':
                variable_y.setncattr('scale_factor', float(attr_value))
                set_scale_factor = True
            elif attr_key == 'units':
                variable_y.setncattr(attr_key.lower(), str(attr_value))
            elif attr_key == 'format':
                variable_y.setncattr(attr_key.lower(), str(attr_value))
            else:
                variable_y.setncattr(attr_key.lower(), str(attr_value).lower())

    variable_y[:, :] = np.transpose(np.rot90(y, -1))

    if not set_scale_factor:
        variable_y.setncattr('scale_factor', 1)

    # variable geo system
    variable_system = handle.createVariable(var_system, 'i')
    if attrs_system is not None:
        for attr_key, attr_value in attrs_system.items():
            if attr_key == 'scale_factor':
                variable_system.setncattr('scale_factor', float(attr_value))
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

        fill_value = -9999.0
        if 'fill_value' in list(attrs_data.keys()):
            fill_value = attrs_data['fill_value']
        scale_factor = 1
        if 'scale_factor' in list(attrs_data.keys()):
            scale_factor = attrs_data['scale_factor']

        variable_data[np.isnan(variable_data)] = fill_value
        variable_data[variable_data <= fill_value] = fill_value

        '''
        import matplotlib
        matplotlib.use('TkAgg')
        import matplotlib.pylab as plt
        plt.figure()
        plt.imshow(variable_data); plt.colorbar()
        plt.show()
        '''

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

        set_scale_factor = False
        for attr_key, attr_value in attrs_variable.items():
            if attr_key == 'scale_factor':
                var_handle.setncattr('scale_factor', float(attr_value))
                set_scale_factor = True
            elif attr_key == 'units':
                var_handle.setncattr(attr_key.lower(), str(attr_value))
            elif attr_key == 'format':
                var_handle.setncattr(attr_key.lower(), str(attr_value))
            else:
                var_handle.setncattr(attr_key.lower(), str(attr_value).lower())

        if not set_scale_factor:
            if scale_factor is not None:
                var_handle.setncattr('scale_factor', scale_factor)

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

    # if needed compress the file
    if file_compression:
        compress_and_remove(path_unzip, path_zip, remove_original=True)

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

# ----------------------------------------------------------------------------------------------------------------------
# method to adjust dimensions naming
def get_dims_by_object(obj):
    if isinstance(obj, xr.Dataset):
        # Already a dict
        return dict(obj.sizes)
    elif isinstance(obj, xr.DataArray):
        # Need to zip names with sizes
        return dict(zip(obj.dims, obj.shape))
    else:
        raise TypeError("Input must be an xarray.Dataset or xarray.DataArray")
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to convert dataarray to dataset
def da_to_dset(da: xr.DataArray) -> xr.Dataset:
    """
    Convert a DataArray to a Dataset, preserving its name.
    If the DataArray has no name, assign a default one.
    """
    var_name = da.name if da.name is not None else "variable"
    return da.to_dataset(name=var_name)
# ----------------------------------------------------------------------------------------------------------------------
