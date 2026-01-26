"""
Library Features:

Name:          lib_io_nc_s3m
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20260126'
Version:       '1.1.0'
"""
# ----------------------------------------------------------------------------------------------------------------------
# libraries
import os.path
import time as tm
import pandas as pd
import numpy as np
import xarray as xr

from datetime import datetime
from netCDF4 import Dataset, date2num

from shybox.default.lib_default_info import file_conventions, file_title, file_institution, file_source, \
    file_history, file_references, file_comment, file_email, file_web_site, file_project_info, file_algorithm
from shybox.default.lib_default_time import time_units, time_calendar
from shybox.io_toolkit.lib_io_gzip import define_compress_filename, compress_and_remove

from shybox.logging_toolkit.lib_logging_utils import with_logger

from shybox.generic_toolkit.lib_utils_debug import plot_data
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to read nc file (xarray library for s3m dataset)
@with_logger(var_name='logger_stream')
def read_datasets_s3m(
        path: str,
        lon_name=("lon", "longitude", "Longitude", "nav_lon"),
        lat_name=("lat", "latitude", "Latitude", "nav_lat"),
        make_1d_if_regular=True,
        south_to_north=True) -> xr.Dataset:

    ds = xr.open_dataset(path, decode_times=True)

    def _find_var(cands):
        for c in cands:
            if c in ds.variables:
                return c
        low = {v.lower(): v for v in ds.variables}
        for c in cands:
            if c.lower() in low:
                return low[c.lower()]
        return None

    latv = _find_var(lat_name)
    lonv = _find_var(lon_name)

    if latv is None or lonv is None:
        logger_stream.error(f"Could not find lat/lon variables. Found lat={latv}, lon={lonv} in {list(ds.variables)}")
        raise KeyError(f"Could not find lat/lon variables. Found lat={latv}, lon={lonv} in {list(ds.variables)}")

    lat = ds[latv]
    lon = ds[lonv]

    if lat.ndim != 2 or lon.ndim != 2:
        logger_stream.error(f"Expected 2D lat/lon variables. Got lat.ndim={lat.ndim}, lon.ndim={lon.ndim}")
        raise ValueError(f"Expected 2D lat/lon variables. Got lat.ndim={lat.ndim}, lon.ndim={lon.ndim}")

    # original dims of lat/lon
    ydim, xdim = lat.dims   # ex: ('Y','X') or ('y','x')

    # Rename dims to canonical 'Y','X'
    rename_dims = {}
    if ydim != "Y":
        rename_dims[ydim] = "Y"
    if xdim != "X":
        rename_dims[xdim] = "X"
    ds = ds.rename(rename_dims)

    # Reload after renaming
    lat = ds[latv]
    lon = ds[lonv]
    ydim, xdim = "Y", "X"

    def _is_regular(lat2d, lon2d, tol=1e-8):
        lon_row0 = lon2d.isel({ydim: 0}).values
        lat_col0 = lat2d.isel({xdim: 0}).values
        lon_ok = np.nanmax(np.abs(lon2d.values - lon_row0[None, :])) < tol
        lat_ok = np.nanmax(np.abs(lat2d.values - lat_col0[:, None])) < tol
        return bool(lon_ok and lat_ok)

    if make_1d_if_regular and _is_regular(lat, lon):
        # Extract 1D coordinates
        lon_1d = lon.isel({ydim: 0}).values
        lat_1d = lat.isel({xdim: 0}).values

        ds = ds.assign_coords(
            longitude=("X", lon_1d),
            latitude=("Y", lat_1d),
        )

        # Ensure latitude is South -> North (ascending)
        if south_to_north and np.isfinite(ds["latitude"].values[[0, -1]]).all():
            if ds["latitude"].values[0] > ds["latitude"].values[-1]:
                ds = ds.isel(Y=slice(None, None, -1))

        # Keep original 2D fields as aux variables (optional)
        ds = ds.assign(
            Longitude_2d=(("Y", "X"), lon.values),
            Latitude_2d=(("Y", "X"), lat.values),
        )

    else:
        # Curvilinear grid: keep 2D coordinates
        ds = ds.assign_coords(
            Longitude=(("Y", "X"), lon.values),
            Latitude=(("Y", "X"), lat.values),
        )

        if south_to_north:
            row_mean = np.nanmean(ds["Latitude"].values, axis=1)
            if row_mean[0] > row_mean[-1]:
                ds = ds.isel(Y=slice(None, None, -1))

    return ds
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to write nc file (netcdf library for s3m dataset)
@with_logger(var_name='logger_stream')
def write_dataset_s3m(
        path, data, time: (pd.DatetimeIndex, pd.Timestamp) = None,
        attrs_data: dict = None, attrs_system: dict = None, attrs_x: dict = None, attrs_y: dict = None,
        file_format: str = 'NETCDF4', time_format: str ='%Y%m%d%H%M',
        file_compression: bool = True, file_update: bool = True,
        compression_flag: (bool, None) = True, compression_level: (int, None) = 5,
        var_system: str = 'crs',
        var_time: str = 'time', var_x: str = 'X', var_y: str = 'Y',
        dim_time: str = 'time', dim_x: str = 'X', dim_y: str = 'Y',
        type_time: str = 'float64',
        type_terrain: str = 'float64', type_x: str = 'float64', type_y: str = 'float64', **kwargs):

    # check file format
    if not isinstance(file_format, str):
        logger_stream.warning('File format is not defined as string type! Using default format NETCDF4')
        file_format = 'NETCDF4'

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
    dset_sizes = data.sizes
    if dim_time in dset_sizes:
        n_cols, n_rows, n_time = dset_sizes[dim_x], dset_sizes[dim_y], dset_sizes[dim_time]
    else:
        n_cols, n_rows, n_time = dset_sizes[dim_x], dset_sizes[dim_y], 1

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
        logger_stream.error('Geographical coordinates dimensions are not correctly defined!')
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

        # check variable dimensions and create variable
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
            logger_stream.error('Variable dimensions are not correctly defined!')
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
            logger_stream.error('Variable dimensions are not correctly defined!')
            raise NotImplementedError('Case not implemented yet')

    # close file
    handle.close()

    # if needed compress the file
    if file_compression:
        compress_and_remove(path_unzip, path_zip, remove_original=True)

# ----------------------------------------------------------------------------------------------------------------------
