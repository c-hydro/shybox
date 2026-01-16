"""
Library Features:

Name:          lib_io_nc_hmc
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20260113'
Version:       '1.1.0'
"""
# ----------------------------------------------------------------------------------------------------------------------
# libraries
import os.path
import time as tm
import xarray as xr
import pandas as pd
import numpy as np

from datetime import datetime
from netCDF4 import Dataset, date2num, num2date, stringtochar

from shybox.default.lib_default_args import file_conventions, file_title, file_institution, file_source, \
    file_history, file_references, file_comment, file_email, file_web_site, file_project_info, file_algorithm
from shybox.default.lib_default_args import time_units, time_calendar
from shybox.io_toolkit.lib_io_gzip import define_compress_filename, compress_and_remove
from shybox.io_toolkit.lib_io_nc_generic import get_dims_by_object, da_to_dset

from shybox.logging_toolkit.lib_logging_utils import with_logger

from shybox.generic_toolkit.lib_utils_debug import plot_data
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# info default type(s)
info_default_attrs = {
    'tag': str,
    'id': int,
    'section_name': str,
    'station_name': str,
    'catchment_name': str,
    'domain_name': str,
    'municipality': str,
    'province': str,
    'region': str,
    'basin': int,
    'longitude': np.float64,
    'latitude': np.float64,
    'catchment_area_km2': float,
    'correlation_time_hr': float,
    'curve_number': float,
    'threshold_level_1': float,
    'threshold_level_2': float,
    'threshold_level_3': float,
    'alert_zone': int,
    'is_calibrated': str
}

# time default type(s)
time_default_type = {
    'time_type': 'GMT',  # 'GMT', 'local'
    'time_units': 'days since 1970-01-01 00:00:00',
    'time_calendar': 'gregorian'
}
# crs default type(s)
crs_attrs_default = {
    'crs_epsg_code': 4326,
    'crs_epsg_code_string': 'EPSG:4326',
    'crs_grid_mapping_name': "latitude_longitude",
    'crs_longitude_of_prime_meridian': 0.0,
    'crs_semi_major_axis': 6378137.0,
    'crs_inverse_flattening': 298.257223563
}
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# helper to squeeze (N,1) arrays to (N,) arrays
def squeeze_n1(a):
    a = np.asarray(a)
    if a.ndim == 2 and a.shape[1] == 1:
        return a[:, 0]
    return a

# helper to write 1d string variable
def write_string_1d(ds, name, data_1d, dim_n):
    seq = ["" if x is None else str(x) for x in data_1d]
    nchar = max((len(s) for s in seq), default=1)
    dim_c = f"{name}_nchar"
    if dim_c not in ds.dimensions:
        ds.createDimension(dim_c, nchar)

    v = ds.createVariable(name, "S1", (dim_n, dim_c))
    v[:, :] = stringtochar(np.array(seq, dtype=f"S{nchar}"))

# method to write time series in netcdf file
@with_logger(var_name='logger_stream')
def write_ts_hmc(
        file_name: str = None, file_format='NETCDF4',
        ts : pd.DataFrame = None, info_attrs : dict = info_default_attrs,
        var_time_name : str = 'time',
        var_time_format: str = '%Y-%m-%d %H:%M', var_time_dim: str = 'time', var_time_type: str = 'float64',
        var_data_name : str = 'discharge',
        var_data_units : str = 'm3 s-1', var_data_dim: str = 'sections', var_data_type : str = 'float64',
        var_data_fill_value: (float, int) = -9999.0,  var_data_no_value: (float, int) = -9999.0,
        var_name_crs: str = 'crs', crs_attrs: dict = crs_attrs_default,
        var_compression_flag : bool = True, var_compression_level: int = 5,
        debug_flag : bool = True, **kwargs)  -> None:

    # ------------------------------------------------------------------------------------------------------------------
    ## TIME PREPARATION
    # define time data
    time_data = ts[var_time_name].to_numpy()
    time_list_string, time_list_numeric = [], []
    for time_step in time_data:
        time_numeric = pd.to_datetime(str(time_step))
        time_string = time_numeric.strftime(var_time_format)

        time_list_string.append(time_string)
        time_list_numeric.append(time_numeric)

    time_list_nc = date2num(
        time_list_numeric, units=time_default_type['time_units'], calendar=time_default_type['time_calendar'])

    # get time min and max
    time_start, time_end = time_data.min().strftime(var_time_format), time_data.max().strftime(var_time_format)
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## ATTRS PREPARATION
    attrs_data = ts.attrs

    info_data, info_dim = {}, None
    for key, obj in attrs_data.items():
        if isinstance(obj, pd.Series):
            values = obj.values
        else:
            values = np.array(obj)

        if info_dim is None:
            info_dim = values.shape[0]

        info_data[key] = values
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## DATA PREPARATION
    # define ts data (in 2d dimensions: time, ts)
    var_data = ts.drop(columns=var_time_name).to_numpy(dtype=float)
    # get the data dimensions (time and time-series)
    time_n, var_n = var_data.shape
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # FILE NETCDF - OPEN FILE AND GENERIC INFO
    # open file
    file_handle = Dataset(file_name, 'w', format=file_format)

    # set generic attribute
    file_handle.Conventions = "CF-1.8"
    file_handle.title = "time series data (time, variable)"
    file_handle.filedate = 'Created ' + tm.ctime(tm.time())

    # create dimensions
    file_handle.createDimension(var_data_dim, var_n)
    file_handle.createDimension(var_time_dim, time_n)
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # FILE NETCDF - TIME OBJECT
    # create time object
    var_time_obj = file_handle.createVariable(
        varname=var_time_name, dimensions=(var_time_dim,), datatype=var_time_type)
    var_time_obj.calendar = time_default_type['time_calendar']
    var_time_obj.units = time_default_type['time_units']
    var_time_obj.time_start = time_start
    var_time_obj.time_end = time_end
    var_time_obj.time_dates = time_list_string
    var_time_obj.axis = 'T'
    var_time_obj[:] = time_list_nc

    # debug variable time object
    if debug_flag:
        date_check = num2date(var_time_obj[:],
                              units=time_default_type['time_units'], calendar=time_default_type['time_calendar'])
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # FILE NETCDF - CRS OBJECT
    # create crs object
    variable_crs_obj = file_handle.createVariable(var_name_crs, 'i')
    if crs_attrs is not None:
        for crs_key, crs_value in crs_attrs.items():
            if isinstance(crs_value, str):
                variable_crs_obj.setncattr(crs_key.lower(), str(crs_value))
            elif isinstance(crs_value, (int, np.integer)):
                variable_crs_obj.setncattr(crs_key.lower(), int(crs_value))
            elif isinstance(crs_value, (float, np.floating)):
                variable_crs_obj.setncattr(crs_key.lower(), float(crs_value))
            else:
                logger_stream.warning(
                    f'Attribute CRS for "{crs_key}" is defined by NoneType or not supported')
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # FILE NETCDF - INFO OBJECT(S)
    if info_data is not None:

        # set info dimension (same as var_data dimension)
        info_dim = var_data_dim

        # iterate over registry variable(s)
        for info_key, info_obj in info_data.items():

            # convert from pandas series to numpy array (str, float or int)
            if isinstance(info_obj, pd.Series):
                info_values = info_obj.values
            else:
                info_values = np.array(info_obj)

            # set registry type
            if info_key in list(info_attrs.keys()):
                info_type = info_attrs[info_key]
                if info_type is not None:
                    info_obj = file_handle.createVariable(
                        varname=info_key, dimensions=(info_dim,), datatype=info_type)
                    info_obj[:] = info_values

                else:
                    logger_stream.warning(f'Info type for "{info_key}" is defined by NoneType')
    else:
        logger_stream.warning('Info data object is defined by NoneType')
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # FILE NETCDF - DATA OBJECT
    # create data object - 2d format
    var_data_obj = file_handle.createVariable(
        varname=var_data_name, datatype=var_data_type, fill_value=var_data_fill_value,
        dimensions=(var_time_dim, var_data_dim),
        zlib=var_compression_flag, complevel=var_compression_level)
    var_data_obj.units = var_data_units
    var_data_obj.no_data = var_data_no_value

    var_data_obj[:, :] = var_data
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # FILE NETCDF - CLOSE FILE
    # close file
    file_handle.close()
    # ------------------------------------------------------------------------------------------------------------------

# ------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to write nc file (netcdf library for hmc dataset)
@with_logger(var_name='logger_stream')
def write_dataset_hmc(
        path, data, time: (pd.DatetimeIndex, pd.Timestamp) = None,
        attrs_data: dict = None, attrs_system: dict = None, attrs_x: dict = None, attrs_y: dict = None,
        file_format: str = 'NETCDF4', time_format: str ='%Y%m%d%H%M',
        compression_flag: bool =True, compression_level: int = 5,
        file_compression: bool =True, file_update: bool = True,
        var_system: str ='crs',
        var_time: str = 'time', var_x: str = 'longitude', var_y: str = 'latitude',
        dim_time: str = 'time', dim_x: str = 'west_east', dim_y: str = 'south_north',
        type_time: str = 'float64', type_x: str = 'float64', type_y: str = 'float64',
        debug_geo: bool = False, debug_data: bool = False, **kwargs):

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
        logger_stream.error('Data obj not expected. Case not implemented yet')

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
        logger_stream.error('Geographical object not expected. Case not implemented yet')

    # debug geo information
    if debug_geo: plot_data(x, var_name='longitude/west_east'); plot_data(y, name='latitude/south_north')

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

        # debug data
        if debug_data: plot_data(variable_data, var_name=variable_name)

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
            logger_stream.error('Variable dimensions not expected. Case not implemented yet')

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
            logger_stream.error('Data dimensions not expected. Case not implemented yet')

    # close file
    handle.close()

    # if needed compress the file
    if file_compression:
        compress_and_remove(path_unzip, path_zip, remove_original=True)

# ----------------------------------------------------------------------------------------------------------------------


