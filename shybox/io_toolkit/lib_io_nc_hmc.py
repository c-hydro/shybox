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

from shybox.default.lib_default_info import file_conventions, file_title, file_institution, file_source, \
    file_history, file_references, file_comment, file_email, file_web_site, file_project_info, file_algorithm
from shybox.default.lib_default_time import time_units as time_default_units, time_calendar as time_default_calendar
from shybox.default.lib_default_geo import crs_obj as crs_default
from shybox.io_toolkit.lib_io_gzip import define_compress_filename, compress_and_remove
from shybox.io_toolkit.lib_io_nc_generic import get_dims_by_object, da_to_dset, to_nc_dtype

from shybox.logging_toolkit.lib_logging_utils import with_logger

from shybox.generic_toolkit.lib_utils_debug import plot_data
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# helper ro
def _build_geo_from_attrs(ds: xr.Dataset,
                         y_dim: str = "south_north",
                         x_dim: str = "west_east",
                         lon_name: str = "Longitude",
                         lat_name: str = "Latitude") -> tuple[xr.DataArray, xr.DataArray]:

    # get attributes
    attrs = ds.attrs

    # check required attributes
    required = ["xllcorner", "yllcorner", "xcellsize", "ycellsize", "ncols", "nrows"]
    missing = [k for k in required if k not in attrs]
    if missing:
        raise ValueError(
            f"Cannot rebuild {lat_name}/{lon_name}: missing attributes {missing}. "
            f"Available attrs: {list(attrs.keys())}"
        )

    # get values from attributes
    xll = float(attrs["xllcorner"])
    yll = float(attrs["yllcorner"])
    dx = float(attrs["xcellsize"])
    dy = float(attrs["ycellsize"])
    ncols = int(attrs["ncols"])
    nrows = int(attrs["nrows"])

    # create 1D coordinate vectors (assumes yllcorner is minimum latitude, xllcorner is minimum longitude)
    lon_1d = xll + np.arange(ncols, dtype=np.float64) * dx
    lat_1d = yll + np.arange(nrows, dtype=np.float64) * dy

    # define 2D meshgrid with (y, x) indexing
    lon2d, lat2d = np.meshgrid(lon_1d, lat_1d)
    lon_da = xr.DataArray(lon2d.astype(np.float32), dims=(y_dim, x_dim), name=lon_name)
    lat_da = xr.DataArray(lat2d.astype(np.float32), dims=(y_dim, x_dim), name=lat_name)

    # Add standard-ish attributes
    lon_da.attrs.update({"standard_name": "longitude", "units": "degrees_east"})
    lat_da.attrs.update({"standard_name": "latitude", "units": "degrees_north"})

    return lon_da, lat_da

# helper to check if geo is complete
def _geo_is_complete(ds: xr.Dataset,
                    y_dim: str = "south_north",
                    x_dim: str = "west_east",
                    lon_name: str = "Longitude",
                    lat_name: str = "Latitude") -> bool:

    # return False if lon/lat variables are missing
    if lon_name not in ds or lat_name not in ds:
        return False

    # get values
    lon = ds[lon_name].values
    lat = ds[lat_name].values

    # apply limits
    lon = np.where((lon < -180) | (lon > 180), np.nan, lon)
    lat = np.where((lat < -90) | (lat > 90), np.nan, lat)

    # consider "not complete" if any NaNs
    if np.isnan(lon).any() or np.isnan(lat).any():
        return False

    return True

# method to read hmc dataset from netcdf file
def read_datasets_hmc(
        path: str, debug: bool = False,
        y_dim: str = "south_north", x_dim: str = "west_east",
        lon_name: str = "Longitude", lat_name: str = "Latitude",) -> xr.Dataset:

    # read dataset
    ds = xr.open_dataset(path)

    # check if geo is complete
    if _geo_is_complete(ds, y_dim=y_dim, x_dim=x_dim, lon_name=lon_name, lat_name=lat_name):
        lon_da, lat_da = ds[lon_name], ds[lat_name]
    else:
        lon_da, lat_da = _build_geo_from_attrs(ds, y_dim=y_dim, x_dim=x_dim, lon_name=lon_name, lat_name=lat_name)

    # get time
    time = ds['time'].values

    # compute geo arrays from data
    lat_1d = lat_da.isel({x_dim: 0}).values  # take first column -> varies along y
    lon_1d = lon_da.isel({y_dim: 0}).values  # take first row    -> varies along x

    # ensure increasing order
    if lat_1d[0] > lat_1d[-1]:
        lat_1d = lat_1d[::-1]
    if lon_1d[0] > lon_1d[-1]:
        lon_1d = lon_1d[::-1]

    # create the update datasets
    ds_new = xr.Dataset(
        coords={
            "time": ("time", time),
            "latitude": ("south_north", lat_1d),
            "longitude": ("west_east", lon_1d),
        }
    )
    # store all variables in the new dataset
    for var in ds.data_vars:
        ds_new[var] = ds[var]
    ds_new['Latitude'] = lat_da
    ds_new['Longitude'] = lon_da

    # Flip dataset along latitude dimension
    ds_new = ds_new.isel(south_north=slice(None, None, -1))

    # debug testing variable(s)
    if debug:
        plot_data(ds_new['Latitude'].values)
        plot_data(ds_new['SM'].values)

    return ds_new

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
        file_name: str = None, file_format='NETCDF4', file_update: bool = True,
        ts_sim : pd.DataFrame = None, ts_obs: pd.DataFrame = None,
        time_name : str = 'time',
        time_format: str = '%Y-%m-%d %H:%M', time_dim: str = 'time', time_type: str = 'float64',
        time_calendar: str = time_default_calendar, time_units: str = time_default_units,
        data_units: str = 'm3 s-1', data_dim : str = 'sections', data_type : str = 'float64',
        data_fill_value: (float, int) = -9999.0,  data_no_value: (float, int) = -9999.0,
        data_name_sim : str = 'simulated_discharge', data_name_obs: str = 'observed_discharge',
        crs_name: str = 'crs', crs_attrs: dict = crs_default,
        compression_flag : bool = True, compression_level: (None, int) = 5,
        debug_flag : bool = True, **kwargs)  -> None:
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # INFO START
    logger_stream.info(f'Writing time series data to netCDF file "{file_name}" ... ')

    # FILE PREPARATION
    if file_update:
        if os.path.exists(file_name):
            os.remove(file_name)
    # check ts sim object
    if ts_sim is None:
        logger_stream.warning(
            f'Time series simulated dataframe is None. Skip time-series writing process to {file_name}.')
        # INFO END - SKIPPED
        logger_stream.info(f'Writing time series data to netCDF file "{file_name}" ... SKIPPED')
        return None
    # check ts obs object
    if ts_obs is None:
        logger_stream.warning(
            f'Time series observed dataframe is None')
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## TIME PREPARATION
    # define time data
    time_data = ts_sim[time_name].to_numpy()
    time_list_string, time_list_numeric = [], []
    for time_step in time_data:
        time_numeric = pd.to_datetime(str(time_step))
        time_string = time_numeric.strftime(time_format)

        time_list_string.append(time_string)
        time_list_numeric.append(time_numeric)

    time_list_nc = date2num(
        time_list_numeric, units=time_units, calendar=time_calendar)

    # get time min and max
    time_start, time_end = time_data.min().strftime(time_format), time_data.max().strftime(time_format)
    # get the data dimensions (time)
    time_n = time_data.shape[0]
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## ATTRS PREPARATION
    attrs_data_sim, attrs_type_sim = {}, {}
    if 'data' in ts_sim.attrs:
        attrs_data_sim = ts_sim.attrs['data']
    if 'type' in ts_sim.attrs:
        attrs_type_sim = ts_sim.attrs['type']

    info_data_sim, info_type_sim, info_dim_sim = {}, {}, None
    for key, obj in attrs_data_sim.items():
        if isinstance(obj, pd.Series):
            data_values_sim = obj.values
        else:
            data_values_sim = np.array(obj)

        if key in list(attrs_type_sim.keys()):
            type_values_sim = attrs_type_sim[key]
        else:
            type_values_sim = data_values_sim.dtype

        if info_dim_sim is None:
            info_dim_sim = data_values_sim.shape[0]

        info_type_sim[key] = type_values_sim
        info_data_sim[key] = data_values_sim
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## SIM DATA PREPARATION
    # define ts data (in 2d dimensions: time, ts)
    data_sim = ts_sim.drop(columns=time_name).to_numpy(dtype=float)
    # replace NaNs with no_data
    data_sim[np.isnan(data_sim)] = data_no_value
    # get the data dimensions (time and time-series)
    data_steps_sim, data_n_sim = data_sim.shape

    ## OBS DATA PREPARATION
    if ts_obs is None:
        # create empty obs matrix with same dims as sim
        data_obs = np.full((time_n, data_n_sim), data_no_value, dtype=float)
    else:
        # convert obs dataframe to numpy (same structure as sim)
        data_obs = ts_obs.drop(columns=time_name).to_numpy(dtype=float)

        # check dims consistency (optional but recommended)
        if data_obs.shape != (time_n, data_n_sim):
            raise ValueError(
                f"OBS shape {data_obs.shape} does not match SIM shape {(time_n, data_n_sim)}"
            )

        # replace NaNs with no_data
        data_obs[np.isnan(data_obs)] = data_no_value

    # get the data dimensions (time and time-series)
    data_steps_obs, data_n_obs = data_obs.shape

    # check dims consistency
    assert data_steps_sim == data_steps_obs == time_n, \
        f"Simulations time steps {data_steps_sim} does not match observed time steps {data_steps_obs}"
    assert data_n_sim == data_n_obs, \
        f"Simulations sections number {data_n_sim} does not match observed sections number {data_n_obs}"

    data_n = data_n_sim
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
    file_handle.createDimension(data_dim, data_n)
    file_handle.createDimension(time_dim, time_n)
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # FILE NETCDF - TIME OBJECT
    # create time object
    time_obj = file_handle.createVariable(
        varname=time_name, dimensions=(time_dim,), datatype=time_type)
    time_obj.calendar = time_calendar
    time_obj.units = time_units
    time_obj.time_start = time_start
    time_obj.time_end = time_end
    time_obj.time_dates = time_list_string
    time_obj.axis = 'T'
    time_obj[:] = time_list_nc

    # debug variable time object
    if debug_flag:
        date_check = num2date(time_obj[:], units=time_units, calendar=time_calendar)
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # FILE NETCDF - CRS OBJECT
    # create crs object
    crs_obj = file_handle.createVariable(crs_name, 'i')
    if crs_attrs is not None:
        for crs_key, crs_value in crs_attrs.items():
            if isinstance(crs_value, str):
                crs_obj.setncattr(crs_key.lower(), str(crs_value))
            elif isinstance(crs_value, (int, np.integer)):
                crs_obj.setncattr(crs_key.lower(), int(crs_value))
            elif isinstance(crs_value, (float, np.floating)):
                crs_obj.setncattr(crs_key.lower(), float(crs_value))
            else:
                logger_stream.warning(
                    f'Attribute CRS for "{crs_key}" is defined by NoneType or not supported')
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # FILE NETCDF - INFO OBJECT(S)
    if info_data_sim is not None:

        # iterate over registry variable(s)
        for field_key_sim, field_obj_sim in info_data_sim.items():

            # convert from pandas series to numpy array (str, float or int)
            if isinstance(field_obj_sim, pd.Series):
                field_values_sim = field_obj_sim.values
            else:
                field_values_sim = np.array(field_obj_sim)

            # get field type safely
            if (info_type_sim is not None) and isinstance(info_type_sim, dict):
                field_type_sim = info_type_sim.get(field_key_sim, None)

                if field_key_sim not in info_type_sim:
                    logger_stream.warning(
                        f'Info type for "{field_key_sim}" is not defined in info_type dictionary. '
                        f'Trying to infer dtype automatically (float -> string fallback).'
                    )
            else:
                field_type_sim = None
                logger_stream.warning(
                    f'info_type dictionary is not defined (None or invalid type). '
                    f'Trying to infer dtype for "{field_key_sim}" automatically (float -> string fallback).'
                )

            # check / infer type
            if field_type_sim is not None:
                field_type_sim = to_nc_dtype(field_type_sim)
            else:
                try:
                    np.asarray(field_values_sim, dtype=float)
                    field_type_sim = to_nc_dtype(float)
                    logger_stream.warning(f'Info type for "{field_key_sim}" inferred as float.')
                except Exception:
                    field_type_sim = to_nc_dtype(str)
                    logger_stream.warning(
                        f'Info type for "{field_key_sim}" inferred as string (fallback). Please verify values.'
                    )

            # create variable object
            info_obj = file_handle.createVariable(
                varname=field_key_sim, dimensions=(data_dim,), datatype=field_type_sim
            )
            # assign variable values
            info_obj[:] = field_values_sim
    else:
        # warning for info_data defined by NoneType
        logger_stream.warning('Info data object is defined by NoneType')
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # FILE NETCDF - DATA OBJECT
    # create data object - 2d format
    data_obj_sim = file_handle.createVariable(
        varname=data_name_sim, datatype=data_type, fill_value=data_fill_value,
        dimensions=(time_dim, data_dim),
        zlib=compression_flag, complevel=compression_level)
    data_obj_sim.units = data_units
    data_obj_sim.missing_value = data_no_value

    data_obj_sim[:, :] = data_sim

    # create obs object - 2d format
    data_obj_obs = file_handle.createVariable(
        varname=data_name_obs, datatype=data_type, fill_value=data_fill_value,
        dimensions=(time_dim, data_dim),
        zlib=compression_flag, complevel=compression_level)
    data_obj_obs.units = data_units
    data_obj_obs.missing_value = data_no_value

    data_obj_obs[:, :] = data_obs
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # FILE NETCDF - CLOSE FILE
    # close file
    file_handle.close()

    # INFO END - DONE
    logger_stream.info(f'Writing time series data to netCDF file "{file_name}" ... DONE')
    # ------------------------------------------------------------------------------------------------------------------

# ------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to write nc file (netcdf library for hmc dataset)
@with_logger(var_name='logger_stream')
def write_dataset_hmc(
        path, data, time: (pd.DatetimeIndex, pd.Timestamp) = None,
        attrs_data: dict = None, attrs_system: dict = None, attrs_x: dict = None, attrs_y: dict = None,
        file_format: str = 'NETCDF4', time_format: str ='%Y%m%d%H%M',
        compression_flag: bool =True, compression_level: (int, None) = 5,
        file_compression: bool =True, file_update: bool = True,
        time_units: str = time_default_units, time_calendar: str = time_default_calendar,
        var_system: str ='crs',
        var_time: str = 'time', var_x: str = 'longitude', var_y: str = 'latitude',
        dim_time: str = 'time', dim_x: str = 'west_east', dim_y: str = 'south_north',
        type_time: str = 'float64', type_x: str = 'float64', type_y: str = 'float64',
        debug_geo: bool = False, debug_data: bool = False, **kwargs):

    # check file format
    if not isinstance(file_format, str):
        logger_stream.warning(' ===> File format is not defined as string type! Using default format NETCDF4')
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


