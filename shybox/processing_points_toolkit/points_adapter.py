"""
Library Features:

Name:          points_adapter
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20260902'
Version:       '1.0.0'
"""
# ----------------------------------------------------------------------------------------------------------------------
# libraries
import numpy as np
import pandas as pd
import xarray as xr

from typing import Union

from shybox.io_toolkit.lib_io_utils import create_darray
from shybox.processing_points_toolkit.lib_proc_join import join_points_to_time_series
from shybox.processing_points_toolkit.lib_proc_interpolate import interpolate_point2grid_base

# common decorators
from shybox import with_logger, deprecated, as_process
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# helper to interpolate data
@as_process(input_type='pandas',
            input_adapter=None,
            args_adapter=None,
            output_adapter={
                'data': {"method": "fx_to_dict", "kwargs": {}},
            },
            output_type='xarray')
@with_logger(var_name='logger_stream')
def exec_interpolate_points_hs(
        points_hs: pd.DataFrame,
        viirs_sca: xr.DataArray, viirs_cp: xr.DataArray, s3m_hs: xr.DataArray,
        dem: xr.DataArray, m_bianco: xr.DataArray, m_paradiso: xr.DataArray,
        inv_combin: xr.DataArray = None, forest: xr.DataArray = None,
        fill_value: float = -9999.0, no_data_value: float = -9999.0,
        min_n_updating: int = 20, min_r2: float = 0.7,
        **kwargs):

    pnt_value_avg = points_hs['HSavg'].values
    pnt_elev = points_hs['elev'].values
    pnt_lons = points_hs['lon'].values
    pnt_lats = points_hs['lat'].values

    if viirs_sca is not None:
        print()
    else:
        print()



# ----------------------------------------------------------------------------------------------------------------------




# ----------------------------------------------------------------------------------------------------------------------
# helper to execute join points to time-series
@as_process(input_type='dict',
            input_adapter={
                'data': [
                    {'method': 'adapter_variables_to_group', 'kwargs': {}},
                    {'method': 'dict_to_fx', 'kwargs': {}},
                ]
            },
            args_adapter=None,
            output_adapter=None,
            output_type='pandas')
@with_logger(var_name='logger_stream')
def exec_join_points_to_time_series(data, ref, **kwargs):

    # info method start
    logger_stream.info_up('Join points to time series ... ')

    # call method
    time_series = join_points_to_time_series(data=data, ref=ref, **kwargs)

    # info method end
    logger_stream.info_down('Join points to time series ... DONE')

    return time_series
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# helper to mask grid outside the reference
@as_process(input_type='tiff',
            input_adapter=None, args_adapter=None, output_adapter=None,
            output_type='tiff')
def exec_mask_grid(data_raw, ref=None, **kwargs):
    data_in = data_raw
    return data_in
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# helper to set null value over grid
@as_process(input_type='tiff',
            input_adapter=None, args_adapter=None, output_adapter=None,
            output_type='tiff')
def exec_set_null(data_raw, ref=None, **kwargs):
    return data_raw
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# helper to interpolate points to grid
@as_process(input_type='pandas',
            input_adapter=None,
            args_adapter=None,
            output_adapter={
                'data': {"method": "fx_to_dict", "kwargs": {}},
            },
            output_type='xarray')
@with_logger(var_name='logger_stream')
def exec_interpolate_points_air_temperature(
        data: Union[pd.DataFrame, None] = None,
        code: Union[pd.DataFrame, None] = None,
        time: pd.Timestamp = None,
        registry: Union[pd.DataFrame, None] = None,
        ref: xr.DataArray = None, terrain: xr.DataArray = None,
        variable_name: str = 'data',
        longitude_coord: str = 'longitude', latitude_coord: str = 'latitude',
        longitude_dim: str = 'longitude', latitude_dim: str = 'latitude',
        time_dim: str = "time",
        **kwargs):

    # info method start
    logger_stream.info_up('Interpolate points to grid ... ')

    # get and normalize time
    try:
        time = pd.Timestamp(time)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"The argument 'time' cannot be converted to pd.Timestamp: {time}" ) from exc

    # check datasets
    if data is None:
        raise RuntimeError("Dataset 'data' must be defined.")
    if code is None:
        raise RuntimeError("Dataset 'code' must be defined.")
    if registry is None:
        raise RuntimeError("Dataset 'registry' must be defined.")
    if terrain is None:
        raise RuntimeError("Dataset 'terrain' must be defined.")

    # get attrs
    common_attrs = {**data.attrs, **code.attrs}

    # Select only requested time
    data_step = _select_by_time(data,time=time,time_dim=time_dim,)
    code_step = _select_by_time(code, time=time,time_dim=time_dim,)
    # get lons and lats
    arr_lons, arr_lats = terrain[longitude_dim].values, terrain[latitude_dim].values
    grid_lons, grid_lats = np.meshgrid(arr_lons, arr_lats)

    # Select common station columns
    station_columns = [
        column_name
        for column_name in data_step.index
        if column_name in code_step.index
    ]

    if not station_columns:
        raise RuntimeError("No common stations are available between data and code.")

    # Prepare registry lookup
    registry_code = kwargs.get("registry_code", "code")
    registry_latitude = kwargs.get("registry_latitude", "latitude")
    registry_longitude = kwargs.get("registry_longitude", "longitude")

    # normalize registry codes
    registry_tmp = registry.copy()
    registry_tmp[registry_code] = (registry_tmp[registry_code].astype(int))
    registry_lookup = registry_tmp.set_index(registry_code)

    # Prepare point datasets
    point_values, point_codes, point_lons, point_lats = [], [], [], []
    for station_column in station_columns:

        station_name, station_code = station_column.split(':')
        station_code = int(station_code)

        # get value
        point_value = data_step[station_column]
        # get registry code from code dataframe
        point_code = code_step[station_column]

        # skip invalid data
        if pd.isna(point_value) or pd.isna(point_code):
            logger_stream.warning(f'Station {station_name} has invalid values. Remove them from interpolate')

        # get registry information
        registry_step = registry_lookup.loc[station_code]

        # manage duplicated codes if needed
        if isinstance(registry_step, pd.DataFrame):
            registry_step = registry_step.iloc[0]

        point_lon = registry_step[registry_longitude]
        point_lat = registry_step[registry_latitude]

        # check coordinates
        if pd.isna(point_lon) or pd.isna(point_lat):
            continue

        # collect point information
        point_values.append(point_value)
        point_codes.append(point_code)
        point_lons.append(point_lon)
        point_lats.append(point_lat)

    # Convert to numpy
    point_values = np.asarray(point_values, dtype=float)
    point_codes = np.asarray(point_codes)
    point_lons = np.asarray(point_lons, dtype=float)
    point_lats = np.asarray(point_lats, dtype=float)

    # check points
    if point_values.size == 0:
        raise RuntimeError(f"No valid points are available for time '{time}'.")

    # Ready for interpolate_point2grid_base
    logger_stream.info(f' ::: Time: "{time}"')
    logger_stream.info(f' ::: Points available: {point_values.size}')

    # method to interpolate points
    grid_values = interpolate_point2grid_base(
        point_values, point_lons, point_lats,
        grid_lons=grid_lons, grid_lats=grid_lats)

    # convert to data array obj
    grid_da = create_darray(
        grid_values, grid_lons[0, :], grid_lats[:, 0], name=variable_name,
        coord_name_x=longitude_coord, coord_name_y=latitude_coord,
        dim_name_x=longitude_dim, dim_name_y=latitude_dim)
    grid_da.attrs = common_attrs

    # info method end
    logger_stream.info_down('Interpolate points to grid ... DONE')

    ''' debug grid
    import matplotlib.pylab as plt
    plt.figure()
    plt.imshow(grid_da.values)
    plt.colorbar()
    plt.show(block=True)
    '''

    return grid_da

# helper to select dataframe by time
def _select_by_time(
        dframe: pd.DataFrame,
        time: pd.Timestamp,
        time_dim: str = "time") -> pd.Series:

    # normalize time
    time = pd.Timestamp(time)

    # case 1: time is defined in the index
    if isinstance(dframe.index, pd.DatetimeIndex):

        # dataframe index
        time_index = pd.DatetimeIndex(pd.to_datetime(dframe.index))

        # align timezone information
        if time.tzinfo is not None and time_index.tz is None:
            time = time.tz_localize(None)

        # check time
        if time not in time_index:
            raise RuntimeError(f"Time '{time}' is not available in dataframe index.")

        # select time
        dframe_step = dframe.loc[time]

    # case 2: time is defined as a column
    elif time_dim in dframe.columns:

        time_values = pd.to_datetime(dframe[time_dim])
        time_mask = time_values == time

        if not time_mask.any():
            raise RuntimeError(f"Time '{time}' is not available in column '{time_dim}'.")

        dframe_step = dframe.loc[time_mask]

        # only one row is expected
        if dframe_step.shape[0] > 1:
            raise RuntimeError(f"Multiple rows found for time '{time}'.")

        dframe_step = dframe_step.iloc[0]
        # remove time from station data
        dframe_step = dframe_step.drop(labels=time_dim,errors="ignore")

    else:
        raise RuntimeError(f"Time information is not available. Expected a DatetimeIndex or column '{time_dim}'.")

    return dframe_step
# ----------------------------------------------------------------------------------------------------------------------
