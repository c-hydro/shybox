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

from shybox.orchestrator_toolkit.lib_orchestrator_utils_processes import as_process
from shybox.dataset_toolkit.dataset_handler_local import DataLocal
from shybox.logging_toolkit.lib_logging_utils import with_logger

from shybox.processing_points_toolkit.lib_proc_interpolate import interpolate_point2grid_base
from shybox.io_toolkit.lib_io_tiff import read_tiff_base
# ----------------------------------------------------------------------------------------------------------------------

@as_process(input_type='tiff', adapter_type=None, output_type='tiff')
def exec_mask_grid(data_in, ref):
    return data_in

@as_process(input_type='tiff', adapter_type=None, output_type='tiff')
def exec_set_null(data_raw, ref):
    return data_raw

# ----------------------------------------------------------------------------------------------------------------------
# helper to execute average over mask
@as_process(input_type='pandas', adapter_type=None, output_type='tiff')
@with_logger(var_name='logger_stream')
def exec_interpolate_points(
        data: Union[pd.DataFrame, None] = None,
        code: Union[pd.DataFrame, None] = None,
        time: pd.Timestamp = None,
        registry: Union[pd.DataFrame, None] = None,
        ref: xr.DataArray = None, terrain: xr.DataArray = None,
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
            print('warn')

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
    # -------------------------------------------------------------------------

    # -------------------------------------------------------------------------
    # Ready for interpolate_point2grid_base
    logger_stream.info(f' ---> Time: "{time}"')
    logger_stream.info(f' ---> Points available: {point_values.size}')

    # method to interpolate points
    grid_values = interpolate_point2grid_base(
        point_values, point_lons, point_lats,
        grid_lons=grid_lons, grid_lats=grid_lats)

    # info method end
    logger_stream.info_down('Interpolate points to grid ... DONE')

    import matplotlib.pylab as plt
    plt.figure()
    plt.imshow(grid_values)
    plt.colorbar()
    plt.show(block=True)

    return grid_values
# ----------------------------------------------------------------------------------------------------------------------



# method to select dataframe by time
def _select_by_time(
        dframe: pd.DataFrame,
        time: pd.Timestamp,
        time_dim: str = "time") -> pd.Series:

    # normalize time
    time = pd.Timestamp(time)

    # case 1: time is defined in the index
    if isinstance(dframe.index, pd.DatetimeIndex):

        # dataframe index
        time_index = pd.DatetimeIndex(
            pd.to_datetime(dframe.index)
        )

        # align timezone information
        if time.tzinfo is not None and time_index.tz is None:
            time = time.tz_localize(None)

        # check time
        if time not in time_index:
            raise RuntimeError(
                f"Time '{time}' is not available in dataframe index."
            )

        # select time
        dframe_step = dframe.loc[time]

    # case 2: time is defined as a column
    elif time_dim in dframe.columns:

        time_values = pd.to_datetime(dframe[time_dim])

        time_mask = time_values == time

        if not time_mask.any():
            raise RuntimeError(
                f"Time '{time}' is not available in column '{time_dim}'."
            )

        dframe_step = dframe.loc[time_mask]

        # only one row is expected
        if dframe_step.shape[0] > 1:
            raise RuntimeError(
                f"Multiple rows found for time '{time}'."
            )

        dframe_step = dframe_step.iloc[0]

        # remove time from station data
        dframe_step = dframe_step.drop(
            labels=time_dim,
            errors="ignore"
        )

    else:
        raise RuntimeError(
            f"Time information is not available. "
            f"Expected a DatetimeIndex or column '{time_dim}'."
        )

    return dframe_step