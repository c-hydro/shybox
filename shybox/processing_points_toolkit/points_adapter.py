"""
Library Features:

Name:          points_adapter
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20261016'
Version:       '1.3.0'
"""
# ----------------------------------------------------------------------------------------------------------------------
# libraries
import numpy as np
import pandas as pd
import xarray as xr

from typing import Union

from shybox.io_toolkit.lib_io_utils import create_darray
from shybox.processing_points_toolkit.lib_proc_join import join_points_to_time_series
from shybox.processing_points_toolkit.lib_proc_interpolate_generic import interpolate_points2grid_base
from shybox.processing_points_toolkit.lib_proc_interpolate_hs import interp_hsnow
from shybox.processing_points_toolkit.lib_proc_interpolate_rain import interpolate_griso

from shybox.generic_toolkit.lib_utils_debug import plot_data

# common decorators
from shybox import with_logger, deprecated, as_process
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# helper to interpolate data
@as_process(input_type='pandas', input_adapter=None,
            args_adapter=None, output_adapter=None, output_type='xarray')
@with_logger(var_name='logger_stream')
def exec_interpolate_points_rain(
        data: pd.DataFrame, grid: xr.DataArray,
        debug_data: bool = False, debug_results: bool = False,
        **kwargs):

    # info method start
    logger_stream.info_up('Interpolate rain points to grid ... ')

    # check observed points
    if data is None:
        logger_stream.error("Observed points are not defined. Interpolation cannot proceed.")
        raise ValueError("Observed points are None. Check your workflow configuration.")
    if not isinstance(data, pd.DataFrame):
        logger_stream.error("Observed points format is not expected. Interpolation cannot proceed.")
        raise TypeError(f"Observed points must be a pandas DataFrame. Got {type(points_hs).__name__}.")
    # check if points are empty
    if data.empty:
        logger_stream.warning("Observed points DataFrame is empty. Interpolation skipped.")
        logger_stream.info_down('Interpolate hs points to grid ... SKIPPED')
        return None

    # info input points
    logger_stream.info_status(f'Points | N: {len(data)} | Columns: {list(data.columns)}')
    logger_stream.info_status(f'BBox | [{data["lon"].min():.4f}, {data["lat"].min():.4f}, '
                              f'{data["lon"].max():.4f}, {data["lat"].max():.4f}]')
    logger_stream.info_status(f'Values | [{data["value"].min():.4f}, {data["value"].max():.4f}]')

    # call method to interpolate points
    grid = interpolate_griso(data, grid, **kwargs)

    # get grid values
    grid_values = grid.values
    # get grid coordinates
    grid_lon, grid_lat = grid["lon"].values, grid["lat"].values

    # analyze grid values
    valid_mask = np.isfinite(grid_values)
    n_total = grid_values.size
    n_valid = np.count_nonzero(valid_mask)
    n_invalid = n_total - n_valid
    coverage = (n_valid / n_total * 100) if n_total else 0.0

    # compute grid statistics
    if n_valid > 0:
        value_min, value_max = np.nanmin(grid_values), np.nanmax(grid_values)
        value_mean, value_std = np.nanmean(grid_values), np.nanstd(grid_values)
    else:
        value_min = value_max = value_mean = value_std = np.nan

    # info output grid
    logger_stream.info_status(f'Grid | Shape: {grid.shape}')
    logger_stream.info_status(f'BBox: [{np.nanmin(grid_lon):.4f}, {np.nanmin(grid_lat):.4f}, '
                              f'{np.nanmax(grid_lon):.4f}, {np.nanmax(grid_lat):.4f}]')
    logger_stream.info_status(
        f'Values | Min: {value_min:.4f} | Max: {value_max:.4f} | Mean: {value_mean:.4f} | Std: {value_std:.4f}')
    logger_stream.info_status(f'Quality | Valid: {n_valid}/{n_total} ({coverage:.2f}%) | NaN/Inf: {n_invalid}')

    # warn if grid contains no valid data
    if n_valid == 0:
        logger_stream.warning('Grid contains no valid values.')
    elif n_invalid > 0:
        logger_stream.warning(f'Grid contains {n_invalid} invalid cells ({100 - coverage:.2f}%).')

    # check grid result(s)
    if debug_results:
        plot_data(grid, title='interpolated grid --> rainfall')

    # info method start
    logger_stream.info_down('Interpolate rain points to grid ... DONE')

    return grid
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# helper to interpolate data
@as_process(input_type='pandas',
            input_adapter=None,
            args_adapter=None,
            output_adapter=None,
            output_type='xarray')
@with_logger(var_name='logger_stream')
def exec_interpolate_points_hs(
        points_hs: pd.DataFrame,
        satellite_sca: xr.DataArray, satellite_cp: xr.DataArray,
        s3m_hs: xr.DataArray,
        dem: xr.DataArray, m_bianco: xr.DataArray, m_paradiso: xr.DataArray,
        inv_combin: xr.DataArray = None, forest: xr.DataArray = None,
        debug_data: bool = False, debug_results: bool = False,
        **kwargs):

    # info method start
    logger_stream.info_up('Interpolate hs points to grid ... ')

    # check observed points
    if points_hs is None:
        logger_stream.error("Observed points are not defined. Interpolation cannot proceed.")
        raise ValueError("Observed points are None. Check your workflow configuration.")
    if not isinstance(points_hs, pd.DataFrame):
        logger_stream.error("Observed points format is not expected. Interpolation cannot proceed.")
        raise TypeError(f"Observed points must be a pandas DataFrame. Got {type(points_hs).__name__}.")
    # check if points are empty
    if points_hs.empty:
        logger_stream.warning("Observed points DataFrame is empty. Interpolation skipped.")
        logger_stream.info_down('Interpolate hs points to grid ... SKIPPED')
        return None

    # add options to kwargs
    kwargs['logger_stream'] = logger_stream

    # method to interpolate points data
    grid_sca, grid_sqa, grid_kernel, grid_snow_height = interp_hsnow(
        HS_df=points_hs,
        SCA_da=satellite_sca, CLOUD_da=satellite_cp,
        DEM_da=dem, Bianco_da=m_bianco, Paradiso_da=m_paradiso,
        Combin_da=inv_combin, foreste_da=forest,
        **kwargs)

    # check grid result(s)
    if debug_results:
        plot_data(grid_sca, title='interpolate points hs --> grid sca')
        plot_data(grid_sqa, title='interpolate points hs --> grid sqa')
        plot_data(grid_kernel, title='interpolate points hs --> grid kernel')
        plot_data(grid_snow_height, title='interpolate points hs --> grid snow height')

    # info method start
    logger_stream.info_down('Interpolate hs points to grid ... DONE')

    return grid_sca, grid_sqa, grid_kernel, grid_snow_height

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
    logger_stream.info_up('Interpolate air temperature points to grid ... ')

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
    grid_values = interpolate_points2grid_base(
        point_values, point_lons, point_lats,
        grid_lons=grid_lons, grid_lats=grid_lats)

    # convert to data array obj
    grid_da = create_darray(
        grid_values, grid_lons[0, :], grid_lats[:, 0], name=variable_name,
        coord_name_x=longitude_coord, coord_name_y=latitude_coord,
        dim_name_x=longitude_dim, dim_name_y=latitude_dim)
    grid_da.attrs = common_attrs

    # info method end
    logger_stream.info_down('Interpolate air temperature points to grid ... DONE')

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
