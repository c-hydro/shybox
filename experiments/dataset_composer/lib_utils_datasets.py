"""Time splitting, spatial resampling and copying prepared model forcing."""
import gzip
from pathlib import Path
import shutil

import numpy as np
import pandas as pd
import xarray as xr


def split_3d_to_2d(data, time=None, time_name='time'):
    if not isinstance(data, (xr.DataArray, xr.Dataset)):
        raise TypeError('Splitting requires an xarray DataArray or Dataset.')
    if time_name not in data.dims:
        selected = data
    else:
        if time_name not in data.coords:
            raise ValueError('The time dimension requires datetime coordinates.')
        times = pd.DatetimeIndex(data[time_name].values)
        if not times.is_unique or times.hasnans:
            raise ValueError('Time coordinates must be valid and unique.')
        if time is None:
            if len(times) != 1:
                raise ValueError('Select a time explicitly when splitting a multi-time cube.')
            time = times[0]
        stamp = pd.Timestamp(time)
        if stamp not in times:
            raise ValueError('Requested time is missing from the cube.')
        selected = data.isel({time_name: times.get_loc(stamp)}, drop=True)
    arrays = selected.data_vars.values() if isinstance(selected, xr.Dataset) else [selected]
    if any(array.ndim > 2 for array in arrays):
        raise ValueError('Splitting leaves unsupported ensemble or vertical dimensions.')
    return selected


def interpolate_data(values, x, y, target_x, target_y, method='nn', roi=None):
    """Use SciPy nearest/linear base methods with spherical ROI in metres."""
    from scipy.spatial import cKDTree
    values = np.asarray(values, dtype=float)
    x, y, tx, ty = map(np.asarray, (x, y, target_x, target_y))
    if x.ndim == y.ndim == 1:
        x, y = np.meshgrid(x, y)
    if tx.ndim == ty.ndim == 1:
        tx, ty = np.meshgrid(tx, ty)
    if values.ndim != 2 or values.shape != x.shape or x.shape != y.shape or tx.ndim != 2 or tx.shape != ty.shape:
        raise ValueError('Data and source/target geographical arrays have incompatible shapes.')
    if method not in ('nn', 'nearest', 'linear'):
        raise ValueError('Interpolation method must be nn, nearest or linear.')
    if roi is not None and (isinstance(roi, bool) or not np.isfinite(roi) or roi <= 0):
        raise ValueError('ROI must be null or a positive finite distance in metres.')
    valid = np.isfinite(values) & np.isfinite(x) & np.isfinite(y)
    if not valid.any():
        return np.full(tx.shape, np.nan)
    radius = 6371000.
    def cartesian(lon, lat):
        lon, lat = np.deg2rad(lon), np.deg2rad(lat)
        return radius * np.column_stack((np.cos(lat) * np.cos(lon), np.cos(lat) * np.sin(lon), np.sin(lat)))
    distance, index = cKDTree(cartesian(x[valid], y[valid])).query(cartesian(tx.ravel(), ty.ravel()))
    if method in ('nn', 'nearest'):
        result = values[valid][index].reshape(tx.shape)
    else:
        from scipy.interpolate import griddata
        result = griddata(np.column_stack((x[valid], y[valid])), values[valid], (tx, ty), method='linear')
    if roi is not None:
        threshold = 2 * radius * np.sin(min(roi / radius, np.pi) / 2)
        result[distance.reshape(tx.shape) > threshold] = np.nan
    result[(tx < x.min()) | (tx > x.max()) | (ty < y.min()) | (ty > y.max())] = np.nan
    return result


def resample_data(values, x, y, target_x, target_y, **options):
    return interpolate_data(values, x, y, target_x, target_y, **options)


def copy_as_is(source, destination):
    """Copy file contents, changing only the outer gzip envelope when needed."""
    source, destination = Path(source), Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    if (source.suffix == '.gz') == (destination.suffix == '.gz'):
        shutil.copy2(source, destination)
    else:
        opener_in = gzip.open if source.suffix == '.gz' else open
        opener_out = gzip.open if destination.suffix == '.gz' else open
        with opener_in(source, 'rb') as reader, opener_out(destination, 'wb') as writer:
            shutil.copyfileobj(reader, writer)
