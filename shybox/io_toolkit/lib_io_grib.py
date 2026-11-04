"""
Library Features:

Name:          lib_io_grib
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20261102'
Version:       '1.1.0'
"""
# ----------------------------------------------------------------------------------------------------------------------
# libraries
import os
import struct

from copy import deepcopy

import numpy as np
import pandas as pd
import xarray as xr

from shybox import with_logger

# debugging
import matplotlib.pylab as plt
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# Method to read 2d variable in binary format (saved as 1d integer array)
@with_logger(var_name='logger_stream')
def read_datasets_grib(file_name, filter_by_keys=None, indexpath=None,
                       time_frequency=None, accumulated=False, spatial_bounds=None):

    # read the grib file
    backend_kwargs = {}
    if filter_by_keys is not None:
        backend_kwargs['filter_by_keys'] = filter_by_keys
    if indexpath is not None:
        backend_kwargs['indexpath'] = indexpath
    data = xr.open_dataset(file_name, engine="cfgrib", backend_kwargs=backend_kwargs)

    # 1) Preserve the original reference time
    #    (rename 'time' -> 'time_start' to keep it)
    if "time" not in data.coords:
        raise ValueError("Dataset has no 'time' coordinate.")
    data = data.rename({"time": "time_start"})

    # 2) Sanity check
    if "step" not in data.coords and "step" not in data.dims:
        logger_stream.error("Dataset has no 'step' coordinate/dimension from cfgrib.")
        raise ValueError("Dataset has no 'step' coordinate/dimension from cfgrib.")

    # 3) Compute valid timestamps and assign them back onto the 'step' coordinate
    #    (this keeps the dimension name 'step' but makes its values datetimes)
    valid_time = data["time_start"] + data["step"]
    data = data.assign_coords(step=valid_time)
    # 4) Sort by the new datetime 'step' (handy if steps weren't monotonic)
    data = data.sortby("step")

    # 5) Optional: prove it's now a DatetimeIndex
    assert np.issubdtype(data["step"].dtype, np.datetime64), "step is not datetime64 after conversion"

    # check if there is a single variable in the dataset
    if len(data.data_vars) == 1:
        data = data[list(data.data_vars)[0]]

    if spatial_bounds is not None:
        west, south, east, north = spatial_bounds
        if west >= east or south >= north:
            raise ValueError('spatial_bounds must be [west, south, east, north].')
        for axis, lower, upper in [('longitude', west, east), ('latitude', south, north)]:
            if data[axis].ndim != 1:
                raise ValueError('Spatial subsetting requires a regular latitude/longitude grid.')
            data = data.sel({axis: slice(lower, upper) if data[axis][0] < data[axis][-1]
                             else slice(upper, lower)})
            if data.sizes[axis] == 0:
                raise ValueError(f'No GRIB points within spatial_bounds on {axis}.')

    if time_frequency is not None:
        if not isinstance(data, xr.DataArray):
            raise ValueError('Temporal preparation requires a single filtered GRIB variable.')
        data = prepare_forecast_time(data, frequency=time_frequency, accumulated=accumulated)

    return data
# ----------------------------------------------------------------------------------------------------------------------


def prepare_forecast_time(data, frequency='h', accumulated=False, negative_tolerance=0.01):
    """Prepare regular forcing timestamps; rainfall is an interval total ending at each timestamp.

    Instantaneous fields are linearly interpolated. Accumulations must start at
    the forecast reference with zero total. Differences are distributed uniformly
    over each interval, conserving totals; no extrapolation is performed.
    """
    if not isinstance(data, xr.DataArray) or 'step' not in data.dims:
        raise ValueError('Forecast preparation requires a DataArray with step timestamps.')
    times = pd.DatetimeIndex(data.step.values)
    if len(times) < 2 or times.hasnans or times.has_duplicates or not times.is_monotonic_increasing:
        raise ValueError('Forecast step timestamps must be valid, sorted, unique and contain at least two values.')
    delta = pd.Timedelta(pd.tseries.frequencies.to_offset(frequency))
    intervals = np.diff(times.asi8)
    if delta.value <= 0 or np.any(intervals % delta.value):
        raise ValueError('Forecast intervals must be exact multiples of the output frequency.')
    target = pd.date_range(times[0], times[-1], freq=frequency)
    attrs = data.attrs.copy()
    if not accumulated:
        result = data.interp(step=target)
    else:
        if 'time_start' not in data.coords or pd.Timestamp(data.time_start.item()) != times[0]:
            raise ValueError('Cumulative rainfall must include the forecast initialization step.')
        units = attrs.get('units', '').lower().replace(' ', '')
        factors = {'m': 1000., 'mm': 1., 'kgm**-2': 1., 'kgm^-2': 1., 'kg/m^2': 1.}
        if units not in factors:
            raise ValueError(f'Unsupported cumulative rainfall units: {attrs.get("units")}')
        total = data * factors[units]
        baseline = total.isel(step=0)
        if bool((abs(baseline) > negative_tolerance).any()):
            raise ValueError('Cumulative rainfall must start at zero; refusing to invent a baseline.')
        increments = total.diff('step')
        if bool((increments < -negative_tolerance).any()):
            raise ValueError('Cumulative rainfall decreases beyond tolerance; check forecast resets.')
        counts = xr.DataArray(intervals / delta.value, dims='step', coords={'step': increments.step})
        rates = increments.clip(min=0) / counts
        # Each timestamp belongs to the interval ending at the next native step.
        index = np.searchsorted(times.asi8[1:], target.asi8[1:], side='left')
        result = rates.isel(step=xr.DataArray(index, dims='step')).assign_coords(step=target[1:])
        attrs.update(units='mm', processing='Interval accumulation; uniform subdivision of native forecast totals')
    result.name = data.name
    result.attrs = attrs
    return result
