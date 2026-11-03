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
def read_datasets_grib(file_name):

    # read the grib file
    data = xr.open_dataset(file_name, engine="cfgrib")

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

    return data
# ----------------------------------------------------------------------------------------------------------------------
