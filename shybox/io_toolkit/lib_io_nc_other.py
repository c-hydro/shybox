"""
Library Features:

Name:          lib_io_nc_custom
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20250127'
Version:       '1.0.0'
"""
# ----------------------------------------------------------------------------------------------------------------------
# libraries
import os.path
import xarray as xr
import pandas as pd

from copy import deepcopy

from shybox.logging_toolkit.lib_logging_utils import with_logger
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to write nc file (xarray library for itwater dataset)
@with_logger(var_name='logger_stream')
def write_dataset_itwater(path: str, data: xr.Dataset, time: pd.DatetimeIndex, attrs_data: dict = None,
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
