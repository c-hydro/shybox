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
import numpy as np
import xarray as xr
import pandas as pd

from copy import deepcopy

from shybox.generic_toolkit.lib_utils_geo import find_geo_names, check_geo_orientation, fix_geo_orientation
from shybox.logging_toolkit.lib_logging_utils import with_logger
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to read nc file (xarray library for itwater dataset)
@with_logger(var_name='logger_stream')
def read_dataset_itwater(
    path: str,
    lon_candidates = ("lon", "longitude", "Longitude", "nav_lon", "X"),
    lat_candidates = ("lat", "latitude", "Latitude", "nav_lat", "Y"),
    fix: bool = True, report: bool = True,
    decode_times: bool =True) -> xr.Dataset:

    # read datasets
    ds = xr.open_dataset(path, decode_times=decode_times)
    # find geo names
    lonv, latv = find_geo_names(ds, lon_candidates, lat_candidates)
    # check geo orientation (before fix)
    report_before = check_geo_orientation(ds, lonv, latv)

    # active fix geo orientation (if required)
    actions = []
    if fix:
        ds, actions = fix_geo_orientation(ds, lonv, latv)
    # check geo orientation (after fix)
    report_after = check_geo_orientation(ds, lonv, latv)

    # print only if requested
    if report:
        logger_stream.info("=== Lon/Lat check summary ===")
        logger_stream.info(f"File: {path}")
        logger_stream.info(f"Lon variable: {lonv} | Lat variable: {latv}")
        logger_stream.info("")
        logger_stream.info("Before -> After")
        logger_stream.info(f"  Lat South->North : {report_before['lat_south_to_north']} -> {report_after['lat_south_to_north']}")
        logger_stream.info(f"  Lon West->East   : {report_before['lon_west_to_east']} -> {report_after['lon_west_to_east']}")
        logger_stream.info(f"  Lon in [-180,180]: {report_before['lon_in_-180_180']} -> {report_after['lon_in_-180_180']}")

        if fix:
            logger_stream.info("")
            if actions:
                logger_stream.info("Fix actions applied:")
                for a in actions:
                    logger_stream.info(f"  - {a}")
            else:
                logger_stream.info("Fix requested, but no changes were needed.")

        logger_stream.info("=============================")

    return ds

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
