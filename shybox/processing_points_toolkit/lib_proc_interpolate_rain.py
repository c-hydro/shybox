"""
Library Features:

Name:          lib_proc_interpolate_rain
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20261015'
Version:       '1.0.0'
"""
# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import pandas as pd
import xarray as xr

# import external libraries
from prism.griso import GrisoInterpolator

# common decorators
from shybox import with_logger
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to interpolate rain using griso
@with_logger(var_name='logger_stream')
def interpolate_griso(data: pd.DataFrame, grid: xr.DataArray,
                      radius_km: float = 30, correlation: str = 'fixed',
                      station_no_data: float = -9999, background_no_data: float = None,
                      **kwargs):

    # define griso configuration
    griso_config = {'radius_km': radius_km, 'correlation': correlation}

    # check correlation type
    if correlation != 'fixed':
        logger_stream.error(f'Griso correlation {correlation} not implemented yet')
        raise NotImplementedError('Case not implemented yet')
    else:
        background = None

    # check background_no_data
    if background_no_data is None:
        background_no_data = station_no_data

    # define griso handler
    griso_handler = GrisoInterpolator(config=griso_config)

    # perform the interpolation using the GRISO algorithm
    rainfall = griso_handler.interpolate(
        data, grid, background=background,
        station_nodata=station_no_data,
        background_nodata=background_no_data,
    )

    return rainfall
# ----------------------------------------------------------------------------------------------------------------------