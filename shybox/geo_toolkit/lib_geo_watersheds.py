"""
Library Features:

Name:          lib_geo_watersheds
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20260818'
Version:       '1.1.0'

## NOTE FOR PROJ CONFIGURATION:
# PROJ configuration for the active Conda environment
export PROJ_DATA="${CONDA_PREFIX}/share/proj"

# Remove potentially stale PROJ_LIB inherited from the shell.
# PROJ >= 9.1 uses PROJ_DATA.
unset PROJ_LIB

# Configure in pycharm using (for example):
PROJ_DATA=/home/fabio/Documents/Work_Area/Code_Development/Workspace/shybox/conda/envs/shybox_geo_libraries/share/proj
PROJ_LIB=""
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import os
import numpy as np

from typing import Any

import matplotlib.pyplot as plt

from shybox.logging_toolkit.lib_logging_utils import with_logger

# default HMC / numeric-keypad D8
DIRMAP = (8, 9, 6, 3, 2, 1, 4, 7)
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to import pysheds only for defined methods
def import_pysheds():
    try:
        from pysheds.grid import Grid
        from pysheds.sview import Raster
    except ImportError as exc:
        raise ImportError("This method requires the optional dependency 'pysheds'.") from exc
    return Grid, Raster
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# helper to check pysheds raster
def is_pysheds_raster(obj):
    return (
        obj.__class__.__name__ == "Raster"
        and obj.__class__.__module__.startswith("pysheds.")
    )
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to create a pyshed grid from ascii file
@with_logger(var_name="logger_stream")
def create_grid(file_raster, name_raster='grid'):

    # import pysheds library
    Grid, Raster = import_pysheds()

    # define metadata (to trace variable)
    metadata_grid = {'name': name_raster}
    # initialize grid
    obj_grid = Grid.from_ascii(file_raster, metadata=metadata_grid)
    return obj_grid
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to compute grid longitude and latitude
def compute_grid2coords(grid):

    # get grid shape
    nrows, ncols = grid.shape

    # get affine transform
    transform = grid.affine

    # create row and column indexes
    cols, rows = np.meshgrid(np.arange(ncols), np.arange(nrows))

    # compute coordinates at cell centers
    grid_lon = (transform.c
        + (cols + 0.5) * transform.a
        + (rows + 0.5) * transform.b
    )

    grid_lat = (transform.f
        + (cols + 0.5) * transform.d
        + (rows + 0.5) * transform.e
    )

    return grid_lon, grid_lat
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to read flow direction ascii file
@with_logger(var_name="logger_stream")
def read_geo(file_name, name_data='raster', obj_data=None, mandatory_data=True):

    # check file availability
    if not os.path.exists(file_name):
        if mandatory_data:
            logger_stream.error(f'File {file_name} does not exist. Exit.')
            raise FileNotFoundError('Datasets is mandatory. Check your settings')
        else:
            logger_stream.warning(f'File {file_name} does not exist. Return None')
            return None

    # create reference object
    if obj_data is None:
        obj_data = create_grid(file_name)

    # define metadata (to trace variable)
    metadata_geo = {'name': name_data}

    # add raster data
    obj_geo = obj_data.read_ascii(file_name, metadata=metadata_geo)

    return obj_geo
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to create channel_mask
@with_logger(var_name="logger_stream")
def create_channel_mask(obj_channel, channel_value=1):

    # import pysheds library
    Grid, Raster = import_pysheds()

    # create mask array
    arr_mask = np.asarray(obj_channel) == channel_value

    # copy spatial information and adapt nodata to bool
    viewfinder = obj_channel.viewfinder.copy()
    viewfinder.nodata = False

    # create mask raster
    obj_mask = Raster(arr_mask.astype(bool), viewfinder=viewfinder)

    return obj_mask

# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to snap outlet
@with_logger(var_name="logger_stream")
def snap_outlet(obj_grid, obj_mask, x, y):

    # get data
    arr_mask = np.asarray(obj_mask)
    row, col = int(y), int(x)

    # check bounds
    nrows, ncols = arr_mask.shape
    if not (0 <= row < nrows and 0 <= col < ncols):
        raise IndexError(f"Outlet (row={row}, col={col}) is outside raster shape {arr_mask.shape}")

    # if outlet is over the channel network
    if arr_mask[row, col] > 0:
        logger_stream.info(f"Outlet already on channel network: row={row}, col={col}")
        return x, y

    x_snap, y_snap = obj_grid.snap_to_mask(
        obj_mask, (x, y), xytype="index")

    # info coordinates
    logger_stream.info(f"Outlet moved from (row={row}, col={col}) to row={y_snap}, col={x_snap})")

    return x_snap, y_snap
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to delineate catchment
@with_logger(var_name="logger_stream")
def delineate_catchment(obj_grid, obj_fdir, x, y, map_fdir=DIRMAP, xy_type='index'):
    obj_catchment = obj_grid.catchment(x=x, y=y,fdir=obj_fdir, dirmap=map_fdir,xytype=xy_type)
    return obj_catchment
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to create catchment binary map
@with_logger(var_name="logger_stream")
def create_catchment_mask(obj_catchment):

    # import pysheds library
    Grid, Raster = import_pysheds()

    # create mask array
    arr_mask = np.asarray(obj_catchment,dtype=bool)

    # copy spatial information and adapt nodata to bool
    viewfinder = obj_catchment.viewfinder.copy()
    viewfinder.nodata = False

    # create mask raster
    obj_mask = Raster(arr_mask, viewfinder=viewfinder)

    return obj_mask
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to compute catchment area
def compute_catchment_area(
        grid_values, grid_transform,
        unit_type='kilometers', transform_is_metric=True,
        latitude=None, decimal_round=2):

    # get grid values
    grid_values = np.asarray(grid_values)
    # create catchment mask
    mask_catchment = (np.isfinite(grid_values) &(grid_values > 0))

    # count catchment cells
    grid_n = int(np.count_nonzero(mask_catchment))
    # get grid resolution
    res_x, res_y = abs(grid_transform[0]), abs(grid_transform[4])

    # compute cell area
    if transform_is_metric:
        # transform units are meters
        cell_area_m2 = res_x * res_y
    else:

        # check latitude availability
        if latitude is None:
            raise ValueError("'latitude' must be provided when transform_is_metric=False")

        # latitude can be scalar or grid
        latitude = np.asarray(latitude)
        if latitude.ndim > 0:
            if latitude.shape != grid_values.shape:
                raise ValueError("'latitude' grid must have the same shape as 'grid_values'")

            # representative latitude of catchment
            latitude_ref = float(np.nanmean(latitude[mask_catchment]))

        else:
            latitude_ref = float(latitude)

        # approximate degree -> meters
        meters_per_degree_lat = 111_320.0
        meters_per_degree_lon = (111_320.0 * np.cos(np.deg2rad(latitude_ref)))

        cell_size_x_m = res_x * meters_per_degree_lon
        cell_size_y_m = res_y * meters_per_degree_lat
        cell_area_m2 = cell_size_x_m * cell_size_y_m

    # compute total catchment area
    grid_area_m2 = float(grid_n * cell_area_m2)

    # convert units
    if unit_type == 'kilometers':
        grid_area = grid_area_m2 / 1_000_000.0
    elif unit_type == 'meters':
        grid_area = grid_area_m2
    else:
        raise NotImplementedError(f"Unit type '{unit_type}' is not supported")

    # force scalar output
    grid_area = float(np.round(grid_area, decimal_round))

    return grid_area, grid_n
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to plot catchment mask
@with_logger(var_name="logger_stream")
def plot_catchment_mask(
        obj_mask: Any,
        title: str = "Catchment mask",
        show: bool = True):

    # get arr mask
    arr_mask = np.asarray(obj_mask, dtype=bool)

    # recover raster extent
    xmin, ymin, xmax, ymax = obj_mask.extent

    fig, ax = plt.subplots(figsize=(10, 8))
    ax.imshow(arr_mask, extent=(xmin, xmax, ymin, ymax), origin="upper")

    ax.set_title(title)
    ax.set_xlabel("x")
    ax.set_ylabel("y")

    if show:
        plt.show()

    return fig, ax
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to convert raster to array
def convert_raster_2_array(obj_raster: Any, obj_dtype=float):
    return np.asarray(obj_raster,dtype=obj_dtype)
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to convert geographical coordinates to raster indexes
def convert_coords_geo2idx(grid, point_lon, point_lat):
    point_x, point_y = grid.nearest_cell(point_lon, point_lat)
    return point_x, point_y
# ----------------------------------------------------------------------------------------------------------------------
