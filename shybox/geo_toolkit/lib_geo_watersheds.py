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
import warnings
import os
import numpy as np
import pandas as pd
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
# method to create mask
@with_logger(var_name="logger_stream")
def create_mask_channel(obj_channel, channel_value=1):

    # create mask array
    arr_mask = np.asarray(obj_channel) == channel_value

    # copy spatial information and adapt nodata to bool
    viewfinder = obj_channel.viewfinder.copy()
    viewfinder.nodata = False

    # create mask raster
    obj_mask = Raster(arr_mask.astype(bool), viewfinder=viewfinder)

    return obj_mask

# ----------------------------------------------------------------------------------------------------------------------

def snap_outlet(obj_grid, obj_mask, x, y):

    arr_mask = np.asarray(obj_mask)

    row = int(y)
    col = int(x)

    if arr_mask[row, col] > 0:
        return x, y

    x_snap, y_snap = obj_grid.snap_to_mask(
        obj_mask,
        (x, y)
    )

    return x_snap, y_snap

# ----------------------------------------------------------------------------------------------------------------------
# method to snap outlet
@with_logger(var_name="logger_stream")
def snap_outlet_OLD(obj_grid, obj_mask, x, y):

    # cast x and y
    col, row = int(x), int(y)
    # get arra mask
    arr_mask = np.asarray(obj_mask, dtype=bool)

    # check bounds
    nrows, ncols = arr_mask.shape
    if not (0 <= row < nrows and 0 <= col < ncols):
        raise IndexError(f"Outlet (row={row}, col={col}) is outside raster shape {arr_mask.shape}")

    # outlet already belongs to channel network
    if arr_mask[row, col]:
        logger_stream.info(f"Outlet already on channel network: row={row}, col={col}")
        return col, row

    # otherwise find nearest channel cell
    x_snap, y_snap = obj_grid.snap_to_mask(obj_mask, (col, row), xytype="index")
    # info coordinates
    logger_stream.info(f"Outlet moved from (row={row}, col={col}) to row={y_snap}, col={x_snap})")

    return x_snap, y_snap
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to delineate catchment
@with_logger(var_name="logger_stream")
def delineate_catchment(obj_grid, obj_fdir, x, y, map_fdir=DIRMAP, xy_type='coordinate'):
    obj_catchment = obj_grid.catchment(
        x=x, y=y,
        fdir=obj_fdir, dirmap=map_fdir,
        xytype=xy_type)
    return obj_catchment
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to create binary catchment map
@with_logger(var_name="logger_stream")
def create_mask_catchment(obj_catchment):

    # create mask array
    arr_mask = np.asarray(obj_catchment,dtype=bool)

    # copy spatial information and adapt nodata to bool
    viewfinder = obj_catchment.viewfinder.copy()
    viewfinder.nodata = False

    # create mask raster
    obj_mask = Raster(arr_mask, viewfinder=viewfinder)

    return obj_mask
# ----------------------------------------------------------------------------------------------------------------------

def compute_catchment_area(
        grid_values,
        grid_transform,
        unit_type='kilometers',
        decimal_round=2):

    grid_values = np.asarray(grid_values).copy()

    grid_values[grid_values <= 0] = 0
    grid_values[grid_values > 0] = 1

    grid_n = np.sum(grid_values)
    grid_area = (grid_n * abs(grid_transform[0]) * abs(grid_transform[4]))

    if unit_type == 'kilometers':
        grid_area /= 1_000_000
    elif unit_type == 'meters':
        pass
    else:
        raise NotImplementedError(f"Unit type '{unit_type}' is not supported")

    grid_area = np.round(grid_area,decimal_round)

    return grid_area, grid_n

# ----------------------------------------------------------------------------------------------------------------------
# method to compute catchment area
@with_logger(var_name="logger_stream")
def compute_area_catchment(obj_mask: Any, unit: str = "km2"):

    # convert to array
    arr_mask = np.asarray(obj_mask, dtype=bool)

    # number of active catchment cells
    n_cells = np.count_nonzero(arr_mask)
    # affine transform
    affine = obj_mask.affine

    # pixel dimensions
    dx, dy = abs(affine.a), abs(affine.e)

    # pixel area
    area_cell = dx * dy
    # total area
    area = n_cells * area_cell

    if unit == "km2":
        area = area / 1e6
    elif unit == "ha":
        area = area / 1e4
    elif unit == "m2":
        pass
    else:
        raise ValueError(f"Unsupported area unit '{unit}'. Supported units: 'm2', 'km2', 'ha'")

    logger_stream.info(f"Catchment area: {area:.3f} {unit} ({n_cells} cells, cell size={dx} x {dy})")

    return area
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to plot catchment area
@with_logger(var_name="logger_stream")
def plot_mask_catchment(
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
def convert_raster_2_array(
        obj_raster: Any,
        obj_dtype=float):

    return np.asarray(
        obj_raster,
        dtype=obj_dtype
    )
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to convert geographical coordinates to raster indexes
def convert_coords_geo2idx(
        grid,
        point_lon,
        point_lat):

    point_x, point_y = grid.nearest_cell(
        point_lon,
        point_lat
    )

    return point_x, point_y
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to snap raster-index point to channel/accumulation mask
def snap_coords_point(
        x,
        y,
        grid,
        obj_mask):

    x_snap, y_snap = grid.snap_to_mask(
        obj_mask,
        (x, y)
    )

    return x_snap, y_snap
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to compute catchment mask
def compute_catchment_mask(
        x_out,
        y_out,
        grid_fdir,
        obj_fdir,
        map_fdir=DIRMAP,
        xy_type='index'):

    obj_catch = grid_fdir.catchment(
        x=x_out,
        y=y_out,
        fdir=obj_fdir,
        dirmap=map_fdir,
        xytype=xy_type
    )

    return obj_catch
# ----------------------------------------------------------------------------------------------------------------------
