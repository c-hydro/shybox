"""
Library Features:

Name:          lib_proc_interpolate_generic
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20260928'
Version:       '1.1.0'
"""
# ----------------------------------------------------------------------------------------------------------------------
# libraries
import numpy as np
import numpy as np

from pyresample import geometry
from pyresample import kd_tree

from shybox.io_toolkit.lib_io_utils import create_darray
from shybox.logging_toolkit.lib_logging_utils import with_logger
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to join time series by registry
@with_logger(var_name='logger_stream')
def interpolate_points2grid_base(
        point_values: np.ndarray, point_lons: np.ndarray, point_lats: np.ndarray,
        grid_lons: np.ndarray, grid_lats: np.ndarray,
        method: str = "idw",
        radius_of_influence: float = 50000.0, neighbours: int = 8,
        sigma: float = 25000.0, power: float = 2.0, epsilon: float = 0.0,
        fill_value: float = np.nan, nprocs: int = 1,
        **kwargs) -> np.ndarray:

    """
    Interpolate scattered point data to a geographical grid using pyresample.

    Parameters
    ----------
    point_values : np.ndarray
        Point values, shape (n_points,).

    point_lons : np.ndarray
        Point longitudes, shape (n_points,).

    point_lats : np.ndarray
        Point latitudes, shape (n_points,).

    grid_lons : np.ndarray
        Grid longitudes, shape (ny, nx).

    grid_lats : np.ndarray
        Grid latitudes, shape (ny, nx).

    method : str
        Interpolation method:
            - "nearest"
            - "idw"
            - "gauss"

    radius_of_influence : float
        Maximum search distance in meters.

    neighbours : int
        Maximum number of neighbours used for interpolation.

    sigma : float
        Gaussian sigma in meters.

    power : float
        IDW exponent.

    epsilon : float
        Allowed uncertainty in KD-tree search, in meters.

    fill_value : float
        Value assigned where interpolation is not possible.

    nprocs : int
        Number of processes used by pyresample.

    Returns
    -------
    np.ndarray
        Interpolated grid, shape equal to grid_lons/grid_lats.
    """

    # define inputs to numpy arrays
    point_values = np.asarray(point_values, dtype=float).ravel()
    point_lons = np.asarray(point_lons, dtype=float).ravel()
    point_lats = np.asarray(point_lats, dtype=float).ravel()

    # define grid lons and latts
    grid_lons = np.asarray(grid_lons, dtype=float)
    grid_lats = np.asarray(grid_lats, dtype=float)

    # Check point dimensions
    if not (point_values.size == point_lons.size == point_lats.size):
        raise RuntimeError("Point values, longitude and latitude arrays must have the same number of elements.")
    # check output grid
    if grid_lons.shape != grid_lats.shape:
        raise RuntimeError("Grid longitude and latitude arrays must have the same shape.")

    # Remove invalid source points
    valid_points = (np.isfinite(point_values) & np.isfinite(point_lons) & np.isfinite(point_lats))
    # fitler valid points, longitudes and latitudes
    point_values = point_values[valid_points]
    point_lons = point_lons[valid_points]
    point_lats = point_lats[valid_points]

    # check points availability
    if point_values.size == 0:
        return np.full(grid_lons.shape, fill_value, dtype=float,)

    # Normalize interpolation method
    method = str(method).lower().strip()
    # method aliases
    method_aliases = {
        "nn": "nearest", "nearest_neighbour": "nearest", "nearest_neighbor": "nearest",
        "gaussian": "gauss",
        "inverse_distance": "idw", "inverse_distance_weighting": "idw",
    }

    method_selected = method_aliases.get(method, method)
    methods_supported = ["nearest", "idw", "gauss",]

    if method_selected not in methods_supported:
        raise ValueError(
            f'Interpolation method "{method_selected}" is not supported. Available methods: {methods_supported}')

    # Point geometry observations are represented as a SwathDefinition.
    source_geometry = geometry.SwathDefinition(lons=point_lons, lats=point_lats,)
    # target geometry grid is also represented by its geographical coordinates
    target_geometry = geometry.SwathDefinition(lons=grid_lons, lats=grid_lats,)

    # (1) Nearest-neighbours interpolation
    if method == "nearest":

        grid_values = kd_tree.resample_nearest(
            source_geo_def=source_geometry, data=point_values, target_geo_def=target_geometry,
            radius_of_influence=radius_of_influence, epsilon=epsilon,
            fill_value=fill_value, nprocs=nprocs,
        )

    # (2) Gaussian interpolation
    elif method == "gauss":

        grid_values = kd_tree.resample_gauss(
            source_geo_def=source_geometry, data=point_values, target_geo_def=target_geometry,
            radius_of_influence=radius_of_influence, sigmas=sigma, neighbours=neighbours, epsilon=epsilon,
            fill_value=fill_value, nprocs=nprocs,
        )

    # (3) Inverse Distance Weighting
    elif method == "idw":

        # avoid division by zero for grid cells located exactly
        # at the same position as an input point
        min_distance = kwargs.get("min_distance", 1.0)

        # compute idw weights
        def idw_weights(distance):
            distance = np.asarray(distance, dtype=float,)
            distance = np.maximum(distance, min_distance,)
            return 1.0 / np.power(distance, power,)

        grid_values = kd_tree.resample_custom(
            source_geo_def=source_geometry, data=point_values, target_geo_def=target_geometry,
            radius_of_influence=radius_of_influence, weight_funcs=idw_weights,
            neighbours=neighbours, epsilon=epsilon,
            fill_value=fill_value, nprocs=nprocs,
        )

    # Convert masked output if needed
    if np.ma.isMaskedArray(grid_values):
        grid_values = grid_values.filled(fill_value)

    # convert to float
    grid_values = np.asarray(grid_values,dtype=float,)
    # ensure expected grid shape
    grid_values = grid_values.reshape(grid_lons.shape)

    return grid_values
# ----------------------------------------------------------------------------------------------------------------------
