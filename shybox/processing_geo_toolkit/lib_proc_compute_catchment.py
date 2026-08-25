"""
Library Features:

Name:          lib_proc_compute_catchment
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20260818'
Version:       '1.3.0'
"""
# ----------------------------------------------------------------------------------------------------------------------
# libraries
import warnings
import numpy as np
import pandas as pd

try:
    from pysheds.sgrid import sGrid
    from pysheds.sview import Raster
except ImportError:
    sGrid, Raster = None, None
    warnings.warn("pysheds is not available; Raster support will be skipped.", ImportWarning)

from shybox.logging_toolkit.lib_logging_utils import with_logger
from shybox.orchestrator_toolkit.lib_orchestrator_utils_processes import as_process

from shybox.geo_toolkit.lib_geo_watersheds import (
    DIRMAP, create_channel_mask, compute_grid2coords,
    create_catchment_mask, compute_catchment_area, plot_catchment_mask, delineate_catchment)
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to join time series by registry
@as_process(input_type='psndas', output_type='pandas',
            lazy_undefined_obj=False, lazy_undefined_args=False, lazy_undefined_value=None)
@with_logger(var_name='logger_stream')
def delineate_watershed(
        cnet: Raster,fdir: Raster, sections: pd.DataFrame = None,
        grid: sGrid = None, map_fdir=DIRMAP,
        longitude_tag: str = "longitude", latitude_tag: str = "latitude",
        x_tag: str = "x", y_tag: str = "y",
        debug: bool = False,
        **kwargs):

    # check sections
    if sections is None:
        logger_stream.warning("Sections dataframe is None. Return None")
        return None
    if not isinstance(sections, pd.DataFrame):
        raise TypeError(f"'sections' must be a pandas DataFrame. Got {type(sections)}")
    if sections.empty:
        logger_stream.warning("Sections dataframe is empty. Return empty dictionary")
        return {}

    # identify index columns
    if x_tag not in sections.columns:
        raise KeyError(
            f"X coordinate column '{x_tag}' not found in sections. "
            f"Available columns: {list(sections.columns)}"
        )
    if y_tag not in sections.columns:
        raise KeyError(
            f"Y coordinate column '{y_tag}' not found in sections. "
            f"Available columns: {list(sections.columns)}"
        )

    # identify coordinate columns
    if longitude_tag not in sections.columns:
        raise KeyError(
            f"X coordinate column '{longitude_tag}' not found in sections. "
            f"Available columns: {list(sections.columns)}"
        )
    if latitude_tag not in sections.columns:
        raise KeyError(
            f"Y coordinate column '{latitude_tag}' not found in sections. "
            f"Available columns: {list(sections.columns)}"
        )

    # recover pysheds Grid
    if grid is None:
        logger_stream.error("Grid object is required to delineate watersheds. Pass it using grid=<Grid>.")
        raise ValueError("Missing pysheds Grid object. Use delineate_watershed(..., grid=obj_grid)")

    # create coords from grid
    lon_2d, lat_2d = compute_grid2coords(grid)

    # create channel mask once
    channel_mask = create_channel_mask(cnet)

    # iterate over sections
    watersheds = {}
    for section_idx, section in sections.iterrows():

        # define section tag
        if ("tag" in sections.columns and pd.notna(section.get("tag"))):
            section_tag = str(section["tag"]).strip()
        else:
            section_tag = _make_tag(section)

        # info section start
        logger_stream.info(f"Delineating watershed for section '{section_tag}' ... ")

        # recover geographical and index outlet coordinates
        point_lon, point_lat = float(section[longitude_tag]), float(section[latitude_tag])
        point_x, point_y = int(section[y_tag]), int(section[x_tag])

        # shift to zero base index
        point_x, point_y = point_x - 1, point_y - 1

        # info coords
        logger_stream.info(f" ::: coordinates lon: {point_lon} lat: {point_lat} x: {point_x} y: {point_y}")

        # delineate catchment using raster indexes
        obj_catchment = delineate_catchment(
            obj_grid=grid, obj_fdir=fdir,
            x=point_x, y=point_y,
            map_fdir=map_fdir, xy_type="index"
        )

        # create binary catchment mask
        catchment_mask = create_catchment_mask(obj_catchment=obj_catchment)

        # compute catchment area
        catchment_area, catchment_cells = compute_catchment_area(
            grid_values=np.asarray(catchment_mask).copy(),
            grid_transform=catchment_mask.affine,
            unit_type="kilometers",
            transform_is_metric=False, latitude=lat_2d,
            decimal_round=2
        )

        # info catchment
        logger_stream.info(f" ::: area: {catchment_area} - cells: {catchment_cells}")

        # debug plot
        if debug:
            plot_catchment_mask(catchment_mask, title=(f"{section_tag} - Area: {catchment_area:.2f} km²"))

        # save section information
        watersheds[section_tag] = {
            "type": "watershed",
            "tag": section_tag, "section_index": section_idx,
            "x": point_x, "y": point_y,
            "longitude": point_lon, "latitude": point_lat,
            "catchment": obj_catchment, "mask": catchment_mask,
            "section": section.to_dict()
        }

        # info section end
        logger_stream.info(f"Delineating watershed for section '{section_tag}' ... DONE ")

    return watersheds
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# helper: create tag from section row
def _make_tag(row):

    parts = []
    if "catchment_name" in row and pd.notna(row["catchment_name"]):
        parts.append(str(row["catchment_name"]).strip())
    if "section_name" in row and pd.notna(row["section_name"]):
        parts.append(str(row["section_name"]).strip())

    extra = row.get("extra", np.nan)

    if pd.notna(extra):
        extra_s = str(extra).strip()
        if extra_s and extra_s.lower() != "nan":
            parts.append(extra_s)
    if not parts:
        parts.append(str(row.name))

    return " ".join(parts)
# ----------------------------------------------------------------------------------------------------------------------
