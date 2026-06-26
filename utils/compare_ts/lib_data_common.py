"""
Library Features:

Name:           lib_data_common
Author(s):      Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:           '20260626'
Version:        '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import pandas as pd
import numpy as np
from pathlib import Path

from lib_utils_base import build_file_path
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# helper to build file name
def build_file_name(settings, time, tag=None):

    # check if tag is provided
    if tag is None:
        raise ValueError("'tag' must be provided.")

    # convert tag to build paths
    tag = tag.replace(":", "_").lower()

    # create full file path
    file_path = build_file_path(template=settings, time=time, tag=tag)
    file_path = Path(file_path)

    # create destination folder if needed
    file_path.parent.mkdir(parents=True, exist_ok=True)

    return file_path
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# helper to merge data
def merge_data(df_hydro_txt, df_hydro_nc):

    # merge dataframe(s)
    df_hydro_common = pd.merge(df_hydro_txt, df_hydro_nc,
                               on="time", how="outer"  # or "inner" if you only want matching timestamps
                               ).sort_values("time")

    return df_hydro_common
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# helper to get metadata
def get_metadata(dset_hydro, section_id=None, variable_list=None):

    if variable_list is None:
        variable_list = [
            "tag", "id", "crs",
            "section_name", "station_name", "catchment_name", "data_from", "domain_name",
            "municipality", "province", "region",
            "basin", "longitude", "latitude",
            "catchment_area_km2", "correlation_time_hr", "curve_number",
            "threshold_level_1", "threshold_level_2", "threshold_level_3"
        ]

    if section_id is None:
        raise ValueError("'section_id' must be provided.")

    metadata = {}
    for var_name in variable_list:

        if var_name not in dset_hydro:
            raise RuntimeError(
                f"Metadata variable '{var_name}' not found in dataset."
            )

        values = dset_hydro[var_name].values
        if values.ndim == 0:
            value = values.item()
        else:
            value = values[section_id]
            if hasattr(value, "item"):
                value = value.item()

        metadata[var_name] = value

    return metadata

# ----------------------------------------------------------------------------------------------------------------------
