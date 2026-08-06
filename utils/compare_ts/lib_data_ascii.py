"""
Library Features:

Name:           lib_data_ascii
Author(s):      Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:           '20260626'
Version:        '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging

import numpy as np

from lib_utils_base import build_file_path, find_files
from lib_io_ascii import read_hydrograph_ascii

from config_info import LOGGER_NAME

# set logger
logger = logging.getLogger(LOGGER_NAME)
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# internal method to builds arrays
def _pad_or_trim_array(values, target_length):
    values = np.asarray(values, dtype=float)
    current_length = len(values)

    if current_length == target_length:
        return values

    out = np.full(target_length, np.nan, dtype=float)

    if current_length < target_length:
        out[:current_length] = values
    else:
        out[:] = values[:target_length]

    return out

# helper to add scenarios bounds and mean
def add_scenarios(
        df_hydro,
        scenario_min=None,
        scenario_max=None,
        scenario_mean=None
):

    df_hydro = df_hydro.copy()
    n_rows = len(df_hydro)

    if scenario_min is not None:
        df_hydro["scenario_min"] = _pad_or_trim_array(scenario_min, n_rows)

    if scenario_max is not None:
        df_hydro["scenario_max"] = _pad_or_trim_array(scenario_max, n_rows)

    if scenario_mean is not None:
        df_hydro["scenario_mean"] = _pad_or_trim_array(scenario_mean, n_rows)

    return df_hydro
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# helper to organize data ascii
def organize_data_ascii(dframe_hydro, cols_tag="scenario_"):

    # get scenarios
    scenario_cols = [c for c in dframe_hydro.columns if c.startswith(cols_tag)]
    scenario_min = dframe_hydro[scenario_cols].min(axis=1)
    scenario_max = dframe_hydro[scenario_cols].max(axis=1)
    scenario_mean = dframe_hydro[scenario_cols].mean(axis=1)

    dframe_hydro = add_scenarios(dframe_hydro, scenario_min, scenario_max, scenario_mean)

    return dframe_hydro
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# helper to get data ascii
def get_data_ascii(settings, time, section=None):

    if section is None:
        raise RuntimeError("Argument 'section' must be defined")

    # create file path
    file_path_raw = build_file_path(template=settings, time=time, tag=section)
    # search file path
    file_path_list = find_files(file_path_raw)

    if not file_path_list:
        logger.warning(f" ===> Hydrograph not found for pattern: {file_path_raw}. Return NoneType")
        return None

    if len(file_path_list) > 1:
        file_path_list.sort(key=lambda p: p.stat().st_mtime)

        logger.warning(
            f"WARNING: Found {len(file_path_list)} TXT files matching '{file_path_list}'. "
            f"Using the newest: {file_path_list[-1].name}"
        )

    file_path = file_path_list[-1]
    df_hydro = read_hydrograph_ascii(file_path)

    return df_hydro
# ----------------------------------------------------------------------------------------------------------------------
