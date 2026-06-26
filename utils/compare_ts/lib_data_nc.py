"""
Library Features:

Name:           lib_data_nc
Author(s):      Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:           '20260626'
Version:        '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import warnings
warnings.filterwarnings("ignore",category=UserWarning,module="pyproj.*")

import pandas as pd
import numpy as np

from lib_utils_base import build_file_path, adjust_tags
from lib_io_nc import read_hydrograph_nc, get_data_from_nc
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# helper to organize data nc
def organize_data_nc(dset_hydro, section_id=None, section_fields=None):

    if section_fields is None:
        section_fields = [
            "time",
            "observed_discharge",
            "simulated_discharge"
        ]

    # check required fields
    missing_fields = [field for field in section_fields if field not in dset_hydro]
    if missing_fields:
        raise RuntimeError(
            f"Required field(s) {missing_fields} not found in NetCDF dataset. "
            f"Available fields: {list(dset_hydro.data_vars)} "
            f"(coordinates: {list(dset_hydro.coords)})"
        )

    time_field = section_fields[0]
    obs_field = section_fields[1]
    sim_field = section_fields[2]

    time_values = dset_hydro[time_field].values
    obs_values = dset_hydro[obs_field][:, section_id].values
    sim_values = dset_hydro[sim_field][:, section_id].values

    df_obs = pd.DataFrame({time_field: time_values,obs_field: obs_values})
    df_obs.loc[df_obs[obs_field] < 0, obs_field] = np.nan

    df_sim = pd.DataFrame({time_field: time_values,sim_field: sim_values})
    df_sim.loc[df_sim[sim_field] < 0, sim_field] = np.nan

    return pd.concat([df_obs, df_sim.drop(columns=[time_field])], axis=1)
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# helper to get data nc
def get_data_nc(settings, time, mandatory_fields=None):

    if mandatory_fields is None:
        mandatory_fields = {
            "data_from": "data_from",
            "sections_tag": "tag"
        }

    # create file path
    file_path = build_file_path(template=settings, time=time)
    # check file availability
    if not file_path.exists():
        raise FileNotFoundError(f"NetCDF file not found: {file_path}")

    # read data
    dset_hydro = read_hydrograph_nc(file_path)
    # filter data
    metadata_hydro = get_data_from_nc(dset_hydro, mandatory_fields)

    # convert data_from to filename ascii tag
    if "data_from" in metadata_hydro:
        metadata_hydro["sections_ascii"] = adjust_tags(metadata_hydro["data_from"])
    else:
        raise RuntimeError("Field 'data_from' not found in hydrograph and is mandatory")

    return dset_hydro, metadata_hydro
# ----------------------------------------------------------------------------------------------------------------------
