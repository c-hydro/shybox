"""
Library Features:

Name:           lib_io_json
Author(s):      Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:           '20260626'
Version:        '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import os
import json
import numpy as np
import pandas as pd
# ----------------------------------------------------------------------------------------------------------------------

#-----------------------------------------------------------------------------------------------------------------------
# helper to read settings
def read_settings(settings_file):
    with open(settings_file, "r") as fp:
        return json.load(fp)
#-----------------------------------------------------------------------------------------------------------------------

#-----------------------------------------------------------------------------------------------------------------------
# method for json safety
def _to_json_safe(value):
    if isinstance(value, (np.integer,)):
        return int(value)

    if isinstance(value, (np.floating,)):
        if np.isnan(value):
            return None
        return float(value)

    if isinstance(value, (np.ndarray,)):
        return [_to_json_safe(v) for v in value.tolist()]

    if isinstance(value, (pd.Timestamp,)):
        return value.isoformat()

    if pd.isna(value):
        return None

    return value

# helper to write hydrograph in json format
def write_hydrograph_json(
        json_name,
        df_hydro_common, metadata_hydro_common,
        time_reference, run_reference
):

    folder_name = os.path.dirname(json_name)
    if folder_name:
        os.makedirs(folder_name, exist_ok=True)

    df_json = df_hydro_common.copy()
    df_json["time"] = pd.to_datetime(df_json["time"]).dt.strftime("%Y-%m-%d %H:%M:%S")

    time_start = pd.to_datetime(df_hydro_common["time"]).min()
    time_end = pd.to_datetime(df_hydro_common["time"]).max()

    json_data = {
        "metadata": {
            key: _to_json_safe(value)
            for key, value in metadata_hydro_common.items()
        },
        "time_reference": pd.to_datetime(time_reference).strftime("%Y-%m-%d %H:%M:%S"),
        "time_start": time_start.strftime("%Y-%m-%d %H:%M:%S"),
        "time_end": time_end.strftime("%Y-%m-%d %H:%M:%S"),
        "run_reference": run_reference,
        "data": df_json.replace({np.nan: None}).to_dict(orient="list")
    }

    for scenario_col in ["scenario_min", "scenario_max", "scenario_mean"]:

        if scenario_col in df_hydro_common.columns:
            json_data[scenario_col] = [
                _to_json_safe(v) for v in df_hydro_common[scenario_col].values
            ]

    with open(json_name, "w") as fp:
        json.dump(json_data, fp, indent=4)
# ----------------------------------------------------------------------------------------------------------------------
