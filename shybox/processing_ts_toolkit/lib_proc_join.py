"""
Library Features:

Name:          lib_proc_join
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20260309'
Version:       '1.1.0'
"""
import numpy as np
# ----------------------------------------------------------------------------------------------------------------------
# libraries
import pandas as pd

from shybox.logging_toolkit.lib_logging_utils import with_logger
from shybox.orchestrator_toolkit.lib_orchestrator_utils_processes import as_process
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to join time series by registry
@as_process(
    input_type='pandas', output_type='pandas',
    lazy_undefined_args=True, lazy_undefined_value=None)
@with_logger(var_name='logger_stream')
def join_time_series_by_registry(
        df_sim, df_obs,
        sections_hmc: pd.DataFrame = None, sections_db: pd.DataFrame = None,
        name: str = 'time_series_hmc',
        fill_value: float = -9998.0, no_data_value: float = -9999.0,
        **kwargs):

    ## GENERIC CHECK
    var_time_name = 'time'

    # check dataframe data
    check_df_sim = _check_dataframe(df_sim, name="time-series datasets")
    if not check_df_sim:
        logger_stream.warning("Simulated dataframe is empty or defined by None. Return NoneType object.")
        return None
    # check dataframe obs
    check_df_obs = _check_dataframe(df_obs, name="observations datasets", allow_empty=True)
    if not check_df_obs:
        logger_stream.warning("Observed dataframe is empty or defined by None.")
        df_obs = None

    # check dataframe sections and database
    if sections_hmc is None or sections_hmc.empty:
        logger_stream.warning("'sections_hmc' should be a non-empty DataFrame.")
    if sections_db is None or sections_db.empty:
        logger_stream.warning("'sections_db' should be a non-empty DataFrame.")
    # check sections and database tags
    names_domains, names_db, names_missing = [], [], []
    if sections_hmc is not None and sections_db is not None:
        names_domains, names_db, names_missing = _join_tags(sections_hmc, sections_db)
    else:
        logger_stream.warning('No section or model tags provided to create the time series joined datasets')

    ## DATAFRAME PREPARATION
    # prepare time series dataframes: time column/index, coercion, sorting, renaming
    ts_sim = _prepare_timeseries(df_sim,names_domains,
                                 name="simulation datasets", time_name=var_time_name, allow_empty=False)
    if ts_sim is None:
        return None

    # DETECT DATA NAMES FROM SIM
    names_data = [c for c in ts_sim.columns if c != var_time_name]
    names_data = list(names_data)
    if not names_data:
        logger_stream.error("No data names found.")

    # DETECT REGISTRY NAMES
    names_db = [c for c in sections_db["tag"]]
    names_db = list(names_db)
    if not names_db:
        raise ValueError("No db names found.")

    # KEEP ONLY NAMES PRESENT IN REGISTRY, AND PRESERVE SIM ORDER
    names_in_db = [name for name in names_data if name in names_db]
    registry_db = sections_db.set_index("tag").loc[names_in_db].reset_index()

    # REMOVE MISSING NAMES FROM SIM
    missing_names = [name for name in names_data if name not in names_db]
    if missing_names:
        ts_sim = ts_sim.drop(columns=missing_names)
        names_data = [c for c in ts_sim.columns if c != var_time_name]
        names_data = list(names_data)
        logger_stream.warning(f"Removed sim columns not found in registry: {missing_names}")

    # COERCE SIM TO NUMERIC
    for c in names_data:
        ts_sim[c] = pd.to_numeric(ts_sim[c], errors="coerce")

    # observations are optional, so allow empty and handle later
    ts_obs = _prepare_timeseries(df_obs, names_domains,
                                 name="observations datasets", time_name=var_time_name, allow_empty=True)

    # PREPARE OBS USING THE SAME COLUMN SET / ORDER
    if ts_obs is not None and not ts_obs.empty:

        obs_data_cols = [c for c in ts_obs.columns if c != var_time_name]

        # keep only obs columns that are in sim canonical names
        obs_keep = [c for c in names_data if c in obs_data_cols]

        # create missing obs columns if needed
        obs_missing = [c for c in names_data if c not in obs_data_cols]
        for c in obs_missing:
            ts_obs[c] = pd.NA

        # reorder obs exactly like sim
        ts_obs = ts_obs[[var_time_name] + names_data]

        # coerce obs to numeric
        for c in names_data:
            ts_obs[c] = pd.to_numeric(ts_obs[c], errors="coerce")

    # JOIN DATASET
    ts_sim = ts_sim.set_index(var_time_name)
    if ts_obs is not None and not ts_obs.empty:
        ts_obs = ts_obs.set_index(var_time_name)
        # align obs to simulation timeline
        ts_obs = ts_obs.reindex(ts_sim.index)
    else:
        ts_obs = pd.DataFrame(no_data_value, index=ts_sim.index, columns=ts_sim.columns)

    # fill missing values in sim and obs with no_data_value (before joining, to avoid mixing with fill_value)
    ts_sim = ts_sim.fillna(no_data_value)
    ts_obs = ts_obs.fillna(no_data_value)

    # create combined dataframe with multi-level columns: obs and sim
    df_common = pd.concat({"obs": ts_obs, "sim": ts_sim}, axis=1)
    # optional
    df_common.index.name = var_time_name
    # fill missing values
    df_common = df_common.fillna(fill_value)

    # manage time series attributes
    registry_type = {}
    if 'type' in list(registry_db.attrs.keys()):
        registry_type = registry_db.attrs['type']
    # store time series attributes
    df_common.attrs['data'], df_common.attrs['type'] = registry_db, registry_type

    # manage time series name
    if name is not None:
        df_common.name = name

    return df_common

# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# helper to prepare time-series dataframe: time column/index, coercion, sorting, renaming
@with_logger(var_name='logger_stream')
def _prepare_timeseries(
        df, names_domains,
        name="time-series dataset", time_name="time", allow_empty=False):

    check_df = _check_dataframe(df, name=name, allow_empty=allow_empty)
    if not check_df:
        return None

    if df is None or df.empty:
        return None

    # determine time source
    if time_name in df.columns:
        ts = df.copy()
        ts[time_name] = pd.to_datetime(ts[time_name], errors="coerce", utc=True)
    elif isinstance(df.index, pd.DatetimeIndex):
        ts = df.copy().reset_index().rename(columns={"index": time_name})
        ts[time_name] = pd.to_datetime(ts[time_name], errors="coerce", utc=True)
    else:
        logger_stream.warning(
            f" ===> {name}: missing time information; "
            f"no '{time_name}' column and index is not DatetimeIndex."
        )
        return None

    # sanitize dataframe
    ts = (
        ts.dropna(subset=[time_name])
          .sort_values(time_name)
          .drop_duplicates(time_name, keep="last")
          .reset_index(drop=True)
    )

    # rename non-time columns
    ts_cols_no_time = ts.columns.drop(time_name)
    if len(ts_cols_no_time) == len(names_domains):
        rename_dict = dict(zip(ts_cols_no_time, names_domains))
        ts = ts.rename(columns=rename_dict)
    else:
        logger_stream.warning(
            f"{name}: column length mismatch between time series data "
            f"and section domain names."
        )

    return ts
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# helper to check dataframe(s)
@with_logger(var_name='logger_stream')
def _check_dataframe(data, name="data", allow_empty=False):

    if data is None:
        logger_stream.warning(f"{name} is None.")
        return False

    if not isinstance(data, pd.DataFrame):
        logger_stream.warning(f"{name} must be a pandas DataFrame, got {type(data)}.")
        return False

    if not allow_empty and data.empty:
        logger_stream.warning(f"{name} must be a non-empty DataFrame.")
        return False

    return True
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# helper to check and join tags from reference and target dataframes, reporting missing tags
@with_logger(var_name='logger_stream')
def _join_tags(df_ref: pd.DataFrame, df_target: pd.DataFrame, col: str = "tag"):

    # validate
    if col not in df_ref.columns:
        logger_stream.error(f"Column '{col}' not found in reference DataFrame.")
    if col not in df_target.columns:
        logger_stream.error(f"Column '{col}' not found in target DataFrame.")

    # extract unique tags (preserving order)
    tmp_tags = list(df_ref[col].values)
    ref_tags = [t for t in pd.unique(df_ref[col].dropna())]

    if ref_tags.__len__() != tmp_tags.__len__():

        tmp_unique = []
        for step_tags in tmp_tags:
            if step_tags not in ref_tags:
                logger_stream.warning(f"Tag '{step_tags}' appears multiple times in reference DataFrame.")

            if step_tags not in tmp_unique:
                tmp_unique.append(step_tags)
            else:
                logger_stream.error(f"Tag '{step_tags}' appears multiple times in reference DataFrame.")
                raise ValueError(f"Tag '{step_tags}' appears multiple times in reference DataFrame.")

    target_tags = [t for t in pd.unique(df_target[col].dropna())]

    # compute missing tags (in order of appearance in df_ref)
    target_set = set(target_tags)
    missing = [t for t in ref_tags if t not in target_set]

    # report
    if missing:
        logger_stream.warning("The following tags are missing in the target DataFrame:")
        for t in missing:
            print(f"  - {t}")
        logger_stream.warning(f"\nTotal missing: {len(missing)}")
    else:
        logger_stream.info("All tags from the reference DataFrame are present in the target DataFrame.")

    return ref_tags, target_tags, missing
# ----------------------------------------------------------------------------------------------------------------------
