"""
Library Features:

Name:          lib_proc_join
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20260108'
Version:       '1.0.0'
"""
# ----------------------------------------------------------------------------------------------------------------------
# libraries
import pandas as pd

from shybox.logging_toolkit.lib_logging_utils import with_logger
from shybox.orchestrator_toolkit.lib_orchestrator_utils import as_process
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to join time series by registry
@as_process(input_type='pandas', output_type='pandas')
@with_logger(var_name='logger_stream')
def join_time_series_by_registry(
        data, ref, sections_hmc: pd.DataFrame = None, sections_db: pd.DataFrame = None,
        fill_value: float = -9999.0, no_data_value: float = -9999.0,
        **kwargs):

    ## GENERIC CHECK
    var_time_name = 'time'
    if data is None or data.empty:
        logger_stream.error("'data' must be a non-empty DataFrame.")
    if sections_hmc is None or sections_hmc.empty:
        logger_stream.warning("'sections_hmc' should be a non-empty DataFrame.")
    if sections_db is None or sections_db.empty:
        logger_stream.warning("'sections_db' should be a non-empty DataFrame.")

    names_domains, names_db, names_missing = [], [], []
    if sections_hmc is not None and sections_db is not None:
        names_domains, names_db, names_missing = check_section_tags(sections_hmc, sections_db)
    else:
        logger_stream.warning('No section or model tags provided to create the time series joined datasets')

    ## TIME CHECK
    # determine time source (column or index) ---
    if var_time_name in data.columns:
        ts = data.copy()
        ts[var_time_name] = pd.to_datetime(ts[var_time_name], errors="coerce", utc=True)
    elif isinstance(data.index, pd.DatetimeIndex):
        ts = data.copy().reset_index().rename(columns={"index": var_time_name})
        ts[var_time_name] = pd.to_datetime(ts[var_time_name], errors="coerce", utc=True)
    else:
        logger_stream.error(
            f" ===> Missing time information: no '{var_time_name}' column and index is not DatetimeIndex.")

    ## DATAFRAME CREATION
    # sanitize dset dataframe
    ts[var_time_name] = pd.to_datetime(ts[var_time_name], errors="coerce", utc=True)
    ts = (
        ts.dropna(subset=[var_time_name])
        .sort_values(var_time_name)
        .drop_duplicates(var_time_name, keep="last")
        .reset_index(drop=True)
    )

    ## DATAFRAME CHECKS
    # exclude the time column
    ts_cols_no_time = ts.columns.drop('time')
    # check length equality
    if len(ts_cols_no_time) == len(names_domains):
        # rename columns based on sections_hmc
        rename_dict = {old_name: new_name for old_name, new_name in zip(ts_cols_no_time, names_domains)}
        ts = ts.rename(columns=rename_dict)
    else:
        logger_stream.warning("Column length mismatch between time series data and section domain names.")

    # detect name data
    names_data = [c for c in ts.columns if c != var_time_name]
    names_data = list(names_data)
    if not names_data:
        logger_stream.error("No data names found.")

    # detect name registry
    names_db = [c for c in sections_db['tag']]
    names_db = list(names_db)
    if not names_db:
        raise ValueError("No db names found.")

    # filter and reorder registry_df based on names_data
    names_in_db = [name for name in names_data if name in names_db]
    registry_db = sections_db.set_index('tag').loc[names_in_db].reset_index()

    # DATAFRAME ORGANIZATION
    # optional: log or check for missing names
    missing_names = [name for name in names_data if name not in names_db]
    if missing_names:

        # remove missing names from dataframe
        ts = ts.drop(columns=missing_names)
        # update names_data
        names_data = [c for c in ts.columns if c != var_time_name]
        names_data = list(names_data)

        logger_stream.warning(f"Removed columns not found in registry: {missing_names}")

    # coerce numeric data
    for c in names_data:
        ts[c] = pd.to_numeric(ts[c], errors="coerce")

    ts.attrs = registry_db
    ts.name = 'time_series_hmc'

    return ts

# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to join time series by database registry
@with_logger(var_name='logger_stream')
def check_section_tags(df_ref: pd.DataFrame, df_target: pd.DataFrame, col: str = "tag"):
    """
    Check if all tags from df_ref[col] exist in df_target[col].
    If any are missing, print them and return useful lists while preserving order.

    Parameters
    ----------
    df_ref : pd.DataFrame
        Reference DataFrame (expected tags).
    df_target : pd.DataFrame
        Target DataFrame (tags to check against).
    col : str, default="tag"
        Name of the column containing tags to compare.

    Returns
    -------
    tuple of (list, list, list)
        ref_tags : list of unique tags in df_ref[col] (order preserved)
        target_tags : list of unique tags in df_target[col] (order preserved)
        missing : list of tags in ref_tags but not in target_tags (order preserved)
    """
    # --- Validate ---
    if col not in df_ref.columns:
        logger_stream.error(f"Column '{col}' not found in reference DataFrame.")
    if col not in df_target.columns:
        logger_stream.error(f"Column '{col}' not found in target DataFrame.")

    # --- Extract unique tags (preserving order) ---
    ref_tags = [t for t in pd.unique(df_ref[col].dropna())]
    target_tags = [t for t in pd.unique(df_target[col].dropna())]

    # --- Compute missing tags (in order of appearance in df_ref) ---
    target_set = set(target_tags)
    missing = [t for t in ref_tags if t not in target_set]

    # --- Report ---
    if missing:
        logger_stream.warning("The following tags are missing in the target DataFrame:")
        for t in missing:
            print(f"  - {t}")
        logger_stream.warning(f"\nTotal missing: {len(missing)}")
    else:
        logger_stream.info("All tags from the reference DataFrame are present in the target DataFrame.")

    return ref_tags, target_tags, missing
# ----------------------------------------------------------------------------------------------------------------------
