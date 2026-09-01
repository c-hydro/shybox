"""
Library Features:

Name:          lib_proc_join
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20260309'
Version:       '1.1.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import pandas as pd

from shybox.logging_toolkit.lib_logging_utils import with_logger
from shybox.orchestrator_toolkit.lib_orchestrator_utils_processes import as_process, with_dict_input
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to join time series by registry
@as_process(
    input_type='pandas', output_type='pandas',
    lazy_undefined_args=True, lazy_undefined_value=None)
@with_logger(var_name='logger_stream')
def join_time_series_by_registry(
        datasets: dict[str, pd.DataFrame | None],
        sections_hmc: pd.DataFrame = None,
        sections_db: pd.DataFrame = None,
        name: str = "time_series_hmc",
        mapping: dict = None,
        fill_value: float = -9998.0,
        no_data_value: float = -9999.0,
        **kwargs):

    var_time_name = "time"

    if not isinstance(datasets, dict) or not datasets:
        logger_stream.warning("'datasets' should be a non-empty dictionary. Return NoneType object.")
        return None

    if sections_hmc is None or sections_hmc.empty:
        logger_stream.warning("'sections_hmc' should be a non-empty DataFrame.")

    if sections_db is None or sections_db.empty:
        logger_stream.warning("'sections_db' should be a non-empty DataFrame.")
        return None

    names_domains, names_db, names_missing = [], [], []

    if sections_hmc is not None and sections_db is not None:
        names_domains, names_db, names_missing = _join_tags(sections_hmc, sections_db,)
    else:
        logger_stream.warning("No section or model tags provided to create the time series joined datasets.")

    names_db = list(sections_db["tag"])
    if not names_db: raise ValueError("No db names found.")

    datasets_prepared = {}
    datasets_missing = []
    names_data_common = None

    # PREPARE AVAILABLE DATASETS
    for tmp_key, dataset_df in datasets.items():

        if mapping is not None:
            if tmp_key in list(mapping.keys()):
                dataset_key = mapping[tmp_key]
            else:
                dataset_key = tmp_key
        else:
            dataset_key = tmp_key

        dataset_key = str(dataset_key)

        # keep track of None datasets
        if dataset_df is None:
            logger_stream.warning(
                f"Dataset '{dataset_key}' is defined by None; it will be filled with no_data_value={no_data_value}."
            )
            datasets_missing.append(dataset_key)
            continue

        check_df = _check_dataframe(dataset_df, name=f"dataset '{dataset_key}'",allow_empty=True,)
        if not check_df:
            logger_stream.warning(
                f"Dataset '{dataset_key}' is empty or invalid; "
                f"it will be filled with no_data_value={no_data_value}."
            )
            datasets_missing.append(dataset_key)
            continue

        ts_data = _prepare_timeseries(
            dataset_df, names_domains,
            name=f"dataset '{dataset_key}'",time_name=var_time_name, allow_empty=True,
        )

        if ts_data is None or ts_data.empty:
            logger_stream.warning(
                f"Dataset '{dataset_key}' could not be prepared; it will be filled with no_data_value={no_data_value}."
            )
            datasets_missing.append(dataset_key)
            continue

        # normalize time
        ts_data[var_time_name] = pd.to_datetime(ts_data[var_time_name], errors="coerce",).dt.floor("h")

        names_data = [c for c in ts_data.columns if c != var_time_name]
        if not names_data:
            logger_stream.warning(
                f"Dataset '{dataset_key}' has no data columns; it will be filled with no_data_value={no_data_value}."
            )
            datasets_missing.append(dataset_key)
            continue

        # remove names not present in registry
        missing_names = [name_tmp for name_tmp in names_data if name_tmp not in names_db]

        if missing_names:
            ts_data = ts_data.drop(columns=missing_names, errors="ignore",)

            logger_stream.warning(
                f"Dataset '{dataset_key}': removed columns not found in registry: {missing_names}"
            )

        names_data = [c for c in ts_data.columns if c != var_time_name]
        if not names_data:
            logger_stream.warning(
                f"Dataset '{dataset_key}' has no valid registry columns; it will be filled with no_data_value={no_data_value}."
            )
            datasets_missing.append(dataset_key)
            continue

        # first valid dataset defines canonical columns
        if names_data_common is None:
            names_data_common = list(names_data)

        # create missing canonical columns
        missing_common = [ c for c in names_data_common if c not in names_data]
        for c in missing_common:
            ts_data[c] = pd.NA

        extra_common = [c for c in names_data if c not in names_data_common]
        if extra_common:
            logger_stream.warning(
                f"Dataset '{dataset_key}': columns not present in the reference dataset will be ignored: {extra_common}"
            )

        ts_data = ts_data[[var_time_name] + names_data_common]
        for c in names_data_common:
            ts_data[c] = pd.to_numeric(ts_data[c], errors="coerce",)
        ts_data = ts_data.set_index(var_time_name)

        datasets_prepared[dataset_key] = ts_data


    # NEED AT LEAST ONE VALID DATASET TO DEFINE N x T
    if not datasets_prepared:
        logger_stream.warning(
            "No valid datasets are available to define the common time axis and section dimensions. Return NoneType object."
        )
        return None

    # DEFINE COMMON TIME INDEX
    reference_key = next(iter(datasets_prepared))
    reference_index = datasets_prepared[reference_key].index

    # align valid datasets
    for dataset_key, ts_data in datasets_prepared.items():

        if not ts_data.index.equals(reference_index):
            logger_stream.warning(
                f"Dataset '{dataset_key}' has a different time axis; aligning to reference dataset '{reference_key}'."
            )

        datasets_prepared[dataset_key] = (ts_data.reindex(reference_index).fillna(no_data_value))

    # CREATE N x T DATAFRAMES FOR MISSING DATASETS
    for dataset_key in datasets_missing:

        ts_data_missing = pd.DataFrame(no_data_value, index=reference_index, columns=names_data_common, dtype=float,)
        ts_data_missing.index.name = var_time_name

        datasets_prepared[dataset_key] = ts_data_missing

        logger_stream.warning(
            f"Dataset '{dataset_key}' created as no-data array "
            f"with shape {ts_data_missing.shape}."
        )

    # RESTORE ORIGINAL DATASET ORDER
    original_keys = [str(key) for key in datasets.keys()]

    ordered_keys = (
            [key for key in original_keys if key in datasets_prepared]
            +
            [key for key in datasets_prepared if key not in original_keys]
    )

    datasets_prepared = {
        key: datasets_prepared[key]
        for key in ordered_keys
    }

    # JOIN DATASETS
    df_common = pd.concat(datasets_prepared, axis=1)
    df_common.index.name = var_time_name
    df_common = df_common.fillna(fill_value)

    # REGISTRY
    names_in_db = [name_tmp for name_tmp in names_data_common if name_tmp in names_db]

    registry_db = (sections_db.set_index("tag").loc[names_in_db].reset_index())
    registry_type = {}
    if "type" in registry_db.attrs:
        registry_type = registry_db.attrs["type"]

    df_common.attrs["data"] = registry_db
    df_common.attrs["type"] = registry_type
    df_common.attrs["datasets"] = list(datasets_prepared.keys())

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
