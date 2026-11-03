"""
Library Features:

Name:          lib_proc_join
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20261008'
Version:       '1.4.0'
"""
# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import numpy as np
import pandas as pd

# common decorators
from shybox import with_logger, deprecated
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to join time series by registry
@with_logger(var_name='logger_stream')
def join_points_to_time_series(
        data: list, ref: pd.DataFrame, time: list,
        name: str = "points_time_series",
        fill_missing_step: (float, int) = -9998.0, fill_no_data_step: (float, int) = -9999.0,
        fill_missing_tag: (float, int) = -9999.0,
        time_fmt: str = "%Y%m%d%H%M", time_reference: pd.Timestamp = None,
        fixed_width: bool = True, decimals: int = 2, col_width: int = 12,
        logger_stream: logging.Logger = None,
        **kwargs):

    # define logger stream (if needed)
    logger_stream = (logger_stream or globals().get('logger_stream') or logging.getLogger(__name__))

    # check input lengths
    if len(data) != len(time):
        raise ValueError(f"'data' and 'time' must have the same length. Got data={len(data)}, time={len(time)}")

    # check data availability
    if len(data) == 0:
        logger_stream.warning("Empty data for the whole time series. Return None")
        return None

    # find first available dataframe
    template = None
    for step_data in data:
        if step_data is not None:
            if not isinstance(step_data, pd.DataFrame):
                raise TypeError(f"Per-step data must be a pd.DataFrame or None. Got {type(step_data)}")
            template = step_data
            break

    # all timesteps are None
    if template is None:
        logger_stream.warning("All data steps are None. Return None")
        return None

    # recover time_reference from kwargs
    if time_reference is None:
        for key in (
                "time_reference",
                "time_ref", "time_run", "time_now",
                "time", "reference_time", "run_time", "date_reference"):
            if key in kwargs and kwargs[key] is not None:
                time_reference = kwargs[key]
                logger_stream.info(f"Using '{key}' from kwargs as time_reference: {time_reference}")
                break

    # define tags using reference dataframe
    if "tag" in ref.columns:
        tags = ref["tag"].tolist()
        logger_stream.info("Tags found in reference data")
    else:
        tags = [_make_tag(row) for _, row in ref.iterrows()]
        ref["tag"] = tags
        logger_stream.info("Tags not found: created using the helper function _make_tag()")

    # normalize reference tags once
    # tags_normalized = {_normalize_tag(tag): tag for tag in tags}

    # iterate over timesteps
    rows = []
    for step_data, step_time in zip(data, time):
        # format timestep
        ts = pd.to_datetime(step_time).strftime(time_fmt)
        # completely missing timestep
        if step_data is None:
            logger_stream.warning(f"Adding missing step for time: {step_time}")
            rows.append([ts] + [fill_missing_step] * len(tags))
            continue

        # validate timestep data
        if not isinstance(step_data, pd.DataFrame):
            logger_stream.error(f"Per-step data must be a pd.DataFrame or None. Got {type(step_data)}")
            raise TypeError(f"Per-step data must be a pd.DataFrame or None. Got {type(step_data)}")

        # empty dataframe
        if step_data.empty:
            logger_stream.warning(f"Empty dataframe at time '{ts}'. Using {fill_missing_step}.")
            rows.append([ts] + [fill_missing_step] * len(tags))
            continue

        # normalize dataframe index
        normalized_index = {_normalize_tag(index_value): index_value for index_value in step_data.index}

        # extract values according to reference tag order
        vals = []
        missing_tags, invalid_tags, duplicate_tags = [], [], []
        for tag in tags:

            # normalize tag
            key = _normalize_tag(tag)

            # check point availability
            if key not in normalized_index:
                missing_tags.append(str(tag))
                vals.append(fill_missing_tag)
                continue

            # recover point value
            original_index = normalized_index[key]
            value = step_data.loc[original_index]

            # handle duplicate indexes
            if isinstance(value, pd.DataFrame):
                duplicate_tags.append(str(tag))
                value = value.iloc[0]

            # handle pandas Series
            if isinstance(value, pd.Series):
                value = value.iloc[0] if not value.empty else None

            # handle numpy arrays
            if isinstance(value, np.ndarray):
                if value.size > 1:
                    invalid_tags.append(str(tag))
                value = value.flat[0] if value.size else None

            # validate and convert value
            try:
                if value is None or pd.isna(value):
                    raise ValueError("Missing or NaN value")

                value = float(value)

            except (TypeError, ValueError):
                invalid_tags.append(str(tag))
                value = fill_missing_tag

            # append value
            vals.append(value)

        # compact warnings
        warnings = {
            "missing": missing_tags,
            "invalid": list(dict.fromkeys(invalid_tags)),
            "duplicate": duplicate_tags,
        }

        # create message(s)
        warnings_msg = [f"{key} ({len(values)}): {', '.join(values)}" for key, values in warnings.items() if values]
        # print message(s)
        if warnings_msg:
            logger_stream.warning(f"Time '{ts}' | " + " | ".join(warnings_msg))

        # append timestep
        rows.append([ts] + vals)

    # create output dataframe
    df = pd.DataFrame(rows, columns=["time"] + tags)

    # set future values to no-data
    if time_reference is not None:

        # define time reference (if not none)
        time_reference = pd.to_datetime(time_reference)

        # remove timezone
        if time_reference.tzinfo is not None:
            time_reference = time_reference.tz_localize(None)

        time_check = pd.to_datetime(df["time"], format=time_fmt, errors="coerce")

        # remove timezone if needed
        if getattr(time_check.dt, "tz", None) is not None:
            time_check = time_check.dt.tz_localize(None)

        mask_after_reference = (time_check > time_reference)

        if mask_after_reference.any():

            logger_stream.warning(
                f"Setting values after time_reference '{time_reference}' to no_data {fill_no_data_step}'")

            df.loc[mask_after_reference, df.columns[1:]] = fill_no_data_step

    # fill remaining NaN values
    df.iloc[:, 1:] = (df.iloc[:, 1:].fillna(fill_no_data_step))

    # fixed-width formatting
    if fixed_width:
        for column in df.columns[1:]:
            df[column] = df[column].map(lambda value: f"{float(value):{col_width}.{decimals}f}")

    # dataframe name
    df.name = name

    return df

# helper: create tag from reference row
def _make_tag(row):

    parts = [str(row["catchment_name"]).strip(), str(row["section_name"]).strip()]
    extra = row.get("extra", np.nan)
    if pd.notna(extra):
        extra_s = str(extra).strip()
        if extra_s and extra_s.lower() != "nan":
            parts.append(extra_s)
    return " ".join(parts)

# helper: normalize tag/key
def _normalize_tag(value):
    return " ".join(str(value).strip().split()).lower()
# ----------------------------------------------------------------------------------------------------------------------
