"""
Library Features:

Name:          lib_proc_join
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20260630'
Version:       '1.3.0'
"""
# ----------------------------------------------------------------------------------------------------------------------
# libraries
import numpy as np
import pandas as pd

from shybox.logging_toolkit.lib_logging_utils import with_logger
from shybox.orchestrator_toolkit.lib_orchestrator_utils_processes import as_process
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to join time series by registry
@as_process(input_type='pandas', output_type='pandas',
            lazy_undefined_obj=True, lazy_undefined_args=True, lazy_undefined_value=None)
@with_logger(var_name='logger_stream')
def join_points_to_time_series(
        data, ref, time,
        name='points_time_series',
        fill_missing_step: (float, int) = -9998.0, fill_no_data_step: (float, int) = -9999.0,
        fill_missing_tag: (str, float, int) = -9999.0,
        time_fmt: str = "%Y%m%d%H%M", time_reference: pd.Timestamp = None,
        fixed_width: bool = True, decimals: int = 2, col_width: int = 12, **kwargs):
    """
    data: iterable of per-step objects where:
          - d is None OR
          - d has d.index (point names) and d.values (numeric values)
            e.g. a pandas.Series

    ref : DataFrame with columns: catchment_name, section_name, extra
    time: iterable of timestamps (same length as data)

    Returns: DataFrame with columns:
        time, <tag1>, <tag2>, ... (ordered exactly as ref)
    """

    # make tag from ref
    def make_tag(row):
        parts = [str(row["catchment_name"]).strip(),
                 str(row["section_name"]).strip()]
        extra = row.get("extra", np.nan)
        if pd.notna(extra):
            extra_s = str(extra).strip()
            if extra_s and extra_s.lower() != "nan":
                parts.append(extra_s)
        return " ".join(parts)

    # check data availability
    if len(data) == 0:
        logger_stream.warning("Empty data for the whole time series. Return None")
        return None
    else:
        nd_df = None
        for template in data:
            if template is not None:
                nd_df = pd.DataFrame(fill_missing_step,
                    index=template.index, columns=template.columns)
                break

    # if all data steps are None, template is not available due to the data are not available
    if nd_df is None:
        logger_stream.warning("All data steps are None. Return None")
        return None

    # recover time_reference from kwargs if not explicitly provided
    if time_reference is None:

        for key in (
                "time_reference",
                "time_ref",
                "time_run",
                "time_now",
                "time",
                "reference_time",
                "run_time",
                "date_reference",
        ):
            if key in kwargs and kwargs[key] is not None:
                time_reference = kwargs[key]
                logger_stream.info(
                    f"Using '{key}' from kwargs as time_reference: {time_reference}"
                )
                break

    # tags ordered as ref
    tags = [make_tag(r) for _, r in ref.iterrows()]

    # iterate over data and time together
    rows = []
    for d, t in zip(data, time):

        # check if d is None, if so use template with fill_missing_step
        if d is None:
            logger_stream.warning(f"Adding missing step for time: {t}")
            d = nd_df

        # format time string
        ts = pd.to_datetime(t).strftime(time_fmt)

        # missing whole timestep
        if d is None:
            rows.append([ts] + [fill_missing_step] * len(tags))
            continue

        # must have index and values
        if not hasattr(d, "index") or not hasattr(d, "values"):
            logger_stream.error(f"Per-step data must have .index and .values (e.g. pd.Series). Got {type(d)}")
            raise TypeError(f"Per-step data must have .index and .values (e.g. pd.Series). Got {type(d)}")

        # build lookup: point_name -> value
        # normalize spaces to avoid mismatches
        step_map = {}
        for p, v in zip(d.index, d.values):
            p_str = " ".join(str(p).strip().split())
            step_map[p_str] = v  # if duplicates, last wins

        # iterate over tags
        vals = []
        for tag in tags:

            # organize key by normalizing spaces to avoid mismatches
            key = " ".join(str(tag).strip().split())

            # lookup value for this tag; if missing, use fill_missing_tag
            if key in step_map:

                # get value and check if it's valid (not None or NaN)
                v = step_map[key]

                # check value format and handle missing/invalid values
                if v is None or (isinstance(v, float) and np.isnan(v)):
                    vals.append(fill_missing_tag)
                else:
                    # if value is an array, try to extract a single numeric value
                    if isinstance(v, np.ndarray):
                        if v.size == 1:
                            v = v.item()
                        else:
                            logger_stream.warning(f"Array with shape {v.shape}, using first element")
                            v = v.flat[0]
                    # ensure numeric value
                    vals.append(float(v))
            else:
                logger_stream.warning(f"Missing point '{tag}' at time '{ts}'. Using {fill_missing_tag}.")
                vals.append(fill_missing_tag)

        # define row for this time step
        rows.append([ts] + vals)

    # create dataframe
    df = pd.DataFrame(rows, columns=["time"] + tags)

    # set all values after time_reference to no_data
    if time_reference is not None:

        time_reference = pd.to_datetime(time_reference)

        # remove timezone if present
        if time_reference.tzinfo is not None:
            time_reference = time_reference.tz_localize(None)

        time_check = pd.to_datetime(df["time"], format=time_fmt, errors="coerce")

        # remove timezone from parsed dataframe time if present
        if getattr(time_check.dt, "tz", None) is not None:
            time_check = time_check.dt.tz_localize(None)

        mask_after_reference = time_check > time_reference

        if mask_after_reference.any():
            logger_stream.warning(
                f"Setting values after time_reference '{time_reference}' "
                f"to no_data '{fill_no_data_step}'"
            )

            df.loc[mask_after_reference, df.columns[1:]] = fill_no_data_step

    # fill missing numeric values first
    df = df.fillna(fill_no_data_step)

    # fixed width formatting (still dataframe, ready for to_csv elsewhere) --> this is string formatting
    if fixed_width:
        for c in df.columns[1:]:
            df[c] = df[c].map(lambda x: f"{float(x):{col_width}.{decimals}f}")

    # assign name to dataframe for better logging and debugging
    df.name = name

    return df
# ----------------------------------------------------------------------------------------------------------------------
