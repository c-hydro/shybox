"""
Library Features:

Name:          lib_proc_join
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20260305'
Version:       '1.0.0'
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
@as_process(input_type='pandas', output_type='pandas')
@with_logger(var_name='logger_stream')
def join_points_to_time_series(data, ref, time, name='points_time_series',
                 fill_missing_step=-9998.0, fill_missing_tag=-9999.0,
                 time_fmt="%Y%m%d%H%M",
                 fixed_width=True,decimals=2, col_width=12, **kwargs):
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

    # tags ordered as ref
    tags = [make_tag(r) for _, r in ref.iterrows()]

    # iterate over data and time together
    rows = []
    for d, t in zip(data, time):
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

        vals = []
        for tag in tags:
            key = " ".join(str(tag).strip().split())

            if key in step_map:
                v = step_map[key]
                if v is None or (isinstance(v, float) and np.isnan(v)):
                    vals.append(fill_missing_tag)
                else:

                    if isinstance(v, np.ndarray):
                        if v.size == 1:
                            v = v.item()
                        else:
                            logger_stream.warning(f"Array with shape {v.shape}, using first element")
                            v = v.flat[0]

                    vals.append(float(v))
                    print(type(v), v.shape)
            else:
                logger_stream.warning(f"Missing point '{tag}' at time '{ts}'. Using {fill_missing_tag}.")
                vals.append(fill_missing_tag)

        rows.append([ts] + vals)

    df = pd.DataFrame(rows, columns=["time"] + tags)

    # fixed width formatting (still DF, ready for to_csv elsewhere)
    if fixed_width:
        for c in df.columns[1:]:
            df[c] = df[c].map(lambda x: f"{float(x):{col_width}.{decimals}f}")

    # assign name to dataframe for better logging and debugging
    df.name = name

    return df
# ----------------------------------------------------------------------------------------------------------------------
