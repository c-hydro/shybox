"""
Library Features:

Name:          lib_utils_time
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20241202'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import re
import numpy as np
import pandas as pd
from typing import Union, Optional

from datetime import date, datetime

from shybox.generic_toolkit.lib_default_args import (logger_name, logger_arrow,
                                                     time_format_datasets as format_dset,
                                                     time_format_algorithm as format_alg)

# manage logger
try:
    from shybox.logging_toolkit.lib_logging_utils import with_logger
    logger_stream = logging.getLogger(logger_name) # double import for logging (to manage old loggers)
except Exception as e:
    logger_stream = logging.getLogger(logger_name)
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# convert time format (from string to timestamp and vice versa)
@with_logger(var_name="logger_stream")
def convert_time_format(
        time_in: Union[str, pd.Timestamp, None],
        time_conversion: str = 'str_to_stamp') -> Optional[Union[pd.Timestamp, str]]:

    if time_in is None:
        return None

    if time_conversion == 'str_to_stamp':
        return pd.Timestamp(time_in) if isinstance(time_in, str) else time_in

    elif time_conversion in ('stamp_to_str', 'str_to_str'):
        ts = pd.Timestamp(time_in)
        return ts.strftime(format_dset)

    else:
        logger_stream.error(f'Incorrect time conversion: {time_conversion}')
        raise ValueError(f"Unrecognized time_conversion: {time_conversion}")
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to normalize various time inputs to pandas.DatetimeIndex
@with_logger(var_name="logger_stream")
def normalize_to_datetime_index(
    obj,
    *,
    unit=None,               # epoch unit for numeric inputs: 's','ms','us','ns'
    tz=None,                 # e.g. 'Europe/Rome'
    assume_utc=False,        # treat naive times as UTC before converting to tz
    drop_duplicates=False,
    sort=True,
    allow_empty=False,
):
    """
    Convert many possible 'time' inputs into a pandas.DatetimeIndex.

    Supported inputs:
      - DatetimeIndex (returned as-is, optionally tz-adjusted)
      - Timestamp / datetime (wrapped into length-1 index)
      - str (parsed with pd.to_datetime)
      - number-like (epoch; uses `unit` if given, default 's')
      - sequence/array/Series/Index (vectorized parse)
    Returns DatetimeIndex or None (when empty and allow_empty=False).
    """

    if obj is None:
        return None

    if isinstance(obj, pd.DatetimeIndex):
        idx = obj
    elif isinstance(obj, (pd.Timestamp, datetime)):
        idx = pd.DatetimeIndex([pd.Timestamp(obj)])
    elif isinstance(obj, str):
        idx = pd.to_datetime([obj], errors="coerce", utc=False)
    elif isinstance(obj, (int, float, np.integer, np.floating)):
        idx = pd.to_datetime([obj], unit=unit or "s", errors="coerce", utc=False)
    elif isinstance(obj, (list, tuple, np.ndarray, pd.Series, pd.Index)):
        arr = np.array(obj, dtype=object)
        # If all are numeric/None -> treat as epoch
        if np.all([isinstance(x, (int, float, np.integer, np.floating)) or x is None for x in arr]):
            idx = pd.to_datetime(arr, unit=unit or "s", errors="coerce", utc=False)
        else:
            idx = pd.to_datetime(arr, errors="coerce", utc=False)
    else:
        # Generic parse attempt
        try:
            idx = pd.to_datetime(obj, errors="coerce", utc=False)
            if not isinstance(idx, pd.DatetimeIndex):
                idx = pd.DatetimeIndex([idx])
        except Exception as e:
            logger_stream.error(f"Failed to normalize datetime from object of type {type(obj)}: {e}")
            raise TypeError(f"Unsupported type for datetime normalization: {type(obj)}") from e

    # Timezone handling
    if tz is not None:
        if idx.tz is None:
            idx = (idx.tz_localize("UTC").tz_convert(tz)) if assume_utc else idx.tz_localize(tz)
        else:
            idx = idx.tz_convert(tz)

    # Clean up
    idx = idx[~idx.isna()]
    if drop_duplicates:
        idx = idx.drop_duplicates()
    if sort:
        idx = idx.sort_values()

    if len(idx) == 0:
        return idx if allow_empty else None
    return idx
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to convert time frequency from string to seconds
def convert_time_frequency(time_frequency: (str, int), frequency_conversion: str = 'str_to_int') -> (int, str):
    if frequency_conversion == 'str_to_int':
        if isinstance(time_frequency, str):
            time_delta = pd.Timedelta(1, unit=time_frequency.lower())
            time_frequency = int(time_delta.total_seconds())
        elif isinstance(time_frequency, (int, float)):
            time_frequency = int(time_frequency)
        else:
            logger_stream.error(logger_arrow.error + 'Time frequency type is not expected')
            raise NotImplementedError('Only string or integer/float type are allowed')
    elif frequency_conversion == 'int_to_str':
        if isinstance(time_frequency, (int, float)):
            obj_timedelta = pd.Timedelta(time_frequency, unit='s')
            time_frequency = obj_timedelta.resolution_string
        elif isinstance(time_frequency, str):
            if time_frequency.isdigit():
                obj_timedelta = pd.Timedelta(int(time_frequency), unit='s')
                time_frequency = obj_timedelta.resolution_string
            elif time_frequency.isalpha():
                time_frequency = time_frequency.lower()
            else:
                logger_stream.error(logger_arrow.error + 'Time frequency type is not expected')
                raise NotImplementedError('Only string or integer/float type are allowed')
        else:
            logger_stream.error(logger_arrow.error + 'Time frequency type is not expected')
            raise NotImplementedError('Only string or integer/float type are allowed')
    else:
        logger_stream.error(logger_arrow.error + 'Time frequency conversion not recognized')
        raise NotImplementedError('Time frequency conversion not recognized')
    return time_frequency
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to check string has the date format
def is_date(date_obj: (str, pd.Timestamp), date_format: str = '%Y%m%d%H%M') -> bool:
    """
    Return whether the string can be interpreted as a date.
    :param date_obj: str, string to check for date
    :param date_format: str, format of the date
    """
    try:
        if isinstance(date_obj, str):
            pd.to_datetime(date_obj, format=date_format, errors='raise')
        elif isinstance(date_obj, pd.Timestamp):
            pd.to_datetime(date_obj.strftime(date_format), format=date_format, errors='raise')
        else:
            raise ValueError('Date type is not recognized')
        return True
    except ValueError:
        return False
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to select time run
def select_time_run(time_run_args: pd.Timestamp = None, time_run_file: pd.Timestamp = None,
                    time_rounding: str = 'H', **kwargs) -> (pd.Timestamp, None):

    # info algorithm (start)
    logger_stream.info(logger_arrow.info(tag='time_fx_lev1') + 'Select time run ... ')

    # case 1: time information defined by "time_now" argument
    if (time_run_args is not None) or (time_run_file is not None):

        # check time args and time file
        if time_run_args is not None:
            time_run = time_run_args
            logger_stream.info(
                logger_arrow.info(tag='time_fx_lev2') + 'Time "' + time_run.strftime(format_alg) + '" set by argument')
        elif (time_run_args is None) and (time_run_file is not None):
            time_run = time_run_file
            logger_stream.info(
                logger_arrow.info(tag='time_fx_lev2') + 'Time "' + time_run.strftime(format_alg) + '" set by user')
        elif (time_run_args is None) and (time_run_file is None):
            time_run = date.today()
            logger_stream.info(
                logger_arrow.info(tag='time_fx_lev2') + 'Time "' + time_run.strftime(format_alg) + '" set by system')
        else:
            logger_stream.info(logger_arrow.info(tag='time_fx_lev1') + 'Select time run ... FAILED')
            logger_stream.error(logger_arrow.error + 'Argument "time_run" is not correctly set')
            raise IOError('Time type or format is wrong')

        time_df = pd.DataFrame([{'time_now': pd.Timestamp(time_run)}])
        time_df['time_round'] = time_df['time_now'].dt.floor(time_rounding.lower())

        time_run = pd.Timestamp(time_df['time_round'].values[0])

        # info algorithm (end)
        logger_stream.info(logger_arrow.info(tag='time_fx_lev1') + 'Select time run ... DONE')

    else:
        # info algorithm (end)
        time_run = None
        logger_stream.info(logger_arrow.info(tag='time_fx_lev1') +
                           'Select time run ... SKIPPED. Variable is defined by NoneType')

    return time_run
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to select time restart
def select_time_restart(time_run: pd.Timestamp, time_shift: int = 1, time_frequency: str = 'h') -> pd.Timestamp:

    # info algorithm (start)
    logger_stream.info(logger_arrow.info(tag='time_fx_lev1') + 'Select time restart ... ')
    # compute time restart
    time_restart = time_run - pd.Timedelta(time_shift, unit=time_frequency.lower())
    # info algorithm (start)
    logger_stream.info(logger_arrow.info(tag='time_fx_lev1') + 'Select time restart ... DONE')

    return time_restart
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to select time range
def select_time_range(time_start: (str, pd.Timestamp) = None, time_end: (str, pd.Timestamp) = None,
                      time_period: int = 1,
                      time_frequency: str = 'h', time_rounding: str = 'h',
                      ensure_range: bool = False, flat_if_single: bool = False) -> (pd.date_range, None):

    # strip quotes and convert to pd.Timestamp
    if isinstance(time_start, str):
        time_start = time_start.strip("\"")
        time_start = pd.Timestamp(time_start)
    if isinstance(time_end, str):
        time_end = time_end.strip("\"")
        time_end = pd.Timestamp(time_end)

    # check time start and time end if are both defined
    if (time_start is not None) and (time_end is not None):
        if time_start > time_end:
            logger_stream.error(logger_arrow.error + 'Time start is after time end')
            raise ValueError('Time start is after time end')

    time_rounding, time_frequency = time_rounding.lower(), time_frequency.lower()
    time_frequency = normalize_frequency(time_frequency)

    if (time_start is not None) and (time_end is not None):

        # If only a unit like 'D' is given, prepend '1'
        if len(time_frequency) == 1 or time_frequency.isalpha():
            ref_frequency = f"1{time_frequency}"
        else:
            ref_frequency = time_frequency

        ref_delta = pd.Timedelta(ref_frequency)
        time_delta = pd.Timedelta(time_end - time_start)

        if time_delta < ref_delta:
            if not ensure_range:
                time_start = time_end
            else:
                time_start, time_end = ensure_time_range(
                    time_start, time_end, ref_delta, when_short="forward")

    if (time_start is not None) and (time_end is not None):

        time_start, time_end = time_start.floor(time_rounding), time_end.floor(time_rounding)
        time_range = pd.date_range(start=time_start, end=time_end, freq=time_frequency)

    elif (time_start is None) and (time_end is not None):

        if time_period is None:
            raise ValueError('Time period is not defined')

        time_end = time_end.floor(time_rounding.lower())
        time_range = pd.date_range(end=time_end, periods=time_period, freq=time_frequency.lower())

    elif (time_start is not None) and (time_end is None):

        if time_period is None:
            raise ValueError('Time period is not defined')

        time_start = time_start.floor(time_rounding.lower())
        time_range = pd.date_range(start=time_start, periods=time_period, freq=time_frequency.lower())

    else:
        raise ValueError('Time range is not correctly defined')

    # flat time range (if list of 1-element)
    if flat_if_single and len(time_range) == 1:
        time_range = time_range[0]

    return time_range
# ----------------------------------------------------------------------------------------------------------------------

def ensure_time_range(
    time_start,
    time_end,
    ref_frequency,
    when_short="forward",      # how to extend if too short
    when_long="keep",           # how to shrink if too long
):
    """
    Ensure the [time_start, time_end] range is consistent with ref_frequency.

    Parameters
    ----------
    time_start, time_end : Timestamp
        Current time range.
    ref_frequency : str or Timedelta-like
        Minimum / target window, e.g. "1d", "6h", "30min".
    when_short : {"backward", "forward", "both", "keep"}
        What to do if the window is SHORTER than ref_delta:
        - "backward": extend backwards (move start earlier)
        - "forward" : extend forwards (move end later)
        - "both"    : extend symmetrically around the current center
        - "keep"    : do nothing
    when_long : {"keep", "shrink-backward", "shrink-forward", "shrink-both"}
        What to do if the window is LONGER than ref_delta:
        - "keep"           : leave as is
        - "shrink-backward": shrink by moving start forward
        - "shrink-forward" : shrink by moving end backward
        - "shrink-both"    : shrink symmetrically around the center
    """
    ref_delta = pd.Timedelta(ref_frequency)
    time_delta = time_end - time_start

    # --- Case 1: window is shorter than ref_delta → maybe extend it
    if time_delta < ref_delta:
        if when_short == "backward":
            time_start = time_end - ref_delta
        elif when_short == "forward":
            time_end = time_start + ref_delta
        elif when_short == "both":
            center = time_start + time_delta / 2
            half = ref_delta / 2
            time_start = center - half
            time_end = center + half
        elif when_short == "keep":
            pass
        else:
            raise ValueError("when_short must be 'backward', 'forward', 'both', or 'keep'")

    # --- Case 2: window is longer than ref_delta → maybe shrink it
    elif time_delta > ref_delta:
        if when_long == "shrink-backward":
            # keep end fixed, move start forward
            time_start = time_end - ref_delta
        elif when_long == "shrink-forward":
            # keep start fixed, move end backward
            time_end = time_start + ref_delta
        elif when_long == "shrink-both":
            center = time_start + time_delta / 2
            half = ref_delta / 2
            time_start = center - half
            time_end = center + half
        elif when_long == "keep":
            pass
        else:
            raise ValueError("when_long must be 'keep', 'shrink-backward', 'shrink-forward', or 'shrink-both'")

    # if time_delta == ref_delta → nothing to do

    return time_start, time_end


# ----------------------------------------------------------------------------------------------------------------------
# method to select time format
def select_time_format(time_range, time_format: str = '%Y-%m-%d %H:%M',
                       flat_if_single: bool = False) -> Union[str, list]:

    # Normalize input to DatetimeIndex
    if isinstance(time_range, pd.Timestamp):
        formatted = time_range.strftime(time_format)
        return formatted  # return a single string

    # Assume it's a DatetimeIndex or similar iterable of timestamps
    time_range_formatted = time_range.strftime(time_format).tolist()

    # flat time range (if list of 1-element)
    if flat_if_single and len(time_range_formatted) == 1:
        time_range_formatted = time_range_formatted[0]

    return time_range_formatted
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to normalize pandas frequency aliases
@with_logger(var_name="logger_stream")
def normalize_frequency(freq: str) -> str:
    """
    Normalize pandas frequency aliases.
    Supports multipliers (e.g. '2h', '3d') and raises an error for unsupported values.
    """
    # remove blanks from frequency string
    freq = freq.strip()

    # Split optional multiplier and base code
    match = re.fullmatch(r"(\d*)\s*([A-Za-z]+)", freq)
    if not match:
        logger_stream.error(f"Invalid frequency format: '{freq}'")
        raise ValueError(f"Invalid frequency format: '{freq}'")

    multiplier, base = match.groups()
    base_low = base.lower()

    # Uppercase codes (Pandas prefers uppercase for months/years)
    uppercase_mapping = {
        "m": "M",
        "me": "ME",
        "ms": "MS",
        "y": "A",
        "a": "A",
        "ys": "AS",
        "as": "AS",
    }

    # Lowercase codes (Pandas prefers lowercase to avoid warnings)
    lowercase_mapping = {
        "h": "h",
        "hour": "h",
        "s": "s",
        "sec": "s",
        "t": "min",     # 'T' deprecated → use 'min'
        "min": "min",
        "d": "d",
        "w": "w",
    }

    if base_low in uppercase_mapping:
        norm = uppercase_mapping[base_low]
    elif base_low in lowercase_mapping:
        norm = lowercase_mapping[base_low]
    else:
        logger_stream.error(f"Unsupported frequency code: '{base}'")
        raise ValueError(f"Unsupported frequency code: '{base}'")

    # Add multiplier if present
    return (multiplier + norm) if multiplier else norm
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to get period bounds
@with_logger(var_name="logger_stream")
def get_time_bounds(ts, freq: str, time_format: str = None):
    """
    Return (period_start, period_end) for the given date and freq.

    ts : pd.Timestamp | datetime | str
        If str, it will be converted automatically.
    freq : str
        Normalized frequency (D, M, MS, ME, A, AS)
    time_format : str | None
        Optional explicit string format (e.g. '%Y%m%d').
    """

    # 1️⃣ --- Convert ts to Timestamp ---
    if not isinstance(ts, pd.Timestamp):
        try:
            if isinstance(ts, str):
                if time_format is None:
                    ts = pd.to_datetime(ts)
                else:
                    ts = pd.to_datetime(ts, format=time_format)
            else:
                ts = pd.to_datetime(ts)
        except Exception as e:
            if logger_stream:
                logger_stream.error(f"Invalid date '{ts}': {e}")
            raise ValueError(f"Invalid date '{ts}': {e}")

    # 2️⃣ --- Compute bounds based on freq ---
    if freq.lower() == "d":
        start = ts.normalize()
        end = start + pd.Timedelta(days=1)

    elif freq in ("M", "MS", "ME"):
        start = ts.replace(day=1)
        end = start + pd.offsets.MonthBegin(1)

    elif freq in ("A", "AS"):
        start = datetime(ts.year, 1, 1)
        end = datetime(ts.year + 1, 1, 1)

    else:
        msg = f"Unsupported period frequency: '{freq}'"
        if logger_stream:
            logger_stream.error(msg)
        raise ValueError(msg)

    return start, end
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to compute period length in chosen resolution
@with_logger(var_name="logger_stream")
def get_time_length(start_date, period_freq: str, resolution: str, type: (int, float) = None) -> float:
    """
    Compute the length of a period (month/year/day) using a chosen resolution,
    even if `start_date` is NOT the first day of the period.
    """

    period_freq = normalize_frequency(period_freq)
    resolution = normalize_frequency(resolution)

    # Make start_date always a Timestamp
    ts = pd.to_datetime(start_date)

    # Find actual period boundaries
    period_start, period_end = get_time_bounds(ts, period_freq)
    delta = pd.Timedelta(period_end - period_start)

    # resolution conversion
    conv = {
        "s": delta.total_seconds(),
        "min": delta.total_seconds() / 60,
        "h": delta.total_seconds() / 3600,
        "d": delta.total_seconds() / 86400,
    }

    if resolution not in conv:
        logger_stream.error(f"Unsupported resolution: '{resolution}'")
        raise ValueError(f"Unsupported resolution: '{resolution}'")

    period = conv[resolution]

    if type is not None:
        if type == float:
            period = float(period)
        elif type == int:
            period = int(period)
        else:
            period = float(period)
    else:
        period = float(period)

    return period
# ----------------------------------------------------------------------------------------------------------------------
