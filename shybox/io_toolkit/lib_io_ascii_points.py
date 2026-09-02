"""
Library Features:

Name:          lib_io_ascii_points
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20260902'
Version:       '1.1.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
from __future__ import annotations

import os
import re
import numpy as np
import pandas as pd

from pathlib import Path
from typing import Dict, List, Union, Iterable, Hashable

from shybox.logging_toolkit.lib_logging_utils import with_logger
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# LUT
LUT_REGISTRY_DEFAULT = {
    "id": "ID", "code": "code", "name": "name",
    "longitude": "longitude", "latitude": "latitude", "altitude": "quota",
    "is_ott": "isOTT", "is_riscaldato": "isRISCALDATO",
    "tag": "tag"
}
# TYPES
TYPE_REGISTRY_DEFAULT = {
    'id': int, 'code': int, 'name': str,
    'longitude': np.float64, 'latitude': np.float64, 'altitude': float,
    'is_ott': int, 'is_riscaldato': int,
    'tag': str,
}
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# helper to read sections database (csv format)
@with_logger(var_name='logger_stream')
def _parse_tag_from_data_from(text: str) -> str:
    if text is None or pd.isna(text):
        return ""
    s = str(text).strip().strip('"').strip("'")
    parts = re.split(r"\s*,\s*", s, maxsplit=1)
    if len(parts) == 2:
        left = parts[0].strip().lower()
        right = parts[1].strip().lower().replace(" ", "_")
        return f"{left}:{right}"
    return s.lower()

# helper to cast columns
@with_logger(var_name='logger_stream')
def _select_warn_and_cast(
    df: pd.DataFrame,
    type_map: dict,
    keep_only_typed_cols: bool = True,
    store_info: bool = True,
) -> pd.DataFrame:

    if type_map is None:
        return df

    wanted = list(type_map.keys())
    missing = [c for c in wanted if c not in df.columns]
    existing = [c for c in wanted if c in df.columns]

    if missing:
        logger_stream.warning(f"Missing expected columns (skipping): {missing}")

    # store metadata in attributes
    info = {}
    if store_info:
        info["fields_requested"] = wanted
        info["fields_selected"] = existing
        info["fields_missing"] = missing
        info["fields_types"] = {k: str(v) for k, v in type_map.items()}

    # select fields
    if keep_only_typed_cols:
        df = df[existing].copy()
    else:
        df = df.copy()

    # cast fields
    for col in existing:
        t = type_map[col]

        if t is int:
            s = pd.to_numeric(df[col], errors="coerce")
            df[col] = s.astype("Int64")

        elif t in (float, np.float64, np.float32):
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")

        elif t is str:
            df[col] = df[col].astype("string")
        else:
            try:
                df[col] = df[col].astype(t)
            except Exception as e:
                logger_stream.warning(f"Could not cast column '{col}' to {t}: {e}")

    return df, info

# method to read points registry
@with_logger(var_name='logger_stream')
def read_points_registry(
    file_path: str, name: str = 'registry',
    lut_map: dict = None, lut_type: dict = None,
    col_reference: str = "tag", col_filter: str = None, filter_value: str = None,
    sep: str = ",", encoding: str = "ISO-8859-1", case: bool = False, regex: bool = False,
    out_col: str = "tag",final_cols: list[str] = None, out_first: bool = True,) -> Hashable:

    # check file availability
    if not os.path.exists(file_path):
        logger_stream.error(f"File registry not found: {file_path}")

    # check lut map and type
    if lut_map is None:
        lut_map = LUT_REGISTRY_DEFAULT
        logger_stream.warning("Variable lut_map is not defined; using default LUT_REGISTRY_DEFAULT")
    if lut_type is None:
        lut_type = TYPE_REGISTRY_DEFAULT
        logger_stream.warning("Variable lut_type is not defined; using default TYPE_REGISTRY_DEFAULT")
    else:
        map_type = {"str": str, "int": int, "float": float, "np.float64": np.float64,}
        lut_type = {key: map_type[value] for key, value in lut_type.items()}

    # read the csv file (db sections)
    df = pd.read_csv(file_path, sep=sep, encoding=encoding)

    # check reference column (according to which the 'tag' is created)
    if col_reference not in df.columns:
        logger_stream.error(f"Required column '{col_reference}' not found in CSV. Exit.")
        raise RuntimeError('Column reference is mandatory. Check the registry file and change the column name.')

    # apply data types
    df_out = df.copy()
    if col_filter and filter_value is not None:
        if col_filter not in df_out.columns:
            logger_stream.error(f"Column '{col_filter}' not found for filtering.")
        df_out = df_out[df_out[col_filter].astype(str).str.contains(str(filter_value), case=case, regex=regex)]
        logger_stream.info(f"Filtered by {col_filter} = {filter_value} → {len(df_out)} rows")

    # cast column used for tagging datasets
    df_out[out_col] = df_out[col_reference].map(_parse_tag_from_data_from)

    # rename variables, excluding out_col
    rename_map = {src: dst for dst, src in lut_map.items() if src in df_out.columns and dst != out_col}
    df_out = df_out.rename(columns=rename_map)

    if final_cols is not None:
        existing = [c for c in final_cols if c in df_out.columns]
        others = [c for c in df_out.columns if c not in existing]
        df_out = df_out[existing + others]

    # move 'out_col' to first position (if requested)
    if out_first and out_col in df_out.columns:
        cols = [out_col] + [c for c in df_out.columns if c != out_col]
        df_out = df_out[cols]

    # select + warn + cast
    df_out, df_info = _select_warn_and_cast(
        df_out,
        type_map=lut_type,  # or `type_map` if you add it as an arg
        keep_only_typed_cols=True,  # keep only these fields
        store_info=True
    )

    # organize type info in attributes
    info_type = {}
    if df_info:
        if 'fields_types' in list(df_info.keys()):
            type_info = df_info['fields_types']
            # assign info to dataframe series
            for type_key, type_value in type_info.items():
                if type_key in df_out.columns:
                    info_type[type_key] =  type_value
    df_out.attrs['type'] = info_type

    # add name to the dataframe (to recognize its type)
    if name is not None:
        df_out.attrs["name"] = name

    return df_out

# ----------------------------------------------------------------------------------------------------------------------



# ----------------------------------------------------------------------------------------------------------------------
# method to read points 2d
@with_logger(var_name='logger_stream')
def read_points_2d(
        file_path: str, header: bool = True, delimiter: str =',',
        time_name: str ='time', time_format: str = '%Y-%m-%d %H:%M',):

    # check file availability
    if not os.path.exists(file_path):
        logger_stream.warning(f"File {file_path} does not exists.")
        return None

    # consider header presence
    header_row = 0 if header else None

    # read file
    df = pd.read_csv(file_path, sep=delimiter,header=header_row, na_values=[-9998, -9999, -9998.0, -9999.0, "NaN"])
    # check dataframe
    if df.empty:
        logger_stream.warning(f"Points 2D file is empty: {file_path}")
        return df

    # normalize column names
    df.columns = [str(col).strip() for col in df.columns]
    # search time column case-insensitively
    column_map = {str(col).strip().lower(): col for col in df.columns}

    # get time column
    time_key = time_name.strip().lower()
    if time_key not in column_map:
        logger_stream.error(f"Time column '{time_name}' not found. Available columns: {list(df.columns)}")
        raise ValueError(f"Check the source data {file_path}")

    # get actual time column name
    time_column = column_map[time_key]
    # parse time
    time_raw = df[time_column].copy()
    if time_format is not None:
        df[time_column] = pd.to_datetime(time_raw, format=time_format, errors='coerce')
    else:
        df[time_column] = pd.to_datetime(time_raw,errors='coerce')

    # check invalid time values
    time_invalid = df[time_column].isna()
    if time_invalid.any():
        # info of the invalid values
        logger_stream.warning(f"Found {time_invalid.sum()} invalid time values using format '{time_format}' in file '{file_path}'")
        # show the invalid values
        invalid_values = time_raw[time_invalid].unique()
        # warn for the invalid values
        logger_stream.warning(f"Invalid time values: {invalid_values.tolist()}")

    # convert all point columns to numeric
    point_columns = [col for col in df.columns if col != time_column]
    # apply numeric types
    df[point_columns] = df[point_columns].apply(pd.to_numeric, errors='coerce')

    # set time as index
    df = df.set_index(time_column)
    # standardize index name
    df.index.name = time_name

    # info
    logger_stream.info(f"Points 2D data found: {df.shape[0]} time steps, {df.shape[1]} points")

    return df
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to read points 1d
@with_logger(var_name='logger_stream')
def read_points_1d(file_path, header=True, delimiter=';'):

    """
    Load section values from a CSV-like file.

    Parameters
    ----------
    file_path : str
        Path to the input file.
    delimiter : str, optional
        Column delimiter (default ';').
    header : bool, optional
        Whether the file contains a header (default True).

    Returns
    -------
    pandas.DataFrame
        DataFrame indexed by section name with a column 'value'.
    """

    if not os.path.exists(file_path):
        return None

    # consider header presence
    header_row = 0 if header else None
    # read the file, treating -9998, -9999, and "NaN" as missing values
    df = pd.read_csv(file_path, sep=delimiter, header=header_row, na_values=[-9998, -9999, "NaN"])

    # normalize column names for searching
    column_map = {str(col).strip().lower(): col for col in df.columns}
    if len(df.columns) == 2:
        # standard two-column format
        df.columns = ["points", "values"]
        # info format
        logger_stream.info(f"Points 1D data type found: 2-column format")

    else:
        # info format
        logger_stream.info("Points 1D data type found: multi-column format")
        logger_stream.info(f"Available columns: {list(df.columns)}")

        # multi-column format: extract tag and discharge
        if "tag" not in column_map:
            logger_stream.error(f"Column 'tag' not found. Available columns: {list(df.columns)}")
            raise ValueError(f"Check the source data {file_path}")

        if "discharge" not in column_map:
            logger_stream.error(f"Column 'discharge' not found. Available columns: {list(df.columns)}")
            raise ValueError(f"Check the source data {file_path}")

        df = df[[column_map["tag"], column_map["discharge"]]].copy()
        df.columns = ["points", "values"]

    # convert value column to numeric (safe conversion)
    df["values"] = pd.to_numeric(df["values"], errors="coerce")

    # optional: set points as index
    df = df.set_index("points")

    return df
# ----------------------------------------------------------------------------------------------------------------------
