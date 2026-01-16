# ----------------------------------------------------------------------------------------------------------------------
# libraries
from __future__ import annotations

import os
import re
import numpy as np
import pandas as pd

from pathlib import Path
from typing import Dict, List, Union, Iterable

from shybox.logging_toolkit.lib_logging_utils import with_logger
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# LUT: destination (internal) → source (original CSV header)
LUT_DB_DEFAULT = {
    "id": "ID",
    "section_name": "SEZIONE",
    "station_name": "NOME IDROMETRO",
    "catchment_name": "NOME_BACIN",
    "data_from": "DATI_DA",
    "domain_name": "NOME_DOMIN",
    "municipality": "COMUNE",
    "province": "PROV",
    "region": "REGIONE",
    "basin": "BACINO",
    "longitude": "LON",
    "latitude": "LAT",
    "catchment_area_km2": "AREA",
    "correlation_time_hr": "CORR_TIME",
    "curve_number": "CN",
    "threshold_level_1": "THR1",
    "threshold_level_2": "THR2",
    "threshold_level_3": "THR3",
    "alert_zone": "ALERTZONE",
    "discharge_yellow": "Q_ALLARME",
    "discharge_orange": "Q_ARANCIONE",
    "discharge_red": "Q_ALLERTA",
    "threshold_source": "Fonte Q_soglia",
    "discharge_estimation": "StimaQoss",
    "quality_index_1_5": "QUAL1_5",
    "is_calibrated": "CALIBRATO",
    "error_relative_peakflow": "Err_Rel_Qp",
    "nash_sutcliffe": "NashSut",
    "source_sdd": "FonteSdD"
}
# columns for domain registry
LUT_DOMAIN_DEFAULT = ['X', 'Y', 'catchment_name', 'section_name', 'extra']

# data types for sections database
TYPE_DB_DEFAULT = {
    'tag': str,
    'id': int,
    'section_name': str,
    'station_name': str,
    'catchment_name': str,
    'domain_name': str,
    'municipality': str,
    'province': str,
    'region': str,
    'basin': int,
    'longitude': np.float64,
    'latitude': np.float64,
    'catchment_area_km2': float,
    'correlation_time_hr': float,
    'curve_number': float,
    'threshold_level_1': float,
    'threshold_level_2': float,
    'threshold_level_3': float,
    'alert_zone': int,
    'is_calibrated': str
}
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to read sections database (csv format)
def _parse_tag_from_data_from(text: str) -> str:
    """
    Parse 'Catchment, Section' → 'catchment:section_with_underscores'
    """
    if text is None or pd.isna(text):
        return ""
    s = str(text).strip().strip('"').strip("'")
    parts = re.split(r"\s*,\s*", s, maxsplit=1)
    if len(parts) == 2:
        left = parts[0].strip().lower()
        right = parts[1].strip().lower().replace(" ", "_")
        return f"{left}:{right}"
    return s.lower()

# method to read sections database (csv format from fp chain)
@with_logger(var_name='logger_stream')
def read_sections_db(
    file_path: str,
    lut: dict = None,
    col_datafrom: str = "DATI_DA",
    col_filter: str = None,
    filter_value: str = None,
    sep: str = ";",
    encoding: str = "ISO-8859-1",
    case: bool = False,
    regex: bool = False,
    out_col: str = "tag",
    final_cols: list[str] = None,
    out_first: bool = True,
) -> pd.DataFrame:

    # check file availability
    if not os.path.exists(file_path):
        logger_stream.error(f"File sections database not found: {file_path}")

    if lut is None:
        lut = LUT_DB_DEFAULT
        logger_stream.warning("Using default LUT_DB_DEFAULT")

    # read the csv file (db sections)
    df = pd.read_csv(file_path, sep=sep, encoding=encoding)

    # check reference column (according to which the 'tag' is created)
    if col_datafrom not in df.columns:
        logger_stream.error(f"Required column '{col_datafrom}' not found in CSV.")

    # apply data types
    df_out = df.copy()
    if col_filter and filter_value is not None:
        if col_filter not in df_out.columns:
            logger_stream.error(f"Column '{col_filter}' not found for filtering.")
        df_out = df_out[df_out[col_filter].astype(str).str.contains(str(filter_value), case=case, regex=regex)]
        logger_stream.info(f"Filtered by {col_filter} = {filter_value} → {len(df_out)} rows")

    df_out[out_col] = df_out[col_datafrom].map(_parse_tag_from_data_from)

    rename_map = {src: dst for dst, src in lut.items() if src in df_out.columns}
    df_out = df_out.rename(columns=rename_map)

    if final_cols is not None:
        existing = [c for c in final_cols if c in df_out.columns]
        others = [c for c in df_out.columns if c not in existing]
        df_out = df_out[existing + others]

    # move 'out_col' to first position (if requested)
    if out_first and out_col in df_out.columns:
        cols = [out_col] + [c for c in df_out.columns if c != out_col]
        df_out = df_out[cols]

    # add name to the dataframe (to recognize its type)
    df_out.name = "sections_db"

    return df_out

# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to read sections data hmc (txt format)
@with_logger(var_name='logger_stream')
def read_sections_data(
    path: str,
    var_name: str = "discharge",
    base_name: str = "sec",
    datetime_format: str = "%Y%m%d%H%M",
    tz: str = None,
    column_names: list = None,
) -> pd.DataFrame:

    # check file availability
    if not os.path.exists(path):
        logger_stream.error(f"File sections data not found: {path}")

    # Load the raw file (no header)
    raw = pd.read_csv(path, sep=r"\s+", header=None, engine="python")
    if raw.shape[1] < 2:
        logger_stream.error("Expected at least 2 columns: datetime + one or more section columns.")

    # Parse datetime
    idx = pd.to_datetime(raw.iloc[:, 0].astype(str), format=datetime_format, errors="raise")
    if tz is not None:
        idx = idx.dt.tz_localize(tz)

    # Slice section data
    data = raw.iloc[:, 1:].copy()

    # Name columns
    n_sections = data.shape[1]
    if column_names is not None:
        if len(column_names) != n_sections:
            logger_stream.error(
                f"column_names length ({len(column_names)}) must equal number of sections ({n_sections})."
            )
        data.columns = column_names
    else:
        data.columns = [f"{base_name}_{i:02d}" for i in range(1, n_sections + 1)]

    # Convert all to numeric
    data = data.apply(pd.to_numeric, errors="coerce")
    data.name = var_name

    # Assign DateTimeIndex
    data.index = idx
    data.index.name = "time"

    return data

# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# methods to read sections hmc (txt format)
@with_logger(var_name='logger_stream')
def read_sections_registry(
    filepath: Union[str, os.PathLike],
    colnames: Union[List[str], Dict[int, str], None] = None,
    *,
    encoding: str = "utf-8",
    errors: str = "strict",
    comment_prefixes: Iterable[str] = ("#", ";"),
    skip_blank: bool = True,
    strict: bool = False,
    out_col: str = "tag",
    out_first: bool = False,
) -> pd.DataFrame:
    """
    Read a info_section-style TXT with flexible column naming and create a
    'catchment:section' tag using columns 2, 3, and 4 (where 3 and 4 are merged).
    """

    # resolve column names to an exact list of 5
    if isinstance(colnames, dict):
        cols = [colnames.get(i, LUT_DOMAIN_DEFAULT[i]) for i in range(5)]
    elif isinstance(colnames, list):
        cols = (colnames + LUT_DOMAIN_DEFAULT[len(colnames):])[:5]
    elif colnames is None:
        cols = LUT_DOMAIN_DEFAULT[:]
    else:
        logger_stream.error("Sections hmc columns names must be list, dict, or None")

    # check file availability
    path = Path(filepath)
    if not path.exists():
        logger_stream.error(f"File sections hmc not found: {path}")

    # open file and parse lines
    records: List[dict] = []
    with path.open("r", encoding=encoding, errors=errors) as f:
        for lineno, raw in enumerate(f, start=1):
            line = raw.strip()
            if skip_blank and not line:
                continue
            if comment_prefixes and any(line.lstrip().startswith(p) for p in comment_prefixes):
                continue

            # split into up to 5 parts; 5th is the full remainder (with spaces)
            parts = line.split(maxsplit=4)
            if len(parts) < 4:
                if strict:
                    logger_stream.error(f"Malformed line {lineno}: expected >=4 fields -> {raw!r}")
                continue

            try:
                id1 = int(parts[0])
                id2 = int(parts[1])
            except ValueError as e:
                if strict:
                    logger_stream.error(f"Line {lineno}: IDs must be integers -> {raw!r}")
                continue

            name1 = parts[2]
            name2 = parts[3]
            extra = parts[4] if len(parts) >= 5 else None

            records.append({
                cols[0]: id1,
                cols[1]: id2,
                cols[2]: name1,
                cols[3]: name2,
                cols[4]: extra,
            })

    # create dataframe from records (list of dicts)
    df = pd.DataFrame.from_records(records, columns=cols)

    # create tag: col2 : (col3 + " " + col4) ---
    # ensure col4 exists and use NA-safe string ops
    if cols[4] not in df.columns:
        df[cols[4]] = pd.NA

    # normalize and clean strings
    c2 = df[cols[2]].astype("string").str.strip()          # catchment
    c3 = df[cols[3]].astype("string").str.strip()          # section (part 1)
    c4 = df[cols[4]].astype("string").str.strip()          # section (part 2 / extra)

    # merge section = c3 if c4 empty/NA, else "c3 c4"
    section = c3.where(c4.isna() | (c4 == ""), c3 + " " + c4)

    # clean and format
    catchment_clean = (
        c2.str.replace(r"\s+", " ", regex=True)
          .fillna("_")
          .str.lower()
          .str.strip()
    )

    section_clean = (
        section.fillna("_")
               .replace(r"^\s*$", "_", regex=True)  # empty → "_"
               .str.replace(r"\s+", "_", regex=True)  # spaces → underscores
               .str.lower()
               .str.strip()
    )

    # build tag (form "catchment:section")
    df[out_col] = catchment_clean + ":" + section_clean

    # move tag to the first column (if requested)
    if out_first and out_col in df.columns:
        df = df[[out_col] + [c for c in df.columns if c != out_col]]
    # add name to the dataframe (to recognize its type)
    df.name = "sections_hmc"

    return df

# ----------------------------------------------------------------------------------------------------------------------
