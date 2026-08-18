"""
Library Features:

Name:          lib_io_ascii_points
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20260305'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import os
import pandas as pd

from shybox.logging_toolkit.lib_logging_utils import with_logger
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
