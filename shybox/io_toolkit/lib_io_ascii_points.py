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
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to read points 1d
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
    df.columns = ["points", "values"]

    # convert value column to numeric (safe conversion)
    df["values"] = pd.to_numeric(df["values"], errors="coerce")

    # optional: set section as index
    df = df.set_index("points")

    return df
# ----------------------------------------------------------------------------------------------------------------------
