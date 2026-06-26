"""
Library Features:

Name:           lib_io_nc
Author(s):      Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:           '20260626'
Version:        '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import glob
from pathlib import Path
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# helper to build file path
def build_file_path(template, time, **kwargs):
    values = {
        "time_folder": time.strftime(template["folder_time_format"]),
        "time_file": time.strftime(template["file_time_format"]),
        **kwargs
    }

    folder = Path(template["folder"].format(**values))
    filename = template["filename"].format(**values)

    return folder / filename
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# helper to find files
def find_files(path_pattern):
    return [Path(f) for f in sorted(glob.glob(str(path_pattern)))]
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# helper to adjust path (from data_from to string in filenames
def adjust_tags(data_from):
    """
    Convert NetCDF data_from values to TXT filename format.
    Accepts either a string or a list of strings.
    """

    if isinstance(data_from, (list, tuple)):
        return [adjust_tags(item) for item in data_from]

    parts = [part.strip() for part in str(data_from).split(",")]
    return "_".join(parts)
# ----------------------------------------------------------------------------------------------------------------------
