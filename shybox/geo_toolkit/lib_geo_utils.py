"""
Library Features:

Name:          lib_geo_utils
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20260925'
Version:       '1.1.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import numpy as np

from shybox.logging_toolkit.lib_logging_utils import with_logger
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# helper to merge data by watermark
@with_logger(var_name='logger_stream')
def map_1d_to_ref_indices(ref_1d, sub_1d):
    """
    ref_1d must be monotonic. Returns indices into ref_1d for each sub_1d (nearest).
    """
    ref_1d = np.asarray(ref_1d)
    sub_1d = np.asarray(sub_1d)

    asc = ref_1d[0] < ref_1d[-1]
    if not asc:
        ref_1d = ref_1d[::-1]

    pos = np.searchsorted(ref_1d, sub_1d)
    pos = np.clip(pos, 1, ref_1d.size - 1)

    left = ref_1d[pos - 1]
    right = ref_1d[pos]
    choose_right = (np.abs(right - sub_1d) < np.abs(sub_1d - left))
    idx = pos.copy()
    idx[~choose_right] = pos[~choose_right] - 1

    if not asc:
        idx = (ref_1d.size - 1) - idx
    return idx
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# helper to check the regularity of a 1D grid (monotonic, non-constant, regular spacing within tol)
def is_regular_grid(arr, tol=1e-6):

    if arr.ndim != 1 or arr.size < 2:
        return False
    # must not be constant
    if np.all(arr == arr[0]):
        return False

    # check spacing
    diffs = np.diff(arr)

    return np.all(np.abs(diffs - diffs[0]) < tol)
# ----------------------------------------------------------------------------------------------------------------------
