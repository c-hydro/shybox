"""
Library Features:

Name:          lib_proc_interpolate
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20260925'
Version:       '1.1.0'
"""
# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import numpy as np
import xarray as xr

from pyresample.geometry import GridDefinition, SwathDefinition
from pyresample.kd_tree import resample_nearest, resample_gauss, resample_custom
from repurpose.resample import resample_to_grid

from shybox.io_toolkit.lib_io_utils import create_darray
from shybox.orchestrator_toolkit.lib_orchestrator_utils_processes import as_process

# common decorators
from shybox import with_logger, deprecated
# ----------------------------------------------------------------------------------------------------------------------


def interpolate_data_hs(points_hs, grid_sca, grid_hs):

    print()