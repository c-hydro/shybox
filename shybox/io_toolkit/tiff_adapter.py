"""
Library Features:

Name:          tiff_adapter
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20260824'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
from functools import wraps
from pathlib import Path
import numpy as np

from shybox.io_toolkit.lib_io_utils import substitute_string_by_tags, make_file_path
from shybox.io_toolkit.lib_io_decoretors import obj_to_array
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# adapter of watersheds
def adapt_watershed_to_tiff(obj, obj_name=None, **kwargs):

    # get path and ancillary notes
    path = kwargs.get("file_path", None)
    tag, workflow = obj.get('tag', None), obj.get('workflow', None)

    # get data and attributes
    mask = np.asarray(obj['mask'])
    data = np.asarray(obj['catchment'])
    attrs = obj.get('section', {})

    # expand attrs
    attrs['tag'], attrs['workflow'] = tag, workflow

    # get ref, units and mapping
    ref, units, mapping = kwargs.get('ref', None), kwargs.get('units', 'NA'), kwargs.get('map', {})
    var_name = kwargs.get('variable_name', 'NA')

    # define transform and crs
    transform, crs = None, None
    if ref is not None:
        grid = ref['grid']
        transform = grid.viewfinder.affine
        crs = grid.viewfinder.crs

    # update path using tags and values
    var_name = var_name.replace(':', '.')
    path = substitute_string_by_tags(path, {'section_name': var_name })
    path = Path(path)
    # create path if needed
    _, _, _ = make_file_path(path, create_folder=True)

    # collect fx args
    fx_args = {
        "path": path,
        "data": data, "attrs": attrs,
        "transform": transform, "crs": crs,
        "name": var_name,
    }

    return fx_args
# ----------------------------------------------------------------------------------------------------------------------
