"""
Library Features:

Name:          tiff_adapter
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20260930'
Version:       '1.1.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
from pathlib import Path
import numpy as np
import xarray as xr

from shybox.io_toolkit.lib_io_utils import substitute_string_by_tags, make_file_path

# common decoretor
from shybox import with_logger
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# adapter of watersheds
@with_logger(var_name='logger_stream')
def adapt_watershed_to_tiff(obj, **kwargs):

    # get path and ancillary notes
    path = kwargs.get("file_path", None)
    tag, workflow = obj.get("tag"), obj.get("workflow")
    transform, crs = obj.get("transform"), obj.get("crs")

    # get data and attributes
    mask = np.asarray(obj["mask"])
    data = np.asarray(obj["catchment"])
    attrs = obj.get("section", {}).copy()

    # expand attrs
    attrs.update({"tag": tag, "workflow": workflow,"transform": transform,"crs": crs})

    # get ref, units and mapping
    ref, units, mapping = kwargs.get('ref', None), kwargs.get('units', 'NA'), kwargs.get('map', {})
    var_name = kwargs.get('variable_name', 'NA')

    # define transform and crs
    if transform is None or crs is None:
        if 'mask' in list(obj.keys()):
            grid = obj['mask']
            transform = grid.viewfinder.affine
            crs = grid.viewfinder.crs
    # check transform and crs
    if transform is None or crs is None:
        logger_stream.error('Geo transform and crs must be defined to correctly defined the data')
        raise RuntimeError('Check the settings and the script to get the mandatory variables')

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

# ----------------------------------------------------------------------------------------------------------------------
# adapter of grid 2d
def adapt_grid2d_to_tiff(obj, **kwargs):

    if not isinstance(obj, xr.DataArray):
        raise TypeError("obj must be an xarray.DataArray")

    # get values and attrs
    values, attrs = obj.values, obj.attrs

    # get path and ancillary notes
    path = kwargs.get("file_path", None)

    # get ref, units and mapping
    ref, units, mapping = kwargs.get('ref', None), kwargs.get('units', 'NA'), kwargs.get('map', {})
    var_name = kwargs.get('variable_name', 'NA')

    # define transform and crs
    transform, crs = None, None
    if ref is not None:
        grid = ref['terrain']
        # get transform
        try:
            transform = grid.rio.transform()
        except Exception:
            transform = None
        # get crs
        try:
            crs = grid.rio.crs
        except Exception:
            crs = None

    # ensure path availability
    path = Path(path)
    # create path if needed
    _, _, _ = make_file_path(path, create_folder=True)

    # collect fx args
    fx_args = {
        "path": path,
        "data": values, "attrs": attrs,
        "transform": transform, "crs": crs,
        "name": var_name,
    }

    return fx_args
# ----------------------------------------------------------------------------------------------------------------------
