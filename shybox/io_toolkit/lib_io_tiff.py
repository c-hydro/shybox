"""
Library Features:

Name:          lib_io_tiff
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20260824'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
from __future__ import annotations

import os
import numpy as np
import xarray as xr
import rasterio

from shybox.logging_toolkit.lib_logging_utils import with_logger
from shybox.io_toolkit.lib_io_utils import make_file_path
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to read tiff
@with_logger(var_name='logger_stream')
def read_tiff_base(
        file_name: str, var_name: str = "data", file_mandatory: bool = False,
        coord_name_x: str = "longitude", coord_name_y: str = "latitude",
        no_data: float = None, dtype=None):

    # check file
    if file_name is None:
        raise ValueError("'file_name' must be defined.")

    # check file availability
    if not os.path.exists(file_name):
        if file_mandatory:
            logger_stream.error(f'Mandatory file "{file_name}" was not found.')
            raise FileNotFoundError('Mandatory file not found: "{file_name}"')
        else:
            logger_stream.warning(f'Optional file "{file_name}" was not found. File will be skipped.')
            return None

    # read raster
    with rasterio.open(file_name) as src:

        # read first band
        data = src.read(1)

        # get metadata
        transform = src.transform
        crs = src.crs
        src_nodata = src.nodata

        # cast dtype if requested
        if dtype is not None:
            data = data.astype(dtype)

        # get raster shape
        nrows, ncols = data.shape

        # compute cell-center coordinates
        x_coords = transform.c + (np.arange(ncols) + 0.5) * transform.a
        y_coords = transform.f + (np.arange(nrows) + 0.5) * transform.e

        # choose nodata
        nodata_value = (no_data if no_data is not None else src_nodata)

        # mask nodata
        if nodata_value is not None:
            data = np.where(data == nodata_value, np.nan, data)

        # create DataArray
        da = xr.DataArray(
            data,
            dims=(coord_name_y, coord_name_x),
            coords={
                coord_name_y: y_coords,
                coord_name_x: x_coords,
            },
            name=var_name
        )

        # store raster metadata
        da.attrs["transform"] = transform
        da.attrs["crs"] = crs
        da.attrs["nodata"] = nodata_value

    return da
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to write tiff file
@with_logger(var_name='logger_stream')
def write_tiff_base(
        path, data, attrs,
        transform=None, crs=None,
        name='NA',
        dtype="float32", nodata=-9999.0, compress="deflate"):

    # info start
    logger_stream.info_up(f"Write file {path} ... ")

    # manage file path
    file_path, file_folder, file_name = make_file_path(path)
    # get values
    values = np.asarray(data)

    # check raster dimensions
    if values.ndim != 2: raise ValueError(f'Raster data must be 2D. Found shape: {values.shape}')

    # get transform from attributes if not explicitly defined
    if transform is None: transform = attrs.get("transform", None)
    # get crs from attributes if not explicitly defined
    if crs is None: crs = attrs.get("crs", None)

    # check transform
    if transform is None: raise ValueError('Raster transform is not defined.')
    # check crs
    if crs is None:raise ValueError('Raster CRS is not defined.')

    # write GeoTIFF
    with rasterio.open(
            file_path,
            "w",
            driver="GTiff", height=values.shape[0], width=values.shape[1],
            count=1, dtype=dtype, crs=crs, transform=transform,
            nodata=nodata,
            compress=compress) as dst:

        # write raster values
        dst.write(values.astype(dtype), 1)

        # set band name
        if name is not None and name != "NA":
            dst.set_band_description(1, str(name))

        # save attributes as metadata
        if attrs:
            metadata = {
                str(key): str(value)
                for key, value in attrs.items()
                if key not in ["transform", "crs"]
            }

            if metadata:
                dst.update_tags(**metadata)

    # info end
    logger_stream.info_down(f"Write file {path} ... DONE")

    return str(file_path)
# ----------------------------------------------------------------------------------------------------------------------
