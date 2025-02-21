"""
Class Features

Name:          driver_data_grid
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20241126'
Version:       '4.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import os
import pandas as pd
import xarray as xr

from shybox.generic_toolkit.lib_utils_settings import get_data_template

from shybox.generic_toolkit.lib_utils_time import convert_time_format
from shybox.generic_toolkit.lib_utils_string import fill_tags2string
from shybox.generic_toolkit.lib_default_args import logger_name, logger_arrow

from shybox.io_toolkit.tmp import io_handler_base

from shybox.io_toolkit.lib_io_utils import extract_time_from_string
from shybox.io_toolkit.tmp.io_handler_base import IOHandler
from shybox.io_toolkit.tmp.zip_handler_base import ZipHandler

# logging
logger_stream = logging.getLogger(logger_name)

# debugging
# import matplotlib.pylab as plt
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# class to wrap zip handler
class ZipWrapper(ZipHandler):
    def __init__(self, file_name_compress: str, file_name_uncompress: str = None,
                 zip_extension: str = '.gz') -> None:
        super().__init__(file_name_compress, file_name_uncompress, zip_extension)

# class to wrap io handler
class IOWrapper(IOHandler):
    def __init__(self, file_name, file_format: str = None, **kwargs) -> None:
        super().__init__(file_name, file_format, **kwargs)

    def from_path(self, file_name: str, file_format: str = None, **kwargs):

        if file_format is None:
            file_format = file_name.split('.')[-1]
            logger_stream.warning(
                logger_arrow.warning +
                'File format not provided. Trying to infer it from file name. Select: "' + file_format + '"')

        return super().__init__(file_name=file_name, file_format=file_format, **kwargs)
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# class to configure data
class DrvData(ZipWrapper, IOWrapper):

    # ------------------------------------------------------------------------------------------------------------------
    # global variable(s)
    class_type = 'driver_data'
    file_handler = None
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # class initialization
    def __init__(self, file_name: str, file_time: pd.Timestamp = None,
                 file_type: str = 'raster', file_format: str = 'netcdf',
                 map_dims: dict = None, map_geo: dict = None, map_data: dict = None, **kwargs)-> None:

        self.file_name = file_name
        self.file_format = file_format
        self.file_type = file_type

        super().__init__(file_name_compress=self.file_name, file_name_uncompress=None,
                         zip_extension='.gz')

        extra_args = {}
        if self.zip_check:
            super().from_path(self.file_name_uncompress, file_format=self.file_format, **extra_args)
            self.uncompress_file_name()
            self.file_name, self.file_tmp = self.file_name_uncompress, self.file_name_compress
        else:
            super().from_path(self.file_name_compress, **extra_args)

        self.file_time = file_time
        self.map_dims, self.map_geo, self.map_data = map_dims, map_geo, map_data
        self.file_handler = io_handler_base.IOHandler(
            file_name=self.file_name, file_time=self.file_time,
            file_type=self.file_type, file_format=self.file_format,
            map_dims=self.map_dims, map_geo=self.map_geo, map_data=self.map_data)

    @classmethod
    def by_template(cls, file_name: str, file_time: (str, pd.Timestamp) = None,
                    file_template: dict = None, file_mandatory: bool = True,
                    tags_value: dict = None, tags_format: dict = None,
                    **kwargs):

        file_time = convert_time_format(file_time, time_conversion='str_to_stamp')

        if tags_value is None:
            tags_value = {'file_datetime': file_time, 'file_sub_path': file_time, 'domain_name': 'default'}
        if tags_format is None:
            tags_format = {'file_datetime': '%Y%m%d%H00', 'file_sub_path': '%Y/%m/%d', 'domain_name': 'string'}

        if file_template is None or not file_template:
            logger_stream.error(logger_arrow.error + 'File template is not defined')
            raise ValueError('File template must be defined to run the process')
        else:
            data_template = get_data_template(file_template)

        file_name = fill_tags2string(file_name, tags_format, tags_value)[0]

        if file_mandatory:
            if not os.path.exists(file_name):
                logger_stream.error(logger_arrow.error + 'File "' + file_name + '" does not exist')
                raise FileNotFoundError(f'File {file_name} is mandatory and must defined to run the process')

        file_type, file_format = None, None
        if 'format' in list(data_template.keys()):
            file_format = data_template['format']
        if 'type' in list(data_template.keys()):
            file_type = data_template['type']

        map_data, map_geo, map_dims = {}, {}, {}
        if 'map_data' in list(data_template.keys()):
            map_data = data_template['map_data']
        if 'map_geo' in list(data_template.keys()):
            map_geo = data_template['map_geo']
        if 'map_dims' in list(data_template.keys()):
            map_dims = data_template['map_dims']

        return cls(file_name=file_name, file_time=file_time, file_type=file_type, file_format=file_format,
                   map_dims=map_dims, map_geo=map_geo, map_data=map_data, **kwargs)


    @classmethod
    def by_file_generic(cls, file_name: (str, None) = 'hmc.forcing-grid.{file_datetime}.nc.gz',
                       file_time: (str, pd.Timestamp) = None, file_format='netcdf', file_mandatory: bool = True,
                       tags_value: dict = None, tags_format: dict = None,
                       map_dims: dict = None, map_geo: dict = None, map_data: dict = None):

        file_time = convert_time_format(file_time, time_conversion='str_to_stamp')

        if tags_value is None:
            tags_value = {'file_datetime': file_time, 'file_sub_path': file_time, 'domain_name': 'default'}
        if tags_format is None:
            tags_format = {'file_datetime': '%Y%m%d%H00', 'file_sub_path': '%Y/%m/%d', 'domain_name': 'string'}
        if map_dims is None:
            map_dims = {}
        if map_geo is None:
            map_geo = {}
        if map_data is None:
            map_data = {}

        file_name = fill_tags2string(file_name, tags_format, tags_value)[0]

        if file_mandatory:
            if not os.path.exists(file_name):
                logger_stream.error(logger_arrow.error + 'File "' + file_name + '" does not exist')
                raise FileNotFoundError(f'File {file_name} is mandatory and must defined to run the process')

        return cls(file_name=file_name, file_format=file_format,
                   map_dims=map_dims, map_geo=map_geo, map_data=map_data)

    # ------------------------------------------------------------------------------------------------------------------


    # ------------------------------------------------------------------------------------------------------------------
    # organize variable data
    def get_variable_data(self, row_start: int = None, row_end: int = None,
                               col_start: int = None, col_end: int = None) -> (xr.Dataset, xr.DataArray):

        # info algorithm (start)
        logger_stream.info(logger_arrow.info(tag='info_method') + 'Get "variable_data" ... ')

        # method to get data
        file_data = self.file_handler.get_data(
            row_start=row_start, row_end=row_end, col_start=col_start, col_end=col_end, mandatory=True)

        # info algorithm (end)
        logger_stream.info(logger_arrow.info(tag='info_method') + 'Get "variable_data" ... DONE')

        return file_data

    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # method to organize data
    def organize_variable_data(self, file_data: (xr.Dataset, xr.DataArray) = None) -> (xr.Dataset, xr.DataArray):

        # info algorithm (start)
        logger_stream.info(logger_arrow.info(tag='info_method') + 'Organize "variable_data" ... ')

        # method to select data (if needed)
        file_data = self.file_handler.select_data(file_data)
        # method to remap data (if needed)
        file_data = self.file_handler.remap_data(file_data)

        # info algorithm (end)
        logger_stream.info(logger_arrow.info(tag='info_method') + 'Organize "variable_data" ... DONE')

        return file_data

    # ------------------------------------------------------------------------------------------------------------------


    # ------------------------------------------------------------------------------------------------------------------
    # method to view data
    def view_variable_data(self, data: (xr.Dataset, xr.DataArray) = None,
                           var_name: str = None, var_min: float = 0, var_max: float = None,
                           mode: bool = True) -> None:
        # info algorithm (start)
        logger_stream.info(logger_arrow.info(tag='info_method') + 'View "variable_data" ... ')
        if mode:
            self.file_handler.view_data(
                obj_data=data, var_name=var_name, var_data_min=var_min, var_data_max=var_max)
        # info algorithm (start)
        logger_stream.info(logger_arrow.info(tag='info_method') + 'View "variable_data" ... DONE')
    # ------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# class to handle multi data
class MultiData(DrvData):

    def __init__(self, file_handler: object, **kwargs: object) -> None:

        self.file_handler = file_handler

    @classmethod
    def by_iterable(cls, file_iterable: (str, list, dict) = None,
                    file_time: (str, pd.Timestamp) = None, format_time: str = None,
                    file_template: dict = None, file_mandatory: bool = True,
                    tags_value: dict = None, tags_format: dict = None):

        if isinstance(file_iterable, str):
            file_iterable = [file_iterable]
        if isinstance(file_iterable, list):
            tmp_iterable = {}
            for n, v in enumerate(file_iterable):
                tmp_iterable[n] = v
            file_iterable = tmp_iterable

        if file_time is None:
            file_time = None
            for file_key, file_name in file_iterable.items():
                tmp_time = extract_time_from_string(file_name, time_format=format_time)
                if file_time is None:
                    file_time = tmp_time
                elif file_time is not None and file_time != tmp_time:
                    logger_stream.error(logger_arrow.error + 'Time format is not consistent')
                    raise ValueError('Time format is not consistent')

        # using list comprehension to append instances to list
        return cls({file_key: DrvData.by_template(
            file_name=file_name, file_time=file_time,
            file_template=file_template, file_mandatory=file_mandatory,
            tags_value=tags_value, tags_format=tags_format) for file_key, file_name in file_iterable.items()})

    def get_variable_data(self, **kwargs) -> (xr.Dataset, xr.DataArray):
        file_data = {file_key: file_handler.get_variable_data(
            row_start=None, row_end=None, col_start=None, col_end=None)
            for file_key, file_handler in self.file_handler.items()}
        return file_data
