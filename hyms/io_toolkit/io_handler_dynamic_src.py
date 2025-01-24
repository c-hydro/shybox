# libraries
import os
import numpy as np
import pandas as pd
import xarray as xr
from typing import Optional

from hmc.generic_toolkit.data.lib_io_utils import substitute_string_by_date, substitute_string_by_tags
from hmc.generic_toolkit.data.lib_io_variables import fill_var_generic, fill_var_air_pressure, fill_var_error
from hmc.generic_toolkit.data.io_handler_base import IOHandler
from hmc.generic_toolkit.data.zip_handler_base import ZipHandler


class ZipWrapper(ZipHandler):
    def __init__(self, file_name_compress: str, file_name_uncompress: str = None,
                 zip_extension: str = '.gz') -> None:
        super().__init__(file_name_compress, file_name_uncompress, zip_extension)


class IOWrapper(IOHandler):
    def __init__(self, folder_name: str, file_name: str, file_format: Optional[str] = None, **kwargs) -> None:
        super().__init__(folder_name, file_name, file_format, **kwargs)

    def from_path(self, file_path: str, file_format: Optional[str] = None, **kwargs):
        folder_name, file_name = os.path.split(file_path)
        return super().__init__(folder_name, file_name, file_format, **kwargs)


class DynamicSrcHandler(ZipWrapper, IOWrapper):

    type_class = 'io_dynamic_src'
    type_data_fill = {'rain': fill_var_generic, 'airt': fill_var_generic,
                      'wind': fill_var_generic, 'inc_rad': fill_var_generic,
                      'rh': fill_var_generic, 'airp': fill_var_air_pressure}

    def __init__(self, folder_name: str, file_name: str, file_version: str = 'hmc_netcdf_v1',
                 vars_list: list = None, vars_mapping: dict = None) -> None:

        self.folder_name = folder_name
        self.file_name = file_name
        self.file_version = file_version

        self.vars_list = vars_list
        self.vars_mapping = vars_mapping

        extra_args = {'vars_list': self.vars_list, 'vars_mapping': self.vars_mapping}

        super().__init__(file_name_compress=os.path.join(folder_name, file_name), file_name_uncompress=None,
                         zip_extension='.gz')

        if self.zip_check:
            super().from_path(self.file_name_uncompress, **extra_args)
        else:
            super().from_path(self.file_name_compress, **extra_args)

    @classmethod
    def organize_file_data(cls, folder_name: str, file_name: str = 'hmc.forcing-grid.{datetime_dynamic_src_grid}.nc.gz',
                           file_time: pd.Timestamp = None,
                           file_tags: dict = None,
                           file_mandatory: bool = True, file_template: dict = None,
                           vars_list: dict = None, vars_tags: dict = None):

        if file_tags is None:
            file_tags = {}
        if file_template is None:
            file_template = {}
        if vars_list is None:
            vars_list = {}
        if vars_tags is None:
            vars_tags = {}

        vars_mapping = dict(zip(vars_list, vars_tags))

        folder_name = substitute_string_by_tags(folder_name, file_tags)
        folder_name = substitute_string_by_date(folder_name, file_time, file_template)
        file_name = substitute_string_by_tags(file_name, file_tags)
        file_name = substitute_string_by_date(file_name, file_time, file_template)

        if file_mandatory:
            if not os.path.exists(os.path.join(folder_name, file_name)):
                raise FileNotFoundError(f'File {file_name} does not exist in path {folder_name}.')

        return cls(folder_name, file_name, vars_list=vars_list, vars_mapping=vars_mapping)

    def get_file_data(self):

        check_data = self.uncompress_file_name()
        if check_data is not None:
            file_data = self.get_data(
                row_start=None, row_end=None, col_start=None, col_end=None, mandatory=True)
        else:
            file_data = self.get_data(
                row_start=None, row_end=None, col_start=None, col_end=None, mandatory=True)

        file_data = self.filter_data(file_data)
        file_data = self.map_data(file_data)

        return file_data

    def adjust_file_data(self, file_data_in: xr.Dataset) -> xr.Dataset:

        if self.file_version == 'hmc_netcdf_v1':

            file_vars = list(file_data_in.variables)

            file_dict = {}
            file_geo_x, file_geo_y, file_time = None, None, None
            for var_name in file_vars:
                if var_name == 'longitude':
                    var_data = file_data_in[var_name].values
                    file_geo_x = var_data[0, :]
                elif var_name == 'latitude':
                    var_data = file_data_in[var_name].values
                    file_geo_y = var_data[:, 0]
                elif var_name == 'time':
                    file_time = pd.Timestamp(file_data_in[var_name].values)
                else:
                    var_data = file_data_in[var_name].values
                    var_data = np.rot90(np.transpose(var_data))

                    file_dict[var_name] = var_data

            if file_geo_x is None or file_geo_y is None:
                raise ValueError('Longitude and latitude not found in file data.')
            file_data_out = xr.Dataset(coords={'longitude': file_geo_x, 'latitude': file_geo_y})
            for var_name, var_data in file_dict.items():

                da_data = xr.DataArray(var_data,
                                       coords={'latitude': file_geo_y, 'longitude': file_geo_x},
                                       dims=['latitude', 'longitude'])

                file_data_out[var_name] = da_data

            file_data_out.attrs = {'time': file_time}

        else:
            raise NotImplementedError(f'File format {self.file_format} not implemented.')

        return file_data_out

    def fill_file_data(self, file_data: xr.Dataset, ref_data: xr.DataArray,
                       vars_tags: list, vars_mandatory: list, vars_no_data: list) -> xr.Dataset:

        file_geo_x, file_geo_y = file_data['longitude'].values, file_data['latitude'].values
        ref_values = ref_data.values

        for var_tag, vars_mandatory, var_no_data in zip(vars_tags, vars_mandatory, vars_no_data):

            if var_tag in list(file_data.variables):

                var_data = file_data[var_tag].values
                if np.all(var_data == var_no_data):
                    var_fx_fill = self.type_data_fill.get(var_tag, fill_var_error)
                    var_data = var_fx_fill(terrain=ref_values, default_value=var_no_data, no_data_value=var_no_data)
                    file_data[var_tag].values = var_data
            else:
                if vars_mandatory:
                    raise ValueError(f'Variable {var_tag} not found in file data.')
                else:
                    var_fx_fill = self.type_data_fill.get(var_tag, fill_var_error)
                    var_data = var_fx_fill(terrain=ref_values, default_value=var_no_data)
                    var_data[var_data == var_no_data] = np.nan
                    da_data = xr.DataArray(var_data,
                                           coords={'latitude': file_geo_y, 'longitude': file_geo_x},
                                           dims=['latitude', 'longitude'])
                    file_data[var_tag] = da_data

        return file_data

