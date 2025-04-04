
# ----------------------------------------------------------------------------------------------------------------------
# libraries
import datetime as dt
from typing import Callable
from functools import partial

import pandas as pd
import xarray as xr

from shybox.generic_toolkit.lib_utils_time import convert_time_format
from shybox.dataset_toolkit.dataset_handler_local import DataLocal
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# class to handle the process
class ProcessorContainer:
    def __init__(self,
                 function: Callable,
                 in_obj: DataLocal,
                 args: dict = {},
                 out_obj: (DataLocal, dict) = None,
                 in_opts: dict = {}, out_opts: dict = {}) -> None:

        self.break_point = False

        fx_args, fx_static = {}, {}
        for arg_name, arg_value in args.items():
            if isinstance(arg_value, DataLocal):
                if not arg_value.is_static:
                    fx_args[arg_name] = arg_value
                else:
                    fx_static[arg_name] = arg_value.get_data()
            else:
                fx_static[arg_name] = arg_value

        if 'variable' in args:
            self.variable = args.pop('variable')
        else:
            self.variable = 'generic'

        self.fx_name = function.__name__
        self.fx_obj = partial(function, **fx_static)

        self.fx_static = fx_static
        self.fx_args = fx_args

        self.in_obj = in_obj
        self.in_opts = in_opts

        self.out_obj = out_obj
        self.out_opts = out_opts

        self.dump_state = False

    def __repr__(self):
        return f'ProcessorContainer({self.fx_name, self.variable})'

    def run(self, time: (dt.datetime, str, pd.Timestamp), **kwargs) -> (None, None):

        if isinstance(time, str):
            time = convert_time_format(time, 'str_to_stamp')

        fx_id = kwargs['id'] if 'id' in kwargs else None
        fx_variable = kwargs['variable'] if 'variable' in kwargs else None

        fx_map_in = kwargs['map_in'] if 'map_in' in kwargs else None
        fx_map_out = kwargs['map_out'] if 'map_out' in kwargs else None

        if fx_id == 0 and 'memory' in kwargs:
            if (fx_variable is not None) and (fx_variable in kwargs['memory']):
                data_raw = kwargs['memory'][fx_variable]
            else:
                data_raw = self.in_obj
        else:
            data_raw = self.in_obj

        if isinstance(data_raw, list):
            fx_data = []
            for data_id, data_tmp in enumerate(data_raw):
                fx_tmp = data_tmp.get_data(time=time, **kwargs)
                fx_data.append(fx_tmp)
            fx_metadata = {}
        else:
            fx_data = data_raw.get_data(time=time, **kwargs)
            fx_metadata = {}

        fx_memory = None
        if fx_id == 0:
            if isinstance(data_raw, list):
                fx_memory = [data_tmp.memory_data for data_tmp in data_raw]
            else:
                fx_memory = data_raw.memory_data

        #fx_args = {arg_name: arg_value.get_data(time, **kwargs) for arg_name, arg_value in self.fx_args.items()}
        fx_args = {arg_name: arg_value for arg_name, arg_value in self.fx_args.items()}
        fx_save = self.fx_obj(data=fx_data, **fx_args)

        fx_var = None
        if isinstance(fx_save, xr.DataArray):
            if hasattr(fx_save, 'name'):
                fx_var = fx_save.name
            if fx_var is None and fx_variable is not None:
                fx_var = fx_variable
        # remove variable from args (directly pass to args)
        kwargs.pop('variable', None)

        out_opts = self.out_opts
        print(f'{self.fx_name} - {time} - {self.variable}')

        #if 'variable' in kwargs:
        #    if kwargs.pop('variable', None) is not None:
        #        fx_var = kwargs.pop('variable', None)
        if fx_var is not None:
            fx_save.name = fx_var

        if self.dump_state:
            if 'collections' in kwargs:
                fx_collections = kwargs.pop('collections', None)
                if fx_collections:
                    fx_save = fx_save.to_dataset(name = fx_var)
                    for tmp_key, tmp_data in fx_collections.items():
                        fx_save[tmp_key] = tmp_data

        # organize metadata
        kwargs['time_format'] = self.out_obj.get_attribute('time_format')
        kwargs['ref'] = self.fx_static['ref']
        # save the data
        self.out_obj.write_data(fx_save, time, metadata=fx_metadata, variable=fx_var, **kwargs)

        # arrange data to keep the data array format
        if isinstance(fx_save, xr.DataArray):
            fx_out = fx_save
        elif isinstance(fx_save, xr.Dataset):
            fx_out = fx_save[fx_var]
            fx_out.name = fx_var
        else:
            raise ValueError('Unknown fx output type')

        return fx_out, fx_memory
# ----------------------------------------------------------------------------------------------------------------------
