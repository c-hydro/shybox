

# libraries
import datetime as dt
from typing import Callable
from functools import partial

import pandas as pd
import xarray as xr

from shybox.generic_toolkit.lib_utils_time import convert_time_format
from shybox.dataset_toolkit.dataset_handler_local import DataLocal

class ProcessorContainer:
    def __init__(self,
                 function: Callable,
                 in_obj: DataLocal,
                 args: dict = {},
                 out_obj: DataLocal = None,
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

    def run(self, time: (dt.datetime, str, pd.Timestamp), **kwargs) -> None:

        if isinstance(time, str):
            time = convert_time_format(time, 'str_to_stamp')

        if 'data' not in kwargs:
            data_raw = self.in_obj
        else:
            data_raw = kwargs['data']

        if isinstance(data_raw, dict):
            for data_key, data_tmp in data_raw.items():
                self.run(time, data = data_tmp, **kwargs)
            return
        else:
            fx_data = data_raw.get_data(time=time, **kwargs)
            metadata = {}

        #fx_args = {arg_name: arg_value.get_data(time, **kwargs) for arg_name, arg_value in self.fx_args.items()}
        fx_args = {arg_name: arg_value for arg_name, arg_value in self.fx_args.items()}
        fx_out = self.fx_obj(data=fx_data, **fx_args)

        out_opts = self.out_opts
        print(f'{self.fx_name} - {time} - {self.variable}')

        if self.dump_state:
            if 'collections' in kwargs:
                fx_collections = kwargs.pop('collections', None)
                fx_out = fx_out.to_dataset(name = self.variable)
                for tmp_key, tmp_data in fx_collections.items():
                    fx_out[tmp_key] = tmp_data

        kwargs['time_format'] = self.out_obj.get_attribute('time_format')

        self.out_obj.write_data(fx_out, time, metadata=metadata, **kwargs)

        return fx_out