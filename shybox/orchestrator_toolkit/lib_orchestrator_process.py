

# libraries
import datetime as dt
from typing import Callable
from functools import partial

import pandas as pd
import xarray as xr

from shybox.generic_toolkit.lib_utils_time import convert_time_format
from shybox.type_toolkit.io_dataset_grid import DataObj

class ProcessorContainer:
    def __init__(self,
                 function: Callable,
                 in_obj: DataObj,
                 args: dict = {},
                 out_obj: DataObj = None,
                 in_opts: dict = {}, out_opts: dict = {}) -> None:

        self.break_point = False

        fx_args, fx_static = {}, {}
        for arg_name, arg_value in args.items():
            if isinstance(arg_value, DataObj):
                if not arg_value.is_static:
                    fx_args[arg_name] = arg_value
                else:
                    fx_static[arg_name] = arg_value.get_data()
            else:
                fx_static[arg_name] = arg_value

        self.fx_name = function.__name__
        self.fx_obj = partial(function, **fx_static)

        self.fx_static = fx_static
        self.fx_args = fx_args

        self.in_obj = in_obj
        self.in_opts = in_opts

        self.out_obj = out_obj
        self.out_opts = out_opts

    def __repr__(self):
        return f'ProcessorContainer({self.fx_name})'

    def run(self, time: (dt.datetime, str, pd.Timestamp), **kwargs) -> None:

        if isinstance(time, str):
            time = convert_time_format(time, 'str_to_stamp')

        if 'data' not in kwargs:
            data_raw = self.in_obj
        else:
            data_raw = kwargs['data']

        if isinstance(data_raw, dict):
            if 'tiles' in list(self.in_opts.keys()):
                for data_key, data_tmp in data_raw.items():
                    fx_data = data_tmp.squeeze_data_by_time(time_step=time, **kwargs)
                metadata = {'tiles': list(data_raw.keys())}
            else:
                for data_key, data_tmp in data_raw.items():
                    self.run(time, data = data_tmp, **kwargs)
                return

        else:
            fx_data = data_raw.get_data(time_step=time, **kwargs)
            metadata = {}

        #fx_args = {arg_name: arg_value.get_data(time, **kwargs) for arg_name, arg_value in self.fx_args.items()}
        fx_args = {arg_name: arg_value for arg_name, arg_value in self.fx_args.items()}
        fx_out = self.fx_obj(data=fx_data, **fx_args)

        out_opts = self.out_opts
        print(f'{self.fx_name} - {time}, {kwargs}')

        self.out_obj.write_data(fx_out, time, metadata = metadata, **kwargs)

        return fx_out