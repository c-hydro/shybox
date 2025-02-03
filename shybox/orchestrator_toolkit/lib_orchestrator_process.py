

# libraries
import datetime as dt
from typing import Callable
from functools import partial

import xarray as xr

from shybox.generic_toolkit.lib_utils_time import convert_time_format

class ProcessorContainer:
    def __init__(self,
                 function: Callable,
                 data: dict,
                 args: dict = {},
                 output: dict = None,
                 options: dict = {}) -> None:

        self.break_point = False

        fx_args, fx_static = {}, {}
        for arg_name, arg_value in args.items():
            if isinstance(arg_value, (xr.Dataset, xr.DataArray)):
                fx_args[arg_name] = arg_value
            else:
                fx_static[arg_name] = arg_value

        self.fx_name = function.__name__
        self.fx_obj = partial(function, **fx_static)

        self.fx_static = fx_static
        self.fx_args = fx_args

        self.data = data
        self.options = options

        self.output = output

    def __repr__(self):
        return f'ProcessorContainer({self.fx_name})'

    def run(self, time: (dt.datetime,str), **kwargs) -> None:

        if isinstance(time, str):
            time = convert_time_format(time, 'str_to_stamp')

        if 'data' not in kwargs:
            data_raw = self.data
        else:
            data_raw = kwargs['data']

        if isinstance(data_raw, dict):
            if 'tiles' in list(self.options.keys()):
                for data_key, data_tmp in data_raw.items():
                    fx_data = data_tmp.squeeze_data_by_time(time_step=time, **kwargs)
                metadata = {'tiles': list(data_raw.keys())}
            else:
                for data_key, data_tmp in data_raw.items():
                    self.run(time, data = data_tmp, **kwargs)
                return

        else:
            fx_data = data_raw.squeeze_data_by_time(time_step=time, **kwargs)
            metadata = {}

        #fx_args = {arg_name: arg_value.get_data(time, **kwargs) for arg_name, arg_value in self.fx_args.items()}
        fx_args = {arg_name: arg_value for arg_name, arg_value in self.fx_args.items()}
        fx_out = self.fx_obj(data=fx_data, **fx_args)

        output_options = self.output_options
        print(f'{self.fx_name} - {time}, {kwargs}')

        self.output.write_data(output, time, metadata = metadata, **kwargs)