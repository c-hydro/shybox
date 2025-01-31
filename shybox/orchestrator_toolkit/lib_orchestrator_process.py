

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
                 wf_options: dict = {}) -> None:
        self.break_point = False

        ds_args = {}
        static_args = {}
        for arg_name, arg_value in args.items():
            if isinstance(arg_value, xr.Dataset):
                if not arg_value.is_static:
                    ds_args[arg_name] = arg_value
                else:
                    static_args[arg_name] = arg_value.get_data()
            else:
                static_args[arg_name] = arg_value

        self.funcname = function.__name__
        self.function = partial(function, **static_args)
        self.ds_args = ds_args
        self.data = data

        self.output = output

    def __repr__(self):
        return f'ProcessorContainer({self.funcname})'

    def run(self, time: (dt.datetime,str), **kwargs) -> None:

        if isinstance(time, str):
            time = convert_time_format(time, 'str_to_stamp')

        if 'data' not in kwargs:
            data_raw = self.data
        else:
            data_raw = kwargs['data']

        if isinstance(data_raw, dict):
            for key, data_tmp in data_raw.items():
                self.run(time, data = data_tmp, **kwargs)
            return

        else:
            data_selection = data_raw.squeeze_data_by_time(time_data=time, **kwargs)
            metadata = {}

        ''''
        if 'tile' not in kwargs:
            all_tiles = self.input.tile_names if input_options['break_on_missing_tiles'] else self.input.find_tiles(time, **kwargs)
            if not input_options['tiles']:
                for tile in all_tiles:
                    self.run(time, tile = tile, **kwargs)
                return
            else:
                input_data = (self.input.get_data(time, tile = tile, **kwargs) for tile in all_tiles)
                metadata = {'tiles': all_tiles}
        else:
            input_data = self.input.get_data(time, **kwargs)
            metadata = {}
        '''
        ds_args = {arg_name: arg.get_data(time, **kwargs) for arg_name, arg in self.ds_args.items()}
        output = self.function(input = input_data, **ds_args)

        output_options = self.output_options
        print(f'{self.funcname} - {time}, {kwargs}')

        self.output.write_data(output, time, metadata = metadata, **kwargs)