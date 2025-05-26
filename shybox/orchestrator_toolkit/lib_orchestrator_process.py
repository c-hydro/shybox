
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

        if isinstance(time, pd.Timestamp):
            time = [time]
        elif isinstance(time, list):
            if isinstance(time[0], pd.Timestamp):
                pass
            else:
                raise ValueError('Time format is not pd.Timestamp in the time list')
        else:
            raise ValueError('Time format is not pd.Timestamp in the time step')

        if isinstance(time, list):
            if len(time) == 1:
                time = time[0]

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

        if isinstance(time, list):
            if isinstance(data_raw, list):
                data_raw = data_raw * len(time)
            elif isinstance(data_raw, DataLocal):
                data_raw = [data_raw] * len(time)

        if isinstance(data_raw, list):
            if not isinstance(time, list):
                time = [time] * len(data_raw)
            elif isinstance(time, list):
                if len(data_raw) != len(time):
                    time = time[0] * len(data_raw)
                else:
                    pass
            else:
                raise ValueError('Time object is not compatible with data_raw')

        # memory is active only for start process
        if fx_id != 0:
            kwargs['memory_active'] = False


        if isinstance(data_raw, list):
            fx_data = []
            for data_id, (data_tmp, time_tmp) in enumerate(zip(data_raw, time)):
                fx_tmp = data_tmp.get_data(time=time_tmp, **kwargs)
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

        if isinstance(fx_data, list):
            if len(fx_data) == 1:
                fx_data = fx_data[0]

        fx_args = {arg_name: arg_value for arg_name, arg_value in self.fx_args.items()}
        fx_args['time'] = time
        fx_args['ref'] = self.fx_static['ref']
        fx_save = self.fx_obj(data=fx_data, **fx_args)

        fx_var = None
        if isinstance(fx_save, xr.DataArray):
            if hasattr(fx_save, 'name'):
                fx_var = fx_save.name
            if fx_var is None and fx_variable is not None:
                fx_var = fx_variable
        elif isinstance(fx_save, xr.Dataset):
            fx_var = list(fx_save.data_vars)
        # remove variable from args (directly pass to args)
        kwargs.pop('variable', None)

        out_opts = self.out_opts

        # check if time is a list of timestamps and reduce it if they are the same
        time = reduce_if_same_timestamps(time)

        if isinstance(time, list):
            print(f'{self.fx_name} - from {time[0]} to {time[-1]} - {self.variable}')
        else:
            print(f'{self.fx_name} - {time} - {self.variable}')

        #if 'variable' in kwargs:
        #    if kwargs.pop('variable', None) is not None:
        #        fx_var = kwargs.pop('variable', None)
        if fx_var is not None:
            if isinstance(fx_save, xr.DataArray):
                fx_save.name = fx_var
            elif isinstance(fx_save, xr.Dataset):
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
            if len(fx_var) == 1:
                fx_out = fx_save[fx_var[0]]
                fx_out.name = fx_var[0]
            else:
                fx_out = fx_save[fx_var]
                fx_out.name = fx_var
        else:
            raise ValueError('Unknown fx output type')

        return fx_out, fx_memory
# ----------------------------------------------------------------------------------------------------------------------

def reduce_if_same_timestamps(timestamps):
    if not timestamps:
        return []

    if not isinstance(timestamps, list):
        timestamps = [timestamps]

    # Ensure input is a list of pd.Timestamp
    timestamps = [pd.Timestamp(ts) for ts in timestamps]
    first = timestamps[0]

    if all(ts == first for ts in timestamps):
        return first  # All elements are the same
    else:
        return timestamps  # Elements differ, return as-is