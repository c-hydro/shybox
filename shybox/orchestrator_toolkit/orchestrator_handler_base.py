#from processor import DAMProcessor
#from register_process import DAM_PROCESSES

#from ..tools.data import Dataset
#from ..tools.data.memory_dataset import MemoryDataset
#from ..tools.data.local_dataset import LocalDataset

#from ..tools.timestepping import TimeRange,estimate_timestep
#from ..tools.timestepping.time_utils import get_date_from_str

#from ..tools.config.options import Options

Options = dict()

from shybox.generic_toolkit.lib_utils_time import convert_time_format
from shybox.orchestrator_toolkit.lib_orchestrator_utils import PROCESSES
from shybox.orchestrator_toolkit.lib_orchestrator_process import ProcessorContainer

from shybox.type_toolkit.io_dataset_grid import DataObj

import datetime as dt
from typing import Optional
import tempfile
import os
import shutil
import pandas as pd
import xarray as xr

class OrchestratorHandler:

    default_options = {
        'intermediate_output'   : 'Mem', # 'Mem' or 'Tmp'
        'break_on_missing_tiles': False,
        'tmp_dir'               : None
    }

    def __init__(self,
                 data_in: (dict, DataObj),
                 data_out: (dict, DataObj) = None,
                 options: Optional[dict] = None) -> None:
        
        self.data_in = data_in
        self.data_out = data_out
        self.processes = []
        self.break_points = []

        self.options = self.default_options
        if options is not None:
            self.options.update(options)

        if self.options['intermediate_output'] == 'Tmp':
            tmp_dir = self.options.get('tmp_dir', tempfile.gettempdir())
            os.makedirs(tmp_dir, exist_ok = True)
            self.tmp_dir = tempfile.mkdtemp(dir = tmp_dir)

    @classmethod
    def from_options(cls, options: dict) -> 'Orchestrator':

        options = options.get('options', None, ignore_case=True)
        data_in = options['in']
        data_out = options['out']

        wf = cls(data_in=data_in, data_out=data_out, options=options)
        processes = options.get(['processes','process_list'], [], ignore_case=True)
        for process in processes:
            function_str = process.pop('function')
            function = PROCESSES[function_str]
            output = process.pop('output', None)
            wf.add_process(function, output, **process)

        return wf

    def clean_up(self):
        if hasattr(self, 'tmp_dir') and os.path.exists(self.tmp_dir):
            try:
                shutil.rmtree(self.tmp_dir)
            except Exception as e:
                print(f'Error cleaning up temporary directory: {e}')

    def make_output(self, in_obj: DataObj, out_obj: DataObj = None,
                    function = None) -> DataObj:

        if isinstance(out_obj, DataObj):
            return out_obj

        output_name = 'test'
        output_type = self.options['intermediate_output']
        if output_type == 'Mem':
            out_obj = DataObj
        elif output_type == 'Tmp':
            filename = 'test' #os.path.basename(output_pattern)
            out_obj = DataObj(path=self.tmp_dir, filename=filename)
        out_obj.name = output_name

        return out_obj

    def add_process(self, function, output: (DataObj, xr.Dataset, dict) = None, **kwargs) -> None:

        if len(self.processes) == 0:
            previous = None
            this_input = self.data_in
        else:
            previous = self.processes[-1]
            this_input = previous.output

        this_output = self.make_output(this_input, output, function)
        this_process = ProcessorContainer(
            function = function,
            in_obj = this_input, in_opts=self.options,
            args = kwargs,
            out_obj = this_output, out_opts=self.options)


        if this_process.break_point:
            self.break_points.append(len(self.processes))

        self.processes.append(this_process)

    def run(self, time: (pd.Timestamp, str, pd.date_range), **kwargs) -> None:

        if isinstance(time, str):
            time = convert_time_format(time, 'str_to_stamp')

        #data_time = self.data_in['time'].values

        if isinstance(time, pd.DatetimeIndex):
            time_steps = time
        elif isinstance(time, str):
            time_steps = pd.DatetimeIndex(pd.to_datetime(time))
        elif isinstance(time, pd.Timestamp):
            time_steps = pd.DatetimeIndex([time])
        else:
            time_steps = pd.DatetimeIndex([time])

        if len(time_steps) == 0:
            return None

        for ts in time_steps:
            self.run_single_ts(time=ts, **kwargs)
        return None
    
    def run_single_ts(self, time: (pd.Timestamp, str, pd.date_range), **kwargs) -> None:
        
        if isinstance(time, str):
            time = convert_time_format(time, 'str_to_stamp')

        if len(self.break_points) == 0:
            self._run_processes(self.processes, time, **kwargs)
        else:
            # proceed in chunks: run until the breakpoint, then stop
            i = 0
            processes_to_run = []
            while i < len(self.processes):
                #collect all processes until the breakpoint
                if i not in self.break_points:
                    processes_to_run.append(self.processes[i])
                else:
                    # run the processes until the breakpoint
                    self._run_processes(processes_to_run, time, **kwargs)
                    # then run the breakpoint by itself
                    self.processes[i].run(time, **kwargs)

                    # reset the list of processes
                    processes_to_run = []

                i += 1
            # run the remaining processes
            self._run_processes(processes_to_run, time, **kwargs)
        
        # clean up the temporary directory
        self.clean_up()

    def _run_processes(self, processes, time: dt.datetime, **kwargs) -> None:
        if len(processes) == 0:
            return

        for process in processes:
            process.run(time, **kwargs)