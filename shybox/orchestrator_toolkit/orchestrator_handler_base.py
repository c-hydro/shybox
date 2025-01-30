#from processor import DAMProcessor
#from register_process import DAM_PROCESSES

#from ..tools.data import Dataset
#from ..tools.data.memory_dataset import MemoryDataset
#from ..tools.data.local_dataset import LocalDataset

#from ..tools.timestepping import TimeRange,estimate_timestep
#from ..tools.timestepping.time_utils import get_date_from_str

#from ..tools.config.options import Options

Options = dict()

from shybox.orchestrator_toolkit.lib_orchestrator_utils import PROCESSES
from shybox.orchestrator_toolkit.lib_orchestrator_process import ProcessorContainer

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
                 data_in: (xr.Dataset, dict),
                 data_ref: (xr.Dataset, xr.DataArray, dict),
                 data_out: Optional[xr.Dataset] = None,
                 options: Optional[dict] = None) -> None:
        
        self.data_in = data_in
        self.data_ref = data_ref
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

        wf_options = options.get('options', None, ignore_case=True)
        data_in = wf_options['in']
        data_ref = wf_options['ref']
        data_out = wf_options['out']

        wf = cls(data_in=data_in, data_ref=data_ref, data_out=data_out, options=wf_options)
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

    def make_output(self, input: xr.Dataset,
                    output: (xr.Dataset,dict) = None,
                    function = None) -> xr.Dataset:
        if isinstance(output, xr.Dataset):
            return output

        output_name = 'test'
        output_type = self.options['intermediate_output']
        if output_type == 'Mem':
            output_ds = xr.Dataset()
        elif output_type == 'Tmp':
            filename = 'test' #os.path.basename(output_pattern)
            output_ds = xr.Dataset()

        # output_ds.name = output_name
        return output_ds

    def add_process(self, function, output: (xr.Dataset,dict) = None, **kwargs) -> None:

        if len(self.processes) == 0:
            previous = None
            this_input = self.data_in
        else:
            previous = self.processes[-1]
            this_input = previous.output

        this_output = self.make_output(this_input, output, function)
        this_process = ProcessorContainer(function = function,
                                    data = this_input,
                                    args = kwargs,
                                    output = this_output,
                                    wf_options = self.options)

        if this_process.break_point:
            self.break_points.append(len(self.processes))

        self.processes.append(this_process)

    def run(self, time: (dt.datetime, str, pd.date_range), **kwargs) -> None:

        '''
        if len(self.processes) == 0:
            raise ValueError('No processes have been added to the workflow.')
        elif isinstance(self.processes[-1].output, MemoryDataset) or\
            (isinstance(self.processes[-1].output, LocalDataset) and
             hasattr(self, 'tmp_dir') and self.tmp_dir in self.processes[-1].output.dir):
            if self.output is not None:
                self.processes[-1].output = self.output.copy()
            else:
                raise ValueError('No output dataset has been set.')
        '''

        data_time = self.data_in['time'].values

        if hasattr(time, 'start') and hasattr(time, 'end'):
            timestamps = self.data_in.get_times(time, **kwargs)

            if self.data_in.time_signature == 'end+1':
                timestamps = [t - dt.timedelta(days = 1) for t in timestamps]

            if len(timestamps) > 5:
                timestep = estimate_timestep(timestamps)
            else:
                timestep = self.data_in.estimate_timestep()

            if timestep is not None:
                time_steps = [timestep.from_date(t) for t in timestamps]
            else:
                time_steps = timestamps
            
            if len(time_steps) == 0:
                return
            else:
                for time_step in time_steps:
                    self.run_single_ts(time_step, **kwargs)
                return
        else:
            self.run_single_ts(time, **kwargs)
    
    def run_single_ts(self, time: (dt.datetime, str, pd.date_range), **kwargs) -> None:
        
        if isinstance(time, str):
            time = get_date_from_str(time)

        if len(self.break_points) == 0:
            self._run_processes(self.processes, time, **kwargs)
        else:
            # proceed in chuncks: run until the breakpoint, then stop
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