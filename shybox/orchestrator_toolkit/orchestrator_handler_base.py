
from shybox.generic_toolkit.lib_utils_time import convert_time_format
from shybox.orchestrator_toolkit.lib_orchestrator_utils import PROCESSES
from shybox.orchestrator_toolkit.lib_orchestrator_process import ProcessorContainer

from shybox.dataset_toolkit.dataset_handler_mem import DataMem
from shybox.dataset_toolkit.dataset_handler_local import DataLocal


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
                 data_in: (DataLocal, dict),
                 data_out: DataLocal = None,
                 options: dict = None) -> None:
        
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

        self.vars_list = []
        if isinstance(data_in, dict):
            self.vars_list = list(data_in.keys())

        self.save_var = None
        self.save_base = None

    @classmethod
    def from_options(cls, options: dict) -> 'Orchestrator':

        options = options.get('options', None, ignore_case=True)
        data_in = options['in']
        data_out = options['out']

        workflow = cls(data_in=data_in, data_out=data_out, options=options)
        processes = options.get(['processes','process_list'], [], ignore_case=True)
        for process in processes:
            function_str = process.pop('function')
            function = PROCESSES[function_str]
            output = process.pop('output', None)
            workflow.add_process(function, output, **process)

        return workflow

    @classmethod
    def multi_tile(self, data_package: (dict, list), data_out: DataLocal = None, data_ref: DataLocal = None,
                   configuration: dict = None) -> 'Orchestrator':

        workflow_fx = configuration.get('process_list', [])
        workflow_options = configuration.get('options', [])

        workflow_common = OrchestratorHandler(data_in=data_package, data_out=data_out,
                                              options=workflow_options)

        for process_fx_args in workflow_fx:

            process_fx_name = process_fx_args.pop('function')
            process_fx_obj = PROCESSES[process_fx_name]

            '''
            if hasattr(process_fx_obj, 'output_ext'):
                process_fx_out = process_fx_obj.output_ext
            else:
                process_fx_out = None
            #process_fx_out = process_fx_obj.pop('output', None)
            '''
            process_fx_args = {**process_fx_args, **{'variable': 'tiles'}}
            workflow_common.add_process(process_fx_obj, ref=data_ref, **process_fx_args)

        return workflow_common

    @classmethod
    def multi_variable(cls, data_package: (dict, list), data_out: DataLocal = None, data_ref: DataLocal = None,
                       configuration: dict = None) -> 'Orchestrator':

        workflow_fx = configuration.get('process_list', [])
        workflow_options = configuration.get('options', []) # derivare il dizionario come fatto per options per usare ignore_case=True

        if isinstance(data_package, list):
            data_collections = {}
            for data_id, data_obj in enumerate(data_package):
                data_name = None
                if hasattr(data_obj, 'file_variable'):
                    data_name = data_obj.file_variable
                if data_name is None:
                    data_name = 'default_{:}'.format(int(data_id))
                data_collections[data_name] = data_obj
        else:
            data_collections = data_package

        assert data_collections.keys() == workflow_fx.keys(), \
            'Data collections and workflow functions must have the same keys.'

        workflow_common = OrchestratorHandler(data_in=data_collections, data_out=data_out,
                                              options=workflow_options)

        for var_tag in list(data_collections.keys()):

            process_fx_var = workflow_fx[var_tag]

            for process_fx_args in process_fx_var:

                process_fx_name = process_fx_args.pop('function')
                process_fx_obj = PROCESSES[process_fx_name]

                '''
                if hasattr(process_fx_obj, 'output_ext'):
                    process_fx_out = process_fx_obj.output_ext
                else:
                    process_fx_out = None
                #process_fx_out = process_fx_obj.pop('output', None)
                '''
                process_fx_args = {**process_fx_args, **{'variable': var_tag}}
                workflow_common.add_process(process_fx_obj, ref=data_ref, **process_fx_args)

        return workflow_common

    @property
    def has_variables(self):
        if isinstance(self.data_in, dict):
            return True if len(self.data_in) > 0 else False
        else:
            return False

    def clean_up(self):
        if hasattr(self, 'tmp_dir') and os.path.exists(self.tmp_dir):
            try:
                shutil.rmtree(self.tmp_dir)
            except Exception as e:
                print(f'Error cleaning up temporary directory: {e}')

    def make_output(self, in_obj: DataLocal, out_obj: DataLocal = None,
                    function = None, **kwargs) -> DataLocal:

        if isinstance(out_obj, DataLocal):
            return out_obj

        path_in = in_obj.loc_pattern
        file_name_in = in_obj.file_name

        has_times, format_times = False, None
        if hasattr(in_obj ,'has_time'):
            has_times = in_obj.has_time
            format_times = "%Y%m%d%H%M%S"

        # create the name of the output file based on the function name
        file_history = None
        if function is not None:
            # get the name of process fx
            fx_name = f'_{function.__name__}'
            # create the output file name pattern
            ext_in = os.path.splitext(path_in)[1][1:]
            ext_out = function.__getattribute__('output_ext') or ext_in

            if self.has_variables:

                variable = kwargs['variable']

                name_base, ext_base = os.path.basename(path_in).split('.')
                save_base = f'{name_base}_{variable}'

                if has_times:
                    path_out = f'{save_base}_{fx_name}_{format_times}.{ext_out}'
                    file_history = f'{file_name_in}_{variable}_{fx_name}_{format_times}'
                else:
                    path_out = f'{save_base}_{fx_name}.{ext_out}'
                    file_history = f'{file_name_in}_{variable}_{fx_name}'

            else:

                name_base, ext_base = os.path.basename(path_in).split('.')
                save_base = f'{name_base}'

                if has_times:
                    path_out = f'_{save_base}_{fx_name}_{format_times}.{ext_out}'
                    file_history = f'{file_name_in}_{fx_name}_{format_times}'
                else:
                    path_out = f'_{save_base}_{fx_name}.{ext_out}'
                    file_history = f'{file_name_in}_{fx_name}'

        if out_obj is None:
            path_out = path_out
        elif isinstance(out_obj, dict):
            path_out = out_obj.get('loc_pattern', path_out)
        else:
            raise ValueError('Output must be a Dataset or a dictionary.')

        output_type = self.options['intermediate_output']
        if output_type == 'Mem':
            out_obj = DataMem(loc_pattern=path_out)
        elif output_type == 'Tmp':
            file_name_tmp = os.path.basename(path_out)
            out_obj = DataLocal(path=self.tmp_dir, file_name=file_name_tmp)
        else:
            raise ValueError('Orchestrator output type must be "Mem" or "Tmp"')

        out_obj.file_history = file_history

        return out_obj

    def add_process(self, function, process_output: (DataLocal, xr.Dataset, dict) = None, **kwargs) -> None:

        if 'variable' not in kwargs:
            kwargs['variable'] = 'generic'

        process_obj = self.processes
        process_previous = self.processes[-1] if len(process_obj) > 0 else None
        if (process_previous is not None) and (kwargs['variable'] not in process_previous.variable):
            process_init = True
        elif len(process_obj) == 0:
            process_init = True
        else:
            process_init = False

        if process_init:
            process_previous = None
            if isinstance(self.data_in, dict):
                this_input = self.data_in[kwargs['variable']]
            elif isinstance(self.data_in, DataLocal):
                this_input = self.data_in
            else:
                raise RuntimeError('Input data must be DataObj or dictionary of DataObj instance.')
        else:
            process_previous = self.processes[-1]
            this_input = process_previous.out_obj

        this_output = self.make_output(this_input, process_output, function, **kwargs)
        this_process = ProcessorContainer(
            function = function,
            in_obj = this_input, in_opts=self.options,
            args = kwargs,
            out_obj = this_output, out_opts=self.options)

        if this_process.break_point:
            self.break_points.append(len(self.processes))

        self.processes.append(this_process)

    def run(self, time: (pd.Timestamp, str, pd.date_range), **kwargs) -> None:

        # manage process execution and process output
        if len(self.processes) == 0:
            raise ValueError('No processes have been added to the workflow.')
        elif isinstance(self.processes[-1].out_obj, DataMem) or \
            (isinstance(self.processes[-1].out_obj, DataLocal) and hasattr(self, 'tmp_dir') and
             self.tmp_dir in self.processes[-1].out_obj.dir_name):
            if self.data_out is not None:
                self.processes[-1].out_obj = self.data_out.copy()
                self.processes[-1].dump_state = True
            else:
                raise ValueError('No output dataset has been set.')

        if isinstance(time, str):
            time = convert_time_format(time, 'str_to_stamp')

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

        proc_group = {}
        for proc_obj in processes:
            proc_var = proc_obj.variable

            if proc_var not in proc_group:
                proc_group[proc_var] = [proc_obj]
            else:
                tmp_obj = proc_group[proc_var]
                tmp_obj.append(proc_obj)
                proc_group[proc_var] = tmp_obj

        proc_memory, proc_ws = None, {}
        for proc_var_key, proc_list in proc_group.items():

            proc_result, proc_return, proc_var_name = None, [], None
            for proc_id, proc_name in enumerate(proc_list):

                tmp_var_name = None
                if proc_result is not None:
                    tmp_var_name = proc_result.name

                kwargs['id'] = proc_id
                kwargs['variable'] = tmp_var_name
                kwargs['collections'] = proc_ws

                if proc_memory is not None:
                    kwargs['memory'] = {}
                    if tmp_var_name not in kwargs['memory']:
                        kwargs['memory'][tmp_var_name] = proc_memory

                if tmp_var_name is None:
                    print()

                proc_result, proc_memory = proc_name.run(time, **kwargs)

                if isinstance(proc_result, xr.DataArray):
                    proc_var_name = proc_result.name
                else:
                    raise ValueError('Process output must be a DataArray.')

                proc_return.append(proc_result)

            proc_ws[proc_var_name] = proc_return[-1]



def remove_none(lst):
    # Remove NoneType values from the list
    return [x for x in lst if x is not None]