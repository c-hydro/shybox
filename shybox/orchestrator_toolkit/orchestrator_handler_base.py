
from shybox.generic_toolkit.lib_utils_string import get_filename_components
from shybox.generic_toolkit.lib_utils_time import convert_time_format
from shybox.orchestrator_toolkit.lib_orchestrator_utils import PROCESSES
from shybox.orchestrator_toolkit.lib_orchestrator_process import ProcessorContainer

from shybox.dataset_toolkit.dataset_handler_mem import DataMem
from shybox.dataset_toolkit.dataset_handler_local import DataLocal

from copy import deepcopy

import datetime as dt
from typing import Optional
from typing import Iterable
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
                 data_out: (DataLocal, dict) = None,
                 options: dict = None, data_map: dict = None) -> None:
        
        self.data_in = data_in
        self.data_out = data_out
        self.data_map = data_map

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

        self.memory_active = True

    @classmethod
    def multi_time(cls, data_package_in: (dict, list),
                   data_package_out: (DataLocal, dict, list) = None, data_ref: DataLocal = None,
                   configuration: dict = None) -> 'Orchestrator':

        return cls.multi_tile(
            data_package_in=data_package_in, data_package_out=data_package_out,
            data_ref=data_ref, configuration=configuration)

    @classmethod
    def multi_tile(cls, data_package_in: (dict, list),
                   data_package_out: (DataLocal, dict, list) = None, data_ref: DataLocal = None,
                   configuration: dict = None) -> 'Orchestrator':

        workflow_fx = configuration.get('process_list', [])
        workflow_options = configuration.get('options', [])

        if isinstance(data_package_in, list):
            data_collections_in = {}

            for data_id, data_obj in enumerate(data_package_in):
                var_package = data_obj.file_variable

                if not isinstance(var_package, list):
                    var_package = [var_package]

                for var_id, var_name in enumerate(var_package):
                    if var_name not in data_collections_in:
                        data_collections_in[var_name] = {}
                        data_collections_in[var_name] = [data_obj]
                    else:
                        data_collections_in[var_name].append(data_obj)
        else:
            data_collections_in = data_package_in

        assert data_collections_in.keys() == workflow_fx.keys(), \
            'Data collections and workflow functions must have the same keys.'

        if isinstance(data_package_out, list):
            data_collections_out = {}

            for data_id, data_obj in enumerate(data_package_out):
                var_package = data_obj.file_variable

                if not isinstance(var_package, list):
                    var_package = [var_package]

                for var_id, var_name in enumerate(var_package):
                    if var_name not in data_collections_out:
                        data_collections_out[var_name] = {}
                        data_collections_out[var_name] = [data_obj]
                    else:
                        data_collections_out[var_name].append(data_obj)
        else:
            data_collections_out = data_package_out

        assert data_collections_out.keys() == workflow_fx.keys(), \
            'Data collections and workflow functions must have the same keys.'

        # method to remap variable tags, in and out
        workflow_map = mapper(data_collections_in, data_collections_out)

        # class to create workflow based using the orchastrator
        workflow_common = OrchestratorHandler(data_in=data_collections_in,
                                              data_out=data_collections_out,
                                              options=workflow_options,
                                              data_map=workflow_map)

        # iterate over the defined process(es)
        for var_tag in list(data_collections_in.keys()):

            process_fx_var = deepcopy(workflow_fx[var_tag])
            process_fx_map = deepcopy(workflow_map[var_tag])
            for process_fx_tmp in process_fx_var:

                process_fx_name = process_fx_tmp.pop('function')
                process_fx_obj = PROCESSES[process_fx_name]

                process_fx_args = {**process_fx_tmp, **{'variable': var_tag, 'map': process_fx_map}}
                workflow_common.add_process(process_fx_obj, ref=data_ref, **process_fx_args)

        return workflow_common

    @classmethod
    def multi_variable(cls, data_package_in: (dict, list), data_package_out: DataLocal = None, data_ref: DataLocal = None,
                       configuration: dict = None) -> 'Orchestrator':

        workflow_fx = configuration.get('process_list', [])
        workflow_options = configuration.get('options', []) # derivare il dizionario come fatto per options per usare ignore_case=True

        if not isinstance(data_package_out, list):
            data_package_out = [data_package_out]

        if isinstance(data_package_in, list):
            data_collections_in = {}
            for data_id, data_obj in enumerate(data_package_in):
                data_name = None
                if hasattr(data_obj, 'file_variable'):
                    data_name = data_obj.file_variable
                if data_name is None:
                    data_name = 'default_{:}'.format(int(data_id))
                data_collections_in[data_name] = data_obj
        else:
            data_collections_in = data_package_in

        assert data_collections_in.keys() == workflow_fx.keys(), \
            'Data collections and workflow functions must have the same keys.'

        if isinstance(data_package_out, list):
            data_collections_out = {}

            for data_id, data_obj in enumerate(data_package_out):
                var_package = data_obj.file_variable

                if not isinstance(var_package, list):
                    var_package = [var_package]

                for var_id, var_name in enumerate(var_package):
                    if var_name not in data_collections_out:
                        data_collections_out[var_name] = {}
                        data_collections_out[var_name] = [data_obj]
                    else:
                        data_collections_out[var_name].append(data_obj)
        else:
            data_collections_out = data_package_out

        #assert data_collections_out.keys() == workflow_fx.keys(), \
        #    'Data collections and workflow functions must have the same keys.'

        # method to remap variable tags, in and out
        workflow_map = mapper(data_collections_in, data_collections_out)

        workflow_common = OrchestratorHandler(data_in=data_collections_in, data_out=data_collections_out,
                                              options=workflow_options,
                                              data_map=workflow_map)

        for var_tag in list(data_collections_in.keys()):

            process_fx_var = deepcopy(workflow_fx[var_tag])
            for process_fx_tmp in process_fx_var:

                process_fx_name = process_fx_tmp.pop('function')
                process_fx_obj = PROCESSES[process_fx_name]

                process_fx_args = {**process_fx_tmp, **{'variable': var_tag}}
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

            path_obj = get_filename_components(path_in)
            name_base, ext_base = path_obj['base_name'], path_obj['ext_name']

            if self.has_variables:

                variable = kwargs['variable']
                save_base = f'{name_base}_{variable}'

                if has_times:
                    path_out = f'{save_base}_{fx_name}_{format_times}.{ext_out}'
                    file_history = f'{file_name_in}_{variable}_{fx_name}_{format_times}'
                else:
                    path_out = f'{save_base}_{fx_name}.{ext_out}'
                    file_history = f'{file_name_in}_{variable}_{fx_name}'

            else:

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
                raise RuntimeError('Input data must be DataLocal or dictionary of DataLocal instance.')
        else:
            process_previous = self.processes[-1]
            this_input = process_previous.out_obj

        if isinstance(this_input, list):
            tmp_input = this_input[-1]
        else:
            tmp_input = this_input

        this_output = self.make_output(tmp_input, process_output, function, **kwargs)
        this_process = ProcessorContainer(
            function = function,
            in_obj = this_input, in_opts=self.options,
            args = kwargs,
            out_obj = this_output, out_opts=self.options)

        if this_process.break_point:
            self.break_points.append(len(self.processes))

        self.processes.append(this_process)

    def run(self, time: (pd.Timestamp, str, pd.date_range), **kwargs) -> None:

        # group process by variable
        proc_group = group_process(self.processes)

        # manage process execution and process output
        if len(self.processes) == 0:
            raise ValueError('No processes have been added to the workflow.')
        elif isinstance(self.processes[-1].out_obj, DataMem) or \
            (isinstance(self.processes[-1].out_obj, DataLocal) and hasattr(self, 'tmp_dir') and
             self.tmp_dir in self.processes[-1].out_obj.dir_name):
            if self.data_out is not None:

                # get the output element(s)
                proc_elements = list(self.data_out.values())
                # check the output element(s)
                proc_bucket = []
                for proc_obj in proc_elements:
                    if isinstance(proc_obj, list):
                        proc_obj = proc_obj[0]
                    if proc_obj not in proc_bucket:
                        proc_bucket.append(proc_obj)

                if len(proc_bucket) == 1:

                    self.processes[-1].out_obj = proc_bucket[0].copy()
                    self.processes[-1].dump_state = True

                elif len(proc_bucket) > 1:

                    for proc_key, proc_obj in proc_group.items():

                        proc_out = self.data_out[proc_key].copy()[0]

                        proc_last = proc_obj[-1]
                        proc_idx = self.processes.index(proc_last)

                        self.processes[proc_idx].out_obj = proc_out
                        self.processes[proc_idx].dump_state = True

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

        group_type = None
        if 'group' in kwargs:
            group_type = kwargs['group']
            if group_type == 'by_time':
                time_steps = [time_steps]
                self.memory_active = False

        for ts in time_steps:
            self.run_single_ts(time=ts, **kwargs)
        return None
    
    def run_single_ts(self, time: (pd.Timestamp, str, pd.date_range), **kwargs) -> None:
        
        if isinstance(time, str):
            time = convert_time_format(time, 'str_to_stamp')
        elif isinstance(time, pd.DatetimeIndex):
            tmp = [convert_time_format(ts, 'str_to_stamp') for ts in time]
            time = deepcopy(tmp)
        else:
            raise ValueError('Time must be a string or a DatetimeIndex.')

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

        # return if no processes
        if len(processes) == 0:
            return

        # get the data map
        data_map = self.data_map

        # group process by variable
        proc_group = group_process(processes)
        # execute process
        proc_memory, proc_ws = None, {}
        for proc_var_key, proc_list in proc_group.items():

            proc_map = data_map.get(proc_var_key, {})

            proc_result, proc_return, proc_var_name = None, [], None
            for proc_id, proc_name in enumerate(proc_list):

                tmp_var_name = None
                if proc_result is not None:
                    tmp_var_name = proc_result.name

                kwargs['id'] = proc_id
                kwargs['variable'] = tmp_var_name
                kwargs['collections'] = proc_ws
                kwargs['map_in'] = proc_map['in']
                kwargs['map_out'] = proc_map['out']
                kwargs['key'] = proc_var_key
                kwargs['memory_active'] = self.memory_active

                if proc_memory is not None:
                    kwargs['memory'] = {}
                    if tmp_var_name not in kwargs['memory']:
                        kwargs['memory'][tmp_var_name] = proc_memory

                proc_result, proc_memory = proc_name.run(time, **kwargs)

                if isinstance(proc_result, xr.DataArray):
                    proc_var_name = proc_result.name
                else:
                    raise ValueError('Process output must be a DataArray.')

                proc_return.append(proc_result)

            proc_ws[proc_var_name] = proc_return[-1]


def mapper(data_collections_in: dict, data_collections_out: dict) -> dict:
    var_mapper = {}
    for (key_in, obj_in), (key_out, obj_out) in zip(
            data_collections_in.items(), data_collections_out.items()):
        if key_in == key_out:
            var_mapper[key_in] = {}

            if not isinstance(obj_in, list):
                obj_in = [obj_in]
            if not isinstance(obj_out, list):
                obj_out = [obj_out]

            for partial_in in obj_in:
                file_vars = partial_in.file_variable
                tmpl_vars = partial_in.file_template['vars_data']

                if not isinstance(file_vars, list):
                    file_vars = [file_vars]

                for var_map, (var_in, var_out) in zip(file_vars, tmpl_vars.items()):
                    if var_map in list(var_mapper.keys()):
                        var_mapper[key_in].update({'in': {var_in: var_out}})

                for partial_out in obj_out:
                    file_vars = partial_out.file_variable
                    tmpl_vars = partial_out.file_template['vars_data']

                    if not isinstance(file_vars, list):
                        file_vars = [file_vars]

                    for var_map, (var_start, var_end) in zip(file_vars, tmpl_vars.items()):
                        if var_map in list(var_mapper.keys()):
                            var_mapper[key_in].update({'out': {var_start: var_end}})
    return var_mapper

def remove_none(lst):
    # Remove NoneType values from the list
    return [x for x in lst if x is not None]

def group_process(proc_list):

    proc_group = {}
    for proc_obj in proc_list:
        proc_var = proc_obj.variable

        if proc_var not in proc_group:
            proc_group[proc_var] = [proc_obj]
        else:
            tmp_obj = proc_group[proc_var]
            tmp_obj.append(proc_obj)
            proc_group[proc_var] = tmp_obj

    return proc_group