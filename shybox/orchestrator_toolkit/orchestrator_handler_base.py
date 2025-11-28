"""
Class Features

Name:          orchestrator_handler_base
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20251104'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
from __future__ import annotations
import warnings

from contextlib import contextmanager
from typing import Any, Dict, List, Mapping, Optional, Tuple, Union
from copy import deepcopy
from collections import defaultdict
from collections.abc import Mapping as AbcMapping

import datetime as dt
import tempfile
import os
import shutil
import pandas as pd
import xarray as xr

from shybox.generic_toolkit.lib_utils_string import get_filename_components
from shybox.generic_toolkit.lib_utils_time import convert_time_format, normalize_to_datetime_index
from shybox.generic_toolkit.lib_utils_tmp import ensure_folder_tmp
from shybox.orchestrator_toolkit.lib_orchestrator_utils import PROCESSES
from shybox.orchestrator_toolkit.lib_orchestrator_process import ProcessorContainer

from shybox.dataset_toolkit.dataset_handler_mem import DataMem
from shybox.dataset_toolkit.dataset_handler_local import DataLocal
from shybox.logging_toolkit.logging_handler import LoggingManager
from shybox.logging_toolkit.lib_logging_utils import with_logger
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# class orchestrator handler
class OrchestratorHandler:

    default_options = {
        'intermediate_output'   : 'Mem', # 'Mem' or 'Tmp'
        'break_on_missing_tiles': False,
        'tmp_dir'               : None
    }

    def __init__(self,
                 data_in: (DataLocal, dict),
                 data_out: (DataLocal, dict) = None,
                 deps_in: (DataLocal, dict) = None, deps_out: (DataLocal, dict) = None,
                 args_in: dict = None, args_out: dict = None,
                 options: (dict, None) = None, mapper: MapperHandler = None,
                 logger: LoggingManager = None) -> None:

        self.logger = logger or LoggingManager(name="Orchestrator")

        self.data_in = data_in
        self.data_out = data_out

        self.deps_in = deps_in
        self.deps_out = deps_out

        self.args_in = args_in
        self.args_out = args_out

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

        # mapper object to organize variables
        self.mapper = mapper

    @classmethod
    def multi_time(cls,
                   data_package_in: (dict, list), data_package_out: (DataLocal, dict, list) = None, data_ref: DataLocal = None,
                   priority: list = None,
                   configuration: dict = None, logger: LoggingManager = None ) -> 'Orchestrator':

        return cls.multi_tile(
            data_package_in=data_package_in, data_package_out=data_package_out,
            data_ref=data_ref, configuration=configuration)

    @classmethod
    def multi_tile(cls,
                   data_package_in: (dict, list), data_package_out: (DataLocal, dict, list) = None, data_ref: DataLocal = None,
                   priority: list = None,
                   configuration: dict = None, logger: LoggingManager = None ) -> 'Orchestrator':

        # info orchestrator start
        logger.info_up(f'Organize orchestrator [multi-tile] ...', tag='ow')

        # get workflow functions and options
        workflow_fx = configuration.get('process_list', None)
        workflow_options = configuration.get('options', [])

        # check workflow functions
        if workflow_fx is None:
            logger.error('Workflow functions must be provided in the configuration.')
            raise RuntimeError('Workflow functions must be provided in the configuration.')

        # ensure data collections in
        if isinstance(data_package_in, list):

            # iterate over data package in
            fx_collections, data_collections_in = {}, {}
            for data_id, data_obj in enumerate(data_package_in):

                file_variable = data_obj.file_variable
                file_namespace = data_obj.file_namespace

                if not isinstance(file_variable, list):
                    file_variable = [file_variable]

                # build pairs tag and process
                pairs_tag_str, pairs_tag_tuple, pairs_process, pairs_info = build_pairs_and_process(
                    workflow_fx, file_variable, file_namespace)

                for var_id, (var_tag, var_process) in enumerate(zip(pairs_tag_str, pairs_process)):

                    if var_tag not in fx_collections:
                        fx_collections[var_tag] = {}
                        fx_collections[var_tag] = [var_process]
                    else:
                        fx_collections[var_tag].append(var_process)

                    if var_tag not in data_collections_in:
                        data_collections_in[var_tag] = {}
                        data_collections_in[var_tag] = [data_obj]
                    else:
                        data_collections_in[var_tag].append(data_obj)

        else:
            logger.error('Data package in must be a list of DataLocal instances.')
            raise NotImplementedError('Case not implemented yet')

        # check if data collections in and workflow have the same keys
        assert data_collections_in.keys() == fx_collections.keys(), \
            'Data collections and workflow functions must have the same keys.'

        # ensure data collections out
        if isinstance(data_package_out, list):

            # iterate over data package out
            fx_collections, data_collections_out = {}, {}

            for data_id, data_obj in enumerate(data_package_out):

                file_variable = data_obj.file_variable
                file_namespace = data_obj.file_namespace

                if not isinstance(file_variable, list):
                    file_variable = [file_variable]

                # build pairs tag and process
                pairs_tag_str, pairs_tag_tuple, pairs_process, pairs_info = build_pairs_and_process(
                    workflow_fx, file_variable, file_namespace)

                for var_id, (var_tag, var_process) in enumerate(zip(pairs_tag_str, pairs_process)):

                    if var_tag not in fx_collections:
                        fx_collections[var_tag] = {}
                        fx_collections[var_tag] = [var_process]
                    else:
                        fx_collections[var_tag].append(var_process)

                    if var_tag not in data_collections_out:
                        data_collections_out[var_tag] = {}
                        data_collections_out[var_tag] = [data_obj]
                    else:
                        data_collections_out[var_tag].append(data_obj)
        else:
            logger.error('Data package out must be a list of DataLocal instances.')
            raise NotImplementedError('Case not implemented yet')

        # check if data collections and workflow have the same keys
        assert data_collections_out.keys() == fx_collections.keys(), \
            'Data collections out and workflow functions must have the same keys.'

        # method to remap variable tags, in and out
        workflow_mapper = MapperHandler(data_collections_in, data_collections_out)

        # class to create workflow based using the orchestrator
        workflow_common = OrchestratorHandler(
            data_in=data_collections_in, data_out=data_collections_out,
            deps_in=None, deps_out=None, args_in=None, args_out=None,
            options=workflow_options,
            mapper=workflow_mapper, logger=logger)

        # iterate over the defined input variables and their process(es)
        for workflow_row in workflow_mapper.get_rows_by_priority(priority_vars=priority):

            # get workflow information by tag
            workflow_tag, workflow_name = workflow_row['tag'], workflow_row['workflow']

            # info workflow start
            logger.info_up(f'Configure workflow "{workflow_name}" ... ', tag='ow')

            # iterate over the defined process(es)
            process_fx_var = deepcopy(workflow_fx[workflow_tag])
            for process_fx_id, process_fx_tmp in enumerate(process_fx_var):

                # get process name and object
                process_fx_name = process_fx_tmp.pop('function')
                process_fx_obj = PROCESSES[process_fx_name]

                # define process arguments
                process_fx_args = {**process_fx_tmp, **workflow_row}
                # add the process to the workflow
                workflow_common.add_process(process_fx_obj, ref=data_ref, **process_fx_args)

            # info workflow end
            logger.info_down(f'Configure workflow "{workflow_name}" ... DONE', tag='ow')

        # info orchestrator end
        logger.info_down(f'Organize orchestrator [multi-tile] ... DONE', tag='ow')

        return workflow_common

    @classmethod
    def multi_variable(cls,
                       data_package_in: (dict, list), data_package_out: DataLocal = None, data_ref: DataLocal = None,
                       priority: list = None,
                       configuration: dict = None, logger: LoggingManager = None ) -> 'Orchestrator':

        # info orchestrator start
        logger.info_up(f'Organize orchestrator [multi-variable] ...', tag='ow')

        # get workflow functions and options
        workflow_fx = configuration.get('process_list', [])
        workflow_options = configuration.get('options', [])

        # check workflow functions
        if workflow_fx is None:
            logger.error('Workflow functions must be provided in the configuration.')
            raise RuntimeError('Workflow functions must be provided in the configuration.')

        # normalize input/output data packages
        if not isinstance(data_package_in, list):
            data_package_in = [data_package_in]
        if not isinstance(data_package_out, list):
            data_package_out = [data_package_out]

        # ensure data collections in
        fx_collections = {}
        if isinstance(data_package_in, list):

            # iterate over data package in
            data_collections_in = {}
            for data_id, data_obj in enumerate(data_package_in):

                file_variable = data_obj.file_variable
                file_namespace = data_obj.file_namespace

                if not isinstance(file_variable, list):
                    file_variable = [file_variable]
                if not isinstance(file_namespace, list):
                    file_namespace = [file_namespace]

                # build pairs tag and process
                pairs_tag_str, pairs_tag_tuple, pairs_process, pairs_info = build_pairs_and_process(
                    workflow_fx, file_variable, file_namespace)

                # iterate over variable tags and processes
                for var_id, (var_tag, var_process) in enumerate(zip(pairs_tag_str, pairs_process)):

                    if var_tag not in fx_collections:
                        fx_collections[var_tag] = {}
                        fx_collections[var_tag] = [var_process]
                    else:
                        fx_name = extract_tag_value(fx_collections[var_tag], 'function')
                        for tmp_process in var_process:
                            tmp_name = tmp_process['function']
                            if tmp_name not in fx_name:
                                fx_collections[var_tag].append(tmp_process)

                    if var_tag not in data_collections_in:
                        data_collections_in[var_tag] = {}
                        data_collections_in[var_tag] = [data_obj]
                    else:
                        data_collections_in[var_tag].append(data_obj)

        else:
            logger.error('Data package in must be a list of DataLocal instances.')
            raise NotImplementedError('Case not implemented yet')

        # check if data collections in and workflow have the same keys
        check_variables_in = ensure_variables(data_collections_in, fx_collections, mode='strict')
        if not check_variables_in:
            logger.error(
                'Input data collections do not cover the workflow variables as defined by the check rule.')
            raise RuntimeError(
                'Input data collections do not cover the workflow variables as defined by the check rule.')

        # ensure data collections out
        if isinstance(data_package_out, list):

            # iterate over data package out
            data_collections_out = {}
            for data_id, data_obj in enumerate(data_package_out):

                file_variable = data_obj.file_variable
                file_namespace = data_obj.file_namespace

                if not isinstance(file_variable, list):
                    file_variable = [file_variable]
                if not isinstance(file_namespace, list):
                    file_namespace = [file_namespace]

                for step_variable, step_namespace in zip(file_variable, file_namespace):

                    # build pairs tag and process
                    pairs_tag_str, pairs_tag_tuple, pairs_process, pairs_info = build_pairs_and_process(
                        workflow_fx, step_variable, step_namespace)

                    # iterate over variable tags and processes
                    for var_id, (var_tag, var_process) in enumerate(zip(pairs_tag_str, pairs_process)):

                        if var_tag not in fx_collections:
                            fx_collections[var_tag] = {}
                            fx_collections[var_tag] = [var_process]
                        else:

                            fx_name = extract_tag_value(fx_collections[var_tag], 'function')
                            for tmp_process in var_process:
                                tmp_name = tmp_process['function']
                                if tmp_name not in fx_name:
                                    fx_collections[var_tag].append(tmp_process)

                        if var_tag not in data_collections_out:
                            data_collections_out[var_tag] = {}
                            data_collections_out[var_tag] = [data_obj]
                        else:
                            data_collections_out[var_tag].append(data_obj)
        else:
            logger.error('Data package out must be a list of DataLocal instances.')
            raise NotImplementedError('Case not implemented yet')

        # check if data collections and workflow have the same keys
        check_variables_out = ensure_variables(data_collections_out, fx_collections, mode='lazy')
        if not check_variables_out:
            logger.error(
                'Output data collections do not cover the workflow variables as defined by the check rule.')
            raise RuntimeError(
                'Output data collections do not cover the workflow variables as defined by the check rule.')

        # method to remap variable tags, in and out
        workflow_mapper = MapperHandler(data_collections_in, data_collections_out)

        # organize deps collections in
        deps_collections_in, args_collections_in = {}, {}
        for data_key, data_config in data_collections_in.items():

            # normalize: always iterate a list, remember if originally a seq
            configs, is_sequence = as_list(data_config)

            deps_list, args_list = [], []
            for idx, cfg in enumerate(configs):
                # your original logic, but on `cfg`
                data_deps = getattr(cfg, 'file_deps', [])
                args_deps = getattr(cfg, 'args_deps', [])

                deps_list.append(remove_none(data_deps))
                args_list.append(args_deps)

            # if original was a single object → store single element
            if is_sequence:
                deps_collections_in[data_key] = deps_list[0]
                args_collections_in[data_key] = args_list[0]
            else:
                deps_collections_in[data_key] = deps_list
                args_collections_in[data_key] = args_list

        # organize deps collections out
        # organize deps collections in
        deps_collections_out, args_collections_out = {}, {}
        for data_key, data_config in data_collections_out.items():

            # normalize: always iterate a list, remember if originally a seq
            configs, is_sequence = as_list(data_config)

            deps_list, args_list = [], []
            for idx, cfg in enumerate(configs):
                # your original logic, but on `cfg`
                data_deps = getattr(cfg, 'file_deps', [])
                args_deps = getattr(cfg, 'args_deps', [])

                deps_list.append(remove_none(data_deps))
                args_list.append(args_deps)

            # if original was a single object → store single element
            if is_sequence:
                deps_collections_out[data_key] = deps_list[0]
                args_collections_out[data_key] = args_list[0]
            else:
                deps_collections_out[data_key] = deps_list
                args_collections_out[data_key] = args_list

        # class to create workflow based using the orchestrator
        workflow_common = OrchestratorHandler(
            data_in=data_collections_in, data_out=data_collections_out,
            deps_in=deps_collections_in, deps_out=deps_collections_out,
            args_in=args_collections_in, args_out=args_collections_out,
            options=workflow_options,
            mapper=workflow_mapper, logger=logger)

        # iterate over the defined input variables and their process(es)
        workflow_configuration = workflow_mapper.get_rows_by_priority(priority_vars=priority, field='tag')
        for workflow_row in workflow_configuration:

            # get workflow information by tag
            workflow_tag = workflow_row['tag']
            workflow_name = workflow_row['workflow']

            # info workflow start
            logger.info_up(f'Configure workflow "{workflow_name}" ... ', tag='ow')

            # iterate over the defined process(es)
            process_fx_var = deepcopy(workflow_fx[workflow_tag])
            for process_fx_id, process_fx_tmp in enumerate(process_fx_var):

                # get process name and object
                process_fx_name = process_fx_tmp.pop('function')
                process_fx_obj = PROCESSES[process_fx_name]

                # define process arguments
                process_fx_args = {**process_fx_tmp, **workflow_row}
                # add the process to the workflow
                workflow_common.add_process(process_fx_obj, ref=data_ref, **process_fx_args)

            # info workflow end
            logger.info_down(f'Configure workflow "{workflow_name}" ... DONE', tag='ow')

        # info orchestrator end
        logger.info_down(f'Organize orchestrator [multi-variable] ... DONE', tag='ow')

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

    # create the output object
    def make_output(self, in_obj: DataLocal, out_obj: DataLocal = None,
                    function = None, message: bool = True, **kwargs) -> DataLocal:

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
            if path_in is not None:
                ext_in = os.path.splitext(path_in)[1][1:]
            else:
                ext_in = in_obj.file_format
            ext_out = function.__getattribute__('output_ext') or ext_in

            path_obj = get_filename_components(path_in)
            name_base, ext_base = path_obj['base_name'], path_obj['ext_name']

            if self.has_variables:

                variable = kwargs['tag']
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

        else:
            # ensure a temporary output path (if no function is provided)
            path_out = ensure_folder_tmp()

        # use the output pattern provided by the user
        if out_obj is None:
            path_out = deepcopy(path_out)
        elif isinstance(out_obj, dict):
            path_out = out_obj.get('loc_pattern', path_out)
        else:
            self.logger.error('Output must be a Dataset or a dictionary.')
            raise ValueError('Output must be a Dataset or a dictionary.')

        # manage and intermediate output in geotiff format
        output_type = self.options['intermediate_output']
        if output_type == 'Mem':

            out_obj = DataMem(loc_pattern=path_out)

        elif output_type == 'Tmp':

            # prepare file name (temporary)
            file_name_tmp = os.path.basename(path_out)

            # ensure variable template and file variable attributes
            if 'variable_template' not in kwargs:
                kwargs['variable_template'] = {}
            kwargs['variable_template']['vars_data'] = {kwargs['workflow']: kwargs['workflow']}
            if 'file_variable' not in kwargs:
                kwargs['file_variable'] = kwargs['workflow']

            # create the temporary output object
            out_obj = DataLocal(
                path=self.tmp_dir, file_name=file_name_tmp, message=message, **kwargs)

        else:
            self.logger.error('Orchestrator output type must be "Mem" or "Tmp".')
            raise ValueError('Orchestrator output type must be "Mem" or "Tmp"')

        # store output file history
        out_obj.file_history = file_history

        return out_obj

    def add_process(self, function, process_output: (DataLocal, xr.Dataset, dict) = None, **kwargs) -> None:

        # get process var tag
        if 'workflow' in kwargs:
            process_wf = kwargs['workflow']
        else:
            raise RuntimeError('Process variable "workflow" must be provided in the process arguments.')

        # get process map
        process_map = self.mapper.get_pairs(name=process_wf, type='workflow') if self.mapper is not None else {}
        # get process variable in/out
        process_var_in, process_var_out = process_map['in'], process_map['out']
        process_var_tag, process_var_workflow = process_map['tag'], process_map['workflow']
        process_var_reference = ':'.join([process_var_tag, process_var_workflow])
        # update kwargs with process map
        kwargs = {**kwargs, **process_map}

        # ensure the state of the process (initial or not)
        process_obj = self.processes
        process_previous = self.processes[-1] if len(process_obj) > 0 else None
        if (process_previous is not None) and (process_wf not in process_previous.workflow):
            process_init = True
        elif len(process_obj) == 0:
            process_init = True
        else:
            process_init = False

        # create the process input
        if process_init:
            if isinstance(self.data_in, dict):
                if process_var_reference in list(self.data_in.keys()):
                    this_input = self.data_in[process_var_reference]
                else:
                    self.logger.error(
                        f'Input data for variable "{process_var_reference}" not found in the input data collection.'
                    )
                    raise RuntimeError(
                        f'Input data for variable "{process_var_reference}" not found in the input data collection.')
            elif isinstance(self.data_in, DataLocal):
                this_input = self.data_in
            else:
                self.logger.error('Input data must be DataLocal or dictionary of DataLocal instance.')
                raise RuntimeError('Input data must be DataLocal or dictionary of DataLocal instance.')

            if self.deps_in is not None:
                if isinstance(self.deps_in, dict):
                    deps_input = self.deps_in[process_var_reference]
                else:
                    self.logger.error('Input deps must be dictionary instance.')
                    raise RuntimeError('Input deps must be dictionary instance.')
            else:
                deps_input = None
        else:
            deps_input = process_previous.out_deps
            process_previous = self.processes[-1]
            this_input = process_previous.out_obj

        # create the temporary input (for making output)
        if isinstance(this_input, list):
            tmp_input = this_input[-1]
        else:
            tmp_input = this_input

        # create the process output
        this_output = self.make_output(
            tmp_input, process_output, function, message=False, **kwargs)

        # create the process container
        this_process = ProcessorContainer(
            function = function,
            in_obj = this_input, in_opts=self.options,
            in_deps=deps_input, out_deps=None,
            args = kwargs,
            out_obj = this_output, out_opts=self.options, logger=self.logger, tag=process_var_reference,)

        # check if break point is required
        if this_process.break_point:
            self.break_points.append(len(self.processes))

        # append this process to the list of process
        self.processes.append(this_process)

    # method to run the workflow
    def run(self, time: (pd.Timestamp, str, pd.date_range), **kwargs) -> None:

        # info orchestrator start
        self.logger.info_up('Run orchestrator ...')

        # group process by variable
        proc_group = group_process(self.processes, proc_tag='reference')

        # manage process execution and process output
        if len(self.processes) == 0:

            # exit no processes declared
            self.logger.error('No processes have been added to the workflow.')
            raise ValueError('No processes have been added to the workflow.')

        elif isinstance(self.processes[-1].out_obj, DataMem) or \
            (isinstance(self.processes[-1].out_obj, DataLocal) and hasattr(self, 'tmp_dir') and
             self.tmp_dir in self.processes[-1].out_obj.dir_name):

            # check the output datasets
            if self.data_out is not None:

                # get the output element(s)
                proc_elements = list(self.data_out.values())
                # check the output element(s)
                proc_bucket = []
                for proc_obj in proc_elements:

                    if isinstance(proc_obj, list):
                        proc_obj = proc_obj[0]

                    if proc_obj not in proc_bucket:
                        #proc_obj.logger = self.logger.compare(proc_obj.logger)
                        proc_bucket.append(proc_obj)

                # assign the output element(s)
                if len(proc_bucket) == 1:

                    self.processes[-1].out_obj = proc_bucket[0].copy()
                    self.processes[-1].dump_state = True

                elif len(proc_bucket) > 1:

                    # iterate over all variable groups
                    for proc_key, proc_obj in proc_group.items():

                        proc_out = self.data_out[proc_key].copy()[0]

                        proc_last = proc_obj[-1]
                        proc_idx = self.processes.index(proc_last)

                        self.processes[proc_idx].out_obj = proc_out
                        self.processes[proc_idx].dump_state = True

            else:
                # exit if no output dataset defined
                self.logger.error('No output dataset has been set.')
                raise ValueError('No output dataset has been set.')

        # normalize time steps
        time_steps = normalize_to_datetime_index(time)
        # check time steps
        if len(time_steps) == 0:
            return None

        # check group tag in kwargs (by_time disables memory) --> da controllare con merge time
        if 'group' in kwargs:
            group_type = kwargs['group']
            if group_type == 'by_time':
                time_steps = [time_steps]
                self.memory_active = False

        # iterate over time steps
        for ts in time_steps:

            # info time start
            self.logger.info_up(f'Time "{ts}" ...')
            # run time step
            self.run_single_ts(time=ts, **kwargs)
            # info time end
            self.logger.info_down(f'Time "{ts}" ... DONE')

        # info orchestrator end
        self.logger.info_down('Run orchestrator ... DONE')

        return None

    # method to run single time step
    def run_single_ts(self, time: (pd.Timestamp, str, pd.date_range), **kwargs) -> None:

        # time formatting
        if isinstance(time, str):
            time = convert_time_format(time, 'str_to_stamp')
        elif isinstance(time, pd.DatetimeIndex):
            tmp = [convert_time_format(ts, 'str_to_stamp') for ts in time]
            time = deepcopy(tmp)
        elif isinstance(time, pd.Timestamp):
            pass
        else:
            self.logger.error('Time must be a string or a DatetimeIndex.')
            raise ValueError('Time must be a string or a DatetimeIndex.')

        # run all processes if no breakpoints
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

    # method to run processes (internal use)
    def _run_processes(self, processes, time: dt.datetime, **kwargs) -> None:

        # return if no processes
        if not processes: return None

        # group process by variable
        proc_group = group_process(processes)

        # iterate over all variable groups
        proc_memory, proc_ws = None, {}
        for proc_var, proc_list in proc_group.items():

            # VARIABLE BLOCK START
            self.logger.info_up(f'Variable "{proc_var}" ...')

            # get the variable mapping
            proc_vars_map = self.mapper.get_pairs(name=proc_var, type='reference')

            # iterate over all processes for this variable
            proc_result, proc_return, proc_wf_current = None, [], None
            proc_current, proc_previous = None, None
            for proc_id, proc_obj in enumerate(proc_list):

                # PROCESS BLOCK START
                self.logger.info_up(f'Process "{proc_obj.fx_name}" ...')

                # check process dump state
                proc_dump = proc_obj.dump_state

                try:
                    # if previous process returned None → skip
                    if proc_id > 0 and proc_return[proc_id - 1] is None:
                        self.logger.warning(
                            f'Process "{proc_obj.fx_name}" ... SKIPPED. Previous process was NoneType'
                        )
                        proc_return.append(None)
                        continue

                    # previous workflow name
                    proc_previous = proc_current
                    proc_wf_previous = proc_result.name if proc_result is not None else None

                    # organize kwargs
                    local_kwargs = dict(kwargs)
                    local_kwargs.update({
                        'id': proc_id,
                        'collections': proc_ws,
                        #'workflow': proc_wf_previous,
                        'workflow': proc_previous,
                        'memory_active': self.memory_active,
                        **proc_vars_map
                    })

                    # inject memory if available
                    if proc_memory is not None:
                        local_kwargs.setdefault('memory', {})
                        #if proc_wf_previous not in local_kwargs['memory']:
                        if proc_previous not in local_kwargs['memory']:
                            #local_kwargs['memory'][proc_wf_previous] = proc_memory
                            local_kwargs['memory'][proc_previous] = proc_memory

                    # run process
                    proc_result, proc_memory = proc_obj.run(time, **local_kwargs)
                    proc_current = proc_var

                    # determine current workflow name
                    if proc_result is not None:
                        if isinstance(proc_result, xr.DataArray):
                            proc_wf_current = proc_result.name
                        elif isinstance(proc_result, xr.Dataset):
                            proc_wf_current = list(proc_result.data_vars.keys())
                        else:
                            self.logger.error('Process output must be a DataArray.')
                            raise ValueError('Process output must be a DataArray.')
                    else:
                        proc_wf_current = proc_vars_map['workflow']

                    if not proc_dump:

                        # store process result
                        proc_return.append(proc_result)

                        # DETAIL: if skipped / empty
                        if proc_result is None:
                            self.logger.warning(
                                f'Process "{proc_obj.fx_name}" ... SKIPPED. Data not available'
                            )

                        # assign current workflow to workspace
                        proc_ws[proc_current] = proc_return[-1]

                    else:
                        # dump state active the data in memory is cleared (empty dict)
                        proc_ws.pop(proc_current, None)

                finally:
                    # PROCESS BLOCK END
                    self.logger.info_down(f'Process "{proc_obj.fx_name}" ... DONE')

            # VARIABLE BLOCK END
            self.logger.info_down(f'Variable "{proc_var}" ... DONE')

            # Normalize proc_wf_current
            if isinstance(proc_wf_current, list):
                proc_wf_tmp = ":".join(str(wf) for wf in proc_wf_current)
            else:
                proc_wf_tmp = str(proc_wf_current)

            # re-assign current workflow to workspace
            proc_wf_current = proc_wf_tmp
            #proc_ws[proc_wf_current] = proc_return[-1]
            #proc_ws[proc_current] = proc_return[-1]
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to check compatibility between data and fx dicts
@with_logger(var_name='logger_stream')
def ensure_variables(data_collections, fx_collections, mode='strict'):
    # Keys sets
    keys_data = set(data_collections.keys())
    keys_fx = set(fx_collections.keys())

    # Differences
    only_in_data = keys_data - keys_fx
    only_in_fx = keys_fx - keys_data
    common = keys_data & keys_fx

    # Mode checks
    if mode == 'strict':
        logger_stream.info(f"[strict] Keys in BOTH: {sorted(common)}")
        logger_stream.info(f"[strict] Only in DATA: {sorted(only_in_data)}")
        logger_stream.info(f"[strict] Only in FX:  {sorted(only_in_fx)}")

        if keys_data != keys_fx:
            logger_stream.error("Strict mode failed: key mismatch.")
            raise AssertionError("Strict mode failed: key mismatch.")

    elif mode == 'less_from_data':
        logger_stream.info(f"[less_from_out] DATA keys: {sorted(keys_data)}")
        logger_stream.info(f"[less_from_out] FX keys:  {sorted(keys_fx)}")
        logger_stream.info(f"[less_from_out] Missing in FX (problem): {sorted(only_in_data)}")

        if only_in_data:
            logger_stream.error("less_from_out failed: some DATA keys are not in FX.")
            raise AssertionError("less_from_out failed: some DATA keys are not in FX.")

    elif mode == 'less_from_fx':

        logger_stream.info(f"[less_from_fx] FX keys: {sorted(keys_fx)}")
        logger_stream.info(f"[less_from_fx] DATA keys: {sorted(keys_data)}")
        logger_stream.info(f"[less_from_fx] Missing in OUT (problem): {sorted(only_in_fx)}")

        if only_in_fx:
            logger_stream.error("less_from_fx failed: some FX keys are not in DATA.")
            raise AssertionError("less_from_fx failed: some FX keys are not in DATA.")

    elif mode == 'lazy':

        logger_stream.info(f"[lazy] Keys in DATA: {sorted(keys_data)}")
        logger_stream.info(f"[lazy] Keys in FX:  {sorted(keys_fx)}")
        logger_stream.info(f"[lazy] Common keys: {sorted(common)}")

        if not common:
            logger_stream.error("lazy failed: no common keys.")
            raise AssertionError("lazy failed: no common keys.")

    else:
        logger_stream.error(f"Unknown mode '{mode}'")
        raise ValueError(f"Unknown mode '{mode}'")

    return True
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# class mapper handler
class MapperHandler:
    """
    Build a flat mapping between input and output variable templates, and produce
    compact or tag-specific rows. Mirrors the behavior of the procedural
    functions 'mapper_generator' and 'compact'.
    """

    def __init__(self,
                 data_collections_in: Mapping[str, Union[Any, List[Any]]],
                 data_collections_out: Mapping[str, Union[Any, List[Any]]],
                 logger: LoggingManager = None) -> None:

        self.logger = logger or LoggingManager(name="Mapper")
        self._data_in = data_collections_in
        self._data_out = data_collections_out
        self._mapping: Optional[Dict[str, Dict[str, Dict[str, Any]]]] = None

    # Build mapping
    def build_mapping(self) -> Dict[str, Dict[str, Dict[str, Any]]]:
        """Compute and return the flat mapping. Cached after first build."""
        if self._mapping is not None:
            return self._mapping

        result: Dict[str, Dict[str, Dict[str, Any]]] = {}

        keys_in = set(self._data_in.keys())
        keys_out = set(self._data_out.keys())

        # Warn about top-level keys present only on one side
        missing_in = sorted(keys_out - keys_in)
        missing_out = sorted(keys_in - keys_out)
        if missing_out:
            self.logger.warning(f"Keys present only in input: {missing_out}")
        if missing_in:
            self.logger.warning(f"Keys present only in output: {missing_in}")

        shared_keys = keys_in & keys_out

        for key in shared_keys:
            obj_in = self._as_list(self._data_in.get(key))
            obj_out = self._as_list(self._data_out.get(key))

            for side, objs in (('in', obj_in), ('out', obj_out)):
                for idx, partial in enumerate(objs):
                    labels_sorted, items_sorted = self._sorted_labels_and_items(partial, key, side, idx)

                    # Warn if counts differ
                    if len(labels_sorted) != len(items_sorted):
                        self.logger.warning(
                            f"[{key}] {side.upper()} side mismatch: "
                            f"{len(labels_sorted)} labels vs {len(items_sorted)} template items; "
                            f"extra entries will be ignored."
                        )

                    # Pair after sorting and merge into flat result
                    for label, (tpl_key, tpl_val) in zip(labels_sorted, items_sorted):
                        label = str(label)
                        tpl_key = str(tpl_key)
                        if label not in result:
                            result[label] = {'in': {}, 'out': {}}

                        if tpl_key in result[label][side] and result[label][side][tpl_key] != tpl_val:
                            self.logger.warning(
                                f"[{label}] {side.upper()} template key '{tpl_key}' is being overwritten."
                            )
                        result[label][side][tpl_key] = tpl_val

        self._mapping = result
        return result

    # Public helpers
    def compact_rows(self, start_id: int = 1) -> List[Dict[str, Any]]:
        """
        Turn the build_mapping() result into compact rows with keys:
          {'tag', 'in', 'workflow', 'out', 'id'}
        """
        mapping = self.build_mapping()
        rows: List[Dict[str, Any]] = []
        next_id = start_id

        for tag in sorted(mapping.keys(), key=str):
            in_map = mapping[tag].get('in', {}) or {}
            out_map = mapping[tag].get('out', {}) or {}

            for in_key, workflow in sorted(in_map.items(), key=lambda kv: str(kv[0])):
                out_val: Optional[Any] = out_map.get(workflow)
                if out_val is None:
                    self.logger.warning(
                        f"[{tag}] No matching OUT for workflow '{workflow}'. "
                        f"Available OUT keys: {list(out_map.keys())}"
                    )
                rows.append({
                    'tag': str(tag),
                    'in': str(in_key),
                    'workflow': str(workflow),
                    'out': (str(out_val) if out_val is not None else None),
                    'id': next_id,
                    'reference': f"{tag}:{workflow}",
                })
                next_id += 1

        return rows

    def get_rows_by_priority(
            self,
            priority_vars: Optional[List[str]] = None,
            rows: Optional[List[Dict[str, Any]]] = None,
            *,
            sort_others: bool = True,
            start_id: int = 1,
            field: str = "in"  # <-- default field name
    ) -> List[Dict[str, Any]]:

        """Reorder rows so priority variables appear first in the given order,
        using a configurable field name (default='in')."""

        if rows is None:
            rows = self.compact_rows(start_id=start_id)

        if not priority_vars:
            return rows

        priority_vars_str = [str(v) for v in priority_vars]

        priority_part: List[Dict[str, Any]] = []
        others_part: List[Dict[str, Any]] = []

        for row in rows:
            var_name = str(row.get(field, ""))
            (priority_part if var_name in priority_vars_str else others_part).append(row)

        # sort priority rows in the order given
        priority_part.sort(
            key=lambda r: priority_vars_str.index(str(r.get(field, "")))
            if str(r.get(field, "")) in priority_vars_str else len(priority_vars_str)
        )

        # optionally sort remaining rows alphabetically
        if sort_others:
            others_part.sort(key=lambda r: str(r.get(field, "")))

        return priority_part + others_part

    def get_tag_mapping(self, tag: str) -> Dict[str, Dict[str, Any]]:
        """Return the raw mapping for a single tag: {'in': {...}, 'out': {...}}."""
        mapping = self.build_mapping()
        if tag not in mapping:
            self.logger.error(f"Tag '{tag}' not found. Available: {sorted(mapping.keys(), key=str)}")
            raise KeyError(f"Tag '{tag}' not found. Available: {sorted(mapping.keys(), key=str)}")
        return {
            'in': dict(mapping[tag].get('in', {}) or {}),
            'out': dict(mapping[tag].get('out', {}) or {}),
        }

    def get_pairs(self, name: str, type: str = "workflow") -> Union[List[Dict[str, Any]], Dict[str, Any]]:
        """
        Get mapping pairs either by tag or workflow name, resolving directly
        from the current mapping (no helper lookups).

        If type == "tag": returns all rows for that tag.
        If type == "workflow": returns all rows (across tags) whose workflow == name.

        Returns a single dict if only one row, else a list of dicts.
        """
        if type not in ("tag", "workflow", "reference"):
            self.logger.error(f"Invalid type '{type}'. Must be 'tag', 'workflow' or 'reference'.")
            raise ValueError(f"Invalid type '{type}'. Must be 'tag', 'workflow' or 'reference'.")

        mapping = self.build_mapping()
        rows: List[Dict[str, Any]] = []

        if type == "tag":
            if name not in mapping:
                raise ValueError(f"Tag '{name}' not found. Available: {sorted(mapping.keys(), key=str)}")

            tag = name
            in_map = mapping[tag].get('in', {}) or {}
            out_map = mapping[tag].get('out', {}) or {}

            for in_key, wf_name in sorted(in_map.items(), key=lambda kv: str(kv[0])):
                out_val = out_map.get(wf_name)
                if out_val is None:
                    self.logger.warning(
                        f"[{tag}] No matching OUT for workflow '{wf_name}'. "
                        f"Available OUT keys: {list(out_map.keys())}"
                    )
                rows.append({
                    'tag': str(tag),
                    'in': str(in_key),
                    'workflow': str(wf_name),
                    'reference': f'{tag}:{wf_name}',
                    'out': (str(out_val) if out_val is not None else None),
                })

        elif type == "reference":

            # Expect reference in format "tag:workflow"
            if ":" not in name:
                self.logger.error(f"Invalid reference '{name}'. Expected format 'tag:workflow'.")
                raise ValueError(f"Invalid reference '{name}'. Expected format 'tag:workflow'.")

            tag, wf_name = name.split(":", 1)
            mapping = self.build_mapping()

            if tag not in mapping:
                self.logger.error(f"Tag '{tag}' not found. Available: {sorted(mapping.keys(), key=str)}")
                raise ValueError(f"Tag '{tag}' not found. Available: {sorted(mapping.keys(), key=str)}")

            in_map = mapping[tag].get('in', {}) or {}
            out_map = mapping[tag].get('out', {}) or {}

            matched_in_keys = [k for k, v in in_map.items() if v == wf_name]
            if not matched_in_keys:
                self.logger.error(f"No IN entries found for workflow '{wf_name}' under tag '{tag}'.")
                raise ValueError(f"No IN entries found for workflow '{wf_name}' under tag '{tag}'.")

            rows = []
            for in_key in sorted(matched_in_keys, key=str):
                out_val = out_map.get(wf_name)
                if out_val is None:
                    self.logger.warning(
                        f"[{tag}] No matching OUT for workflow '{wf_name}'. "
                        f"Available OUT keys: {list(out_map.keys())}"
                    )
                rows.append({
                    'tag': str(tag),
                    'in': str(in_key),
                    'workflow': str(wf_name),
                    'reference': f'{tag}:{wf_name}',
                    'out': (str(out_val) if out_val is not None else None),
                })

        else:  # type == "workflow"
            target_wf = name
            # scan all tags and collect matching rows
            for tag in sorted(mapping.keys(), key=str):
                in_map = mapping[tag].get('in', {}) or {}
                out_map = mapping[tag].get('out', {}) or {}

                for in_key, wf_name in sorted(in_map.items(), key=lambda kv: (str(tag), str(kv[0]))):
                    if wf_name != target_wf:
                        continue
                    out_val = out_map.get(wf_name)
                    if out_val is None:
                        self.logger.warning(
                            f"[{tag}] No matching OUT for workflow '{wf_name}'. "
                            f"Available OUT keys: {list(out_map.keys())}"
                        )
                    rows.append({
                        'tag': str(tag),
                        'in': str(in_key),
                        'workflow': str(wf_name),
                        'reference': f'{tag}:{wf_name}',
                        'out': (str(out_val) if out_val is not None else None),
                    })

            if not rows:
                self.logger.error(f"No mapping rows found for workflow '{name}'.")
                raise ValueError(f"No mapping rows found for workflow '{name}'.")

        return rows[0] if len(rows) == 1 else rows

    def get_tags_for_workflow(self, workflow: str) -> List[str]:
        """
        Return all tags where the given workflow appears (either as a value in the
        tag's 'in' map or as a key in the tag's 'out' map).
        Raises ValueError if not found anywhere.
        """
        mapping = self.build_mapping()
        tags: List[str] = []
        for tag, tag_map in mapping.items():
            in_map = tag_map.get('in', {}) or {}
            out_map = tag_map.get('out', {}) or {}
            if workflow in in_map.values() or workflow in out_map:
                tags.append(str(tag))
        if not tags:
            self.logger.error(f"No tags found for workflow '{workflow}'.")
            raise ValueError(f"Workflow '{workflow}' not found in any tag.")
        return sorted(set(tags), key=str)

    def get_workflows_for_tag(self, tag: str) -> List[str]:
        """
        Return all distinct workflows defined under a tag.
        Pulls from the tag's 'in' map values and 'out' map keys.
        Raises ValueError if the tag does not exist or has no workflows.
        """
        mapping = self.build_mapping()
        if tag not in mapping:
            raise ValueError(f"Tag '{tag}' not found. Available: {sorted(mapping.keys(), key=str)}")

        tag_map = mapping[tag]
        in_map = tag_map.get('in', {}) or {}
        out_map = tag_map.get('out', {}) or {}

        workflows = set(str(wf) for wf in in_map.values())
        workflows.update(str(k) for k in out_map.keys())

        if not workflows:
            self.logger.error(f"No workflows found for tag '{tag}'.")
            raise ValueError(f"No workflows found under tag '{tag}'.")
        return sorted(workflows, key=str)

    def resolve_counterpart(self, name: str, type: str = "workflow") -> List[str]:
        """
        Convenience wrapper:
          - type='workflow' -> returns tags for that workflow
          - type='tag'      -> returns workflows for that tag
        """
        if type not in ("workflow", "tag"):
            self.logger.error("type must be 'workflow' or 'tag'")
            raise ValueError("type must be 'workflow' or 'tag'")
        return (
            self.get_tags_for_workflow(name)
            if type == "workflow"
            else self.get_workflows_for_tag(name)
        )


    def all_tag_mappings(self) -> Dict[str, Dict[str, Dict[str, Any]]]:
        """Expose the full mapping (tag -> {'in': {...}, 'out': {...}})."""
        return self.build_mapping()

    # -----------------------------
    # Internals
    # -----------------------------
    @staticmethod
    def _as_list(obj: Union[Any, List[Any]]) -> List[Any]:
        """Normalize to list: None->[], list/tuple/set->list, else [obj]."""
        if obj is None:
            return []
        if isinstance(obj, (list, tuple, set)):
            return list(obj)
        return [obj]

    @staticmethod
    def _getattr_or_key(partial: Any, key: str, default=None):
        """Support both attribute-style and dict-style access."""
        if isinstance(partial, (dict, AbcMapping)):
            return partial.get(key, default)
        return getattr(partial, key, default)

    def _sorted_labels_and_items(
        self,
        partial: Any,
        tag: str,
        side: str,
        index_in_tag: int
    ) -> Tuple[List[str], List[Tuple[str, Any]]]:
        """
        Extract and sort labels and vars_data items from a single partial entry.

        Expected keys/attrs on each partial:
          - 'file_variable': scalar or list
          - 'variable_template': mapping with key 'vars_data' (a mapping)
        """
        # Validate partial
        if not isinstance(partial, (dict, AbcMapping)) and not hasattr(partial, "__dict__"):
            self.logger.warning(
                f"[{tag}] {side.upper()} partial #{index_in_tag} is not a mapping/obj "
                f"(got {type(partial).__name__}); skipping."
            )
            return [], []

        # file_variable -> list[str]
        file_vars = self._getattr_or_key(partial, "file_variable", None)
        if file_vars is None:
            self.logger.warning(f"[{tag}] {side.upper()} partial #{index_in_tag} missing 'file_variable'; skipping.")
            return [], []
        if isinstance(file_vars, (list, tuple, set)):
            labels = [str(x) for x in file_vars]
        else:
            labels = [str(file_vars)]
        labels_sorted: List[str] = sorted(labels, key=str)

        # variable_template.vars_data -> Dict[str, Any]
        variable_template = self._getattr_or_key(partial, "variable_template", None)
        if not isinstance(variable_template, (dict, AbcMapping)):
            warnings.warn(f"[{tag}] {side.upper()} partial #{index_in_tag} missing 'variable_template'; skipping.")
            return [], []

        vars_data = variable_template.get("vars_data")
        if not isinstance(vars_data, (dict, AbcMapping)):
            warnings.warn(f"[{tag}] {side.upper()} partial #{index_in_tag} 'vars_data' is not a mapping; skipping.")
            return [], []

        # Sort items by key as strings
        items_sorted: List[Tuple[str, Any]] = sorted(
            ((str(k), v) for k, v in vars_data.items()),
            key=lambda kv: kv[0]
        )

        return labels_sorted, items_sorted
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to extract tag value from a list of dicts
def extract_tag_value(data, tag):

    # Normalize to a flat list of dicts
    if isinstance(data, dict):
        data = [data]
    elif isinstance(data, list):
        # Flatten any nested lists
        flat = []
        for item in data:
            if isinstance(item, list):
                flat.extend(item)
            else:
                flat.append(item)
        data = flat
    else:
        raise TypeError("Input must be a dict or list of dicts (possibly nested).")

    # Extract tag values
    values = [d[tag] for d in data if isinstance(d, dict) and tag in d]

    return values if values else None
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
def build_pairs_and_process(process_list, file_variable, dataset_namespace, str_separator=':'):
    """
    Build variable-workflow pairs from DatasetNamespace entries and gather diagnostics.

    Parameters
    ----------
    process_list : dict
        {var_name: process_info}
    file_variable : list[str]
        Variable names (order matters if dataset_namespace is a list/tuple)
    dataset_namespace : DatasetNamespace | dict[str, DatasetNamespace] | list[DatasetNamespace] | tuple[...]
        Each DatasetNamespace exposes .variable and .workflow
    str_separator : str, optional
        Separator for compact "key:workflow" strings (default ':')

    Returns
    -------
    pairs_list_str    : list[str]             # ["key:workflow", ...]
    pairs_list_tuple  : list[tuple[str,str]]  # [(key, workflow), ...]
    process_found     : list                  # [process_list[key], ...]
    process_dict      : dict[str, any]        # {"key:workflow": process_info, ...}
    info              : dict                  # diagnostics + {"workflow_tags": {key: workflow or None}}
    """

    def _ns_has_fields(ns):
        return ns is not None and hasattr(ns, "variable") and hasattr(ns, "workflow")

    def _resolve_ns(name, idx):
        if isinstance(dataset_namespace, dict):
            return dataset_namespace.get(name)
        if isinstance(dataset_namespace, (list, tuple)):
            return dataset_namespace[idx] if 0 <= idx < len(dataset_namespace) else None
        return dataset_namespace  # single namespace used for all

    def _dataset_keys():
        if isinstance(dataset_namespace, dict):
            return list(dataset_namespace.keys())
        if isinstance(dataset_namespace, (list, tuple)):
            return [ns.variable for ns in dataset_namespace if _ns_has_fields(ns)]
        return [dataset_namespace.variable] if _ns_has_fields(dataset_namespace) else []

    process_found = []
    pairs_list_tuple = []
    pairs_list_str = []
    process_dict = {}
    workflow_tags = {}

    # check file variable as a list
    if not isinstance(file_variable, list):
        file_variable = [file_variable]

    for i, var_name in enumerate(file_variable):
        if var_name not in process_list:
            continue

        ns = _resolve_ns(var_name, i)
        if _ns_has_fields(ns):
            tag_workflow = ns.workflow
        else:
            tag_workflow = var_name  # fallback if namespace missing

        key = f"{var_name}{str_separator}{tag_workflow}"

        pairs_list_tuple.append((var_name, tag_workflow))
        pairs_list_str.append(key)
        process_found.append(process_list[var_name])
        process_dict[key] = process_list[var_name]
        workflow_tags[var_name] = tag_workflow if _ns_has_fields(ns) else None

    dataset_keys = _dataset_keys()
    info = {
        "missing_in_dataset": [v for v in file_variable if v not in dataset_keys],
        "missing_in_process": [v for v in file_variable if v not in process_list],
        "extras_in_process": [k for k in process_list if k not in file_variable],
        "workflow_tags": workflow_tags,
    }

    return pairs_list_str, pairs_list_tuple, process_found, info
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to return list
def as_list(maybe_seq):
    if isinstance(maybe_seq, (list, tuple)):
        return list(maybe_seq), True   # is_sequence = True
    return [maybe_seq], False

# method to remove NoneType values from a list
def remove_none(lst):
    # Remove NoneType values from the list
    return [x for x in lst if x is not None]

# method to group process by variable
def group_process(proc_list, proc_tag='reference'):
    # import defaultdict
    proc_group = defaultdict(list)

    # iterate over all processes
    for proc in proc_list:
        if proc_tag == 'reference':

            if not hasattr(proc, 'reference') or proc.reference is None:
                raise ValueError(f"Process object {proc!r} has no valid 'reference' attribute.")
            proc_group[proc.reference].append(proc)

        elif proc_tag == 'workflow':

            if not hasattr(proc, 'workflow') or proc.workflow is None:
                raise ValueError(f"Process object {proc!r} has no valid 'workflow' attribute.")
            proc_group[proc.workflow].append(proc)

        elif proc_tag == 'tag':

            if not hasattr(proc, 'tag') or proc.tag is None:
                raise ValueError(f"Process object {proc!r} has no valid 'tag' attribute.")
            proc_group[proc.tag].append(proc)

        else:
            raise ValueError(f"Invalid attributes '{proc_tag}'. Must be 'reference', 'workflow' or 'tag'.")

    return dict(proc_group)
# ----------------------------------------------------------------------------------------------------------------------
