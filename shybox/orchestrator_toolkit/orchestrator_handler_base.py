"""
Class Features

Name:          orchestrator_handler_base
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20260114'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
from __future__ import annotations

import os
import re
import shutil
import tempfile
import random

from copy import deepcopy, copy
from typing import Any, Dict, List, Union

import datetime as dt
import pandas as pd
import xarray as xr

from shybox.generic_toolkit.lib_utils_tmp import ensure_folder_tmp
from shybox.io_toolkit.lib_io_utils import search_common_path
from shybox.time_toolkit.lib_utils_time import convert_time_format, normalize_to_datetime_index

from shybox.orchestrator_toolkit.lib_orchestrator_utils_processes import PROCESSES
from shybox.orchestrator_toolkit.lib_orchestrator_process import ProcessorContainer

from shybox.dataset_toolkit.dataset_handler_mem import DataMem
from shybox.dataset_toolkit.dataset_handler_local import DataLocal

from shybox.logging_toolkit.logging_handler import LoggingManager
from shybox.logging_toolkit.lib_logging_utils import with_logger
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# OrchestratorBase (engine)
class OrchestratorBase:

    # default class options
    default_options = {
        "intermediate_output": "Mem",  # "Mem" or "Tmp"
        "break_on_missing_tiles": False,  # legacy naming; grid uses it, TS may ignore
        "tmp_dir": None,
    }

    def __init__(
        self,
        data_in: Union[DataLocal, Dict[str, Any]],
        data_out: Union[DataLocal, Dict[str, Any], None] = None,
        deps_in: Union[DataLocal, Dict[str, Any], None] = None,
        deps_out: Union[DataLocal, Dict[str, Any], None] = None,
        args_in: dict = None, args_out: dict = None,
        options: Union[dict, None] = None,
        mapper: Any = None,
        logger: LoggingManager = None,
    ) -> None:

        self.logger = logger or LoggingManager(name=self.__class__.__name__)

        self.data_in = data_in
        self.data_out = data_out

        self.deps_in = deps_in
        self.deps_out = deps_out

        self.args_in = args_in
        self.args_out = args_out

        self.processes: List[ProcessorContainer] = []
        self.break_points: List[int] = []

        self.options = deepcopy(self.default_options)
        if options is not None:
            self.options.update(options)

        self.tmp_dir = None
        if self.options["intermediate_output"] == "Tmp":
            tmp_root = self.options.get("tmp_dir", tempfile.gettempdir())
            self.tmp_dir = ensure_folder_tmp(tmp_root)

        self.memory_active = True
        self.mapper = mapper  # injected by builder (Grid/TS)

        self.process_current = None
        self.process_previous = None
        self.process_history, self.process_id, self.process_tag = [], [], []

    # Hooks (override in subclasses)
    def grouping_tag(self) -> str:
        """Attribute used by group_process(). Default matches your current behavior."""
        return "reference"

    def reference_key_from_map(self, process_map: Dict[str, Any]) -> str:
        """Default reference is tag:workflow (your current convention)."""
        return ":".join([process_map["tag"], process_map["workflow"]])

    def get_input_by_reference(self, reference_key: str) -> Any:
        """Resolve initial input object for a variable group."""
        if isinstance(self.data_in, dict):
            if reference_key not in self.data_in:
                raise RuntimeError(f'Input data for "{reference_key}" not found in input collection.')
            return self.data_in[reference_key]
        if isinstance(self.data_in, DataLocal):
            return self.data_in
        raise RuntimeError("Input data must be DataLocal or dict[str, Any].")

    def get_deps_by_reference(self, reference_key: str) -> Any:
        if self.deps_in is None:
            return None
        if not isinstance(self.deps_in, dict):
            raise RuntimeError("Input deps must be a dict or None.")
        return self.deps_in.get(reference_key, None)

    # properties
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
    def make_output(self, in_obj: DataLocal, out_obj: Union[DataLocal, None] = None,
                    variable: str = None, workflow: str = None, mapping: dict = None,
                    function = None, message: bool = True, **kwargs) -> dict:

        # create out key (if needed)
        out_key = create_key(workflow=workflow, variable=variable, separator=':')

        # if output obj is dict or DataLocal return
        out_collections = {}
        if isinstance(out_obj, DataLocal):
            # create output key
            out_collections[out_key] = out_obj
            return out_collections
        elif isinstance(out_obj, dict):
            # use output dictionary
            out_collections = out_obj
            return out_collections
        elif out_obj is None:
            # keep empty collection
            pass
        else:
            raise RuntimeError('Output obj must be defined by DataLocal, dict or None')

        # get input path and filename
        if isinstance(in_obj, list):

            # collect available location patterns
            path_names = [obj.loc_pattern for obj in in_obj if getattr(obj, "loc_pattern", None) is not None]
            # search a common path root (for intermediate step if needed)
            path_name_in = search_common_path(path_names=path_names, path_extra='common')
        else:
            path_name_in = in_obj.loc_pattern

        # initialize folder and filename
        folder_name_tmp, file_name_tmp = None, None
        # get from obj input
        if path_name_in:
            # split folder and filename
            folder_name_in, file_name_in = os.path.split(path_name_in)
            # get available components
            folder_name_tmp = folder_name_in or None
            file_name_tmp = file_name_in or None

        # ensure temporary folder
        if folder_name_tmp is None:
            folder_name_tmp = ensure_folder_tmp()

        # check time placeholder
        has_times = getattr(in_obj, 'has_time', False)
        format_times = "%Y%m%d%H%M%S" if has_times else None

        # initialize file history
        file_history = None
        # create output filename
        if function is not None:

            # get input extension
            if file_name_tmp is not None:
                ext_tmp = os.path.splitext(file_name_tmp)[1].lstrip('.')
            else:
                ext_tmp = str(in_obj.file_format).lstrip('.')

            # get output extension
            ext_out = getattr(function, 'output_ext', None) or ext_tmp
            # generate random id
            random_out = str(random.randint(100, 999))

            # initialize filename parts
            name_parts = []
            # add variable
            if self.has_variables:
                # clean variable name
                variable_part = re.sub(r'_+','_',re.sub(r'[:|,.]', '_', str(variable)))
                name_parts.append(variable_part)
            # add random id
            name_parts.append(random_out)
            # add time placeholder
            if has_times:
                name_parts.append(format_times)

            # create output filename
            file_name_out = f'{"_".join(name_parts)}.{ext_out}'

            # create complete output path
            path_name_out = os.path.join(folder_name_tmp, file_name_out)

            # create file history
            file_history = "_".join(name_parts)

        else:

            # ensure temporary output path
            path_name_out = ensure_folder_tmp()

        # use the output pattern provided automatic or by the user
        if out_obj is None:
            pass
        elif isinstance(out_obj, dict):
            path_name_out = out_obj.get('loc_pattern', path_name_out)
        else:
            self.logger.error('Output object must be defined by dictionary or NoneType.')
            raise ValueError('Check your "make_output" method to update or change settings.')

        # manage and intermediate output in geotiff format
        output_type = self.options['intermediate_output']
        if output_type == 'Mem':
            out_obj = DataMem(loc_pattern=path_name_out)
        elif output_type == 'Tmp':

            # prepare file name (temporary)
            file_name_tmp = os.path.basename(path_name_out)

            # ensure variable template and file variable attributes
            kwargs['variable_template'] = {}
            if mapping is None:
                kwargs['variable_template']['vars_data'] = {kwargs['workflow_name']: 'tmp'}
            else:
                kwargs['variable_template']['vars_data'] = mapping

            # create the temporary output object (for making tmp files in a list of processes)
            out_obj = DataLocal(
                path=self.tmp_dir, file_name=file_name_tmp,
                file_variable=variable, file_workflow=workflow, data_role='data',
                message=message, logger=self.logger, **kwargs)

        else:
            self.logger.error('Orchestrator output type must be "Mem" or "Tmp".')
            raise ValueError('Orchestrator output type must be "Mem" or "Tmp"')

        # store output file history
        out_obj.file_history = file_history
        # organize output collections
        out_collections[out_key] = out_obj

        return out_collections

    # method to add a process to the orchestrator
    def add_process(self,
                    function_obj,
                    process_output: Union[DataLocal, xr.Dataset, dict] = None, **kwargs) -> None:

        # get workflow name
        if 'workflow_name' in kwargs:
            workflow_name = kwargs['workflow_name']
        else:
            self.logger.error('Process variable "workflow_name" must be provided in the process arguments.')
            raise RuntimeError('Process variable "workflow_name" must be provided in the process arguments.')

        # get process tag
        if 'tag' in kwargs:
            process_tag = kwargs['tag']
        else:
            self.logger.error('Process variable "tag" must be provided in the process arguments.')
            raise RuntimeError('Process variable "tag" must be provided in the process arguments.')

        # get process mode
        if 'mode' in kwargs:
            process_mode = kwargs['mode']
        else:
            self.logger.error('Process variable "mode" must be provided in the process arguments.')
            raise RuntimeError('Process variable "mode" must be provided in the process arguments.')

        # get mapping_in process
        if 'mapping_in' in kwargs:
            process_mapping_in = kwargs['mapping_in']
        else:
            self.logger.error('Process variable "mapping_in" must be provided in the process arguments.')
            raise RuntimeError('Process variable "mapping_in" must be provided in the process arguments.')

        # get mapping_out process
        if 'mapping_out' in kwargs:
            process_mapping_out = kwargs['mapping_out']
        else:
            self.logger.error('Process variable "mapping_out" must be provided in the process arguments.')
            raise RuntimeError('Process variable "mapping_out" must be provided in the process arguments.')

        # get process datasets
        if 'datasets' in kwargs:
            process_datasets = kwargs['datasets']
        else:
            self.logger.error('Process variable "datasets" must be provided in the process arguments.')
            raise RuntimeError('Process variable "datasets" must be provided in the process arguments.')

        # get process results
        if 'results' in kwargs:
            process_results = kwargs['results']
        else:
            self.logger.error('Process variable "results" must be provided in the process arguments.')
            raise RuntimeError('Process variable "results" must be provided in the process arguments.')

        # get process fx
        if 'function' in kwargs:
            process_fx = kwargs.pop('function')
        else:
            self.logger.error('Process variable "function" must be provided in the process arguments.')
            raise RuntimeError('Process variable "function" must be provided in the process arguments.')

        # check data in/out format
        if not isinstance(self.data_in, dict):
            self.logger.error('Datasets input must be a Dataset.')
            raise RuntimeError('Datasets input must be a Dataset.')
        if not isinstance(self.data_out, dict):
            self.logger.error('Datasets output must be a Dataset.')
            raise RuntimeError('Datasets output must be a Dataset.')

        # get function name
        function_name = function_obj.__name__
        # get deps in/out
        deps_input, deps_output = self.deps_in, self.deps_out

        # initialize process or not
        process_init, process_obj_previous = False, None
        if process_mode == 'start':
            process_init = True
            process_wf = workflow_name
        elif process_mode == 'unique':
            process_init = True
            process_wf = workflow_name
        elif process_mode == 'intermediate':
            process_obj_previous = self.processes[-1]
            if (process_obj_previous is not None) and (workflow_name not in process_obj_previous.workflow_name):
                process_init = True
            process_wf = workflow_name
        elif process_mode == 'end':
            process_obj_previous = self.processes[-1]
            if (process_obj_previous is not None) and (workflow_name not in process_obj_previous.workflow_name):
                process_init = True
            process_wf = workflow_name
        else:
            raise RuntimeError('Process mode must be "start", "intermediate", "unique" or "end".')

        # create the process input
        if process_init:

            # define process settings for the method 1
            this_input, this_type = {}, {}
            in_keys = list(self.data_in.keys())
            for step_key in in_keys:
                in_flag = False
                for process_key, process_mapper in process_mapping_in.items():

                    step_collection = process_mapper['collection']
                    step_type = process_mapper['type']

                    if step_key == step_collection:
                        tmp_input = self.data_in[step_key]
                        if isinstance(tmp_input, list) and len(tmp_input) == 1:
                            tmp_input = tmp_input[0]
                        this_input[step_key] = tmp_input
                        this_type[step_key] = step_type
                        in_flag = True

                if not in_flag:
                    raise RuntimeError(f'Dataset defined by {step_key} is not found in the mapping structure')
        else:
            # define process settings for the method n
            deps_input = process_obj_previous.out_deps
            process_obj_previous = self.processes[-1]
            this_input = process_obj_previous.out_obj

        # check input type
        if not isinstance(this_input, dict):
            raise TypeError(f"Object 'this_input' must be a dictionary, got {type(this_input).__name__}.")
        # check input content
        if not this_input:
            raise ValueError("Object 'this_input' dictionary is empty.")

        # get input at last key
        last_key = next(reversed(this_input))
        tmp_output = this_input[last_key]

        # create the process output
        this_output = self.make_output(
            in_obj=tmp_output, out_obj=process_output, function=function_obj,
            variable=process_tag, workflow=process_wf, mapping=process_mapping_out,
            message=False, **kwargs)

        # create the process container
        this_process = ProcessorContainer(
            function = function_obj,
            in_obj = this_input, in_opts=self.options,
            in_deps=deps_input, out_deps=deps_output,
            out_obj = this_output, out_opts=self.options,
            args = kwargs,
            logger=self.logger)

        # check if break point is required
        if this_process.break_point:
            self.break_points.append(len(self.processes))

        # executed only if loop did NOT break
        self.processes.append(this_process)

    # method to wrap the run of the orchestrator
    def run(self, time: Union[pd.Timestamp, str, pd.DatetimeIndex, None] = None, **kwargs) -> None:

        # info orchestrator start
        self.logger.info_up('Run orchestrator ...')

        # check time availability
        if time is not None:
            self.logger.info('Active dynamic mode')
        else:
            self.logger.info('Active static mode')
            time = pd.Timestamp.now()

        # manage process execution and process output
        if len(self.processes) == 0:
            # exit no processes declared
            self.logger.error('No processes have been added to the workflow. Exit')
            raise ValueError('Check the messages to found the issue in adding processes.')

        # iterate over process(es)
        for proc_id, proc_obj in enumerate(self.processes):

            # get mode
            proc_mode, proc_tag = proc_obj.mode, proc_obj.tag

            # set by the mode
            if proc_mode == 'end' or proc_mode == 'unique':

                # get data from self or process(es)
                out_collections = self.data_out
                default_obj = proc_obj.out_obj
                tmp_obj = copy(proc_obj)

                # iterate over default obj (usually a dict with one key)
                for workflow_default, data_default in default_obj.items():

                    # define data and workflow out
                    data_out, workflow_out = None, None
                    if isinstance(out_collections, dict):
                        for key_tmp, data_tmp in out_collections.items():
                            workflow_tmp = key_tmp.split('|')[0]
                            if workflow_tmp in workflow_default:
                                data_out = data_tmp[0]
                                workflow_out = workflow_tmp
                                continue
                    else:
                        raise TypeError('out_collections must be a dictionary.')
                    # check data and workflow out
                    if (data_out is None) or (workflow_out is None):
                        raise RuntimeError(f'Data_out and workflow_out is/are defined by NoneType.')

                    self.processes[proc_id].out_obj[workflow_default] = data_out
                    self.processes[proc_id].dump_state = True

            elif proc_mode == 'start':
                pass
            elif proc_mode == 'intermediate':
                pass
            else:
                self.logger.error('Variable "mode" can be defined by "end", "start", "unique" or "intermediate" tag')

        # normalize time steps
        time_steps = normalize_to_datetime_index(time)
        # check time steps
        if len(time_steps) == 0:
            return None

        ### LASCIATO  DA CONTROLLARE OCN CASO SPECIFICO
        # check group tag in kwargs (by_time disables memory) --> da controllare con merge time
        if 'group' in kwargs:
            group_type = kwargs['group']
            if group_type == 'by_time':
                # if group by time the algorithm will pass a list of time steps to each process (memory disabled)
                # merger by time requires all time steps at once
                time_steps = _merge_by_time(time_steps)
                self.memory_active = False

        # iterate over time steps
        for ts in time_steps:

            # format ts only for logging
            pretty_ts = pretty_time(ts)

            # info time start
            self.logger.info_up(f'Time "{pretty_ts}" ...')
            # run time step
            self.run_single_ts(time=ts, **kwargs)
            # info time end
            self.logger.info_down(f'Time "{pretty_ts}" ... DONE')

        # info orchestrator end
        self.logger.info_down('Run orchestrator ... DONE')

        return None

    # method to run single time step
    def run_single_ts(self, time: Union[pd.Timestamp, str, pd.DatetimeIndex], **kwargs) -> None:

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
                # collect all processes until the breakpoint
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

    # method to iterate over process(es)
    def _run_processes(self, processes, time: dt.datetime, **kwargs) -> None:

        # return if no processes
        if not processes: return None

        # group process by variable
        proc_group = ProcessorContainer.group_processes(self.processes, fields='workflow_name')

        # iterate over all variable groups
        for group_id, (proc_workflow, proc_list) in enumerate(proc_group.items()):

            # end workflow end
            self.logger.info_up(f'Workflow "{proc_workflow}" ...')

            # iterate over all processes for this variable
            proc_current, proc_previous, proc_history, proc_memory = None, None, {}, False
            for proc_id, proc_obj in enumerate(proc_list):

                # set proc current
                self.process_current = copy(proc_obj)

                # get process fx and tag
                proc_fx, proc_tag = proc_obj.fx_name, proc_obj.tag
                # get process mode and dump
                proc_mode, proc_dump = proc_obj.mode, proc_obj.dump_state

                # start process block
                self.logger.info_up(f'Process "{proc_fx}" ...')

                # define proc args
                proc_args = {}
                # execute process
                try:
                    # set collections args (stored object for data iterations)
                    if proc_id > 0 and proc_history[proc_id - 1] is None:
                        self.logger.warning(f'Process "{proc_fx}" ... SKIPPED. Previous process was NoneType')
                        proc_history.append(None)
                        continue
                    elif proc_id > 0 and proc_history[proc_id - 1] is not None:
                        proc_args['collections_history'].update(self.process_history)
                        proc_args['collections_tags'] = self.process_tag
                    else:
                        proc_args['collections_history'] = {}
                        proc_args['collections_tags'] = []

                    # define process args
                    proc_args['memory_active'] = self.memory_active
                    proc_args['process_id'] = proc_id
                    proc_args['group_id'] = group_id

                    # run process (return obj or dict just to test for iterations ... )
                    proc_result, proc_memory = proc_obj.run(time, **proc_args)

                    # manage history process
                    if not proc_dump:

                        # DETAIL: if skipped / empty
                        if proc_result is None:
                            self.logger.warning(f'Process "{proc_fx}" ... SKIPPED. Data not available')

                        # store process result
                        self.process_id = proc_id
                        self.process_history.append(proc_result)
                        self.process_tag.append(proc_tag)
                        # set proc previous
                        self.process_previous = copy(proc_obj)

                    else:
                        # dump state active the data in memory is cleared (empty dict)
                        self.process_current, self.process_previous = None, None
                        self.process_history, self.process_id, self.process_tag = {}, [], []

                finally:
                    # end process block
                    self.logger.info_up(f'Process "{proc_fx}" ... DONE')

            # end workflow end
            self.logger.info_down(f'Workflow "{proc_workflow}" ... DONE')

# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# helpers
# helper to create a data key
def create_key(workflow: Union[str, None] = None, variable: Union[str, None] = None, separator: str = ":",) -> str:
    # check workflow and variable
    if workflow is None or variable is None:
        raise ValueError("Workflow and variable must be defined.")
    # create key
    return separator.join([str(workflow), str(variable),])

# helper to merge by times
def _merge_by_time(times):
    return [times]

# helper to merge by process
def _merge_by_process(containers):
    merged = {}

    for pc in containers:

        process, key, variable, sources = pc.args  # adapt if attribute name differs
        base_key = (process, variable)

        # split existing sources
        src_list = sources.split(',')

        if base_key not in merged:
            merged[base_key] = set(src_list)
        else:
            merged[base_key].update(src_list)

    result = []
    for (process, variable), src_set in merged.items():
        src_str = ",".join(sorted(src_set))
        key = f"{variable}:{src_str}"
        result.append(ProcessorContainer((process, key, variable, src_str)))

    return result

# helper to format time for logging
def pretty_time(ts):

    if isinstance(ts, pd.DatetimeIndex):
        if len(ts) == 1:
            return ts[0].strftime("%Y-%m-%d %H:%M")
        else:
            start = ts[0].strftime("%Y-%m-%d %H:%M")
            end = ts[-1].strftime("%Y-%m-%d %H:%M")
            return f"{start} :: {end} ({len(ts)} steps)"

    elif isinstance(ts, pd.Timestamp):
        return ts.strftime("%Y-%m-%d %H:%M")

    else:
        return str(ts)
# ----------------------------------------------------------------------------------------------------------------------
