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

from shybox.dataset_toolkit.dataset_handler_memory import DataMemory
from shybox.dataset_toolkit.dataset_handler_local import DataLocal
from shybox.dataset_toolkit.dataset_handler_collections import DataCollection

from shybox.logging_toolkit.logging_handler import LoggingManager

# package utils
from shybox.logging_toolkit.lib_logging_utils import with_logger
from shybox.generic_toolkit.lib_utils_code import deprecated
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# OrchestratorBase (engine)
class OrchestratorBase:

    # default class options
    DEFAULT_OPTION_REGISTRY = {
        "intermediate_output": "Mem",  # "Mem" or "Tmp"
        "break_on_missing_tiles": False,  # legacy naming; grid uses it, TS may ignore
        "tmp_dir": None,
        "io_config": {'type_in': None, 'type_out': None, 'description': 'NA'}
    }

    # io configuration registry
    IO_REGISTRY = {
        "one_data_to_one_file": {
            "type_in": "single",
            "type_out": "single",
            "description": "Dump each data object to a separate file",
        },
        "many_data_ts_to_one_file": {
            "type_in": "collections",
            "type_out": "single",
            "description": "Merge multiple time-series into one file",
        },
        "many_data_variables_to_one_file": {
            "type_in": "collections",
            "type_out": "single",
            "description": "Merge multiple variables into one file",
        },
        "many_data_tiles_to_one_file": {
            "type_in": "collections",
            "type_out": "single",
            "description": "Merge multiple tiles into one file"
        },
        "many_data_times_to_one_file": {
            "type_in": "collections",
            "type_out": "single",
            "description": "Merge multiple time steps into one file",
        },
        "many_data_files_to_one_file": {
            "type_in": "collections",
            "type_out": "single",
            "description": "Merge multiple files into one file",
        },
    }

    # set recognized group modes
    GROUP_REGISTRY = {
        "by:step": "Group data by single time step",
        "by:period": "Group data by time period",
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
        logger: LoggingManager = None,) -> None:

        self.logger = logger or LoggingManager(name=self.__class__.__name__)

        self.data_in = data_in
        self.data_out = data_out

        self.deps_in = deps_in
        self.deps_out = deps_out

        self.args_in = args_in
        self.args_out = args_out

        self.processes: List[ProcessorContainer] = []
        self.break_points: List[int] = []

        self.options = deepcopy(self.DEFAULT_OPTION_REGISTRY)
        if options is not None:
            options = {key: value for key, value in options.items() if not (key.startswith('_') and key.endswith('_'))}
            self.options.update(options)

        # get io configuration
        self.io_config = self.options.get("io_config", None)
        # set io attributes
        self.io_mode = self.io_config["mode"]
        self.type_in = self.io_config["type_in"]
        self.type_out = self.io_config["type_out"]

        # get tmp dir
        if self.options["intermediate_output"] == "Tmp":
            tmp_root = self.options.get("tmp_dir", tempfile.gettempdir())
            self.tmp_dir = ensure_folder_tmp(tmp_root)

        self.memory_active = True
        self.mapper = mapper  # injected by builder (Grid/TS)

        self.process_current = None
        self.process_previous = None
        self.process_history, self.process_id, self.process_tag = {}, [], []
        self.result_history = {}

        self.group = 'by:step'
        self.time = None

    # Hooks (override in subclasses)
    @deprecated(since='2026-10-02')
    def grouping_tag(self) -> str:
        """Attribute used by group_process(). Default matches your current behavior."""
        return "reference"

    @deprecated(since='2026-10-02')
    def reference_key_from_map(self, process_map: Dict[str, Any]) -> str:
        """Default reference is tag:workflow (your current convention)."""
        return ":".join([process_map["tag"], process_map["workflow"]])

    @deprecated(since='2026-10-02')
    def get_input_by_reference(self, reference_key: str) -> Any:
        """Resolve initial input object for a variable group."""
        if isinstance(self.data_in, dict):
            if reference_key not in self.data_in:
                raise RuntimeError(f'Input data for "{reference_key}" not found in input collection.')
            return self.data_in[reference_key]
        if isinstance(self.data_in, DataLocal):
            return self.data_in
        raise RuntimeError("Input data must be DataLocal or dict[str, Any].")

    @deprecated(since='2026-10-02')
    def get_deps_by_reference(self, reference_key: str) -> Any:
        if self.deps_in is None:
            return None
        if not isinstance(self.deps_in, dict):
            raise RuntimeError("Input deps must be a dict or None.")
        return self.deps_in.get(reference_key, None)

    ## ORCHESTRATOR PROPERTIES
    # io configuration
    @property
    def io_config(self):
        return self._io_config
    @io_config.setter
    def io_config(self, value):

        # Validate configuration
        if not isinstance(value, dict):
            raise TypeError('IO configuration must be a dictionary.')

        # Get IO mode
        mode = value.get("mode")

        # Format available modes
        available_modes = "\n".join(
            f'  - {name}: {cfg["description"]} '
            f'[{cfg["type_in"]} -> {cfg["type_out"]}]'
            for name, cfg in self.IO_REGISTRY.items()
        )

        # Validate mode
        if mode not in self.IO_REGISTRY:
            raise ValueError(
                f'Unsupported IO mode "{mode}".\n'
                f'Check "workflow.options.io_config.mode".\n'
                f'Available modes:\n{available_modes}'
            )

        # Get IO settings from registry
        io_settings = self.IO_REGISTRY[mode]

        # Set IO configuration
        self._io_config = {"mode": mode,"type_in": io_settings["type_in"],"type_out": io_settings["type_out"],}

    # IO mode property
    @property
    def io_mode(self):
        return self._io_mode
    @io_mode.setter
    def io_mode(self, value):
        if value not in self.IO_REGISTRY:
            raise ValueError(f'Unsupported IO mode "{value}".')
        self._io_mode = value

    # Input data type property
    @property
    def type_in(self):
        return self._type_in
    @type_in.setter
    def type_in(self, value):
        if value not in ("single", "collections"):
            raise ValueError(f'Unsupported input data type "{value}".')
        self._type_in = value

    # Output data type property
    @property
    def type_out(self):
        return self._type_out
    @type_out.setter
    def type_out(self, value):
        if value not in ("single", "collections"):
            raise ValueError(f'Unsupported output data type "{value}".')
        self._type_out = value

    # group property
    @property
    def group(self):
        return self._group
    @group.setter
    def group(self, value):
        if value not in self.GROUP_REGISTRY:
            raise ValueError(f'Group mode "{value}" is not supported. '
                             f'Available modes: {", ".join(self.GROUP_REGISTRY)}')
        self._group = value

    # time property
    @property
    def time(self):
        return self._time
    @time.setter
    def time(self, value):
        if value is None:
            self._time = None
        elif isinstance(value, pd.Timestamp):
            self._time = value
        elif isinstance(value, str):
            self._time = pd.Timestamp(value)
        elif isinstance(value, pd.DatetimeIndex):
            self._time = value
        else:
            raise TypeError(f'Time type "{type(value).__name__}" is not supported. '
                            f'Expected pd.Timestamp, str, pd.DatetimeIndex or None.')

    # properties
    @property
    def has_variables(self):
        if isinstance(self.data_in, dict):
            return True if len(self.data_in) > 0 else False
        else:
            return False

    # clean tmp directory where process ends
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
        if workflow not in variable:
            out_key = create_key(workflow=workflow, variable=variable, separator=':')
        else:
            out_key = variable

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
        if output_type == 'Memory':
            # NOTE. Update class definition ...
            out_obj = DataMemory(loc_pattern=path_name_out)
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

        # get process fx
        if 'args' in kwargs:
            process_args = kwargs['args']
        else:
            process_args = {}

        # define process options
        option_process = {**process_args, **self.options}

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

            # get input keys for the selected workflow
            in_keys = [key for key in self.data_in.keys() if key.split('|')[0] == workflow_name]
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
            in_obj = this_input, in_opts=option_process,
            in_deps=deps_input, out_deps=deps_output,
            out_obj = this_output, out_opts=option_process,
            args = kwargs,
            logger=self.logger)

        # check if break point is required
        if this_process.break_point:
            self.break_points.append(len(self.processes))

        # executed only if loop did NOT break
        self.processes.append(this_process)

    # method to wrap the run of the orchestrator
    def run(self,
            time: Union[pd.Timestamp, str, pd.DatetimeIndex, None] = None,
            group: str = 'by:step', **kwargs) -> None:

        # info orchestrator start
        self.logger.info_up('Run orchestrator ...')

        # check time availability
        if time is None:
            self.logger.info_status("RunExecution -- TYPE static")
            time = pd.Timestamp.now()
        else:
            self.logger.info_status("RunExecution -- TYPE dynamic")

        # update orchestrator properties
        self.time, self.group = time, group

        # manage process execution and process output
        if len(self.processes) == 0:
            # exit no processes declared
            self.logger.error('No processes have been added to the workflow. Exit')
            raise ValueError('Check the messages to found the issue in adding processes.')

        # get global process obj and id
        process_last, id_last = self.processes[-1], len(self.processes) - 1

        # iterate over process(es)
        data_out_collections = None
        for proc_id, proc_obj in enumerate(self.processes):

            # get mode
            proc_mode, proc_tag = proc_obj.mode, proc_obj.tag

            # info status process
            self.logger.info_status(f"RunExecution -- PROCESS | STEP | ID: {proc_id} | MODE: {proc_mode} | TAG: {proc_tag}")

            # Set by process mode
            if proc_mode in ('end', 'unique'):

                # Get data from self or process(es)
                out_collections = self.data_out
                default_obj = proc_obj.out_obj
                tmp_obj = copy(proc_obj)

                # Iterate over default objects
                for workflow_default, data_default in default_obj.items():

                    # Define data and workflow out
                    data_out, workflow_out = None, None
                    if not isinstance(out_collections, dict):
                        raise TypeError('out_collections must be a dictionary.')

                    for key_tmp, data_tmp in out_collections.items():
                        workflow_tmp = key_tmp.split('|', 1)[0]
                        if workflow_tmp in workflow_default:

                            # validate output list
                            if not isinstance(data_tmp, list) or len(data_tmp) != 1:
                                raise ValueError(f'Output "{key_tmp}" must contain exactly one object.')

                            data_out = data_tmp[0]
                            workflow_out = workflow_tmp
                            break

                    # Check data and workflow out
                    if data_out is None or workflow_out is None:
                        raise RuntimeError(f'Output data not found for workflow "{workflow_default}".')

                    # status process output
                    self.logger.info_status(
                        f"RunExecution -- PROCESS | OUTPUT | DUMP: {self.io_mode} | "
                        f"TYPE_IN: {self.type_in} | TYPE_OUT: {self.type_out}"
                    )

                    # manage io type
                    if (self.type_in == 'collections' and self.type_out == 'single') and self.group == 'by:step':

                        # initialize data collection object
                        if data_out_collections is None:
                            data_out_collections = DataCollection(logger=self.logger)

                        # add output object
                        for out_key, out_config in out_collections.items():

                            # filter by workflow
                            if out_key.split('|', 1)[0] != workflow_out:
                                continue

                            # check that exactly one object is available
                            if not isinstance(out_config, list) or len(out_config) != 1:
                                raise ValueError(
                                    f'Output "{out_key}" must contain exactly one object. '
                                    f'Found: {len(out_config) if isinstance(out_config, list) else type(out_config).__name__}'
                                )

                            # add current DataLocal to collection
                            data_out_collections.add(out_key, out_config[0])

                        # Assign collection only at the last process
                        if proc_id == id_last:
                            self.processes[proc_id].out_obj[workflow_default] = data_out_collections
                            self.processes[proc_id].dump_state = True

                    elif (self.type_in == 'collections' and self.type_out == 'single') and self.group == 'by:period':

                        # assign DataLocal at the end of time period
                        self.processes[proc_id].out_obj[workflow_default] = data_out
                        self.processes[proc_id].dump_state = True

                    elif self.type_in == 'single' and self.type_out == 'single':

                        # assign individual DataLocal
                        self.processes[proc_id].out_obj[workflow_default] = data_out
                        self.processes[proc_id].dump_state = True

                    elif self.type_in == 'single' and self.type_out == 'collections':
                        raise NotImplementedError(
                            f'Not implemented IO "{self.type_in} :: {self.type_out}" for IO mode "{self.io_mode}". ')

                    elif self.type_in == 'collections' and self.type_out == 'collections':
                        raise NotImplementedError(
                            f'Not implemented IO "{self.type_in} :: {self.type_out}" for IO mode "{self.io_mode}". ')

                    else:
                        raise ValueError(f'Unsupported IO input type "{self.type_in}" for IO mode "{self.io_mode}".')

            elif proc_mode in ('start', 'intermediate'):
                pass
            else:
                raise ValueError(f'Unsupported process mode "{proc_mode}". Expected: start, intermediate, end, unique.')

        # normalize time steps
        time_steps = normalize_to_datetime_index(time)
        # check time steps
        if len(time_steps) == 0:
            return None

        # defined group time or objects
        if self.group == 'by:step':
            # info status group
            self.logger.info_status(f"RunExecution -- GROUP {self.group}")
        elif self.group == 'by:period':
            self.logger.info_status(f"RunExecution -- GROUP {self.group}")
            time_steps = _merge_by_period(time_steps)

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
    def _run_processes(self, processes, time: Union[dt.datetime, list[dt.datetime]], **kwargs) -> None:

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
                # get process workflow name and id
                proc_wflow_name, proc_wflow_id = proc_obj.workflow_name, proc_obj.workflow_id
                # get process mode and dump
                proc_mode, proc_dump = proc_obj.mode, proc_obj.dump_state
                # get process datasets and results
                proc_datasets, proc_results = proc_obj.datasets, proc_obj.results

                # start process block
                self.logger.info_up(f'Process "{proc_fx}" ...')

                # get collections of tags and objects
                proc_tag_collections = self.process_tag
                proc_history_obj_collections = self.process_history

                # define process ids
                proc_id_current = proc_id
                proc_id_previous = proc_id - 1 if proc_id > 0 else None
                # define process tags
                proc_tag_current = proc_tag
                proc_tag_previous = proc_tag_collections[proc_id - 1] if proc_id > 0 else None
                # define process history tags
                proc_history_id_current = f'process_{proc_id_current}'
                proc_history_id_previous = (f'process_{proc_id_previous}' if proc_id_previous is not None else None)

                # define proc args
                proc_args = {}
                # execute process
                try:
                    # initialize process collections
                    proc_args['collections_history'], proc_args['collections_tags'] = {}, []
                    proc_args['results_history'] = {}

                    # manage previous process
                    proc_history_obj_previous = {}
                    if proc_id_previous is not None:

                        # get previous process objects matching the workflow
                        proc_history_obj_tmp = {
                            key: value
                            for key, value in proc_history_obj_collections.items()
                            if key.startswith(f"{proc_wflow_name}")
                        }

                        # check previous process objects
                        if not proc_history_obj_tmp or all(value is None for value in proc_history_obj_tmp.values()):

                            self.logger.warning(
                                f'Process "{proc_fx}" ... SKIPPED. '
                                f'Previous process for workflow "{proc_wflow_name}" was NoneType'
                            )
                            proc_history_obj_previous = {}
                            continue

                        else:
                            # create previous process history
                            proc_history_obj_previous = copy(proc_history_obj_tmp)

                        # get keys history and previous process(es)
                        key_previous = list(proc_history_obj_previous.keys())
                        key_history = list(self.process_history.keys())
                        # check validity of previous process
                        if len(key_previous) != 1 or key_previous[0] not in key_history:
                            raise RuntimeError(f'Previous process for workflow "{proc_wflow_name}" was NoneType or not available')

                        proc_args['collections_history'] = self.process_history
                        proc_args['collections_tags'] = self.process_tag

                    # define process args
                    proc_args['memory_active'] = self.memory_active
                    proc_args['process_id'] = proc_id_current
                    proc_args['process_tag'] = proc_tag_current
                    proc_args['workflow_name'] = proc_wflow_name
                    proc_args['workflow_id'] = proc_wflow_id
                    proc_args['group_id'] = group_id
                    # collect results from other workflow(s) if available
                    proc_args['results_history'] = self.result_history

                    # run process (return obj or dict just to test for iterations ... )
                    proc_result, proc_memory = proc_obj.run(time, **proc_args)

                    # manage history process
                    if not proc_dump:

                        # check expected process results
                        available_variables = list(proc_result.keys())
                        for variable_result in proc_results.values():
                            tag_result = f'{proc_workflow}|{variable_result}'
                            if tag_result not in available_variables:
                                error_message = (
                                    f'Variable "{tag_result}" not found in process results. '
                                    f'Available variables: {", ".join(available_variables)}'
                                )
                                self.logger.error(error_message)
                                raise RuntimeError(error_message)

                        # alert if result obj is defined by NoneType
                        if proc_result is None:
                            self.logger.warning(f'Process "{proc_fx}" ... SKIPPED. Data not available')

                        # check result format
                        if not isinstance(proc_result, dict):
                            self.logger.error(f'Process "{proc_fx}" ... FAILED')
                            raise RuntimeError(f'Object result must be defined by Dict. Found type: {type(proc_result).__name__}')

                        # store process result
                        self.process_id = proc_id
                        self.process_tag.append(proc_tag)
                        # set proc previous
                        self.process_previous = copy(proc_obj)

                        # get workflow from new result
                        proc_workflows = {key.split("|", 1)[0] for key in proc_result.keys()}

                        # remove previous entries belonging to the same workflow
                        self.process_history = {
                            key: value
                            for key, value in self.process_history.items()
                            if key.split("|", 1)[0] not in proc_workflows
                        }

                        # update with new result
                        self.process_history.update(proc_result)

                    else:
                        # dump state active the data in memory is cleared (empty dict)
                        self.process_current, self.process_previous = None, None
                        self.process_history, self.process_id, self.process_tag = {}, [], []
                        self.result_history = {}

                except Exception as exc:

                    # capture expections
                    self.logger.exception(f'Process "{proc_fx}" ... FAILED: {exc}')

                    # create obj proc_history
                    proc_history_obj_current = {proc_history_id_current: None}

                    # optional: keep history aligned with process ids
                    self.process_history[proc_wflow_name].update(proc_history_obj_current)
                    self.process_tag.append(proc_tag)

                    # propagate exception
                    raise

                finally:
                    # end process block
                    self.logger.info_down(f'Process "{proc_fx}" ... DONE')

            # end workflow end
            self.logger.info_down(f'Workflow "{proc_workflow}" ... DONE')

# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# HELPERS
# helper to create a data key
def create_key(workflow: Union[str, None] = None, variable: Union[str, None] = None, separator: str = ":",) -> str:
    # check workflow and variable
    if workflow is None or variable is None:
        raise ValueError("Workflow and variable must be defined.")
    # create key
    return separator.join([str(workflow), str(variable),])

# helper to merge by times
def _merge_by_period(times):
    return [times]

# helper to merge by process
@deprecated(since='2026-10-07')
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
