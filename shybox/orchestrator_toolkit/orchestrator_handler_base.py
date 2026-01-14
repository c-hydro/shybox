"""
OrchestratorBase: shared execution engine for Grid and TimeSeries orchestrators.

Those belong to dedicated modules (e.g. mapper_handler.py, orchestrator_utils.py)
and are used by the builder classes (Grid/TimeSeries).
"""
# ----------------------------------------------------------------------------------------------------------------------
from __future__ import annotations

import os
import shutil
import tempfile
from copy import deepcopy
from collections import defaultdict
from typing import Any, Dict, List, Union

import datetime as dt
import pandas as pd
import xarray as xr

from shybox.generic_toolkit.lib_utils_string import get_filename_components
from shybox.generic_toolkit.lib_utils_tmp import ensure_folder_tmp
from shybox.time_toolkit.lib_utils_time import convert_time_format, normalize_to_datetime_index

from shybox.orchestrator_toolkit.lib_orchestrator_utils import PROCESSES
from shybox.orchestrator_toolkit.lib_orchestrator_process import ProcessorContainer

from shybox.dataset_toolkit.dataset_handler_mem import DataMem
from shybox.dataset_toolkit.dataset_handler_local import DataLocal

from shybox.logging_toolkit.logging_handler import LoggingManager
from shybox.logging_toolkit.lib_logging_utils import with_logger
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to normalise deps (args or dataset)
@with_logger(var_name='logger_stream')
def normalize_deps(deps):
    if deps is None:
        return {}
    elif isinstance(deps, dict):
        return deps
    elif isinstance(deps, (list, tuple)):
        return {i + 1: v for i, v in enumerate(deps)}
    else:
        logger_stream.error(f"Unsupported deps type: {type(deps)}")

# method to ensure list
def as_list(maybe_seq):
    if isinstance(maybe_seq, (list, tuple)):
        return list(maybe_seq), True
    return [maybe_seq], False

# method to remove none values from a list
def remove_none(lst):
    return [x for x in lst if x is not None]

# method to group processes by attribute
@with_logger(var_name='logger_stream')
def group_process(proc_list, proc_tag="reference"):
    proc_group = defaultdict(list)
    for proc in proc_list:
        if proc_tag == "reference":
            if not hasattr(proc, "reference") or proc.reference is None:
                logger_stream.error(f"Process object {proc!r} has no valid 'reference' attribute.")
            proc_group[proc.reference].append(proc)
        elif proc_tag == "workflow":
            if not hasattr(proc, "workflow") or proc.workflow is None:
                logger_stream.error(f"Process object {proc!r} has no valid 'workflow' attribute.")
            proc_group[proc.workflow].append(proc)
        elif proc_tag == "tag":
            if not hasattr(proc, "tag") or proc.tag is None:
                logger_stream.error(f"Process object {proc!r} has no valid 'tag' attribute.")
            proc_group[proc.tag].append(proc)
        else:
            logger_stream.error(f"Invalid proc_tag '{proc_tag}'. Must be 'reference', 'workflow' or 'tag'.")
    return dict(proc_group)

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

# -------------------------------------------------------------------------------------
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
        args_in: dict = None,
        args_out: dict = None,
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
            os.makedirs(tmp_root, exist_ok=True)
            self.tmp_dir = tempfile.mkdtemp(dir=tmp_root)

        self.memory_active = True
        self.mapper = mapper  # injected by builder (Grid/TS)

    # -------------------------------
    # Hooks (override in subclasses)
    # -------------------------------
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

    # -------------------------------

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

    def add_process(self, function, process_n: [int, None] = None, process_output: Union[DataLocal, xr.Dataset, dict] = None, **kwargs) -> None:

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

    def run(self, time: Union[pd.Timestamp, str, pd.DatetimeIndex], **kwargs) -> None:

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
