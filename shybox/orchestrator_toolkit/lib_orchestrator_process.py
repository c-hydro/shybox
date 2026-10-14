"""
Class Features

Name:          lib_orchestrator_process
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20251104'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import warnings
import datetime as dt
from copy import copy, deepcopy
from typing import Callable, Any, Dict, Iterable, List, Tuple, Union, Optional
from functools import partial
from collections import defaultdict

import re
import numpy as np
import pandas as pd
import xarray as xr

from shybox.dataset_toolkit.dataset_handler_collections import DataCollection
from shybox.generic_toolkit.lib_utils_debug import plot_data

from shybox.dataset_toolkit.dataset_handler_local import DataLocal
from shybox.dataset_toolkit.dataset_handler_memory import DataMemory
from shybox.dataset_toolkit.dataset_handler_on_demand import DataOnDemand
from shybox.logging_toolkit.logging_handler import LoggingManager

from shybox.orchestrator_toolkit.lib_orchestrator_data import ProcessorData
from shybox.orchestrator_toolkit.lib_orchestrator_data import ProcessorCollection

# common decorators
from shybox import with_logger, deprecated
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# class to handle the process
class ProcessorContainer:

    def __init__(self,
                 function: Callable,
                 args: dict = None,
                 in_obj: Union[DataLocal, dict] = None, out_obj: Union[DataLocal, dict] = None,
                 in_deps: Union[DataLocal, dict] = None, out_deps: Union[DataLocal, dict] = None,
                 in_opts: dict = None, out_opts: dict = None,
                 logger: LoggingManager = None) -> None:

        # set attributes needed by the environment
        self.ref = None
        self.workflow_name, self.workflow_id = None, None
        self.args, self.tag, self.mode, self.step = None, None, None, None
        self.valid = None
        self.mapping_in, self.mapping_out = None, None
        self.variables_in_structure, self.variables_out_structure = None, None
        self.variables_in_left, self.variables_in_right = None, None
        self.variables_out_left, self.variables_out_right = None, None

        # ensure logging always works (to console)
        self.logger = logger or LoggingManager(name="Processor")
        # set break points
        self.break_point = False

        # get static and dynamic arguments
        args_dynamic, args_static = {}, {}
        for arg_name, arg_value in args.items():

            # check format of arg values
            if isinstance(arg_value, dict):

                # iterate over keys and values
                for key, value in arg_value.items():

                    # check value format
                    if isinstance(value, DataLocal) or isinstance(value, DataOnDemand):

                        loc_pattern = value.loc_pattern
                        has_placeholder, _ = self._get_placeholders(loc_pattern)
                        if has_placeholder:
                            if arg_name not in list(args_dynamic.keys()):
                                args_dynamic[arg_name] = {}
                            args_dynamic[arg_name][key] = value
                            continue

                        if not value.is_static:
                            if arg_name not in list(args_static.keys()):
                                args_static[arg_name] = {}
                            args_dynamic[arg_name][key] = value
                        else:
                            if arg_name not in list(args_static.keys()):
                                args_static[arg_name] = {}
                            variable_template = {}
                            if hasattr(value, 'variable_template'):
                                variable_template = value.variable_template

                            # update logger (for messages consistency)
                            value.logger = self.logger.compare(value.logger)
                            # get static data
                            if isinstance(value, DataLocal):
                                args_static[arg_name][key] = value.get_data(mapping=variable_template, name=f"{arg_name}:{key}")
                            elif isinstance(value, DataOnDemand):
                                args_static[arg_name][key] = value.create_data(mapping=variable_template, name=f"{arg_name}:{key}")
                            else:
                                self.logger.error('Argument data object is not DataLocal or DataOnDemand instance')
                                raise TypeError('Argument data object is not DataLocal or DataOnDemand instance')
                    else:
                        if arg_name not in list(args_static.keys()):
                            args_static[arg_name] = {}
                        args_static[arg_name][key] = value
            else:
                args_static[arg_name] = arg_value

        # set function object
        self.fx_name = function.__name__
        self.fx_obj = partial(function, **args_static)

        # set input and output data objects
        self.in_obj, self.in_deps, self.in_opts = in_obj, in_deps, in_opts
        self.out_obj, self.out_deps, self.out_opts = out_obj, out_deps, out_opts

        # set dump state
        self.dump_state = False
        # set debug state
        self.debug_state_in = False
        self.debug_state_out = False

        # set original arguments as attributes
        args_common = {**args_static, **args_dynamic}

        # merge static and dynamic references
        ref_static, ref_dynamic = args_static.get("ref"), args_dynamic.get("ref")
        if isinstance(ref_static, dict) and isinstance(ref_dynamic, dict):

            # check overlapping reference keys
            ref_duplicates = set(ref_static) & set(ref_dynamic)

            if ref_duplicates:
                self.logger.warning(
                    f'Dynamic "ref" overwrites static subkey(s): '
                    f'{", ".join(sorted(map(str, ref_duplicates)))}'
                )
            # merge references, dynamic values take priority
            args_common["ref"] = {**ref_static, **ref_dynamic}

        # collect updated attributes
        updated_args = []
        for arg_name, arg_value in args_common.items():
            if hasattr(self, arg_name):
                updated_args.append((arg_name, type(arg_value).__name__))
            # keep original object, including DataLocal / DataOnDemand
            setattr(self, arg_name, arg_value)

        # print updated attributes
        if updated_args:
            updated_keys = ' | '.join(name for name, _ in updated_args)
            self.logger.info(f'Attributes updated by args: {updated_keys}')

        self.separator_root = '|'
        self.separator_sub = ':'

    # method to represent the object
    def __repr__(self):
        return (
            f"ProcessorContainer("
            f"fx_name={self.fx_name!r}, "
            f"mode={self.mode!r}, tag={self.tag!r}, workflow_name={self.workflow_name!r})"
        )

    # helper to get key from list of dict objects
    def get_group_key(self, fields: Union[str, Iterable[str]],) -> Union[Any, Tuple[Any, ...]]:

        if isinstance(fields, str): fields = [fields]
        values = []
        for field in fields:

            if not hasattr(self, field):
                raise AttributeError(f'Attribute "{field}" is not available in ProcessorContainer.')

            value = getattr(self, field)
            if not isinstance(value, (str, int, type(None))):
                raise TypeError( f'Attribute "{field}" must be str, int or None. Found {type(value).__name__}.')
            values.append(value)

        if len(values) == 1:
            return values[0]

        return tuple(values)

    # helper to group processes
    @staticmethod
    def group_processes(
            processes: List["ProcessorContainer"],
            fields: Union[str, Iterable[str]],) -> Dict[Any, List["ProcessorContainer"]]:

        groups = defaultdict(list)
        for process in processes:
            group_key = process.get_group_key(fields)
            groups[group_key].append(process)

        return dict(groups)

    # helper to split tag
    def split_tag(self, field_separator="|", variable_separator=":"):
        # split workflow, input and output
        workflow, vars_in, vars_out = self.tag.split(field_separator)
        # split variables
        vars_in = vars_in.split(variable_separator) if vars_in else []
        vars_out = vars_out.split(variable_separator) if vars_out else []

        return workflow, vars_in, vars_out

    # helper to get placeholders from string
    def _get_placeholders(self, value):
        # check value type
        if not isinstance(value, str): return False, []

        # get placeholders
        placeholders = re.findall(r"\{([^{}]+)\}", value)
        # print placeholders
        if placeholders:
            self.logger.info(f"Placeholders found: {placeholders}")
            return True, placeholders
        return False, []

    # method to get attribute by name
    def get_attribute(self, name: str, default: Any = None, mandatory: bool = False) -> Any:

        # check attribute name
        if not isinstance(name, str):
            raise TypeError(f'Attribute name must be a string. Found "{type(name).__name__}".')
        # get attribute
        if hasattr(self, name):
            return getattr(self, name)

        # check mandatory attribute
        if mandatory:
            self.logger.error(f'Attribute "{name}" is not available in ProcessorContainer.')
            raise AttributeError(f'Attribute "{name}" is not available in ProcessorContainer.')

        return default

    # method to run the process
    def run(self, time: Union[dt.datetime, str, pd.Timestamp], **kwargs) \
            -> tuple[Optional[Union[dict, list, None, bool]], Optional[bool]]:

        # parse timestamps
        time = _parse_time_stamps(time)

        # adjust list if only one timestamp is provided
        if isinstance(time, list):
            if len(time) == 1: time = time[0]

        # get information from process
        fx_id, fx_name, fx_args, fx_tag, fx_mode = self.step, self.fx_name, self.args, self.tag, self.mode
        fx_workflow_name, fx_workflow_id = self.workflow_name, self.workflow_id
        fx_mapping_in, fx_mapping_out = self.mapping_in, self.mapping_out
        fx_variables_in_structure = self.variables_in_structure
        fx_variables_out_structure = self.variables_out_structure
        fx_variables_in_right, fx_variables_in_left = self.variables_in_right, self.variables_in_left
        fx_variables_out_right, fx_variables_out_left = self.variables_out_right, self.variables_out_left

        # memory is active only for start process
        if fx_id > 1:
            kwargs['memory_active'] = False

        # organize data raw and names
        # use internal input object for first processes, otherwise use the output from previous processes
        if fx_id > 1:

            # check collection history key
            if 'collections_history' in kwargs:

                # get collections history
                collections_history = kwargs['collections_history']

                # get last collection step in insertion order
                collections_step = {}
                if isinstance(collections_history, dict) and collections_history:
                    collection_key = next(reversed(collections_history))
                    collections_step[collection_key] = collections_history[collection_key]
                if not collections_step:
                    self.logger.warning('Variable collection_step is undefined. Some errors may occur.')

                # select only objects belonging to the current workflow
                data_raw = {key: value for key, value in collections_step.items()
                            if key.startswith(f"{fx_workflow_name}")}

                # check workflow data availability
                if not data_raw:
                    raise RuntimeError(
                        f'Workflow "{fx_workflow_name}" is not available '
                        f'in collections_history. '
                        f'Available keys: {list(collections_history.keys())}'
                    )

            else:
                raise RuntimeError('Previous process did not return anything. Exit')
        else:
            data_raw = self.in_obj

        # organize data collections
        data_collection = ProcessorData.organize(data_raw)
        # add deps to data
        data_collection = ProcessorData.add_dependencies(data_collection=data_collection, deps=self.in_deps,)
        # get data roles
        data_roles = ProcessorData.get_roles(data_collection)
        # get data variables
        data_variables = ProcessorData.get_variables(data_collection)
        # add time to data
        data_collection = ProcessorData.manage_time(data_collection=data_collection, time=time)

        # get attrs message
        message = data_collection.get_attr_by_name(name="message", remove_dups=True,)

        # order data groups
        data_groups = data_collection.order_obj()
        # compact data groups
        data_groups = ProcessorCollection.compact_obj(data_groups)

        # info process start
        self.logger.info_up(f"Run :: {fx_tag} - {message}  ... ")

        # get fx data names
        data_names = [
            mapping_obj.get('var_fx')
            for mapping_obj in fx_mapping_in.values()
            if mapping_obj.get('var_fx') is not None
        ]

        # iterate over collections
        fx_data, fx_deps, fx_metadata, fx_roles = {}, {}, {}, []
        for data_id, (data_key, data_collections) in enumerate(data_groups.items()):

            # get fx name
            data_name = data_names[data_id]
            # group datasets
            group_objects = ProcessorCollection.extract_group(group=data_collections)
            # group multiple tiles
            group_multiple_data = isinstance(data_collections, list) and len(data_collections) > 1

            # iterate over groups
            data_id, deps_id = 0, 0
            for group_id, (group_time, group_datasets, group_attrs) in enumerate(group_objects):

                # iterate over datasets
                for dset_id, dset_collections in enumerate(group_datasets):

                    # get dataset info
                    dset_role = dset_collections[0]
                    dset_tag = dset_collections[1]
                    dset_workflow = dset_collections[2]
                    dset_var = dset_collections[3]
                    dset_obj = dset_collections[4]
                    dset_time = dset_collections[5]

                    # update counter related to role
                    if dset_role == 'data':
                        data_id += 1
                    elif dset_role == 'deps':
                        deps_id += 1
                    else:
                        raise RuntimeError(f"Unknown role: {dset_role}")

                    # update logger (for messages consistency)
                    dset_obj.logger = self.logger.compare(dset_obj.logger)

                    # check dataset readability
                    status_tag, status_readable = dset_obj.is_readable()
                    status_io = dset_obj.file_io

                    # define object availability
                    is_available = (status_readable or status_tag == "template")

                    # check object status
                    if status_tag == "template":
                        self.logger.info(
                            f"Object for variable '{dset_var}' is a template. The template will be filled according to the available times.")
                    elif not status_readable:
                        if status_io == 'derived':
                            self.logger.info(
                                f"Object for variable '{dset_var}' is derived by other variables.")
                        else:
                            self.logger.warning(
                                f"Object for variable '{dset_var}' is not readable. File is not available.")
                    # store general check
                    if is_available:
                        self.logger.info(f"Check object for variable '{dset_var}' ... PASS")
                    else:
                        if status_io == 'derived':
                            self.logger.info(f"Check object for variable '{dset_var}' ... DERIVED FROM OTHERS")
                        else:
                            self.logger.info(f"Check object for variable '{dset_var}' ... SKIP")

                    # iterate over dataset times
                    for time_id, time_step in enumerate(group_time):

                        # info start
                        self.logger.info(f"Check object '{time_step}' and variable '{dset_var}' ... ")

                        # group multiple times
                        group_multiple_time = len(group_time) > 1

                        # compose key
                        if group_multiple_data:
                            if dset_role == "data":
                                dset_key = f"{dset_tag}::data|{data_id}@time|{time_id}"
                            elif dset_role == "deps":
                                dset_key = f"{dset_tag}::data|{deps_id}@time|{time_id}"
                            else:
                                self.logger.error('dset_role must be defined as "data" or "deps"')
                                raise RuntimeError(f"Unknown dset_role '{dset_role}'")
                        else:
                            dset_key = f"{dset_tag}@time|{time_id}"

                        # read data (check if data is readable or not)
                        if is_available:

                            # get object settings
                            as_is, mandatory = dset_obj.data_as_is, dset_obj.data_mandatory
                            # get object time reference (if available)
                            time_ref = getattr(dset_obj, "time_reference", None)

                            # get data
                            fx_tmp = dset_obj.get_data(time=time_step, name=dset_tag, as_is=as_is ,**kwargs)

                            # store data (readable)
                            if dset_role == "data":
                                fx_data[dset_key] = fx_tmp
                            elif dset_role == "deps":
                                fx_deps[dset_key] = fx_tmp
                            else:
                                raise RuntimeError(f"Unknown dset_type '{dset_role}'")

                            # debug data in
                            if self.debug_state_in: plot_data(fx_tmp)

                            # info end (done)
                            self.logger.info(f"Check object '{time_step}' and variable '{dset_var}' ... DONE")

                        else:

                            # store data (not readable)
                            if dset_role == "data":
                                fx_data[dset_key] = None
                            elif dset_role == "deps":
                                fx_deps[dset_key] = None
                            else:
                                raise RuntimeError(f"Unknown dset_type '{dset_role}'")

                            # info end (not readable)
                            self.logger.info(f"Check object '{time_step}' and variable '{dset_var}' ... NOT READABLE")

                        # save roles
                        fx_roles.append(dset_role)

                        # create metadata
                        if dset_role == 'data':
                            if 'fx_data' not in fx_metadata:
                                fx_metadata['fx_data'] = []
                            fx_metadata['fx_data'].append(dset_tag)
                        elif dset_role == 'deps':
                            if 'fx_deps' not in fx_metadata:
                                fx_metadata['fx_deps'] = []
                            fx_metadata['fx_deps'].append(dset_tag)

        # check if fx data is available or not
        if _is_empty_fx_data(fx_data) :
            # info process end
            self.logger.info_down(f"Run :: {fx_tag} - {message} ... SKIPPED. DATA NOT AVAILABLE")
            return None, None

        # manage memory data [DELETE ?]
        fx_memory = None
        if fx_id == 0:
            if isinstance(data_raw, list):
                # normalize data to list of DataLocal (if needed)
                data_list = _normalize_local_data(data_raw)
                # check memory data for each data object
                fx_memory = [_get_memory_data(data_tmp) for data_tmp in data_list]
            else:
                fx_memory = _get_memory_data(data_raw)

        # collect and prepare function arguments
        fx_deps_args = {
            'time': time, 'time_reference': time_ref,
            'keys': None, 'roles': fx_roles,
            'mapping_in': fx_mapping_in, 'mapping_out': fx_mapping_out,
            'variable_in': fx_variables_in_structure.get('groups', None),
            'variable_out': fx_variables_out_structure.get('groups', None),
        }

        # info organize refs start
        self.logger.info(f"Organize reference object(s) ... ")

        # manage reference argument and add it to fx_args
        ref_obj = getattr(self, 'ref', None)
        # check reference object
        if isinstance(ref_obj, dict):
            # remove generic ref argument because its content will be expanded
            fx_deps_args.pop('ref', None)
            # add reference arguments
            for key, value in ref_obj.items():
                if key not in fx_deps_args:
                    if isinstance(value, DataLocal):
                        has_placeholder, _ = self._get_placeholders(value.loc_pattern)
                        if not has_placeholder:
                            value = value.get_data()
                    fx_deps_args[key] = value
                else:
                    self.logger.warning(
                        f"Key '{key}' in 'ref' is already present in fx_args. The value from 'ref' will be ignored.")

        elif isinstance(ref_obj, (list, tuple, set)):
            self.logger.error("Reference value cannot be a list, tuple, or set.")
            raise TypeError("Reference value cannot be a list, tuple, or set.")
        else:
            # add single reference object (also None if not defined)
            if isinstance(ref_obj, DataLocal):

                has_placeholder, _ = self._get_placeholders(ref_obj.loc_pattern)
                if not has_placeholder:
                    ref_obj = ref_obj.get_data()

            fx_deps_args['ref'] = ref_obj

        # info organize refs end
        self.logger.info(f"Organize reference object(s) ... DONE")

        # info organize deps start
        self.logger.info(f"Organize deps object(s) ... ")
        # iterate over deps
        for deps_key, deps_data in fx_deps.items():

            # parse dependency tag
            deps_tag = _parse_tag(deps_key)

            # get variable information
            deps_variable = deps_tag["variable"]
            deps_data_idx = deps_tag["data_idx"] or deps_tag["variable_idx"]
            deps_time_idx = deps_tag["time_idx"]

            # build nested key when both indexes are available
            if deps_data_idx is not None and deps_time_idx is not None:

                # build tag
                deps_nested = _build_tag(variable="data", data_idx=deps_data_idx, time_idx=deps_time_idx)

                # remove initial variable name from nested key
                deps_nested = deps_nested.split("::", 1)[-1]

                fx_deps_args.setdefault(deps_variable, {})
                fx_deps_args[deps_variable][deps_nested] = deps_data

            else:
                fx_deps_args[deps_variable] = deps_data

        # info organize deps end
        self.logger.info(f"Organize deps object(s) ... DONE")

        # info organize data start
        self.logger.info(f"Organize data object(s) ... ")

        # remap fx data keys using mapping information
        self.logger.info("Organize data object(s) ... ")
        fx_variable_workflow, fx_data_remapped = {}, {}
        for mapping_key, mapping_obj in fx_mapping_in.items():

            mapping_collection = mapping_obj.get("collection")
            mapping_var_fx = mapping_obj.get("var_fx")
            mapping_var_workflow = mapping_obj.get("var_workflow")

            for fx_data_key, fx_data_obj in fx_data.items():

                if mapping_collection is not None and mapping_collection not in fx_data_key:
                    continue

                # parse input tag
                fx_data_tag = _parse_tag(fx_data_key)

                # get indexes
                fx_data_idx = fx_data_tag["data_idx"] or fx_data_tag["variable_idx"]
                fx_time_idx = fx_data_tag["time_idx"]

                # build output tag
                mapping_var_fx_idx = _build_tag(variable=mapping_var_fx, data_idx=fx_data_idx, time_idx=fx_time_idx)

                # store mapped object
                fx_data_remapped[mapping_var_fx_idx] = fx_data_obj
                fx_variable_workflow[mapping_var_fx_idx] = mapping_var_workflow

                # continue for multiple variables
                if fx_data_idx is not None:
                    continue

        # return remap and group variables to the fx_data
        fx_data = _group_tag(fx_data_remapped)

        # get variables args and workflow
        fx_variable_in = fx_deps_args['variable_in']
        fx_variable_out = fx_deps_args['variable_out']

        # manage None data using dependencies
        deps_to_data = False
        if isinstance(fx_data, dict):
            for fx_key, fx_value in list(fx_data.items()):
                # check NoneType values
                if fx_value is None and fx_key in fx_deps_args:
                    if deps_to_data:
                        # move dependency value into data
                        fx_data[fx_key] = fx_deps_args.pop(fx_key)
                    else:
                        # remove from data and keep dependency in fx_deps_args
                        fx_data.pop(fx_key)

        # info organize data end
        self.logger.info(f"Organize data object(s) ... DONE")

        # info process data start
        self.logger.info(f"Process data ...")
        # add fx args
        fx_deps_args.update(fx_args)
        # execute fx
        fx_result = self.fx_obj(data=fx_data, **fx_deps_args)

        # manage output object
        if isinstance(fx_result, dict):

            # iterate over output variables
            fx_collections_results, variable_collections_result = {}, []
            for id_out, (fx_key, fx_step) in enumerate(fx_result.items()):

                # get variable name
                if len(fx_variables_out_right) == 1:
                    fx_variable_out = fx_variables_out_right[0]
                else:
                    fx_variable_out = fx_variables_out_right[id_out]

                # compose attrs
                obj_attrs = {"workflow": fx_workflow_name, "variable_names": fx_variable_out,}

                # DataArray
                if isinstance(fx_step, xr.DataArray):
                    # organize object
                    fx_step.attrs.update(obj_attrs)
                    fx_step.name = fx_variable_out
                    # organize collections
                    fx_collections_results[fx_key] = copy(fx_step)
                    variable_collections_result.append(fx_variable_out)
                # DataFrame
                elif isinstance(fx_step, pd.DataFrame):
                    # organize object
                    fx_step.attrs.update(obj_attrs)
                    fx_step.name = fx_variable_out
                    # organize collections
                    fx_collections_results[fx_key] = copy(fx_step)
                    variable_collections_result.append(fx_variable_out)
                # Dataset
                elif isinstance(fx_step, xr.Dataset):
                    # organize object
                    fx_step.attrs.update(obj_attrs)
                    # organize collections
                    fx_collections_results[fx_key] = copy(fx_step)
                    variable_collections_result.append(fx_variable_out)
                # Collection
                elif isinstance(fx_step, dict):
                    # organize object
                    fx_step["attrs"] = obj_attrs
                    if fx_collections_results is None:
                        fx_collections_results = {}
                    # organize collections
                    fx_collections_results[fx_key] = fx_step
                    variable_collections_result.append(fx_variable_out)

                # Unsupported
                else:
                    # get object type
                    obj_type = type(fx_step).__name__
                    # exit with errors
                    self.logger.error(f'Variable output "{fx_key}" has unsupported type "{obj_type}".')
                    raise TypeError(f'Variable output "{fx_key}" has unsupported type "{obj_type}".')

        else:

            # get result type
            result_type = type(fx_result).__name__
            # error for unknown type
            self.logger.error(f"Variable result has unsupported type {result_type}.")
            raise TypeError("Variable result must be an dictionary")

        # organize attributes
        fx_attrs = {
            'variable_names': variable_collections_result,
            'workflow': fx_variable_in, "variable_out": fx_variable_out,
        }

        # info process data end
        self.logger.info(f"Process data ... DONE")

        # check output object
        if not isinstance(self.out_obj, dict):
            raise TypeError(
                f'Output object must be a dictionary. Found "{type(self.out_obj).__name__}".')
        # only one output object is supported
        if len(self.out_obj) != 1:
            raise RuntimeError(
                f'Only one output object is supported. Found {len(self.out_obj)} objects: {list(self.out_obj.keys())}')

        # flatten output object
        out_key, out_handler = next(iter(self.out_obj.items()))

        # info process end
        self.logger.info_down(f"Run :: {fx_tag} - {message} ... DONE")

        # check the dump state (if active dump the collections instead of the variable)
        if not self.dump_state:

            # info dump variable start
            self.logger.info_up(f"Dump :: step :: {fx_tag} - {message} [mode: {self.mode}] ... ")

            # add time_format to kwargs
            kwargs['time_format'] = out_handler.get_attribute('time_format')
            # add reference object(s) to kwargs
            if getattr(self, 'ref', None) is not None:
                ref_obj = self.ref
                if isinstance(ref_obj, dict):
                    for key, value in ref_obj.items():
                        kwargs[key] = value
                elif ref_obj is not None:
                    kwargs['ref'] = ref_obj
            # add out_options to kwargs
            if getattr(self, 'out_opts', None) is not None:
                kwargs['out_opts'] = self.out_opts

            # save the data
            out_handler.write_data(fx_collections_results, time, metadata=fx_attrs, **kwargs)

            # create data memory objects
            fx_collections_history = {}
            for fx_variable_step, (fx_key, fx_data) in zip(variable_collections_result, fx_collections_results.items()):

                # create memory object
                fx_obj_tmp = DataMemory(
                    data=fx_data,
                    file_variable=fx_variable_step,
                    file_workflow=fx_workflow_name,
                    file_io='memory',
                    data_role='data',
                    logger=self.logger
                )

                # create tag step
                fx_tag_step = fx_workflow_name + '|' + fx_variable_step
                # store object by variable name
                fx_collections_history[fx_tag_step] = fx_obj_tmp

            # info dump variable end
            self.logger.info_down(f"Dump :: step :: {fx_tag} - {message} [mode: {self.mode}] ... DONE")

            return fx_collections_history, fx_memory

        # dump state if required from the process (if collections are available)
        elif self.dump_state:

            # info dump start
            self.logger.info_up(f"Dump :: collections :: {fx_tag} - {message} [mode: {self.mode}] ... ")

            # get data and metadata from memory
            fx_collections_tags = kwargs.pop('collections_tags', [])
            fx_collections_history = kwargs.pop('collections_history', {})
            fx_collections_metadata = kwargs.pop('collections_metadata', {})

            # check fx mode
            if fx_mode == 'end' or fx_mode == 'unique':

                # check fx_save format
                if not isinstance(fx_collections_results, dict):
                    raise RuntimeError('Dataset in fx_collections_results must be defined by dictionary. Check fx decorators.')

                # check if unique variable names match the number of results
                use_key = (len(set(variable_collections_result)) != len(fx_collections_results))

                # remove old workflow items
                fx_collections_history = {
                    key: value
                    for key, value in fx_collections_history.items()
                    if key.split("|", 1)[0] != fx_workflow_name
                }

                # add results directly to collections
                for fx_variable_step, (fx_key_step, fx_data_step) in zip(variable_collections_result, fx_collections_results.items()):

                    # create name step
                    fx_name_step = fx_workflow_name + '|' + fx_variable_step

                    # manage key subdict
                    if use_key:
                        fx_collections_step = {fx_key_step: fx_data_step}
                    else:
                        fx_collections_step = fx_data_step

                    # create key if not exists
                    if fx_name_step not in list(fx_collections_history.keys()):
                        fx_collections_history[fx_name_step] = {}

                    # store object by variable name
                    if use_key:
                        fx_collections_history[fx_name_step].update(fx_collections_step)
                    else:
                        fx_collections_history[fx_name_step] = fx_collections_step

                # add results directly to collections
                fx_collections_tags.append(fx_tag)

                # get args from out kwargs
                if fx_tag not in self.out_obj:
                    self.logger.warning(f'Output tag "{fx_tag}" is not defined in out_obj.')
                    out_kwargs = {}
                else:
                    out_kwargs = self.out_obj[fx_tag]

                # set kwargs
                kwargs['time_format'] = getattr(out_kwargs, 'time_format', '%Y-%m-%d %H:%M:%S')
                kwargs['units'] = getattr(out_kwargs, 'units', {})
                variable_template = getattr(out_kwargs, 'variable_template', {}) or {}
                kwargs['map'] = variable_template.get('vars_data', {})

                # add reference object(s) to kwargs from self
                if getattr(self, 'ref', None) is not None:
                    ref_obj = self.ref
                    if isinstance(ref_obj, dict):
                        for key, value in ref_obj.items():
                            if isinstance(value, DataMemory) or isinstance(ref_obj, DataLocal):
                                value = value.get_data()
                            kwargs[key] = value
                    elif ref_obj is not None:
                        if isinstance(ref_obj, DataMemory) or isinstance(ref_obj, DataLocal):
                            ref_obj = ref_obj.get_data()
                        kwargs['ref'] = ref_obj
                # add out_options to kwargs from self
                kwargs['out_opts'] = getattr(self, 'out_opts', {}) or {}

                # organize data and variable(s)
                fx_collections_output, fx_variable_output = {}, []
                for fx_key, fx_obj in fx_collections_history.items():
                    fx_var = fx_key.split('|')[-1]
                    if isinstance(fx_obj, DataMemory):
                        fx_data = fx_obj.get_data()
                    else:
                        fx_data = copy(fx_obj)
                    fx_collections_output[fx_var] = fx_data
                    fx_variable_output.append(fx_var)

                # define collections metadata
                fx_collections_metadata['variable_names'] = list(set(fx_variable_output))

                # save the data
                if not len(fx_collections_output) == 0:

                    # get signature to use in write_data (if time signature is defined in the output object)
                    time_signature = out_handler.get_attribute('time_signature')
                    if time_signature is None:
                        raise RuntimeError('The time_signature is not defined in the data object.')

                    # parse time by type
                    time = _parse_time_stamps(time)
                    # parse time by signature
                    time = _parser_time_signature(time, time_signature=time_signature)

                    # write collections
                    out_handler.write_data(
                        fx_collections_output, time,
                        metadata=fx_collections_metadata, **kwargs)

                    # info dump end
                    self.logger.info_down(f"Dump :: collections :: {fx_tag} - {message} [mode: {self.mode}] ... DONE")

                    # end of processes
                    return True, False

                else:

                    # define empty collection source
                    if not fx_collections_history:
                        collection_source = 'history'
                    elif not fx_collections_output:
                        collection_source = 'selection'
                    else:
                        collection_source = 'unknown'

                    # info dump end - skipped - collections empty
                    self.logger.warning(f'Collections {collection_source} is empty. Nothing to dump.')
                    self.logger.info_down(f"Dump :: collections :: {fx_tag} - {message} [mode: {self.mode}] ... SKIPPED")

                # end of processes
                return None, False

            else:

                # info dump end - skipped - collections not found
                self.logger.warning('Process step is not defined by tag end/unique. Nothing to dump. ')
                self.logger.info_down(f"Dump :: collections :: {fx_tag} - {message} [mode: {self.mode}] ... SKIPPED")

                # end of processes
                return None, False

        else:
            # exit for errors
            self.logger.error('Dump state is not declared in the object settings. Update object to set the dump_state')
            raise ValueError('Process interrupted for unknown dump state')

# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# helpers
# helper to parse the data tag
def _parse_tag(tag: str) -> dict:
    """
    Parse a workflow tag into its components.

    Examples:
        data|WORKFLOW|VARIABLE
        data|WORKFLOW|VARIABLE|1
        data|WORKFLOW|VARIABLE::data|1@time|0
    """

    # initialize
    tag_base, tag_other, tag_time = tag, None, None

    # split time information
    if '@' in tag_base:
        tag_base, tag_time = tag_base.split('@', 1)

    # split multiple variable information
    if '::' in tag_base:
        tag_base, tag_other = tag_base.split('::', 1)

    # split base components
    tag_parts = tag_base.split('|')

    if len(tag_parts) not in (3, 4):
        raise ValueError(f'Invalid tag "{tag}". Expected 3 or 4 base elements.')

    tag_role, tag_workflow, tag_variable = tag_parts[:3]
    tag_variable_idx = tag_parts[3] if len(tag_parts) == 4 else None

    # split multiple variable index
    tag_data_idx = None
    if tag_other is not None:
        other_parts = tag_other.split('|')
        tag_data_idx = other_parts[-1] if len(other_parts) > 1 else other_parts[0]

    # split time index
    tag_time_name, tag_time_idx = None, None
    if tag_time is not None:
        tag_time_name, tag_time_idx = tag_time.split('|', 1)

    return {
        "role": tag_role, "workflow": tag_workflow, "variable": tag_variable,
        "variable_idx": tag_variable_idx, "data_idx": tag_data_idx,
        "time_name": tag_time_name, "time_idx": tag_time_idx,}

# helper to build tag
def _build_tag(variable: str, data_idx=None, time_idx=None, data_name: str = "data", time_name: str = "time") -> str:
    """Build a normalized variable tag."""
    tag = variable
    if data_idx is not None:
        tag += f"::data|{data_idx}"
    if time_idx is not None:
        tag += f"@{time_name}|{time_idx}"
    return tag

# helper to group tag
def _group_tag(data: dict, separator: str = "::", flatten_single: bool = False) -> dict:
    """Group variables by data index, time index, or unique name."""

    # behaviour
    # data@time|0 -- unique --> data: value
    # data@time|0, data@time|1 ... -- multiple --> data: {time|0: value0, time|1: value1 ...}
    # data|{id}::data|i@time|t --> data: {data|i@time|t: value}
    # data|{t}@time|t1, data|{t}@time|t2 --> data: {time|t1: value, time|t2: value ...}
    # data|q_sim_open_loop@time|0 --> data!q_sim_open_loop: value
    # code@time|0 --> code: value ; data@time|0 --> data: value

    data_grouped = {}
    repeated_groups = set()
    for data_key, data_obj in data.items():

        # split variable name and suffix
        data_variable, sep, data_suffix = data_key.partition(separator)

        # split time information
        data_base, time_sep, time_suffix = data_variable.partition("@")

        # check repeated data and time placeholders
        is_repeated_data = bool(re.search(r"[|:]\{(?:n|id)\}", data_base))
        is_repeated_time = bool(re.search(r"[|:]\{t\}", data_base))

        # remove placeholders
        data_base = re.sub(r"[|:]\{(?:n|id|t)\}", "", data_base)

        # case 1: repeated data variables
        if is_repeated_data:

            # example: data|{id}::data|1@time|0
            # output: data -> {"data|1@time|0": value}
            if not sep or not data_suffix:
                raise ValueError(
                    f'Repeated data tag "{data_key}" requires separator "{separator}" '
                    'and a non-empty suffix.'
                )

            group_key = data_base
            item_key = data_suffix
            repeated_groups.add(group_key)

        # case 2: repeated time variables
        elif is_repeated_time:

            # example: data|{t}@time|0
            # output: data -> {"time|0": value}
            if not time_sep or not time_suffix:
                raise ValueError(f'Missing time index in tag "{data_key}".')

            group_key = data_base
            item_key = time_suffix
            repeated_groups.add(group_key)

        # case 3: named variables
        elif "|" in data_base:

            # example: data|q_sim_open_loop@time|0
            # output: data!q_sim_open_loop -> value
            data_group, data_name = data_base.split("|", 1)

            group_key = f"{data_group}!{data_name}"
            item_key = time_suffix if time_sep else None

        # case 4: unique or multiple time variables
        else:

            # example: data@time|0 -> data: value
            # example: data@time|0, data@time|1 -> data: {time|0: ..., time|1: ...}
            group_key = data_base
            item_key = time_suffix if time_sep else None

        # initialize group
        data_grouped.setdefault(group_key, {})

        # check duplicate keys
        if item_key in data_grouped[group_key]:
            raise KeyError(f'Duplicate tag "{item_key}" in variable "{group_key}".')
        # store data
        data_grouped[group_key][item_key] = data_obj

    # finalize grouped variables
    for group_key, group_values in list(data_grouped.items()):

        # repeated placeholders remain dictionaries unless flattening is requested
        if group_key in repeated_groups:

            if flatten_single and len(group_values) == 1:
                data_grouped[group_key] = next(iter(group_values.values()))
            continue

        # unique variables become direct objects
        if len(group_values) == 1:
            data_grouped[group_key] = next(iter(group_values.values()))

        # multiple time steps remain dictionaries
        elif None in group_values:
            raise ValueError(f'Variable "{group_key}" mixes indexed and non-indexed values.')

    return data_grouped

# helper to test tag grouping
def _test_tag():

    test_cases = [
        # unique variable
        (
            {"data@time|0": 10},
            {"data": 10}
        ),

        # multiple time steps
        (
            {"data@time|0": 10, "data@time|1": 20},
            {"data": {"time|0": 10, "time|1": 20}}
        ),

        # repeated data indexes
        (
            {
                "data|{id}::data|1@time|0": 10,
                "data|{id}::data|2@time|0": 20
            },
            {
                "data": {
                    "data|1@time|0": 10,
                    "data|2@time|0": 20
                }
            }
        ),

        # repeated time indexes
        (
            {
                "data|{t}@time|0": 10,
                "data|{t}@time|1": 20
            },
            {
                "data": {
                    "time|0": 10,
                    "time|1": 20
                }
            }
        ),

        # named variable
        (
            {"data|q_sim_open_loop@time|0": 10},
            {"data!q_sim_open_loop": 10}
        ),

        # multiple unique variables
        (
            {"code@time|0": 10, "data@time|0": 20},
            {"code": 10, "data": 20}
        )
    ]

    # execute tests
    for test_id, (data_in, data_expected) in enumerate(test_cases, start=1):

        data_out = _group_tag(data_in)

        assert data_out == data_expected, (
            f"Test {test_id} failed.\n"
            f"Expected: {data_expected}\n"
            f"Result: {data_out}"
        )

        print(f"Test {test_id} ... OK")

    # test flatten_single
    data_out = _group_tag(
        {"data|{id}::data|1@time|0": 10},
        flatten_single=True
    )

    assert data_out == {"data": 10}

    print("Test flatten_single ... OK")
    print("All tag grouping tests passed.")

# method to get data from memory (USE OR NOT?)
@deprecated(since='2026-10-08')
def _get_memory_data(obj):
    if hasattr(obj, "memory_data"):
        return obj.memory_data
    else:
        print(f"'memory_data' not available for object of type {type(obj).__name__}")
        return None

# method to normalize local data
def _normalize_local_data(data_raw):
    normalized = []
    if isinstance(data_raw, (list, tuple)):
        for item in data_raw:
            if isinstance(item, (list, tuple)):
                normalized.extend(_normalize_local_data(item))
            else:
                normalized.append(item)
    else:
        normalized.append(data_raw)

    return normalized

# check if data is available
@with_logger(var_name="logger_stream")
def _is_empty_fx_data(fx_data):
    """Return True if fx_data is None, empty, or contains only None / empty data structures."""
    # Case 1: None
    if fx_data is None:
        return True
    # Case 2: Empty list, dict, or tuple
    if isinstance(fx_data, (list, dict, tuple)) and not fx_data:
        return True
    # Case 3: List of only None or empty elements
    if isinstance(fx_data, list):
        if all(_is_empty_fx_data(el) for el in fx_data):
            return True
    # Case 4a: Empty Dataset
    if isinstance(fx_data, xr.Dataset):
        if len(fx_data.data_vars) == 0 or all(v.size == 0 for v in fx_data.data_vars.values()):
            return True
    # Case 4b: Empty DataArray
    if isinstance(fx_data, xr.DataArray):
        if fx_data.size == 0:
            return True
    # Case 5: Empty numpy array or pandas object (optional)
    if isinstance(fx_data, np.ndarray) and fx_data.size == 0:
        return True
    if isinstance(fx_data, (pd.Series, pd.DataFrame)) and fx_data.empty:
        return True
    return False

# helper to parse time
@with_logger(var_name="logger_stream")
def _parse_time_stamps(time: Union[pd.Timestamp, list[pd.Timestamp]]) -> list[pd.Timestamp]:

    if isinstance(time, pd.Timestamp):
        return [time]

    if isinstance(time, list):
        if not time:
            logger_stream.error('Time list is empty')
            raise ValueError('Time list is empty')
        if not all(isinstance(time_step, pd.Timestamp) for time_step in time):
            logger_stream.error('One or more time values are not pd.Timestamp')
            raise ValueError('All time values must be pd.Timestamp')
        return time

    logger_stream.error(f'Time format "{type(time).__name__}" is not supported')
    raise TypeError('Time must be a pd.Timestamp or a list of pd.Timestamp')

# helper to parse time according to signature
@with_logger(var_name='logger_stream')
def _parser_time_signature(time: list[pd.Timestamp], time_signature: str) \
        -> pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | list[pd.Timestamp]:

    # signatures keeping original time
    if time_signature in ('step', 'current'):
        return time

    # signatures requiring time values
    if time_signature in ('range', 'start/end', 'period', 'start', 'end'):
        if not time:
            logger_stream.error(f'Time cannot be empty for time signature "{time_signature}".')
            raise ValueError(f'Time cannot be empty for time signature "{time_signature}".')

    # organize time
    if time_signature in ('range', 'start/end', 'period'):
        return time[0], time[-1]
    elif time_signature == 'start':
        return time[0]
    elif time_signature == 'end':
        return time[-1]

    # unsupported signature
    logger_stream.error(f'Unknown time signature "{time_signature}" in output object.')
    raise ValueError(f'Unknown time signature "{time_signature}" in output object.')
# ----------------------------------------------------------------------------------------------------------------------
