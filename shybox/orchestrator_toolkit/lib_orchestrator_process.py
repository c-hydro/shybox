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
from copy import deepcopy
from typing import Callable, Any, Dict, Iterable, List, Tuple, Union, Optional
from functools import partial
from collections import defaultdict

import re
import numpy as np
import pandas as pd
import xarray as xr

from shybox.geo_toolkit.lib_geo_coords import match_coords_to_reference
from shybox.geo_toolkit.lib_geo_watersheds import import_pysheds, is_pysheds_raster
from shybox.generic_toolkit.lib_utils_debug import plot_data

from shybox.dataset_toolkit.dataset_handler_local import DataLocal
from shybox.dataset_toolkit.dataset_handler_on_demand import DataOnDemand
from shybox.logging_toolkit.logging_handler import LoggingManager

from shybox.logging_toolkit.lib_logging_utils import with_logger

from shybox.orchestrator_toolkit.lib_orchestrator_data import ProcessorData
from shybox.orchestrator_toolkit.lib_orchestrator_data import ProcessorCollection
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
        self.workflow_name, self.workflow_id = None, None
        self.tag, self.mode, self.step = None, None, None
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
                            args_dynamic[arg_name] = value
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
                                args_static[arg_name][key] = value.get_data(mapping=variable_template, name=f"{arg_name}_{key}")
                            elif isinstance(value, DataOnDemand):
                                args_static[arg_name][key] = value.create_data(mapping=variable_template, name=f"{arg_name}_{key}")
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
        for arg_name, arg_value in args_common.items():
            # warn if attribute already exists
            if hasattr(self, arg_name):
                self.logger.info(f'Attribute "{arg_name}" is updated by the args value.')
            # keep original object, including DataLocal / DataOnDemand
            setattr(self, arg_name, arg_value)

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
        time = _parse_timestamps(time)

        # adjust list if only one timestamp is provided
        if isinstance(time, list):
            if len(time) == 1: time = time[0]

        # get information from process
        fx_id, fx_name, fx_tag, fx_mode = self.step, self.fx_name, self.tag, self.mode
        fx_workflow_name, fx_workflow_id = self.workflow_name, self.workflow_id
        fx_mapping_in, fx_mapping_out = self.mapping_in, self.mapping_out
        fx_variables_in_structure = self.variables_in_structure
        fx_variables_out_structure = self.variables_out_structure
        fx_variables_in_right, fx_variables_in_left = self.variables_in_right, self.variables_in_left
        fx_variables_out_right, fx_variables_out_left = self.variables_out_right, self.variables_out_left

        ## CHECK MEMORUY BLOCK AND ACTIVE ...
        # organize data raw and names (internal data object or from memory)
        if fx_id == 0 and 'memory_block' in kwargs:
            if (fx_workflow_name is not None) and (fx_workflow_name in kwargs['memory']):
                data_raw = kwargs['memory_block'][fx_workflow_name]
            else:
                data_raw = self.in_obj
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

        # memory is active only for start process
        if fx_id != 0:
            kwargs['memory_active'] = False

        # iterate over collections
        fx_data, fx_deps, fx_metadata, fx_roles = {}, {}, {}, []
        for group_key, group_collections in data_groups.items():

            # group datasets
            group_objects = ProcessorCollection.extract_group(group=group_collections)

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
                    folder_name, file_name = dset_obj.dir_name, dset_obj.file_name

                    # define object availability
                    is_available = (status_readable or status_tag == "template")

                    # check object status
                    if status_tag == "template":
                        self.logger.info(
                            f"Object for variable '{dset_var}' is a template. The template will be filled according to the available times.")
                    elif not status_readable:
                        self.logger.warning(
                            f"Object for variable '{dset_var}' is not readable. File is not available.")
                    # store general check
                    if is_available:
                        self.logger.info(f"Check object for variable '{dset_var}' ... PASS")
                    else:
                        self.logger.info(f"Check object for variable '{dset_var}' ... SKIP")

                    # iterate over dataset times
                    for time_step in group_time:

                        # info start
                        self.logger.info(f"Check object '{time_step}' and variable '{dset_var}' ... ")

                        # read data (check if data is readable or not)
                        if is_available:

                            # get object settings
                            as_is, mandatory = dset_obj.data_as_is, dset_obj.data_mandatory
                            # get object time reference (if available)
                            time_ref = getattr(dset_obj, "time_reference", None)

                            # get data
                            fx_tmp = dset_obj.get_data(time=time_step, name=dset_tag, as_is=as_is ,**kwargs)

                            if dset_role == 'data':
                                fx_data[f'{dset_tag}::{data_id}'] = fx_tmp
                            elif dset_role == 'deps':
                                fx_deps[f'{dset_tag}::{deps_id}'] = fx_tmp
                            else:
                                raise RuntimeError(f"Unknown dset_type '{dset_role}'")

                            # debug data in
                            if self.debug_state_in: plot_data(fx_tmp)
                        else:

                            self.logger.info(f"Object '{time_step}' is not readable ... SKIP")

                            if dset_role == 'data':
                                fx_data[f'{dset_tag}::{data_id}'] = None
                            elif dset_role == 'deps':
                                fx_deps[f'{dset_tag}::{deps_id}'] = None
                            else:
                                raise RuntimeError(f"Unknown dset_type '{dset_role}'")

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

        # manage memory data
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
        fx_args = {
            'time': time, 'time_reference': time_ref,
            'keys': None, 'roles': fx_roles,
            'mapping_in': fx_mapping_in, 'mapping_out': fx_mapping_out,
            'variable_in': fx_variables_in_structure.get('groups', None),
            'variable_out': fx_variables_out_structure.get('groups', None),
        }

        # manage reference argument and add it to fx_args
        ref_obj = getattr(self, 'ref', None)
        # check reference object
        if isinstance(ref_obj, dict):
            # remove generic ref argument because its content will be expanded
            fx_args.pop('ref', None)
            # add reference arguments
            for key, value in ref_obj.items():
                if key not in fx_args:
                    fx_args[key] = value
                else:
                    self.logger.warning(
                        f"Key '{key}' in 'ref' is already present in fx_args. The value from 'ref' will be ignored.")

        elif isinstance(ref_obj, (list, tuple, set)):
            self.logger.error("Reference value cannot be a list, tuple, or set.")
            raise TypeError("Reference value cannot be a list, tuple, or set.")
        else:
            # add single reference object (also None if not defined)
            fx_args['ref'] = ref_obj

        # add deps
        for deps_key, deps_data in fx_deps.items():

            # split dependency key
            deps_role, deps_workflow, deps_variable = deps_key.split('|')
            # check multiple variable suffix, e.g. variable::1
            if '::' in deps_variable:

                deps_variable_base, deps_counter = deps_variable.rsplit('::', 1)
                if deps_counter.isdigit():
                    # initialize variable container
                    if deps_variable_base not in fx_args:
                        fx_args[deps_variable_base] = {}
                    # store dependency by counter
                    fx_args[deps_variable_base][deps_counter] = deps_data

                else:
                    fx_args[deps_variable] = deps_data
            else:
                fx_args[deps_variable] = deps_data

        # remap fx data keys using mapping information
        fx_variable_workflow, fx_data_remapped = {}, {}
        for mapping_key, mapping_obj in fx_mapping_in.items():

            # get collection and target fx variable
            mapping_collection = mapping_obj.get('collection')
            mapping_var_fx = mapping_obj.get('var_fx')
            mapping_var_workflow = mapping_obj.get('var_workflow')

            # search collection in fx_data keys
            for fx_data_key, fx_data_obj in fx_data.items():

                if mapping_collection in fx_data_key:
                    # check for indexed key, e.g. "...::1", "...::2", ...
                    if '::' in fx_data_key:

                        fx_data_base, fx_data_idx = fx_data_key.rsplit('::', 1)
                        if fx_data_idx.isdigit():
                            mapping_var_fx_idx = f'{mapping_var_fx}::{fx_data_idx}'
                            fx_data_remapped[mapping_var_fx_idx] = fx_data_obj
                            fx_variable_workflow[mapping_var_fx_idx] = mapping_var_workflow
                            # do not break: other ::N entries may exist
                            continue

                    # standard case without ::N
                    fx_data_remapped[mapping_var_fx] = fx_data_obj
                    fx_variable_workflow[mapping_var_fx] = mapping_var_workflow
                    break

        # return remap variables to the fx_data
        fx_data = fx_data_remapped

        # get variables args and workflow
        fx_variable_in = fx_args['variable_in']
        fx_variable_out = fx_args['variable_out']

        # run function to process data
        fx_save = self.fx_obj(data=fx_data, **fx_args)

        # organize attributes
        fx_attrs = {
            'variable_names': fx_variables_out_right,
            'workflow': fx_variable_in, "variable_out": fx_variable_out,
        }

        # manage output object
        if isinstance(fx_save, xr.DataArray):
            # update attributes
            fx_save.attrs.update(fx_attrs)
        elif isinstance(fx_save, xr.Dataset):
            # update attributes
            fx_save.attrs.update(fx_attrs)
        elif isinstance(fx_save, pd.DataFrame):
            # update attributes
            fx_save.attrs.update(fx_attrs)
        elif isinstance(fx_save, dict):

            # iterate over output variables
            for fx_key, fx_obj in fx_save.items():
                # manage supported objects
                if isinstance(fx_obj, (xr.DataArray, xr.Dataset, pd.DataFrame)):
                    # define object-specific attributes
                    obj_attrs = {"workflow": fx_workflow_name, "variable_names": fx_variables_out_right,}
                    # update attributes
                    fx_obj.attrs.update(obj_attrs)
                else:
                    self.logger.error(
                        f'Output variable "{fx_key}" has unsupported type {type(fx_obj).__name__}".')
                    raise TypeError(
                        f'Output variable "{fx_key}" must be an xarray DataArray, xarray Dataset or pandas DataFrame.')
        else:

            # error for unknown type
            self.logger.error(f"fx_save has unsupported type {type(fx_save).__name__}.")
            raise TypeError("fx_save must be an xarray DataArray, xarray Dataset, "
                            "pandas DataFrame or dictionary of supported objects.")

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

        # organize extra args
        kwargs['time_format'] = out_handler.get_attribute('time_format')
        if getattr(self, 'ref', None) is not None:
            kwargs['ref'] = self.ref
        if getattr(self, 'out_opts', None) is not None:
            kwargs['out_opts'] = self.out_opts

        # info process end
        self.logger.info_down(f"Run :: {fx_tag} - {message} ... DONE")

        # check the dump state (if active dump the collections instead of the variable)
        if not self.dump_state:

            # info dump variable start
            self.logger.info_up(f"Dump :: step :: {fx_tag} - {message} [mode: {self.mode}] ... ")

            # save the data
            out_handler.write_data(fx_save, time, metadata=fx_attrs, **kwargs)

            # arrange data to keep the data array format
            if isinstance(fx_save, xr.DataArray):
                fx_out = deepcopy(fx_save)
                fx_names = [fx_save.name] if fx_save.name else ["unnamed_var"]

            elif isinstance(fx_save, xr.Dataset):

                fx_var_list = list(fx_save.data_vars)
                if len(fx_var_list) == 1:
                    var_name = fx_variables_out_right[0]
                    fx_out = fx_save[var_name]
                    fx_out.name = var_name
                    fx_names = [var_name]
                else:
                    fx_out = fx_save[fx_var_list]
                    fx_names = fx_var_list

            elif isinstance(fx_save, pd.DataFrame):
                fx_names = fx_save.name

            else:
                self.logger.error("Unknown fx output type")
                raise ValueError("Unknown fx output type")

            # save variable names
            fx_out.attrs["variable_names"] = fx_attrs['variable_names']

            # info dump variable end
            self.logger.info_down(f"Dump :: step :: {fx_tag} - {message} [mode: {self.mode}] ... DONE")

            return fx_out, fx_memory

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

                # add results directly to collections
                fx_collections_history.update(fx_save)
                fx_collections_tags.append(fx_tag)

                # collections kwargs
                kwargs['time_format'] = getattr(self.out_obj, 'time_format', '%Y-%m-%d %H:%M:%S')
                kwargs['units'] = getattr(self.out_obj, 'units', {})

                variable_template = getattr(self.out_obj, 'variable_template', {}) or {}
                kwargs['map'] = variable_template.get('vars_data', {})
                kwargs['ref'] = getattr(self, 'ref', None)
                kwargs['out_opts'] = getattr(self, 'out_opts', {}) or {}

                fx_collections_metadata['variable_names'] = fx_attrs['variable_names']

                # save the data
                if not len(fx_collections_history) == 0:

                    # get signature to use in write_data (if time signature is defined in the output object)
                    time_signature = out_handler.get_attribute('time_signature')
                    if time_signature is None:
                        raise RuntimeError('The time_signature is not defined in the data object.')

                    # parse timestamps
                    time = _parse_timestamps(time)

                    # check time mode
                    if time_signature in ('step', 'current'):
                        pass
                    elif time_signature in ('range', 'start/end', 'period'):
                        if not time:
                            raise ValueError(f'Time cannot be empty for time signature "{time_signature}".')
                        time = (time[0], time[-1])
                    elif time_signature == 'start':
                        if not time:
                            raise ValueError('Time cannot be empty for time signature "start".')
                        time = time[0]
                    elif time_signature == 'end':
                        if not time:
                            raise ValueError('Time cannot be empty for time signature "end".')
                        time = time[-1]
                    else:
                        self.logger.error(f'Unknown time signature "{time_signature}" in output object.')
                        raise ValueError(f'Unknown time signature "{time_signature}" in output object.')

                    # write collections
                    out_handler.write_data(
                        fx_collections_history, time,
                        metadata=fx_collections_metadata, **kwargs)

                    # info dump end
                    self.logger.info_down(f"Dump :: collections :: {fx_tag} - {message} [mode: {self.mode}] ... DONE")

                    # end of processes
                    return True, False

                else:
                    # info dump end - skipped - collections empty
                    self.logger.warning('Collections is empty. Nothing to dump.')
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
from collections import OrderedDict

# helper to organize data (related to keys, types and mapping settings)
def _organize_fx_data_OLD(data_raw, keys, types, io, mapping):

    # normalize metadata
    keys_list = list(keys) if isinstance(keys, (list, tuple)) else [keys]
    types_list = list(types) if isinstance(types, (list, tuple)) else [types]
    io_list = list(io) if isinstance(io, (list, tuple)) else [io]

    # check if derived objects are defined
    has_derived = 'derived' in io_list
    # remove derived from io list
    if has_derived:
        io_list = [data_io for data_io in io_list if data_io != 'derived']

    # organize raw data as a flat list
    data_list = []
    if isinstance(data_raw, (list, tuple)):
        for data_step in data_raw:
            if isinstance(data_step, (list, tuple)):
                data_list.extend(data_step)
            else:
                data_list.append(data_step)
    else:
        data_list.append(data_raw)

    # remove derived objects
    data_selected = {}
    for data_key, data_type, data_io, data_step in zip(keys_list, types_list, io_list, data_list):

        # standard input data
        if data_io == 'input' and data_type == 'data':
            data_selected[data_key] = data_step
            continue
        # dependencies used when derived data are defined
        if data_io == 'input' and data_type == 'deps' and has_derived:
            data_selected[data_key] = data_step
            continue

    # apply argument mapping
    fx_data = {}
    for arg_name, variable_name in mapping.items():

        if variable_name not in data_selected:
            warnings.warn(
                f'Variable "{variable_name}" mapped to argument "{arg_name}" '
                f'was not found. Standard argument organization will be used.'
            )
            return data_selected

        fx_data[arg_name] = data_selected[variable_name]

    return fx_data

def _check_list_of_list_OLD(x):
    return (
        isinstance(x, list) and
        all(
            isinstance(i, list) and
            all(isinstance(j, list) for j in i) or isinstance(i, list)
            for i in x
        )
    )

def _group_by_time_OLD(time_list, data_list, data_names):
    if not (len(time_list) == len(data_list) == len(data_names)):
        raise ValueError("time_list, data_list, data_names must have same length")

    # time -> name -> list_of_data (preserves insertion order)
    grouped = OrderedDict()

    for t, d, n in zip(time_list, data_list, data_names):
        if t not in grouped:
            grouped[t] = OrderedDict()
        if n not in grouped[t]:
            grouped[t][n] = []
        grouped[t][n].append(d)

    time_unique = list(grouped.keys())

    # For each time: list of names (stable order) and corresponding list of data-lists
    names_grouped = []
    data_grouped = []
    has_repeats = False

    for t in time_unique:
        names_t = list(grouped[t].keys())                 # stable order of first appearance
        data_t = [grouped[t][n] for n in names_t]         # each is a list (may have repeats)

        # repeats exist if any (time, name) has >1 data
        if any(len(v) > 1 for v in data_t):
            has_repeats = True

        names_grouped.append(names_t)
        data_grouped.append(data_t)

    return has_repeats, time_unique, data_grouped, names_grouped

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

# method to sync variable names
@with_logger(var_name="logger_stream")
def _sync_variable_name_OLD(data, vars):

    # adjust fx_vars to list
    if not isinstance(vars, list):
        vars = [vars]

    # xr.DataArray
    if isinstance(data, xr.DataArray):
        data_vars = [data.name]
    elif isinstance(data, xr.Dataset):  # xr.Dataset
        data_vars = list(data.data_vars)
    elif isinstance(data, pd.DataFrame):
        if hasattr(data, "name") and data.name is not None:
            data_vars = [data.name]
        else:
            logger_stream.error(f"Object fx_data DataFrame has no attribute 'name' or it is None.")
            raise TypeError("Object fx_data DataFrame has no attribute 'name' or it is None.")
    elif isinstance(data, dict):
        data_vars = list(data.keys())
    elif data is None:
        logger_stream.warning(f"Object fx_data DataFrame is None. Datasets are not available.")
        data_vars = []
    else:
        logger_stream.error("Object fx_data must be an xarray [DataArray, Dataset] or pandas [DataFrame].")
        raise NotImplementedError('Case not implemented yet')

    select_vars = []
    for tmp_var in data_vars:
        if tmp_var in vars:
            if tmp_var not in select_vars:
                select_vars.append(tmp_var)
        else:
            if isinstance(data, pd.DataFrame):
                select_vars = data_vars
            else:
                logger_stream.warning(f"Variable '{tmp_var}' found in data but not in fx_vars list")

    return select_vars

# method to get variable name(s)
@with_logger(var_name="logger_stream")
def _get_variable_name_OLD(obj, mandatory = True,
                       default="undefined", indexed_default="undefined_{}"):

    # Case 1: DataArray -- grid format -- single variable
    if isinstance(obj, xr.DataArray):
        return obj.name if obj.name is not None else default

    # Case 2: Dataset -- grid format -- multiple variables
    elif isinstance(obj, xr.Dataset):

        names = []
        for i, name in enumerate(obj.data_vars):
            if name is None:
                names.append(indexed_default.format(i))
            else:
                names.append(name)
        return names

    # Case 3: DataFrame -- time-series format
    elif isinstance(obj, pd.DataFrame):

        if getattr(obj, "name", None) is None:
            obj.name = default
            logger_stream.warning(f"DataFrame name is not defined; setting default='{default}'")

        names = [obj.name]

        return names

    elif is_pysheds_raster(obj):

        # get metadata from raster object (pysheds)
        metadata = obj.metadata

        name = 'Raster'
        if 'name' in list(metadata.keys()):
            name = metadata['name']

        return name

    # Case 4: None or unknown type
    elif obj is None:

        # if variable name is mandatory, raise an error or return NoneType
        if mandatory:
            logger_stream.error("Object is None but variable name is mandatory. Cannot proceed.")
            raise ValueError("Object is None but variable name is mandatory. Cannot proceed.")
        else:
            logger_stream.warning("Object is None. Variable name will be set to 'undefined'.")
            return default

    else:
        logger_stream.error("Input must be an xarray DataArray, Dataset, DataFrame or None.")
        raise TypeError("Input must be an xarray DataArray, Dataset, DataFrame or None.")

# filter to convert dataset to dataarray if single variable
@with_logger(var_name="logger_stream")
def _to_dataarray_if_single_var_OLD(ds: xr.Dataset) -> xr.Dataset | xr.DataArray:
    """
    If `ds` has only one data variable, return it as a DataArray.
    Otherwise, return the original Dataset.
    """
    if isinstance(ds, xr.Dataset) and len(ds.data_vars) == 1:
        var_name = list(ds.data_vars)[0]
        da = ds[var_name]
        da.name = var_name  # ensure name is set
        return da
    return ds

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

# method to set name and attributes
@with_logger(var_name="logger_stream")
def _set_name_and_attrs_OLD(obj, name, ws, tag):
    """Set name (if missing) and attach metadata attributes."""
    if hasattr(obj, "name") and obj.name is None:
        obj.name = name
    obj.attrs["workflow"] = ws
    obj.attrs["tag"] = tag

# method to reduce timestamps if they are the same
@with_logger(var_name="logger_stream")
def _reduce_if_same_timestamps_OLD(timestamps):
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

# method to parse time stamps
@with_logger(var_name="logger_stream")
def _parse_timestamps(time):

    # check time information
    if isinstance(time, pd.Timestamp):
        time = [time]
    elif isinstance(time, list):
        if isinstance(time[0], pd.Timestamp):
            pass
        else:
            logger_stream.error('Time format is not pd.Timestamp in the time list')
            raise ValueError('Time format is not pd.Timestamp in the time list')
    else:
        logger_stream.error('Time format is not pd.Timestamp in the time step')
        raise ValueError('Time format is not pd.Timestamp in the time step')

    return time
# ----------------------------------------------------------------------------------------------------------------------
