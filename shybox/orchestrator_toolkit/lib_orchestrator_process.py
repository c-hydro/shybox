"""
Class Features

Name:          lib_orchestrator_process
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20251104'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import datetime as dt
from copy import deepcopy
from typing import Callable
from functools import partial

import numpy as np
import pandas as pd
import xarray as xr

from shybox.generic_toolkit.lib_utils_geo import match_coords_to_reference
from shybox.generic_toolkit.lib_utils_debug import plot_data

from shybox.dataset_toolkit.dataset_handler_local import DataLocal
from shybox.dataset_toolkit.dataset_handler_on_demand import DataOnDemand
from shybox.logging_toolkit.logging_handler import LoggingManager

from shybox.logging_toolkit.lib_logging_utils import with_logger
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# class to handle the process
class ProcessorContainer:
    def __init__(self,
                 function: Callable,
                 in_obj: DataLocal,
                 args: dict = None, mapper: dict = None,
                 out_obj: (DataLocal, dict) = None,
                 in_deps: list = None, out_deps: list = None,
                 in_opts: dict = None, out_opts: dict = None,
                 tag: str = None,
                 logger: LoggingManager = None) -> None:

        # ensure logging always works (to console)
        self.logger = logger or LoggingManager(name="Processor")
        # set break points
        self.break_point = False

        # get static and dynamic arguments
        fx_args, fx_static = {}, {}
        for arg_name, arg_value in args.items():
            if isinstance(arg_value, DataLocal) or isinstance(arg_value, DataOnDemand):
                if not arg_value.is_static:
                    fx_args[arg_name] = arg_value
                else:

                    variable_template = {}
                    if hasattr(arg_value, 'variable_template'):
                        variable_template = arg_value.variable_template

                    # update logger (for messages consistency)
                    arg_value.logger = self.logger.compare(arg_value.logger)
                    # get static data
                    if isinstance(arg_value, DataLocal):
                        fx_static[arg_name] = arg_value.get_data(mapping=variable_template, name=arg_name)
                    elif isinstance(arg_value, DataOnDemand):
                        fx_static[arg_name] = arg_value.create_data(mapping=variable_template, name=arg_name)
                    else:
                        self.logger.error('Argument data object is not DataLocal or DataOnDemand instance')
                        raise TypeError('Argument data object is not DataLocal or DataOnDemand instance')

            elif isinstance(arg_value, dict):
                fx_static[arg_name] = {}
                for key, value in arg_value.items():
                    if isinstance(value, DataLocal) or isinstance(value, DataOnDemand):
                        if not value.is_static:
                            fx_args[f"{arg_name}_{key}"] = value
                        else:

                            variable_template = {}
                            if hasattr(value, 'variable_template'):
                                variable_template = value.variable_template

                            # update logger (for messages consistency)
                            value.logger = self.logger.compare(value.logger)
                            # get static data
                            if isinstance(value, DataLocal):
                                fx_static[arg_name][key] = value.get_data(mapping=variable_template, name=f"{arg_name}_{key}")
                            elif isinstance(value, DataOnDemand):
                                fx_static[arg_name][key] = value.create_data(mapping=variable_template, name=f"{arg_name}_{key}")
                            else:
                                self.logger.error('Argument data object is not DataLocal or DataOnDemand instance')
                                raise TypeError('Argument data object is not DataLocal or DataOnDemand instance')
                    else:
                        fx_static[arg_name][key] = value

            else:
                fx_static[arg_name] = arg_value

        # set process tag and workflow
        self.tag = 'generic_tag'
        if 'tag' in args:
            self.tag = args.pop('tag')
        self.workflow = 'generic_workflow'
        if 'workflow' in args:
            self.workflow = args.pop('workflow')
        # set process reference
        self.reference = ':'.join([self.tag, self.workflow])

        # set function object
        self.fx_name = function.__name__
        self.fx_obj = partial(function, **fx_static)
        # set function arguments and static data
        self.fx_static = fx_static
        self.fx_args = fx_args

        # set input and output data objects
        self.in_obj, self.in_deps, self.in_opts = in_obj, in_deps, in_opts
        self.out_obj, self.out_deps, self.out_opts = out_obj, out_deps, out_opts

        # set variables mapper
        self.mapper = mapper
        # set delimiter
        self.variable_delimiter = '#'

        # set dump state
        self.dump_state = False
        # set debug state
        self.debug_state_in = False
        self.debug_state_out = False

    # method to represent the object
    def __repr__(self):
        return f'ProcessorContainer({self.fx_name, self.reference, self.tag, self.workflow})'

    # method to run the process
    def run(self, time: (dt.datetime, str, pd.Timestamp), **kwargs) -> (None, None):

        # check time information
        if isinstance(time, pd.Timestamp):
            time = [time]
        elif isinstance(time, list):
            if isinstance(time[0], pd.Timestamp):
                pass
            else:
                self.logger.error('Time format is not pd.Timestamp in the time list')
                raise ValueError('Time format is not pd.Timestamp in the time list')
        else:
            self.logger.error('Time format is not pd.Timestamp in the time step')
            raise ValueError('Time format is not pd.Timestamp in the time step')

        # adjust list if only one timestamp is provided
        if isinstance(time, list):
            if len(time) == 1: time = time[0]

        # get information about id and variable(s)
        fx_id = kwargs['id']
        fx_variable_name, fx_variable_wf = kwargs['reference'].split(':')
        #fx_variable_wf, fx_variable_tag = kwargs['workflow'], kwargs['tag']
        fx_variable_trace = ':'.join([fx_variable_name, fx_variable_wf])

        # organize data raw and names (internal data object or from memory)
        if fx_id == 0 and 'memory' in kwargs:
            if (fx_variable_wf is not None) and (fx_variable_wf in kwargs['memory']):
                data_raw = kwargs['memory'][fx_variable_wf]
            else:
                data_raw = self.in_obj
        else:
            data_raw = self.in_obj

        # check data names (list or single)
        if isinstance(data_raw, list):
            data_names = [f"data_{i}" for i, _ in enumerate(data_raw)]
        else:
            data_names = 'data'

        # adjust data (if deps are available)
        id_deps, in_deps, names_deps = 0, [], []
        obj_vars_raw, obj_deps_raw = None, None
        if (self.in_deps is not None) and (len(self.in_deps) > 0):

            if isinstance(self.in_deps, dict):

                # get names and deps
                in_deps = list(self.in_deps.values())
                names_deps = list(self.in_deps.keys())

            elif isinstance(self.in_deps, list):

                # get deps
                list_deps = self.in_deps

                # re-organize deps for the steps before the first one (basically for file with multiple times)
                in_deps = []
                for tmp_deps in list_deps:
                    if tmp_deps in data_raw:
                        data_raw.remove(tmp_deps)
                    in_deps.append(tmp_deps)

                # flatten only if it's a list of lists
                if in_deps and isinstance(in_deps[0], (list, tuple)):
                    in_deps = [x for sub in in_deps for x in sub]
                # get names
                if isinstance(in_deps[0], dict):
                    for tmp_deps in in_deps:
                        for dep_key, dep_value in tmp_deps.items():
                            names_deps.append(dep_key)
                else:
                    names_deps = [f"dep_{i}" for i in range(len(in_deps))]

            else:
                # get deps and names type mismatch
                self.logger.error('Data object is parsed as generic object for input dependencies')
                raise TypeError('Deps object is not expected type. It must be a dict or a list.')

            # append deps to data_raw and data_names
            if isinstance(data_raw, list):
                id_deps = len(data_raw)

                if isinstance(in_deps, dict):
                    data_raw.extend(in_deps.values())

                elif isinstance(in_deps, list):
                    for item in in_deps:
                        if isinstance(item, dict):
                            data_raw.extend(item.values())
                        else:
                            data_raw.append(item)

                data_names.extend(names_deps)
            elif isinstance(data_raw, DataLocal):
                id_deps = 1
                data_raw = [data_raw] + in_deps
                data_names.extend(names_deps)
            else:
                self.logger.error('Data object is not compatible with input dependencies')
                raise TypeError('Data object is not compatible with input dependencies')

            # normalize data to list of DataLocal (if needed)
            data_list = _normalize_local_data(data_raw)
            for tmp_parser in data_list:

                data_parser, name_parser = [], []
                if isinstance(tmp_parser, dict):
                    for key, value in tmp_parser.items():
                        data_parser.append(value)
                        name_parser.append(key)
                elif isinstance(tmp_parser, DataLocal):

                    # re-organize data parser and name parser
                    data_parser = [tmp_parser]

                    # analyze file namespace to get variable and workflow for the name (if possible)
                    tmp_namespace = tmp_parser.file_namespace

                    # append data (in list format)
                    tmp_variable = tmp_namespace.get('variable')
                    tmp_workflow = tmp_namespace.get('workflow')
                    tmp_tag = ':'.join([tmp_variable, tmp_workflow])
                    name_parser.append(tmp_tag)

                elif isinstance(tmp_parser, list):
                    data_parser = tmp_parser
                else:
                    self.logger.error('Data object in the list is not a dict, list or DataLocal instance')
                    raise TypeError('Data object in the list is not a dict, list or DataLocal instance')

                for id_tmp, (name_tmp, data_tmp) in enumerate(zip(name_parser, data_parser)):

                    if not isinstance(data_tmp, DataLocal):
                        self.logger.error(
                            f'Data object {type(data_tmp)} is not a DataLocal instance'
                        )
                        raise TypeError('Data object in the list is not a DataLocal instance')

                    # append data (in list format)
                    str_variable = data_tmp.file_namespace.get('variable')
                    str_workflow = data_tmp.file_namespace.get('workflow')

                    var_tag = ':'.join([str_variable, str_workflow])

                    # initialize objects to list
                    if obj_vars_raw is None: obj_vars_raw = []
                    if obj_deps_raw is None: obj_deps_raw = []

                    obj_vars_raw.append(var_tag)
                    obj_deps_raw.append(str_variable)

        else:
            # normalize data to list of DataLocal (if needed)
            id_deps = None
            # iterate over data objects
            data_list = _normalize_local_data(data_raw)

            # store variable names (as found in the data objects)
            dict_vars_raw = {}
            for data_id, data_tmp in enumerate(data_list):
                if not isinstance(data_tmp, DataLocal):
                    self.logger.error(
                        f'Data object {type(data_tmp)} is not a DataLocal instance'
                    )
                    raise TypeError('Data object in the list is not a DataLocal instance')

                # check file namespace (variable or list of variables)
                if isinstance(data_tmp.file_namespace, list):

                    # initialize objects to dict
                    if obj_vars_raw is None: obj_vars_raw = {}
                    if obj_deps_raw is None: obj_deps_raw = {}

                    # multiple variables case
                    str_vars_raw = []
                    for file_ns in data_tmp.file_namespace:

                        str_variable = file_ns.get('variable')
                        str_workflow = file_ns.get('workflow')

                        var_tag = ':'.join([str_variable, str_workflow])
                        str_vars_raw.append(var_tag)
                    # store the list of variables for the data object
                    obj_vars_raw[data_id] = str_vars_raw.copy()

                else:

                    # initialize objects to list
                    if obj_vars_raw is None: obj_vars_raw = []
                    if obj_deps_raw is None: obj_deps_raw = []

                    # single variable case
                    str_variable = data_tmp.file_namespace.get('variable')
                    str_workflow = data_tmp.file_namespace.get('workflow')
                    # store the variable for the data object
                    var_tag = ':'.join([str_variable, str_workflow])
                    obj_vars_raw.append(var_tag)

        # manage time if data_raw is a list or single object
        if isinstance(time, list):
            if isinstance(data_raw, list):

                # compute data length (before expansion)
                data_n = len(data_raw)

                # type_raw (as you already do)
                type_raw = ['data'] * data_n
                if id_deps is not None:
                    type_raw[id_deps:] = ['deps'] * (data_n - id_deps)

                # partial id inside each repeated block: 0..data_n-1, repeated for each time
                id_partial = list(range(data_n)) * len(time)
                # global id over the expanded vectors: 0..(data_n*len(time)-1)
                id_global = list(range(data_n * len(time)))

                # expand everything consistently ---
                type_raw = type_raw * len(time)
                data_raw_expanded = data_raw * len(time)
                time_expanded = [t for t in time for _ in range(data_n)]
                data_names_expanded = [f"data_{i}" for i, _ in enumerate(data_raw_expanded)]

                # overwrite originals if you want
                data_raw = data_raw_expanded
                data_names = data_names_expanded
                time = time_expanded

            elif isinstance(data_raw, DataLocal):

                # compute data length (before expansion)
                data_n = len(data_raw)

                type_raw = ['data'] * len(time)
                data_raw = [data_raw] * len(time)

                # partial id (inside each data_raw block)
                id_partial = [list(range(data_n)) for _ in time]

                # global id across the flattened structure
                id_global = []
                gid = 0
                for _ in time:
                    id_global.append(list(range(gid, gid + data_n)))
                    gid += data_n

            else:
                self.logger.error('Data object is not compatible with time list')
                raise TypeError('Data object is not compatible with time list')

            # create data names (overwrite the previous obj to match time length)
            times_raw, obj_vars_raw = [], []
            for t, data_step in zip(time, data_raw):

                # manage variable and data names
                step_variable = data_step.file_namespace.get('variable')
                step_workflow = data_step.file_namespace.get('workflow')
                # store the variable for the data object
                step_tag = ':'.join([step_variable, step_workflow])
                obj_vars_raw.append(step_tag)

                times_raw.append(f"{t.strftime('%Y%m%d%H%M')}" if hasattr(t, "strftime") else f"{str(t)}")

        else:

            # compute data length (before expansion)
            if isinstance(data_raw, list):
                data_n = len(data_raw)
            elif isinstance(data_raw, DataLocal):
                data_n = 1
            else:
                raise TypeError('Data object is not compatible with time list')

            type_raw = ['data'] * data_n
            if id_deps is not None:
                type_raw[id_deps:] = ['deps'] * (data_n - id_deps)
            type_raw = type_raw * 1

            # ids
            id_partial = list(range(data_n))
            id_global = list(range(data_n))

            times_raw = [str(time)] * data_n

        # check if data_raw is a list and adapt time accordingly
        if isinstance(data_raw, list):
            if not isinstance(time, list):
                time = [time] * len(data_raw)

            elif isinstance(time, list):
                if len(data_raw) != len(time):
                    time = time[0] * len(data_raw)
                else:
                    pass
            else:
                self.logger.error('Time object is not compatible with data_raw')
                raise ValueError('Time object is not compatible with data_raw')

        # time string for logging
        if isinstance(time, list):
            if time[0] == time[-1]:
                time_str = f"{time[0]}"
            else:
                time_str = f"from {time[0]} to {time[-1]}"
        else:
            time_str = str(time)

        # info process start
        self.logger.info_up(
            f"Run :: {self.fx_name} - {time_str} - {fx_variable_trace} ... ")

        # memory is active only for start process
        if fx_id != 0:
            kwargs['memory_active'] = False

        # get the data (using the class signature)
        if isinstance(data_raw, list):

            # normalize data to list of DataLocal (if needed)
            data_list = _normalize_local_data(data_raw)
            type_list = _normalize_local_data(type_raw)
            time_list = _normalize_local_data(times_raw)

            '''
            # check if time step have some groups (repeated time steps) and group data and time accordingly
            data_has_group, data_time_group, data_obj_group, data_names_group = _group_by_time(time, data_list, data_names)
            type_has_group, type_time_group, type_obj_group, type_names_group = _group_by_time(time, type_list, data_names)

            if data_has_group:
                time, data_list, data_names = data_time_group, data_obj_group, data_names_group
            if type_has_group:
                type_list = type_obj_group
            '''

            # iterate over the list of data objects
            id_other = 0
            fx_data, fx_metadata, fx_deps, fx_keys, fx_maps = [], {}, [], [], {}
            fx_other, fx_varid, fx_check = {}, [], [];
            for data_partial_id, data_global_id, data_tmp, name_tmp, type_tmp, time_tmp, time_step in (
                    zip(id_partial, id_global, data_list, data_names, type_list, time_list, time)):

                # assign id to data object (partial id for the repeated time steps, global id for the whole list)
                if data_partial_id == data_global_id:
                    data_id = data_global_id
                else:
                    data_id = data_partial_id

                # define the variable name to read
                if isinstance(obj_vars_raw, list):
                    str_var_tmp = obj_vars_raw[data_id]
                elif isinstance(obj_vars_raw, dict):
                    list_var_tmp = obj_vars_raw.get(data_id)
                    if fx_variable_trace in list_var_tmp:
                        str_var_tmp = fx_variable_trace
                    else:
                        self.logger.error(f'Variable trace {fx_variable_trace} does not exist in the data object')
                        raise ValueError(f'Variable trace {fx_variable_trace} does not exist in the data object')

                else:
                    self.logger.error('obj_vars_raw must be a list or a dict')
                    raise TypeError('obj_vars_raw must be a list or a dict')

                # check nested list
                key_tmp = None
                if isinstance(data_tmp, list):
                    if len(data_tmp) == 1:
                        data_tmp = data_tmp[0]
                        key_tmp = name_tmp
                    else:
                        self.logger.error('Nested lists of data objects are not supported')
                        raise ValueError('Nested lists of data objects are not supported')
                elif isinstance(data_tmp,  dict):
                    tmp_string, tmp_obj = [], []
                    for tmp_key, tmp_value in data_tmp.items():
                        tmp_obj.append(tmp_value)
                        tmp_string.append(tmp_key)
                    if len(tmp_obj) == 1:
                        data_tmp = tmp_obj[0]
                        key_tmp = tmp_string[0]
                    else:
                        self.logger.error('Multiple keys dictionary of data objects are not supported')
                        raise ValueError('Multiple keys dictionary of data objects are not supported')
                elif isinstance(data_tmp, DataLocal):
                    key_tmp = name_tmp
                    pass
                else:
                    self.logger.error('data_tmp must be a DataLocal, list or a dict')
                    raise TypeError('data_tmp must be a DataLocal, list or a dict')

                # check data object type
                if not isinstance(data_tmp, DataLocal):
                    self.logger.error('Data object in the list is not a DataLocal instance')
                    raise ValueError('Data object in the list is not a DataLocal instance')

                # check time reference uniqueness and match
                if data_tmp.time_direction == 'single':
                    time_ref = pd.Timestamp(data_tmp.time_reference)
                    if time_ref != time_step:
                        # skip to next variable if time does not match
                        continue

                # update logger (for messages consistency)
                data_tmp.logger = self.logger.compare(data_tmp.logger)

                # read data only if readable (ok or template) --> condition of data object
                self.logger.info(f"Check object '{time_step}' and variable '{str_var_tmp}' ... ")

                status_tag_tmp, status_readable_tmp =  data_tmp.is_readable()
                if not status_readable_tmp:
                    if not status_tag_tmp == 'template':
                        self.logger.warning(f"Object {time_tmp} is not readable. File is not available.")
                        self.logger.info(f"Control obj '{time_tmp}' variable '{str_var_tmp}' is readable ... SKIP")
                        fx_check.append(False)
                    else:
                        self.logger.info(f"Object {time_tmp} is a template. The template will be filled by the times.")
                        self.logger.info(f"Check object '{time_tmp}' and variable '{str_var_tmp}' ... PASS")
                        fx_check.append(True)
                else:
                    self.logger.info(f"Check object '{time_tmp}' and variable '{str_var_tmp}' ... PASS")
                    fx_check.append(True)

                # manage variable mapping
                kwargs['variable'] = str_var_tmp

                # variable definition (variable, workflow and tag)
                step_variable = data_tmp.file_namespace.get('variable')
                step_workflow = data_tmp.file_namespace.get('workflow')
                step_tag = ':'.join([step_variable, step_workflow])

                # store variable data
                if type_tmp == 'data':
                    tmp_maps = data_tmp.variable_template['vars_data']
                    fx_maps = {**fx_maps, **tmp_maps}

                # variable id
                var_id = data_tmp.data_id

                # read data (check if data is readable or not)
                if fx_check[data_id]:

                    # get object settings
                    as_is, mandatory = data_tmp.data_as_is, data_tmp.data_mandatory
                    # get object time reference (if available)
                    time_ref = getattr(data_tmp, "time_reference", None)

                    # get data
                    fx_tmp = data_tmp.get_data(time=time_step, name=step_tag, as_is=as_is ,**kwargs)
                    fx_deps.append(step_tag)
                    fx_keys.append(step_workflow)

                    # convert to DataArray if single variable
                    fx_tmp = _to_dataarray_if_single_var(fx_tmp)
                    # get variable name(s) from data
                    fx_vars = _get_variable_name(fx_tmp, mandatory)
                    # store variable id
                    fx_varid.append(var_id)

                    # debug data in
                    if self.debug_state_in: plot_data(fx_tmp)
                else:
                    # data is not readable (default)
                    fx_tmp = None
                    fx_vars = time_tmp

                # append data (in list format)
                if (id_deps is None) or (data_id < id_deps):
                    if data_tmp.file_io == 'derived':
                        pass
                    else:
                        fx_data.append(fx_tmp)
                else:
                    if key_tmp is not None:
                        if not key_tmp in list(fx_other.keys()):
                            fx_other[key_tmp] = fx_tmp
                        else:
                            tmp_other = fx_other[key_tmp]
                            if not isinstance(tmp_other, list):
                                tmp_other = [tmp_other]
                            tmp_other.append(fx_tmp)

                            fx_tmp = {key_tmp: tmp_other}
                            fx_other = {**fx_other, **fx_tmp}

                    else:
                        # add other data in the fx_other dict with a generic key (id_other) if no key is available (key_tmp is None)
                        fx_other[id_other] = fx_tmp
                        id_other += 1

                # create metadata
                if 'fx_variable' not in fx_metadata:
                    fx_metadata['fx_variable'] = []
                fx_metadata['fx_variable'].append(fx_vars)

        else:

            # defined time reference check
            if data_raw.time_direction == 'single':
                time_ref = data_raw.time_reference
                if time_ref != time:
                    return None, None
            else:
                time_ref = getattr(data_raw, "time_reference", None)

            # obtain variable name from data object
            str_workflow, str_variable = fx_variable_trace.split(':')
            var_name = ':'.join([str_workflow, str_variable])

            # manage variable mapping
            kwargs['variable'] = var_name

            # update logger (for messages consistency)
            data_raw.logger = self.logger.compare(data_raw.logger)

            # variable id
            var_id = data_raw.data_id
            fx_varid = []

            # get data
            fx_data = data_raw.get_data(time=time, name=var_name, **kwargs)
            fx_deps, fx_keys = [], []

            # convert to DataArray if single variable
            fx_data = _to_dataarray_if_single_var(fx_data)
            # get variable name(s) from data
            fx_vars = _get_variable_name(fx_data)

            # debug data in
            if self.debug_state_in: plot_data(fx_data)

            # create other data dict
            fx_other = {}
            # create metadata
            fx_metadata = {'fx_variable': fx_vars}

        # check if fx data in empty and fx other is available to use (merge for variable that are not in fx_data)
        add_other = True
        if _is_empty_fx_data(fx_data) :
            if fx_other:
                for key, obj in fx_other.items():
                    fx_data.append(obj)
                id_deps = 0 # no data available only deps so init to 0 index
                add_other = False

        # check if fx data is available or not
        if _is_empty_fx_data(fx_data) :
            self.logger.info_down(
                f"Run :: {self.fx_name} - {time_str} - {fx_variable_trace} ... SKIPPED. DATA NOT AVAILABLE")
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

        # reduce data if only one element in the list
        if isinstance(fx_data, list):
            if len(fx_data) == 1: fx_data = fx_data[0]

        # check if time is a list of timestamps and reduce it if they are the same
        time = _reduce_if_same_timestamps(time)

        # collect and prepare function arguments
        fx_args = {arg_name: arg_value for arg_name, arg_value in self.fx_args.items()}
        fx_args['time'] = time
        # add reference time (in case of needed by the procedure)
        fx_args['time_reference'] = time_ref
        # add reference keys for method where input are not fixed
        fx_args["keys"] = fx_keys
        # add variable map
        fx_args['mapping'] = fx_maps

        # manage reference argument (if available in fx_static) and add it to fx_args (if not already present)
        if 'ref' in self.fx_static:
            ref_obj = self.fx_static['ref']

            if isinstance(ref_obj, dict):

                # remove ref if ref is defined by a dictionary
                if 'ref' in fx_args.keys():
                    fx_args.pop('ref', None)

                for key, value in ref_obj.items():
                    if key not in fx_args:
                        fx_args[key] = value
                    else:
                        self.logger.warning(
                            f"Key '{key}' in 'ref' is already present in fx_args. "
                            "The value from 'ref' will be ignored."
                        )

            elif isinstance(ref_obj, (list, tuple, set)):
                self.logger.error("Reference value in fx_static cannot be a list, tuple, or set")
                raise TypeError("Reference value in fx_static cannot be a list, tuple, or set")

            else:
                # single reference object
                fx_args['ref'] = ref_obj

        else:
            fx_args['ref'] = None

        # organize the function data based on deps_vars mapping
        if 'deps_vars' in list(self.fx_obj.keywords.keys()):
            deps_vars = self.fx_obj.keywords['deps_vars']
            if deps_vars:
                tmp_data = deepcopy(fx_data)

                if not isinstance(tmp_data, list):
                    tmp_data = [tmp_data]

                # get deps elements (from the data list)
                tmp_deps = []
                if id_deps > 0:
                    tmp_deps = tmp_data[id_deps:]
                else:
                    tmp_deps = deepcopy(tmp_data)

                # save the data elements using the deps_vars mapping
                fx_data = {}
                for fx_dep, tmp_values in zip(fx_deps, tmp_deps):
                    fx_var, fx_wf = fx_dep.split(':')
                    if fx_var in deps_vars.values():
                        # find the key corresponding to this value
                        dep_key = next(k for k, v in deps_vars.items() if v == fx_var)
                        fx_data[dep_key] = tmp_values

        # add other data and deps if available
        if fx_other:
            # if add in data avoid to add in the args
            if add_other:
                fx_args = {**fx_args, **fx_other}

        # run function to process data
        fx_save = self.fx_obj(data=fx_data, **fx_args)
        fx_metadata['fx_variable'] = _sync_variable_name(fx_save, fx_metadata['fx_variable'])

        # define the variable to control the workflow of processes (grid and time-series datasets)
        if isinstance(fx_save, xr.DataArray):
            fx_variable_data = fx_variable_wf
        elif isinstance(fx_save, xr.Dataset):
            fx_variable_data = list(fx_save.data_vars)
        elif isinstance(fx_save, pd.DataFrame):
            fx_variable_data = fx_save.name
        elif isinstance(fx_save, dict):
            fx_variable_data = list(fx_save.keys())
        elif fx_save is None:
            self.logger.warning('Output datasets are defined NoneType. No output will be saved.')
            return None, fx_memory
        else:
            self.logger.error('Output datasets are defined by unknown type')
            raise ValueError('Unknown fx output type')

        # check variable data
        if fx_variable_data is None:
            self.logger.error("fx_variable_data must be defined (not None).")
            raise ValueError("fx_variable_data must be defined (not None).")

        # manage attributes of the dataarray and dataset object(s) related to workflow, tag and data
        if isinstance(fx_save, xr.DataArray):
            # data array case
            if isinstance(fx_variable_data, str):
                fx_save.name = fx_variable_data
                _set_name_and_attrs(fx_save, fx_variable_data, fx_variable_wf, fx_variable_name)
            elif isinstance(fx_variable_data, list):
                if len(fx_variable_data) == 1:
                    fx_save.name = fx_variable_data[0]
                    _set_name_and_attrs(fx_save, fx_variable_data[0], fx_variable_wf, fx_variable_name)
                else:
                    self.logger.error(
                        'fx_var must be a single string when fx_save is a DataArray with multiple variables.')
                    raise ValueError(
                        "fx_var must be a single string when fx_save is a DataArray with multiple variables."
                    )
            else:
                self.logger.error(
                    "fx_var must be a string or list with length=1 when fx_save is a DataArray."
                )
                raise TypeError(
                    "fx_var must be a string or list with length=1 when fx_save is a DataArray."
                )

        elif isinstance(fx_save, xr.Dataset):

            # dataset case
            if isinstance(fx_variable_data, str):
                if fx_variable_data in fx_save.data_vars:
                    fx_save[fx_variable_data].name = fx_variable_data
                    _set_name_and_attrs(fx_save[fx_variable_data], fx_variable_data, fx_variable_wf, fx_variable_name)
                else:
                    self.logger.error(f'Variable {fx_variable_data} not found in Dataset data_vars.')
                    raise ValueError(f"Variable '{fx_variable_data}' not found in Dataset data_vars.")

            elif isinstance(fx_variable_data, list):
                for var in fx_variable_data:
                    if var in fx_save.data_vars:
                        fx_save[var].name = var
                        _set_name_and_attrs(fx_save[var], var, fx_variable_wf, fx_variable_name)
                    else:
                        self.logger.error(f'Variable {var} not found in Dataset data_vars.')
                        raise ValueError(f"Variable '{var}' not found in Dataset data_vars.")
            else:
                self.logger.error("fx_var must be a string or list when fx_save is a Dataset.")
                raise TypeError("fx_var must be a string or list when fx_save is a Dataset.")

            # Optionally attach dataset-level attributes
            fx_save.attrs["workflow"] = fx_variable_wf
            fx_save.attrs["tag1"] = fx_variable_name
            fx_save.attrs["name"] = fx_variable_name

        elif isinstance(fx_save, pd.DataFrame):

            fx_save.attrs["workflow"] = fx_variable_wf
            fx_save.attrs["tag1"] = fx_variable_name
            fx_save.attrs["name"] = fx_variable_name

        elif isinstance(fx_save, dict):

            if isinstance(fx_variable_data, str):
                variables = [fx_variable_data]
            elif isinstance(fx_variable_data, list):
                variables = fx_variable_data
            else:
                self.logger.error("fx_var must be a string or list when fx_save is a dictionary.")
                raise TypeError("fx_var must be a string or list when fx_save is a dictionary.")

            for var in variables:

                # check variable availability
                if var not in fx_save:
                    self.logger.error(f"Variable '{var}' not found in dictionary.")
                    raise ValueError(f"Variable '{var}' not found in dictionary.")
                var_data = fx_save[var]

                # xarray DataArray
                if isinstance(var_data, xr.DataArray):
                    var_data.name = var
                    _set_name_and_attrs(var_data,var,fx_variable_wf,fx_variable_name)

                # xarray Dataset
                elif isinstance(var_data, xr.Dataset):

                    # apply attributes to matching variable
                    if var in var_data.data_vars:
                        var_data[var].name = var
                        _set_name_and_attrs(var_data[var], var, fx_variable_wf, fx_variable_name)

                    # dataset-level attributes
                    var_data.attrs["workflow"] = fx_variable_wf
                    var_data.attrs["tag1"] = fx_variable_name
                    var_data.attrs["name"] = fx_variable_name

                # pandas DataFrame
                elif isinstance(var_data, pd.DataFrame):

                    var_data.name = var

                    # DataFrame does not have xarray .attrs semantics
                    # for variables, but metadata can be stored here
                    var_data.attrs["workflow"] = fx_variable_wf
                    var_data.attrs["tag1"] = fx_variable_name
                    var_data.attrs["name"] = fx_variable_name

                # None
                elif var_data is None:
                    self.logger.warning(f"Variable '{var}' is None. Attributes cannot be assigned.")
                else:
                    self.logger.error(f"Variable '{var}' has unsupported type: {type(var_data)}")
                    raise TypeError(f"Variable '{var}' has unsupported type: {type(var_data)}")

        else:
            # error for unknown type (dataarray or dataset only)
            self.logger.error("fx_save must be an xarray DataArray or Dataset.")
            raise TypeError("fx_save must be an xarray DataArray or Dataset.")

        # organize metadata
        kwargs['time_format'] = self.out_obj.get_attribute('time_format')
        kwargs['ref'] = self.fx_static['ref']
        kwargs['out_opts'] = self.out_opts

        # info process end
        self.logger.info_down(f"Run :: {self.fx_name} - {time_str} - {fx_variable_trace} ... DONE")

        # check the dump state (if active dump the collections instead of the variable)
        if not self.dump_state:

            # info dump variable start
            self.logger.info_up(f"Dump :: {self.fx_name} - {time_str} - {fx_variable_trace} ... ")

            # save the data
            self.out_obj.write_data(fx_save, time, metadata=fx_metadata, **kwargs)

            # arrange data to keep the data array format
            if isinstance(fx_save, xr.DataArray):
                fx_out = deepcopy(fx_save)
                fx_names = [fx_save.name] if fx_save.name else ["unnamed_var"]

            elif isinstance(fx_save, xr.Dataset):

                fx_var_list = list(fx_save.data_vars)
                if len(fx_var_list) == 1:
                    var_name = fx_variable_data[0]
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
            fx_out.attrs["variable_names"] = fx_names

            # info dump variable end
            self.logger.info_down(f"Dump :: {self.fx_name} - {time_str} - {fx_variable_trace} ... DONE")

            return fx_out, fx_memory

        # dump state if required from the process (if collections are available)
        elif self.dump_state:

            # info dump start
            self.logger.info_up(f"Dump :: write_data - {time_str} - collections ... ")

            # check if collections are available
            if 'collections' in kwargs:

                # get data collections
                fx_collections = kwargs.pop('collections', None)

                # create collections dataset
                if isinstance(fx_collections, dict):

                    # update last variable processed result to dump collections (including last updating)
                    fx_collections[fx_variable_trace] = fx_save

                    # iterate over collections variables
                    collections_obj = None
                    for key, obj_raw in fx_collections.items():
                        self.logger.info_up(f'Variable {key} ... ')
                        if obj_raw is not None:

                            if isinstance(obj_raw, pd.DataFrame):
                                obj_match = obj_raw
                            elif isinstance(obj_raw, xr.DataArray):
                                # method to match coordinates to reference (no interpolation)
                                obj_match = match_coords_to_reference(obj_raw, self.fx_static['ref'])
                            elif isinstance(obj_raw, dict):
                                obj_match = obj_raw
                            else:
                                self.logger.error('Collections data must be xarray DataArray or pandas DataFrame')
                                raise TypeError('Collections data must be xarray DataArray or pandas DataFrame')

                            # manage the collections object
                            if collections_obj is None:
                                if isinstance(obj_match, pd.DataFrame):
                                    collections_obj = obj_match
                                elif isinstance(obj_match, xr.DataArray):
                                    # organize data to keep the data array format
                                    collections_obj = xr.Dataset()
                                    collections_obj[key] = obj_match
                                elif isinstance(obj_match, dict):
                                    collections_obj = obj_match
                                else:
                                    self.logger.error('Collections dataset must be xarray Dataset or dict for pandas DataFrame')
                                    raise TypeError('Collections dataset must be xarray Dataset or dict for pandas DataFrame')

                            else:
                                if isinstance(obj_match, pd.DataFrame):
                                    collections_obj = pd.concat([collections_obj, obj_match], axis=1)
                                elif isinstance(obj_match, xr.DataArray):
                                    collections_obj[key] = obj_match
                                else:
                                    self.logger.error('Collections dataset must be xarray Dataset or dict for pandas DataFrame')
                                    raise TypeError('Collections dataset must be xarray Dataset or dict for pandas DataFrame')

                            # debug data out
                            if self.debug_state_out: plot_data(obj_match)
                            if self.debug_state_out: plot_data(collections_obj[key])

                            self.logger.info_down(f'Variable {key} ... ADDED')
                        else:
                            self.logger.warning('Variable is defined by NoneType')
                            self.logger.info_down(f'Variable {key} ... SKIPPED')

                else:
                    self.logger.error('Collections must be defined as a dictionary')
                    raise TypeError('Collections must be defined as a dictionary')

                # define collections variables
                if isinstance(collections_obj, pd.DataFrame):
                    collections_variables = collections_obj.name
                elif isinstance(collections_obj, xr.Dataset):
                    collections_variables = list(collections_obj.data_vars)
                elif isinstance(collections_obj, dict):
                    collections_variables = list(collections_obj.keys())
                else:
                    self.logger.error('Collections dataset must be xarray Dataset or dict for pandas DataFrame')
                    raise TypeError('Collections dataset must be xarray Dataset or dict for pandas DataFrame')

                # collections kwargs
                kwargs['time_format'] = self.out_obj.time_format
                kwargs['units'] = self.out_obj.units
                kwargs['map'] = self.out_obj.variable_template['vars_data']
                kwargs['ref'] = self.fx_static['ref']
                kwargs['out_opts'] = self.out_opts

                # collections args
                collections_metadata = {'variables': collections_variables}

                # save the data
                if not len(collections_variables) == 0:

                    # get signature to use in write_data (if time signature is defined in the output object)
                    time_signature = self.out_obj.time_signature

                    if time_signature == 'step' or time_signature == 'current':
                        pass
                    elif time_signature == 'range' or time_signature == 'start/end':
                        time = (time[0], time[-1])
                    elif time_signature == 'end':
                        time = time[-1]
                    elif time_signature == 'start':
                        time = time[0]
                    elif time_signature == 'period':
                        time = (time[0], time[-1])
                    else:
                        self.logger.error(f"Unknown time signature '{time_signature}' in output object")
                        raise ValueError(f"Unknown time signature '{time_signature}' in output object")

                    # write collections
                    self.out_obj.write_data(
                        collections_obj, time,
                        metadata=collections_metadata, **kwargs)

                    # info dump end
                    self.logger.info_down(f"Dump :: write_data - {time_str} - collections ... DONE")

                else:
                    # skip (collections empty)
                    self.logger.warning('No collections data to dump')
                    self.logger.info_down(f"Dump :: write_data - {time_str} - collections ... DONE")

                return None, False

            else:
                # info dump end
                self.logger.info_down(f'Dump collections at time {time_str} ... SKIPPED. NO COLLECTIONS FOUND')

        else:
            # exit for errors
            self.logger.error('Process unknown dump state')
            raise ValueError('Process unknown dump state')

# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# helpers
from collections import OrderedDict

def _check_list_of_list(x):
    return (
        isinstance(x, list) and
        all(
            isinstance(i, list) and
            all(isinstance(j, list) for j in i) or isinstance(i, list)
            for i in x
        )
    )

def _group_by_time(time_list, data_list, data_names):
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
def _sync_variable_name(data, vars):

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
def _get_variable_name(obj, mandatory = True,
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
def _to_dataarray_if_single_var(ds: xr.Dataset) -> xr.Dataset | xr.DataArray:
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
def _set_name_and_attrs(obj, name, ws, tag):
    """Set name (if missing) and attach metadata attributes."""
    if hasattr(obj, "name") and obj.name is None:
        obj.name = name
    obj.attrs["workflow"] = ws
    obj.attrs["tag"] = tag

# method to reduce timestamps if they are the same
@with_logger(var_name="logger_stream")
def _reduce_if_same_timestamps(timestamps):
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
# ----------------------------------------------------------------------------------------------------------------------
