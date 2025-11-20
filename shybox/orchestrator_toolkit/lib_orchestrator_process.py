"""
Class Features

Name:          lib_orchestrator_process
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20251104'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import sys
import datetime as dt
from copy import deepcopy
from typing import Callable, List
from functools import partial

import numpy as np
import pandas as pd
import xarray as xr

from shybox.generic_toolkit.lib_utils_time import convert_time_format

from shybox.generic_toolkit.lib_utils_geo import match_coords_to_reference
from shybox.generic_toolkit.lib_utils_debug import plot_data

from shybox.dataset_toolkit.dataset_handler_local import DataLocal
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
            if isinstance(arg_value, DataLocal):
                if not arg_value.is_static:
                    fx_args[arg_name] = arg_value
                else:

                    variable_template = {}
                    if hasattr(arg_value, 'variable_template'):
                        variable_template = arg_value.variable_template

                    # update logger (for messages consistency)
                    arg_value.logger = self.logger.compare(arg_value.logger)
                    # get static data
                    fx_static[arg_name] = arg_value.get_data(mapping=variable_template, name=arg_name)
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
<<<<<<< HEAD
        self.debug_state_in = False
        self.debug_state_out = False
=======
        self.debug_state = False
>>>>>>> shybox/destine

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
        fx_variable_wf, fx_variable_tag = kwargs['workflow'], kwargs['tag']
<<<<<<< HEAD
        fx_variable_trace = ':'.join([fx_variable_tag, fx_variable_wf])
=======
>>>>>>> shybox/destine

        if fx_id == 0 and 'memory' in kwargs:
            if (fx_variable_wf is not None) and (fx_variable_wf in kwargs['memory']):
                data_raw = kwargs['memory'][fx_variable_wf]
            else:
                data_raw = self.in_obj
        else:
            data_raw = self.in_obj

        # adjust data (if deps are available)
        if (self.in_deps is not None) and (len(self.in_deps) > 0):

            if isinstance(data_raw, list):
                data_raw.extend(self.in_deps)
            elif isinstance(data_raw, DataLocal):
                data_raw = [data_raw] + self.in_deps
            else:
                self.logger.error('Data object is not compatible with input dependencies')
                raise TypeError('Data object is not compatible with input dependencies')

            str_vars_raw, str_deps_raw = [], []
            for data_tmp in data_raw:
                if isinstance(data_tmp, DataLocal):
                    str_part1_tmp = data_tmp.file_namespace.get('variable')
                    str_part2_tmp = data_tmp.file_namespace.get('workflow')
                    var_tmp = ':'.join([str_part1_tmp, str_part2_tmp])
                    str_vars_raw.append(var_tmp)
                    str_deps_raw.append(str_part1_tmp)
                else:
                    self.logger.error('Data object in the list is not a DataLocal instance')
                    raise TypeError('Data object in the list is not a DataLocal instance')
        else:
            str_vars_raw = [':'.join([fx_variable_tag, fx_variable_wf])]
            str_deps_raw = [fx_variable_tag]

        if isinstance(time, list):
            if isinstance(data_raw, list):
                data_raw = data_raw * len(time)
            elif isinstance(data_raw, DataLocal):
                data_raw = [data_raw] * len(time)

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
<<<<<<< HEAD
            f"Run :: {self.fx_name} - {time_str} - {fx_variable_trace} ... ")
=======
            f"Run :: {self.fx_name} - {time_str} - {fx_variable_tag} - {fx_variable_wf} ... ")
>>>>>>> shybox/destine

        # memory is active only for start process
        if fx_id != 0:
            kwargs['memory_active'] = False

        # get the data (using the class signature)
        if isinstance(data_raw, list):

            # iterate over the list of data objects
            fx_data, fx_metadata, fx_deps = [], {}, []
            for data_id, (data_tmp, str_var_tmp, time_tmp) in enumerate(zip(data_raw, str_vars_raw, time)):

                # check nested list
                if isinstance(data_tmp, list):
                    if len(data_tmp) == 1:
                        data_tmp = data_tmp[0]
                    else:
                        self.logger.error('Nested lists of data objects are not supported')
                        raise ValueError('Nested lists of data objects are not supported')

                # check data object type
                if not isinstance(data_tmp, DataLocal):
                    self.logger.error('Data object in the list is not a DataLocal instance')
                    raise ValueError('Data object in the list is not a DataLocal instance')

                # check time reference uniqueness and match
                if data_tmp.time_direction == 'single':
                    time_ref = pd.Timestamp(data_tmp.time_reference)
                    if time_ref != time_tmp:
                        # skip to next variable if time does not match
                        continue

                # read data only if readable (condition of data object)
                if not data_tmp.is_readable():
                    continue  # skip unreadable data

                # manage variable mapping
                kwargs['variable'] = str_var_tmp

                # update logger (for messages consistency)
                data_tmp.logger = self.logger.compare(data_tmp.logger)
                # read data
                fx_tmp = data_tmp.get_data(time=time_tmp, name=str_var_tmp, **kwargs)
                fx_deps.append(str_var_tmp)

                # convert to DataArray if single variable
                fx_tmp = _to_dataarray_if_single_var(fx_tmp)
                # get variable name(s) from data
                fx_vars = _get_variable_name(fx_tmp)

<<<<<<< HEAD
                # debug data in
                if self.debug_state_in: plot_data(fx_tmp)
=======
                # debug data
                if self.debug_state: plot_data(fx_tmp, show=True)
>>>>>>> shybox/destine

                # append data (in list format)
                fx_data.append(fx_tmp)

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

            # manage variable mapping
            kwargs['variable'] = str_vars_raw[0]

            # update logger (for messages consistency)
            data_raw.logger = self.logger.compare(data_raw.logger)
            # get data
            fx_data = data_raw.get_data(time=time, name=str_vars_raw[0], **kwargs)
            fx_deps = [str_vars_raw[0]]

            # convert to DataArray if single variable
            fx_data = _to_dataarray_if_single_var(fx_data)
            # get variable name(s) from data
            fx_vars = _get_variable_name(fx_data)

<<<<<<< HEAD
            # debug data in
            if self.debug_state_in: plot_data(fx_data)

=======
>>>>>>> shybox/destine
            # create metadata
            fx_metadata = {'fx_variable': fx_vars}

        # check if data is available
        if _is_empty_fx_data(fx_data):
            self.logger.info_down(
<<<<<<< HEAD
                f"Run :: {self.fx_name} - {time_str} - {fx_variable_trace} ... SKIPPED. DATA NOT AVAILABLE")
=======
                f"Run :: {self.fx_name} - {time_str} - "
                f"{fx_variable_tag} - {fx_variable_wf} ... SKIPPED. DATA NOT AVAILABLE")
>>>>>>> shybox/destine
            return None, None

        # manage memory data
        fx_memory = None
        if fx_id == 0:
            if isinstance(data_raw, list):
                fx_memory = [data_tmp.memory_data for data_tmp in data_raw]
            else:
                fx_memory = data_raw.memory_data

        # reduce data if only one element in the list
        if isinstance(fx_data, list):
            if len(fx_data) == 1: fx_data = fx_data[0]

        # check if time is a list of timestamps and reduce it if they are the same
        time = _reduce_if_same_timestamps(time)

        # collect and prepare function arguments
        fx_args = {arg_name: arg_value for arg_name, arg_value in self.fx_args.items()}
        fx_args['time'] = time
        fx_args['ref'] = self.fx_static['ref']

        # organize the function data based on deps_vars mapping
        if 'deps_vars' in list(self.fx_obj.keywords.keys()):
            deps_vars = self.fx_obj.keywords['deps_vars']
            if deps_vars:
                tmp_data = deepcopy(fx_data)
<<<<<<< HEAD

                if not isinstance(tmp_data, list):
                    tmp_data = [tmp_data]

=======
>>>>>>> shybox/destine
                fx_data = {}
                for fx_dep, tmp_values in zip(fx_deps, tmp_data):
                    fx_var, fx_wf = fx_dep.split(':')
                    if fx_var in deps_vars.values():
                        # find the key corresponding to this value
                        dep_key = next(k for k, v in deps_vars.items() if v == fx_var)
                        fx_data[dep_key] = tmp_values

        # run function to process data
        fx_save = self.fx_obj(data=fx_data, **fx_args)
        fx_metadata['fx_variable'] = _sync_variable_name(fx_save, fx_metadata['fx_variable'])

        # define the variable to control the workflow of processes
        if isinstance(fx_save, xr.DataArray):
            fx_variable_data = fx_variable_wf
        elif isinstance(fx_save, xr.Dataset):
            fx_variable_data = list(fx_save.data_vars)
        else:
            self.logger.error('Unknown fx output type')
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
                _set_name_and_attrs(fx_save, fx_variable_data, fx_variable_wf, fx_variable_tag)
            elif isinstance(fx_variable_data, list):
                if len(fx_variable_data) == 1:
                    fx_save.name = fx_variable_data[0]
                    _set_name_and_attrs(fx_save, fx_variable_data[0], fx_variable_wf, fx_variable_tag)
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
                    _set_name_and_attrs(fx_save[fx_variable_data], fx_variable_data, fx_variable_wf, fx_variable_tag)
                else:
                    self.logger.error(f'Variable {fx_variable_data} not found in Dataset data_vars.')
                    raise ValueError(f"Variable '{fx_variable_data}' not found in Dataset data_vars.")

            elif isinstance(fx_variable_data, list):
                for var in fx_variable_data:
                    if var in fx_save.data_vars:
                        fx_save[var].name = var
                        _set_name_and_attrs(fx_save[var], var, fx_variable_wf, fx_variable_tag)
                    else:
                        self.logger.error(f'Variable {var} not found in Dataset data_vars.')
                        raise ValueError(f"Variable '{var}' not found in Dataset data_vars.")
            else:
                self.logger.error("fx_var must be a string or list when fx_save is a Dataset.")
                raise TypeError("fx_var must be a string or list when fx_save is a Dataset.")

            # Optionally attach dataset-level attributes
            fx_save.attrs["workflow"] = fx_variable_wf
            fx_save.attrs["tag"] = fx_variable_tag

        else:
            # error for unknown type (dataarray or dataset only)
            self.logger.error("fx_save must be an xarray DataArray or Dataset.")
            raise TypeError("fx_save must be an xarray DataArray or Dataset.")
<<<<<<< HEAD
=======

        # dump state if required from the process (if collections are available)
        if self.dump_state:

            # info dump start
            self.logger.info_up(f'Dump collections at time {time_str} ... ')

            # check if collections are available
            if 'collections' in kwargs:

                # get data collections
                fx_collections = kwargs.pop('collections', None)

                # create collections dataset
                collections_dset = xr.Dataset()
                if isinstance(fx_collections, dict):

                    for key, data in fx_collections.items():
                        self.logger.info_up(f'Variable {key} ... ')
                        if data is not None:
                            collections_dset[key] = data
                            self.logger.info_down(f'Variable {key} ... ADDED')
                        else:
                            self.logger.warning('Variable is defined by NoneType')
                            self.logger.info_down(f'Variable {key} ... SKIPPED')

                else:
                    self.logger.error('Collections must be defined as a dictionary')
                    raise TypeError('Collections must be defined as a dictionary')

                # define collections variables
                collections_variables = list(collections_dset.data_vars)

                # collections kwargs
                kwargs['time_format'] = self.out_obj.get_attribute('time_format')
                kwargs['ref'] = self.fx_static['ref']
                kwargs['out_opts'] = self.out_opts

                # collections args
                collections_metadata = {'variables': collections_variables}

                # save the data
                if not len(collections_dset.data_vars) == 0:

                    # write collections
                    self.out_obj.write_data(
                        collections_dset, time,
                        metadata=collections_metadata, **kwargs)

                    # info dump end
                    self.logger.info_down(f'Dump collections at time {time_str} ... DONE')

                else:
                    # skip (collections empty)
                    self.logger.warning('No collections data to dump')
                    self.logger.info_down(f'Dump collections at time {time_str} ... SKIPPED. NO VARIABLES FOUND')

                # info process end
                self.logger.info_down(
                    f"Run :: {self.fx_name} - {time_str} - {fx_variable_tag} - {fx_variable_wf} ... DONE")

                return None, False

            else:
                # info dump end
                self.logger.info_up(f'Dump collections at time {time_str} ... SKIPPED. NO COLLECTIONS FOUND')
>>>>>>> shybox/destine

        # organize metadata
        kwargs['time_format'] = self.out_obj.get_attribute('time_format')
        kwargs['ref'] = self.fx_static['ref']
        kwargs['out_opts'] = self.out_opts

<<<<<<< HEAD
        # info process end
        self.logger.info_down(f"Run :: {self.fx_name} - {time_str} - {fx_variable_trace} ... DONE")

        if fx_variable_tag == 'WIND_SPEED':
            print()

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
                    collections_dset = xr.Dataset()
                    for key, da_raw in fx_collections.items():
                        self.logger.info_up(f'Variable {key} ... ')
                        if da_raw is not None:

                            # method to match coordinates to reference (no interpolation)
                            da_match = match_coords_to_reference(da_raw, self.fx_static['ref'])

                            # organize data to keep the data array format
                            collections_dset[key] = da_match

                            # debug data out
                            if self.debug_state_out: plot_data(da_match)
                            if self.debug_state_out: plot_data(collections_dset[key])

                            self.logger.info_down(f'Variable {key} ... ADDED')
                        else:
                            self.logger.warning('Variable is defined by NoneType')
                            self.logger.info_down(f'Variable {key} ... SKIPPED')

                else:
                    self.logger.error('Collections must be defined as a dictionary')
                    raise TypeError('Collections must be defined as a dictionary')

                # define collections variables
                collections_variables = list(collections_dset.data_vars)

                # collections kwargs
                kwargs['time_format'] = self.out_obj.get_attribute('time_format')
                kwargs['ref'] = self.fx_static['ref']
                kwargs['out_opts'] = self.out_opts

                # collections args
                collections_metadata = {'variables': collections_variables}

                # save the data
                if not len(collections_dset.data_vars) == 0:

                    # write collections
                    self.out_obj.write_data(
                        collections_dset, time,
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
    else:
        logger_stream.error("fx_data must be an xarray DataArray or Dataset")
        raise NotImplementedError('Case not implemented yet')

    select_vars = []
    for tmp_var in data_vars:
        if tmp_var in vars:
            if tmp_var not in select_vars:
                select_vars.append(tmp_var)
        else:
            logger_stream.warning(f"Variable '{tmp_var}' found in data but not in fx_vars list")

    return select_vars

# method to get variable name(s)
@with_logger(var_name="logger_stream")
def _get_variable_name(obj, default="undefined", indexed_default="undefined_{}"):

    # Case 1: DataArray → single variable
    if isinstance(obj, xr.DataArray):
        return obj.name if obj.name is not None else default

    # Case 2: Dataset → multiple variables
    elif isinstance(obj, xr.Dataset):
        names = []
        for i, name in enumerate(obj.data_vars):
            if name is None:
                names.append(indexed_default.format(i))
            else:
                names.append(name)
        return names

    else:
        logger_stream.error("Input must be an xarray DataArray or Dataset")
        raise TypeError("Input must be an xarray DataArray or Dataset")

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
=======
        # save the data
        self.out_obj.write_data(fx_save, time, metadata=fx_metadata, **kwargs)

        # info process end
        self.logger.info_down(
            f"Run :: {self.fx_name} - {time_str} - {fx_variable_tag} - {fx_variable_wf} ... DONE")


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
        else:
            self.logger.error("Unknown fx output type")
            raise ValueError("Unknown fx output type")

        # save variable names
        fx_out.attrs["variable_names"] = fx_names
>>>>>>> shybox/destine

    if all(ts == first for ts in timestamps):
        return first  # All elements are the same
    else:
        return timestamps  # Elements differ, return as-is
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# helpers
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
    else:
        logger_stream.error("fx_data must be an xarray DataArray or Dataset")
        raise NotImplementedError('Case not implemented yet')

    select_vars = []
    for tmp_var in data_vars:
        if tmp_var in vars:
            if tmp_var not in select_vars:
                select_vars.append(tmp_var)
        else:
            logger_stream.warning(f"Variable '{tmp_var}' found in data but not in fx_vars list")

    return select_vars

# method to get variable name(s)
@with_logger(var_name="logger_stream")
def _get_variable_name(obj, default="undefined", indexed_default="undefined_{}"):

    # Case 1: DataArray → single variable
    if isinstance(obj, xr.DataArray):
        return obj.name if obj.name is not None else default

    # Case 2: Dataset → multiple variables
    elif isinstance(obj, xr.Dataset):
        names = []
        for i, name in enumerate(obj.data_vars):
            if name is None:
                names.append(indexed_default.format(i))
            else:
                names.append(name)
        return names

    else:
        logger_stream.error("Input must be an xarray DataArray or Dataset")
        raise TypeError("Input must be an xarray DataArray or Dataset")

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
