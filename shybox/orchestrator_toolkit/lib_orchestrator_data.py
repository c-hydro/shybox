"""
Class Features

Name:          lib_orchestrator_data
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20260914'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
from dataclasses import dataclass, field
from typing import Any, Dict, Iterator, List, Optional, Union
import pandas as pd
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# class to handle processor collections (link to ProcessorData)
class ProcessorCollection(dict):

    @staticmethod
    def extract_group(
            group: Union[Dict[str, Any], List[Dict[str, Any]]],
    ) -> List[tuple]:

        # normalize group to list
        group_list = (
            group
            if isinstance(group, (list, tuple))
            else [group]
        )

        # initialize extracted groups
        groups_obj = []

        # iterate over single groups
        for group_obj in group_list:

            # get group information
            group_time = group_obj.get("time", None)
            group_datasets = group_obj.get("datasets", {}) or {}
            group_attrs = group_obj.get("attrs", {}) or {}

            # normalize group time to DatetimeIndex
            if group_time is not None:
                if isinstance(group_time, pd.DatetimeIndex):
                    pass
                elif isinstance(group_time, pd.Timestamp):
                    group_time = pd.DatetimeIndex([group_time])
                else:
                    group_time = pd.DatetimeIndex(
                        pd.to_datetime(group_time)
                    )

            # initialize extracted datasets
            datasets_obj = []

            # iterate over datasets
            for obj_key, obj_value in group_datasets.items():

                # split role and variable
                try:
                    obj_role, obj_variable = obj_key.split("|", 1)
                except ValueError:
                    raise ValueError(
                        f"Dataset key '{obj_key}' must have format "
                        f"'role|variable'."
                    )

                # get workflow
                obj_workflow = getattr(
                    obj_value,
                    "file_workflow",
                    None
                )

                # get object time
                obj_time = getattr(
                    obj_value,
                    "time",
                    None
                )

                # normalize object time to DatetimeIndex
                if obj_time is not None:
                    if isinstance(obj_time, pd.DatetimeIndex):
                        pass
                    elif isinstance(obj_time, pd.Timestamp):
                        obj_time = pd.DatetimeIndex([obj_time])
                    else:
                        obj_time = pd.DatetimeIndex(
                            pd.to_datetime(obj_time)
                        )

                # store dataset information
                datasets_obj.append(
                    (
                        obj_role,
                        obj_key,
                        obj_workflow,
                        obj_variable,
                        obj_value,
                        obj_time,
                    )
                )

            # store extracted single group
            groups_obj.append(
                (
                    group_time,
                    tuple(datasets_obj),
                    group_attrs,
                )
            )

        return groups_obj

    # method to order objects
    def order_obj(self, obj_sep: str = '|') -> Dict[str, Dict[str, Any]]:

        # initialize ordered collection
        obj_ordered = {}

        # initialize group references and occurrences
        group_map = {}
        group_occurrences = {}

        # initialize group id
        group_id = 0

        # iterate over processor data objects
        for data_obj in ProcessorData._iter_data(self):

            # get workflow and variable information
            workflow = data_obj.workflow
            variable = data_obj.variable

            # create data reference
            data_reference = f"{workflow}{obj_sep}{variable}"

            # check if reference is already available
            if data_reference not in group_map:
                # assign group id
                group_map[data_reference] = group_id

                # initialize occurrence
                group_occurrences[data_reference] = 0

                # update group id
                group_id += 1

            # update occurrence
            group_occurrences[data_reference] += 1

            # get group and occurrence ids
            group_idx = group_map[data_reference]
            occurrence_idx = group_occurrences[data_reference]

            # create dataset group key
            group_key = f"group_dset_{group_idx}{obj_sep}{occurrence_idx}"

            # create dataset group
            group_dset = {
                "time": data_obj.time,
                "attrs": data_obj.attrs,
                "datasets": {},
            }

            # add main data object
            data_key = f"data|{workflow}{obj_sep}{variable}"
            group_dset["datasets"][data_key] = data_obj.data

            # add dependencies
            for dep_key, dep_obj in data_obj.deps.items():
                dep_reference = f"deps|{workflow}{obj_sep}{dep_key}"
                group_dset["datasets"][dep_reference] = dep_obj

            # store dataset group
            obj_ordered[group_key] = group_dset

        return obj_ordered

    @staticmethod
    def compact_obj(data_groups: Dict[str, Any], sep_obj: str = '|') -> Dict[str, List[Any]]:

        # initialize organized groups
        groups_organized = {}
        # iterate over groups
        for group_key, group_obj in data_groups.items():
            # get group name
            group_name = group_key.split(sep_obj, 1)[0]

            # initialize group and append object
            groups_organized.setdefault(group_name, []).append(group_obj)

        return groups_organized

    # method to get obj attributes
    def get_attrs(self, remove_dups: bool = False,) -> Dict[str, Any]:

        # initialize attributes collection
        attrs_collection = {}
        # iterate over processor data objects
        for data_obj in ProcessorData._iter_data(self):
            # collect attributes
            for attr_name, attr_value in data_obj.attrs.items():
                attrs_collection.setdefault(attr_name, []).append(attr_value)

        # remove duplicated values
        if remove_dups:
            for attr_name, attr_values in attrs_collection.items():

                # initialize unique values
                unique_values = []
                # remove duplicates preserving order
                for attr_value in attr_values:
                    if attr_value not in unique_values:
                        unique_values.append(attr_value)

                # simplify single occurrence
                attrs_collection[attr_name] = (
                    unique_values[0] if len(unique_values) == 1 else unique_values
                )

        return attrs_collection

    # method to get obj attribute by name
    def get_attr_by_name(self, name: str,remove_dups: bool = False, ) -> Any:

        # get attributes
        attrs_collection = self.get_attrs(remove_dups=remove_dups,)

        # check attribute availability
        if name not in attrs_collection:
            raise KeyError(
                f"Attribute '{name}' is not available. "
                f"Available attributes: "
                f"{list(attrs_collection.keys())}"
            )

        # get attrs value
        attrs_value = attrs_collection[name]

        return attrs_value
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# class to handle processor data
@dataclass
class ProcessorData:
    workflow: str
    variable: str
    role: Optional[str]
    data: Dict[str, Any]
    deps: Dict[str, Any] = field(default_factory=dict)
    time: Optional[Union[pd.Timestamp, pd.DatetimeIndex]] = None
    attrs: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def _iter_data(cls, data_collection: Dict[str, Dict[str, "ProcessorData"]],) -> Iterator["ProcessorData"]:

        # iterate over reference groups
        for data_group in data_collection.values():

            if not isinstance(data_group, dict):
                raise TypeError(f"Data group must be a dictionary, got {type(data_group).__name__}.")

            # iterate over processor data objects
            for data_obj in data_group.values():

                # multiple processor data objects
                if isinstance(data_obj, (list, tuple)):
                    for obj in data_obj:
                        yield obj

                # single processor data object
                else:
                    yield data_obj

    @classmethod
    def from_data(cls, data_obj) -> List["ProcessorData"]:

        # normalize data object to list
        if not isinstance(data_obj, (list, tuple)):
            data_obj = [data_obj]

        # initialize processor data
        processor_data = []
        # iterate over data objects
        for obj in data_obj:
            # get variable
            variable = obj.file_variable
            # get workflow
            workflow = obj.file_workflow
            # get role
            variable_role = obj.variable_role or {}
            role = variable_role.get(variable, None)

            # create data info
            processor_data.append(cls(workflow=workflow, variable=variable,role=role,data=obj,))

        return processor_data

    @classmethod
    def organize(cls, data: Dict[str, Any],) -> "ProcessorCollection":

        # check data type
        if not isinstance(data, dict):
            raise TypeError(f"Data must be a dictionary, got {type(data).__name__}.")

        # initialize processor data collection
        data_collection = ProcessorCollection()
        # iterate over data objects
        for data_key, data_obj in data.items():

            # create data info
            data_info = cls.from_data(data_obj)

            # normalize data info to list
            data_info_list = data_info if isinstance(data_info, (list, tuple)) else [data_info]
            # get unique workflow and variable values
            data_references = {(info.workflow, info.variable) for info in data_info_list}

            # check consistency
            if len(data_references) != 1:
                cls.logger.error(f"Multiple workflow/variable combinations found: {data_references}")
                raise RuntimeError("All data_info objects must have the same workflow and variable.")

            # get unique workflow and variable
            data_workflow, data_variable = next(iter(data_references))
            # create reference
            data_reference = f"{data_workflow}|{data_variable}"

            # initialize reference group
            if data_reference not in data_collection:
                data_collection[data_reference] = {}

            # store by variable
            data_collection[data_reference][data_variable] = data_info

        return data_collection

    @classmethod
    def get_workflows(cls, data_collection: Dict[str, Dict[str, "ProcessorData"]],) -> Union[str, List[str]]:
        # get workflow occurrences
        workflows = [data_obj.workflow for data_obj in cls._iter_data(data_collection)]
        # return single workflow or list of workflows
        return workflows[0] if len(workflows) == 1 else workflows

    @classmethod
    def get_roles(cls,data_collection: Dict[str, Dict[str, "ProcessorData"]],) -> Union[str, List[str]]:

        # initialize roles
        roles = []
        # iterate over data objects
        for data_obj in cls._iter_data(data_collection):

            # add data role
            if data_obj.role is not None:
                roles.append(data_obj.role)

            # add dependency roles
            if data_obj.deps:
                for dep_obj in data_obj.deps.values():
                    # get dependency role
                    dep_role = getattr(dep_obj, "role", None)
                    # fallback to data_role
                    if dep_role is None:
                        dep_role = getattr(dep_obj, "data_role", None)
                    # remap auxiliary role to deps
                    if dep_role == "auxiliary":
                        dep_role = "deps"
                    # add dependency role
                    if dep_role is not None:
                        roles.append(dep_role)

        # return single role or list of roles
        return roles[0] if len(roles) == 1 else roles

    @classmethod
    def get_variables(cls,data_collection: Dict[str, Dict[str, "ProcessorData"]]) -> Union[str, List[str]]:

        # initialize variables
        variables = []
        # iterate over data objects
        for data_obj in cls._iter_data(data_collection):
            # add data variable
            if data_obj.variable is not None:
                variables.append(data_obj.variable)

            # add dependency variables
            if data_obj.deps:
                for dep_obj in data_obj.deps.values():

                    # get dependency variable
                    dep_variable = (getattr(dep_obj, "variable", None) or getattr(dep_obj, "file_variable", None))

                    # add dependency variable
                    if dep_variable is not None:
                        variables.append(dep_variable)

        # return single variable or list of variables
        return variables[0] if len(variables) == 1 else variables

    @classmethod
    def get_reference(cls, data_collection: Dict[str, Dict[str, "ProcessorData"]],) -> Dict[str, Any]:

        # get information from processor data
        variable = cls.get_variables(data_collection)
        workflow = cls.get_workflows(data_collection)
        role = cls.get_roles(data_collection)

        # organize reference information
        return {
            "id": None,
            "variable": variable,
            "workflow": workflow,
            "role": role,
            "mapping": None,
            "trace": None,
        }

    @classmethod
    def add_dependencies(cls,
                         data_collection: Dict[str, Dict[str, "ProcessorData"]],
                         deps: Optional[Dict[str, Dict[str, Any]]],
                         dep_sep: str = '|') -> Dict[str, Dict[str, "ProcessorData"]]:

        # no dependencies available
        if not deps:
            return data_collection

        # check dependencies type
        if not isinstance(deps, dict):
            raise TypeError(f"Dependencies must be a dictionary, got {type(deps).__name__}.")

        # iterate over dependency groups
        for data_key, deps_group in deps.items():
            # skip unknown data references
            if data_key not in data_collection:
                continue

            # check dependency group
            if not isinstance(deps_group, dict):
                raise TypeError(f"Dependencies for '{data_key}' must be a dictionary.")

            # get processor data group
            data_group = data_collection[data_key]

            # link dependencies to each variable belonging to the reference
            for data_obj in data_group.values():
                # normalize data objects to list
                data_obj_list = data_obj if isinstance(data_obj, (list, tuple)) else [data_obj]
                # iterate over data objects
                for obj_id, obj in enumerate(data_obj_list, start=0):

                    # initialize dependencies for current object
                    deps_obj = {}
                    # iterate over dependencies
                    for dep_key, dep_value in deps_group.items():

                        # dependency specific to one object
                        if dep_sep in dep_key:
                            dep_name, dep_id = dep_key.split(dep_sep, 1)
                            # check dependency id
                            try:
                                dep_id = int(dep_id)
                            except ValueError:
                                raise RuntimeError(f"Invalid dependency key '{dep_key}'. Expected format '<name>|<id>'.")

                            # add only if it belongs to current object
                            if dep_id == obj_id:
                                deps_obj[dep_name] = dep_value

                        # common dependency
                        else:
                            deps_obj[dep_key] = dep_value

                    # update object dependencies
                    obj.deps.update(deps_obj)

        return data_collection

    @classmethod
    def manage_time(
            cls,
            data_collection: Dict[str, Dict[str, "ProcessorData"]],
            time: Union[pd.Timestamp, str, List, pd.DatetimeIndex,],
            time_format: str = "%Y-%m-%d %H:%M") -> Dict[str, Dict[str, "ProcessorData"]]:

        # normalize time
        if isinstance(time, pd.DatetimeIndex):
            time_values = time
        elif isinstance(time, (list, tuple)):
            time_values = pd.DatetimeIndex(time)
        else:
            time_values = pd.Timestamp(time)

        # get time bounds
        if isinstance(time_values, pd.DatetimeIndex):

            if len(time_values) == 0:
                time_start = None
                time_end = None
            else:
                time_start = time_values[0]
                time_end = time_values[-1]

        else:
            time_start = time_values
            time_end = time_values

        # create message
        if time_start is None:
            message = "Time is not available"
        elif time_start == time_end:
            message = time_start.strftime(time_format)
        else:
            message = (
                f"from {time_start.strftime(time_format)} "
                f"to {time_end.strftime(time_format)}"
            )

        # assign time and attributes
        for data_obj in cls._iter_data(data_collection):
            # set time
            data_obj.time = time_values
            # set attributes
            data_obj.attrs["message"] = message
            data_obj.attrs["time_info"] = {
                "time_start": time_start, "time_end": time_end, "time_format": time_format,
            }

        return data_collection

    def get_attrs(
            self,
            remove_dups: bool = False,
    ) -> Dict[str, Any]:

        # initialize attributes collection
        attrs_collection = {}

        # iterate over data objects
        for data_group in self.values():
            for data_obj in data_group.values():

                # collect attributes
                for attr_name, attr_value in data_obj.attrs.items():
                    attrs_collection.setdefault(
                        attr_name, []
                    ).append(attr_value)

        # remove duplicated values
        if remove_dups:

            for attr_name, attr_values in attrs_collection.items():

                unique_values = []

                for attr_value in attr_values:
                    if attr_value not in unique_values:
                        unique_values.append(attr_value)

                # simplify single occurrence
                attrs_collection[attr_name] = (
                    unique_values[0]
                    if len(unique_values) == 1
                    else unique_values
                )

        return attrs_collection

    def get_attr_by_name(
            self,
            name: str,
            remove_dups: bool = False,
    ) -> Any:

        # get attributes
        attrs_collection = self.get_attrs(
            remove_dups=remove_dups,
        )

        # check attribute availability
        if name not in attrs_collection:
            raise KeyError(
                f"Attribute '{name}' is not available. "
                f"Available attributes: "
                f"{list(attrs_collection.keys())}"
            )

        return attrs_collection[name]
# ----------------------------------------------------------------------------------------------------------------------
