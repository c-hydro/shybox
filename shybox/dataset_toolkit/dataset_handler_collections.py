"""
Class Features

Name:          dataset_handler_collections
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20261005'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
from copy import deepcopy
from typing import Dict, Iterator, Optional, Union

from shybox.dataset_toolkit.dataset_handler_base import Dataset
from shybox.dataset_toolkit.lib_dataset_generic import write_to_file
from shybox.io_toolkit.lib_io_utils import get_unresolved_tags

from shybox.dataset_toolkit.dataset_handler_local import DataLocal
from shybox.dataset_toolkit.dataset_handler_memory import DataMemory

from shybox.logging_toolkit.logging_handler import LoggingManager
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# class to handle collections dataset
class DataCollection(Dataset):
    """
    Collection of DataLocal / DataMemory objects.

    Aggregation rules:
    - same scalar values      -> keep scalar
    - different scalar values -> convert to list
    - lists                   -> merge/append
    - dictionaries            -> recursively merge
    """

    def __init__(self, data: Optional[Dict[str, Union["DataLocal", "DataMemory"]]] = None,
                 logger: Optional[LoggingManager] = None,  **kwargs):

        # set data collections logger
        self.logger = logger or LoggingManager(name="DataCollections")

        # initialize collection
        self.data = {}

        # initialize dataset runtime attributes
        self._structure_template = {}
        self.nan_value = None
        self.file_placeholder = {}
        self.memory_active = False
        self.memory_data = None
        self.tags = {}
        self.options = kwargs
        self.message = kwargs.get("message", True)

        # initialize collection data
        if data is not None:
            self.update(data)

    @staticmethod
    def _validate_obj(obj):
        if not isinstance(obj, (DataLocal, DataMemory)):
            raise TypeError("DataCollection supports only DataLocal or DataMemory objects. Found: {type(obj).__name__}")

    @classmethod
    def _merge_value(cls, current, new):
        """
        Merge two attribute values.

        Rules:
        - None -> new value
        - dict + dict -> recursive merge
        - list -> append / extend
        - equal scalars -> keep scalar
        - different scalars -> list
        """

        # first value
        if current is None:
            return deepcopy(new)

        # new value is None
        if new is None:
            return current

        # dictionaries -> recursive merge
        if isinstance(current, dict) and isinstance(new, dict):
            result = deepcopy(current)
            for key, value in new.items():
                if key in result:
                    result[key] = cls._merge_value(result[key], value)
                else:
                    result[key] = deepcopy(value)
            return result

        # current value already list
        if isinstance(current, list):
            result = deepcopy(current)
            if isinstance(new, list):
                for value in new:
                    if value not in result:
                        result.append(deepcopy(value))
            else:
                if new not in result:
                    result.append(deepcopy(new))
            return result

        # new value is list
        if isinstance(new, list):
            result = [deepcopy(current)]
            for value in new:
                if value not in result:
                    result.append(deepcopy(value))
            return result

        # same scalar value -> keep scalar
        if current == new:
            return current

        # different scalar values -> convert to list
        return [deepcopy(current), deepcopy(new)]

    def add(self, key, obj):
        """Add one DataLocal / DataMemory object."""
        self._validate_obj(obj)
        self.data[key] = obj

    def update(self, data):
        """Add/update multiple objects."""
        if isinstance(data, DataCollection):
            data = data.data
        if not isinstance(data, dict):
            raise TypeError("DataCollection.update expects a dict or DataCollection. Found: {type(data).__name__}")
        for key, obj in data.items():
            self.add(key, obj)

    def get_attr(self, attr, default=None):
        """
        Aggregate an attribute across all objects.

        Examples
        --------
        file_name:
            destination.nc
            destination.nc

        -> "destination.nc"

        file_variable:
            AIR_T_DATA
            RAIN_DATA

        -> ["AIR_T_DATA", "RAIN_DATA"]

        variable_template:
            dictionaries are recursively merged.
        """

        if not self.data:
            return default

        value_out, found = None, False
        for obj in self.data.values():
            if not hasattr(obj, attr):
                continue
            value = getattr(obj, attr)
            if not found:
                value_out = deepcopy(value)
                found = True
            else:
                value_out = self._merge_value(value_out, value)
        if not found:
            return default

        return value_out

    # Common aggregated properties
    @property
    def file_name(self):
        return self.get_attr("file_name")
    @property
    def file_variable(self):
        return self.get_attr("file_variable")
    @property
    def file_workflow(self):
        return self.get_attr("file_workflow")
    @property
    def file_format(self):
        return self.get_attr("file_format")
    @property
    def file_type(self):
        return self.get_attr("file_type")
    @property
    def file_mode(self):
        return self.get_attr("file_mode")
    @property
    def file_io(self):
        return self.get_attr("file_io")
    @property
    def file_args(self):
        return self.get_attr("file_args")
    @property
    def file_deps(self):
        return self.get_attr("file_deps")
    @property
    def variable_template(self):
        return self.get_attr("variable_template")
    @property
    def variable_role(self):
        return self.get_attr("variable_role")
    @property
    def data_role(self):
        return self.get_attr("data_role")
    @property
    def data_layout(self):
        return self.get_attr("data_layout")
    @property
    def loc_pattern(self):
        return self.get_attr("loc_pattern")
    @property
    def units(self):
        return self.get_attr("units")
    @property
    def time_format(self):
        return self.get_attr("time_format")

    # method to get attributes
    def get_attribute(self, attribute):
        """Get an aggregated attribute from the collection."""
        return self.get_attr(attribute)

    ## abstract method to cover dataset class
    # method to write data
    def _write_data(self, data, path: str, **kwargs) -> None:

        # message info start
        path_tags = get_unresolved_tags(path)
        if path_tags:
            self.logger.info_up(
                f"Write data to {path} ... "
                f"[tags {', '.join(path_tags)} filled by the output procedure]"
            )
        else:
            self.logger.info_up(f"Write data to {path} ... ")

        # method to write data to file
        write_to_file(
            data,
            path, file_format=self.file_format, file_type=self.file_type, file_mode=self.file_mode,
            **kwargs)

        # message info end
        if path_tags:
            self.logger.info_down(
                f"Write data to {path} ... DONE "
                f"[tags {', '.join(path_tags)} filled by the output procedure]"
            )
        else:
            self.logger.info_down(f"Write data to {path} ... DONE")

    def _read_data(self, *args, **kwargs):
        return None
    def _rm_data(self, *args, **kwargs) -> None:
        return None
    def _create_data(self, *args, **kwargs):
        return None
    def _check_data(self, *args, **kwargs) -> bool:
        return False

    # dictionary-like interface
    def get(self, key, default=None):
        return self.data.get(key, default)
    def keys(self):
        return self.data.keys()
    def values(self):
        return self.data.values()
    def items(self):
        return self.data.items()
    def __getitem__(self, key):
        return self.data[key]
    def __setitem__(self, key, obj):
        self.add(key, obj)
    def __contains__(self, key):
        return key in self.data
    def __iter__(self) -> Iterator:
        return iter(self.data)
    def __len__(self):
        return len(self.data)
    def __bool__(self):
        return bool(self.data)

    # repr method
    def __repr__(self):
        obj_types = {key: type(obj).__name__ for key, obj in self.data.items()}
        return f"{self.__class__.__name__} (n={len(self.data)} objects={obj_types}"
