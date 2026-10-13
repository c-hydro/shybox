"""
Class Features

Name:          dataset_handler_local
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20260903'
Version:       '1.2.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import os
import xarray as xr
import pandas as pd

from datetime import datetime

from shybox.dataset_toolkit.dataset_handler_base import Dataset
from shybox.dataset_toolkit.lib_dataset_generic import write_to_file, read_from_file, rm_file
from shybox.generic_toolkit.lib_utils_tmp import ensure_folder_tmp, ensure_file_tmp
from shybox.io_toolkit.lib_io_utils import get_unresolved_tags
from shybox.logging_toolkit.logging_handler import LoggingManager

from typing import Optional
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# class to handle local dataset
class DataLocal(Dataset):

    type = 'local_dataset'

    # Backward-compatible layout templates (defaults used only if user doesn't provide them)
    _layout_templates = {
        "geo": {
            "dims_key": "dims_geo",
            "coords_key": "coords_geo",
            "dims": {"longitude": "longitude", "latitude": "latitude"},
            "coords": {"longitude": "longitude", "latitude": "latitude"},
        },
        "grid": {  # same defaults as geo (you said you can use geo also for grid)
            "dims_key": "dims_geo",
            "coords_key": "coords_geo",
            "dims": {"longitude": "longitude", "latitude": "latitude"},
            "coords": {"longitude": "longitude", "latitude": "latitude"},
        },
        "points": {
            # keep supporting your old "dims_point" convention
            "dims_key": "dims_point",
            "coords_key": "coords_geo",  # points may still have lon/lat coords
            "dims": {"point": "point"},  # safe generic fallback
            "coords": {"longitude": "longitude", "latitude": "latitude"},
        },
        "time_series": {
            "dims_key": "dims_time",
            "coords_key": "coords_time",
            "dims": {"time": "time"},
            "coords": {"time": "time"},
        },
    }

    # Aliases to be forgiving with naming
    _layout_aliases = {
        "ts": "time_series",
        "timeseries": "time_series",
        "time-series": "time_series",
        "point": "points",
    }

    # default property
    _defaults_property = {
        'data_role': {'NA', 'NA'}
    }

    _default_vars_wf = ["variable"]

    def __init__(self,
                 path: Optional[str] = None,
                 file_name: Optional[str] = None, file_io: str = None,
                 file_variable: str = None, file_workflow : str = None,
                 logger: Optional[LoggingManager] = None,
                 message: bool = True,
                 **kwargs):

        # set data local logger
        self.logger = logger or LoggingManager(name="DataLocal")

        # store path
        self.path = path

        self.file_args = {}
        if 'file_args' in kwargs:
            self.file_args = kwargs.pop('file_args')

        # check file dependencies
        if 'file_deps' in kwargs:
            self.file_deps = kwargs.pop('file_deps')
        else:
            self.file_deps = {}

        # check data layout
        if 'data_layout' in kwargs:
            self.data_layout = kwargs.pop('data_layout')
        else:
            self.data_layout = 'geo'
        # check data mandatory
        if 'data_mandatory' in kwargs:
            self.data_mandatory = kwargs.pop('data_mandatory')
        else:
            self.data_mandatory = True

        # check data_id (to link data to ancillary info if needed)
        if 'data_id' in kwargs:
            self.data_id = kwargs.pop('data_id')
        else:
            self.data_id = -1
        # check data_role (to define if Data is data, deps, auxiliary ... )
        if 'data_role' in kwargs:
            self.data_role = kwargs.pop('data_role')
        else:
            self.data_role = 'data'

        # determine directory name
        if path is not None:
            self.dir_name = path
        elif 'dir_name' in kwargs:
            self.dir_name = kwargs.pop('dir_name')
        elif 'loc_pattern' in kwargs:
            self.dir_name = os.path.dirname(kwargs.get('loc_pattern'))
        elif 'tmp_pattern' in kwargs:
            self.dir_name = ensure_folder_tmp()
        else:
            self.dir_name = ensure_folder_tmp()

        self._file_io = 'undefined'  # internal storage
        if file_io is not None:
            self.file_io = file_io  # triggers the setter

        # define file name (different options)
        if file_name is not None:
            self.file_name = file_name
        elif 'file_name' in kwargs:
            self.file_name = kwargs.pop('file_name')
        elif 'loc_pattern' in kwargs:
            self.file_name = os.path.basename(kwargs.get('loc_pattern'))
        elif 'tmp_pattern' in kwargs:
            self.file_name = ensure_file_tmp()
        else:
            self.file_name = ensure_file_tmp()

        # check file delimiter (default ,)
        if 'file_delimiter' in kwargs:
            self.delimiter = kwargs.pop('file_delimiter')
        else:
            self.delimiter = ';'

        # check file variable not NoneType obj
        if file_variable is None:
            self.logger.error("The file_variable is defined by NoneType.")
            raise RuntimeError("The file_variable must be defined by string.")
        # check file variable type
        if not isinstance(file_variable, str):
            self.logger.error(f"The file_variable must be a string. Found type: {type(file_variable).__name__}.")
            raise RuntimeError("The file_variable must be defined by string.")

        # variable template normalization
        variable_template = kwargs.pop('variable_template', None)
        if variable_template is None:
            variable_template = {}
        elif not isinstance(variable_template, dict):
            self.logger.error("Variable template must be a dict if provided.")
            raise ValueError("Variable template must be a dict if provided.")

        # variable role normalization
        if isinstance(self.data_role, str):
            variable_role = {file_variable: self.data_role}
        else:
            self.logger.error("Attribute data_role must be a str")
            raise RuntimeError("Check your settings in the DataObject definition")

        # define dims and vars - layout handling (backward compatible)
        layout_raw = getattr(self, "data_layout", "geo") or "geo"
        layout_norm = self._layout_aliases.get(str(layout_raw).lower(), str(layout_raw).lower())

        if layout_norm not in self._layout_templates:
            raise ValueError(f"Unsupported data_layout '{layout_raw}'. Allowed: {list(self._layout_templates.keys())}")

        layout_cfg = self._layout_templates[layout_norm]
        dims_key_default = layout_cfg["dims_key"]
        coords_key_default = layout_cfg["coords_key"]

        # handle as_is flag (if True, data are returned as read without any processing)
        self.data_as_is = False
        if 'data_as_is' in kwargs:
            self.data_as_is = kwargs.pop('data_as_is')

        dims_user = (
                variable_template.get(dims_key_default)
                or variable_template.get("dims_geo")
                or variable_template.get("dims_point")
                or variable_template.get("dims_time")
        )
        coords_user = (
                variable_template.get(coords_key_default)
                or variable_template.get("coords_geo")
                or variable_template.get("coords_point")
                or variable_template.get("coords_time")
        )

        dims_geo = dims_user or layout_cfg["dims"]
        coords_geo = coords_user or layout_cfg["coords"]

        # vars_data (unchanged)
        vars_data = variable_template.get("vars_data")
        if not vars_data:
            if file_variable:
                vars_data = {file_variable: file_variable}
            else:
                self.logger.error("Attribute vars_data must be defined by dictionary.")
                raise ValueError("Attribute vars_data must be defined by dictionary.")

        # units nad ensure units are defined for all variables
        units = variable_template.get("units", {})
        units = {var_name: units.get(var_name, "NA") for var_name in vars_data}

        # select file_workflow based on args or file_io (defined in calling code)
        if file_workflow is None:
                self.logger.error("Attribute file_workflow must be defined by string.")
                raise ValueError("Attribute file_workflow must be defined by string.")


        # add placeholder for file (for example if output file needs some extra definitions to fill the string)
        file_placeholder = kwargs.pop('file_placeholder', {})
        # check placeholder format
        if not isinstance(file_placeholder, dict):
            logger_stream.error('File placeholder must be a dictionary. Exit.')
            raise TypeError(f'File placeholder must be a dictionary. Got {type(file_placeholder).__name__}.')
        # replace undefined values with "NA"
        file_placeholder = {key: "NA" if value is None else value for key, value in file_placeholder.items()}

        # ensure valid structure
        if not all(isinstance(x, dict) for x in (dims_geo, coords_geo, vars_data)):
            self.logger.error("File_template fields 'dims_geo', 'coords_geo', and 'vars_data' must be dicts.")
            raise ValueError("File_template fields 'dims_geo', 'coords_geo', and 'vars_data' must be dicts.")

        # creation metadata
        self._creation_kwargs = {
            'type': self.type,
            'time_creation': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

        # update kwargs for parent class
        kwargs.update({
            'loc_pattern': self.loc_pattern,
            'logger': self.logger,
            'message': message,
            'units': units,
            'variable_template': {"dims_geo": dims_geo, "coords_geo": coords_geo, "vars_data": vars_data},
            'variable_role': variable_role,
            "data_layout": layout_norm,
            "data_id": self.data_id,
            "data_role": self.data_role,
            "data_as_is": self.data_as_is,
            "data_mandatory": self.data_mandatory,
            'file_io': self.file_io,
            'file_variable': file_variable,
            'file_workflow': file_workflow,
            'file_placeholder': file_placeholder
        })

        # initialize parent class
        super().__init__(**kwargs)

    # LOC PATTERS MANAGEMENT
    @property
    def loc_pattern(self):
        if self.dir_name is None or self.file_name is None:
            return None
        return os.path.join(self.dir_name, self.file_name)
    @loc_pattern.setter
    def loc_pattern(self, path):
        if path is not None:
            self.dir_name  = os.path.dirname(path)
            self.file_name = os.path.basename(path)
        else:
            self.dir_name, self.file_name = None, None

    # FILE IO MANAGEMENT
    @property
    def file_io(self):
        """Get the file_io value."""
        return self._file_io
    @file_io.setter
    def file_io(self, value):
        """Set the file_io value, ensuring it is valid."""
        if value not in {'input', 'output', 'derived', 'tmp', 'undefined', 'geo'}:
            self.logger.error("attribute 'file_io' has not the expected value.")
            raise ValueError(f"attribute 'file_io' value is '{value}'. "
                             f"Must be one of {'input', 'output', 'derived', 'tmp', 'undefined', 'geo'}.")
        self._file_io = value

    # FILE DEPS MANAGEMENT
    @property
    def file_deps(self):
        """Get file dependencies."""
        return self._file_deps
    @file_deps.setter
    def file_deps(self, value):
        """Set file dependencies."""
        if value is None or isinstance(value, list):
            self.logger.warning(f"file_deps must be a dict, got {type(value).__name__}. Using an empty dictionary.",)
            value = {}
        elif not isinstance(value, dict):
            self.logger.error("attribute 'file_deps' is not in expected format.")
            raise TypeError(f"attribute 'file_deps' must be a dict, got {type(value).__name__}.")
        self._file_deps = value

    ## DATA MANAGEMENT
    @property
    def data_role(self):
        if not hasattr(self, '_data_role'):
            self._data_role = self._defaults_property['data_role']
        return self._data_role
    @data_role.setter
    def data_role(self, value):
        if value not in ['data', 'deps', 'ancillary', 'auxiliary']:
            self.logger.error(f"attribute 'data role' has defined by not expected value")
            raise ValueError(f"attribute 'data_role' value is '{value}'. "
                             f"Must be one of {'data', 'deps', 'ancillary', 'auxiliary'}.")
        self._data_role = value

    # method to apply time to path
    def path(self, time: Optional[pd.Timestamp] = None, **kwargs):
        return self.get_key(time, **kwargs)

    ## INPUT/OUTPUT METHODS
    # method to read data
    def _read_data(self, path: str,
                   vars_data: dict = None, vars_geo: dict = None, dims_geo: dict = None,
                   **kwargs) -> (xr.DataArray, xr.Dataset, pd.DataFrame):

        # message info start
        self.logger.info_up(f"Read data from {path} ... ")

        # set variable if defined
        variable = None
        if 'variable' in kwargs:
            variable = kwargs['variable']

        # method to read data from file
        data = read_from_file(
            path,
            file_format=self.file_format, file_type=self.file_type,
            file_args=self.file_args,
            file_variable=variable)

        # message info end
        self.logger.info_down(f"Read data from {path} ... DONE")

        return data

    # method to write data
    def _write_data(self, data: (xr.DataArray, pd.DataFrame), path: str, **kwargs) -> None:

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

    # method to remove data
    def _rm_data(self, path) -> None:
        rm_file(path)

    # method to create data (not used - just to initialize the abstract method in the base)
    def _create_data(self, input_obj: dict):
        self.data = None  # or some neutral placeholder
        return None

    ## METHODS TO CHECK DATA AVAILABILITY
    def _check_data(self, path) -> bool:
        return os.path.exists(path)
    
    def _walk(self, prefix):
        for root, _, filenames in os.walk(prefix):
            for filename in filenames:
                yield os.path.join(root, filename)
# ----------------------------------------------------------------------------------------------------------------------
