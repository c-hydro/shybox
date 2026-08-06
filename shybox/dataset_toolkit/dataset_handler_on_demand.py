"""
Class Features

Name:          dataset_handler_on_demand
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20260123'
Version:       '1.1.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import os
import xarray as xr
import pandas as pd

from datetime import datetime

from shybox.dataset_toolkit.dataset_handler_base import Dataset

from shybox.dataset_toolkit.lib_dataset_ondemand import create_object
from shybox.dataset_toolkit.lib_dataset_generic import write_to_file, read_from_file, rm_file
from shybox.generic_toolkit.lib_utils_tmp import ensure_folder_tmp, ensure_file_tmp
from shybox.logging_toolkit.logging_handler import LoggingManager

from typing import Optional
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# class to handle local dataset
class DataOnDemand(Dataset):

    type = 'on_demand_dataset'

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

    _default_vars_wf = ["variable"]

    def __init__(self,
                 info: dict = None,
                 logger: Optional[LoggingManager] = None, file_io: str = None,
                 message: bool = True,
                 **kwargs):

        # set data local logger
        self.logger = logger or LoggingManager(name="DataLocal")

        # store info (to define the object pattern and type)
        self.info = info or {}

        # check file dependencies
        if 'file_deps' in kwargs:
            self.file_deps = kwargs.pop('file_deps')
        else:
            self.file_deps = []
        # check data layout
        if 'data_layout' in kwargs:
            self.data_layout = kwargs.pop('data_layout')
        else:
            self.data_layout = 'geo'

        self._file_io = None  # internal storage
        if file_io is not None:
            self.file_io = file_io  # triggers the setter

        # handle file_template normalization
        file_variable = kwargs.get('file_variable', 'variable')
        n_vars = kwargs.pop('n_vars', 1) or 1

        # variable template normalization
        variable_template = kwargs.pop('variable_template', None)
        if variable_template is None:
            variable_template = {}
        elif not isinstance(variable_template, dict):
            self.logger.error("Variable template must be a dict if provided.")
            raise ValueError("Variable template must be a dict if provided.")

        # define dims and vars - layout handling (backward compatible)
        layout_raw = getattr(self, "data_layout", "geo") or "geo"
        layout_norm = self._layout_aliases.get(str(layout_raw).lower(), str(layout_raw).lower())

        if layout_norm not in self._layout_templates:
            raise ValueError(f"Unsupported data_layout '{layout_raw}'. Allowed: {list(self._layout_templates.keys())}")

        layout_cfg = self._layout_templates[layout_norm]
        dims_key_default = layout_cfg["dims_key"]
        coords_key_default = layout_cfg["coords_key"]

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
                vars_data = {f"var_{i}": f"var{i}" for i in range(1, int(n_vars) + 1)}

        # select file_workflow based on file_io (defined in calling code)
        if self.file_io == 'input':
            file_workflow = list(vars_data.values())
        elif self.file_io == 'output':
            file_workflow = list(vars_data.keys())
        elif self.file_io == 'derived':
            file_workflow = list(vars_data.values())
        else:
            file_workflow = variable_template.get("vars_wf") or self._default_vars_wf

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
            'obj_pattern': self.obj_pattern,
            'obj_type': self.obj_type,
            'logger': self.logger,
            'message': message,
            'variable_template': {"dims_geo": dims_geo, "coords_geo": coords_geo, "vars_data": vars_data},
            "data_layout": layout_norm,
            'file_variable': file_variable,
            'file_workflow': file_workflow
        })

        # initialize parent class
        super().__init__(**kwargs)

    # property obj pattern
    @property
    def obj_pattern(self):
        if self.info is None or not self.info:
            return None
        return self.info
    @obj_pattern.setter
    def obj_pattern(self, info: dict):
        self.info = info if info is not None else None

    # back compatibility (to remove in future)
    @property
    def loc_pattern(self):
        return None
    @loc_pattern.setter
    def loc_pattern(self, path: str=None):
        if path is not None:
            self.dir_name  = os.path.dirname(path)
            self.file_name = os.path.basename(path)
        else:
            self.dir_name, self.file_name = None, None

    # property obj type
    @property
    def obj_type(self):

        # check info structure is not None
        if self.info is None:
            return None
        keys = set(self.info.keys())

        # Raster grid definition (your case)
        raster_keys = {"x_ll", "y_ll", "rows", "cols", "res"}
        if raster_keys.issubset(keys):
            return "raster_grid"
        # Bounding box definition
        bbox_keys = {"xmin", "ymin", "xmax", "ymax"}
        if bbox_keys.issubset(keys):
            return "bbox"
        # Single point definition
        point_keys = {"lon", "lat"}
        if point_keys.issubset(keys):
            return "point"
        #  Unknown format
        return "unknown"

    @property
    def file_io(self):
        """Get the file_io value."""
        return self._file_io

    @file_io.setter
    def file_io(self, value):
        """Set the file_io value, ensuring it is valid."""
        if value not in {'input', 'output', 'derived','tmp'}:
            raise ValueError(f"Invalid file_io '{value}'. Must be one of {'input', 'output', 'derived', 'tmp'}.")
        self._file_io = value

    def path(self, time: Optional[pd.Timestamp] = None, **kwargs):
        return self.get_key(time, **kwargs)

    ## INPUT/OUTPUT METHODS
    # method to read data
    def _read_data(self, path: str,
                   vars_data: dict = None, vars_geo: dict = None, dims_geo: dict = None,
                   **kwargs) -> (xr.DataArray, xr.Dataset, pd.DataFrame):

        # message info start
        self.logger.info_up(f"Read data from {path} ... ")

        # manage variables to read
        variable = None
        if vars_data is not None:
            variable = list(vars_data.keys())

        # method to read data from file
        data = read_from_file(
            path,
            file_format=self.file_format, file_type=self.file_type, file_variable=variable)

        # message info end
        self.logger.info_down(f"Read data from {path} ... DONE")

        return data

    # method to create data
    def _create_data(self, obj: dict=None, variable: (str, None) = 'variable',
                     **kwargs) -> (xr.DataArray, xr.Dataset, pd.DataFrame):

        # message info start
        self.logger.info_up(f"Create data ... ")

        # manage variables to create
        if variable is None:
            variable = 'variable'

        # method to create obj from info
        data = create_object(
            obj, obj_format=self.file_format, obj_type=self.file_type, obj_variable=variable,)

        # message info start
        self.logger.info_down(f"Create data ... DONE")

        return data

    # method to write data
    def _write_data(self, data: (xr.DataArray, pd.DataFrame), path: str, **kwargs) -> None:

        # message info start
        self.logger.info_up(f"Write data to {path} ... ")

        # method to write data to file
        write_to_file(
            data,
            path, file_format=self.file_format, file_type=self.file_type, file_mode=self.file_mode,
            **kwargs)

        # message info end
        self.logger.info_down(f"Write data to {path} ... DONE")

    # method to remove data
    def _rm_data(self, path) -> None:
        rm_file(path)

    ## METHODS TO CHECK DATA AVAILABILITY
    def _check_data(self, path) -> bool:
        return os.path.exists(path)
    
    def _walk(self, prefix):
        for root, _, filenames in os.walk(prefix):
            for filename in filenames:
                yield os.path.join(root, filename)
# ----------------------------------------------------------------------------------------------------------------------
