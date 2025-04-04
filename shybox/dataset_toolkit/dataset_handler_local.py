import os
import xarray as xr
import pandas as pd

from datetime import datetime

from shybox.dataset_toolkit.dataset_handler_base import Dataset
from shybox.dataset_toolkit.lib_dataset_generic import write_to_file, read_from_file, rm_file

from typing import Optional

class DataLocal(Dataset):

    type = 'local_dataset'

    def __init__(self, path: Optional[str] = None, file_name: Optional[str] = None, **kwargs):

        if path is not None:
            self.dir_name = path
        elif 'dir_name' in kwargs:
            self.dir_name = kwargs.pop('dir_name')
        elif 'loc_pattern' in kwargs:
            self.dir_name = os.path.dirname(kwargs.get('loc_pattern'))
        else:
            self.dir_name = None

        if file_name is not None:
            self.file_name = file_name
        elif 'file_name' in kwargs:
            self.file_name = kwargs.pop('file_name')
        elif 'loc_pattern' in kwargs:
            self.file_name = os.path.basename(kwargs.pop('loc_pattern'))
        else:
            self.file_name = None

        self._creation_kwargs = {'type' : self.type, 'time_creation': datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

        super().__init__(**kwargs)

    @property
    def loc_pattern(self):
        return os.path.join(self.dir_name, self.file_name)

    @loc_pattern.setter
    def loc_pattern(self, path):
        self.dir_name  = os.path.dirname(path)
        self.file_name = os.path.basename(path)

    def path(self, time: Optional[pd.Timestamp] = None, **kwargs):
        return self.get_key(time, **kwargs)

    ## INPUT/OUTPUT METHODS
    def _read_data(self, input_path) -> (xr.DataArray, xr.Dataset, pd.DataFrame):
        return read_from_file(
            input_path, file_format=self.file_format, file_mode=self.file_mode)
    
    def _write_data(self, output: (xr.DataArray, pd.DataFrame), output_path: str, **kwargs) -> None:
        write_to_file(output,
                      output_path, file_format=self.file_format, file_mode=self.file_mode, file_type=self.file_type,
                      **kwargs)

    def _rm_data(self, path) -> None:
        rm_file(path)

    ## METHODS TO CHECK DATA AVAILABILITY
    def _check_data(self, data_path) -> bool:
        return os.path.exists(data_path)
    
    def _walk(self, prefix):
        for root, _, filenames in os.walk(prefix):
            for filename in filenames:
                yield os.path.join(root, filename)