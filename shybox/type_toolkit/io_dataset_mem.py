import xarray as xr
import pandas as pd

from shybox.type_toolkit.io_dataset_base import Dataset
from parse_utils import extract_date_and_tags

class DataMem(Dataset):

    type = 'memory_dataset'

    def __init__(self, loc_pattern: str, keep_after_reading = False, **kwargs):
        self.loc_pattern = loc_pattern
        super().__init__(**kwargs)
        self.data_dict = {}
        self.keep_after_reading = keep_after_reading

    @property
    def loc_pattern(self):
        return self._loc_pattern

    @loc_pattern.setter
    def loc_pattern(self, loc_pattern):
        self._loc_pattern = loc_pattern

    ## INPUT/OUTPUT METHODS
    def _read_data(self, input_key):
        if self.keep_after_reading:
            return self.data_dict.get(input_key)
        else:
            return self.data_dict.pop(input_key)
    
    def _write_data(self, output: (xr.DataArray, pd.DataFrame), output_key: str, **kwargs):
        self.data_dict[output_key] = output

    def _rm_data(self, key):
        self.data_dict.pop(key)

    ## METHODS TO CHECK DATA AVAILABILITY
    def _check_data(self, data_path) -> bool:
        for key in self.data_dict.keys():
            if key.startswith(data_path):
                return True
        else:
            return False
    
    def _walk(self, prefix):
        for key in self.data_dict.keys():
            if key.startswith(prefix):
                yield key
    
    def update(self, in_place = False, **kwargs):
        new_self = super().update(in_place = in_place, **kwargs)

        for key in self.available_keys:
            try: 
                extract_date_and_tags(key, new_self.key_pattern)
                new_self.data_dict[key] = self.data_dict.get(key)
            except ValueError:
                pass

        if in_place:
            self = new_self
            return self
        else:
            return new_self
