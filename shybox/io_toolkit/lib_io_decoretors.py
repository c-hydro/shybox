"""
Library Features:

Name:          lib_io_decoretors
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20260824'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
from functools import wraps
import numpy as np
import pandas as pd
import xarray as xr
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# decoretor to adapt object (call an adapter to make it)
def adapt_obj(adapter):

    def decorator(func):

        @wraps(func)
        def wrapper(obj, *args, **kwargs):
            adapted = adapter(obj, **kwargs)
            return func(*args, **adapted)
        return wrapper

    return decorator
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# decoretor to iterate object(s) dict, list and tuple
def iterate_obj(func):

    @wraps(func)
    def wrapper(data, *args, **kwargs):

        # dictionary
        if isinstance(data, dict):
            results = {}
            for variable_name, variable_data in data.items():
                results[variable_name] = func(variable_data, *args, variable_name=variable_name, **kwargs)
            return results

        # list
        elif isinstance(data, list):

            results = []
            for variable_idx, variable_data in enumerate(data):
                results.append(func(variable_data, *args, variable_name=str(variable_idx), **kwargs))
            return results

        # tuple
        elif isinstance(data, tuple):

            results = []
            for variable_idx, variable_data in enumerate(data):
                results.append(func(variable_data, *args, variable_name=str(variable_idx), **kwargs))
            return tuple(results)

        # single object
        else:

            return func(data, *args, **kwargs)

    return wrapper
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to compose decorators
def compose_decorators(func, *decorators):
    func_out = func
    for decorator in reversed(decorators):
        func_out = decorator(func_out)
    return func_out
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# helper to convert obj to array
def obj_to_array(obj, obj_type=None):

    if isinstance(obj, np.ndarray):
        return obj
    if isinstance(obj, xr.DataArray):
        return obj.values
    if isinstance(obj, pd.DataFrame):
        return obj.values
    if hasattr(obj, "values"):
        return np.asarray(obj.values)

    return np.asarray(obj)
# ----------------------------------------------------------------------------------------------------------------------
