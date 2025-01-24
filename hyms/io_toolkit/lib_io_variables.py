"""
Library Features:

Name:          lib_io_variables
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20240911'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import numpy as np
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to fill variable generic
def fill_var_generic(terrain: np.ndarray,
                     default_value: (int, float) = -9999.0, no_data_value: (int, float) = -9999.0,
                     **kwargs) -> np.ndarray:
    var_generic = np.where(terrain >= 0, default_value, no_data_value)
    return var_generic
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to fill variable air pressure
def fill_var_air_pressure(terrain: np.ndarray, no_data: float = -9999.0, **kwargs) -> np.ndarray:
    var_pa = np.where(terrain >= 0, 101.3 * ((293 - 0.0065 * terrain) / 293) ** 5.26, no_data)  # [kPa]
    return var_pa
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to fill variable error
def fill_var_error(variable: str):
    raise RuntimeError('Function to fill variable "' + variable + '" not found in the library')
# ----------------------------------------------------------------------------------------------------------------------
