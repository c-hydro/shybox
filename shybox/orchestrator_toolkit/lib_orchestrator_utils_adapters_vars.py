# ----------------------------------------------------------------------------------------------------------------------
"""
Library Features:

Name:          lib_orchestrator_utils_vars
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20260123'
Version:       '1.1.0'
"""
# ----------------------------------------------------------------------------------------------------------------------
# libraries
from shybox.logging_toolkit.lib_logging_utils import with_logger
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# helper to map input variables
@with_logger(var_name='logger_stream')
def mapping_keys_by_separator(
        data: dict, mapping: dict,
        key_separator: str = '|', other_separator: str = '::',
        **kwargs) -> dict:

    # initialize mapped data
    data_mapped = {}
    # iterate over input groups
    for group_name, group_mapping in mapping.items():
        # iterate over input data
        for data_key_current, data_value in data.items():

            # initialize base key and multiple variable index
            data_key_base = data_key_current
            data_key_idx = None

            # check for multiple variable suffix, e.g. data::1
            if other_separator in data_key_current:

                # split variable name and index
                key_base, key_idx = data_key_current.rsplit(other_separator, 1)

                # use suffix only if it is a valid numeric index
                if key_idx.isdigit():
                    data_key_base = key_base
                    data_key_idx = key_idx

            # check if key is available in mapping
            if data_key_base in group_mapping:

                # get mapped variable name
                variable_name = group_mapping[data_key_base]

                # restore multiple variable suffix
                if data_key_idx is not None:
                    variable_name = other_separator.join([variable_name, data_key_idx])

                # update dataset key
                data_key_updated = key_separator.join([group_name, variable_name])
                # store using mapped name
                data_mapped[data_key_updated] = data_value

            else:
                # warning for missing mapping
                logger_stream.warning(
                    f'Input variable "{data_key_current}" is not available in mapping for group "{group_name}".')
                # keep the current key if not declared
                data_mapped[data_key_current] = data_value

    return data_mapped
# ----------------------------------------------------------------------------------------------------------------------
