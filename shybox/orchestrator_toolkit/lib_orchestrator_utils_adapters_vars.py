# ----------------------------------------------------------------------------------------------------------------------
"""
Library Features:

Name:          lib_orchestrator_utils_adapters_vars
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
        group_separator: str = '|', key_separator: str = '|', other_separator: str = '::',
        **kwargs) -> dict:

    # initialize mapped data
    data_mapped = {}
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

        # initialize mapping status
        mapping_found = False
        # iterate over mapping groups
        for group_name, group_mapping in mapping.items():

            # check if key is available in mapping
            if data_key_base not in group_mapping:
                continue

            # split group name
            if group_separator in group_name:
                group_root, group_var = group_name.rsplit(group_separator, 1)
            else:
                group_root, group_var = group_name, None

            # get mapped variable name
            variable_name = group_mapping[data_key_base]

            # restore multiple variable suffix
            if data_key_idx is not None:
                variable_name = other_separator.join([variable_name, data_key_idx])

            # update data key
            if group_var is not None:
                data_key_updated = group_separator.join([group_root, variable_name])
            else:
                data_key_updated = group_separator.join([group_name, variable_name])

            # store using updated key
            data_mapped[data_key_updated] = data_value

            # mark mapping as found
            mapping_found = True
            break

        # manage missing mapping
        if not mapping_found:

            # warning for missing mapping
            logger_stream.warning(f'Input variable "{data_key_current}" is not available in mapping.')

            # keep the current key if not declared
            data_mapped[data_key_current] = data_value

    return data_mapped
# ----------------------------------------------------------------------------------------------------------------------
