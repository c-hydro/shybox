"""
Library Features:

Name:          lib_orchestrator_adapters_registry
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20260925'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
from shybox.orchestrator_toolkit.lib_orchestrator_utils_adapters_fx_list import (
    adapter_list_to_xarray, adapter_variables_to_group,)
from shybox.orchestrator_toolkit.lib_orchestrator_utils_adapters_fx_single import dict_to_list, obj_to_dict

from shybox.orchestrator_toolkit.lib_orchestrator_utils_adapters_vars import mapping_keys_by_separator
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# collect adapters type
ADAPTERS_TYPE = {
    'list_to_darray': adapter_list_to_xarray,
    'dict_to_list': dict_to_list,
    'obj_to_dict': obj_to_dict,
    'mapping_keys_by_separator': mapping_keys_by_separator,
}

# collect adapter fx time-series
ADAPTERS_FX_TS = {
    'adapter_variables_to_group': adapter_variables_to_group
}

# collect adapter fx maps
ADAPTERS_FX_MAPS= {
    'interpolate': 'test'}
# ----------------------------------------------------------------------------------------------------------------------
