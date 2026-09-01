# libraries
from shybox.orchestrator_toolkit.lib_orchestrator_utils_adapters_fx_list import adapter_list_to_xarray
from shybox.orchestrator_toolkit.lib_orchestrator_utils_adapters_fx_single import adapter_source_obj

# collect adapters type
ADAPTERS_TYPE = {
    'list_to_darray': adapter_list_to_xarray,
    'single_obj': adapter_source_obj,
}

# collect adapter fx time-series
ADAPTERS_FX_TS = {
    'adapt_averate_over_mask': 'test'}

# collect adapter fx maps
ADAPTERS_FX_MAPS= {
    'interpolate': 'test'}
