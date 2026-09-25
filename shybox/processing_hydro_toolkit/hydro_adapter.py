"""
Library Features:

Name:          lib_proc_compute_sm
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20260327'
Version:       '1.3.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import xarray as xr

from shybox.orchestrator_toolkit.lib_orchestrator_utils_processes import as_process
from shybox.logging_toolkit.lib_logging_utils import with_logger

from shybox.orchestrator_toolkit.lib_orchestrator_utils_adapters_fx_single import xarray_to_dataarray
from shybox.processing_hydro_toolkit.lib_proc_compute_sm import merge_soil_moisture
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to compute soil moisture using domain mask, gaps filling and weighted mean
@as_process(input_type='xarray', output_type='xarray',
            input_adapter={
                'data': [
                    {'method': 'mapping_keys_by_separator', 'kwargs': {'key_separator':'|', 'other_separator':'::'}},
                    {'method': 'adapter_variables_to_group', 'kwargs': {}},
                    {'method': 'dict_to_list', 'kwargs': {}},
                ]
            },
            args_adapter={
                'watermark': {
                    'method': 'dict_to_list',
                    'kwargs': {'flat_list_one_element': False},
                },
                'ref': {
                    'method': 'dict_to_list',
                    'kwargs': {'flat_list_one_element': True},
                },
            },
            output_adapter={
                'data': {"method": "obj_to_dict", "kwargs": {}},
            }
        )
@with_logger(var_name='logger_stream')
def exec_soil_moisture(
        data,
        ref: xr.DataArray,
        watermark: (list, xr.DataArray) = None,
        var_name_in: str = 'soil_moisture', var_name_out: str = 'soil_moisture',
        ref_no_data=-9999.0, value_no_data=-9999.0,
        coord_name_x='longitude', coord_name_y='latitude',
        dim_name_x='longitude', dim_name_y='latitude',
        thr_sm_min_flag: bool = False, thr_sm_max_flag: bool = False,
        thr_sm_min_value: float = 0.0, thr_sm_max_value: float = 1.0,
        debug_box: bool = False, debug_geo: bool = False, debug_data_raw: bool = False,
        debug_data_by_watermark: bool = False, debug_data_by_river_network: bool = False,
        debug_data_by_resampling: bool = False,
        debug_data_merge_domain_partial: bool = False, debug_data_merge_domain_gaps: bool = False,
        debug_data_merge_domain_merged: bool = False, debug_data_merge_domain_weighted: bool = False,
        fill_gaps_flag: bool = True, fill_gaps_distance: int = 2,
        interpolate_flag: bool = False,
        interpolate_method: str = "nn", interpolate_roi: float = 20000, interpolate_neighbours: int =  7,
        weight_flag: bool = True,
        weight_radius: int = 1, weight_roi: int = 5,weight_method: str ='distance',
        **kwargs):

    # algorithm info start
    logger_stream.info_up("Execute soil moisture ... ")

    # normalize to list of Datasets
    if isinstance(data, (xr.DataArray, xr.Dataset)):
        data_list = [xarray_to_dataarray(data)]
    elif isinstance(data, (list, tuple)):
        data_list = [xarray_to_dataarray(it) for it in data]
    else:
        raise TypeError(f"`data` must be a DataArray, Dataset, or list/tuple. Got {type(data)}")

    # normalize watermark to list aligned to ds_list (KEEP list behavior)
    if watermark is None:
        wm_list = [None] * len(data_list)
    elif isinstance(watermark, (xr.DataArray, xr.Dataset)):
        # same watermark for all datasets (your previous behavior)
        wm_list = [xarray_to_dataarray(watermark)] * len(ds_list)
    elif isinstance(watermark, (list, tuple)):
        wm_list = [xarray_to_dataarray(it) for it in watermark]
    else:
        raise TypeError(f"`watermark` must be DataArray/Dataset or list/tuple. Got {type(watermark)}")

    # check data and watermark list lengths
    if len(data_list) != len(wm_list):
        logger_stream.error(f"Data and watermark lengths do not match: data={len(data_list)}, watermark={len(wm_list)}")
        raise ValueError(f"Data and watermark lengths do not match: data={len(data_list)}, watermark={len(wm_list)}")

    # define the variable list
    var_list = sorted({da.name if da.name is not None else 'var' for da in data_list})
    if isinstance(var_list, list) and len(var_list) == 1:
        logger_stream.info(f"Variable datasets: {var_list[0]} "
                           f"-- Variable src: {var_name_in} -- Variable dst {var_name_out}")
        var_fx_in = var_fx_out = var_list[0]
    else:
        logger_stream.error('Multiple variables found in datasets. Please specify variable names explicitly.')
        raise ValueError('Multiple variables found in datasets. Please specify variable names explicitly.')

    # merge soil moisture
    output = merge_soil_moisture(
        data=data_list, ref=ref, watermark=wm_list,

        var_name_in=var_fx_in, var_name_out=var_fx_out,
        ref_no_data=ref_no_data, value_no_data=value_no_data,
        coord_name_x=coord_name_x, coord_name_y=coord_name_y, dim_name_x=dim_name_x, dim_name_y=dim_name_y,

        thr_sm_min_flag=thr_sm_min_flag, thr_sm_max_flag=thr_sm_max_flag,
        thr_sm_min_value=thr_sm_min_value, thr_sm_max_value=thr_sm_max_value,

        debug_box=debug_box,debug_geo=debug_geo,
        debug_data_raw=debug_data_raw,
        debug_data_by_watermark=debug_data_by_watermark,
        debug_data_by_river_network=debug_data_by_river_network,
        debug_data_by_resampling=debug_data_by_resampling,
        debug_data_merge_domain_partial=debug_data_merge_domain_partial,
        debug_data_merge_domain_gaps=debug_data_merge_domain_gaps,
        debug_data_merge_domain_merged=debug_data_merge_domain_merged,
        debug_data_merge_domain_weighted=debug_data_merge_domain_weighted,

        fill_gaps_flag=fill_gaps_flag, fill_gaps_distance=fill_gaps_distance,

        interpolate_flag=interpolate_flag, interpolate_method=interpolate_method,
        interpolate_roi=interpolate_roi, interpolate_neighbours=interpolate_neighbours,

        weight_flag=weight_flag, weight_radius=weight_radius,
        weight_max_roi=weight_roi, weight_method=weight_method,

        **kwargs
    )

    # algorithm info end
    logger_stream.info_down("Execute soil moisture ... DONE")

    if isinstance(output, xr.DataArray):
        output.name = var_name_out

    return output
# ----------------------------------------------------------------------------------------------------------------------
