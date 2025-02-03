"""
Library Features:

Name:          lib_proc_interp
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20250127'
Version:       '1.0.0'
"""
# ----------------------------------------------------------------------------------------------------------------------
# libraries
import numpy as np
import xarray as xr

from shybox.orchestrator_toolkit.lib_orchestrator_utils import as_process

# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# emthod to interpolate data
@as_process(input_type='data_grid', output_type='xarray')
def interpolate_data(data: (xr.DataArray, xr.Dataset), ref: xr.DataArray,
                     method='nn', max_distance=18000, neighbours=8, fill_value=np.nan,
                     var_name_geo_x='longitude', var_name_geo_y='latitude',
                     coord_name_x='longitude', coord_name_y='latitude', dim_name_x='longitude', dim_name_y='latitude',
                     **kwargs) -> xr.DataArray:
    # get data in
    values_in = data.values
    data_x_arr = data[var_name_geo_x].values
    data_y_arr = data[var_name_geo_y].values
    data_x_grid, data_y_grid = np.meshgrid(data_x_arr, data_y_arr)

    # get data reference
    ref_x_arr = ref[var_name_geo_x].values
    ref_y_arr = ref[var_name_geo_y].values
    ref_x_grid, ref_y_grid = np.meshgrid(ref_x_arr, ref_y_arr)

    geo_grid_in = GridDefinition(lons=geo_x_grid_in, lats=geo_y_grid_in)
    geo_grid_ref = GridDefinition(lons=geo_x_grid_ref, lats=geo_y_grid_ref)

    if interpolating_method == 'nn':
        values_out_tmp = resample_nearest(
            geo_grid_in, values_in, geo_grid_ref,
            radius_of_influence=interpolating_max_distance,
            fill_value=interpolating_fill_value)

    elif interpolating_method == 'gauss':
        values_out_tmp = resample_gauss(
            geo_grid_in, values_in, geo_grid_ref,
            radius_of_influence=interpolating_max_distance,
            neighbours=resampling_neighbours, sigmas=250000,
            fill_value=interpolating_fill_value)

    elif interpolating_method == 'idw':
        weight_fx = lambda r: 1 / r ** 2
        values_out_tmp = resample_custom(
            geo_grid_in, values_in, geo_grid_ref,
            radius_of_influence=interpolating_max_distance, neighbours=interpolating_neighbours,
            weight_funcs=weight_fx,
            fill_value=interpolating_fill_value)
    else:
        logging.error(' ===> Interpolating method "' + interpolating_method + '" is not available')
        raise NotImplemented('Case not implemented yet')

    if interpolating_fill_value is None:
        values_out_resampled = values_out_tmp.data
    else:
        values_out_resampled = deepcopy(values_out_tmp)

    obj_da_out = create_darray_2d(
        values_out_resampled, geo_x_grid_ref[0, :], geo_y_grid_ref[:, 0], name=var_name_data,
        coord_name_x=coord_name_x, coord_name_y=coord_name_y,
        dim_name_x=dim_name_x, dim_name_y=dim_name_y)



# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to interpolate data
def interpolate_data_NEW(obj_da_in, obj_da_ref,
                     var_name_data='surface_soil_moisture', var_name_geo_x='longitude', var_name_geo_y='latitude',
                     coord_name_x='longitude', coord_name_y='latitude', dim_name_x='longitude', dim_name_y='latitude',
                     interpolating_active=True,
                     interpolating_method='nn', interpolating_max_distance=18000,
                     interpolating_neighbours=8, interpolating_fill_value=np.nan,
                     ):

    # get data in
    values_in = obj_da_in.values
    geo_x_arr_in = obj_da_in[var_name_geo_x].values
    geo_y_arr_in = obj_da_in[var_name_geo_y].values
    geo_x_grid_in, geo_y_grid_in = np.meshgrid(geo_x_arr_in, geo_y_arr_in)

    # get data reference
    geo_x_arr_ref = obj_da_ref[var_name_geo_x].values
    geo_y_arr_ref = obj_da_ref[var_name_geo_y].values
    geo_x_grid_ref, geo_y_grid_ref = np.meshgrid(geo_x_arr_ref, geo_y_arr_ref)

    if (((geo_x_grid_in.shape[0] != geo_x_grid_ref.shape[0]) or (geo_y_grid_in.shape[1] != geo_y_grid_ref.shape[1]))
            or interpolating_active):

        geo_grid_in = GridDefinition(lons=geo_x_grid_in, lats=geo_y_grid_in)
        geo_grid_ref = GridDefinition(lons=geo_x_grid_ref, lats=geo_y_grid_ref)

        if interpolating_method == 'nn':
            values_out_tmp = resample_nearest(
                geo_grid_in, values_in, geo_grid_ref,
                radius_of_influence=interpolating_max_distance,
                fill_value=interpolating_fill_value)

        elif interpolating_method == 'gauss':
            values_out_tmp = resample_gauss(
                geo_grid_in, values_in, geo_grid_ref,
                radius_of_influence=interpolating_max_distance,
                neighbours=resampling_neighbours, sigmas=250000,
                fill_value=interpolating_fill_value)

        elif interpolating_method == 'idw':
            weight_fx = lambda r: 1 / r ** 2
            values_out_tmp = resample_custom(
                geo_grid_in, values_in, geo_grid_ref,
                radius_of_influence=interpolating_max_distance, neighbours=interpolating_neighbours,
                weight_funcs=weight_fx,
                fill_value=interpolating_fill_value)
        else:
            logging.error(' ===> Interpolating method "' + interpolating_method + '" is not available')
            raise NotImplemented('Case not implemented yet')

        if interpolating_fill_value is None:
            values_out_resampled = values_out_tmp.data
        else:
            values_out_resampled = deepcopy(values_out_tmp)

        obj_da_out = create_darray_2d(
            values_out_resampled, geo_x_grid_ref[0, :], geo_y_grid_ref[:, 0], name=var_name_data,
            coord_name_x=coord_name_x, coord_name_y=coord_name_y,
            dim_name_x=dim_name_x, dim_name_y=dim_name_y)

    else:
        obj_da_out = deepcopy(obj_da_in)

    return obj_da_out
# ----------------------------------------------------------------------------------------------------------------------
