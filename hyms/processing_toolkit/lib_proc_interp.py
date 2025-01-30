# ----------------------------------------------------------------------------------------------------------------------
# method to mask data
def mask_data(da_obj_in, da_reference, mask_value_min=0, mask_value_max=None, mask_no_data=np.nan,
              var_name_data='variable', var_name_geo_x='longitude', var_name_geo_y='latitude',
              coord_name_x='longitude', coord_name_y='latitude', dim_name_x='longitude', dim_name_y='latitude',
              ):

    data_values = da_obj_in.values
    geo_x_values = da_obj_in[var_name_geo_x].values
    geo_y_values = da_obj_in[var_name_geo_y].values
    mask_values = da_reference.values

    if mask_value_min is not None:
        data_values[mask_values < mask_value_min] = mask_no_data
    if mask_value_max is not None:
        data_values[mask_values > mask_value_max] = mask_no_data

    # method to create data array
    da_obj_out = create_darray_2d(
        data_values, geo_x_values, geo_y_values, name=var_name_data,
        coord_name_x=coord_name_x, coord_name_y=coord_name_y,
        dim_name_x=dim_name_x, dim_name_y=dim_name_y)

    return da_obj_out

# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to interpolate data
def interpolate_data(obj_da_in, obj_da_ref,
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
