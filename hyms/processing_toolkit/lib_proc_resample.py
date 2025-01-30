

# ----------------------------------------------------------------------------------------------------------------------
# method to resample args
def resample_data_args(
        grid_dframe, resampling_max_distance=18000,
        resampling_grid_resolution=None, resampling_min_neighbours=1, resampling_neighbours=8,
        resampling_kernel_active=False,
        resampling_kernel_method='box', resampling_kernel_width=15,
        resampling_kernel_mode='center', resampling_kernel_stddev=4):

    grid_attrs = grid_dframe.attrs
    grid_bbox = grid_attrs['bbox']
    grid_bb_right, grid_bb_left = grid_attrs['bb_right'], grid_attrs['bb_left']
    grid_bb_bottom, grid_bb_top = grid_attrs['bb_bottom'], grid_attrs['bb_top']
    if resampling_grid_resolution is None:
        grid_res_geo_x, grid_res_geo_y = grid_attrs['res_lon'], grid_attrs['res_lat']
    else:
        grid_res_geo_x = grid_res_geo_y = resampling_grid_resolution

    # organize resample argument(s)
    resample_kwargs = {
        'geo_bbox': grid_bbox,
        'geo_x_right': grid_bb_right, 'geo_x_left': grid_bb_left,
        'geo_y_lower': grid_bb_bottom, 'geo_y_upper': grid_bb_top,
        'geo_x_res': grid_res_geo_x, 'geo_y_res': grid_res_geo_y,
        'resampling_max_distance': resampling_max_distance,
        'resampling_min_neighbours': resampling_min_neighbours,
        'resampling_neighbours': resampling_neighbours,
        'filtering_active': resampling_kernel_active,
        'filtering_method': resampling_kernel_method,
        'filtering_width': resampling_kernel_width,
        'filtering_mode': resampling_kernel_mode,
        'filtering_stddev': resampling_kernel_stddev,
    }

    return resample_kwargs
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to resample data
def resample_data_fx(file_dframe,
                     geo_x_left, geo_x_right, geo_y_lower, geo_y_upper,
                     geo_x_res=0.01, geo_y_res=0.01, geo_no_data=np.nan,
                     var_name_data='surface_soil_moisture', var_name_geo_x='longitude', var_name_geo_y='latitude',
                     coord_name_x='longitude', coord_name_y='latitude', dim_name_x='longitude', dim_name_y='latitude',
                     resampling_max_distance=18000, resampling_min_neighbours=1, resampling_neighbours=8,
                     filtering_active=True,
                     filtering_method='box', filtering_width=15, filtering_mode='center', filtering_stddev=4,
                     **kwargs):

    # get data out
    geo_x_arr_out = np.arange(geo_x_left, geo_x_right, geo_x_res)
    geo_y_arr_out = np.arange(geo_y_lower, geo_y_upper, geo_y_res)
    geo_x_grid_out, geo_y_grid_out = np.meshgrid(geo_x_arr_out, geo_y_arr_out)

    # get data in
    data_in = file_dframe[var_name_data].values
    geo_x_arr_in = file_dframe[var_name_geo_x].values
    geo_y_arr_in = file_dframe[var_name_geo_y].values
    geo_x_grid_in, geo_y_grid_in = np.meshgrid(geo_x_arr_in, geo_y_arr_in)

    # method to resample data to grid
    data_masked = resample_to_grid(
        {var_name_data: data_in},
        geo_x_arr_in, geo_y_arr_in, geo_x_grid_out, geo_y_grid_out, search_rad=resampling_max_distance,
        min_neighbours=resampling_min_neighbours, neighbours=resampling_neighbours)
    data_grid_out = data_masked[var_name_data].data

    filter_grid = None
    if filtering_active:
        filter_masked = resample_to_grid(
            {var_name_data: data_in},
            geo_x_arr_in, geo_y_arr_in, geo_x_grid_out, geo_y_grid_out, search_rad=resampling_max_distance / 5,
            min_neighbours=resampling_min_neighbours, neighbours=resampling_neighbours)
        filter_grid = filter_masked[var_name_data].data

    # check south-north array orientation
    geo_y_upper, geo_y_lower = geo_y_grid_out[0, 0], geo_y_grid_out[-1, 0]
    if geo_y_upper < geo_y_lower:
        geo_y_grid_out = np.flipud(geo_y_grid_out)
        data_grid_out = np.flipud(data_grid_out)
        if filtering_active:
            filter_grid = np.flipud(filter_grid)

    if filtering_active:
        if filtering_method == 'gaussian':
            # filter values and create a filtered grid
            obj_kernel = Gaussian2DKernel(
                x_stddev=filtering_stddev, mode=filtering_mode)
        elif filtering_method == 'box':
            obj_kernel = Box2DKernel(width=filtering_width, mode=Box2DKernel)
        else:
            raise NotImplementedError('Filtering method "' + filtering_method + '" not implemented yet')

        filter_kernel = convolve(filter_grid, obj_kernel)
        data_grid_out[filter_kernel == 0] = geo_no_data

    ''' debug
    plt.figure()
    plt.imshow(data_grid)
    plt.colorbar()
    plt.clim(0, 100)
    plt.show()
    '''

    # method to create data array
    data_out = create_darray_2d(
        data_grid_out, geo_x_grid_out[0, :], geo_y_grid_out[:, 0], name=var_name_data,
        coord_name_x=coord_name_x, coord_name_y=coord_name_y,
        dim_name_x=dim_name_x, dim_name_y=dim_name_y)

    return data_out
# ----------------------------------------------------------------------------------------------------------------------