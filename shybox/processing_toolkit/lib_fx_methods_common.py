"""
Library Features:

Name:          lib_fx_methods_common
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20230727'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import warnings
import os
import numpy as np

from copy import deepcopy

from pyresample.geometry import GridDefinition
from pyresample.kd_tree import resample_nearest, resample_gauss, resample_custom#
from repurpose.resample import resample_to_grid

from lib_utils_io import create_darray_2d

# logging
warnings.filterwarnings("ignore")

# debug
import matplotlib.pylab as plt
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to crop data
def crop_data(obj_in,
              geo_x_left, geo_x_right, geo_y_lower, geo_y_upper,
              var_name_geo_x='longitude', var_name_geo_y='latitude'):

    obj_out, mask_lon, mask_lat = {}, None, None
    for var_key, var_da_in in obj_in.items():

        if (mask_lon is None) or (mask_lat is None):
            values_lon, values_lat = var_da_in[var_name_geo_x], var_da_in[var_name_geo_y]
            mask_lon = (values_lon >= geo_x_left) & (values_lon <= geo_x_right)
            mask_lat = (values_lat >= geo_y_lower) & (values_lat <= geo_y_upper)

        var_da_out = var_da_in.where(mask_lon & mask_lat, drop=True)

        ''' debug
        plt.figure(); plt.imshow(var_da_in.values); plt.colorbar()
        plt.figure(); plt.imshow(var_da_out.values); plt.colorbar()
        plt.show()
        '''

        obj_out[var_key] = var_da_out

    return obj_out
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to resample data
def resample_data(
        obj_in,
        geo_x_out, geo_y_out,
        var_name_geo_x='longitude', var_name_geo_y='latitude',
        coord_name_x='longitude', coord_name_y='latitude', dim_name_x='longitude', dim_name_y='latitude',
        resampling_method='nn',
        resampling_max_distance=18000, resampling_neighbours=8, resampling_min_neighbours=1,
        resampling_fill_value=np.nan, resampling_extend_data=True,
        **kwargs):

    # define geographical information
    if (geo_x_out.ndim == 1) and (geo_y_out.ndim == 1):
        geo_x_grid_out, geo_y_grid_out = np.meshgrid(geo_x_out, geo_y_out)
    elif (geo_x_out.ndim == 2) and (geo_y_out.ndim == 2):
        geo_x_grid_out, geo_y_grid_out = deepcopy(geo_x_out), deepcopy(geo_y_out)
    else:
        logging.error(' ===> Geographical information format is not supported')
        raise NotImplemented('Case not implemented yet')

    # check south-north array orientation
    geo_y_upper_out, geo_y_lower_out = geo_y_grid_out[0, 0], geo_y_grid_out[-1, 0]
    if geo_y_upper_out < geo_y_lower_out:
        geo_y_grid_out = np.flipud(geo_y_grid_out)

    geo_grid_out = GridDefinition(lons=geo_x_grid_out, lats=geo_y_grid_out)

    obj_out, mask_lon, mask_lat = {}, None, None
    for var_key, var_da_in in obj_in.items():

        values_grid_in = var_da_in.values
        geo_x_arr_in, geo_y_arr_in = var_da_in[var_name_geo_x].values, var_da_in[var_name_geo_y].values
        geo_x_grid_in, geo_y_grid_in = np.meshgrid(geo_x_arr_in, geo_y_arr_in)

        geo_y_upper_in, geo_y_lower_in = geo_y_grid_in[0, 0], geo_y_grid_in[-1, 0]
        if geo_y_upper_in < geo_y_lower_in:
            geo_y_grid_in = np.flipud(geo_y_grid_in)
            values_grid_in = np.flipud(values_grid_in)

        geo_grid_in = GridDefinition(lons=geo_x_grid_in, lats=geo_y_grid_in)

        if resampling_method == 'nn':
            values_masked = resample_nearest(
                geo_grid_in, values_grid_in, geo_grid_out,
                radius_of_influence=resampling_max_distance,
                fill_value=resampling_fill_value)

        elif resampling_method == 'gauss':
            values_masked = resample_gauss(
                geo_grid_in, values_grid_in, geo_grid_out,
                radius_of_influence=resampling_max_distance, neighbours=resampling_neighbours, sigmas=250000,
                fill_value=resampling_fill_value)

        elif resampling_method == 'idw':
            weight_fx = lambda r: 1 / r ** 2
            values_masked = resample_custom(
                geo_grid_in, values_grid_in, geo_grid_out,
                radius_of_influence=resampling_max_distance, neighbours=resampling_neighbours,
                weight_funcs=weight_fx,
                fill_value=resampling_fill_value)
        else:
            logging.error(' ===> Resampling method "' + resampling_method + '" is not available')
            raise NotImplemented('Case not implemented yet')

        if resampling_fill_value is None:
            values_out_resampled = values_masked.data
        else:
            values_out_resampled = deepcopy(values_masked)

        # check if all values are nan(s)
        flag_all_nans = np.all(np.isnan(values_out_resampled))

        # condition finite value(s)
        if not flag_all_nans:
            # condition extend data
            if resampling_extend_data:
                values_arr_tmp = values_grid_in.ravel()
                nan_idx_tmp = np.argwhere(np.isnan(values_arr_tmp))[:, 0]
                geo_x_arr_tmp, geo_y_arr_tmp = geo_x_grid_in.ravel(), geo_y_grid_in.ravel()

                # extent to grid without nan(s)
                values_arr_ext = np.delete(values_arr_tmp, nan_idx_tmp)
                geo_x_arr_ext = np.delete(geo_x_arr_tmp, nan_idx_tmp)
                geo_y_arr_ext = np.delete(geo_y_arr_tmp, nan_idx_tmp)

                # check finite values
                flag_any_finite = np.any(values_arr_ext)
                if flag_any_finite:
                    ext_obj = resample_to_grid(
                        {var_key: values_arr_ext},
                        geo_x_arr_ext, geo_y_arr_ext, geo_x_grid_out, geo_y_grid_out,
                        search_rad=resampling_max_distance, fill_values=resampling_fill_value,
                        min_neighbours=resampling_min_neighbours, neighbours=resampling_neighbours)

                    if resampling_fill_value is None:
                        values_out_extended = ext_obj[var_key].data
                    else:
                        values_out_extended = ext_obj[var_key]
                    values_out_finite = True
                else:
                    values_out_extended, values_out_finite = None, False
                    logging.warning(' ===> Resample method was skipped; all extended values are defined by Nan(s)')

            else:
                values_out_extended, values_out_finite = None, True
        else:
            values_out_extended, values_out_finite = None, False
            logging.warning(' ===> Resample method was skipped; all resampled values are defined by Nan(s)')

        if values_out_extended is not None:
            idx_finite = np.argwhere(np.isfinite(values_out_resampled))
            idx_nans = np.argwhere(np.isnan(values_out_resampled) & np.isnan(values_out_extended))
            idx_finite_x, idx_finite_y = idx_finite[:, 0].astype(int), idx_finite[:, 1].astype(int)
            idx_nans_x, idx_nans_y = idx_nans[:, 0].astype(int), idx_nans[:, 1].astype(int)
            values_out_extended[idx_finite_x, idx_finite_y] = values_out_resampled[idx_finite_x, idx_finite_y]
            values_out_extended[idx_nans_x, idx_nans_y] = resampling_fill_value
            values_grid_out = deepcopy(values_out_extended)
        else:
            if values_out_finite:
                values_grid_out = deepcopy(values_out_resampled)
            else:
                logging.warning(' ===> Resampled datasets for variable "' +
                                var_key + '" is not available. Datasets will be defined by NoneType')
                values_grid_out = None

        ''' debug
        plt.figure(); plt.imshow(values_grid_in); plt.colorbar(); plt.clim(0,1)
        plt.figure(); plt.imshow(values_grid_out); plt.colorbar(); plt.clim(0,1)
        plt.show()
        '''
        # method to create data array
        if values_out_finite:
            var_da_out = create_darray_2d(
                values_grid_out, geo_x_grid_out[0, :], geo_y_grid_out[:, 0], name=var_key,
                coord_name_x=coord_name_x, coord_name_y=coord_name_y,
                dim_name_x=dim_name_x, dim_name_y=dim_name_y)
        else:
            var_da_out = None

        obj_out[var_key] = var_da_out

    return obj_out
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to mask data
def mask_data(obj_in, reference_da, mask_value_min=0, mask_value_max=None, mask_no_data=np.nan,
              var_name_geo_x='longitude', var_name_geo_y='latitude',
              coord_name_x='longitude', coord_name_y='latitude', dim_name_x='longitude', dim_name_y='latitude',
              ):

    obj_out = {}
    for var_key, var_da_in in obj_in.items():

        if var_da_in is not None:
            values_out = deepcopy(var_da_in.values)
            geo_x_arr, geo_y_arr = var_da_in[var_name_geo_x].values, var_da_in[var_name_geo_y].values
            mask_values = reference_da.values

            if mask_value_min is not None:
                values_out[mask_values < mask_value_min] = mask_no_data
            if mask_value_max is not None:
                values_out[mask_values > mask_value_max] = mask_no_data

            # method to create data array
            var_da_out = create_darray_2d(
                values_out, geo_x_arr, geo_y_arr, name=var_key,
                coord_name_x=coord_name_x, coord_name_y=coord_name_y,
                dim_name_x=dim_name_x, dim_name_y=dim_name_y)

            ''' debug
            plt.figure(); plt.imshow(var_da_in.values); plt.colorbar(); plt.clim(0, 1)
            plt.figure(); plt.imshow(var_da_out.values); plt.colorbar(); plt.clim(0 , 1)
            plt.show()
            '''
        else:
            logging.warning(' ===> Masked datasets for variable "' +
                            var_key + '" is not available. Datasets will be defined by NoneType')
            var_da_out = None

        obj_out[var_key] = var_da_out

    return obj_out

# ----------------------------------------------------------------------------------------------------------------------

