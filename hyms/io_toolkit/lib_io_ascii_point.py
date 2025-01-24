# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import warnings
import pandas as pd

from collections import OrderedDict
from copy import deepcopy

from hmc.generic_toolkit.data.lib_io_utils import parse_row2str
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to read point data section(s)
def get_file_point_section(file_name: str, columns_name: list = None) -> (pd.DataFrame, dict):

    if columns_name is None:
        columns_name = ['x', 'y', 'catchment', 'section', 'code', 'area', 'thr_1', 'thr_2']

    file_data = pd.read_table(file_name, header=None, delim_whitespace=True)

    if len(columns_name) != len(file_data.columns):
        file_data = file_data.iloc[:, 0: len(columns_name)]

    if len(file_data.columns) < 4:
        raise ValueError(f'File {file_name} does not have the minimum number of columns required.')

    if columns_name is None:
        columns_name = ['index', 'values']
    file_data.columns = columns_name

    file_dims = {'section': len(file_data.index)}

    return file_data, file_dims
# ----------------------------------------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# method to read point data dam(s)
def get_file_point_dam(file_name: str, line_delimiter: str = '#', line_reorder: bool = False) -> (dict, dict):

    file_handle = open(file_name, 'r')
    file_lines = file_handle.readlines()
    file_handle.close()

    row_id = 0
    dam_n = int(file_lines[row_id].split(line_delimiter)[0])
    row_id += 1
    plant_n = int(file_lines[row_id].split(line_delimiter)[0])

    if dam_n > 0:

        point_frame = None
        for dam_id in range(0, dam_n):
            row_id += 1
            _ = parse_row2str(file_lines[row_id], line_delimiter)
            row_id += 1
            dam_name = parse_row2str(file_lines[row_id], line_delimiter)
            row_id += 1
            dam_idx_ji = list(map(int, parse_row2str(file_lines[row_id], line_delimiter).split()))
            row_id += 1
            dam_plant_n = int(parse_row2str(file_lines[row_id], line_delimiter))
            row_id += 1
            dam_cell_lake_code = int(parse_row2str(file_lines[row_id], line_delimiter))
            row_id += 1
            dam_volume_max = float(parse_row2str(file_lines[row_id], line_delimiter))
            row_id += 1
            dam_volume_init = float(parse_row2str(file_lines[row_id], line_delimiter))
            row_id += 1
            dam_discharge_max = float(parse_row2str(file_lines[row_id], line_delimiter))
            row_id += 1
            dam_level_max = float(parse_row2str(file_lines[row_id], line_delimiter))
            row_id += 1
            dam_h_max = float(parse_row2str(file_lines[row_id], line_delimiter))
            row_id += 1
            dam_lin_coeff = float(parse_row2str(file_lines[row_id], line_delimiter))
            row_id += 1
            dam_storage_curve = parse_row2str(file_lines[row_id], line_delimiter)

            for plant_id in range(0, int(dam_plant_n)):
                row_id += 1
                plant_name = parse_row2str(file_lines[row_id], line_delimiter)
                row_id += 1
                plant_idx_ji = list(map(int, parse_row2str(file_lines[row_id], line_delimiter).split()))
                row_id += 1
                plant_tc = int(parse_row2str(file_lines[row_id], line_delimiter))
                row_id += 1
                plant_discharge_max = float(parse_row2str(file_lines[row_id], line_delimiter))
                row_id += 1
                plant_discharge_flag = int(parse_row2str(file_lines[row_id], line_delimiter))

                # define dam and plant tag(s)
                if plant_name != '':
                    dam_key = ':'.join([dam_name, plant_name])
                else:
                    plant_name = 'plant_{:}'.format(plant_id)
                    dam_key = ':'.join([dam_name, plant_name])

                if point_frame is None:
                    point_frame = OrderedDict()
                if dam_key not in list(point_frame.keys()):
                    point_frame[dam_key] = {}
                    point_frame[dam_key]['dam_name'] = dam_name
                    point_frame[dam_key]['dam_idx_ji'] = dam_idx_ji
                    point_frame[dam_key]['dam_plant_n'] = dam_plant_n
                    point_frame[dam_key]['dam_lake_code'] = dam_cell_lake_code
                    point_frame[dam_key]['dam_volume_max'] = dam_volume_max
                    point_frame[dam_key]['dam_volume_init'] = dam_volume_init
                    point_frame[dam_key]['dam_discharge_max'] = dam_discharge_max
                    point_frame[dam_key]['dam_level_max'] = dam_level_max
                    point_frame[dam_key]['dam_h_max'] = dam_h_max
                    point_frame[dam_key]['dam_lin_coeff'] = dam_lin_coeff
                    point_frame[dam_key]['dam_storage_curve'] = dam_storage_curve

                    point_frame[dam_key]['plant_name'] = plant_name
                    point_frame[dam_key]['plant_idx_ji'] = plant_idx_ji
                    point_frame[dam_key]['plant_tc'] = plant_tc
                    point_frame[dam_key]['plant_discharge_max'] = plant_discharge_max
                    point_frame[dam_key]['plant_discharge_flag'] = plant_discharge_flag
                else:
                    raise IOError('Key value must be different for adding it in the dam object')

    else:
        warnings.warn(f'File info "{file_name}" for dam(s) was found; dam(s) are equal to zero. Datasets is None')
        point_frame = None
        dam_n, plant_n = 0, 0

    if point_frame is not None:
        if line_reorder:
            point_frame_reorder, point_frame_root = {}, []
            for point_key, point_fields in point_frame.items():

                point_name_root, point_name_other = point_key.split(':')

                if point_name_root not in point_frame_root:
                    point_frame_reorder[point_key] = point_fields
                    point_frame_root.append(point_name_root)
                else:
                    point_idx_root = point_frame_root.index(point_name_root)

                    point_key_root = list(point_frame_reorder.keys())[point_idx_root]
                    point_values_root = point_frame_reorder[point_key_root]

                    point_key_tmp, point_other_tmp = point_key_root.split(':')
                    point_other_joined = '_'.join([point_other_tmp, point_name_other])
                    point_key_joined = ':'.join([point_key_tmp, point_other_joined])

                    point_value_joined = {}
                    for point_tag, point_data in point_fields.items():
                        if point_tag in list(point_values_root.keys()):

                            root_data = point_values_root[point_tag]

                            if isinstance(root_data, float):
                                tmp_data = [root_data, point_data]
                            elif isinstance(root_data, int):
                                tmp_data = [root_data, point_data]
                            elif isinstance(root_data, str):
                                tmp_data = [root_data, point_data]
                            elif isinstance(root_data, list):
                                tmp_data = [root_data, point_data]
                            else:
                                warnings.warn(f'Type of key "{point_tag}" is not implemented yet for dam merged object')

                            point_value_joined[point_tag] = tmp_data
                        else:
                            point_value_joined[point_tag] = point_data

                    point_frame_reorder[point_key_joined] = point_value_joined
                    point_frame_reorder.pop(point_key_root)

            point_frame = deepcopy(point_frame_reorder)

        # convert to dataframe
        index_df, data_df = list(point_frame.keys()), point_frame.values()
        point_dframe = pd.DataFrame(data_df, index=index_df)

    else:
        point_dframe = None

    point_dims = {'dam': dam_n, 'plant': plant_n}

    return point_dframe, point_dims
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# method to read point data intake(s)
def get_file_point_intake(file_name: str, line_delimiter: str = '#') -> (dict, dict):

    file_handle = open(file_name, 'r')
    file_lines = file_handle.readlines()
    file_handle.close()

    row_id = 0
    catch_n = int(file_lines[row_id].split(line_delimiter)[0])
    row_id += 1
    release_n = int(file_lines[row_id].split(line_delimiter)[0])

    if release_n > 0:

        point_frame = OrderedDict()
        for release_id in range(0, release_n):
            row_id += 1
            _ = parse_row2str(file_lines[row_id], line_delimiter)
            row_id += 1
            release_name = parse_row2str(file_lines[row_id], line_delimiter)
            row_id += 1
            release_idx_ji = list(map(int, parse_row2str(file_lines[row_id], line_delimiter).split()))
            row_id += 1
            release_catch_n = int(parse_row2str(file_lines[row_id], line_delimiter))

            for catch_id in range(0, int(release_catch_n)):
                row_id += 1
                catch_name = parse_row2str(file_lines[row_id], line_delimiter)
                row_id += 1
                catch_tc = int(parse_row2str(file_lines[row_id], line_delimiter))
                row_id += 1
                catch_idx_ji = list(map(int, parse_row2str(file_lines[row_id], line_delimiter).split()))
                row_id += 1
                catch_discharge_max = float(parse_row2str(file_lines[row_id], line_delimiter))
                row_id += 1
                catch_discharge_min = float(parse_row2str(file_lines[row_id], line_delimiter))
                row_id += 1
                catch_discharge_weight = float(parse_row2str(file_lines[row_id], line_delimiter))

                release_key = ':'.join([release_name, catch_name])

                point_frame[release_key] = {}
                point_frame[release_key]['release_name'] = release_name
                point_frame[release_key]['release_idx_ji'] = release_idx_ji
                point_frame[release_key]['release_catch_n'] = release_catch_n
                point_frame[release_key]['catch_name'] = catch_name
                point_frame[release_key]['catch_idx_ji'] = catch_idx_ji
                point_frame[release_key]['catch_tc'] = catch_tc
                point_frame[release_key]['catch_discharge_max'] = catch_discharge_max
                point_frame[release_key]['catch_discharge_min'] = catch_discharge_min
                point_frame[release_key]['catch_discharge_weight'] = catch_discharge_weight

        # convert to dataframe
        index_df, data_df = list(point_frame.keys()), point_frame.values()
        point_dframe = pd.DataFrame(data_df, index=index_df)

    else:
        warnings.warn(f'File info "{file_name}" for intake(s) was found; intake(s) are equal to zero. Datasets is None')
        point_dframe = None
        catch_n, release_n = 0, 0

    point_dims = {'catch': catch_n, 'release': release_n}

    return point_dframe, point_dims
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# method to read point data joint(s)
def get_file_point_joint(file_name: str, line_delimiter: str = '#') -> (dict, None):

    file_handle = open(file_name, 'r')
    file_lines = file_handle.readlines()
    file_handle.close()

    row_id = 0
    joint_n = int(file_lines[row_id].split(line_delimiter)[0])

    if joint_n > 0:
        raise NotImplemented(' File info for joint was found; function to read joints is not implemented')
    else:
        warnings.warn(f'File info "{file_name}" for joint(s) was found; joint(s) are equal to zero. Datasets is None')
        point_dframe = None

    point_dims = {'joint': joint_n}

    return point_dframe, point_dims

# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# method to read point data lake(s)
def get_file_point_lake(file_name: str, line_delimiter: str = '#'):

    file_handle = open(file_name, 'r')
    file_lines = file_handle.readlines()
    file_handle.close()

    row_id = 0
    lake_n = int(file_lines[row_id].split(line_delimiter)[0])

    if lake_n > 0:

        point_frame = OrderedDict()
        for lake_id in range(0, lake_n):
            row_id += 1
            _ = parse_row2str(file_lines[row_id], line_delimiter)
            row_id += 1
            lake_name = parse_row2str(file_lines[row_id], line_delimiter)
            row_id += 1
            lake_idx_ji = list(map(int, parse_row2str(file_lines[row_id], line_delimiter).split()))
            row_id += 1
            lake_cell_code = int(parse_row2str(file_lines[row_id], line_delimiter))
            row_id += 1
            lake_volume_min = float(parse_row2str(file_lines[row_id], line_delimiter))
            row_id += 1
            lake_volume_init = float(parse_row2str(file_lines[row_id], line_delimiter))
            row_id += 1
            lake_const_draining = float(parse_row2str(file_lines[row_id], line_delimiter))

            lake_key = lake_name

            point_frame[lake_key] = {}
            point_frame[lake_key]['lake_name'] = lake_name
            point_frame[lake_key]['lake_idx_ji'] = lake_idx_ji
            point_frame[lake_key]['lake_cell_code'] = lake_cell_code
            point_frame[lake_key]['lake_volume_min'] = lake_volume_min
            point_frame[lake_key]['lake_volume_init'] = lake_volume_init
            point_frame[lake_key]['lake_constant_draining'] = lake_const_draining

        # convert to dataframe
        index_df, data_df = list(point_frame.keys()), point_frame.values()
        point_dframe = pd.DataFrame(data_df, index=index_df)

    else:
        warnings.warn('File info "' + file_name + '" for lake(s) was found; lake(s) are equal to zero. Datasets is None')
        point_dframe = None
        lake_n = 0

    point_dims = {'lake': lake_n}

    return point_dframe, point_dims
# -------------------------------------------------------------------------------------
