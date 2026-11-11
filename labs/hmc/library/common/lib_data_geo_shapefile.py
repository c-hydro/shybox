"""
Class Features

Name:          lib_data_geo_shapefile
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20210113'
Version:       '1.0.0'
"""

#######################################################################################
# Libraries
import os
import pandas as pd
import geopandas as gpd

# Debug
# import matplotlib.pylab as plt
#######################################################################################


# -------------------------------------------------------------------------------------
# Method to validate section tag
def create_group_file_section(file_name, file_ext_in=None, file_ext_out=None):
    if file_ext_in is None:
        file_ext_in = 'shp'
    if file_ext_out is None:
        file_ext_out = ['shp', 'cpg', 'dbf', 'prj', 'shx']

    file_root = file_name.split(file_ext_in)[0]

    file_list = []
    for file_ext_step in file_ext_out:
        file_step = file_root + file_ext_step
        file_list.append(file_step)

    return file_list

# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to validate section tag
def validate_tag_section(section_tag=None, tag_delimiter=':', section_name=None, basin_name=None):

    if (basin_name is None) or (section_name is None):
        if section_tag is not None:
            basin_name, section_name = section_tag.split(tag_delimiter)
        else:
            print(' ERROR ===> Basin and section name are not defined')
            raise IOError('Check your settings')

    elif (basin_name is not None) or (section_name is not None):
        if section_tag is not None:
            basin_name_tmp, section_name_tmp = section_tag.split(tag_delimiter)

            if (basin_name != basin_name_tmp) or (section_name != section_name_tmp):
                print(' WARNING ===> Basin and section name are defined by the json and the user input')
                print(' WARNING ===> String are different. The user input selection will be considered.')
                basin_name = basin_name_tmp
                section_name = section_name_tmp
    else:
        print(' ERROR ===> Basin and section name are not correct defined')
        raise NotImplementedError('Case not implemented yet')

    return basin_name, section_name
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to search the tag section
def search_tag_section(section_list, basin_name, section_name, string_delimiter=':'):

    section_name, basin_name = section_name.lower(), basin_name.lower()
    section_tag = string_delimiter.join([basin_name, section_name])

    section_id = None
    for section_n, section_step in enumerate(section_list):
        section_step = section_step.lower()
        if section_tag == section_step:
            section_id = section_n
            break

    if section_id is None:
        print(' ERROR ===> Section ID is defined by NoneType')
        raise IOError('Section "' + section_tag + '" is not defined in the section list. Check you shapefile.')

    return section_id

# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to create tag section
def create_tag_section(section_df, tag_ordered=True,
                       tag_column_section='section_name', tag_column_basin='basin_name', tag_delimiter=':'):

    section_name_list = section_df[tag_column_section].values
    basin_name_list = section_df[tag_column_basin].values

    section_tag_list = []
    section_tag_dict = {}
    for basin_name_step, section_name_step in zip(basin_name_list, section_name_list):

        if basin_name_step not in list(section_tag_dict.keys()):
            section_tag_dict[basin_name_step] = {}
            section_tag_dict[basin_name_step] = [section_name_step]
        else:
            section_name_tmp = section_tag_dict[basin_name_step]
            section_name_tmp.append(section_name_step)
            section_tag_dict[basin_name_step] = section_name_tmp

        section_tag_list.append(tag_delimiter.join([basin_name_step, section_name_step]))

    if tag_ordered:
        section_tag_list = sorted(section_tag_list)

    return section_tag_list, section_tag_dict
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to find data section
def find_data_section(section_df, section_name=None, basin_name=None,
                      tag_column_section_in='section_name', tag_column_basin_in='basin_name',
                      tag_column_section_out='section_name', tag_column_basin_out='basin_name'):

    section_name_ref = section_name.lower()
    basin_name_ref = basin_name.lower()

    section_name_list = section_df[tag_column_section_in].values
    basin_name_list = section_df[tag_column_basin_in].values

    section_dict_tmp = {tag_column_section_in: section_name_list, tag_column_basin_in: basin_name_list}
    section_df_tmp = pd.DataFrame(data=section_dict_tmp)
    section_df_tmp = section_df_tmp.astype(str).apply(lambda x: x.str.lower())

    point_idx = section_df_tmp[(section_df_tmp[tag_column_section_in] == section_name_ref) &
                                 (section_df_tmp[tag_column_basin_in] == basin_name_ref)].index

    if point_idx.shape[0] == 1:
        point_idx = point_idx[0]
        point_dict = section_df.iloc[point_idx, :].to_dict()

        point_dict[tag_column_section_out] = point_dict.pop(tag_column_section_in)
        point_dict[tag_column_basin_out] = point_dict.pop(tag_column_basin_in)

    elif point_idx.shape[0] == 0:
        print(' ERROR ===> Section name not found')
        raise IOError('Selection is failed due to the name of the section')
    else:
        print(' ERROR ===> Section name not found')
        raise NotImplementedError('Selection is failed due to unknown reason')

    return point_dict
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to read shapefile section(s)
def read_data_section(file_name, columns_name_expected_in=None, columns_name_expected_out=None, columns_name_type=None):

    if columns_name_expected_in is None:
        columns_name_expected_in = [
            'HMC_X', 'HMC_Y', 'LAT', 'LON', 'BASIN', 'SEC_NAME', 'SEC_RS', 'AREA', 'Q_THR1', 'Q_THR2']

    if columns_name_expected_out is None:
        columns_name_expected_out = [
            'hmc_idx_x', 'hmc_idx_y', 'latitude', 'longitude', 'basin_name', 'section_name', 'section_code',
            'section_drained_area', 'section_discharge_thr_alert', 'section_discharge_thr_alarm']

    if columns_name_type is None:
        columns_name_type = ['int', 'int', 'float', 'float',
                             'str', 'str', 'str', 'float', 'float', 'float']

    file_dframe_raw = gpd.read_file(file_name)
    file_rows = file_dframe_raw.shape[0]

    section_obj = {}
    for column_name_in, column_name_out, column_type in zip(columns_name_expected_in,
                                                            columns_name_expected_out, columns_name_type):
        if column_name_in in file_dframe_raw.columns:
            column_data = file_dframe_raw[column_name_in].values.tolist()
        else:
            if column_type == 'int':
                column_data = [-9999] * file_rows
            elif column_type == 'str':
                column_data = [''] * file_rows
            elif column_type == 'float':
                column_data = [-9999.0] * file_rows
            else:
                print(' ERROR ===> Column of shapefile must be "str", "int" or "float"')
                raise NotImplementedError('Case not implemented yet')

        section_obj[column_name_out] = column_data

    section_df = pd.DataFrame(data=section_obj)

    return section_df
# -------------------------------------------------------------------------------------
