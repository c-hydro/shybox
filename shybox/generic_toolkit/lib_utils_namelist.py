"""
Library Features:

Name:          lib_utils_namelist
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20241202'
Version:       '4.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import numpy as np
import pandas as pd
from typing import Tuple, List

from shybox.generic_toolkit.lib_default_namelist import (
    type_namelist_hmc_316, structure_namelist_hmc_316, type_namelist_hmc_320, structure_namelist_hmc_320,
    type_namelist_s3m_533, structure_namelist_s3m_533)
from shybox.generic_toolkit.lib_default_args import time_format_datasets

from shybox.generic_toolkit.lib_utils_string import convert_list2string
from shybox.generic_toolkit.lib_utils_fortran import define_var_format
from shybox.generic_toolkit.lib_utils_time import is_date
from shybox.generic_toolkit.lib_default_args import logger_name, logger_arrow

# logging
logger_stream = logging.getLogger(logger_name)
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to select namelist type
def select_namelist_type_hmc(namelist_type: str = 'hmc', namelist_version: str = '3.1.6') -> (dict, dict):
    # select namelist type and structure
    if namelist_type == 'hmc' and namelist_version == '3.1.6':
        type_namelist = type_namelist_hmc_316
        structure_namelist = structure_namelist_hmc_316
    elif namelist_type == 'hmc' and namelist_version == '3.2.0':
        type_namelist = type_namelist_hmc_320
        structure_namelist = structure_namelist_hmc_320
    else:
        logger_stream.error(logger_arrow.error + 'Namelist type "' + namelist_type + '" is not allowed')
        raise NotImplementedError('Namelist type not implemented yet')
    return type_namelist, structure_namelist
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to select namelist type
def select_namelist_type_s3m(namelist_type: str = 's3m', namelist_version: str = '5.3.3') -> (dict, dict):

    if namelist_type == 's3m' and namelist_version == '5.3.3':
        type_namelist = type_namelist_s3m_533
        structure_namelist = structure_namelist_s3m_533
    else:
        logger_stream.error(logger_arrow.error + 'Namelist type "' + namelist_type + '" is not allowed')
        raise NotImplementedError('Namelist type not implemented yet')
    return type_namelist, structure_namelist
# ----------------------------------------------------------------------------------------------------------------------



# ----------------------------------------------------------------------------------------------------------------------
# method to write namelist file
def write_namelist_file(obj_namelist, structure_namelist, line_indent=4 * ' '):

    # write namelist file
    try:
        for group_name, group_vars in structure_namelist.items():
            if isinstance(group_vars, list):
                for variables in group_vars:
                    write_namelist_group(obj_namelist, group_name, variables, line_indent)
            else:
                write_namelist_group(obj_namelist, group_name, group_vars, line_indent)
    finally:
        obj_namelist.close()
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to write group namelist
def write_namelist_group(file_obj, group_name, variables, line_indent=4 * ' '):

    # Write group in namelist file
    print('&{0}'.format(group_name), file=file_obj)

    # Cycle(s) over variable(s) and value(s)
    for variable_name, variable_value in sorted(variables.items()):
        if isinstance(variable_value, list):
            if isinstance(variable_value[0], (int, float)):
                # Reduce number precision (if needed)
                variable_list = define_var_format(np.asarray(variable_value))
                line = write_namelist_line(variable_name, variable_list)
                line = line_indent + line
                print('{0}'.format(line), file=file_obj)
            elif isinstance(variable_value[0], str):
                line = write_namelist_line(variable_name, variable_value)
                line = line_indent + line
                print('{0}'.format(line), file=file_obj)
            else:
                logger_stream.error(logger_arrow.error + 'Variable namelist type not allowed')
        elif isinstance(variable_value, str):
            line = write_namelist_line(variable_name, variable_value)
            line = line_indent + line
            print('{0}'.format(line), file=file_obj)
        else:
            line = write_namelist_line(variable_name, variable_value)
            line = line_indent + line
            print('{0}'.format(line), file=file_obj)

    print('/', file=file_obj)

# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to write line namelist
def write_namelist_line(variable_name, variable_value):

    # Check variable value type
    if isinstance(variable_value, str):
        variable_value_str = variable_value
        variable_value_str = '"' + variable_value_str + '"'
    elif isinstance(variable_value, int):
        variable_value_str = str(int(variable_value))
    elif isinstance(variable_value, float):
        variable_value_str = str(float(variable_value))
    elif isinstance(variable_value, list):
        variable_value_str = convert_list2string(variable_value, ',')
    else:
        variable_value_str = str(variable_value)

    # Line definition in Fortran style
    line = str(variable_name) + ' = ' + variable_value_str
    return line

# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to filter comments from a file stream
def filter_namelist_settings(file_stream: str, file_comment: str = '!') -> Tuple[List[str], List[str]]:
    """
    Filter comments from a file stream.
    :param file_stream:
    :param file_comment:
    :return: settings_lines, comments_line
    """
    settings_lines, comments_lines = [], []
    for line in file_stream.split('\n'):
        if line.strip().startswith(file_comment):
            comments_lines.append(line)
        else:
            settings_lines.append(line)

    return settings_lines, comments_lines
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to read namelist group
def read_namelist_group(settings_blocks: List[str]) -> dict:
    """
    Group settings.
    :param settings_lines:
    :param group_re:
    :return: group_blocks
    """

    settings_groups = {}
    for settings_block in settings_blocks:
        settings_lines_raw = settings_block.split('\n')

        settings_block_name = settings_lines_raw.pop(0).strip()
        settings_groups[settings_block_name] = {}

        settings_lines_filtered = []
        for line in settings_lines_raw:
            # cleanup string
            line = line.strip()
            if line == "":
                continue
            if line.startswith('!'):
                continue

            try:
                k, v = line.split('=')
                settings_lines_filtered.append(line)
            except ValueError:
                # no = in current line, try to append to previous line
                if settings_lines_filtered[-1].endswith(','):
                    settings_lines_filtered[-1] += line
                else:
                    raise

        for line in settings_lines_filtered:
            # commas at the end of lines seem to be optional
            if line.endswith(','):
                line = line[:-1]

            # inline comments are allowed, but we remove them for now
            if "!" in line:
                line = line.split("!")[0].strip()

            k, v = line.split('=')
            variable_name = k.strip()
            variable_value = v.strip()

            settings_groups[settings_block_name][variable_name] = variable_value

    return settings_groups
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to parse settings
def parse_namelist_settings(settings_groups: dict) -> dict:
    """
    Parse settings.
    :param settings_groups:
    :return: parsed_settings
    """
    parsed_settings = {}
    for group_name, group_dict in settings_groups.items():
        parsed_settings[group_name] = {}
        for k, v in group_dict.items():

            v = v.split(',')

            if len(v) == 1:
                v = v[0].strip()

                date_check = is_date(v, date_format=time_format_datasets)

                if not date_check:

                    if v.startswith("'") and v.endswith("'"):
                        parsed_settings[group_name][k] = v[1:-1]
                    elif v.startswith('"') and v.endswith('"'):
                        parsed_settings[group_name][k] = v[1:-1]
                    elif v.startswith("'"):
                        parsed_settings[group_name][k] = v[1:]
                    elif v.endswith("'"):
                        parsed_settings[group_name][k] = v[:-1]
                    elif v.lower() == '.true.':
                        parsed_settings[group_name][k] = True
                    elif v.lower() == '.false.':
                        parsed_settings[group_name][k] = False
                    else:
                        try:
                            parsed_settings[group_name][k] = int(v)
                        except ValueError:
                            try:
                                parsed_settings[group_name][k] = float(v)
                            except ValueError:
                                parsed_settings[group_name][k] = v
                else:
                    parsed_settings[group_name][k] = pd.Timestamp(v)
            else:
                try:
                    v_list = [int(i) for i in v]
                    parsed_settings[group_name][k] = v_list
                except ValueError:
                    try:
                        v_list = [float(i) for i in v]
                        parsed_settings[group_name][k] = v_list
                    except ValueError:
                        v_list = []
                        for n, i in enumerate(v):
                            if i.startswith("'") and i.endswith("'"):
                                v_list.append(i[1:-1])
                            elif i.startswith('"') and i.endswith('"'):
                                v_list.append(i[1:-1])
                            elif i.startswith("'"):
                                v_list.append(i[1:])
                            elif i.endswith("'"):
                                v_list.append(i[:-1])
                            elif i.lower() == '.true.':
                                v_list.append(True)
                            elif i.lower() == '.false.':
                                v_list.append(False)
                            else:
                                v_list.append(i)
                        parsed_settings[group_name][k] = v_list

    return parsed_settings
# ----------------------------------------------------------------------------------------------------------------------
