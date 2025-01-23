"""
Library Features:

Name:          lib_utils_string
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20250116'
Version:       '4.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import re
import numpy as np

from datetime import datetime

from apps.generic_toolkit.lib_default_args import logger_name, logger_arrow

# logging
logger_stream = logging.getLogger(logger_name)
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to separate number and characters in a string
def separate_number_chars(s: str) -> list:
    res = re.split('([-+]?\d+\.\d+)|([-+]?\d+)', s.strip())
    res_f = [r.strip() for r in res if r is not None and r.strip() != '']
    return res_f
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to convert bytes string to character string
def convert_bytes2string(obj_bytes: bytes) -> str:
    if isinstance(obj_bytes, bytes):
        obj_string = obj_bytes.decode()
        obj_string = obj_string.rstrip("\n")
    else:
        obj_string = obj_bytes
    return obj_string
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to remove string part(s)
def remove_string_parts(string_raw: str, deny_parts: list = None) -> str:
    if deny_parts is not None:
        for deny_part in deny_parts:
            if deny_part in string_raw:
                string_raw = string_raw.replace(deny_part, '')
        string_raw = string_raw.replace('{}', '')
        string_raw = string_raw.replace('//', '/')
    return string_raw
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to parse complex row to string
def parse_row2string(row_obj: str, row_delimiter: str ='#'):

    # check if line starts with {number}
    if row_obj.count(row_delimiter) > 1:
        pattern = r'[0-9]'
        row_obj = re.sub(pattern, '', row_obj)

    row_string = row_obj.split(row_delimiter)[0]

    # check delimiter character (in intake file info there are both '#' and '%')
    if ('#' not in row_obj) and ('%' in row_string):
        row_string = row_obj.split('%')[0]

    row_string = row_string.strip()

    return row_string
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to convert list to string (given the delimiter)
def convert_list2string(list_data: list, list_delimiter: str = ',') -> str:
    string_data = list_delimiter.join(str(elem) for elem in list_data)
    return string_data
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to replace string with multiple values
def replace_string(string_text: str, string_replace: dict) -> str:

    # use these three lines to do the replacement
    obj_replace = dict((re.escape(k), v) for k, v in string_replace.items())
    obj_pattern = re.compile("|".join(obj_replace.keys()))
    string_text = obj_pattern.sub(lambda m: obj_replace[re.escape(m.group(0))], string_text)

    return string_text
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to add format(s) string (path or filename)
def fill_tags2string(string_raw, tags_format=None, tags_filling=None, tags_template='[TMPL_TAG_{:}]'):

    apply_tags = False
    if string_raw is not None:
        for tag in list(tags_format.keys()):
            if tag in string_raw:
                apply_tags = True
                break

    if apply_tags:

        string_filled = None
        tag_dictionary = {}
        for tag_id, (tag_key, tag_value) in enumerate(tags_format.items()):
            tag_key_tmp = '{' + tag_key + '}'
            if tag_value is not None:

                tag_id = tags_template.format(tag_id)
                tag_dictionary[tag_id] = {'key': None, 'type': None}

                if tag_key_tmp in string_raw:
                    tag_dictionary[tag_id] = {'key': tag_key, 'type': tag_value}
                    string_filled = string_raw.replace(tag_key_tmp, tag_id)
                    string_raw = string_filled
                else:
                    tag_dictionary[tag_id] = {'key': tag_key, 'type': None}

        dim_max = 1
        for tags_filling_values_tmp in tags_filling.values():
            if isinstance(tags_filling_values_tmp, list):
                dim_tmp = tags_filling_values_tmp.__len__()
                if dim_tmp > dim_max:
                    dim_max = dim_tmp

        string_filled_list = [string_filled] * dim_max

        string_filled_def, string_list_key, string_list_value, string_list_type = [], [], [], []
        for string_id, string_filled_step in enumerate(string_filled_list):

            for tag_dict_template, tag_dict_fields in tag_dictionary.items():
                tag_dict_key = tag_dict_fields['key']
                tag_dict_type = tag_dict_fields['type']

                if string_filled_step is not None and tag_dict_template in string_filled_step:
                    if tag_dict_type is not None:

                        if tag_dict_key in list(tags_filling.keys()):

                            value_filling_obj = tags_filling[tag_dict_key]

                            if isinstance(value_filling_obj, list):
                                value_filling = value_filling_obj[string_id]
                            else:
                                value_filling = value_filling_obj

                            string_filled_step = string_filled_step.replace(tag_dict_template, tag_dict_key)

                            if isinstance(value_filling, datetime):
                                tag_dict_value = value_filling.strftime(str(tag_dict_type))
                            elif isinstance(value_filling, np.datetime64):
                                logger_stream.error(logger_arrow.error + 'DateTime64 format is not expected')
                                raise ValueError('DateTime64 is non supported by the method. Use datetime instead')
                            elif isinstance(value_filling, float):
                                tag_dict_value = '{:}'.format(value_filling)
                            elif isinstance(value_filling, int):
                                tag_dict_value = '{:}'.format(value_filling)
                            else:
                                tag_dict_value = value_filling

                            if tag_dict_value is None:
                                tag_dict_undef = '{' + str(tag_dict_key) + '}'
                                string_filled_step = string_filled_step.replace(tag_dict_key, tag_dict_undef)

                            if tag_dict_value:
                                string_filled_step = string_filled_step.replace(tag_dict_key, tag_dict_value)
                                string_list_key.append(tag_dict_key)
                                string_list_value.append(tag_dict_value)
                                string_list_type.append(tag_dict_type)
                            else:
                                logger_stream.warning(logger_arrow.warning + 'Variable "' + tag_dict_key + '" for "' +
                                                   string_filled_step +
                                                   '" is not correctly filled; the value is set to NoneType')

            string_filled_def.append(string_filled_step)

        if dim_max == 1:
            if string_filled_def[0]:
                string_filled_out = string_filled_def[0].replace('//', '/')
            else:
                string_filled_out = []
        else:
            string_filled_out = []
            for string_filled_tmp in string_filled_def:
                if string_filled_tmp:
                    string_filled_out.append(string_filled_tmp.replace('//', '/'))

        if "'" in string_filled_out:
            string_filled_out = string_filled_out.replace("'", "")
        if '//' in string_filled_out:
            string_filled_out = string_filled_out.replace('// ', '/')

        return string_filled_out, string_list_key, string_list_value, string_list_type
    else:
        string_list_key, string_list_value, string_list_type = [], [], []
        return string_raw, string_list_key, string_list_value, string_list_type
# ----------------------------------------------------------------------------------------------------------------------
