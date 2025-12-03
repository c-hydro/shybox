"""
Library Features:

Name:          lib_utils_dict
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20250102'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import re
import functools
import numpy as np

from functools import reduce
from collections.abc import MutableMapping ##patch
from operator import getitem

from shybox.default.lib_default_args import logger_name, logger_arrow

# logging
logger_stream = logging.getLogger(logger_name)

# debugging
# import matplotlib.pylab as plt
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to get dict value (using keys list)
def get_dict_value(dictionary: dict, keys: list, default=None):
    if not isinstance(keys, list):
        keys = [keys]
    dictionary = reduce(lambda d, key: d.get(key, default) if isinstance(d, dict) else default, keys, dictionary)
    return dictionary
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to add dictionary key
def add_dict_key(dictionary: dict, keys: list, value: (str, int, float)):
    if len(keys) == 1:
        dictionary[keys[0]] = value
    else:
        add_dict_key(dictionary.setdefault(keys[0], {}), keys[1:], value)
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to flat dictionary keys
def flat_dict_key(data: dict, parent_key: str = '',
                  separator_key: str = ":", obj_dict: dict = {}, separator_value: str = ',') -> dict:

    assert separator_key != separator_value, 'Separator key and separator value must be not the same'

    for k, v in data.items():
        key = parent_key + separator_key + k if parent_key else k
        if isinstance(v, dict):
            if v:
                flat_dict_key(v, key, obj_dict=obj_dict)
            else:
                obj_dict[key] = v
        elif isinstance(v, list):
            tmp = [str(i) for i in v]
            obj_dict[key] = separator_value.join(tmp)
        else:
            obj_dict[key] = v
    return obj_dict
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to check if two dictionaries have the same keys
def check_keys_of_dict(d1, d2, name1='lut', name2='format'):
    if d1.keys() == d2.keys():
        return True
    else:

        list1, list2 = list(set(d1.keys())), list(set(d2.keys()))

        one_not_two = [x for x in list1 if x not in list2]
        two_not_one = [x for x in list2 if x not in list1]

        for key in one_not_two:
            logger_stream.error(logger_arrow.error + 'Key "' + key + '" is not in the "' + name2 + '" dictionary')
        for key in two_not_one:
            logger_stream.error(logger_arrow.error + 'Key "' + key + '" is not in the "' + name1 + '" dictionary')
        raise ValueError('The two dictionaries have different keys. Add the keys to the dictionaries')

# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to swap keys and values in a dictionary
def swap_keys_values(d):
    return {v: k for k, v in d.items()}
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to create a dictionary from a list
def filter_dict_by_keys(dict_obj_in: dict, key_list: list = None,
                        default_value: (int, float, str, None) = None) -> dict:

    if key_list is not None:
        dict_obj_out = {}
        for key_step in key_list:
            if key_step in list(dict_obj_in.keys()):

                tmp_value = dict_obj_in[key_step]
                if isinstance(tmp_value, str):
                    to_remove = re.compile(r"[']")
                    tmp_value = to_remove.sub('', tmp_value)

                dict_obj_out[key_step] = tmp_value
            else:
                logger_stream.warning(
                    logger_arrow.warning + 'Variable "' + str(key_step) +
                    '" is not in the dictionary. Variable is set to default value "' + str(default_value) + '"')
                dict_obj_out[key_step] = default_value
    else:
        dict_obj_out = dict_obj_in
    return dict_obj_out
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to create dictionary from keys and values list
def create_dict_from_list(list_keys: list = None, list_values: (list, str, int, float) = None,
                          default_key: str = 'key_{:}') -> dict:

    if list_values is None:
        list_values = [None] * len(list_keys)
    if list_keys is None:
        list_keys = [default_key.format(i + 1) for i in range(len(list_values))]

    if not isinstance(list_keys, list):
        list_keys = [list_keys]
    if not isinstance(list_values, list):
        list_values = [list_values] * len(list_keys)

    if len(list_keys) != len(list_values):
        logger_stream.error(logger_arrow.error + ' Keys and values have different length')
        raise ValueError('Keys and values must have the same length')

    dict_obj = dict(map(lambda i, j: (i, j), list_keys, list_values))
    return dict_obj
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to get dictionary value by key
def get_dict_value_by_key(dct, key, init=None):

    for key_step, value_step in dct.items():

        if key_step == key:
            return value_step, key_step
        elif isinstance(value_step, dict):
            obj_fx = get_dict_value_by_key(value_step, key, init=True)

            if obj_fx is not None:
                value_fx, key_fx = obj_fx[0], obj_fx[1]
                if key_fx == key:
                    return value_fx, key_fx

        else:
            pass

# ----------------------------------------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to get all dictionary item(s)
def get_dict_all_items(dictionary):
    for key, value in dictionary.items():
        if type(value) is dict:
            yield from get_dict_all_items(value)
        else:
            yield (key, value)
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to delete dict keys
def delete_dict_keys(dictionary, keys):
    keys_set = set(keys)  # Just an optimization for the "if key in keys" lookup.

    modified_dict = {}
    for key, value in dictionary.items():
        if key not in keys_set:
            if isinstance(value, MutableMapping):
                modified_dict[key] = delete_dict_keys(value, keys_set)
            else:
                modified_dict[key] = value  # or copy.deepcopy(value) if a copy is desired for non-dicts.
    return modified_dict
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to get nested value
def get_dict_nested_value(input_dict, nested_key):
    internal_dict_value = input_dict
    for k in nested_key:
        internal_dict_value = internal_dict_value.get(k, None)
        if internal_dict_value is None:
            return None
    return internal_dict_value
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to build dict tree
def build_dict_tree(tree_list):
    if tree_list:
        return {tree_list[0]: build_dict_tree(tree_list[1:])}
    return {}
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to get value from dictionary (using a list)
def lookup_dict_keys(dataDict, mapList):
    return functools.reduce(lambda d, k: d[k], mapList, dataDict)
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to set dictionary values
def set_dict_values(dataDict, mapList, val):
    """Set item in nested dictionary"""
    functools.reduce(getitem, mapList[:-1], dataDict)[mapList[-1]] = val
    return dataDict
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to get recursively dictionary value
def get_dict_value(d, key, value=[]):

    for k, v in iter(d.items()):
        if isinstance(v, dict):
            if k == key:
                for kk, vv in iter(v.items()):
                    temp = [kk, vv]
                    value.append(temp)
            else:
                vf = get_dict_value(v, key, value)
                if isinstance(vf, list):
                    if vf:
                        vf_end = vf[0]
                    else:
                        vf_end = None
                elif isinstance(vf, np.ndarray):
                    vf_end = vf.tolist()
                else:
                    vf_end = vf
                if (isinstance(value, list)) and (vf_end not in value):

                    if not isinstance(vf_end, bool):
                        if vf_end:
                            if isinstance(value, list):
                                value.append(vf_end)
                            elif isinstance(value, str):
                                value = [value, vf_end]
                        else:
                            pass
                    else:
                        value.append(vf_end)
                else:
                    pass
        else:
            if k == key:
                if isinstance(v, np.ndarray):
                    value = v
                else:
                    value = v
    return value
# -------------------------------------------------------------------------------------
