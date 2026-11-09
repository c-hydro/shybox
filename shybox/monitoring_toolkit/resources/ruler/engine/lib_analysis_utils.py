"""
Library Features:

Name:          lib_analysis_utils
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20221227'
Version:       '1.0.0'
"""

#######################################################################################
# Libraries
import logging
import subprocess
import math
import re
import numpy as np
import psutil
from psutil._common import bytes2human
from psutil._common import pcputimes
from copy import deepcopy

from .lib_info_args import logger_name

# Logging
log_stream = logging.getLogger(logger_name)
#######################################################################################


# -------------------------------------------------------------------------------------
# Method to get sysctl value
def get_memory_sysctl_value(key):
    command = "sysctl %s" % key
    line = subprocess.check_output(command, shell=True).strip()
    line_sp = line.split()
    return line_sp[2].decode("utf-8")
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to get memory usage by linux command
def get_linux_memory_usage(advices=True):

    # virtual_memory_header = 'total used free shared buff/cache available'
    # swap_memory_header = 'total used free'

    size_name_long = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    size_name_short = ("B", "K", "M", "G", "T", "P", "E", "Z", "Y")

    command = "free -h"
    all_info = subprocess.check_output(command, shell=True).strip()

    virtual_memory_total, virtual_memory_used, virtual_memory_free = None, None, None
    virtual_memory_shared, virtual_memory_filesystem_cache, virtual_memory_available = None, None, None
    swap_memory_total, swap_memory_used, swap_memory_free, swap_memory_swappiness = None, None, None, None
    for line in all_info.decode("utf-8").split("\n"):

        if 'total' in line:
            line_header = line.split()

        if "Mem:" in line:
            line_virtual_memory = line.split()
            virtual_memory_total = line_virtual_memory[1]
            virtual_memory_used = line_virtual_memory[2]
            virtual_memory_free = line_virtual_memory[3]
            virtual_memory_shared = line_virtual_memory[4]
            virtual_memory_filesystem_cache = line_virtual_memory[5]
            virtual_memory_available = line_virtual_memory[6]

        if "Swap:" in line:
            line_swap_memory = line.split()
            swap_memory_total = line_swap_memory[1]
            swap_memory_used = line_swap_memory[2]
            swap_memory_free = line_swap_memory[3]
            swap_memory_swappiness = int(get_memory_sysctl_value('vm.swappiness'))

            if (swap_memory_swappiness <= 10) and (swap_memory_swappiness > 0):
                log_stream.info(' ------> Swappiness value is good [value = "' + str(swap_memory_swappiness) + '"]')
            elif swap_memory_swappiness == 0:
                log_stream.warning(' ===> Swappiness value 0 is dangerous, '
                                   'set it to 5 [value = "' + str(swap_memory_swappiness) + '"]')
            else:
                log_stream.warning(' ===> Swappiness value is to high, '
                                   'set it to a value between 1 and 10 [value = "' + str(swap_memory_swappiness) + '"]')

    virtual_memory_usage = {
        'total': virtual_memory_total, 'used': virtual_memory_used,
        'free': virtual_memory_free, 'shared': virtual_memory_shared,
        'filesystem_buff_cache': virtual_memory_filesystem_cache, 'available': virtual_memory_available
        }
    swap_memory_usage = {'total': swap_memory_total, 'used': swap_memory_used,
                         'free': swap_memory_free, 'swappiness': swap_memory_swappiness}

    for vm_key, vm_value in virtual_memory_usage.items():
        if isinstance(vm_value, str):
            vm_numeric, vm_units = split_size_parts(vm_value)
            if vm_units.__len__() == 2:
                vm_units = vm_units[0]
                if vm_units not in size_name_short:
                    log_stream.error(' ===> Variable "vm_units" not available in default values')
                    raise NotImplemented('Case not implemented yet')
            vm_memory = ''.join([vm_numeric, vm_units])
        elif isinstance(vm_value, int):
            vm_memory = deepcopy(vm_value)
        else:
            log_stream.error(' ===> Variable "vm_value" type is not supported')
            raise NotImplemented('Case not implemented yet')

        virtual_memory_usage[vm_key] = vm_memory

    for sm_key, sm_value in swap_memory_usage.items():
        if isinstance(sm_value, str):
            sm_numeric, sm_units = split_size_parts(sm_value)
            if sm_units.__len__() == 2:
                sm_units = sm_units[0]
                if sm_units not in size_name_short:
                    log_stream.error(' ===> Variable "vm_units" not available in default values')
                    raise NotImplemented('Case not implemented yet')
            sm_memory = ''.join([sm_numeric, sm_units])
        elif isinstance(sm_value, int):
            sm_memory = deepcopy(sm_value)
        else:
            log_stream.error(' ===> Variable "vm_value" type is not supported')
            raise NotImplemented('Case not implemented yet')

        swap_memory_usage[sm_key] = sm_memory

    return virtual_memory_usage, swap_memory_usage
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to split size parts (value and units)
def split_size_parts(size_string):

    if size_string is not None:

        # remove scientific notation from string
        if 'e-' in size_string or 'e+' in size_string:
            match_number = re.compile('-?\ *[0-9]+\.?[0-9]*(?:[Ee]\ *-?\ *[0-9]+)?')
            num_str_scientific_notation = str(float(re.findall(match_number, size_string)[0]))
            num_str_float = np.format_float_positional(float(num_str_scientific_notation), trim='-')

            size_string = size_string.replace(num_str_scientific_notation, num_str_float)

        size_alpha_list, size_numeric_list = None, None
        for size_elem in size_string:

            if size_elem.isalpha():
                if size_alpha_list is None:
                    size_alpha_list = []
                size_alpha_list.append(size_elem)
            elif size_elem.isnumeric() or size_elem == '.':
                if size_numeric_list is None:
                    size_numeric_list = []
                size_numeric_list.append(size_elem)
            elif size_elem.isnumeric() or size_elem == ',':
                if size_numeric_list is None:
                    size_numeric_list = []
                size_elem = size_elem.replace(',', '.')
                size_numeric_list.append(size_elem)
            else:
                log_stream.error(' ===> Object "size_string" must be defined only by character or numbers')
                raise NotImplemented('Case not implemented yet')

        if size_numeric_list is None:
            log_stream.error(' ===> Object "size_string" must be defined by a part that defined the value')
            raise RuntimeError('Obj "size_string" must be defined by list of two elements (value and units)')
        if size_alpha_list is None:
            log_stream.error(' ===> Object "size_string" must be defined by a part that defined the units')
            raise RuntimeError('Obj "size_string" must be defined by list of two elements (value and units)')

        # join elements to create a string
        size_numeric_string = ''.join(size_numeric_list)
        size_alpha_string = ''.join(size_alpha_list).upper()
        # create a list to save value and units
        size_list = [size_numeric_string, size_alpha_string]

    else:
        log_stream.error(' ===> Object "size_string" is defined by NoneType')
        raise RuntimeError('Obj "size_string" must be defined by list of two elements (value and units)')

    return size_list
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# method to check whitespace in size string
def check_size_whitespaces(size_string):
    return any(size_elem.isspace() for size_elem in size_string)
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# convert field from bytes to human
def convert_bytes2human_obj(dict_memory_bytes):
    dict_memory_human = {}
    for memory_key, memory_value in dict_memory_bytes.items():
        if memory_key != 'percent':
            memory_value = bytes2human(memory_value)
        dict_memory_human[memory_key] = memory_value
        # print('%-10s : %7s' % (name.capitalize(), value))
    return dict_memory_human
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# convert value from bytes to human
# https://stackoverflow.com/questions/56243676/python-human-readable-to-byte-conversion
def convert_bytes2human_value(byte, size_type='short'):

    size_name_long = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    size_name_short = ("B", "K", "M", "G", "T", "P", "E", "Z", "Y")

    size_value, size_unit = None, None
    if byte == 0:
        size_value, size_unit = 0, 'B'
    else:
        byte = int(byte)
        index = int(math.floor(math.log(byte, 1024)))
        power = math.pow(1024, index)
        size_value = round(byte / power, 2)

        if size_unit in size_name_long:
            size_name = deepcopy(size_name_long)
        elif size_unit in size_name_short:
            size_name = deepcopy(size_name_short)
        else:
            log_stream.error(' ===> Variable "size_unit" is not supported in long and short names')
            raise NotImplemented('Case not implemented yet')

        if size_type == 'long':
            size_unit = size_name_long[index]
        elif size_type == 'short':
            size_unit = size_name_short[index]
        else:
            log_stream.error(' ===> Variable "size_type" set to "' + size_type + '" is not supported')
            raise NotImplemented('Case not implemented yet')

    return size_value, size_unit
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# convert field from human to bytes
# https://stackoverflow.com/questions/56243676/python-human-readable-to-byte-conversion
def convert_human2bytes_value(size_obj):

    size_name_long = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    size_name_short = ("B", "K", "M", "G", "T", "P", "E", "Z", "Y")

    if isinstance(size_obj, str):
        if check_size_whitespaces(size_obj):
            size_obj = size_obj.split()             # divide '1 GB' into ['1', 'GB']
        else:
            size_obj = split_size_parts(size_obj)   # divide '1GB' into ['1', 'GB']
    elif isinstance(size_obj, list):
        if size_obj.__len__() == 2:
            pass
        else:
            log_stream.error(' ===> Object "size" list is not in supported format')
            raise NotImplemented('Case not implemented yet')
    else:
        log_stream.error(' ===> Object "size" can be defined by string or list')
        raise NotImplemented('Case not implemented yet')

    size_value, size_unit = float(size_obj[0]), size_obj[1]
    if size_value == 0:
        byte = 0
    else:

        if size_unit in size_name_long:
            size_name = deepcopy(size_name_long)
        elif size_unit in size_name_short:
            size_name = deepcopy(size_name_short)
        else:
            log_stream.error(' ===> Variable "size_unit" is not supported in long and short names')
            raise NotImplemented('Case not implemented yet')

        idx = size_name.index(size_unit)        # index in list of sizes determines power to raise it to
        factor = 1024 ** idx                    # ** is the "exponent" operator - you can use it instead of math.pow()
        byte = size_value * factor

    return byte
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# method to convert string to value
def convert_string2num_value(size_obj):
    if isinstance(size_obj, str):
        if check_size_whitespaces(size_obj):
            size_obj = size_obj.split()
        else:
            size_obj = split_size_parts(size_obj)
    size_value, size_unit = float(size_obj[0]), size_obj[1]
    return size_value
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# method to convert human value to desired units
def convert_human_unit(size_obj_start, size_unit='G', size_format='{:0.3}', size_separator=''):

    size_name_long = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    size_name_short = ("B", "K", "M", "G", "T", "P", "E", "Z", "Y")

    size_byte = convert_human2bytes_value(size_obj_start)

    if size_unit in size_name_long:
        size_name = deepcopy(size_name_long)
    elif size_unit in size_name_short:
        size_name = deepcopy(size_name_short)
    else:
        log_stream.error(' ===> Variable "size_unit" is not supported in long and short names')
        raise NotImplemented('Case not implemented yet')

    idx = size_name.index(size_unit)  # index in list of sizes determines power to raise it to
    factor = 1024 ** idx

    size_value = size_byte / factor
    size_string = size_format.format(size_value)
    size_obj_end = size_separator.join([size_string, size_unit])

    return size_obj_end
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# convert memory from obj to dict
def convert_obj2dict(obj_nt):
    obj_dict = {}
    for obj_name in obj_nt._fields:
        obj_value = getattr(obj_nt, obj_name)
        obj_dict[obj_name] = obj_value
    return obj_dict
# -------------------------------------------------------------------------------------
