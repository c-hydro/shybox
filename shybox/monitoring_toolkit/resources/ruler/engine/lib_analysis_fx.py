"""
Library Features:

Name:          lib_analysis_fx
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20221227'
Version:       '1.0.0'
"""

#######################################################################################
# Libraries
import logging
import os
import datetime
import signal
import psutil
import numpy as np

from operator import itemgetter
from copy import deepcopy

from .lib_info_args import logger_name
from .lib_analysis_utils import convert_bytes2human_obj, convert_obj2dict, get_linux_memory_usage

# Logging
log_stream = logging.getLogger(logger_name)
#######################################################################################


# -------------------------------------------------------------------------------------
# method to filter process info
def filter_process_info(process_list, process_filter='first'):

    process_filtered = None
    if process_list is not None:
        if process_filter is not None:

            # define filter string
            if isinstance(process_filter, list):
                tmp_name = [str(elem) for elem in process_filter]
                filter_name = '.'.join(tmp_name)
            else:
                filter_name = deepcopy(process_filter)

            log_stream.info(' -------> Filter process info using "' + filter_name + '" mode ... ')

            if isinstance(process_filter, str):
                if process_filter == 'first':
                    filter_idx = [0]
                elif process_filter == 'last':
                    filter_idx = [-1]
                elif process_filter == 'all':
                    filter_idx = np.arange(0, process_list.__len__(), 1, dtype=int)
                else:
                    log_stream.error(' ===> Process filter type "' + filter_name + '" is not supported')
                    raise NotImplemented('Case not implemented yet')

            elif isinstance(process_filter, list):
                filter_idx = deepcopy(process_filter)
            else:
                log_stream.error(' ===> Process filter type "' + filter_name + '" is not supported')
                raise NotImplemented('Case not implemented yet')

            # filter processes by index
            if filter_idx.__len__() == 1:
                process_filtered = [process_list[filter_idx[0]]]
            elif filter_idx.__len__() > 1:
                process_filtered = list(itemgetter(*filter_idx)(process_list))
            else:
                process_filtered = process_list[:]

            log_stream.info(' -------> Filter process info using "' + filter_name + '" mode ... DONE')

        else:
            # copy list without endpoint(s)
            if process_list is not None:
                process_filtered = process_list[:]

    return process_filtered

# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# method to sort process info
def sort_process_info(process_list, values_list, values_order_type=None):

    if values_list is not None:
        if values_order_type is not None:
            log_stream.info(' -------> Sort process info in "' + values_order_type + '" mode ... ')
            values_array = np.array(values_list)
            if values_order_type == 'ascending':
                index_sorted = np.argsort(values_array)
            elif values_order_type == 'descending':
                index_sorted = np.argsort(values_array)[::-1]
            else:
                log_stream.error(' ===> Process order type "' + values_order_type + '" is not supported')
                raise NotImplemented('Case not implemented yet')

            values_list[:] = [values_list[i] for i in index_sorted]
            process_list[:] = [process_list[i] for i in index_sorted]

            log_stream.info(' -------> Sort process info in "' + values_order_type + '" mode ... DONE')

    return process_list
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# method to get process info
def get_process_info(process_name, process_sort=None, process_filter=None, verbose=False, **kwargs):
    """Preserve Ruler's name/command selection while tolerating exiting processes."""
    processes, sizes = [], []
    for process in psutil.process_iter(['name', 'cmdline']):
        try:
            if process_name == process.info['name'] or process_name in ' '.join(process.info['cmdline'] or []):
                size = process.memory_info().vms / (1024 * 1024)
                processes.append(process)
                sizes.append(size)
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            continue
    if not processes:
        return None
    processes = sort_process_info(processes, sizes, values_order_type=process_sort)
    return filter_process_info(processes, process_filter=process_filter)



# -------------------------------------------------------------------------------------
# method to get process
def add_process_info(obj_process, obj_memory,
                     prefix_percent='percent', separator_percent='_', format_percent="{:.4f}"):

    # cpu info parts
    cpu_affinity = obj_process.cpu_affinity()
    cpu_n = obj_process.cpu_num()
    cpu_info_collections = {'cpu_affinity': cpu_affinity, 'cpu_n': cpu_n}

    # memory info parts
    dict_memory = convert_obj2dict(obj_memory)
    memory_info_list_default = ['rss', 'vms', 'shared', 'text', 'lib', 'data', 'dirty', 'uss', 'pss', 'swap']
    memory_info_list_process = list(dict_memory.keys())

    memory_info_collections = {}
    for memory_info_name in memory_info_list_default:

        if memory_info_name in memory_info_list_process:
            memory_info_percent = obj_process.memory_percent(memtype=memory_info_name)
            memory_info_percent = float(format_percent.format(memory_info_percent))
            memory_info_tag = separator_percent.join([prefix_percent, memory_info_name])
            memory_info_collections[memory_info_tag] = memory_info_percent
        else:
            log_stream.warning(' ===> Memory field "' + memory_info_name + '" was not found in the process object')

    generic_info_collections = {**cpu_info_collections, **memory_info_collections}

    return generic_info_collections

# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# method to organize process info
def organize_process_info(obj_process_list, prefix_name=None, separator_name='_', percent_format="{:.4f}",
                          process_attributes=None, process_memory_type='full', **kwargs):
    # https://thispointer.com/python-get-list-of-all-running-processes-and-sort-by-highest-memory-usage/

    '''
    memory definition(s):
    rss: aka “Resident Set Size”, this is the non-swapped physical memory a process has used. On UNIX it matches “top“‘s RES column). On Windows this is an alias for wset field and it matches “Mem Usage” column of taskmgr.exe.
    vms: aka “Virtual Memory Size”, this is the total amount of virtual memory used by the process. On UNIX it matches “top“‘s VIRT column. On Windows this is an alias for pagefile field and it matches “Mem Usage” “VM Size” column of taskmgr.exe.
    shared: (Linux) memory that could be potentially shared with other processes. This matches “top“‘s SHR column).
    text (Linux, BSD): aka TRS (text resident set) the amount of memory devoted to executable code. This matches “top“‘s CODE column).
    data (Linux, BSD): aka DRS (data resident set) the amount of physical memory devoted to other than executable code. It matches “top“‘s DATA column).
    lib (Linux): the memory used by shared libraries.
    dirty (Linux): the number of dirty pages.
    uss (Linux, macOS, Windows): aka “Unique Set Size”, this is the memory which is unique to a process and which would be freed if the process was terminated right now.
    pss (Linux): aka “Proportional Set Size”, is the amount of memory shared with other processes, accounted in a way that the amount is divided evenly between the processes that share it. I.e. if a process has 10 MBs all to itself and 10 MBs shared with another process its PSS will be 15 MBs.
    swap (Linux): amount of memory that has been swapped out to disk.
    '''

    # Info organize process info start
    log_stream.info(' ------> Organize process info ... ')

    if prefix_name is None:
        prefix_name = 'process_name'
    if separator_name is None:
        separator_name = '_'
    if process_attributes is None:
        process_attributes = ['pid', 'name', 'cpu_percent']

    info_process_collections = {}
    if obj_process_list is not None:
        for obj_process_id, obj_process_step in enumerate(obj_process_list):

            # memory basic fields
            if process_memory_type == 'partial':
                # info fields partial memory information
                info_process_fields_basic = obj_process_step.as_dict(attrs=process_attributes)
            elif process_memory_type == 'full':
                # info fields full memory information
                info_process_fields_basic = obj_process_step.as_dict(attrs=process_attributes)
                info_process_fields_memory = obj_process_step.memory_full_info()

                if 'memory_info' in list(info_process_fields_basic.keys()):
                    info_process_fields_basic.pop('memory_info')
                info_process_fields_basic['memory_info'] = info_process_fields_memory
            else:
                log_stream.error(' ===> The information "process_memory_type" defined by "' + process_memory_type +
                                 '" is not supported')
                raise NotImplemented('Case not implemented yet')

            # memory extra fields
            info_process_fields_extras = add_process_info(obj_process_step, info_process_fields_basic['memory_info'])

            # merge memory fields
            info_process_fields = {**info_process_fields_basic, **info_process_fields_extras}
            for info_key_raw, info_value_raw in info_process_fields.items():

                info_fields_collections = {}
                if isinstance(info_value_raw, str):
                    info_fields_collections[info_key_raw] = deepcopy(info_value_raw)
                elif isinstance(info_value_raw, (int, float)):
                    info_fields_collections[info_key_raw] = deepcopy(info_value_raw)
                elif isinstance(info_value_raw, list):
                    info_value_list = [str(info_value_step) for info_value_step in info_value_raw]
                    if info_key_raw == 'cmdline':
                        info_value_str = ' '.join(info_value_list)
                    else:
                        info_value_str = ','.join(info_value_list)
                    info_fields_collections[info_key_raw] = info_value_str
                elif isinstance(info_value_raw, tuple):
                    info_fields_tmp = convert_obj2dict(info_value_raw)
                    if info_key_raw == 'memory_info':
                        info_fields_collections = convert_bytes2human_obj(info_fields_tmp)
                    else:
                        info_fields_collections = deepcopy(info_fields_tmp)
                else:
                    log_stream.error(' ===> Field "' + info_key_raw + '" has not supported type')
                    raise NotImplemented('Case not implemented yet')

                for info_key_sub, info_value_sub in info_fields_collections.items():

                    if info_key_sub == 'create_time':
                        info_value_sub = datetime.datetime.fromtimestamp(info_value_sub).strftime("%Y-%m-%d %H:%M:%S")

                    if 'percent' in info_key_sub:
                        info_value_sub = float(percent_format.format(info_value_sub))

                    info_key_tmp = separator_name.join([prefix_name, info_key_sub])
                    info_key_format = {'proc_n': str(obj_process_id)}
                    info_key_def = info_key_tmp.format(**info_key_format)

                    info_process_collections[info_key_def] = info_value_sub

        # Info organize process info end
        log_stream.info(' ------> Organize process info ... DONE')

    else:

        # Info organize process info end
        log_stream.info(' ------> Organize process info ... SKIPPED')

    return info_process_collections
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# method to get disk information
def get_disk_info(disk_path=None, prefix_name='disk_usage', separator_name='_', **kwargs):

    # Info get disk info start
    log_stream.info(' ------> Get disk info ... ')

    if disk_path is None:
        disk_path = '/'
    if prefix_name is None:
        prefix_name = 'disk_usage'
    if separator_name is None:
        separator_name = '_'

    obj_disk_usage = psutil.disk_usage(disk_path)
    dict_disk_usage_bytes = convert_obj2dict(obj_disk_usage)
    dict_disk_usage_human = convert_bytes2human_obj(dict_disk_usage_bytes)

    info_disk_collection = {}
    if dict_disk_usage_human is not None:
        if isinstance(dict_disk_usage_human, dict):
            for obj_key, obj_value in dict_disk_usage_human.items():
                obj_key = separator_name.join([prefix_name, obj_key])
                info_disk_collection[obj_key] = obj_value
        else:
            log_stream.error(' ===> Disk obj is not defined by dictionary obj')
            raise RuntimeError('Only dictionary obj is supported by the function')
    else:
        log_stream.warning(' ===> Disk obj is defined by NoneType')

    # Info get disk info end
    log_stream.info(' ------> Get disk info ... DONE')

    return info_disk_collection
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# method to get memory information
def get_memory_info():
    """Collect host/swap memory through psutil without parsing shell utilities."""
    virtual = convert_bytes2human_obj(convert_obj2dict(psutil.virtual_memory()))
    swap = convert_bytes2human_obj(convert_obj2dict(psutil.swap_memory()))
    from pathlib import Path
    swappiness = Path('/proc/sys/vm/swappiness')
    if swappiness.is_file():
        swap['swappiness'] = int(swappiness.read_text().strip())
    return virtual, swap


def organize_memory_info(obj_virtual_memory=None, obj_swap_memory=None,
                         prefix_name_virtual_memory='virtual_memory', prefix_name_swap_memory='swap_memory',
                         separator_name='_', **kwargs):

    # Info organize memory info start
    log_stream.info(' ------> Organize memory info ... ')

    if prefix_name_virtual_memory is None:
        prefix_name_virtual_memory = 'virtual_memory'
    if prefix_name_swap_memory is None:
        prefix_name_swap_memory = 'swap_memory'
    if separator_name is None:
        separator_name = '_'

    info_memory_collections = {}
    if obj_virtual_memory is not None:
        if isinstance(obj_virtual_memory, dict):
            for obj_key, obj_value in obj_virtual_memory.items():
                obj_key = separator_name.join([prefix_name_virtual_memory, obj_key])
                info_memory_collections[obj_key] = obj_value
        else:
            log_stream.error(' ===> Virtual memory obj is not defined by dictionary obj')
            raise RuntimeError('Only dictionary obj is supported by the function')
    else:
        log_stream.warning(' ===> Virtual memory obj is defined by NoneType')
    if obj_swap_memory is not None:
        if isinstance(obj_swap_memory, dict):
            for obj_key, obj_value in obj_swap_memory.items():
                obj_key = separator_name.join([prefix_name_swap_memory, obj_key])
                info_memory_collections[obj_key] = obj_value
        else:
            log_stream.error(' ===> Swap memory obj is not defined by dictionary obj')
            raise RuntimeError('Only dictionary obj is supported by the function')
    else:
        log_stream.warning(' ===> Swap memory obj is defined by NoneType')

    # Info organize memory info end
    log_stream.info(' ------> Organize memory info ... DONE')

    return info_memory_collections
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# method to get system load information
def get_system_load_info(list_load_system_keys=None, prefix_name='system_load', separator_name='_', **kwargs):

    # args default
    if list_load_system_keys is None:
        list_load_system_keys = ['1m', '5m', '15m']

    # Info get system load info start
    log_stream.info(' ------> Get system load info ... ')

    obj_load_system = psutil.getloadavg()
    list_load_system_values = list(obj_load_system)

    dict_load_system = dict(zip(list_load_system_keys, list_load_system_values))

    info_system_load_collection = {}
    if isinstance(dict_load_system, dict):
        for obj_key, obj_value in dict_load_system.items():
            obj_key = separator_name.join([prefix_name, obj_key])
            info_system_load_collection[obj_key] = obj_value
    else:
        log_stream.error(' ===> Disk obj is not defined by dictionary obj')
        raise RuntimeError('Only dictionary obj is supported by the function')

    # Info get system load info end
    log_stream.info(' ------> Get system load info ... DONE')

    return info_system_load_collection
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# method to kill process tree
def kill_process_tree(pid, sig=signal.SIGTERM, include_parent=True,
                      timeout=None, on_terminate=None):
    """Kill a process tree (including grandchildren) with signal
    "sig" and return a (gone, still_alive) tuple.
    "on_terminate", if specified, is a callback function which is
    called as soon as a child terminates.
    """
    assert pid != os.getpid(), "won't kill myself"
    parent = psutil.Process(pid)
    children = parent.children(recursive=True)
    if include_parent:
        children.append(parent)
    for p in children:
        try:
            p.send_signal(sig)
        except psutil.NoSuchProcess:
            pass
    gone, alive = psutil.wait_procs(children, timeout=timeout,
                                    callback=on_terminate)
    return (gone, alive)
# -------------------------------------------------------------------------------------
