"""
Library Features:

Name:          lib_utils_system
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20210113'
Version:       '3.0.0'
"""

#######################################################################################
# Library
import logging
import os
import shutil
import subprocess
import pipes
import numpy as np

from datetime import datetime
from copy import deepcopy
from os.path import exists

from library.common.lib_info_args import logger_name

# Logging
log_stream = logging.getLogger(logger_name)

# Debug
# import matplotlib.pylab as plt
#######################################################################################


# -------------------------------------------------------------------------------------
# Method to get the machine information
def info_machine(machine_dict, tag_ip='ip', tag_user='user', tag_type='type'):

    machine_ip = None
    if tag_ip in list(machine_dict.keys()):
        machine_ip = machine_dict[tag_ip]
    machine_user = None
    if tag_user in list(machine_dict.keys()):
        machine_user = machine_dict[tag_user]
    machine_type = 'local'
    if tag_type in list(machine_dict.keys()):
        machine_type = machine_dict[tag_type]

    if (machine_ip is not None) and (machine_user is not None):
        machine_address = '{machine_user}@{machine_ip}'.format(
            machine_user=machine_user, machine_ip=machine_ip)
    else:
        machine_address = None

    if (machine_address is None) and machine_type == 'remote':
        print(' ERROR ===> The machine is declared as remote; the machine host must be not None type')
        raise RuntimeError('Define the address and the user for your remote machine')

    return machine_address, machine_user, machine_ip, machine_type
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to check the server connection using ssh
def check_connection(machine_ip, machine_type='local'):
    if machine_type == 'remote':
        cmd = "ping -c 1 " + machine_ip.strip(";")

        process_handle = subprocess.Popen(
            cmd, shell=True, executable='/bin/bash',
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        std_out, std_err = process_handle.communicate()

        # machine_response = os.system("ping -c 1 " + machine_ip.strip(";"))
        if std_err == b'' or std_err == '':
            machine_status = True
        else:
            std_error = std_err.decode('UTF-8')
            print(' ERROR ===> The machine "' + machine_ip + '" is unreachable using the ssh connection')
            print(' ERROR ===> Standard error(s) was:  "' + std_error + '"')
            raise ConnectionError('Check your connection and your settings')

    elif machine_type == 'local':
        machine_status = None
    else:
        print(' ERROR ===> The machine is not correctly defined. Only "local" and "remote" tags are permitted')
        raise RuntimeError('Check your "machine_type" settings')
    return machine_status
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to check file with ssh on remote machine
def exists_remote(host, path):
    """Test if a file exists at path on a host accessible with SSH."""
    status = subprocess.call(
        ['ssh', host, 'test -f {}'.format(pipes.quote(path))])
    if status == 0:
        return True
    if status == 1:
        return False
    raise Exception('SSH failed')
# -------------------------------------------------------------------------------------
