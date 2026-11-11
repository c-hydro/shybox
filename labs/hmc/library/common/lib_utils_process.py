"""
Library Features:

Name:          lib_utils_process
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20210113'
Version:       '3.0.0'
"""

#######################################################################################
# Library
import logging
import os
import subprocess

from copy import deepcopy
from datetime import datetime

from library.common.lib_info_args import logger_name

# Logging
log_stream = logging.getLogger(logger_name)

# Debug
# import matplotlib.pylab as plt
#######################################################################################


# -------------------------------------------------------------------------------------
# Method to fill the command line
def fill_command_line_template(command_line_raw, template_raw=None, template_filled=None, key_tag='KEY_TMPL_{:}'):

    command_line_map = deepcopy(command_line_raw)

    map_filled = {}
    for key_id, (key_raw, format_raw) in enumerate(list(template_raw.items())):

        if key_raw in list(template_filled.keys()):
            value_filled = template_filled[key_raw]

            if key_raw in template_raw:
                key_raw = '{' + key_raw + '}'
                key_tmp = key_tag.format(str(key_id))
                command_line_map = command_line_map.replace(key_raw, key_tmp)

                if isinstance(value_filled, datetime):
                    map_value_filled = value_filled.strftime(format_raw)
                elif isinstance(value_filled, (float, int)):
                    map_value_filled = format_raw.format(value_filled)
                elif isinstance(value_filled, str):
                    map_value_filled = deepcopy(value_filled)
                else:
                    print(' ERROR ===> Object to fill the cmd template is defined by unknown format')
                    raise NotImplementedError('Case not implemented yet')

                map_filled[key_tmp] = map_value_filled

    for map_key, map_value in map_filled.items():
        if map_value is not None:
            command_line_map = command_line_map.replace(map_key, map_value)

    return command_line_map
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to execute process
def exec_process(command_line=None, command_path=None):

    try:

        # Info command-line start
        print(' INFO -----> Process execution: ' + command_line + ' ... ')

        # Execute command-line
        if command_path is not None:
            os.chdir(command_path)
        process_handle = subprocess.Popen(
            command_line, shell=True,
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # Read standard output
        while True:
            string_out = process_handle.stdout.readline()
            if isinstance(string_out, bytes):
                string_out = string_out.decode('UTF-8')

            if string_out == '' and process_handle.poll() is not None:
                if process_handle.poll() == 0:
                    break
                else:
                    std_out, std_err = process_handle.communicate()
                    string_err = std_err.decode('UTF-8').lower()
                    if 'no such file or directory' in string_err:
                        print(' WARNING ===> File or directory not found')
                        break
                    else:
                        print(' ERROR ===> Run failed! Check command-line settings!')
                        raise RuntimeError('Error in executing process')
            if string_out:
                print(' INFO ------> ' + str(string_out.strip()))

        # Collect stdout and stderr and exitcode
        std_out, std_err = process_handle.communicate()
        std_exit = process_handle.poll()

        if std_out == b'' or std_out == '':
            std_out = None
        if std_err == b'' or std_err == '':
            std_err = None

        # Check stream process
        stream_process(std_out, std_err)

        # Info command-line end
        if std_err is None:
            print(' INFO -----> Process execution: ' + command_line + ' ... DONE')
        else:
            print(' INFO -----> Process execution: ' + command_line + ' ... FAILED')

        return std_out, std_err, std_exit

    except subprocess.CalledProcessError:
        # Exit code for process error
        print(' ERROR ===> Process execution FAILED! Errors in the called executable!')
        raise RuntimeError('Errors in the called executable')

    except OSError:
        # Exit code for os error
        print(' ERROR ===> Process execution FAILED!')
        raise RuntimeError('Executable not found!')

# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to stream process
def stream_process(std_out=None, std_err=None):

    if std_out is None and std_err is None:
        return True
    else:
        print(' WARNING ===> Exception occurred during process execution!')
        return False
# -------------------------------------------------------------------------------------
