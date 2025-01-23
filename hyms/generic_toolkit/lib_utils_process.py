"""
Library Features:

Name:          lib_utils_process
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20250116'
Version:       '4.0.0'
"""
# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import subprocess
import time
import os

from apps.generic_toolkit.lib_default_args import logger_name, logger_arrow

# logging
logger_stream = logging.getLogger(logger_name)
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to make executable a bash file
def make_process(file):
    mode = os.stat(file).st_mode
    mode |= (mode & 0o444) >> 2    # copy R bits to X
    os.chmod(file, mode)
# -----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to execute process
def exec_process(command_line=None, time_elapsed_min=1):

    try:

        # info command-line start
        logger_stream.info(logger_arrow.info(tag='ex_process_main') + 'Process execution: ' + command_line + ' ... ')

        # execute command-line
        process_handle = subprocess.Popen(
            command_line, shell=True,
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # read standard output
        time_start_run = time.time()
        while True:
            string_out = process_handle.stdout.readline()
            if isinstance(string_out, bytes):
                try:
                    string_out = string_out.decode('UTF-8')
                except BaseException as BExp:
                    logger_stream.warning(logger_arrow.warning +
                                          'StdOut returns some non-ascii character. Get exception: ' + str(BExp))
                    logger_stream.warning(logger_arrow.warning +
                                          'Some string variables have a wrong initialization')
                    string_out = string_out.strip()
            else:
                logger_stream.info(logger_arrow.info(tag='ex_process_sub_string') + str(string_out.strip()))

            if string_out == '' and process_handle.poll() is not None:

                if process_handle.poll() == 0:
                    logger_stream.info(logger_arrow.info(tag='ex_process_sub_kill_pool') +
                                       'Process POOL = ' + str(process_handle.poll()) + ' KILLED!')
                    break
                else:
                    logger_stream.error(logger_arrow.error + 'Run failed! Check command-line settings!')
                    raise RuntimeError('Error in executing process')
            if string_out:
                logger_stream.info(logger_arrow.info(tag='ex_process_sub_string') + str(string_out.strip()))

        # collect stdout and stderr and exitcode
        std_out, std_err = process_handle.communicate()
        std_exit = process_handle.poll()

        if std_out == b'' or std_out == '':
            std_out = None
        if std_err == b'' or std_err == '':
            std_err = None

        # check stream process
        stream_process(std_out, std_err)

        # compute elapsed time run
        time_elapsed_run = round(time.time() - time_start_run, 1)

        if time_elapsed_run < time_elapsed_min:
            logger_stream.error(logger_arrow.error +
                                'Process execution FAILED! Run time elapsed: ' + str(time_elapsed_run))
            raise RuntimeError('Run execution is not correctly completed. '
                               'Check your model version, namelist version, algorithm or datasets configurations')

        # info command-line end
        logger_stream.info(logger_arrow.info(tag='ex_process_main') + 'Process execution: ' + command_line + ' ... DONE')
        return [std_out, std_err, std_exit]

    except subprocess.CalledProcessError:
        # Exit code for process error
        logger_stream.error(logger_arrow.error + 'Process execution FAILED! Errors in the called executable!')
        raise RuntimeError('Errors in the called executable')

    except OSError:
        # Exit code for os error
        logger_stream.error(logger_arrow.error + 'Process execution FAILED!')
        raise RuntimeError('Executable not found!')

# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to stream process
def stream_process(std_out=None, std_err=None):
    if std_out is None and std_err is None:
        return True
    else:
        logger_stream.warning(logger_arrow.warning + 'Exception occurred during process execution!')
        return False
# ----------------------------------------------------------------------------------------------------------------------
