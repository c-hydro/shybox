"""
Library Features:

Name:          lib_utils_logging
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20241202'
Version:       '4.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import os
import sys
import logging.config
import glob

from copy import deepcopy

from shybox.default.lib_default_args import logger_name as logger_name_default
from shybox.default.lib_default_args import logger_file as logger_file_default
# from lib_default_args import logger_handle as logger_handle_default
from shybox.default.lib_default_args import logger_format as logger_format_default

# debugging
# import matplotlib.pylab as plt
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to reset logging stream
def reset_logging_stream(logger_name: str = logger_name_default):
    loggers = [logging.getLogger(name) for name in logging.root.manager.loggerDict]
    loggers.append(logging.getLogger())
    for logger in loggers:
        handlers = logger.handlers[:]
        for handler in handlers:
            logger.removeHandler(handler)
            handler.close()
        logger.setLevel(logging.NOTSET)
        logger.propagate = True
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to set logging stream
def set_logging_stream(logger_name: str = logger_name_default,
                       logger_folder: str = None,
                       logger_file: str = logger_file_default, logger_format: str = logger_format_default,
                       logger_history_flag: bool = False, logger_history_count: int = 4) -> None:

    # reset logging stream
    reset_logging_stream(logger_name=logger_name)

    if logger_format is None:
        logger_format = deepcopy(logger_format)
    if logger_file is None:
        logger_file = deepcopy(logger_file)

    if logger_folder is not None:
        logger_path = os.path.join(logger_folder, logger_file)
    else:
        logger_path = deepcopy(logger_file)

    # Save old logger file (to check run in the past)
    if logger_history_flag:
        save_logging_stream(logger_path, logger_count_max=logger_history_count)

    # remove logging file
    if os.path.exists(logger_path):
        os.remove(logger_path)

    logger_loc = os.path.split(logger_path)
    if logger_loc[0] == '' or logger_loc[0] == "":
        logger_folder_name, logger_file_name = os.path.dirname(os.path.abspath(sys.argv[0])), logger_loc[1]
    else:
        logger_folder_name, logger_file_name = logger_loc[0], logger_loc[1]

    os.makedirs(logger_folder_name, exist_ok=True)

    # define logger path
    logger_path = os.path.join(logger_folder_name, logger_file_name)

    # Remove old logging file
    if os.path.exists(logger_path):
        os.remove(logger_path)

    # Open logger
    logging.getLogger(logger_name)
    logging.root.setLevel(logging.DEBUG)

    # Open logging basic configuration
    logging.basicConfig(level=logging.DEBUG, format=logger_format, filename=logger_path, filemode='w')

    # Set logger handle
    logger_handle_1 = logging.FileHandler(logger_path, 'w')
    logger_handle_2 = logging.StreamHandler()
    # Set logger level
    logger_handle_1.setLevel(logging.DEBUG)
    logger_handle_2.setLevel(logging.DEBUG)
    # Set logger formatter
    logger_formatter = logging.Formatter(logger_format)
    logger_handle_1.setFormatter(logger_formatter)
    logger_handle_2.setFormatter(logger_formatter)

    # Add handle to logging
    logging.getLogger('').addHandler(logger_handle_1)
    logging.getLogger('').addHandler(logger_handle_2)

# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to save logging stream (history of logging files)
def save_logging_stream(logger_file: str, logger_suffix: str = '.old.{}', logger_count_max: int = 12) -> None:

    # get folder name
    folder_name, _ = os.path.split(logger_file)

    # iterate to store old logging files
    if os.path.exists(logger_file):

        loop_file = deepcopy(logger_file)
        loop_count_id = 0
        while os.path.exists(loop_file):
            loop_count_id += 1
            loop_file = logger_file + logger_suffix.format(loop_count_id)

            if loop_count_id > logger_count_max:
                logger_file_list = glob.glob(os.path.join(folder_name, '*'))
                for logger_file_old in logger_file_list:
                    if logger_file_old.startswith(logger_file):
                        os.remove(logger_file_old)
                loop_file = logger_file
                break

        if loop_file:
            if logger_file != loop_file:
                os.rename(logger_file, loop_file)
# ----------------------------------------------------------------------------------------------------------------------
