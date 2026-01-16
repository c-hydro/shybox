"""
Class Features

Name:          arguments_handler
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20260115'
Version:       '1.5.0'
"""
# ----------------------------------------------------------------------------------------------------------------------
# libraries

import argparse
import os
from typing import Optional, Tuple, Dict, Any, Union

from shybox.logging_toolkit.logging_handler import LoggingManager

# defaults
try:
    # Import your default definitions
    from shybox.default.lib_default_log import (
        log_name as DEFAULT_LOG_NAME,
        log_file as DEFAULT_LOG_FILE,
        log_folder as DEFAULT_LOG_FOLDER,
        log_format as DEFAULT_LOG_FORMAT,
        log_handler as DEFAULT_LOG_HANDLER,
    )
except ImportError:
    # Fallbacks if lib_default_log is not available
    DEFAULT_LOG_NAME = "app"
    DEFAULT_LOG_FILE = "app.log"
    DEFAULT_LOG_FOLDER = None
    DEFAULT_LOG_FORMAT = "%(asctime)s %(name)s %(levelname)s %(message)s"
    DEFAULT_LOG_HANDLER = ["file", "stream"]
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# class to handle application arguments
class ArgumentsManager:

    # initializer
    def __init__(self,
                 handlers: Optional[list] = None,
                 settings_folder: Optional[str] = None,
                 logger: LoggingManager | None = None):

        # define logging setup kwargs
        logger_kwargs = {'handlers': handlers}

        # Normalize logger: use provided, or current, or create default
        self.log = LoggingManager.get_logger(
            logger=logger, name="ArgumentsManager", set_as_current=False, setup_kwargs=logger_kwargs
            )

        # set the handlers (file, stream)
        self.handlers = handlers if handlers is not None else DEFAULT_LOG_HANDLER

        # Folder passed by the caller (priority)
        # If None then fallback handled in _resolve_path()
        self.settings_folder = settings_folder

    # method to get arguments
    def get(self) -> Union[Tuple[str, Optional[str]], Tuple[str, Optional[str], Dict[str, Any]]]:

        # info start
        self.log.info_up("Arguments ... ")

        # parse arguments (settings file)
        parser = argparse.ArgumentParser(description="parse the application arguments")
        parser.add_argument(
            "-settings_file", dest="settings_file", required=True,
            help="Path to the settings JSON file (mandatory)"
        )
        # parse arguments (settings time)
        parser.add_argument(
            "-time", dest="settings_time", required=False,
            help="Optional time string for processing",
        )

        # parse known and unknown args
        args, unknown = parser.parse_known_args()

        # get settings file and time (expected arguments)
        settings_file = self._resolve_path(args.settings_file)
        settings_time = args.settings_time
        # get extra arguments (unexpected arguments)
        extra_args = self._parse_extra_args(unknown)

        # info end
        self.log.info_down("Arguments ... DONE")

        # return different outputs based on extra args presence
        if extra_args:
            return settings_file, settings_time, extra_args
        else:
            return settings_file, settings_time

    # method to resolve path (in different scenarios)
    def _resolve_path(self, path: str) -> str:

        # If path already contains a folder → accept as-is (relative or absolute)
        if os.path.dirname(path):
            # If it's absolute, return directly
            if os.path.isabs(path):
                return path
            # Relative folder → use it as relative to working directory
            return os.path.abspath(path)
        # If no folder in path → prepend caller folder
        folder = self.settings_folder or os.path.dirname(os.path.realpath(__file__))

        return os.path.join(folder, path)

    # method to parse extra arguments (internal use)
    def _parse_extra_args(self, items) -> Dict[str, Any]:

        out, key = {}, None
        for item in items:
            if item.startswith('-'):
                key = item.lstrip('-')
                out[key] = True
            else:
                if key:
                    out[key] = item
                    key = None

        return out
# ----------------------------------------------------------------------------------------------------------------------
