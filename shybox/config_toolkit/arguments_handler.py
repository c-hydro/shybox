import argparse
import os
from typing import Optional, Tuple, Dict, Any, Union

from shybox.logging_toolkit.logging_handler import LoggingManager

class ArgumentsManager:
    def __init__(self,
                 settings_folder: Optional[str] = None,
                 logger: LoggingManager | None = None):

        # Normalize logger: use provided, or current, or create default
        self.log = LoggingManager.get_logger(
            logger=logger,
            name="ArgumentsManager",
            set_as_current=False,
        )

        # Folder passed by the caller → priority
        # If None → fallback handled in _resolve_path()
        self.settings_folder = settings_folder

    def get(self) -> Union[
        Tuple[str, Optional[str]],
        Tuple[str, Optional[str], Dict[str, Any]]
    ]:

        # info start
        self.log.info_up("Arguments ... ")

        # parse arguments
        parser = argparse.ArgumentParser(description="parse the application arguments")
        parser.add_argument(
            "-settings_file",
            dest="settings_file",
            required=True,
            help="Path to the settings JSON file (mandatory)"
        )

        parser.add_argument(
            "-time",
            dest="settings_time",
            help="Optional time string for processing",
            required=False,
        )

        args, unknown = parser.parse_known_args()

        settings_file = self._resolve_path(args.settings_file)
        settings_time = args.settings_time

        extra_args = self._parse_extra_args(unknown)

        # info end
        self.log.info_down("Arguments ... DONE")

        if extra_args:
            return settings_file, settings_time, extra_args
        else:
            return settings_file, settings_time

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

    def _parse_extra_args(self, items) -> Dict[str, Any]:
        out = {}
        key = None

        for item in items:
            if item.startswith('-'):
                key = item.lstrip('-')
                out[key] = True
            else:
                if key:
                    out[key] = item
                    key = None

        return out