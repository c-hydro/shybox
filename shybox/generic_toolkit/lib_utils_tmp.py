"""
Library Features:

Name:          lib_utils_tmp
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20260120'
Version:       '1.1.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
import os
import tempfile
from copy import deepcopy
from pathlib import Path

from shybox.logging_toolkit.lib_logging_utils import with_logger
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to ensure temporary folder
@with_logger(var_name='logger_stream')
def ensure_folder_tmp(folder_tmp: str = None, prefix_tmp: str = "shybox_") -> str:
    """
    Ensure a temporary folder exists.
    If folder_tmp is None / empty / unresolved placeholder like "{dir_tmp}",
    fallback to a system temporary folder and warn the user.
    """

    # check unresolved placeholder patterns like "{dir_tmp}"
    if folder_tmp is not None:
        folder_tmp_str = str(folder_tmp).strip()

        # empty -> invalid
        if folder_tmp_str == "":
            logger_stream.warning("Temp folder is empty/undefined: using system temp folder.")
            folder_tmp = None

        # placeholder not resolved -> invalid
        elif folder_tmp_str.startswith("{") and folder_tmp_str.endswith("}"):
            logger_stream.warning(
                f"Temp folder '{folder_tmp_str}' looks like an unresolved placeholder: "
                f"using system temp folder instead."
            )
            folder_tmp = None

    # try to use custom folder
    if folder_tmp is not None:
        try:
            os.makedirs(folder_tmp, exist_ok=True)
            folder_def = deepcopy(folder_tmp)
        except Exception as exc:
            folder_def = tempfile.mkdtemp(prefix=prefix_tmp)
            logger_stream.warning(
                f"Temp folder '{folder_tmp}' cannot be created/used ({exc}). "
                f"Using system temp folder '{folder_def}'."
            )
    else:
        folder_def = tempfile.mkdtemp(prefix=prefix_tmp)
        logger_stream.warning(
            f"Using system temp folder '{folder_def}'."
        )

    if folder_def is None:
        logger_stream.error("Failed to create a temporary folder.")
        raise RuntimeError("Failed to create a temporary folder.")

    return folder_def
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to ensure temporary file
@with_logger(var_name='logger_stream')
def ensure_file_tmp(filename: str = None, folder_tmp: str = None, ext: str = ".tmp") -> Path:
    """
    Create and ensure a temporary file in a safe way.

    Args:
        filename (str, optional): Desired base filename (without extension).
                                  If None, a secure random tmp name is generated.
        folder_tmp (str, optional): Folder for the temp file. If None, system tmp folder is used.
        ext (str, optional): Extension for the file (default '.tmp').

    Returns:
        Path: Path to the created temporary file.
    """
    # Normalize extension
    if not ext.startswith('.'):
        ext = '.' + ext

    # Ensure folder
    folder = ensure_folder_tmp(folder_tmp)

    if filename is None:
        # Secure random tmp file
        fd, tmp_path = tempfile.mkstemp(suffix=ext, dir=str(folder))
        os.close(fd)
        return Path(tmp_path)
    else:
        # Deterministic name with collision handling
        path = folder / f"{filename}{ext}"
        counter = 1
        while path.exists():
            path = folder / f"{filename}_{counter}{ext}"
            counter += 1
        path.touch(exist_ok=False)
        return path
# ----------------------------------------------------------------------------------------------------------------------
