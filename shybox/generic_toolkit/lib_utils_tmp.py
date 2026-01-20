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
from pathlib import Path

from shybox.logging_toolkit.lib_logging_utils import with_logger
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to ensure temporary folder
@with_logger(var_name='logger_stream')
def ensure_folder_tmp(folder_tmp: str = None) -> str:
    """
    Ensure a temporary folder exists.

    Args:
        folder_tmp (str, optional): path to the temporary folder.
                                    If None, uses the system temp directory.

    Returns:
        folder_def: path to the ensured temporary folder.
    """

    folder_def = None
    if folder_tmp is not None:
        try:
            os.makedirs(folder_tmp, exist_ok=True)
            folder_def = tempfile.mkdtemp(dir=folder_tmp)
        except Exception as exc:
            logger_stream.warning(
                f"Cannot use temp folder '{tmp_root}' ({exc}). Falling back to system temp folder."
            )
            folder_def = tempfile.mkdtemp()
    else:
        logger_stream.warning("Temp folder is None: using system temp folder instead of a custom temp folder.")
        folder_def = tempfile.mkdtemp()

    if folder_def is None:
        logger_stream.error('Failed to create a temporary folder.')
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
