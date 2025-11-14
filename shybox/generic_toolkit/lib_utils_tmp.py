"""
Library Features:

Name:          lib_utils_tmp
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20251030'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
import os
import tempfile
from pathlib import Path
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to ensure temporary folder
def ensure_folder_tmp(folder_tmp: str = None) -> Path:
    """
    Ensure a temporary folder exists.

    Args:
        folder_tmp (str, optional): Path to the temporary folder.
                                    If None, uses the system temp directory.

    Returns:
        Path: Path to the ensured temporary folder.
    """
    folder = Path(folder_tmp or tempfile.gettempdir())
    folder.mkdir(parents=True, exist_ok=True)
    return folder
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to ensure temporary file
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
