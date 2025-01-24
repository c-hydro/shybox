"""
Library Features:

Name:          lib_utils_file
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20250114'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import os
import sys
import shutil
import errno, os

from hyms.generic_toolkit.lib_default_args import logger_name, logger_arrow

# Sadly, Python fails to provide the following magic number for us.
ERROR_INVALID_NAME = 123

# logging
logger_stream = logging.getLogger(logger_name)
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to expand file path
def expand_file_path(file_path: str, home_string: str = '$HOME') -> str:
    if home_string in file_path:
        home_path = os.path.expanduser('~')
        file_path = file_path.replace(home_string, home_path)
    return file_path
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to sanitize file path
def sanitize_file_path(file_path: str, remove_sep_repetition: bool = True) -> str:
    file_path = os.path.normpath(file_path)
    if remove_sep_repetition:
        file_path = file_path.replace('//', '/')
    return file_path
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to split file path
def split_file_path(file_path: str) -> (str, str):
    file_name = os.path.basename(file_path)
    folder_name = os.path.dirname(file_path)
    return file_name, folder_name
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to join file path
def join_file_path(file_name: str, folder_name: str = None) -> str:

    if file_name is None:
        logger_stream.error(logger_arrow.error + ' File name is not defined')
        raise IOError('File name is not defined')

    if folder_name is not None:
        file_path = os.path.join(folder_name, file_name)
    else:
        file_path = file_name

    return file_path
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to check file path
def check_file_path(file_path: str) -> None:

    file_name = os.path.basename(file_path)
    folder_name = os.path.dirname(file_path)

    check_valid = True
    if folder_name == '':
        logger_stream.warning(logger_arrow.warning + 'Folder name is defined by empty string. '
                                                     'Folder will be based on current working directory')
    else:
        check_valid = is_pathname_valid(folder_name)

    if not check_valid:
        logger_stream.error(logger_arrow.error + ' Folder name "' + folder_name + '" is not valid')
        raise IOError('Folder name is not valid')

    check_creatable = True
    if folder_name == '':
        pass
    else:
        check_creatable = is_path_creatable(folder_name)

    if not check_creatable:
        logger_stream.error(logger_arrow.error + ' Folder name "' + folder_name + '" is not creatable')
        raise IOError('Folder name is not creatable')

    pass
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to copy file from source to destination
def copy_file(file_path_src: str, file_path_dest: str) -> None:
    if os.path.exists(file_path_src):
        if not file_path_src == file_path_dest:
            if os.path.exists(file_path_dest):
                os.remove(file_path_dest)
            shutil.copy2(file_path_src, file_path_dest)
    else:
        logger_stream.warning(logger_arrow.warning + 'Copy file "' + file_path_src + '" failed! Source not available!')
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to check if path is creatable
def is_path_creatable(pathname: str) -> bool:
    '''
    `True` if the current user has sufficient permissions to create the passed
    pathname; `False` otherwise.
    '''
    # Parent directory of the passed path. If empty, we substitute the current
    # working directory (CWD) instead.
    dirname = os.path.dirname(pathname) or os.getcwd()
    return os.access(dirname, os.W_OK)
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to check if path exists or is creatable
def is_path_exists_or_creatable(pathname: str) -> bool:
    '''
    `True` if the passed pathname is a valid pathname for the current OS _and_
    either currently exists or is hypothetically creatable; `False` otherwise.

    This function is guaranteed to _never_ raise exceptions.
    '''
    try:
        # To prevent "os" module calls from raising undesirable exceptions on
        # invalid pathnames, is_pathname_valid() is explicitly called first.
        return is_pathname_valid(pathname) and (
            os.path.exists(pathname) or is_path_creatable(pathname))
    # Report failure on non-fatal filesystem complaints (e.g., connection
    # timeouts, permissions issues) implying this path to be inaccessible. All
    # other exceptions are unrelated fatal issues and should not be caught here.
    except OSError:
        return False
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to check if path is valid
def is_pathname_valid(pathname: str) -> bool:
    '''
    `True` if the passed pathname is a valid pathname for the current OS;
    `False` otherwise.
    '''
    # If this pathname is either not a string or is but is empty, this pathname
    # is invalid.
    try:
        if not isinstance(pathname, str) or not pathname:
            return False

        # Strip this pathname's Windows-specific drive specifier (e.g., `C:\`)
        # if any. Since Windows prohibits path components from containing `:`
        # characters, failing to strip this `:`-suffixed prefix would
        # erroneously invalidate all valid absolute Windows pathnames.
        _, pathname = os.path.splitdrive(pathname)

        # Directory guaranteed to exist. If the current OS is Windows, this is
        # the drive to which Windows was installed (e.g., the "%HOMEDRIVE%"
        # environment variable); else, the typical root directory.
        root_dirname = os.environ.get('HOMEDRIVE', 'C:') \
            if sys.platform == 'win32' else os.path.sep
        assert os.path.isdir(root_dirname)   # ...Murphy and her ironclad Law

        # Append a path separator to this directory if needed.
        root_dirname = root_dirname.rstrip(os.path.sep) + os.path.sep

        # Test whether each path component split from this pathname is valid or
        # not, ignoring non-existent and non-readable path components.
        for pathname_part in pathname.split(os.path.sep):
            try:
                os.lstat(root_dirname + pathname_part)
            # If an OS-specific exception is raised, its error code
            # indicates whether this pathname is valid or not. Unless this
            # is the case, this exception implies an ignorable kernel or
            # filesystem complaint (e.g., path not found or inaccessible).
            #
            # Only the following exceptions indicate invalid pathnames:
            #
            # * Instances of the Windows-specific "WindowsError" class
            #   defining the "winerror" attribute whose value is
            #   "ERROR_INVALID_NAME". Under Windows, "winerror" is more
            #   fine-grained and hence useful than the generic "errno"
            #   attribute. When a too-long pathname is passed, for example,
            #   "errno" is "ENOENT" (i.e., no such file or directory) rather
            #   than "ENAMETOOLONG" (i.e., file name too long).
            # * Instances of the cross-platform "OSError" class defining the
            #   generic "errno" attribute whose value is either:
            #   * Under most POSIX-compatible OSes, "ENAMETOOLONG".
            #   * Under some edge-case OSes (e.g., SunOS, *BSD), "ERANGE".
            except OSError as exc:
                if hasattr(exc, 'winerror'):
                    if exc.winerror == ERROR_INVALID_NAME:
                        return False
                elif exc.errno in {errno.ENAMETOOLONG, errno.ERANGE}:
                    return False
    # If a "TypeError" exception was raised, it almost certainly has the
    # error message "embedded NUL character" indicating an invalid pathname.
    except TypeError as exc:
        return False
    # If no exception was raised, all path components and hence this
    # pathname itself are valid. (Praise be to the curmudgeonly python.)
    else:
        return True
    # If any other exception was raised, this is an unrelated fatal issue
    # (e.g., a bug). Permit this exception to unwind the call stack.
    #
    # Did we mention this should be shipped with Python already?
# ----------------------------------------------------------------------------------------------------------------------
