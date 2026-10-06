"""
Library Features:

Name:          lib_logging_utils
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20260930'
Version:       '1.1.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
from functools import wraps

from shybox.logging_toolkit.logging_handler import LoggingManager
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# helper to get default log
def _get_default_log():
    """
    Try to return current LoggingManager; if not available,
    return plain logging.getLogger().
    """
    try:
        return LoggingManager.require_current()
    except Exception:
        return logging.getLogger("default")
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# decorator to inject logger into function scope
def with_logger(var_name: str = "log"):
    """
    Decorator that makes a logger variable available inside the decorated function.

    Priority:
    1. If logger is explicitly passed in kwargs using `var_name`, use it.
    2. Otherwise use LoggingManager.require_current().
    3. If LoggingManager is not active, fall back to logging.getLogger("default").
    """

    def decorator(func):

        @wraps(func)
        def wrapper(*args, **kwargs):

            # use explicitly passed logger, otherwise get default one
            logger = kwargs.get(var_name, None)
            if logger is None:
                logger = _get_default_log()

            # get function global namespace
            func_globals = func.__globals__
            # save original global value
            sentinel = object()
            original = func_globals.get(var_name, sentinel)
            try:
                # inject logger into function scope
                func_globals[var_name] = logger
                return func(*args, **kwargs)

            finally:
                # restore previous state
                if original is sentinel:
                    func_globals.pop(var_name, None)
                else:
                    func_globals[var_name] = original

        return wrapper

    return decorator
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# global helper to get logger
def get_log() -> logging.Logger:
    """
    Global helper: returns the active LoggingManager (if any)
    or a standard logger fallback.

    Allows using:
        log = get_log()
    anywhere, without decorator.
    """
    return _get_default_log()
# ----------------------------------------------------------------------------------------------------------------------
