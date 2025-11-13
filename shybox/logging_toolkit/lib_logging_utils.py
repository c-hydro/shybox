"""
Library Features:

Name:          lib_logging_utils
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20251110'
Version:       '1.0.0'
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
    Try to return current LoggingManager; if not available, return plain logging.getLogger().
    """
    try:
        return LoggingManager.require_current()
    except Exception:
        # fallback to a normal Python logger
        return logging.getLogger("default")
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# decorator to inject logger into function scope
def with_logger(var_name: str = "log"):
    """
    Decorator that makes a logger variable (default name 'log') available
    inside the decorated function, without passing it as a parameter.
    If LoggingManager is not active, falls back to standard logging.getLogger().
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                logger = LoggingManager.require_current()
            except Exception:
                logger = logging.getLogger("default")

            func_globals = func.__globals__
            original = func_globals.get(var_name, None)
            try:
                func_globals[var_name] = logger
                return func(*args, **kwargs)
            finally:
                # Restore state
                if original is None:
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
    Global helper: returns the active LoggingManager (if any) or a standard logger fallback.
    Allows using `log = get_log()` anywhere, no decorator needed.
    """
    return _get_default_log()
# ----------------------------------------------------------------------------------------------------------------------
