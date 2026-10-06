"""
Library Features:

Name:          lib_utils_code
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20260930'
Version:       '1.1.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import warnings
from functools import wraps
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to deprecate old methods
def deprecated(
        *,
        use: str | None = None,
        since: str | None = None,
        use_new: bool = False):

    """
    Deprecation decorator with optional redirection to a new implementation.

    Parameters
    ----------
    use : str | None
        Name of the new function/method to use instead.

    since : str | None
        Version/date information for the warning.

    use_new : bool
        If True, call the new implementation instead of executing the old code.
        If False, execute the old implementation and only emit a warning.
    """

    def decorator(old_func):

        # build warning message
        msg = f'{old_func.__qualname__} is deprecated'
        if since:
            msg += f' (since {since})'
        if use:
            msg += f'; use {use} instead'

        @wraps(old_func)
        def wrapper(*args, **kwargs):

            # emit warning
            warnings.warn(msg,category=DeprecationWarning,stacklevel=2)
            # optionally redirect to new implementation
            if use_new:

                if not use:
                    raise RuntimeError(f'{old_func.__qualname__}: use_new=True requires use="new_function_name"')

                # try class method first
                if args and hasattr(args[0], use):
                    new_func = getattr(args[0], use)
                    return new_func(*args[1:], **kwargs)

                # otherwise try function in same module
                new_func = old_func.__globals__.get(use)
                if new_func is None:
                    raise RuntimeError(f'{old_func.__qualname__}: replacement "{use}" was not found')

                return new_func(*args, **kwargs)

            # default: execute old implementation
            return old_func(*args, **kwargs)

        return wrapper

    return decorator
# ----------------------------------------------------------------------------------------------------------------------
