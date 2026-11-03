"""
Library Features:

Name:          lib_utils_code
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20261015'
Version:       '1.2.0'
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
        use_new: bool = False,
        future_removal: bool = False):

    """
    Deprecation decorator with optional redirection and future removal warning.

    Parameters
    ----------
    use : str | None
        Name of the replacement function/method.
    since : str | None
        Version/date when the method was deprecated.
    use_new : bool
        If True, execute the replacement instead of the old method.
    future_removal : bool
        If True, warn that the method will be removed in a future release.
    """

    def decorator(old_func):

        # build warning message
        msg = f'{old_func.__qualname__} is deprecated'
        if since:
            msg += f' (since {since})'
        if future_removal:
            msg += ' and will be removed in a future release'
        if use:
            msg += f'; please use {use} instead'

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

            # execute old implementation
            return old_func(*args, **kwargs)

        return wrapper

    return decorator
# ----------------------------------------------------------------------------------------------------------------------
