"""
Library Features:

Name:          lib_utils_code
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20260122'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import warnings
from functools import wraps
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to deprecate old methods
def deprecated(*, use: str | None = None, since: str | None = None, use_new: bool = False):
    """
    Deprecation decorator with optional redirection to a new method.

    Parameters
    ----------
    use : str | None
        Name of the new method to call instead (same class).
    since : str | None
        Version/date info for the warning.
    use_new : bool
        If True, call the new method instead of executing the old code.

    Example
    -------
    class MyClass:

    def compute(self, x):
        # NEW implementation
        return x * 2

    @deprecated(use="compute", since="2.0", use_new=False)
    def compute_old(self, x):
        # OLD implementation
        return x + x
    """
    def decorator(old_func):
        msg = f"{old_func.__qualname__} is deprecated"
        if since:
            msg += f" (since {since})"
        if use:
            msg += f"; use {use} instead"

        @wraps(old_func)
        def wrapper(self, *args, **kwargs):
            warnings.warn(msg, category=DeprecationWarning, stacklevel=2)

            # 🔥 switch implementation here
            if use_new:
                if not use:
                    raise RuntimeError(
                        f"{old_func.__qualname__}: use_new=True requires `use='new_method_name'`"
                    )
                new_func = getattr(self, use)
                return new_func(*args, **kwargs)

            # default: run old implementation
            return old_func(self, *args, **kwargs)

        return wrapper
    return decorator
# ----------------------------------------------------------------------------------------------------------------------
