"""Compatibility alias for execution.handler."""
import sys
from .execution import handler as _handler
sys.modules[__name__] = _handler
