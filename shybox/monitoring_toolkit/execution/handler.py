"""Compatibility imports for the monitoring toolkit."""

import sys
from .. import monitoring_execution_handler

sys.modules[__name__] = monitoring_execution_handler
