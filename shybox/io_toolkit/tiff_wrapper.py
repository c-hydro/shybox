"""
Library Features:

Name:          tiff_wrapper
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20260824'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# library
from shybox.io_toolkit.lib_io_tiff import write_tiff_base
from shybox.io_toolkit.tiff_adapter import adapt_watershed_to_tiff
from shybox.io_toolkit.lib_io_decoretors import iterate_obj, adapt_obj, compose_decorators
from shybox.logging_toolkit.lib_logging_utils import with_logger

# example of logger decoretor @with_logger(var_name='logger_stream')
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# TIFF writer - watershed
write_tiff_watershed = compose_decorators(
    write_tiff_base,
    iterate_obj,
    adapt_obj(adapt_watershed_to_tiff),
)
# ----------------------------------------------------------------------------------------------------------------------
