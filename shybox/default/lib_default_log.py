"""
Library Features:

Name:          lib_default_log
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20251113'
Version:       '1.1.0'
"""
# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import os
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# log information
log_name = 'shybox'
log_file = 'shybox.log'
log_folder = None
log_handler = ['stream']  # ['file', 'stream']
log_format = \
    '%(asctime)s %(name)-12s %(levelname)-8s ' \
    '%(message)-80s %(filename)-20s:[%(lineno)-6s - %(funcName)-20s()] '
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to initialize and return logger by default
def logger_default(module_name: str) -> logging.Logger:
    """
    Initialize logging based on the base configuration and
    return a logger named after the module using it.
    """

    # -------- Build handlers list ----------
    handlers = []

    # File handler
    if 'file' in log_handler:
        logfile_path = log_file if log_folder is None else os.path.join(log_folder, log_file)
        fh = logging.FileHandler(logfile_path)
        fh.setFormatter(logging.Formatter(log_format))
        handlers.append(fh)

    # Stream handler
    if 'stream' in log_handler:
        sh = logging.StreamHandler()
        sh.setFormatter(logging.Formatter(log_format))
        handlers.append(sh)

    # Configure root logging
    logging.basicConfig(
        level=logging.INFO,
        handlers=handlers,
        format=log_format,
        force=True  # avoids duplicate handler issue
    )

    # Return logger for caller
    return logging.getLogger(module_name)
# ----------------------------------------------------------------------------------------------------------------------
