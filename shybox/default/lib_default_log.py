"""
Library Features:

Name:          lib_default_log
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20250207'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# log information
log_name = 'shybox'
log_file = 'log.txt'
log_folder = None
log_handler = ['file', 'stream']  # 'file' and 'stream'
log_format = '%(asctime)s %(name)-12s %(levelname)-8s ' \
             '%(message)-80s %(filename)-20s:[%(lineno)-6s - %(funcName)-20s()] '
# ----------------------------------------------------------------------------------------------------------------------
