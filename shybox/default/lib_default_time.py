"""
Library Features:

Name:          lib_default_time
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20240808'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import pandas as pd
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# time information
time_type = 'GMT'  # 'GMT', 'local'
time_units = 'days since 1858-11-17 00:00:00'
time_calendar = 'gregorian'
time_format_datasets = "%Y%m%d%H%M"
time_format_algorithm = '%Y-%m-%d %H:%M'
time_machine = pd.Timestamp.now
# ----------------------------------------------------------------------------------------------------------------------
