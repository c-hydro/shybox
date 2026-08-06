"""
Library Features:

Name:          lib_default_time
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20261020'
Version:       '1.1.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import pandas as pd
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# definition of time attributes
time_type = 'GMT'  # 'GMT', 'local'
time_units = 'days since 1858-11-17 00:00:00'
time_calendar = 'gregorian'
time_format_datasets = "%Y%m%d%H%M"
time_format_algorithm = '%Y-%m-%d %H:%M'
time_machine = pd.Timestamp.now

# definition of time object
time_obj = {
    'time_type': 'GMT',  # 'GMT', 'local'
    'time_units': time_units,
    'time_calendar': time_calendar
}
# ----------------------------------------------------------------------------------------------------------------------
