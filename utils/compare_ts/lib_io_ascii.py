"""
Library Features:

Name:           lib_io_ascii
Author(s):      Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:           '20260626'
Version:        '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# helper to read hydrograph txt
def read_hydrograph_ascii(txt_file, nodata=-9998.0):
    """
    Parse hydrograph TXT file into a pandas DataFrame.

    Header example:
      Procedure=ECMWF_Probabilistic
      DateMeteoModel=202603310000
      DateStart=202603310000
      Temp.Resolution=60
      SscenariosNumber=16

    First data row  = observed
    Other data rows = scenarios
    """

    attrs = {}
    series_rows = []

    with open(txt_file, "r") as fp:
        for line in fp:
            line = line.strip()

            if not line:
                continue

            if "=" in line:
                key, value = line.split("=", 1)
                attrs[key.strip()] = value.strip()
            else:
                values = [float(v) for v in line.split()]
                series_rows.append(values)

    procedure = attrs.get("Procedure")
    date_start = attrs.get("DateStart")
    time_resolution = int(attrs.get("Temp.Resolution"))
    scenarios_number = int(attrs.get("SscenariosNumber"))

    time_start = datetime.strptime(date_start, "%Y%m%d%H%M")

    n_steps = max(len(row) for row in series_rows)

    time_index = [
        time_start + timedelta(minutes=time_resolution * i)
        for i in range(n_steps)
    ]

    data = {}

    for row_id, row in enumerate(series_rows):
        values = np.array(row, dtype=float)
        values[values == nodata] = np.nan

        if len(values) < n_steps:
            values = np.pad(
                values,
                (0, n_steps - len(values)),
                constant_values=np.nan
            )

        if row_id == 0:
            name = "observed"
        else:
            name = f"scenario_{row_id:02d}"

        data[name] = values

    df = pd.DataFrame(data, index=pd.DatetimeIndex(time_index, name="time"))

    df.attrs = {
        "procedure": procedure,
        "date_meteo_model": attrs.get("DateMeteoModel"),
        "date_start": date_start,
        "time_start": time_start.strftime("%Y-%m-%d %H:%M:%S"),
        "time_resolution_minutes": time_resolution,
        "scenarios_number": scenarios_number,
        "nodata": nodata,
        "source_file": str(txt_file),
    }

    return df
# ----------------------------------------------------------------------------------------------------------------------
