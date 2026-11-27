"""PostgreSQL access point for CVA reservoir and turbine observations."""

import pandas as pd

from .access_point_vda import read_rows, timeseries_query


PARAMETER_IDS = {'TURBINATE': 101, 'LIVELLI-DIGHE': 100}

STATION_QUERY = """\
SELECT t.station_id, t.stpr_table_id, t.station_name, t.station_fulltable,
       t.parameter_unit, s.st_info_zone, s.st_info_altitude,
       s.st_info_lat_wgs84, s.st_info_lon_wgs84
FROM metadata.view_stations_parameters t
LEFT JOIN metadata.stations_info s ON t.station_id = s.station_id
WHERE t.station_schema = %s AND t.param_id = %s
ORDER BY t.station_id
"""

COLUMNS = [
    'station_id', 'station_name', 'latitude', 'longitude', 'altitude',
    'alert_area', 'time', 'value', 'unit', 'variable',
]


def get_data(source, configuration, time_start, time_end):
    """Read inclusive hourly means for a CVA variable, leaving missing values empty."""
    variable = configuration.get('variable')
    if variable not in PARAMETER_IDS:
        raise ValueError(f'Unsupported CVA variable {variable!r}. Choose TURBINATE or LIVELLI-DIGHE.')

    start = pd.Timestamp(time_start).tz_localize(None)
    end = pd.Timestamp(time_end).tz_localize(None)
    if start != start.floor('h') or end != end.floor('h') or start > end:
        raise ValueError('CVA time bounds must be ordered and aligned to the hour.')

    limits = configuration.get('geolimits', [None] * 4)
    if not isinstance(limits, (list, tuple)) or len(limits) != 4:
        raise ValueError('CVA geolimits must contain [lat_min, lat_max, lon_min, lon_max].')
    lat_min, lat_max, lon_min, lon_max = limits

    rows = []
    with source.connect() as connection, connection.cursor() as cursor:
        stations = read_rows(cursor, STATION_QUERY, ('tables_cva', PARAMETER_IDS[variable]))
        for station_id, table_id, name, table, unit, zone, altitude, lat, lon in stations:
            if not _within_bounds(lat, lat_min, lat_max) or not _within_bounds(lon, lon_min, lon_max):
                continue

            query = timeseries_query(table, instantaneous=False, treatment='avg', quality=1)
            records = read_rows(cursor, query, (start.to_pydatetime(), end.to_pydatetime(), table_id))
            for timestamp, value in records:
                rows.append((station_id, name, lat, lon, altitude, zone, timestamp,
                             float(value) if value is not None else None, unit, variable))

    return pd.DataFrame(rows, columns=COLUMNS)


def _within_bounds(coordinate, lower, upper):
    if lower is None and upper is None:
        return True
    return coordinate is not None and (lower is None or lower <= coordinate) and (upper is None or coordinate <= upper)
