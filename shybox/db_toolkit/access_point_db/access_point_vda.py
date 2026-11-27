"""
Library Features:

Name:          access_point_vda
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20261125'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
from collections.abc import Mapping
from math import isfinite
from pathlib import Path
import pandas as pd

from shybox.db_toolkit import lib_db_io
from shybox.db_toolkit.db_handler_base import write_csv_default

# common decorators
from shybox import with_logger
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# declare networks
NETWORKS = {
    'cf-vda': 'tbls_cae', 'arpa-vda': 'tbls_arpa',
    'piemonte': 'tbls_tpi', 'switzerland': 'tbls_mss', 'france': 'tbls_mfr'
}
# declare variables
VARIABLES = {
    'rain': ([7, 80], 'sum', 'event_stations_rain', False),
    'temperature': ([1], 'avg', 'event_stations_thermo', False),
    'humidity': ([2], 'avg', None, False),
    'radiation': ([5], 'avg', None, False),
    'wind': ([10], 'avg', None, False),
    'snow': ([8], None, 'event_stations_snow', True),
    'discharge': ([30], 'avg', 'event_stations_level', False),
    'soilsat_10': ([230], None, None, True),
    'soilsat_20': ([231], None, None, True),
    'soilsat_40': ([232], None, None, True),
}
# declare ALIASES for VARIABLES
ALIASES = {'p': 'rain', 'r': 'radiation', 't': 'temperature', 'v': 'wind',
           'u': 'humidity', 'gg': 'snow', 'us10': 'soilsat_10',
           'us20': 'soilsat_20', 'us40': 'soilsat_40'}

OBSERVATION_COLUMNS = [
    'station_id', 'station_name', 'latitude', 'longitude', 'altitude',
    'is_ott', 'is_heated', 'network', 'alert_area', 'time', 'value',
    'unit', 'post_validity_code', 'variable',
]
CSV_LEGACY_COLUMNS = ['station', 'longitude', 'latitude', 'code', 'value', 'time']
CSV_COLUMNS = CSV_LEGACY_COLUMNS + [name for name in OBSERVATION_COLUMNS if name not in CSV_LEGACY_COLUMNS]
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# SQL templates and query builders
STATION_METADATA = """\
SELECT t.station_id, t.param_id, t.stpr_table_id, t.station_name,
       t.station_fulltable, t.parameter_unit, t.station_param_pluvio_heated,
       s.st_info_zone, s.st_info_altitude, s.st_info_lat_wgs84,
       s.st_info_lon_wgs84
FROM {source}
LEFT JOIN metadata.stations_info s ON t.station_id = s.station_id
WHERE t.station_schema = %s AND t.param_id = ANY(%s)
  AND t.station_active = TRUE {geo_condition}
ORDER BY t.station_id
"""

HOURLY_AGGREGATED = """\
SELECT h.datetime,
       round({agg}(CASE WHEN {valid} THEN t.meanvalue::numeric END), 2)
         AS measure_value
FROM generate_series(%s::timestamp, %s::timestamp, interval '1 hour') h(datetime)
LEFT JOIN {table} t ON t.id = %s
     AND t.fulldate >= h.datetime AND t.fulldate < h.datetime + interval '1 hour'
     AND (extract(minute FROM t.fulldate)::int %% 10) = 0
     AND extract(second FROM t.fulldate) = 0
GROUP BY h.datetime ORDER BY h.datetime
"""

HOURLY_INSTANTANEOUS = """\
SELECT h.datetime, t.post_validity_code,
       CASE WHEN {valid} THEN t.meanvalue::numeric END AS measure_value
FROM generate_series(%s::timestamp, %s::timestamp, interval '1 hour') h(datetime)
LEFT JOIN {table} t ON t.fulldate = h.datetime AND t.id = %s
ORDER BY h.datetime
"""

# method to set query station
def station_query(geosup_table, use_geo):
    """Return station metadata with an optional Geosupervisor filter."""
    from psycopg import sql
    if use_geo:
        source = sql.SQL('tool_geo_supervis.{} g JOIN metadata.view_stations_parameters t ON g.st_id = t.station_id').format(
            sql.Identifier(geosup_table))
        geo_condition = sql.SQL(' AND g.ev_id = 2')
    else:
        source = sql.SQL('metadata.view_stations_parameters t')
        geo_condition = sql.SQL('')
    return sql.SQL(STATION_METADATA).format(source=source, geo_condition=geo_condition)

# method to set query time-series
def timeseries_query(table_name, instantaneous, treatment, quality):
    from psycopg import sql

    # Quote identifiers returned by the station metadata query.
    parts = table_name.split('.')
    if len(parts) != 2 or not all(parts):
        raise ValueError(f'Unexpected station_fulltable identifier: {table_name!r}')
    table = sql.Identifier(*parts)
    valid = sql.SQL('t.post_validity_code >= -1')
    if quality == 2:
        valid += sql.SQL(' AND t.post_validity_code < 4')
    if instantaneous:
        valid = sql.SQL('(') + valid + sql.SQL(' OR t.post_validity_code IN (-64, -65))')
        return sql.SQL(HOURLY_INSTANTANEOUS).format(valid=valid, table=table)
    agg = sql.SQL('sum') if treatment == 'sum' else sql.SQL('avg')
    return sql.SQL(HOURLY_AGGREGATED).format(agg=agg, valid=valid, table=table)
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to validate selection
@with_logger(var_name='logger_stream')
def validate_selection(job, time_start, time_end):

    # check variable name or alias
    variable = ALIASES.get(job['variable'].lower(), job['variable'].lower())
    if variable not in VARIABLES:
        logger_stream.error(f"Variable {variable} is not expected in {VARIABLES}")
        raise ValueError(f'Unsupported variable: {variable}')

    # check networks
    networks = job['networks']
    if isinstance(networks, str):
        networks = [networks]
    networks = [n.lower() for n in networks]
    if not networks or any(n not in NETWORKS for n in networks):
        logger_stream.error(f"DB network {networks} is not supported")
        raise ValueError(f'Invalid networks: {networks}')

    # check time_start and time_end
    if time_start > time_end:
        logger_stream.error(f'Invalid time range: {time_start} to {time_end}')
        raise ValueError('time_start must not exceed time_end')
    if any(value != value.floor('h') for value in (time_start, time_end)):
        logger_stream.error(f'Invalid hour-aligned {time_start} or {time_end}')
        raise ValueError('time_start/time_end must be hour-aligned (minutes 00)')

    # check quality level
    quality = job.get('quality_level', 1)
    if quality not in (1, 2):
        logger_stream.error(f'Invalid quality level {quality}')
        raise ValueError('quality_level must be 1 or 2')

    # check geo limits
    limits = job.get('geolimits', [None] * 4)
    if not isinstance(limits, list) or len(limits) != 4:
        logger_stream.error('Invalid geolimits values format')
        raise ValueError('geolimits must be [latmin, latmax, lonmin, lonmax]')
    if any(v is not None and not isinstance(v, (float, int)) for v in limits):
        logger_stream.error('Invalid geolimits values type')
        raise ValueError('geolimits entries must be numbers or null')
    if limits[0] is not None and limits[1] is not None and limits[0] > limits[1]:
        logger_stream.error(f'Invalid geolimits latitude values {limits[0]} > {limits[1]}')
        raise ValueError('Invalid latitude bounds')
    if limits[2] is not None and limits[3] is not None and limits[2] > limits[3]:
        logger_stream.error(f'Invalid geolimits longitude values {limits[2]} > {limits[3]}')
        raise ValueError('Invalid longitude bounds')

    return variable, networks, quality, limits
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to get data
@with_logger(var_name='logger_stream')
def get_data(source, configuration, time_start, time_end):

    # get time_start and time_end
    time_start, time_end = pd.Timestamp(time_start).tz_localize(None), pd.Timestamp(time_end).tz_localize(None)

    # validate configuration and times
    variable, networks, quality, limits = validate_selection(configuration, time_start, time_end)
    # get variables settings
    params, treatment, geo_table, instantaneous = VARIABLES[variable]

    # enable geo supervisor
    geo_enabled = bool(configuration.get('geosupervisor', False)) and geo_table is not None
    if configuration.get('geosupervisor') and not geo_enabled:
        logger_stream.warning('Geosupervisor not applicable to %s; disabled', variable)

    # parse time_start and time_end
    time_start, time_end = time_start.to_pydatetime(), time_end.to_pydatetime()

    # compose data from the database extraction
    rows = []
    with source.connect() as connection:
        with connection.cursor() as cur:

            # iterate over networks
            for network in networks:

                # info message start
                logger_stream.info_up(f'Get from "{network}" -- "{variable}" ... ')

                # get stations
                stations = read_rows(
                    cur, station_query(geo_table, geo_enabled), (NETWORKS[network], params),
                )
                # iterate over station(s)
                for station in stations:
                    # extract information
                    (station_id, param_id, table_id, station_name, table_name, unit,
                     heated, zone, altitude, lat, lon) = station
                    if any(v is not None and (coord is None or
                           (coord < v if is_min else coord > v))
                           for v, coord, is_min in zip(limits, (lat, lat, lon, lon),
                                                        (True, False, True, False))):
                        continue

                    # info station status
                    logger_stream.info_status(
                        f'Station: ID "{station_id}" -- Name "{station_name}" -- Params "{param_id}"')

                    # get records
                    records = read_rows(
                        cur, timeseries_query(table_name, instantaneous, treatment, quality),
                        (time_start, time_end, table_id),
                    )

                    # iterate over found records
                    for record in records:
                        # get time
                        dt = record[0]
                        # get code and value
                        code, value = (record[1], record[2]) if instantaneous else (None, record[1])
                        # MATLAB Drops uses zero when an observation has no returned code.
                        if code is None or not isfinite(code):
                            code = 0

                        # append to the row
                        rows.append(dict(station_id=station_id, station_name=station_name,
                                         latitude=lat, longitude=lon, altitude=altitude,
                                         is_ott=param_id == 80, is_heated=heated,
                                         network=network, alert_area=zone,
                                         time=dt, value=float(value) if value is not None else None,
                                         unit=unit, post_validity_code=code,
                                         variable=variable))

                # info message status
                logger_stream.info_status(f'Network: "{network}" Records: "{len(stations)}"')

                # info message end
                logger_stream.info_down(f'Get from "{network}" -- "{variable}" ... DONE')

    return pd.DataFrame(rows, columns=OBSERVATION_COLUMNS)

# helper to read rows by cursor
def read_rows(cursor, query, parameters):
    cursor.execute(query, parameters)
    return cursor.fetchall()
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to configure writer for vda case
def write_csv_vda(data, configuration: Mapping, *, variable: str, logger):

    # Preserve the original CSV columns and include the full provider metadata.
    observations = data.reindex(columns=OBSERVATION_COLUMNS)
    observations = observations.assign(station=observations['station_name'], code=observations['post_validity_code'])
    # get options and path (folder and file_name)
    options = configuration.get('options', configuration.get('csv', {}))
    path = Path(configuration['path']) / configuration['file_name']

    # call default writer
    return write_csv_default(path, observations[CSV_COLUMNS], options=options, variable=variable, logger=logger)

# method to overwrite the write_data
def write_data(data, configuration: Mapping, *, variable: str = 'observations', logger=None):
    # declare writers
    writers = {'csv': write_csv_vda}
    # call write_data using WRITERS
    return lib_db_io.write_data(data, configuration, writers=writers, variable=variable, logger=logger)
# ----------------------------------------------------------------------------------------------------------------------
