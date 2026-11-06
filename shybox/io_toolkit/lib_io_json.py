"""
Library Features:

Name:          lib_io_json
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20261106'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
from contextlib import contextmanager
import json
import os
from pathlib import Path
import tempfile

import numpy as np
import pandas as pd
# ----------------------------------------------------------------------------------------------------------------------

# Output field -> registry fields, in order of preference.
SECTION_MAP = {
    'section_baseflow': ('section_baseflow',),
    'section_code': ('section_code',),
    'section_discharge_thr_alarm': ('discharge_orange', 'threshold_level_2'),
    'section_discharge_thr_alert': ('discharge_yellow', 'threshold_level_1'),
    'section_domain': ('basin', 'catchment_name'),
    'section_drained_area': ('catchment_area_km2',),
    'section_id': ('id',),
    'section_idx_i': ('hmc_x',),
    'section_idx_j': ('hmc_y',),
    'section_name': ('section_name',),
    'section_reference': ('domain_name',),
    'section_longitude': ('longitude',),
    'section_latitude': ('latitude',),
}
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# helper to parse scalar to string
def _scalar_string(value, no_data_value):
    if not np.isscalar(value) and value is not None and value is not pd.NA:
        raise TypeError('JSON metadata values must be scalars.')
    if pd.isna(value) or (isinstance(value, (float, np.floating)) and not np.isfinite(value)):
        return str(float(no_data_value))
    return str(value)

# helper to parse time-stamp to string
def _timestamp_string(value, time_format):
    # Accept the singleton time containers used by SHYBOX output signatures.
    while isinstance(value, (list, tuple, pd.DatetimeIndex, np.ndarray)):
        if len(value) != 1:
            raise ValueError('Run timestamps must contain exactly one value.')
        value = value[0]
    if isinstance(value, str) and len(value) == 12 and value.isdigit():
        value = pd.to_datetime(value, format='%Y%m%d%H%M')
    value = pd.Timestamp(value)
    if pd.isna(value):
        raise ValueError('Run timestamps cannot be missing.')
    return value.strftime(time_format)

# helper to default mapping
def _default_mapping(columns):

    # default aliases
    aliases = {
        'discharge_simulated_analysis': ('discharge_sim_analysis', 'discharge_simulated_analysis'),
        'discharge_simulated_openloop': ('discharge_sim_openloop', 'discharge_simulated_openloop'),
        'discharge_simulated': ('discharge_simulated',),
        'discharge_observed': ('discharge_obs', 'discharge_observed'),
        'air_temperature_observed': ('air_temperature_obs', 'air_temperature_observed'),
        'rain_observed': ('rain_obs', 'rain_observed'),
        'soil_moisture_simulated': ('soil_moisture_sim', 'soil_moisture_simulated'),
    }

    # mapping using default prefix time_series
    mapping = {name: 'time_series_' + str(name) for name in columns}
    analysis = any(name in columns for name in aliases['discharge_simulated_analysis'])
    for target, sources in aliases.items():
        if target == 'discharge_simulated_analysis' or (target == 'discharge_simulated_openloop' and not analysis):
            target = 'discharge_simulated'
        for source in sources:
            if source in columns:
                mapping[source] = 'time_series_' + target
    return mapping

# method to create json time series
def create_json_time_series(data, *, section_metadata=None, run_metadata=None,
                     section_map=None, var_mapping=None, time=None,
                     time_format='%Y-%m-%d %H:%M', run_time_format='%Y-%m-%d %H:%M',
                     data_no_value=-9999.0, decimal_places=None):

    # check data
    if not isinstance(data, pd.DataFrame) or data.empty:
        raise ValueError('JSON time series requires a nonempty section DataFrame.')
    if not isinstance(data.index, pd.DatetimeIndex) or data.index.hasnans:
        raise ValueError('JSON time series requires a valid DatetimeIndex.')
    if not data.index.is_monotonic_increasing or data.index.has_duplicates:
        raise ValueError('JSON time series timestamps must be sorted and unique.')
    if isinstance(data.columns, pd.MultiIndex) or not data.columns.is_unique:
        raise ValueError('Section DataFrame must have unique, single-level variable columns.')
    if not np.isfinite(data_no_value):
        raise ValueError('data_no_value must be finite.')
    if decimal_places is not None and (type(decimal_places) is not int or decimal_places < 0):
        raise ValueError('decimal_places must be a nonnegative integer or None.')

    # get sections info
    section = dict(section_metadata or {})
    # define fields
    fields = {**SECTION_MAP, **(section_map or {})}

    # initialize result object
    result = {}
    for target, sources in fields.items():
        sources = (sources,) if isinstance(sources, str) else sources
        for source in sources:
            if source in section:
                result[target] = _scalar_string(section[source], data_no_value)
                break
    # special case for i,j continuum
    if 'section_idx_i' in result and 'section_idx_j' in result:
        result['section_idx_ji'] = result['section_idx_j'] + ',' + result['section_idx_i']
    # convert to the time and scalar to string
    for name, value in dict(run_metadata or {}).items():
        if name not in {'run_domain', 'run_mode', 'run_name', 'run_var',
                        'time_run', 'time_restart', 'time_start'}:
            raise ValueError(f'Unsupported run metadata field: {name}')
        result[name] = (_timestamp_string(value, run_time_format) if name.startswith('time_')
                        else _scalar_string(value, data_no_value))

    # add time_run to the result object
    if 'time_run' not in result and time is not None:
        result['time_run'] = _timestamp_string(time, run_time_format)

    # manage time(s)
    times = data.index.strftime(time_format).tolist()
    if len(set(times)) != len(times):
        raise ValueError('time_format would collapse distinct timestamps.')
    result['time_period'] = ','.join(times)

    # map variable to the output name(s)
    if var_mapping is not None:
        mapping = {}
        for source, target in (var_mapping or {}).items():
            # The columns may already use output names after earlier processing.
            if target in data.columns:
                mapping[target] = target
            elif source in data.columns:
                mapping[source] = target
    else:
        mapping = _default_mapping(data.columns)

    # check mapping (if not defined exit)
    if not mapping:
        raise ValueError('At least one time-series variable must be selected.')

    # iterate over source and target and convert to the output name(s)
    targets = set()
    for source, target in mapping.items():

        # check source
        if source not in data:
            raise ValueError(f'Time-series variable not found: {source}')
        # check target
        if not isinstance(target, str) or not target:
            raise ValueError('JSON variable field names must be nonempty strings.')
        target = target if target.startswith('time_series_') else 'time_series_' + target
        if target in targets:
            raise ValueError(f'Multiple variables map to JSON field {target}; provide a unique var_mapping.')
        targets.add(target)

        # convert to numeric the time-series
        values = pd.to_numeric(data[source], errors='raise').to_numpy(dtype=float, na_value=np.nan, copy=True)
        # find the finite numeric
        finite = np.isfinite(values)
        # convert to the right decimal places
        if decimal_places is not None:
            values[finite] = np.round(values[finite], decimal_places)
        # set no value for not-finite values
        values[~finite] = data_no_value

        # manage to result object
        result[target] = ','.join(str(float(value)) for value in values)

    return result
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# helper to write JSON without risking a partially written destination file
@contextmanager
def _atomic_json_file(file_name):
    path = Path(file_name)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(
                mode='w', encoding='utf-8', dir=path.parent, prefix='.' + path.name + '.',
                suffix='.tmp', delete=False) as stream:
            temporary = Path(stream.name)
            yield stream
        os.replace(temporary, path)
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink()

# method to dump each section to json file
def write_json_time_series_by_section(file_name, data, **kwargs):
    """Write one section atomically, preserving the previous file on failure."""
    payload = create_json_time_series(data, **kwargs)
    with _atomic_json_file(file_name) as stream:
        json.dump(payload, stream, indent=4, ensure_ascii=False, allow_nan=False)
        stream.write('\n')
    return str(file_name)

# method to dump all section to json file
def write_json_time_series_by_collection(file_name, records):
    with _atomic_json_file(file_name) as stream:
        stream.write('[\n')
        for index, record in enumerate(records):
            if index:
                stream.write(',\n')
            json.dump(record, stream, indent=4, ensure_ascii=False, allow_nan=False)
        stream.write('\n]\n')
    return str(file_name)
# ----------------------------------------------------------------------------------------------------------------------
