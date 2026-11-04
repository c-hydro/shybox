"""
Library Features:

Name:          nc_wrapper
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20261104'
Version:       '1.0.0'
"""
# ----------------------------------------------------------------------------------------------------------------------
# library
import os
from pathlib import Path
import inspect

import numpy as np
import xarray as xr

from shybox.external_toolkit import check_external_toolkit

# common decorators
from shybox import with_logger
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
## HELPERS
# helper to import RainFARM method to dump result
@with_logger(var_name='logger_stream')
def _load_rfarm_result():

    # check RainFARM in the external toolkits
    check_external_toolkit('rfarm', auto_install=False, raise_error=True)
    try:
        from rfarm.driver.drv_model_io import RFarmResult
    except ModuleNotFoundError as exc:

        raise ImportError(
            f'RFarm requires unavailable dependency "{exc.name}". '
            'Install it in the active Python environment before running RainFARM.'
        ) from exc

    logger_stream.info(f'RainFARM model source: {inspect.getfile(RFarmResult)}')

    return RFarmResult

# helper to read members manifest
@with_logger(var_name='logger_stream')
def _ensemble_pickles(obj: dict):

    # check obj
    if not isinstance(obj, dict):
        logger_stream.error('RainFARM pickle manifest files is not in the right format.')
        raise TypeError('RainFARM NetCDF output requires an ensemble-to-pickle mapping.')
    # check number of entries
    entries = {key: value for key, value in obj.items() if key != 'attrs'}
    if len(entries) == 1 and isinstance(next(iter(entries.values())), dict):
        return _ensemble_pickles(next(iter(entries.values())))
    if not entries:
        logger_stream.error('RainFARM entries are empty or not found in the pickle manifest files.')
        raise ValueError('RainFARM ensemble mapping is empty.')

    # iterate over members
    members = {}
    for key, value in entries.items():
        if not str(key).isdigit() or int(key) < 1:
            logger_stream.error('RainFARM pickle manifest files identifier must be an integer.')
            raise ValueError(f'Invalid RainFARM ensemble identifier: {key!r}')

        member = int(key)
        if member in members:
            logger_stream.error('RainFARM pickle manifest member is not unique.')
            raise ValueError(f'Duplicate RainFARM ensemble identifier: {member}')
        if not isinstance(value, (str, Path)):
            logger_stream.error('RainFARM pickle manifest path does not exist.')
            raise TypeError(f'Ensemble {member} must reference a local pickle file.')

        path = Path(value)

        if path.suffix.lower() not in ('.pkl', '.pickle'):
            logger_stream.error('RainFARM pickle manifest file extension is not supported.')
            raise ValueError(f'RainFARM native writer requires pickle inputs: {path}')
        if not path.is_file():
            logger_stream.error('RainFARM pickle manifest file does not exist.')
            raise FileNotFoundError(f'RainFARM ensemble pickle does not exist: {path}')
        members[member] = str(path)

    return dict(sorted(members.items()))

# helper to get reference grid
@with_logger(var_name='logger_stream')
def _reference_grid(ref: xr.DataArray, terrain_name):

    # check reference grid format and dimensions
    if not isinstance(ref, xr.DataArray) or ref.ndim != 2:
        logger_stream.error('Reference object is not in the expected format')
        raise TypeError('RainFARM NetCDF output requires a two-dimensional terrain DataArray.')
    if 'longitude' not in ref.coords or 'latitude' not in ref.coords:
        logger_stream.error('Reference object has not the requested geographical dimensions')
        raise ValueError('Reference terrain requires longitude and latitude coordinates.')

    # get longitude, latitude and calues
    lats, lons = xr.broadcast(ref.latitude, ref.longitude)
    lons = lons.transpose(*ref.dims).values
    lats = lats.transpose(*ref.dims).values
    # get values
    values = ref.values

    # check longitude and latitude
    if not np.isfinite(lons).all() or not np.isfinite(lats).all():
        logger_stream.error('Reference coordinatas has not the expected format')
        raise ValueError('Reference coordinates must be finite.')

    return values, lons, lats

## WRITERS
# method to write netcdf ensemble using the RainFARM functions
@with_logger(var_name='logger_stream')
def write_nc_rfarm_ensemble(obj, file_path, ref, *, var_name='Rain', var_freq='h',
                            var_attrs=None, terrain_name='Terrain', compression_level=0,
                            write_engine='netcdf4', remove_pickle: bool = True, **kwargs):

    # get members file
    member_pickles = _ensemble_pickles(obj)
    # parse file_path
    file_template = str(file_path)
    if '{ensemble}' not in file_template:
        raise ValueError('RainFARM output file_path must contain {ensemble} to keep members separate.')
    # define file outputs
    file_outputs = {member: Path(file_template.replace('{ensemble}',
                                                       f'{member:03d}')) for member in member_pickles}

    # check file outputs
    if any('{' in str(path) or '}' in str(path) for path in file_outputs.values()):
        raise ValueError('Resolve time and domain placeholders before writing RainFARM ensemble outputs.')
    if any(path.resolve() == Path(member_pickles[member]).resolve() for member, path in file_outputs.items()):
        raise ValueError('RainFARM output must not overwrite its source pickle.')

    # get reference domain
    terrain, lons, lats = _reference_grid(ref, terrain_name)

    # define attributes
    attrs = {'units': 'mm', 'Missing_value': -9999.0}
    if var_attrs is not None:
        attrs.update(var_attrs)

    # get result method
    writer_class = _load_rfarm_result()

    # iterate over member data
    member_written = {}
    for member_id, member_path_pickle in member_pickles.items():

        # get file output
        member_path_output = file_outputs[member_id]

        # info start
        logger_stream.info(f'Write RainFARM ensemble {member_id:03d} to {member_path_output} ...')

        # method to initialize class result
        writer_handle = writer_class(
            ensemble_group_in=[member_path_pickle], ensemble_n={'start': member_id, 'end': member_id},
            folder_out=str(member_path_output.parent), filename_out=member_path_output.name,
            var_name=var_name, var_attrs=attrs.copy(), var_freq=var_freq,
            file_format='netcdf', write_engine=write_engine,
        )

        # method to write ensemble
        writer_handle.organize_result([member_path_pickle], terrain, lons, lats,
                               terrain_name=terrain_name, compression_level=compression_level)

        # check if file is saved or not
        if not path_output.is_file():
            raise RuntimeError(f'RainFARM did not create the expected NetCDF file: {member_path_output}')

        # manifest of file written
        member_written[f'{member_id:03d}'] = str(path_output)

        # remove pickle temporary file
        if os.path.exists(member_path_pickle):
            if remove_pickle:
                os.remove(member_path_pickle)

        # remove write handle
        del writer_handle

        # info end
        logger_stream.info(f'Write RainFARM ensemble {member_id:03d} ... DONE')

    return member_written
# ----------------------------------------------------------------------------------------------------------------------
