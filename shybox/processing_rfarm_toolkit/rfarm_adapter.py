"""
Library Features:

Name:          rfarm_adapter
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20261103'
Version:       '1.1.0'
"""
# ----------------------------------------------------------------------------------------------------------------------
# libraries
import numpy as np
import pandas as pd
import xarray as xr

import tempfile
import inspect
from pathlib import Path
from string import Formatter

from shybox.dataset_toolkit.dataset_handler_temporary import DataTemporary
from shybox.external_toolkit import check_external_toolkit

# common decorators
from shybox import with_logger, as_process
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# helper to disaggregate datasets using RainFARM
@as_process(input_type='xarray', input_adapter=None,
            args_adapter=None, output_adapter=None, output_type='file')
@with_logger(var_name='logger_stream')
def exec_rfarm_nwp(
        data: xr.DataArray, ref: xr.DataArray = None,
        debug_data: bool = False, debug_results: bool = False,
        ensemble: dict = None, ratio_s: int = 4, ratio_t: int = 1,
        slope_s: float = None, slope_t: float = None,
        cs_sf: int = 2, ct_sf: int = 1,
        multi_core: bool = False, domain_extension: float = 0,
        folder_tmp: str = None, filename_tmp: str = 'rf_{ensemble}.pkl',
        tmp: bool = True, rain_max_thr: float = 180,
        negative_tolerance: float = 0.01, nodata_value: float = -9999,
        tmp_data: DataTemporary = None,
        ancillary_data: DataTemporary = None,
        **kwargs):

    """Run RFarm on interval rainfall in mm and return an ancillary file manifest.

    The result is ``{'rain': {'001': '/.../rf_001.pkl', ...}}``. Each
    ancillary file contains one xarray DataArray; consumers must load,
    process and write one member at a time. The existing array-only masking
    and output steps need a file-consumption step before using this result.

    Input/reference coordinates must describe regular longitude/latitude
    grids. Input times label interval ends and must be evenly spaced.
    Files are kept for the caller to clean up after successful conversion.
    A unique run directory prevents concurrent forecasts overwriting members.
    Declared missing values and nodata_value become NaN. Negative rainfall
    within negative_tolerance (mm) is clipped to zero with a warning; larger
    negatives remain errors. The default tolerance matches RFarm's 0.01 mm
    lower input threshold.
    When provided, ancillary_data defines the folder, filename and writer type.
    RFarm writes native pickles; NetCDF/Zarr output is converted one member at
    a time after execution. Native pickles remain available for caller cleanup.
    tmp_data remains an alias for existing callers. Otherwise folder_tmp/
    filename_tmp (or the orchestrator tmp_dir) define the storage.
    """

    # Info method start
    logger_stream.info_up('Execute RainFARM NWP ...')

    # Check input data
    if data is None:
        logger_stream.error('RainFARM input data are not defined. Disaggregation cannot proceed.')
        raise ValueError('RainFARM input data are None.')

    # Check input data type
    if not isinstance(data, xr.DataArray):
        logger_stream.error('RainFARM input data format is not expected. Disaggregation cannot proceed.')
        raise TypeError(f'RainFARM input must be an xarray DataArray. Got {type(data).__name__}.')

    # Check if input data are empty
    if data.size == 0:
        logger_stream.warning('RainFARM input DataArray is empty. Disaggregation skipped.')
        logger_stream.info_down('Execute RainFARM NWP ... SKIPPED')
        return None

    # Check if all input values are missing
    if bool(data.isnull().all().item()):
        logger_stream.warning('RainFARM input DataArray contains only NaN values. Disaggregation skipped.')
        logger_stream.info_down('Execute RainFARM NWP ... SKIPPED')
        return None

    # check reference type and dimension(s)
    if not isinstance(ref, xr.DataArray) or ref.ndim != 2:
        raise TypeError('RainFARM requires a two-dimensional reference DataArray.')
    # check data dimensions and time coords
    if data.ndim != 3 or 'time' not in data.dims:
        raise ValueError('RainFARM rainfall must have two spatial dimensions and a time dimension.')
    # set tmp by default to avoid RAM consumption
    if not tmp:
        raise ValueError('RainFARM adapter requires tmp=True for file-backed ensemble output.')
    # disable multi-core options
    if multi_core:
        raise NotImplementedError('The RainFARM model currently supports only single-core execution.')

    # check rfarm parameters
    for name, value in {'ratio_s': ratio_s, 'ratio_t': ratio_t, 'cs_sf': cs_sf, 'ct_sf': ct_sf}.items():
        if isinstance(value, bool) or not isinstance(value, (int, np.integer)) or value < 1:
            raise ValueError(f'{name} must be a positive integer.')
    # Configuration readers may convert JSON null to NaN. RFarm uses None
    # specifically to request its native automatic slope estimation.
    if slope_s is None or (np.isscalar(slope_s) and pd.isna(slope_s)):
        slope_s = None
    if slope_t is None or (np.isscalar(slope_t) and pd.isna(slope_t)):
        slope_t = None
    if (slope_s is None) != (slope_t is None):
        raise ValueError('Specify both slope_s and slope_t, or leave both null for automatic estimation.')
    if slope_s is not None:
        for name, value in (('slope_s', slope_s), ('slope_t', slope_t)):
            if isinstance(value, bool) or not np.isscalar(value) or not np.isfinite(value):
                raise ValueError(f'{name} must be a finite scalar.')

    # define ensemble window
    ensemble = dict(ensemble) if ensemble is not None else {'start': 1, 'end': 1}
    if set(ensemble) != {'start', 'end'} or any(
            isinstance(value, bool) or not isinstance(value, (int, np.integer)) or value < 1
            for value in ensemble.values()):
        raise ValueError('ensemble must contain positive integer start and end values.')
    if ensemble['start'] > ensemble['end']:
        raise ValueError('ensemble start must not exceed end.')

    # check ancillary data (to store rfarm ensemble to pickle files to avoid RAM consumption)
    if ancillary_data is not None:
        if tmp_data is not None and tmp_data is not ancillary_data:
            raise ValueError('Pass ancillary_data or tmp_data, not two different storage objects.')
        tmp_data = ancillary_data
    if tmp_data is None:
        # Keep the legacy system-temp fallback without creating any files yet.
        tmp_data = DataTemporary(
            folder_name=folder_tmp or kwargs.get('tmp_dir') or tempfile.gettempdir(),
            file_name=filename_tmp, prefix='rfarm_')
    elif not isinstance(tmp_data, DataTemporary):
        raise TypeError('ancillary_data/tmp_data must be a DataTemporary object.')

    # get folder and filename of ancillary data
    folder_name_anc, file_name_anc = tmp_data.folder_name, tmp_data.file_name

    # Ensemble naming is an RFarm requirement, not a generic storage rule.
    fields = [field for _, field, _, _ in Formatter().parse(tmp_data.file_name) if field is not None]
    if not fields or any(field != 'ensemble' for field in fields) or '{ensemble}' not in tmp_data.file_name:
        raise ValueError('RainFARM file_name must contain {ensemble} as its only placeholder.')

    # check domain extension
    if not np.isfinite(domain_extension) or domain_extension < 0:
        raise ValueError('domain_extension must be finite and nonnegative.')
    # check times
    times = pd.DatetimeIndex(data['time'].values)
    if times.hasnans or len(times) < 2:
        raise ValueError('RainFARM requires at least two valid timestamps.')
    deltas = np.diff(times.asi8)
    if deltas[0] <= 0 or not np.all(deltas == deltas[0]):
        raise ValueError('RainFARM timestamps must be increasing and evenly spaced.')
    step_seconds = (times[1] - times[0]).total_seconds()

    # upstream configure_time uses timedelta.seconds, which discards days.
    if step_seconds >= 86400 or step_seconds % ratio_t != 0:
        raise ValueError('Input time steps must be below one day and divisible by ratio_t in seconds.')

    # check times and the time rfarm scale
    if len(times) % ct_sf:
        raise ValueError('The number of input times must be divisible by ct_sf.')

    # prepare grid data
    data, lons_in, lats_in, _, _ = _prepare_grid(data)
    # prepare reference data
    ref, lons_ref, lats_ref, res_lon_ref, res_lat_ref = _prepare_grid(ref)
    # get spatial dimensions (longitude and latitude)
    spatial_dims = tuple(dim for dim in data.dims if dim != 'time')

    # prepare rainfall
    values = _prepare_rainfall(
        data.transpose(*spatial_dims, 'time'),
        negative_tolerance=negative_tolerance, nodata_value=nodata_value,)
    # if data are null return None (avoid to execute RainFARM)
    if np.isnan(values).all():
        logger_stream.warning('RainFARM rainfall contains only missing values after decoding no-data markers.')
        logger_stream.info_down('Execute RainFARM NWP ... SKIPPED')
        return None

    # load rfarm module
    model_class = _load_rfarm_algorithm()

    # define run instance
    tmp_run_config = tmp_data.create_run(folder=folder_name_anc, file_name=file_name_anc)
    # select run configuration (user or default)
    tmp_run_native = tmp_run_config if tmp_run_config.writer_type == 'pickle' else DataTemporary(
        folder_name=str(Path(tmp_run_config.folder_name) / 'native'), file_name='rf_{ensemble}.pkl',
        writer_type='pickle',
    )

    # initialize model
    model = model_class(
        ensemble_n=ensemble, ratio_s=ratio_s, ratio_t=ratio_t,
        slope_s=slope_s, slope_t=slope_t, cs_sf=cs_sf, ct_sf=ct_sf,
        multi_core=False, domain_extension=domain_extension,
        folder_tmp=tmp_run_native.folder_name, filename_tmp=tmp_run_native.file_name,
        rain_max_thr=rain_max_thr, model_algorithm='exec_nwp', model_var='rain',
    )
    # configure grid
    model.configure_grid(lons_in, lats_in, lons_ref, lats_ref,
                         res_lon_ref, res_lat_ref, domain_extension=domain_extension)
    # configure time
    model.configure_time(times)
    # configure data
    model.configure_data(values, info_in=None)

    # Upstream's dry branch expects arrays, although configure_data stores
    # dictionaries. Write equivalent zero members without entering that branch.
    if all(not np.any(np.nan_to_num(array, nan=0.0)) for array in model.data_rf.values()):

        # define dry time(s)
        dry_times = pd.DatetimeIndex(model.time_steps_ref['generic'])

        # define dry member(s)
        dry_member = xr.DataArray(
            np.zeros((len(dry_times), *ref.shape)),
            dims=('time', *ref.dims), coords={**ref.coords, 'time': dry_times}, name='rain',
            attrs={'units': 'mm'},
        )

        # define member files
        member_files = []
        for member_id in range(ensemble['start'], ensemble['end'] + 1):

            # define member tags
            member_tags = {'ensemble': f'{member_id:03d}'}
            # store member files
            member_files.append(tmp_run_config.write_data(dry_member, tags=member_tags))
    else:

        # get algorithm mode
        algorithm = model.lib_algorithm
        # check algorithm mode
        model.lib_algorithm = lambda *args, **options: _check_rfarm_algorithm(algorithm, *args, **options)

        # get member file(s)
        member_files = model.execute_run()

        # dump member(s) to pickle format
        if tmp_run_config.writer_type != 'pickle' and member_files is not None:

            converted_files = []
            if len(member_files) != ensemble['end'] - ensemble['start'] + 1:
                logger_stream.error('RainFARM ensemble seem to be incomplete')
                raise RuntimeError('RainFARM did not return the expected number of ensemble files.')

            # write member(s)
            for member_id, member_file in zip(range(ensemble['start'], ensemble['end'] + 1), member_files):

                if not Path(member_file).is_file():
                    logger_stream.error('RainFARM ensemble member file does not exist')
                    raise RuntimeError(f'RainFARM ensemble file was not created: {member_file}')

                # define member tags
                member_tags = {'ensemble': f'{member_id:03d}'}
                # read member data
                member_data = tmp_run_native.read_data(tags=member_tags)
                # write member data and store filename(s)
                converted_files.append(tmp_run_native.write_data(member_data, tags=member_tags))
                del member_data

            member_files = converted_files

    # check expected members range and members files
    members_range = range(ensemble['start'], ensemble['end'] + 1)
    if member_files is None or len(member_files) != len(members_range):
        logger_stream.error('RainFARM ensemble are not available in the expected number of ensemble files')
        raise RuntimeError('RainFARM did not return the expected number of ensemble files.')

    # organize member(s) manifest (to pass only the pickle file names)
    member_manifest = {}
    for member_id, member_file in zip(members_range, member_files):

        # check if member file exists or not
        if not Path(member_file).exists():
            logger_stream.error('RainFARM member file does not exist')
            raise RuntimeError(f'RainFARM ensemble file was not created: {member_file}')

        # save in the member manifest
        member_manifest[f'{member_id:03d}'] = str(Path(member_file).resolve())

    # Info method end
    logger_stream.info_down('Execute RainFARM NWP ... DONE')

    return {'rain': member_manifest}
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# helper to check RainFARM algorithm and deps
@with_logger(var_name='logger_stream')
def _check_rfarm_algorithm(algorithm, *args, **kwargs):

    # get results
    result = algorithm(*args, **kwargs)
    # check results
    rainfall = np.asarray(result[0])

    # check result if are defined by nan's or infinite rainfall
    if not np.isfinite(rainfall).all():
        raise RuntimeError(
            'RainFARM generated NaN or infinite rainfall. Ensemble output was '
            'rejected before writing; check spectral slopes and input data.'
        )
    return result

# helper to check rfarm model algorithm and deps
@with_logger(var_name='logger_stream')
def _load_rfarm_algorithm():

    # check RainFARM in the external toolkits
    check_external_toolkit('rfarm', auto_install=False, raise_error=True)
    try:
        from rfarm.driver.drv_model_exec import RFarmModel
    except ModuleNotFoundError as exc:

        raise ImportError(
            f'RFarm requires unavailable dependency "{exc.name}". '
            'Install it in the active Python environment before running RainFARM.'
        ) from exc

    logger_stream.info(f'RainFARM model source: {inspect.getfile(RFarmModel)}')

    return RFarmModel

# helper to prepare rainfall
@with_logger(var_name='logger_stream')
def _prepare_rainfall(data, negative_tolerance=0.01, nodata_value=-9999, **kwargs):

    # check tolerance
    if not np.isfinite(negative_tolerance) or negative_tolerance < 0:
        logger_stream.error(f'RainFARM negative tolerance: {negative_tolerance}.')
        raise ValueError('negative_tolerance must be finite and nonnegative.')

    # get values
    values = np.array(data.values, dtype=float, copy=True)
    # set markers
    markers = [nodata_value]
    for metadata in (data.attrs, data.encoding):
        for key in ('_FillValue', 'missing_value', 'nodata', 'nodata_value', 'GRIB_missingValue'):
            if key in metadata:
                markers.extend(np.asarray(metadata[key]).ravel())

    # count missing values
    missing_count = 0
    for marker in markers:
        if marker is None:
            continue
        try:
            marker = float(marker)
        except (TypeError, ValueError):
            continue
        if not np.isfinite(marker):
            continue
        missing = values == marker
        missing_count += int(np.count_nonzero(missing))
        values[missing] = np.nan
    # print missing values
    if missing_count:
        logger_stream.warning(f'RainFARM decoded {missing_count} no-data values as NaN.')

    # count infinite values
    infinite_count = int(np.count_nonzero(np.isinf(values)))
    if infinite_count:
        logger_stream.error(f'RainFARM decoded {infinite_count} infinite values.')
        raise ValueError(f'RainFARM rainfall contains {infinite_count} infinite values.')

    # count negative values
    negative_values = values < 0
    negative_count = int(np.count_nonzero(negative_values))
    if negative_count:
        minimum = float(np.min(values, where=negative_values, initial=0.0))
        if minimum < -negative_tolerance:
            logger_stream.error(f'RainFARM minimum is less than {negative_tolerance}.')
            raise ValueError(
                f'RainFARM rainfall must be nonnegative: found {negative_count} negative values, '
                f'minimum={minimum:.9g} mm; negative_tolerance={negative_tolerance:g} mm. '
                'Check no-data metadata/nodata_value and rainfall accumulation handling.'
            )
        logger_stream.warning(
            f'RainFARM clipped {negative_count} small negative rainfall values to zero '
            f'(minimum={minimum:.9g} mm, tolerance={negative_tolerance:g} mm).'
        )
        values[negative_values] = 0.0

    return values

# helper to prepare grid
@with_logger(var_name='logger_stream')
def _prepare_grid(data):

    # get longitude and latitude name(s)
    lon_name = next((name for name in ('longitude', 'lon') if name in data.coords), None)
    lat_name = next((name for name in ('latitude', 'lat') if name in data.coords), None)
    # check longitude and latitudes names
    if lon_name is None or lat_name is None:
        logger_stream.error('Longitude and latitude names are not found in the datasets.')
        raise ValueError('RainFARM requires longitude/latitude (or lon/lat) coordinates.')

    # get longitude and latitude values
    lon, lat = data[lon_name], data[lat_name]
    # check longitude and latitude dims
    if lon.ndim == lat.ndim == 1:
        x_dim, y_dim = lon.dims[0], lat.dims[0]
    elif lon.ndim == lat.ndim == 2 and set(lon.dims) == set(lat.dims):
        y_dim, x_dim = lat.dims
    else:
        logger_stream.error('Longitude and latitude values are not in expected format.')
        raise ValueError('RainFARM coordinates must be matching 1D axes or 2D grids.')

    if x_dim == y_dim or set(data.dims) - {'time'} != {y_dim, x_dim}:
        logger_stream.error('Longitude and latitude values are not in expected format.')
        raise ValueError('RainFARM geographic coordinates must cover both spatial dimensions.')

    # adjust data
    data = data.transpose(*(('time',) if 'time' in data.dims else ()), y_dim, x_dim)

    # adjust longitude and latitude
    lat, lon = xr.broadcast(data[lat_name], data[lon_name])
    lons = lon.transpose(y_dim, x_dim).values
    lats = lat.transpose(y_dim, x_dim).values
    if min(lons.shape) < 2 or not np.isfinite(lons).all() or not np.isfinite(lats).all():
        logger_stream.error('RainFARM geographic coordinates must cover both spatial dimensions.')
        raise ValueError('RainFARM grids require at least two finite coordinates per spatial axis.')
    if not np.allclose(lons, lons[0:1, :]) or not np.allclose(lats, lats[:, 0:1]):
        logger_stream.error('RainFARM geographic coordinates must be rectilinear.')
        raise ValueError('RainFARM requires rectilinear longitude/latitude grids.')

    # check the grids
    dx, dy = np.diff(lons[0]), np.diff(lats[:, 0])
    if dx[0] == 0 or dy[0] == 0 or not np.allclose(dx, dx[0]) or not np.allclose(dy, dy[0]):
        logger_stream.error('RainFARM geographic coordinates must be regular.')
        raise ValueError('RainFARM requires regularly spaced spatial coordinates.')

    # organize data, longitude and latitude
    rows = slice(None, None, -1) if dy[0] > 0 else slice(None)
    cols = slice(None, None, -1) if dx[0] < 0 else slice(None)
    data = data.isel({y_dim: rows, x_dim: cols})

    return data, lons[rows, cols], lats[rows, cols], abs(float(dx[0])), abs(float(dy[0]))
# ----------------------------------------------------------------------------------------------------------------------
