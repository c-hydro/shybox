"""Temporal forcing preparation and the ICON fields used by the VdA converter."""
import unittest
from unittest.mock import patch
from pathlib import Path
import tempfile

import numpy as np
import pandas as pd
import xarray as xr

from shybox.io_toolkit.lib_io_grib import prepare_forecast_time, read_datasets_grib
from shybox.processing_grid_toolkit.lib_proc_compute_humidity import compute_data_rh
from shybox.io_toolkit.lib_io_nc_hmc import write_dataset_hmc
from shybox.logging_toolkit.logging_handler import LoggingManager
from shybox.dataset_toolkit.dataset_handler_local import DataLocal


class TestForecastPreparation(unittest.TestCase):
    def setUp(self):
        self.start = pd.Timestamp('2026-11-04')
        self.times = pd.date_range(self.start, periods=3, freq='3h')
        self.rain = xr.DataArray([0., 6., 12.], dims='step', name='tp',
                                coords={'step': self.times, 'time_start': self.start},
                                attrs={'units': 'kg m**-2'})

    def test_rainfall_intervals_conserve_native_totals(self):
        original = self.rain.copy(deep=True)
        output = prepare_forecast_time(self.rain, accumulated=True)
        np.testing.assert_allclose(output, [2.] * 6)
        self.assertEqual(float(output.sum()), 12.)
        self.assertEqual(output.name, 'tp')
        self.assertEqual(output.attrs['units'], 'mm')
        self.assertEqual(pd.Timestamp(output.step.values[0]), self.start + pd.Timedelta(hours=1))
        xr.testing.assert_identical(self.rain, original)

    def test_metre_accumulations_convert_to_millimetres(self):
        data = self.rain / 1000
        data.attrs['units'] = 'm'
        np.testing.assert_allclose(prepare_forecast_time(data, accumulated=True), [2.] * 6)

    def test_instantaneous_fields_interpolate_without_extrapolation(self):
        data = self.rain.copy(data=[273.15, 279.15, 285.15])
        data.attrs['units'] = 'K'
        output = prepare_forecast_time(data)
        np.testing.assert_allclose(output, 273.15 + 2 * np.arange(7))
        self.assertEqual(output.attrs['units'], 'K')

    def test_resets_missing_baseline_and_invalid_frequency_fail(self):
        with self.assertRaisesRegex(ValueError, 'decreases'):
            prepare_forecast_time(self.rain.copy(data=[0., 6., 1.]), accumulated=True)
        with self.assertRaisesRegex(ValueError, 'initialization'):
            prepare_forecast_time(self.rain.isel(step=slice(1, None)), accumulated=True)
        with self.assertRaisesRegex(ValueError, 'start at zero'):
            prepare_forecast_time(self.rain.copy(data=[1., 6., 12.]), accumulated=True)
        with self.assertRaisesRegex(ValueError, 'multiples'):
            prepare_forecast_time(self.rain, frequency='2h')

    def test_grib_filters_are_forwarded_and_reference_time_is_preserved(self):
        dataset = xr.Dataset({'t2m': (('step', 'latitude', 'longitude'), np.ones((3, 2, 2)))},
                             coords={'time': self.start, 'step': pd.to_timedelta([0, 3, 6], unit='h'),
                                     'latitude': [46., 45.], 'longitude': [7., 8.]})
        filters = {'shortName': '2t', 'typeOfLevel': 'heightAboveGround', 'level': 2}
        with patch('shybox.io_toolkit.lib_io_grib.xr.open_dataset', return_value=dataset) as reader:
            output = read_datasets_grib('icon.grib2', filter_by_keys=filters, indexpath='',
                                        spatial_bounds=[6.5, 45.5, 7.5, 46.5])
        self.assertEqual(output.shape, (3, 1, 1))
        self.assertEqual(pd.Timestamp(output.time_start.item()), self.start)
        self.assertEqual(reader.call_args.kwargs['backend_kwargs'], {'filter_by_keys': filters, 'indexpath': ''})
        np.testing.assert_array_equal(output.step, self.times)

    def test_dewpoint_humidity_does_not_require_unavailable_specific_humidity(self):
        t = xr.DataArray([[280., 280.]], dims=('latitude', 'longitude'))
        td = xr.DataArray([[280., 275.]], dims=t.dims)
        raw = compute_data_rh.__wrapped__
        result = raw(t=t, td=td)
        self.assertEqual(float(result[0, 0]), 100.)
        self.assertTrue(0 < float(result[0, 1]) < 100.)
        xr.testing.assert_identical(result, raw(t=t, td=td, q=xr.full_like(t, .005)))
        with self.assertRaisesRegex(ValueError, 'q'):
            raw(t=t, td=td, return_diag=True)

    def test_hmc_netcdf_roundtrip_preserves_fractions_coordinates_and_missing_values(self):
        terrain = xr.DataArray([[10., 20.], [30., 40.]], dims=('latitude', 'longitude'),
                               coords={'latitude': [45.8, 45.7], 'longitude': [7.1, 7.2]})
        values = np.array([[.25, np.nan], [1.5, 2.75]])
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'forcing.nc'
            with LoggingManager(name='forcing_test', level=50).use_as_current():
                write_dataset_hmc(str(path), {'Rain': values}, terrain, time=self.start,
                                   file_compression=False, var_units={'Rain': 'mm'})
            with xr.open_dataset(path, engine='netcdf4') as dataset:
                np.testing.assert_allclose(dataset.Rain.values[0], np.flipud(values), equal_nan=True)
                np.testing.assert_allclose(dataset.longitude.values[0], [7.1, 7.2])
                self.assertTrue(np.issubdtype(dataset.Rain.dtype, np.floating))

    def test_hourly_rain_handler_retains_interval_end_times_on_repeated_reads(self):
        values = np.broadcast_to([0., 6., 18.], (2, 2, 3)).transpose(2, 0, 1).copy()
        raw = xr.DataArray(values, dims=('step', 'latitude', 'longitude'), name='tp',
                           coords={'step': self.times, 'time_start': self.start,
                                   'latitude': [46., 45.9], 'longitude': [7., 7.1]},
                           attrs={'units': 'kg m**-2'})
        forecast = prepare_forecast_time(raw, accumulated=True)
        with tempfile.TemporaryDirectory() as folder, LoggingManager(name='rain_time_test', level=50).use_as_current():
            path = Path(folder) / 'source.grib2'
            path.touch()
            handler = DataLocal(path=folder, file_name=path.name, file_format='grib', file_type='grid_3d',
                                file_variable='RAIN_DATA_SRC', file_workflow='RAIN_WORKFLOW', file_io='input',
                                data_layout='grid', variable_template={
                                    'dims_geo': {'step': 'time', 'latitude': 'latitude', 'longitude': 'longitude'},
                                    'vars_data': {'tp': 'rain'}}, time_signature='period',
                                time_reference=self.start + pd.Timedelta(hours=1),
                                time_period=6, time_freq='h', time_direction='forward', message=False)
            with patch.object(handler, '_read_data', side_effect=lambda *args, **kwargs: forecast.copy(deep=True)):
                for hour, expected in [(1, 2.), (2, 2.), (3, 2.), (4, 4.)]:
                    result = handler.get_data(time=self.start + pd.Timedelta(hours=hour))
                    np.testing.assert_allclose(result.values, expected)


if __name__ == '__main__':
    unittest.main()
