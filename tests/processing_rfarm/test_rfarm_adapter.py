"""RFarm boundary checks without running the stochastic numerical model."""
import pickle
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import numpy as np
import pandas as pd
import xarray as xr

from shybox.processing_rfarm_toolkit import rfarm_adapter as adapter
from shybox.orchestrator_toolkit.lib_orchestrator_utils_checker import check_data_type
from shybox.dataset_toolkit.dataset_handler_temporary import DataTemporary


class FakeModel:
    instances = []

    def __init__(self, **kwargs):
        self.options = kwargs
        self.slope_s = kwargs['slope_s']
        self.slope_t = kwargs['slope_t']
        self.lib_algorithm = Mock()
        Path(kwargs['folder_tmp']).mkdir(parents=True, exist_ok=True)
        self.ensemble_filename = [
            str(Path(kwargs['folder_tmp']) / kwargs['filename_tmp'].format(ensemble=f'{member:03d}'))
            for member in range(kwargs['ensemble_n']['start'], kwargs['ensemble_n']['end'] + 1)
        ]
        self.executed = False
        self.instances.append(self)

    def configure_grid(self, *args, **kwargs):
        self.grid_args = args
        self.grid_options = kwargs

    def configure_time(self, times):
        self.time_steps_ref = {'generic': times}

    def configure_data(self, values, info_in):
        self.input_values = values.copy()
        self.data_rf = {'generic': values}
        # Emulate RFarm's in-place thresholding.
        values[values < 0.01] = 0

    def execute_run(self):
        self.executed = True
        for filename in self.ensemble_filename:
            with open(filename, 'wb') as handle:
                pickle.dump(xr.DataArray([1.0], name='rain'), handle)
        return self.ensemble_filename


class TestRFarmAdapter(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.loader = patch.object(adapter, '_load_rfarm_model', return_value=FakeModel)
        self.load_model = self.loader.start()
        self.addCleanup(self.loader.stop)
        FakeModel.instances.clear()
        self.data = xr.DataArray(
            np.arange(16, dtype=float).reshape(4, 2, 2) + 0.005,
            dims=('time', 'latitude', 'longitude'),
            coords={'time': pd.date_range('2026-11-01', periods=4, freq='h'),
                    'latitude': [44.0, 45.0], 'longitude': [10.0, 11.0]},
            name='rain', attrs={'units': 'mm'},
        )
        self.ref = self.data.isel(time=0, drop=True)

    def run_adapter(self, data=None, **kwargs):
        # Bypass only orchestration; retain the real logger decorator.
        return adapter.exec_rfarm_nwp.__wrapped__(
            data=self.data if data is None else data, ref=self.ref,
            folder_tmp=self.folder.name, logger_stream=Mock(), **kwargs,
        )

    def test_runs_model_and_returns_only_member_paths(self):
        original = self.data.copy(deep=True)
        result = self.run_adapter(ensemble={'start': 2, 'end': 3}, domain_extension=1)
        model = FakeModel.instances[-1]
        self.assertTrue(model.executed)
        self.assertEqual(set(result['rain']), {'002', '003'})
        self.assertTrue(all(Path(path).is_file() for path in result['rain'].values()))
        self.assertTrue(check_data_type(result, 'file'))
        np.testing.assert_array_equal(model.input_values, original.values[:, ::-1, :].transpose(1, 2, 0))
        xr.testing.assert_identical(self.data, original)
        self.assertEqual(model.grid_args[4:], (1.0, 1.0))
        self.assertEqual(model.grid_options, {'domain_extension': 1})

    def test_runs_use_distinct_directories(self):
        first = self.run_adapter()
        second = self.run_adapter()
        self.assertNotEqual(first['rain']['001'], second['rain']['001'])

    def test_registered_process_returns_file_manifest(self):
        result = adapter.exec_rfarm_nwp(
            data={'data': self.data}, ref=self.ref, folder_tmp=self.folder.name,
            variable_in={}, variable_out={}, logger_stream=Mock(),
        )
        self.assertTrue(check_data_type(result, 'file'))

    def test_dry_input_writes_members_without_upstream_dry_branch(self):
        result = self.run_adapter(data=xr.zeros_like(self.data), ensemble={'start': 1, 'end': 2})
        self.assertFalse(FakeModel.instances[-1].executed)
        for filename in result['rain'].values():
            with open(filename, 'rb') as handle:
                member = pickle.load(handle)
            self.assertEqual(member.shape, self.data.shape)
            self.assertEqual(float(member.sum()), 0)
            self.assertEqual(member.attrs['units'], 'mm')
            self.assertGreater(member.latitude[0], member.latitude[-1])

    def test_invalid_time_or_parameters_fail_before_loading_model(self):
        for options in ({'ratio_t': 0}, {'multi_core': True}, {'tmp': False},
                        {'ensemble': {'start': 3, 'end': 1}}, {'filename_tmp': 'same.pkl'}):
            with self.subTest(options=options), self.assertRaises((ValueError, NotImplementedError)):
                self.run_adapter(**options)
        irregular = self.data.assign_coords(time=pd.to_datetime([
            '2026-11-01 00:00', '2026-11-01 01:00', '2026-11-01 03:00', '2026-11-01 04:00']))
        with self.assertRaisesRegex(ValueError, 'evenly spaced'):
            self.run_adapter(data=irregular)
        self.load_model.assert_not_called()

    def test_missing_data_skips_model(self):
        self.assertIsNone(self.run_adapter(data=xr.full_like(self.data, np.nan)))
        self.assertIsNone(self.run_adapter(data=self.data.isel(time=slice(0, 0))))
        self.load_model.assert_not_called()

    def test_two_dimensional_coordinates_follow_data_orientation(self):
        grid = self.data.rename({'latitude': 'y', 'longitude': 'x'})
        lons, lats = np.meshgrid([11.0, 10.0], [44.0, 45.0])
        grid = grid.assign_coords(longitude=(('y', 'x'), lons), latitude=(('y', 'x'), lats))
        prepared, lon, lat, dx, dy = adapter._prepare_grid(grid)
        np.testing.assert_array_equal(prepared.values, grid.values[:, ::-1, ::-1])
        self.assertTrue(np.all(np.diff(lon[0]) > 0))
        self.assertTrue(np.all(np.diff(lat[:, 0]) < 0))
        self.assertEqual((dx, dy), (1, 1))

    def test_missing_output_file_is_reported(self):
        with patch.object(FakeModel, 'execute_run', return_value=['/missing/rf_001.pkl']):
            with self.assertRaisesRegex(RuntimeError, 'was not created'):
                self.run_adapter()

    def test_invalid_rainfall_or_grid_does_not_load_model(self):
        with self.assertRaisesRegex(ValueError, 'nonnegative'):
            self.run_adapter(data=-self.data)
        irregular = self.data.assign_coords(longitude=[10.0, 10.0])
        with self.assertRaisesRegex(ValueError, 'regularly spaced'):
            self.run_adapter(data=irregular)
        self.load_model.assert_not_called()

    def test_microsecond_time_resolution_is_supported(self):
        times = self.data.time.values.astype('datetime64[us]')
        result = self.run_adapter(data=self.data.assign_coords(time=times))
        self.assertTrue(check_data_type(result, 'file'))

    def test_small_negatives_and_missing_markers_are_normalized(self):
        data = self.data.copy(deep=True)
        data.values[0, 0, 0] = -1e-7
        data.values[1, 0, 0] = -9999
        data.values[2, 0, 0] = -32768
        data.encoding['_FillValue'] = -32768
        original = data.copy(deep=True)
        self.run_adapter(data=data)
        received = FakeModel.instances[-1].input_values
        self.assertEqual(received[1, 0, 0], 0)
        self.assertTrue(np.isnan(received[1, 0, 1:3]).all())
        xr.testing.assert_identical(data, original)

    def test_large_negatives_and_infinity_remain_errors(self):
        data = self.data.copy(deep=True)
        data.values[0, 0, 0] = -0.1
        with self.assertRaisesRegex(ValueError, 'minimum=-0.1 mm'):
            self.run_adapter(data=data)
        data.values[0, 0, 0] = np.inf
        with self.assertRaisesRegex(ValueError, '1 infinite values'):
            self.run_adapter(data=data)
        self.load_model.assert_not_called()

    def test_only_no_data_is_not_treated_as_a_dry_forecast(self):
        self.assertIsNone(self.run_adapter(data=xr.full_like(self.data, -9999)))
        self.load_model.assert_not_called()

    def test_explicit_tolerance_and_no_data_marker(self):
        data = self.data.copy(deep=True)
        data.values[0, 0, 0] = -0.02
        data.values[1, 0, 0] = -12345
        self.run_adapter(data=data, negative_tolerance=0.03, nodata_value=-12345)
        received = FakeModel.instances[-1].input_values
        self.assertEqual(received[1, 0, 0], 0)
        self.assertTrue(np.isnan(received[1, 0, 1]))

    def test_temporary_object_controls_pickle_paths(self):
        tmp_data = DataTemporary(
            folder_name=str(Path(self.folder.name) / 'configured'),
            file_name='rf.icon-2i.20261101_{ensemble}.pkl',
        )
        result = self.run_adapter(tmp_data=tmp_data)
        path = Path(result['rain']['001'])
        self.assertEqual(path.name, 'rf.icon-2i.20261101_001.pkl')
        self.assertEqual(path.parent.parent, Path(tmp_data.folder_name))
        self.assertEqual(FakeModel.instances[-1].options['filename_tmp'], tmp_data.file_name)

    def test_ancillary_object_controls_pickle_paths(self):
        ancillary_data = DataTemporary(
            folder_name=str(Path(self.folder.name) / 'ancillary'),
            file_name='ancillary_{ensemble}.pkl',
        )
        result = self.run_adapter(ancillary_data=ancillary_data)
        path = Path(result['rain']['001'])
        self.assertEqual(path.name, 'ancillary_001.pkl')
        self.assertEqual(path.parent.parent, Path(ancillary_data.folder_name))

    def test_ancillary_array_backends_convert_native_pickles(self):
        for writer_type, suffix in (('netcdf', '.nc'), ('zarr', '.zarr')):
            with self.subTest(writer_type=writer_type):
                storage = DataTemporary(self.folder.name, 'rain_{ensemble}' + suffix,
                                        writer_type=writer_type, prefix='custom_')
                result = self.run_adapter(ancillary_data=storage, ensemble={'start': 1, 'end': 2})
                for path in result['rain'].values():
                    self.assertEqual(Path(path).suffix, suffix)
                    self.assertTrue(Path(path).exists())
                    self.assertTrue(Path(path).parent.name.startswith('custom_'))
                run = DataTemporary(str(Path(result['rain']['001']).parent), storage.file_name,
                                    writer_type=writer_type)
                with run.read_data(tags={'ensemble': '001'}) as restored:
                    xr.testing.assert_identical(restored, xr.DataArray([1.0], name='rain'))

    def test_dry_forecast_uses_selected_writer(self):
        storage = DataTemporary(self.folder.name, 'rain_{ensemble}.nc', writer_type='netcdf')
        result = self.run_adapter(data=xr.zeros_like(self.data), ancillary_data=storage)
        run = DataTemporary(str(Path(result['rain']['001']).parent), storage.file_name, writer_type='netcdf')
        with run.read_data(tags={'ensemble': '001'}) as restored:
            self.assertEqual(float(restored.sum()), 0.0)
            self.assertEqual(restored.shape, self.data.shape)

    def test_conflicting_ancillary_and_temporary_objects_are_rejected(self):
        first = DataTemporary(self.folder.name)
        second = DataTemporary(str(Path(self.folder.name) / 'other'))
        with self.assertRaisesRegex(ValueError, 'two different storage objects'):
            self.run_adapter(ancillary_data=first, tmp_data=second)
        self.load_model.assert_not_called()

    def test_null_slopes_reach_native_model_unchanged(self):
        self.run_adapter()
        model = FakeModel.instances[-1]
        self.assertIsNone(model.slope_s)
        self.assertIsNone(model.slope_t)
        self.assertTrue(model.executed)

    def test_explicit_slopes_reach_native_model_unchanged(self):
        self.run_adapter(slope_s=1.5, slope_t=1.4)
        model = FakeModel.instances[-1]
        self.assertEqual((model.slope_s, model.slope_t), (1.5, 1.4))

    def test_none_and_nan_slopes_are_normalized_in_adapter(self):
        for spatial, temporal in ((None, np.nan), (np.nan, None),
                                  (np.nan, np.nan), (np.float32('nan'), np.float64('nan'))):
            with self.subTest(spatial=spatial, temporal=temporal):
                self.run_adapter(slope_s=spatial, slope_t=temporal)
                model = FakeModel.instances[-1]
                self.assertIsNone(model.slope_s)
                self.assertIsNone(model.slope_t)

    def test_partial_and_nonfinite_slopes_are_rejected(self):
        for options in ({'slope_s': 1.5}, {'slope_t': 1.4},
                        {'slope_s': np.nan, 'slope_t': 1.4},
                        {'slope_s': 1.5, 'slope_t': np.inf}):
            with self.subTest(options=options), self.assertRaises(ValueError):
                self.run_adapter(**options)
        self.load_model.assert_not_called()

    def test_numerical_output_is_checked_before_returning_to_model(self):
        for value in (np.nan, np.inf):
            with self.subTest(value=value), self.assertRaisesRegex(RuntimeError, 'before writing'):
                adapter._checked_rfarm_algorithm(lambda: (np.array([value]), None))
        valid = (np.array([1.0, 2.0]), None)
        self.assertIs(adapter._checked_rfarm_algorithm(lambda: valid), valid)


if __name__ == '__main__':
    unittest.main()
