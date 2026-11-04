"""RFarm pickle-to-NetCDF writing through native and SHYBOX interfaces."""
import pickle
import tempfile
import unittest
import weakref
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd
import xarray as xr

from shybox.io_toolkit import nc_wrapper
from shybox.dataset_toolkit.dataset_handler_local import DataLocal


class TestRFarmNetCDFWrapper(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.root = Path(self.folder.name)
        self.ref = xr.DataArray([[10.0, -9999.0], [20.0, 30.0]],
                                dims=('latitude', 'longitude'),
                                coords={'latitude': [45.0, 44.0], 'longitude': [10.0, 11.0]})
        lons, lats = np.meshgrid(self.ref.longitude, self.ref.latitude)
        self.manifest = {}
        self.times = pd.date_range('2026-11-03', periods=3, freq='h')
        for member in (1, 3):
            array = xr.DataArray(np.arange(12, dtype=float).reshape(3, 2, 2) + member,
                                 dims=('time', 'south_north', 'west_east'),
                                 coords={'time': self.times, 'longitude': (('south_north', 'west_east'), lons),
                                         'latitude': (('south_north', 'west_east'), lats)}, name='rain')
            path = self.root / f'member_{member:03d}.pkl'
            with path.open('wb') as handle:
                pickle.dump(array, handle)
            self.manifest[f'{member:03d}'] = str(path)

    def require_rfarm(self):
        try:
            nc_wrapper._load_rfarm_result()
        except ImportError as exc:
            self.skipTest(f'Optional RFarm dependency unavailable: {exc}')

    def test_native_writer_preserves_members_orientation_and_mask(self):
        self.require_rfarm()
        outputs = nc_wrapper.write_nc_rfarm_ensemble(
            {'Rain': {**self.manifest, 'attrs': {'workflow': 'RFARM_WORKFLOW'}}},
            self.root / 'output_{ensemble}.nc', ref=self.ref, remove_pickle=False)
        self.assertEqual(set(outputs), {'001', '003'})
        for member, path in outputs.items():
            with xr.open_dataset(path, engine='netcdf4') as result:
                expected = np.flip(np.arange(12).reshape(3, 2, 2) + int(member), axis=1).astype(float)
                expected[:, 1, 1] = -9999.0
                np.testing.assert_allclose(result.Rain.values, expected)
                np.testing.assert_array_equal(result.Terrain.values, np.flipud(self.ref.values))
                self.assertEqual(result.Rain.attrs['units'], 'mm')
            self.assertTrue(Path(self.manifest[member]).is_file())

    def test_native_writer_releases_one_member_before_loading_the_next(self):
        self.require_rfarm()
        from rfarm.driver import drv_model_io
        read = drv_model_io.read_obj
        references = []

        def checked_read(path):
            if references:
                self.assertIsNone(references[-1]())
            array = read(path)
            references.append(weakref.ref(array))
            return array

        with patch.object(drv_model_io, 'read_obj', side_effect=checked_read):
            nc_wrapper.write_nc_rfarm_ensemble(self.manifest, self.root / 'out_{ensemble}.nc', self.ref)
        self.assertEqual(len(references), 2)
        self.assertTrue(all(reference() is None for reference in references))

    def test_native_time_aggregation_is_preserved(self):
        self.require_rfarm()
        output = nc_wrapper.write_nc_rfarm_ensemble(
            {'001': self.manifest['001']}, self.root / 'aggregated_{ensemble}.nc',
            self.ref, var_freq='2h', remove_pickle=False)
        with Path(self.manifest['001']).open('rb') as handle:
            source = pickle.load(handle)
        expected = source.resample(time='2h', closed='right', label='right').sum()
        expected_values = np.flip(expected.values, axis=1).copy()
        expected_values[:, 1, 1] = -9999.0
        with xr.open_dataset(output['001'], engine='netcdf4') as result:
            np.testing.assert_allclose(result.Rain.values, expected_values)
            np.testing.assert_array_equal(result.time.values, expected.time.values)

    def test_dataset_handler_dispatches_pickle_manifest_to_wrapper(self):
        self.require_rfarm()
        output = DataLocal(
            path=str(self.root), file_name='handler_{ensemble}.nc',
            file_format='netcdf', file_type='grid_rfarm_nwp', file_mode='local',
            file_variable='RAIN', file_workflow='RFARM_WORKFLOW', file_io='output',
            time_signature='step', message=False,
        )
        output.write_data({'Rain': self.manifest}, self.times[0], metadata={},
                          ref=self.ref, map={'rain': 'Rain'}, units={'rain': 'mm'})
        self.assertTrue((self.root / 'handler_001.nc').is_file())
        self.assertTrue((self.root / 'handler_003.nc').is_file())

    def test_invalid_inputs_fail_before_calling_native_writer(self):
        with patch.object(nc_wrapper, '_load_rfarm_result') as load:
            with self.assertRaisesRegex(ValueError, 'ensemble'):
                nc_wrapper.write_nc_rfarm_ensemble(self.manifest, self.root / 'same.nc', self.ref)
            with self.assertRaises(FileNotFoundError):
                nc_wrapper.write_nc_rfarm_ensemble({'001': self.root / 'missing.pkl'},
                                                   self.root / 'out_{ensemble}.nc', self.ref)
            with self.assertRaisesRegex(ValueError, 'pickle'):
                nc_wrapper.write_nc_rfarm_ensemble({'001': self.root / 'data.zarr'},
                                                   self.root / 'out_{ensemble}.nc', self.ref)
            load.assert_not_called()

    def test_successful_conversion_removes_pickle_when_requested(self):
        self.require_rfarm()
        outputs = nc_wrapper.write_nc_rfarm_ensemble(
            self.manifest, self.root / 'cleaned_{ensemble}.nc', self.ref, remove_pickle=True)
        self.assertTrue(all(Path(path).is_file() for path in outputs.values()))
        self.assertTrue(all(not Path(path).exists() for path in self.manifest.values()))


if __name__ == '__main__':
    unittest.main()
