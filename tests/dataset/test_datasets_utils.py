"""Public dataset operations and model-grid compatibility checks."""
import gzip
from pathlib import Path
import tempfile
import unittest

import numpy as np
import pandas as pd
import xarray as xr

from shybox.runner_toolkit.datasets.lib_utils_datasets import split_3d_to_2d, resample_data, copy_as_is
from shybox.runner_toolkit.datasets.lib_utils_datasets_geo import check_geographical_info, read_geographical_info


class TestDatasetsUtils(unittest.TestCase):
    def setUp(self):
        self.ref = xr.DataArray([[1., 2.], [3., 4.]], dims=('latitude', 'longitude'),
                                coords={'latitude': [44., 43.], 'longitude': [12., 13.]})

    def test_split_selects_exact_time_and_preserves_input(self):
        times = pd.date_range('2026-11-09', periods=2, freq='h')
        cube = self.ref.expand_dims(time=times)
        selected = split_3d_to_2d(cube, times[1])
        self.assertEqual(selected.ndim, 2)
        self.assertEqual(cube.ndim, 3)
        np.testing.assert_array_equal(selected, self.ref)
        with self.assertRaisesRegex(ValueError, 'Select a time'):
            split_3d_to_2d(cube)
        with self.assertRaisesRegex(ValueError, 'missing'):
            split_3d_to_2d(cube, '2026-11-10')
        with self.assertRaisesRegex(ValueError, 'vertical'):
            split_3d_to_2d(cube.expand_dims(level=[1, 2]), times[0])

    def test_geography_detects_same_grid_reversed_axes_and_mismatch(self):
        result = check_geographical_info(self.ref, self.ref)
        self.assertTrue(result['same_grid'])
        flipped = self.ref.isel(latitude=slice(None, None, -1), longitude=slice(None, None, -1))
        result = check_geographical_info(flipped, self.ref)
        self.assertTrue(result['same_grid'])
        values = flipped.values[np.ix_(result['y_index'], result['x_index'])]
        np.testing.assert_array_equal(values, self.ref)
        shifted = self.ref.assign_coords(longitude=[12.1, 13.1])
        self.assertTrue(check_geographical_info(shifted, self.ref)['requires_interpolation'])

    def test_invalid_geographical_metadata_fails(self):
        bad = self.ref.to_dataset(name='Terrain')
        bad.attrs['ncols'] = 10
        with self.assertRaisesRegex(ValueError, 'ncols'):
            read_geographical_info(bad)
        bad.attrs = {}
        bad.longitude.attrs['units'] = 'metres'
        with self.assertRaisesRegex(ValueError, 'projected'):
            read_geographical_info(bad)
        bad.longitude.attrs = {}
        bad.attrs['epsg_code'] = 3857
        with self.assertRaisesRegex(ValueError, '4326'):
            read_geographical_info(bad)

    def test_base_linear_resampling_and_nearest_roi(self):
        x, y = np.meshgrid([12., 13.], [43., 44.])
        value = x + y
        output = resample_data(value, x, y, np.array([[12.5]]), np.array([[43.5]]), method='linear')
        np.testing.assert_allclose(output, [[56.]])
        output = resample_data(value, x, y, np.array([[12.5]]), np.array([[43.5]]), method='nearest', roi=1)
        self.assertTrue(np.isnan(output).all())

    def test_as_is_copy_changes_only_gzip_envelope(self):
        with tempfile.TemporaryDirectory() as folder:
            source = Path(folder) / 'source.nc'
            source.write_bytes(b'model forcing bytes\x00\x01')
            zipped = Path(folder) / 'copied.nc.gz'
            restored = Path(folder) / 'restored.nc'
            copy_as_is(source, zipped)
            with gzip.open(zipped, 'rb') as stream:
                self.assertEqual(stream.read(), source.read_bytes())
            copy_as_is(zipped, restored)
            self.assertEqual(restored.read_bytes(), source.read_bytes())


if __name__ == '__main__':
    unittest.main()
