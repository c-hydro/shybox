"""HMC missing-value metadata must stay numeric through NetCDF round trips."""
from pathlib import Path
import tempfile
import unittest
import warnings

import numpy as np
import pandas as pd
import xarray as xr

from shybox.io_toolkit.lib_io_nc_hmc import write_dataset_hmc
from shybox.dataset_toolkit.lib_dataset_generic import read_from_file


class TestHMCMissingValues(unittest.TestCase):
    def test_single_variable_dataset_is_accepted_without_mutating_input(self):
        ref = xr.DataArray(np.ones((2, 2)), dims=('latitude', 'longitude'),
                           coords={'latitude': [45., 44.], 'longitude': [10., 11.]})
        field = xr.Dataset({'rain': ref * 2})
        before = field.copy(deep=True)
        with tempfile.TemporaryDirectory() as folder:
            path = str(Path(folder) / 'forcing.nc')
            write_dataset_hmc(path, {'Rain': field}, ref, time=pd.Timestamp('2026-11-14'),
                              file_compression=False)
            with xr.open_dataset(path) as result:
                np.testing.assert_allclose(result.Rain, 2.)
            with self.assertRaisesRegex(ValueError, 'expected one data variable'):
                write_dataset_hmc(path, {'Rain': field.assign(second=ref)}, ref,
                                  time=pd.Timestamp('2026-11-14'), file_compression=False)
        xr.testing.assert_identical(field, before)

    def test_generic_reader_normalizes_numeric_decoding_metadata(self):
        raw = xr.Dataset({'rain': ('x', [1., -9999.]), 'air_t': ('x', [2., 3.])})
        raw.rain.attrs.update(scale_factor=np.int64(1), add_offset=np.int64(0),
                              missing_value='-9999.0', _FillValue=-9999.)
        with tempfile.TemporaryDirectory() as folder:
            path = str(Path(folder) / 'source.nc')
            raw.to_netcdf(path)
            with warnings.catch_warnings():
                warnings.simplefilter('error', xr.SerializationWarning)
                decoded = read_from_file(path, file_format='netcdf', file_type='grid_3d')
                try:
                    np.testing.assert_allclose(decoded.rain.values, [1., np.nan])
                    np.testing.assert_allclose(decoded.air_t.values, [2., 3.])
                finally:
                    decoded.close()

    def test_fill_and_missing_values_use_the_variable_dtype_without_decoding_warnings(self):
        ref = xr.DataArray(
            np.ones((2, 2)), dims=('latitude', 'longitude'),
            coords={'latitude': [45., 44.], 'longitude': [10., 11.]},
        )
        values = np.array([[1.25, np.nan], [2.5, 3.75]])
        names = ('Rain', 'Air_Temperature', 'Relative_Humidity', 'Incoming_Radiation', 'Wind')
        with tempfile.TemporaryDirectory() as folder:
            for dtype in ('f4', 'f8'):
                for key, marker in (('missing_value', '-9999.0'), ('Missing_value', -9999.0)):
                    with self.subTest(dtype=dtype, key=key):
                        path = Path(folder) / 'forcing.nc'
                        attrs = {'format': dtype, 'fill_value': -9999.0}
                        attrs.update({name: {key: marker} for name in names})
                        write_dataset_hmc(
                            str(path), {name: values for name in names}, ref,
                            time=pd.Timestamp('2026-11-13'), attrs_data=attrs,
                            attrs_x={key: marker}, attrs_y={key: marker},
                            file_compression=False,
                        )
                        with xr.open_dataset(path, decode_cf=False) as raw:
                            for name in (*names, 'longitude', 'latitude'):
                                missing = np.asarray(raw[name].attrs['missing_value'])
                                self.assertEqual(missing.dtype, raw[name].dtype)
                                self.assertEqual(float(missing), -9999.)
                            for name in names:
                                self.assertEqual(raw[name].attrs['_FillValue'],
                                                 raw[name].attrs['missing_value'])
                        with warnings.catch_warnings():
                            warnings.simplefilter('error', xr.SerializationWarning)
                            with xr.open_dataset(path) as decoded:
                                for name in names:
                                    np.testing.assert_allclose(decoded[name].values[0], np.flipud(values))
        self.assertTrue(np.isnan(values[0, 1]))
        self.assertEqual(values[0, 0], 1.25)


if __name__ == '__main__':
    unittest.main()
