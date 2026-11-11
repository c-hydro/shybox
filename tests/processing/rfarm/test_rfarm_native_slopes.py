"""Compatibility checks against the optional, locally available RFarm library."""
import unittest
from unittest.mock import Mock, patch

import numpy as np

from shybox.external_toolkit import check_external_toolkit


class TestNativeRFarmSlopes(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        try:
            check_external_toolkit('rfarm', auto_install=False, raise_error=True)
            from rfarm.core import lib_core_app, lib_core_fx
        except ImportError as exc:
            raise unittest.SkipTest(f'Optional RFarm dependency is unavailable: {exc}')
        cls.app, cls.fx = lib_core_app, lib_core_fx

    def test_estimated_slopes_are_scalars_with_original_values(self):
        frequencies = np.arange(16, dtype=float) + 1
        spatial = (frequencies ** -2.4)[:, None]
        temporal = (frequencies ** -1.4)[:, None]
        sx, sy, st = self.fx.fitallslopes(
            spatial, spatial, temporal, np.arange(2, 10), np.arange(2, 10), Mock())
        for slope in (sx, sy, st):
            self.assertIsInstance(slope, float)
        self.assertAlmostEqual(sx, 1.4)
        self.assertAlmostEqual(sy, 2.4)
        self.assertAlmostEqual(st, 1.4)
        self.assertIn('sx=1.400000', 'sx=%f sy=%f st=%f' % (sx, sy, st))

    def test_native_auto_estimation_generates_finite_rainfall_and_preserves_volume(self):
        rain = np.random.default_rng(42).uniform(0.1, 5, (16, 16, 16))
        with patch.object(self.fx, 'fitallslopes', wraps=self.fx.fitallslopes) as estimate:
            result, _ = self.app.exec_nwp(rain.copy(), 2, 3, 4, 1, sx=None, st=None)
        estimate.assert_called_once()
        self.assertEqual(result.shape, (32, 32, 48))
        self.assertTrue(np.isfinite(result).all())
        self.assertTrue((result >= 0).all())
        np.testing.assert_allclose(result.sum(), rain.sum() * 2 * 2 * 3, rtol=1e-10)


if __name__ == '__main__':
    unittest.main()
