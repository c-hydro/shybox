"""Coordinate orientation, reference alignment and metric/geographic catchment area."""
import unittest
import numpy as np
import xarray as xr
from affine import Affine

from shybox.geo_toolkit.lib_geo_coords import create_grid_from_corners, fix_geo_orientation, match_coords_to_reference
from shybox.geo_toolkit.lib_geo_watersheds import compute_catchment_area


class GeoProcessingTests(unittest.TestCase):
    def grid(self):return create_grid_from_corners(42,12,3,4,.1,grid_check=False)

    def test_lower_left_corner_defines_cell_centres_and_north_first(self):
        grid=self.grid()
        np.testing.assert_allclose(grid.longitude,[12.05,12.15,12.25,12.35])
        np.testing.assert_allclose(grid.latitude,[42.25,42.15,42.05])
        self.assertEqual(grid.shape,(3,4))

    def test_orientation_reorders_data_with_wrapped_longitude(self):
        ds=xr.Dataset({'rain':(('lat','lon'),[[10,20],[30,40]])},coords={'lat':[44,43],'lon':[350,10]})
        fixed,actions=fix_geo_orientation(ds,'lon','lat')
        np.testing.assert_array_equal(fixed.lat,[43,44]);np.testing.assert_array_equal(fixed.lon,[-10,10])
        np.testing.assert_array_equal(fixed.rain,[[30,40],[10,20]])
        np.testing.assert_array_equal(ds.lat,[44,43]);self.assertTrue(actions)

    def test_small_coordinate_drift_snaps_without_changing_data(self):
        reference=self.grid();shifted=reference.assign_coords(longitude=reference.longitude+.02)
        aligned=match_coords_to_reference(shifted,reference)
        np.testing.assert_array_equal(aligned.longitude,reference.longitude)
        np.testing.assert_array_equal(aligned.values,shifted.values)
        self.assertFalse(np.array_equal(shifted.longitude,reference.longitude))

    def test_shape_mismatch_raises_clear_error(self):
        with self.assertRaisesRegex(ValueError,'Shape mismatch'):match_coords_to_reference(self.grid().isel(longitude=slice(0,2)),self.grid())

    def test_outside_bbox_preserves_original_or_raises(self):
        reference=self.grid();far=reference.assign_coords(longitude=reference.longitude+10)
        aligned=match_coords_to_reference(far,reference)
        np.testing.assert_array_equal(aligned.longitude,far.longitude)
        with self.assertRaisesRegex(ValueError,'Bounding Box'):match_coords_to_reference(far,reference,raise_on_bbox=True)

    def test_metric_catchment_ignores_nodata_and_outside_cells(self):
        mask=np.array([[1,1],[0,np.nan]])
        area,cells=compute_catchment_area(mask,Affine(100,0,0,0,-100,0))
        self.assertEqual(cells,2);self.assertAlmostEqual(area,.02)
        self.assertEqual(compute_catchment_area(mask,Affine(100,0,0,0,-100,0),unit_type='meters')[0],20000)

    def test_geographic_catchment_requires_matching_latitude(self):
        mask=np.ones((2,2));transform=Affine(.01,0,0,0,-.01,0)
        with self.assertRaises(ValueError):compute_catchment_area(mask,transform,transform_is_metric=False)
        with self.assertRaises(ValueError):compute_catchment_area(mask,transform,transform_is_metric=False,latitude=np.ones((1,1)))
        area,cells=compute_catchment_area(mask,transform,transform_is_metric=False,latitude=45,decimal_round=6)
        self.assertEqual(cells,4)
        self.assertAlmostEqual(area,4*(111320*.01)**2*np.cos(np.deg2rad(45))/1e6,places=6)

    def test_empty_catchment_and_invalid_units(self):
        self.assertEqual(compute_catchment_area(np.zeros((2,2)),Affine.identity()),(0.,0))
        with self.assertRaises(NotImplementedError):compute_catchment_area(np.ones((2,2)),Affine.identity(),unit_type='miles')


if __name__=='__main__':unittest.main()
