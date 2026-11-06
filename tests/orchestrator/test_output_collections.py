"""Select joined time-series variables without discarding their collection."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import numpy as np
import pandas as pd
import xarray as xr

from shybox.dataset_toolkit.lib_dataset_generic import write_to_file
from shybox.logging_toolkit.logging_handler import LoggingManager
from shybox.orchestrator_toolkit.lib_orchestrator_process import _filter_output_collections


class TestOutputCollections(unittest.TestCase):
    def setUp(self):
        self.logger = LoggingManager(name='collection_tests', level=50)
        self.enterContext(self.logger.use_as_current())
        self.enterContext(patch.object(self.logger, 'warning'))
        self.times = pd.date_range('2026-11-04', periods=2, freq='h', name='time')
        self.frame = pd.DataFrame({
            ('discharge_sim_analysis', 'basin:a'): [1.25, 2.5],
            ('discharge_sim_analysis', 'basin:b'): [3., 4.],
            ('discharge_obs', 'basin:a'): [np.nan, 2.],
            ('discharge_obs', 'basin:b'): [3., np.nan],
            ('air_temperature_obs', 'basin:a'): [10., 11.],
        }, index=self.times)
        self.frame.columns.names = ['variable', 'section']
        self.frame.attrs['data'] = pd.DataFrame({'tag': ['basin:a', 'basin:b'], 'id': [1, 2]})
        self.frame.attrs['type'] = {'tag': str, 'id': int}
        self.mapping = {'discharge_sim_analysis': 'discharge_simulated_analysis',
                        'discharge_obs': 'discharge_observed'}

    def select(self, collections=None, mapping=None):
        return _filter_output_collections(
            {'time_series': self.frame} if collections is None else collections,
            self.mapping if mapping is None else mapping)

    def test_selects_multiindex_variables_preserving_sections_time_and_registry(self):
        original = self.frame.copy(deep=True)
        result = self.select()
        self.assertEqual(list(result), ['time_series'])
        selected = result['time_series']
        self.assertEqual(selected.columns.names, ['variable', 'section'])
        self.assertEqual(list(selected.columns.get_level_values(0).unique()), list(self.mapping))
        self.assertEqual(selected.shape, (2, 4))
        pd.testing.assert_index_equal(selected.index, self.times)
        pd.testing.assert_frame_equal(selected.attrs['data'], self.frame.attrs['data'])
        pd.testing.assert_frame_equal(self.frame, original)
        self.logger.warning.assert_not_called()

    def test_already_renamed_columns_take_precedence(self):
        frame = self.frame.copy()
        frame[('discharge_simulated_analysis', 'basin:a')] = [99., 100.]
        selected = self.select({'time_series': frame}, {'discharge_sim_analysis': 'discharge_simulated_analysis'})['time_series']
        self.assertEqual(list(selected.columns), [('discharge_simulated_analysis', 'basin:a')])
        self.assertEqual(selected.iloc[0, 0], 99.)

    def test_regular_dataframe_columns_are_supported(self):
        frame = pd.DataFrame({'discharge_obs': [1., 2.], 'other': [3., 4.]}, index=self.times)
        selected = self.select({'series': frame}, {'discharge_obs': 'discharge_observed'})['series']
        self.assertEqual(list(selected), ['discharge_obs'])

    def test_only_truly_missing_variables_warn(self):
        result = self.select(mapping={**self.mapping, 'missing': 'missing_output'})
        self.assertIn('time_series', result)
        self.logger.warning.assert_called_once()
        self.assertIn('missing_output', self.logger.warning.call_args.args[0])
        self.assertEqual(self.select(mapping={'missing': 'missing_output'}), {})

    def test_outer_dictionary_mapping_and_no_mapping_keep_existing_behaviour(self):
        rain, renamed = object(), object()
        selected = self.select({'rain': rain, 'Rain': renamed}, {'rain': 'Rain'})
        self.assertIs(selected['Rain'], renamed)
        collections = {'time_series': self.frame}
        unchanged = self.select(collections, {})
        self.assertIsNot(unchanged, collections)
        self.assertIs(unchanged['time_series'], self.frame)

    def test_selected_frame_writes_mapped_netcdf_names_and_units(self):
        with tempfile.TemporaryDirectory() as folder, LoggingManager(name='output_test', level=50).use_as_current():
            path = Path(folder) / 'series.nc'
            write_to_file(self.select()['time_series'], str(path), file_format='netcdf',
                          file_type='time_series_hmc', map=self.mapping,
                          units={'discharge_sim_analysis': 'm^3/s', 'discharge_obs': 'm^3/s'})
            with xr.open_dataset(path, engine='netcdf4') as ds:
                self.assertIn('discharge_simulated_analysis', ds)
                self.assertIn('discharge_observed', ds)
                self.assertNotIn('air_temperature_obs', ds)
                self.assertEqual(ds.discharge_simulated_analysis.attrs['units'], 'm^3/s')
                np.testing.assert_allclose(ds.discharge_simulated_analysis, [[1.25, 3.], [2.5, 4.]])

    def test_selected_frame_keeps_source_names_for_custom_json_mapping(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'all.json'
            write_to_file(self.select()['time_series'], str(path), file_format='json', file_type='time_series_hmc',
                          layout='single', map=self.mapping,
                          var_mapping={'discharge_sim_analysis': 'time_series_discharge_simulated'})
            records = json.loads(path.read_text())
            self.assertEqual(len(records), 2)
            self.assertEqual(records[0]['time_series_discharge_simulated'], '1.25,2.5')


if __name__ == '__main__':
    unittest.main()
