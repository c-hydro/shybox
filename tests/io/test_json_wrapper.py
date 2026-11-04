"""Continuum section JSON output through the writer, wrapper and DataLocal."""
import json
from pathlib import Path
import tempfile
import unittest

import numpy as np
import pandas as pd
import xarray as xr

from shybox.io_toolkit.lib_io_json import (
    json_time_series, write_json_time_series, write_json_time_series_collection,
)
from shybox.io_toolkit.json_wrapper import write_json_time_series_sections
from shybox.dataset_toolkit.dataset_handler_local import DataLocal
from shybox.dataset_toolkit.lib_dataset_generic import write_to_file
from shybox.logging_toolkit.logging_handler import LoggingManager


class TestTimeSeriesJSON(unittest.TestCase):
    def setUp(self):
        self.enterContext(LoggingManager(name='json_writer_tests', level=50).use_as_current())
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.times = pd.date_range('2024-11-24 23:00', periods=3, freq='h')
        # Intentionally reverse registry rows to catch positional matching.
        self.registry = pd.DataFrame([
            {'tag': 'foglia:montecchio', 'id': 2, 'section_name': 'Montecchio', 'basin': 'Foglia',
             'domain_name': 'Marche', 'threshold_level_1': 72., 'threshold_level_2': 110.,
             'catchment_area_km2': 605.6, 'hmc_x': 4, 'hmc_y': 5},
            {'tag': 'burano:cagli', 'id': 88, 'section_name': 'CagliPonteCavour', 'basin': 'Burano',
             'domain_name': 'Marche', 'threshold_level_1': 22.9, 'threshold_level_2': 40.7,
             'catchment_area_km2': 129.86, 'hmc_x': 203, 'hmc_y': 194},
        ])
        self.data = pd.DataFrame({
            ('discharge_sim_analysis', 'burano:cagli'): [np.nan, 1.56, 1.55],
            ('discharge_sim_analysis', 'foglia:montecchio'): [2., 3., 4.],
            ('discharge_sim_openloop', 'burano:cagli'): [-9998., 1.4, 1.3],
            ('discharge_obs', 'burano:cagli'): [-9998., -9999., np.nan],
            ('air_temperature_obs', 'burano:cagli'): [-2.5, 2.9, np.inf],
        }, index=self.times)
        self.data.index.name = 'time'
        self.data.attrs['data'] = self.registry
        self.run = {'run_domain': 'marche', 'run_mode': 'deterministic',
                    'run_name': 'nwp_ecmwf0100_realtime_marche', 'run_var': 'deterministic',
                    'time_restart': '202411242300', 'time_start': pd.Timestamp('2024-11-25 00:00')}

    def write(self, **kwargs):
        return write_json_time_series_sections(
            self.data, self.root / 'hydrograph_{section_tag}.json',
            run_metadata=self.run, time=pd.Timestamp('2024-11-27 13:00'), **kwargs)

    def test_example_contract_and_registry_alignment(self):
        paths = self.write()
        payload = json.loads(Path(paths['burano:cagli']).read_text())
        self.assertEqual(payload['section_id'], '88')
        self.assertEqual(payload['section_domain'], 'Burano')
        self.assertEqual(payload['section_name'], 'CagliPonteCavour')
        self.assertEqual(payload['section_idx_ji'], '194,203')
        self.assertEqual(payload['section_discharge_thr_alert'], '22.9')
        self.assertEqual(payload['section_discharge_thr_alarm'], '40.7')
        self.assertEqual(payload['time_period'], '2024-11-24 23:00,2024-11-25 00:00,2024-11-25 01:00')
        self.assertEqual(payload['time_run'], '202411271300')
        self.assertEqual(payload['time_restart'], '202411242300')
        self.assertEqual(payload['time_start'], '202411250000')
        self.assertEqual(payload['time_series_discharge_simulated'], '-9999.0,1.56,1.55')
        self.assertEqual(payload['time_series_discharge_simulated_openloop'], '-9998.0,1.4,1.3')
        self.assertEqual(payload['time_series_discharge_observed'], '-9998.0,-9999.0,-9999.0')
        self.assertEqual(payload['time_series_air_temperature_observed'], '-2.5,2.9,-9999.0')
        self.assertTrue(all(isinstance(value, str) for value in payload.values()))
        for key, value in payload.items():
            if key.startswith('time_series_'):
                self.assertEqual(len(value.split(',')), len(payload['time_period'].split(',')))

    def test_single_collection_has_one_object_per_section(self):
        path = write_json_time_series_sections(self.data, self.root / 'all.json', layout='single')
        payload = json.loads(Path(path).read_text())
        self.assertEqual([record['section_id'] for record in payload], ['88', '2'])
        sections = self.write()
        self.assertEqual(payload[0]['time_period'], json.loads(Path(sections['burano:cagli']).read_text())['time_period'])

    def test_variable_and_metadata_mapping_and_primary_selection(self):
        paths = self.write(data_map={'discharge_sim_openloop': 'time_series_discharge_simulated'},
                           section_map={'section_code': 'id'})
        payload = json.loads(Path(paths['burano:cagli']).read_text())
        self.assertEqual(payload['section_code'], '88')
        self.assertEqual(payload['time_series_discharge_simulated'], '-9998.0,1.4,1.3')
        self.assertNotIn('time_series_air_temperature_observed', payload)

    def test_openloop_only_defaults_to_legacy_simulated_field(self):
        frame = self.data.xs('burano:cagli', axis=1, level=1)[['discharge_sim_openloop']]
        payload = json_time_series(frame)
        self.assertEqual(payload['time_series_discharge_simulated'], '-9998.0,1.4,1.3')

    def test_original_dataframe_and_registry_are_not_modified(self):
        original = self.data.copy(deep=True)
        registry = self.registry.copy(deep=True)
        self.write(decimal_places=1)
        pd.testing.assert_frame_equal(self.data, original)
        pd.testing.assert_frame_equal(self.registry, registry)

    def test_filename_collision_and_unknown_tags_fail_before_writing(self):
        registry = self.registry.copy()
        registry['id'] = 1
        with self.assertRaisesRegex(ValueError, 'collide'):
            write_json_time_series_sections(self.data, self.root / '{section_id}.json', sections_db=registry)
        with self.assertRaisesRegex(ValueError, 'placeholder'):
            write_json_time_series_sections(self.data, self.root / '{unknown}.json')
        with self.assertRaisesRegex(ValueError, 'placeholder'):
            write_json_time_series_sections(self.data, self.root / 'same.json')
        self.assertEqual(list(self.root.iterdir()), [])

    def test_registry_tags_are_required_unique_and_not_positional(self):
        for registry in (self.registry.iloc[:1], pd.concat([self.registry, self.registry.iloc[:1]]),
                         self.registry.drop(columns='tag')):
            with self.subTest(registry=registry.shape), self.assertRaises(ValueError):
                self.write(sections_db=registry)

    def test_atomic_collection_keeps_previous_file_on_failure(self):
        path = self.root / 'all.json'
        path.write_text('previous')
        def records():
            yield {'section_id': '88'}
            raise ValueError('bad section')
        with self.assertRaisesRegex(ValueError, 'bad section'):
            write_json_time_series_collection(path, records())
        self.assertEqual(path.read_text(), 'previous')
        self.assertEqual(list(self.root.iterdir()), [path])

    def test_invalid_values_timestamps_and_mappings_are_rejected(self):
        frame = self.data.xs('burano:cagli', axis=1, level=1).copy()
        frame.index = pd.DatetimeIndex([self.times[0]] * 3)
        with self.assertRaisesRegex(ValueError, 'unique'):
            json_time_series(frame)
        frame.index = self.times
        with self.assertRaisesRegex(ValueError, 'Multiple variables'):
            json_time_series(frame, data_map={'discharge_sim_analysis': 'discharge', 'discharge_sim_openloop': 'discharge'})
        with self.assertRaisesRegex(ValueError, 'not found'):
            json_time_series(frame, data_map={'rain_obs': 'rain_observed'})
        with self.assertRaisesRegex(ValueError, 'exactly one'):
            json_time_series(frame, time=self.times)
        with self.assertRaisesRegex(ValueError, 'finite'):
            json_time_series(frame, data_no_value=np.nan)

    def test_direct_single_section_writer(self):
        path = self.root / 'one.json'
        write_json_time_series(path, self.data.xs('burano:cagli', axis=1, level=1),
                               section_metadata={'section_name': 'CagliPonteCavour'}, run_metadata=self.run)
        self.assertEqual(json.loads(path.read_text())['section_name'], 'CagliPonteCavour')

    def test_datalocal_forwards_options_run_time_and_section_template(self):
        handler = DataLocal(path=str(self.root), file_name='hydrograph_{section_id}.json',
                            file_format='json', file_type='time_series_hmc', file_io='output',
                            file_variable='VARS_TS', file_workflow='TIME_SERIES_WORKFLOW',
                            data_layout='time_series', time_signature='step', message=False,
                            file_args={'run_metadata': self.run, 'decimal_places': 1})
        handler.write_data(self.data, time=pd.Timestamp('2024-11-27 13:00'))
        payload = json.loads((self.root / 'hydrograph_88.json').read_text())
        self.assertEqual(payload['run_name'], self.run['run_name'])
        self.assertEqual(payload['time_run'], '202411271300')
        self.assertEqual(payload['time_series_discharge_simulated'], '-9999.0,1.6,1.6')

    def test_generic_dispatch_single_file_and_append_validation(self):
        path = self.root / 'all.json'
        write_to_file(self.data, str(path), file_format='json', file_type='ts_hmc', layout='single')
        self.assertEqual(len(json.loads(path.read_text())), 2)
        with self.assertRaisesRegex(ValueError, 'append'):
            write_to_file(self.data, str(path), file_format='json', file_type='ts_hmc', append=True)
        with self.assertRaisesRegex(ValueError, 'append'):
            write_to_file(self.data, str(self.root / '{section_tag}/new.json'),
                          file_format='json', file_type='ts_hmc', append=True)
        write_to_file(self.data, str(self.root / '{section_tag}/nested.json'),
                      file_format='json', file_type='ts_hmc')
        self.assertFalse((self.root / '{section_tag}').exists())
        self.assertTrue((self.root / 'burano_cagli/nested.json').is_file())

    def test_netcdf_output_and_generic_dictionary_json_remain_available(self):
        # Use a complete common section set, as produced by the real join process.
        columns = pd.MultiIndex.from_product([['discharge_sim_analysis'], ['burano:cagli', 'foglia:montecchio']])
        frame = self.data.loc[:, columns].copy()
        frame.attrs['data'] = self.registry.iloc[::-1].reset_index(drop=True)
        frame.attrs['type'] = {name: str if dtype == object else dtype.type
                               for name, dtype in frame.attrs['data'].dtypes.items()}
        path = self.root / 'unchanged.nc'
        write_to_file(frame, str(path), file_format='netcdf', file_type='time_series_hmc')
        with xr.open_dataset(path, engine='netcdf4') as ds:
            self.assertEqual(ds['discharge_sim_analysis'].shape, (3, 2))
        plain = self.root / 'plain.json'
        write_to_file({'name': 'unchanged'}, str(plain), file_format='json')
        self.assertEqual(json.loads(plain.read_text()), {'name': 'unchanged'})

    def test_orchestrator_options_and_container_mapping_do_not_leak_into_writer(self):
        handler = DataLocal(path=str(self.root), file_name='workflow_{section_tag}.json',
                            file_format='json', file_type='time_series_hmc', file_io='output',
                            file_variable='VARS_TS', data_layout='time_series', time_signature='step',
                            file_workflow='TIME_SERIES_WORKFLOW',
                            file_args={'run_metadata': self.run, 'data_map': {
                                'discharge_sim_analysis': 'time_series_discharge_simulated'}})
        handler.write_data({'time_series': self.data}, time=pd.Timestamp('2024-11-27 13:00'),
                           out_opts={'intermediate_output': 'Tmp', 'io_config': {'mode': 'many_data_ts_to_one_file'},
                                     'fill_value': -9998}, map={'VARS_TS': 'VARS_TS'}, ref={})
        payload = json.loads((self.root / 'workflow_burano_cagli.json').read_text())
        self.assertEqual(payload['time_series_discharge_simulated'], '-9999.0,1.56,1.55')
        self.assertEqual(payload['time_run'], '202411271300')


if __name__ == '__main__':
    unittest.main()
