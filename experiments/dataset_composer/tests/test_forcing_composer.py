"""Forcing time windows, source coverage, model layouts and safe publication."""
from copy import deepcopy
import gzip
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch, MagicMock

import numpy as np
import pandas as pd
import xarray as xr

from shybox.runner_toolkit.datasets.datasets_handler import compose_datasets, plan_datasets_deterministic, MANDATORY
from shybox.runner_toolkit.datasets.datasets_handler import time_configure
from shybox.logging_toolkit.logging_handler import LoggingManager


class TestForcingComposer(unittest.TestCase):
    def setUp(self):
        self.temp = self.enterContext(tempfile.TemporaryDirectory())
        self.root = Path(self.temp)
        self.enterContext(patch.dict('os.environ', {'PATH_TMP': self.temp}))
        self.enterContext(LoggingManager(name='forcing_tests', level=50).use_as_current())
        self.x, self.y = [12., 13.], [44., 43.]
        self.values = np.array([[.25, .5], [1., 2.]])
        xr.Dataset({'Terrain': (('latitude', 'longitude'), [[10., 20.], [30., 40.]])},
                   coords={'latitude': self.y, 'longitude': self.x}).to_netcdf(self.root / 'geo.nc')
        self.cfg = {
            'model': 'hmc',
            'time': {'rounding': '30min', 'frequency': '1h', 'observed': {'lookback': '1h'},
                     'forecast': {'n_steps_model': 11, 'horizon_reference': 'midnight'}},
            'geo': {'folder_name': self.temp, 'file_name': 'geo.nc'},
            'options': {},
            'variables': {k: {'output_name': k, 'units': 'test'} for k in MANDATORY},
            'sources': {g: {k: {'folder_name': self.temp, 'file_name': g + '.{time:%H}.nc',
                               'variable': k, 'format': 'netcdf', 'mode': 'deterministic', 'ensemble': None,
                               'tolerance_steps': 0, 'operations': {'interpolate': None, 'split': {'method': '3d_to_2d'}}} for k in MANDATORY} for g in ('observed', 'forecast')},
            'destination': {'folder_name': str(self.root / 'forcing'), 'file_name': 'forcing.{time:%H}.nc'}}
        for group, hours in (('observed', [9]), ('forecast', [10, 11])):
            for hour in hours:
                self.source(group, hour)

    def source(self, group, hour, omit=(), x=None, y=None, values=None, multi=False):
        data = self.values if values is None else values
        time = pd.Timestamp(f'2026-11-09 {hour:02}:00')
        variables = {k: (('latitude', 'longitude'), data.copy()) for k in MANDATORY - set(omit)}
        ds = xr.Dataset(variables, coords={'longitude': self.x if x is None else x,
                                         'latitude': self.y if y is None else y})
        if multi:
            ds = xr.concat([ds, ds * 2], dim=pd.Index([time, time + pd.Timedelta('1h')], name='time'))
        else:
            ds = ds.expand_dims(time=[time])
        ds.to_netcdf(self.root / f'{group}.{hour:02}.nc')

    def run_compose(self):
        return compose_datasets(self.cfg, '2026-11-09 09:35')

    def test_rounding_midnight_horizon_and_observed_floor(self):
        cfg = deepcopy(self.cfg)
        cfg['time']['observed'] = {'lookback': '2d', 'floor_start': 'D'}
        cfg['time']['forecast']['n_steps_model'] = 72
        plan = plan_datasets_deterministic(cfg, '2026-11-09 09:35')
        self.assertEqual(plan.time_run, pd.Timestamp('2026-11-09 09:30'))
        self.assertEqual(plan.forecast_run, pd.Timestamp('2026-11-09 09:00'))
        self.assertEqual(plan.observed[0], pd.Timestamp('2026-11-07'))
        self.assertEqual(plan.observed[-1], pd.Timestamp('2026-11-09 09:00'))
        self.assertEqual(len(plan.forecast), 72 - 9)
        self.assertEqual(plan.forecast[0], pd.Timestamp('2026-11-09 10:00'))
        self.assertEqual(plan.forecast[-1], pd.Timestamp('2026-11-12'))

    def test_forecast_issue_anchor_and_half_hour_frequency(self):
        cfg = deepcopy(self.cfg)
        cfg['time']['forecast'].update(n_steps_model=4, horizon_reference='forecast_run')
        cfg['time']['frequency'] = '30min'
        plan = plan_datasets_deterministic(cfg, '2026-11-09 09:35')
        self.assertEqual(plan.observed[-1], plan.time_run)
        self.assertEqual(len(plan.forecast), 3)
        self.assertEqual(plan.forecast[-1], pd.Timestamp('2026-11-09 11:00'))

    def test_update_time_composes_forecast_at_missing_observed_validity(self):
        from shybox.runner_toolkit.datasets.datasets_handler import DatasetManager
        (self.root / 'observed.09.nc').unlink()
        self.source('forecast', 9, values=self.values * 3)
        manager = DatasetManager.from_settings(self.cfg, '2026-11-09 09:35')
        manager.update_time(n_steps_observed=0, n_steps_forecast=2)
        self.assertEqual(list(manager.time_table.time_step), [0, 1, 2])
        self.assertTrue((manager.time_table.time_mode == 'frc').all())
        output = manager.run()
        manifest = json.loads((output / 'manifest.json').read_text())
        record = manifest['files'][0]
        self.assertEqual(record['time'], '2026-11-09 09:00:00')
        self.assertEqual(record['group'], 'forecast')
        self.assertTrue(record['variables']['rain']['source'].endswith('forecast.09.nc'))
        with xr.open_dataset(output / 'forcing.09.nc') as dataset:
            np.testing.assert_allclose(dataset.rain.values.squeeze(), (self.values * 3)[::-1])

    def test_group_destination_filenames_match_plan_and_published_files(self):
        from shybox.runner_toolkit.datasets.datasets_handler import DatasetManager
        manager = DatasetManager.from_settings(self.cfg, '2026-11-09 09:35')
        manager.configure_destination(
            file_name_observed='obs/observed.{time:%H}.nc',
            file_name_forecast='frc/forecast.{time:%H}.nc')
        planned = set(manager.plan_files().file_destination)
        output = manager.run()
        published = {str(path) for path in output.rglob('*.nc')}
        self.assertEqual(planned, published)
        self.assertEqual({Path(path).name for path in published},
                         {'observed.09.nc', 'forecast.10.nc', 'forecast.11.nc'})

    def test_hmc_roundtrip_and_manifest(self):
        output = self.run_compose()
        manifest = json.loads((output / 'manifest.json').read_text())
        self.assertEqual([f['group'] for f in manifest['files']], ['observed', 'forecast', 'forecast'])
        self.assertEqual(manifest['status'], 'complete')
        with xr.open_dataset(output / 'forcing.10.nc') as ds:
            self.assertEqual(ds.rain.dims, ('south_north', 'west_east'))
            np.testing.assert_allclose(ds.rain, np.flipud(self.values))
        with self.assertRaises(FileExistsError):
            self.run_compose()

    def test_s3m_roundtrip(self):
        self.cfg['model'] = 's3m'
        output = self.run_compose()
        with xr.open_dataset(output / 'forcing.10.nc') as ds:
            self.assertEqual(ds.rain.dims, ('Y', 'X'))
            self.assertEqual(ds.rain.attrs['units'], 'test')
            np.testing.assert_allclose(ds.rain, np.flipud(self.values))

    def test_missing_first_forecast_step_fails_even_with_persistence(self):
        self.cfg['sources']['forecast']['rain'].update(tolerance_steps=5, missing_policy='persist')
        self.source('forecast', 10, omit=['rain'])
        with self.assertRaisesRegex(ValueError, 'first step is mandatory'):
            self.run_compose()
        self.assertFalse((self.root / 'forcing').exists())
        self.assertEqual(list(self.root.glob('.forcing-stage-*')), [])

    def test_missing_first_observed_step_fails(self):
        (self.root / 'observed.09.nc').unlink()
        with self.assertRaisesRegex(ValueError, 'observed.*rain|observed.*first step'):
            self.run_compose()

    def test_one_missing_step_can_persist_with_provenance(self):
        self.cfg['sources']['forecast']['rain'].update(tolerance_steps=1, missing_policy='persist')
        self.source('forecast', 11, omit=['rain'])
        output = self.run_compose()
        manifest = json.loads((output / 'manifest.json').read_text())
        self.assertEqual(manifest['files'][-1]['variables']['rain']['status'], 'persisted')
        with xr.open_dataset(output / 'forcing.11.nc') as ds:
            np.testing.assert_allclose(ds.rain, np.flipud(self.values))

    def test_tolerance_exceeded_does_not_publish_partial_run(self):
        self.cfg['time']['forecast']['n_steps_model'] = 12
        self.cfg['sources']['forecast']['rain'].update(tolerance_steps=1, missing_policy='persist')
        self.source('forecast', 11, omit=['rain'])
        self.source('forecast', 12, omit=['rain'])
        with self.assertRaisesRegex(ValueError, 'gaps=2'):
            self.run_compose()
        self.assertFalse((self.root / 'forcing').exists())

    def test_tolerated_nodata_and_optional_extra_variable(self):
        self.cfg['sources']['forecast']['rain'].update(tolerance_steps=1, missing_policy='nodata')
        self.cfg['variables']['pressure'] = {'output_name': 'Pressure', 'units': 'hPa'}
        self.source('forecast', 11, omit=['rain'])
        output = self.run_compose()
        with xr.open_dataset(output / 'forcing.11.nc') as ds:
            self.assertTrue(ds.rain.isnull().all())
            self.assertTrue(ds.Pressure.isnull().all())

    def test_grid_mismatch_fails_without_interpolation(self):
        self.source('forecast', 11, x=[11.9, 13.1], y=[44.1, 42.9])
        with self.assertRaisesRegex(ValueError, 'grid differs'):
            self.run_compose()

    def test_interpolation_and_axis_reordering(self):
        for variable in (source for group in self.cfg['sources'].values() for source in group.values()):
            variable['operations']['interpolate'] = {'method': 'nearest', 'roi': None}
        self.source('forecast', 10, x=[11.9, 13.1], y=[44.1, 42.9])
        self.source('forecast', 11, x=self.x[::-1], y=self.y[::-1], values=self.values[::-1, ::-1])
        output = self.run_compose()
        for hour in (10, 11):
            with xr.open_dataset(output / f'forcing.{hour}.nc') as ds:
                np.testing.assert_allclose(ds.rain, np.flipud(self.values))

    def test_3d_splitting_selects_exact_valid_time(self):
        self.source('forecast', 10, multi=True)
        for source in self.cfg['sources']['forecast'].values():
            source['file_name'] = 'forecast.10.nc'
        output = self.run_compose()
        with xr.open_dataset(output / 'forcing.11.nc') as ds:
            np.testing.assert_allclose(ds.rain, 2 * np.flipud(self.values))

    def test_singleton_time_dimension_retained_when_split_disabled(self):
        for variable in (source for group in self.cfg['sources'].values() for source in group.values()):
            variable['operations']['split'] = None
        output = self.run_compose()
        with xr.open_dataset(output / 'forcing.10.nc') as ds:
            self.assertEqual(ds.rain.dims, ('time', 'south_north', 'west_east'))

    def test_gzip_inputs_outputs(self):
        for group in ('observed', 'forecast'):
            for source in self.cfg['sources'][group].values():
                source['file_name'] += '.gz'
        for path in self.root.glob('observed.*.nc'):
            with gzip.open(str(path) + '.gz', 'wb') as stream:
                stream.write(path.read_bytes())
        for path in self.root.glob('forecast.*.nc'):
            with gzip.open(str(path) + '.gz', 'wb') as stream:
                stream.write(path.read_bytes())
        self.cfg['destination']['file_name'] += '.gz'
        output = self.run_compose()
        with gzip.open(output / 'forcing.10.nc.gz', 'rb') as stream:
            self.assertTrue(stream.read(8).startswith(b'\x89HDF'))

    def test_model_ready_as_is_copy_and_observed_forecast_folders(self):
        from shybox.runner_toolkit.datasets import datasets_handler as handler
        ref = xr.DataArray([[10., 20.], [30., 40.]], dims=('latitude', 'longitude'),
                           coords={'latitude': self.y, 'longitude': self.x})
        for source_group in self.cfg['sources'].values():
            for source in source_group.values():
                source.update(as_is=True, operations={'split': None, 'interpolate': None})
        self.cfg['destination']['file_name'] = '{time_mode}/forcing.{time:%H}.nc'
        for model in ('hmc', 's3m'):
            self.cfg['model'] = model
            self.cfg['destination']['folder_name'] = str(self.root / ('as_is_' + model))
            for group, hours in (('observed', [9]), ('forecast', [10, 11])):
                for hour in hours:
                    handler._write_step(self.root / f'{group}.{hour:02}.nc',
                                        {key: self.values for key in MANDATORY}, ref,
                                        pd.Timestamp(f'2026-11-09 {hour:02}:00'), model, self.cfg['variables'])
            with patch.object(handler, '_write_step', side_effect=AssertionError('as_is must not re-encode data')):
                output = self.run_compose()
            self.assertEqual((output / 'obs/forcing.09.nc').read_bytes(), (self.root / 'observed.09.nc').read_bytes())
            self.assertEqual((output / 'frc/forcing.10.nc').read_bytes(), (self.root / 'forecast.10.nc').read_bytes())
            manifest = json.loads((output / 'manifest.json').read_text())
            self.assertTrue(all(record['as_is'] for record in manifest['files']))

    def test_as_is_rejects_requested_transformations(self):
        for source_group in self.cfg['sources'].values():
            for source in source_group.values():
                source['as_is'] = True
        with self.assertRaisesRegex(ValueError, 'without split/interpolate'):
            self.run_compose()

    def test_output_collisions_and_path_escape_fail(self):
        for template in ('same.nc', '../escape.{time:%H}.nc'):
            self.cfg['destination']['file_name'] = template
            with self.assertRaisesRegex(ValueError, 'unique and inside'):
                self.run_compose()
            self.assertFalse((self.root / 'forcing').exists())

    def test_nonfinite_fields_and_wrong_units_fail(self):
        self.source('forecast', 11, values=np.full((2, 2), np.inf))
        with self.assertRaisesRegex(ValueError, 'nonfinite'):
            self.run_compose()
        self.source('forecast', 11)
        path = self.root / 'forecast.11.nc'
        with xr.open_dataset(path) as source:
            ds = source.load()
        ds.rain.attrs['units'] = 'K'
        ds.to_netcdf(path)
        with self.assertRaisesRegex(ValueError, 'units'):
            self.run_compose()

    def test_example_configurations_use_shared_manager_and_preserve_runtime_dates(self):
        from workflow.runner.datasets.app_runner_workflow_hmc_datasets_main import load_configuration
        env = {'DOMAIN_NAME': 'marche', 'PATH_GEO': self.temp, 'PATH_OBS': self.temp,
               'PATH_FRC_NWP': self.temp, 'PATH_FRC_RFARM': self.temp, 'PATH_DST': self.temp, 'PATH_LOG': self.temp}
        root = Path(__file__).resolve().parents[2] / 'workflow/runner/datasets'
        with patch.dict('os.environ', env):
            for model in ('hmc', 's3m'):
                name = 'app_runner_workflow_hmc_datasets_forcing_deterministic.json' if model == 'hmc' else 'app_runner_workflow_s3m_datasets_forcing.json'
                app, cfg = load_configuration(str(root / name))
                self.assertEqual(cfg['model'], model)
                self.assertEqual(cfg['time']['hour_reference_forecast'], '00:00')
                self.assertIn('{path_time_observed}', cfg['sources']['observed']['rain']['folder_name'])
                self.assertIn('{path_time_forecast_nwp}', cfg['sources']['forecast']['rain']['folder_name'])
                self.assertEqual(app['log']['path'], self.temp)
                self.assertEqual(len(plan_datasets_deterministic(cfg, '2026-11-09 09:35').forecast), 63)

    def test_shared_source_group_formats_resolve_observed_issue_and_member_times(self):
        from workflow.runner.datasets.app_runner_workflow_hmc_datasets_main import load_configuration
        from shybox.runner_toolkit.datasets.datasets_handler import _path
        root = Path(__file__).resolve().parents[2] / 'workflow/runner/datasets'
        payload = json.loads((root / 'app_runner_workflow_hmc_datasets_forcing_probabilistic.json').read_text())
        payload['configuration']['variables']['lut']['user']['hour_reference_forecast'] = None
        templates = payload['configuration']['variables']['template']
        templates.update(path_time_observed='%Y-%j', file_time_observed='%H%M',
                         path_time_forecast_nwp='%Y/%m/%d', file_time_forecast_nwp='%Y%m%d_%H')
        env = {'DOMAIN_NAME': 'marche', 'PATH_GEO': self.temp, 'PATH_OBS': self.temp,
               'PATH_FRC_NWP': self.temp, 'PATH_FRC_RFARM': self.temp, 'PATH_DST': self.temp, 'PATH_LOG': self.temp}
        with patch.dict('os.environ', env):
            _, cfg = load_configuration(payload)
        plan = plan_datasets_deterministic(cfg, '2026-11-09 09:35')
        for key in cfg['variables']:
            path = _path(cfg['sources']['observed'][key], plan, plan.observed[-1], cfg['time_tags'])
            self.assertEqual(path, self.root / '2026-313/ws.db.0900.nc')
        path = _path(cfg['sources']['forecast']['air_t'], plan, plan.forecast[0], cfg['time_tags'])
        self.assertEqual(path, self.root / '2026/11/09/nwp.ecmwf0100.20261109_09.nc')
        path = _path(cfg['sources']['forecast']['rain'], plan, plan.forecast[0],
                     cfg['time_tags'], ensemble_member='001')
        self.assertEqual(path, self.root / '2026/11/rf.ecmwf0100.202611090900_001.nc.gz')
        path = _path(cfg['destination'], plan, plan.forecast[0], cfg['time_tags'])
        self.assertEqual(path, self.root / 'probabilistic_ensemble/202611090900/frc/2026/11/09/hmc.forcing-grid.202611091000.nc.gz')

    def test_precomputed_main_time_plan_is_used_without_replanning(self):
        plan = plan_datasets_deterministic(self.cfg, '2026-11-09 09:35')
        with patch('shybox.runner_toolkit.datasets.datasets_handler.plan_datasets_deterministic',
                   side_effect=AssertionError('The main already computed the time windows.')):
            output = compose_datasets(self.cfg, '2026-11-09 09:35', plan=plan)
        self.assertTrue((output / 'manifest.json').exists())

    def test_individual_source_mappings_combine_into_one_netcdf(self):
        from workflow.runner.datasets.app_runner_workflow_hmc_datasets_main import load_configuration
        root = Path(__file__).resolve().parents[2] / 'workflow/runner/datasets'
        payload = json.loads((root / 'app_runner_workflow_hmc_datasets_forcing_deterministic.json').read_text())
        app, declarations = payload['configuration']['application'], payload['configuration']['variables']
        declarations['lut']['user'].update(hour_reference_forecast=None, time_look_observed='0h',
                                           time_horizon_observed=None, time_look_forecast='2h',
                                           time_horizon_forecast=None, file_time_forecast_nwp_valid=None)
        declarations['format']['file_time_forecast_nwp_valid'] = 'time'
        declarations['template'].update(file_time_observed='%H', file_time_forecast_nwp_valid='%H')
        app['geo']['terrain'].update(path=self.temp, file_name='geo.nc', format='netcdf')
        for period in ('observed', 'forecast'):
            self.assertEqual(set(app['data_source'][period]), MANDATORY)
            for key, source in app['data_source'][period].items():
                source['as_is'] = False
                source['path'] = self.temp
                tag = 'file_time_observed' if period == 'observed' else 'file_time_forecast_nwp_valid'
                source['file_name'] = period + '.{' + tag + '}.nc'
                source['variable'] = {key: key}
        app['data_destination'].update(path=str(self.root / 'mapped'), file_name='forcing.{file_time_destination}.nc',
                                       variables={key: 'OUTPUT_' + key.upper() for key in MANDATORY},
                                       units={key: 'test' for key in MANDATORY})
        declarations['template']['file_time_destination'] = '%H'
        env = {'DOMAIN_NAME': 'marche', 'PATH_GEO': self.temp, 'PATH_OBS': self.temp,
               'PATH_FRC_NWP': self.temp, 'PATH_DST': self.temp, 'PATH_LOG': self.temp}
        with patch.dict('os.environ', env):
            _, settings = load_configuration(payload)
        self.assertNotIn('variables', app)
        output = compose_datasets(settings, '2026-11-09 09:35')
        with xr.open_dataset(output / 'forcing.10.nc') as ds:
            self.assertNotIn('rain', ds)
            for key in MANDATORY:
                np.testing.assert_allclose(ds['OUTPUT_' + key.upper()], np.flipud(self.values))

    def test_source_mapping_requires_destination_key_and_rejects_duplicates(self):
        from workflow.runner.datasets.app_runner_workflow_hmc_datasets_main import load_configuration
        root = Path(__file__).resolve().parents[2] / 'workflow/runner/datasets'
        original = json.loads((root / 'app_runner_workflow_hmc_datasets_forcing_deterministic.json').read_text())
        env = {'DOMAIN_NAME': 'marche', 'PATH_GEO': self.temp, 'PATH_OBS': self.temp,
               'PATH_FRC_NWP': self.temp, 'PATH_DST': self.temp, 'PATH_LOG': self.temp}
        for mapping, message in (({'RAIN': 'unknown'}, 'must be a key'),
                                 ({'RAIN': 'rain', 'PRECIP': 'rain'}, 'exactly one source field'),
                                 ('RAIN', 'must map source field names')):
            payload = deepcopy(original)
            payload['configuration']['application']['data_source']['forecast']['rain']['variable'] = mapping
            with patch.dict('os.environ', env), self.assertRaisesRegex(ValueError, message):
                load_configuration(payload)

    def test_main_computes_observed_and_forecast_windows_once(self):
        from workflow.runner.datasets import app_runner_workflow_hmc_datasets_main as entry
        application = {'info': {'model': 'hmc'}, 'time': self.cfg['time'],
                       'log': {'path': self.temp, 'file_name': 'main.log'},
                       'geo': {'terrain': {**self.cfg['geo'], 'path': self.temp}},
                       'data_destination': {**self.cfg['destination'], 'path': self.cfg['destination']['folder_name'],
                                            'format': 'netcdf', 'type': 'hmc',
                                            'variables': {key: value['output_name'] for key, value in self.cfg['variables'].items()},
                                            'units': {key: value['units'] for key, value in self.cfg['variables'].items()}},
                       'data_source': {group: {key: {**source, 'path': self.temp, 'variable': {source['variable']: key}}
                                                for key, source in sources.items()}
                                       for group, sources in self.cfg['sources'].items()}}
        configuration = MagicMock()
        configuration.get_section.side_effect = lambda section, **kwargs: self.cfg['time'] if section == 'time' else {}
        configuration.fill_obj_from_lut.side_effect = lambda section, **kwargs: section
        with patch.object(entry, 'ArgumentsManager') as arguments, \
                patch.object(entry.ConfigManager, 'from_source', return_value=configuration), \
                patch.object(entry, 'load_configuration', return_value=(
                    application, self.cfg)), \
                patch('shybox.runner_toolkit.datasets.datasets_handler.time_configure', wraps=time_configure) as planner, \
                patch('shybox.runner_toolkit.datasets.datasets_handler.compose_datasets') as composer:
            arguments.return_value.get.return_value = ('config.json', '2026-11-09 09:35')
            entry.main()
        self.assertEqual(planner.call_count, 1)
        self.assertEqual(planner.call_args.args[0]['time'], self.cfg['time'])
        self.assertEqual(planner.call_args.args[1], '2026-11-09 09:35')
        configuration.get_section.assert_any_call(root_key='application', section='time', raise_if_missing=True)
        plan = composer.call_args.kwargs['plan']
        self.assertEqual(list(plan.observed), [pd.Timestamp('2026-11-09 09:00')])
        self.assertEqual(len(plan.forecast), 2)

    def test_ascii_terrain_grid_and_nodata_mask(self):
        path = self.root / 'geo.asc'
        path.write_text('ncols 2\nnrows 2\nxllcorner 11.5\nyllcorner 42.5\ncellsize 1\nNODATA_value -9999\n10 20\n30 -9999\n')
        self.cfg['geo'].update(file_name='geo.asc', format='ascii')
        output = self.run_compose()
        with xr.open_dataset(output / 'forcing.10.nc') as ds:
            expected = np.flipud(self.values.copy())
            expected[0, 1] = np.nan
            np.testing.assert_allclose(ds.rain, expected, equal_nan=True)

    def test_missing_time_in_cube_is_not_substituted_with_nearest_time(self):
        for source in self.cfg['sources']['forecast'].values():
            source['file_name'] = 'forecast.11.nc'
        with self.assertRaisesRegex(ValueError, 'timestamp missing'):
            self.run_compose()

    def test_unit_conversion_is_explicit(self):
        for source in self.cfg['sources']['forecast'].values():
            source['scale'], source['offset'] = 2., 1.
        output = self.run_compose()
        with xr.open_dataset(output / 'forcing.10.nc') as ds:
            np.testing.assert_allclose(ds.rain, 2 * np.flipud(self.values) + 1)

    def test_definitions_and_tolerance_validation(self):
        self.cfg['variables'].pop('rain')
        with self.assertRaisesRegex(ValueError, 'Missing mandatory'):
            self.run_compose()
        self.cfg['variables']['rain'] = {'output_name': 'rain', 'units': 'mm'}
        self.cfg['sources']['forecast']['rain']['tolerance_steps'] = -1
        with self.assertRaisesRegex(ValueError, 'tolerance_steps'):
            self.run_compose()

    def test_interpolation_does_not_extrapolate_outside_source_extent(self):
        for variable in (source for group in self.cfg['sources'].values() for source in group.values()):
            variable['operations']['interpolate'] = {'method': 'nearest', 'roi': None}
        self.source('forecast', 10, x=[12.1, 12.9], y=[43.9, 43.1])
        with self.assertRaisesRegex(ValueError, 'missing/nonfinite'):
            self.run_compose()

    def test_operations_default_to_none_for_both_models(self):
        for variable in (source for group in self.cfg['sources'].values() for source in group.values()):
            variable.pop('operations')
        for model in ('hmc', 's3m'):
            with self.subTest(model=model):
                self.cfg['model'] = model
                self.cfg['destination']['folder_name'] = str(self.root / model)
                output = self.run_compose()
                with xr.open_dataset(output / 'forcing.10.nc') as ds:
                    self.assertEqual(ds.rain.ndim, 3)
                    np.testing.assert_allclose(ds.rain.values[0], np.flipud(self.values))

    def test_split_is_per_variable_with_mixed_2d_and_3d_outputs(self):
        for variable in (source for group in self.cfg['sources'].values() for source in group.values()):
            variable['operations']['split'] = None
        for source in self.cfg['sources']['forecast'].values():
            source['operations'] = {'split': {'method': '3d_to_2d'}} if source['variable'] == 'rain' else {}
        for model in ('hmc', 's3m'):
            self.cfg['model'] = model
            self.cfg['destination']['folder_name'] = str(self.root / model)
            output = self.run_compose()
            with xr.open_dataset(output / 'forcing.10.nc') as ds:
                self.assertEqual(ds.rain.ndim, 2)
                self.assertEqual(ds.air_t.ndim, 3)

    def test_cube_rejected_without_explicit_split(self):
        self.cfg['sources']['forecast']['rain']['operations']['split'] = None
        self.source('forecast', 10, multi=True)
        with self.assertRaisesRegex(ValueError, 'split.method'):
            self.run_compose()

    def test_interpolation_and_roi_are_per_variable(self):
        path = self.root / 'rain.nc'
        xr.Dataset({'rain': (('latitude', 'longitude'), self.values)},
                   coords={'latitude': [44.01, 42.99], 'longitude': [11.99, 13.01]}).to_netcdf(path)
        self.cfg['sources']['forecast']['rain']['file_name'] = path.name
        self.cfg['sources']['forecast']['rain']['operations']['interpolate'] = {'method': 'nn', 'roi': 2000}
        output = self.run_compose()
        with xr.open_dataset(output / 'forcing.10.nc') as ds:
            np.testing.assert_allclose(ds.rain, np.flipud(self.values))
        self.cfg['destination']['folder_name'] = str(self.root / 'too_far')
        self.cfg['sources']['forecast']['rain']['operations']['interpolate']['roi'] = 10
        with self.assertRaisesRegex(ValueError, 'missing/nonfinite'):
            self.run_compose()
        self.assertFalse((self.root / 'too_far').exists())

    def test_operations_and_format_validation(self):
        for operations in ({'interpolate': True}, {'split': True},
                           {'split': {'method': 'bad'}}, {'interpolate': {'method': 'nn', 'roi': -1}},
                           {'interpolate': {'method': 'bad'}}, {'unknown': None}):
            with self.subTest(operations=operations):
                self.cfg['sources']['forecast']['rain']['operations'] = operations
                with self.assertRaises(ValueError):
                    self.run_compose()
        self.cfg['sources']['forecast']['rain']['operations'] = {}
        self.cfg['sources']['forecast']['rain']['format'] = 'grib'
        with self.assertRaisesRegex(ValueError, 'accepted formats'):
            self.run_compose()

    def test_ascii_and_tiff_sources(self):
        import rasterio
        from rasterio.transform import from_origin
        for file_format in ('ascii', 'tiff'):
            suffix = 'asc' if file_format == 'ascii' else 'tif'
            path = self.root / ('rain.' + suffix)
            if file_format == 'ascii':
                path.write_text('ncols 2\nnrows 2\nxllcorner 11.5\nyllcorner 42.5\ncellsize 1\nNODATA_value -9999\n.25 .5\n1 2\n')
            else:
                with rasterio.open(path, 'w', driver='GTiff', height=2, width=2, count=1,
                                   dtype='float32', crs='EPSG:4326', transform=from_origin(11.5, 44.5, 1, 1)) as raster:
                    raster.write(self.values.astype('float32'), 1)
            self.cfg['sources']['forecast']['rain'].update(format=file_format, file_name=path.name)
            self.cfg['destination']['folder_name'] = str(self.root / file_format)
            output = self.run_compose()
            with xr.open_dataset(output / 'forcing.10.nc') as ds:
                np.testing.assert_allclose(ds.rain, np.flipud(self.values))

    def test_raw_binary_layout_scaling_and_byte_count(self):
        path = self.root / 'rain.bin'
        path.write_bytes(b'HEAD' + (self.values * 4).astype('<i2').ravel(order='F').tobytes())
        source = self.cfg['sources']['forecast']['rain']
        source.update(format='binary', file_name=path.name,
                      binary={'dtype': '<i2', 'order': 'F', 'header_bytes': 4},
                      grid={'reference': 'terrain'}, scale=.25)
        output = self.run_compose()
        with xr.open_dataset(output / 'forcing.10.nc') as ds:
            np.testing.assert_allclose(ds.rain, np.flipud(self.values))
        self.cfg['destination']['folder_name'] = str(self.root / 'invalid_binary')
        path.write_bytes(b'HEAD')
        with self.assertRaisesRegex(ValueError, 'byte count'):
            self.run_compose()

    def test_binary_cannot_implicitly_assume_terrain_grid(self):
        (self.root / 'rain.bin').write_bytes(self.values.tobytes())
        self.cfg['sources']['forecast']['rain'].update(format='binary', file_name='rain.bin')
        with self.assertRaisesRegex(ValueError, 'explicit grid'):
            self.run_compose()

    def test_integer_scale_metadata_preserves_fractions_and_missing_values(self):
        from netCDF4 import Dataset
        from shybox.runner_toolkit.datasets.datasets_handler import _open_dataset
        path = self.root / 'legacy.nc'
        values = np.array([[.25, .5], [np.nan, 1.75]])
        xr.Dataset({'Rain': (('latitude', 'longitude'), values)},
                   coords={'latitude': self.y, 'longitude': self.x}).to_netcdf(path)
        with Dataset(path, 'a') as ds:
            ds.variables['Rain'].setncattr('scale_factor', np.int64(1))
        with _open_dataset(path) as ds:
            self.assertTrue(np.issubdtype(ds.Rain.dtype, np.floating))
            np.testing.assert_allclose(ds.Rain, values, equal_nan=True)

    def test_compressed_cube_is_decompressed_once_per_member(self):
        from shybox.runner_toolkit.datasets import datasets_handler as composer
        self.source('forecast', 10, multi=True)
        path = self.root / 'forecast.10.nc.gz'
        with gzip.open(path, 'wb') as stream:
            stream.write((self.root / 'forecast.10.nc').read_bytes())
        for source in self.cfg['sources']['forecast'].values():
            source['file_name'] = path.name
        with patch.object(composer, '_uncompressed_file', wraps=composer._uncompressed_file) as unzip:
            self.run_compose()
        self.assertEqual(sum(call.args[0] == path for call in unzip.call_args_list), 1)

    def probabilistic_rain(self):
        self.cfg['run_mode'] = 'probabilistic_ensemble'
        self.cfg['sources']['forecast']['rain'].update(mode='probabilistic', ensemble={'start': 1, 'end': 2, 'format': '03d'})
        self.cfg['sources']['forecast']['rain']['file_name'] = 'rain.{ensemble_member}.{time:%H}.nc'
        self.cfg['destination']['ensemble_folder'] = 'run_{ensemble_member}'
        for member in ('001', '002'):
            for hour in (10, 11):
                with xr.open_dataset(self.root / f'forecast.{hour}.nc') as ds:
                    rain = (ds[['rain']] * int(member)).load()
                rain.to_netcdf(self.root / f'rain.{member}.{hour}.nc')

    def test_probabilistic_rain_and_shared_deterministic_fields_for_both_models(self):
        self.probabilistic_rain()
        for model in ('hmc', 's3m'):
            self.cfg['model'] = model
            self.cfg['destination']['folder_name'] = str(self.root / model)
            output = self.run_compose()
            manifest = json.loads((output / 'manifest.json').read_text())
            self.assertEqual(manifest['ensemble_members'], ['001', '002'])
            self.assertEqual(manifest['run_mode'], 'probabilistic_ensemble')
            for member in ('001', '002'):
                with xr.open_dataset(output / f'run_{member}/forcing.10.nc') as ds:
                    np.testing.assert_allclose(ds.rain, int(member) * np.flipud(self.values))
                    np.testing.assert_allclose(ds.air_t, np.flipud(self.values))
                with xr.open_dataset(output / f'run_{member}/forcing.09.nc') as ds:
                    np.testing.assert_allclose(ds.rain, np.flipud(self.values))

    def test_missing_member_rolls_back_complete_ensemble_publication(self):
        self.probabilistic_rain()
        (self.root / 'rain.002.11.nc').unlink()
        with self.assertRaisesRegex(ValueError, 'file missing'):
            self.run_compose()
        self.assertFalse((self.root / 'forcing').exists())
        self.assertEqual(list(self.root.glob('.forcing-ensemble-*')), [])

    def test_member_ranges_and_deterministic_source_mode_validation(self):
        from shybox.runner_toolkit.datasets.datasets_handler import plan_datasets_probabilistic
        self.probabilistic_rain()
        self.cfg['sources']['forecast']['air_t'].update(mode='probabilistic', ensemble={'start': 1, 'end': 3, 'format': '03d'})
        self.cfg['sources']['forecast']['air_t']['file_name'] = 'air.{ensemble_member}.nc'
        with self.assertRaisesRegex(ValueError, 'same ordered ensemble'):
            plan_datasets_probabilistic(self.cfg)
        self.cfg['sources']['forecast']['air_t']['mode'] = 'deterministic'
        with self.assertRaisesRegex(ValueError, 'deterministic variable'):
            plan_datasets_probabilistic(self.cfg)

    def test_probabilistic_jsons_define_lut_times_modes_and_member_formats(self):
        from workflow.runner.datasets.app_runner_workflow_hmc_datasets_main import load_configuration
        from shybox.runner_toolkit.datasets.datasets_handler import plan_datasets_probabilistic
        root = Path(__file__).resolve().parents[2] / 'workflow/runner/datasets'
        env = {'DOMAIN_NAME': 'marche', 'PATH_GEO': self.temp, 'PATH_OBS': self.temp,
               'PATH_FRC_NWP': self.temp, 'PATH_FRC_RFARM': self.temp, 'PATH_DST': self.temp, 'PATH_LOG': self.temp}
        with patch.dict('os.environ', env):
            for model in ('hmc',):
                file = root / f'app_runner_workflow_{model}_datasets_forcing_probabilistic.json'
                raw = json.loads(file.read_text())
                self.assertNotIn('{time:', file.read_text())
                _, cfg = load_configuration(str(file))
                mode, members = plan_datasets_probabilistic(cfg)
                self.assertEqual(mode, 'probabilistic_ensemble')
                self.assertEqual(members, [f'{member:03d}' for member in range(1, 11)])
                self.assertEqual(cfg['sources']['forecast']['air_t']['mode'], 'deterministic')
                for group in ('observed', 'forecast_nwp', 'forecast_rfarm'):
                    for prefix in ('path', 'file'):
                        tag = f'{prefix}_time_{group}'
                        self.assertIn(tag, raw['configuration']['variables']['lut']['user'])
                        self.assertEqual(raw['configuration']['variables']['format'][tag], 'time')
                for section in ('format', 'template'):
                    self.assertFalse(any(key.endswith('_' + variable)
                                         for key in raw['configuration']['variables'][section]
                                         for variable in cfg['variables']))


if __name__ == '__main__':
    unittest.main()
