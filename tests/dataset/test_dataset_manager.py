"""Metadata-only planning, manager state and execution delegation."""
from copy import deepcopy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import pandas as pd

from shybox.runner_toolkit.datasets.datasets_handler import DatasetManager, MANDATORY


class TestDatasetManager(unittest.TestCase):
    def setUp(self):
        self.temp = self.enterContext(tempfile.TemporaryDirectory())
        self.root = Path(self.temp)
        self.settings = {
            'time': {'time_run': '2026-11-09 09:35', 'time_rounding': '1h', 'time_frequency': '1h',
                     'hour_reference_observed': None, 'hour_reference_forecast': '00:00',
                     'time_look_observed': '0h', 'time_horizon_observed': None,
                     'time_look_forecast': '2h', 'time_horizon_forecast': None,
                     'priority_observed': 'min', 'priority_forecast': 'min'},
            'variables': {key: {'output_name': key.upper(), 'units': 'test'} for key in MANDATORY},
            'sources': {group: {key: {'folder_name': self.temp, 'file_name': group + '.{time:%H}.nc',
                                      'variable': key, 'mode': 'deterministic'} for key in MANDATORY}
                        for group in ('observed', 'forecast')},
            'destination': {'folder_name': str(self.root / 'output'), 'file_name': '{time_mode}/forcing.{time:%H}.nc'},
            'geo': {'folder_name': self.temp, 'file_name': 'not_read_during_planning.asc'}}

    def probabilistic(self):
        self.settings['sources']['forecast']['rain'].update(
            mode='probabilistic', ensemble={'start': 1, 'end': 2, 'format': '03d'},
            file_name='rain.{ensemble_member}.{time:%H}.nc')

    def test_time_tags_follow_declared_types_templates_and_references(self):
        template = {
            'time_run': '%Y-%m-%d %H:%M', 'path_time_run': '%Y%m%d%H%M',
            'file_time_destination': '%Y%m%d%H', 'file_time_forecast_nwp': '%Y%m%d%H',
            'file_time_forecast_nwp_valid': '%Y%m%d%H%M',
            'path_time_label': 'literal', 'custom_issue': '%Y%m%d',
        }
        formats = {name: 'time' for name in template}
        formats['path_time_label'] = 'string'
        tags = DatasetManager.create_time_tags(
            template, formats, references={'custom_issue': 'forecast_run'})
        self.assertNotIn('time_run', tags)
        self.assertNotIn('path_time_label', tags)
        self.assertEqual(tags['path_time_run']['reference'], 'time_run')
        self.assertEqual(tags['file_time_destination']['reference'], 'time')
        self.assertEqual(tags['file_time_forecast_nwp']['reference'], 'source_time')
        self.assertEqual(tags['file_time_forecast_nwp_valid']['reference'], 'time')
        self.assertEqual(tags['custom_issue'], {'reference': 'forecast_run', 'format': '%Y%m%d'})
        tags['custom_issue']['format'] = 'changed'
        self.assertEqual(template['custom_issue'], '%Y%m%d')

    def test_time_tag_templates_drive_cube_and_validity_paths(self):
        self.settings['sources']['forecast']['rain'].update(
            operations={'split': {'method': '3d_to_2d'}},
            file_name='cube.{file_time_forecast_nwp}.{file_time_forecast_nwp_valid}.nc')
        names = ('file_time_forecast_nwp', 'file_time_forecast_nwp_valid')
        self.settings['time_tags'] = DatasetManager.create_time_tags(
            {name: '%Y%m%d%H' for name in names}, {name: 'time' for name in names})
        manager = DatasetManager.from_settings(self.settings)
        rows = manager.plan_files()
        rain = rows[(rows.time_mode == 'frc') & (rows.variable == 'rain')]
        self.assertEqual([Path(name).name for name in rain.file_source],
                         ['cube.2026110900.2026110910.nc', 'cube.2026110900.2026110911.nc'])

    def test_invalid_time_tag_declarations_fail_before_planning(self):
        for template in ({}, {'path_time_run': None}, {'path_time_run': ''}):
            with self.subTest(template=template), self.assertRaisesRegex(ValueError, 'time template'):
                DatasetManager.create_time_tags(template, {'path_time_run': 'time'})
        for args in ((None, {}), ({}, [])):
            with self.subTest(args=args), self.assertRaises(TypeError):
                DatasetManager.create_time_tags(*args)
        for references in ({'custom': 'unknown'}, {'custom': 'time'}):
            with self.subTest(references=references), self.assertRaises(ValueError):
                DatasetManager.create_time_tags({'custom': '%Y'}, {'custom': 'string'}, references=references)
        self.assertEqual(DatasetManager.create_time_tags({}, {}), {})

    def test_constructor_accepts_separate_configuration_sections(self):
        sources = {group: {key: {**source, 'path': self.temp, 'variable': {key: key},
                                'x': 'longitude', 'y': 'latitude', 'time': 'time', 'nodata': -9999,
                                'format': 'netcdf', 'operations': {'interpolate': None, 'split': None},
                                'ensemble': None, 'required': True, 'tolerance_steps': 0,
                                'missing_policy': 'error', 'as_is': False}
                           for key, source in values.items()}
                   for group, values in self.settings['sources'].items()}
        destination = {**self.settings['destination'], 'path': str(self.root / 'output'),
                       'format': 'netcdf', 'type': 'hmc',
                       'variables': {key: key.upper() for key in MANDATORY},
                       'units': {key: 'test' for key in MANDATORY}}
        manager = DatasetManager(
            info={'model': 'hmc'}, time=self.settings['time'],
            data_source_obs=sources['observed'], data_source_frc=sources['forecast'],
            data_destination=destination).configure()
        self.assertEqual(len(manager.plan_files()), 15)
        sources['observed']['rain']['path'] = '/changed'
        self.assertEqual(manager.settings['sources']['observed']['rain']['folder_name'], self.temp)
        with self.assertRaisesRegex(ValueError, 'reference geo grid'):
            manager.run()
        with self.assertRaisesRegex(TypeError, 'info must be a dictionary'):
            DatasetManager(None, self.settings['time'], sources['observed'], sources['forecast'],
                           destination)

    def test_separate_sections_preserve_source_fields_and_group_filenames(self):
        source = {
            'path': self.temp + '/{path_time_observed}',
            'file_name': 'ws.db.{file_time_observed}.nc',
            'x': 'longitude', 'y': 'latitude', 'time': 'time', 'nodata': -9999,
            'format': 'netcdf', 'variable': {'Rain': 'rain'},
            'operations': {'interpolate': None, 'split': None},
            'mode': 'deterministic', 'ensemble': None, 'required': True,
            'tolerance_steps': 0, 'missing_policy': 'error', 'as_is': True,
        }
        sources = {key: {**deepcopy(source), 'variable': {key.upper(): key}} for key in MANDATORY}
        sources['rain'] = source
        destination = {
            'path': str(self.root / 'output'), 'file_name': 'observed.{time:%H}.nc',
            'format': 'netcdf', 'type': 'hmc',
            'variables': {key: key.upper() for key in MANDATORY},
        }
        manager = DatasetManager(
            info={'model': 'hmc'}, time=self.settings['time'],
            data_source_obs=sources, data_source_frc=sources,
            data_destination=destination,
            time_run='2026-11-09 10:35', options=None)
        manager.configure_destination(file_name_forecast='forecast.{time:%H}.nc')
        normalized = manager.settings['sources']['observed']['rain']
        for key in source.keys() - {'variable'}:
            self.assertEqual(normalized[key], source[key])
        self.assertEqual(normalized['variable'], 'Rain')
        files = manager.plan_files(check_exists=True)
        self.assertTrue(files[files.time_mode == 'obs'].file_destination.str.contains('observed.10.nc', regex=False).all())
        self.assertTrue(files[files.time_mode == 'frc'].file_destination.str.contains('forecast.', regex=False).all())
        self.assertTrue(files[files.time_mode == 'obs'].file_source.str.endswith('/2026/11/09/ws.db.202611091000.nc').all())
        for key in self.settings['time']:
            incomplete = {name: value for name, value in self.settings['time'].items() if name != key}
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, 'Missing dataset time fields'):
                DatasetManager({'model': 'hmc'}, incomplete, sources, sources, destination)
        for key in source:
            incomplete_sources = deepcopy(sources)
            del incomplete_sources['rain'][key]
            with self.subTest(source_field=key), self.assertRaisesRegex(ValueError, 'missing source fields'):
                DatasetManager({'model': 'hmc'}, self.settings['time'], incomplete_sources,
                               sources, destination)

    def test_shared_destination_can_be_expanded_and_reset_after_planning(self):
        manager = DatasetManager.from_settings(self.settings).configure()
        period = manager.time_period
        original = manager.plan_files()
        manager.output_path = self.root / 'previous_run'
        destination = manager.configure_destination(
            file_name_observed='obs/input.{time:%H}.nc',
            file_name_forecast='frc/input.{time:%H}.nc')
        self.assertIsNone(manager.output_path)
        pd.testing.assert_index_equal(manager.time_period, period)
        files = manager.file_plan
        self.assertTrue(files[files.time_mode == 'obs'].file_destination.str.contains('/obs/input.09.nc', regex=False).all())
        self.assertTrue(files[files.time_mode == 'frc'].file_destination.str.contains('/frc/input.', regex=False).all())
        self.assertEqual(destination['folder_name'], self.settings['destination']['folder_name'])
        destination['file_names']['forecast'] = 'mutated.nc'
        self.assertNotEqual(manager.settings['destination']['file_names']['forecast'], 'mutated.nc')
        for name in ('', ' ', 3):
            with self.subTest(name=name), self.assertRaisesRegex(ValueError, 'nonempty string'):
                manager.configure_destination(file_name_forecast=name)
        pd.testing.assert_frame_equal(manager.file_plan, files)
        manager.configure_destination()
        pd.testing.assert_frame_equal(manager.file_plan, original)
        self.assertFalse((self.root / 'output').exists())

    def test_deterministic_plan_combines_variables_without_writing(self):
        manager = DatasetManager.from_settings(self.settings).configure()
        files = manager.plan_files()
        self.assertEqual(len(files), 15)
        self.assertEqual(files.file_destination.nunique(), 3)
        self.assertEqual(files.file_source.nunique(), 3)
        self.assertEqual(set(files.member), {'deterministic'})
        self.assertEqual(set(files.time_mode), {'obs', 'frc'})
        self.assertFalse((self.root / 'output').exists())
        self.assertEqual(manager.mode.mode, 'deterministic')
        self.assertEqual(len(manager.time_table), 3)
        pd.testing.assert_index_equal(manager.time_period, manager.time_table.index)
        self.assertEqual(list(files.groupby('step').time.first()), list(manager.time_period))

    def test_probabilistic_plan_preserves_shared_deterministic_sources(self):
        self.probabilistic()
        manager = DatasetManager.from_settings(self.settings).configure()
        files = manager.plan_files()
        self.assertEqual(manager.members, ('001', '002'))
        self.assertEqual(len(files), 30)
        self.assertEqual(files.file_destination.nunique(), 6)
        self.assertEqual(files.file_source.nunique(), 7)
        rain = files[(files.time_mode == 'frc') & (files.variable == 'rain')]
        self.assertEqual(set(rain.member), {'001', '002'})
        shared = files[(files.time_mode == 'obs') | (files.variable != 'rain')]
        self.assertEqual(set(shared.member), {'deterministic'})
        self.assertEqual(files[files.time_mode == 'obs'].file_source.nunique(), 1)
        self.assertIn('/run_001/', files.file_destination.iloc[0])

    def test_each_source_uses_its_step_or_cube_reference(self):
        self.settings['sources']['forecast']['rain'].update(operations={'split': {'method': '3d_to_2d'}},
                                                            file_name='cube.{forecast_run:%H}.nc')
        files = DatasetManager.from_settings(self.settings).plan_files()
        rain = files[(files.time_mode == 'frc') & (files.variable == 'rain')]
        air = files[(files.time_mode == 'frc') & (files.variable == 'air_t')]
        self.assertEqual(set(rain.time_reference), {pd.Timestamp('2026-11-09 00:00')})
        self.assertEqual(list(air.time_reference), list(air.time))

    def test_existence_checks_report_missing_without_creating_files(self):
        (self.root / 'observed.09.nc').touch()
        manager = DatasetManager.from_settings(self.settings)
        self.assertTrue(manager.plan_files().source_exists.isna().all())
        self.assertTrue(manager.file_plan.destination_exists.isna().all())
        files = manager.plan_files(check_exists=True)
        self.assertTrue(files[files.time_mode == 'obs'].source_exists.all())
        self.assertFalse(files[files.time_mode == 'frc'].source_exists.any())
        self.assertFalse(files.destination_exists.any())
        self.assertFalse((self.root / 'output').exists())
        summary = manager.as_dict(include_files=True)
        self.assertEqual(summary['source_files_available'], 1)
        self.assertEqual(summary['source_files_missing'], 2)
        self.assertEqual(summary['destination_files_missing'], 3)
        json.dumps(summary)

    def test_availability_refresh_distinguishes_files_directories_and_unconfigured_sources(self):
        del self.settings['sources']['forecast']['rain']
        (self.root / 'forecast.10.nc').mkdir()
        manager = DatasetManager.from_settings(self.settings)
        files = manager.plan_files(check_exists=True)
        self.assertFalse(files.source_exists.any())
        missing = files[(files.time_mode == 'frc') & (files.variable == 'rain')]
        self.assertTrue(missing.file_source.isna().all())
        self.assertFalse(missing.source_exists.any())
        output = Path(files.file_destination.iloc[0])
        output.parent.mkdir(parents=True)
        output.touch()
        (self.root / 'forecast.11.nc').touch()
        refreshed = manager.plan_files(check_exists=True)
        self.assertTrue(refreshed[refreshed.step == 1].destination_exists.all())
        self.assertFalse(refreshed[refreshed.step == 2].source_exists.any())
        self.assertEqual(int(refreshed[refreshed.step == 3].source_exists.sum()), 4)
        self.assertEqual(manager.as_dict()['destination_files_available'], 1)

    def test_ensemble_plan_is_ordered_by_step_and_checks_shared_files_once(self):
        self.probabilistic()
        manager = DatasetManager.from_settings(self.settings)
        with patch.object(Path, 'is_file', return_value=True) as stat:
            files = manager.plan_files(check_exists=True)
        self.assertTrue(files.step.is_monotonic_increasing)
        self.assertEqual(list(files.step.unique()), [1, 2, 3])
        self.assertEqual(stat.call_count, 13)  # Seven sources and six outputs.
        self.assertTrue(files.source_exists.all())
        self.assertTrue(files.destination_exists.all())
        self.assertEqual(manager.as_dict()['source_files_available'], 7)

    def test_collision_and_path_escape_fail_during_planning(self):
        for template in ('same.nc', '../escape.nc'):
            settings = deepcopy(self.settings)
            settings['destination']['file_name'] = template
            with self.subTest(template=template), self.assertRaises(ValueError):
                DatasetManager.from_settings(settings).plan_files()
        self.assertFalse((self.root / 'output').exists())

    def test_time_reconfiguration_invalidates_file_plan(self):
        manager = DatasetManager.from_settings(self.settings).configure()
        manager.plan_files()
        manager.configure_time('2026-11-09 10:35')
        summary = manager.as_dict(include_files=True)
        self.assertEqual(summary['time_run'], '2026-11-09 10:00:00')
        self.assertEqual(summary['destination_files'], 3)
        self.assertEqual(summary['file_plan'][0]['time'], '2026-11-09 10:00:00')
        json.dumps(summary)

    def test_settings_are_copied_and_run_reuses_configured_time_plan(self):
        manager = DatasetManager.from_settings(self.settings).configure()
        plan = manager.time_plan
        self.settings['destination']['file_name'] = 'changed.nc'
        exported = manager.settings
        exported['destination']['file_name'] = 'changed_again.nc'
        self.assertIn('{time_mode}', manager.settings['destination']['file_name'])
        result = self.root / 'published'
        with patch('shybox.runner_toolkit.datasets.datasets_handler.compose_datasets', return_value=result) as compose:
            self.assertEqual(manager.run(), result)
        self.assertIs(compose.call_args.kwargs['plan'], plan)
        self.assertEqual(manager.output_path, result)

    def test_update_time_checks_window_and_restores_late_observations(self):
        self.settings['time']['time_look_observed'] = '2h'
        for hour in (7, 8, 9, 10, 11):
            (self.root / f'forecast.{hour:02}.nc').touch()
        manager = DatasetManager.from_settings(self.settings)
        plan = manager.update_time(n_steps_observed=1, n_steps_forecast=1)
        table = manager.time_table
        self.assertEqual(list(table.time_step), [-2, -1, 0, 1, 2])
        self.assertEqual(list(table.time_mode), ['obs', 'frc', 'frc', 'frc', 'frc'])
        self.assertEqual(list(table.time_checked), [False, True, True, True, False])
        self.assertTrue(table.loc[table.time_checked, 'time_available'].all())
        self.assertTrue(table.loc[~table.time_checked, 'time_available'].isna().all())
        self.assertEqual(len(plan.observed), 1)
        self.assertTrue(plan.time_period.is_monotonic_increasing)
        self.assertTrue(manager.file_plan[manager.file_plan.time_step == 0].file_source.str.endswith('forecast.09.nc').all())
        (self.root / 'observed.09.nc').touch()
        manager.update_time(n_steps_observed=0, n_steps_forecast=0)
        self.assertEqual(list(manager.time_table.time_mode), ['obs', 'obs', 'obs', 'frc', 'frc'])
        self.assertEqual(int(manager.time_table.time_checked.sum()), 1)
        json.dumps(manager.as_dict(include_files=True), allow_nan=False)

    def test_update_time_keeps_missing_step_when_neither_source_exists(self):
        manager = DatasetManager.from_settings(self.settings)
        manager.update_time(n_steps_observed=0, n_steps_forecast=2)
        self.assertFalse(manager.time_table.time_available.any())
        self.assertEqual(manager.time_table.iloc[0].time_mode, 'obs')
        self.assertFalse(manager.file_plan.source_exists.any())
        for value in (-1, True, 1.5):
            with self.subTest(value=value), self.assertRaises(ValueError):
                manager.update_time(n_steps_observed=value)

    def test_update_time_checks_all_required_ensemble_members_and_cube_references(self):
        self.probabilistic()
        for source in self.settings['sources']['forecast'].values():
            source.update(operations={'split': {'method': '3d_to_2d'}},
                          file_name='cube.{source_time:%H}.nc')
        self.settings['sources']['forecast']['rain']['file_name'] = 'rain.{ensemble_member}.{source_time:%H}.nc'
        (self.root / 'cube.00.nc').touch()
        (self.root / 'rain.001.00.nc').touch()
        manager = DatasetManager.from_settings(self.settings)
        manager.update_time(n_steps_observed=0, n_steps_forecast=2)
        self.assertEqual(manager.time_table.iloc[0].time_mode, 'obs')
        (self.root / 'rain.002.00.nc').touch()
        manager.update_time(n_steps_observed=0, n_steps_forecast=2)
        self.assertEqual(manager.time_table.iloc[0].time_mode, 'frc')
        self.assertTrue(manager.time_table.time_available.all())
        self.assertTrue((manager.file_plan.time_reference == pd.Timestamp('2026-11-09 00:00')).all())
        self.assertEqual(manager.file_plan.source_file_id.nunique(), 3)
        self.assertTrue((manager.time_table.source_file_count == 3).all())
        self.assertTrue((manager.time_table.destination_file_count == 2).all())
        self.assertEqual(manager.file_plan.destination_file_id.nunique(), 6)

    def test_missing_optional_source_does_not_replace_observations(self):
        self.settings['variables']['extra'] = {'output_name': 'EXTRA', 'units': 'test'}
        (self.root / 'observed.09.nc').touch()
        (self.root / 'forecast.09.nc').touch()
        manager = DatasetManager.from_settings(self.settings)
        manager.update_time(n_steps_observed=0)
        self.assertEqual(manager.time_table.iloc[0].time_mode, 'obs')
        self.assertTrue(manager.time_table.iloc[0].time_available)

    def test_update_time_only_stats_explicit_windows_and_none_disables_checks(self):
        self.settings['time']['time_look_observed'] = '2h'
        cases = ((None, None, []), (1, None, [-1, 0]), (None, 1, [0, 1]),
                 (0, None, [0]), (None, 0, [0]), (1, 1, [-1, 0, 1]))
        for observed, forecast, offsets in cases:
            with self.subTest(observed=observed, forecast=forecast):
                manager = DatasetManager.from_settings(self.settings)
                initial = manager.plan_files()
                selected = initial[initial.time_step.isin(offsets)]
                expected = set(selected.file_source) | set(selected.file_destination)
                checked = set()

                def check_file(path):
                    self.assertIn(str(path), expected)
                    checked.add(str(path))
                    return True

                with patch.object(Path, 'is_file', new=check_file):
                    manager.update_time(n_steps_observed=observed, n_steps_forecast=forecast)
                self.assertEqual(checked, expected)
                table = manager.time_table
                self.assertEqual(list(table[table.time_checked].time_step), offsets)
                files = manager.file_plan
                outside = files[~files.time_step.isin(offsets)]
                self.assertTrue(outside.source_exists.isna().all())
                self.assertTrue(outside.destination_exists.isna().all())
                self.assertTrue(table.loc[~table.time_checked, 'time_available'].isna().all())
                if not offsets:
                    self.assertIsNone(manager.as_dict()['source_files_available'])


if __name__ == '__main__':
    unittest.main()
