"""Configuration entry points and deferred dates for metadata-only planning."""
from copy import deepcopy
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock

import pandas as pd

from shybox.runner_toolkit.datasets.planning_handler import DatasetPlanner, DEFAULT_ENSEMBLE_TAGS
from shybox.runner_toolkit.datasets.lib_utils_datasets_time import create_time_period


class TestDatasetPlanner(unittest.TestCase):
    def setUp(self):
        self.logger = object()
        self.time = dict(
            time_run='2026-11-09 09:35', time_rounding='1h', time_frequency='1h',
            hour_reference_observed=None, hour_reference_forecast='00:00',
            time_look_observed='0h', time_horizon_observed=None,
            time_look_forecast='2h', time_horizon_forecast=None,
            priority_observed='min', priority_forecast='min',
        )
        source = dict(
            path='/tmp/planner/source', file_name='obs.{file_time_observed}.nc',
            x='x', y='y', time='time', dims='2d', nodata=-9999, format='netcdf',
            variable={'precip': 'rain'}, operations={}, mode='deterministic',
            ensemble=None, required=True, tolerance_steps=0,
            missing_policy='error', as_is=False,
        )
        forecast = deepcopy(source)
        forecast.update(file_name='cube.{file_time_forecast_nwp}.{file_time_nwp_valid}.nc',
                        operations={'split': {'method': '3d_to_2d'}})
        destination = dict(path='/tmp/planner/output',
                           file_name='{file_time_destination}.nc',
                           format='netcdf', type='grid', dims='2d', variable={'rain': 'RAIN'})
        self.config = dict(info={}, time=self.time,
                           data_src_obs={'rain': source}, data_src_frc={'rain': forecast},
                           data_dst_obs={'forcing': destination},
                           data_dst_frc={'forcing': deepcopy(destination)})

    def test_config_forwards_options_logger_and_run_without_mutating_inputs(self):
        before = deepcopy(self.config)
        options = {'custom': {'enabled': True}}
        planner = DatasetPlanner.from_config(
            **self.config, time_run='2026-11-09 10:35',
            logger=self.logger, options=options, geo={'path': '/tmp/terrain'},
        )
        self.assertIs(planner.log, self.logger)
        self.assertEqual(planner.time_table.index[0], pd.Timestamp('2026-11-09 10:00'))
        self.assertEqual(planner.application['geo'], {'terrain': {'path': '/tmp/terrain'}})
        planner.options['custom']['enabled'] = False
        self.assertTrue(options['custom']['enabled'])
        self.assertEqual(self.config, before)

    def test_generated_tags_distinguish_cube_run_and_validity_dates(self):
        names = ('file_time_observed', 'file_time_forecast_nwp',
                 'file_time_nwp_valid', 'file_time_destination', 'file_time_run')
        template = {name: '%Y%m%d%H' for name in names}
        template.update(time_run='%Y-%m-%d', custom_issue='%H', path_time_label='literal')
        formats = {name: 'time' for name in template}
        formats['path_time_label'] = 'string'
        tags = DatasetPlanner.create_time_tags(
            template, formats, references={'custom_issue': 'forecast_run'})
        self.assertNotIn('time_run', tags)
        self.assertNotIn('path_time_label', tags)
        self.assertEqual(tags['file_time_run']['reference'], 'time_run')
        self.assertEqual(tags['custom_issue']['reference'], 'forecast_run')
        planner = DatasetPlanner.from_config(**self.config, time_tags=tags, logger=self.logger)
        rows = planner.plan_files()
        forecast = rows[rows.group == 'forecast']
        self.assertEqual([Path(path).name for path in forecast.file_source],
                         ['cube.2026110900.2026110910.nc', 'cube.2026110900.2026110911.nc'])
        self.assertEqual([Path(path).name for path in forecast.file_destination],
                         ['2026110910.nc', '2026110911.nc'])

    def test_application_supports_existing_time_plan_and_custom_factory(self):
        planner = DatasetPlanner.from_config(**self.config, logger=self.logger)
        plan = create_time_period(**self.time)
        expected = plan.table.copy(deep=True)
        expected['time_mode'] = expected['time_mode'].map(DatasetPlanner.GROUPS)
        reused = DatasetPlanner.from_application(planner.application, logger=self.logger, time_plan=plan)
        pd.testing.assert_frame_equal(reused.time_table, expected)
        self.assertEqual(list(plan.table.time_mode), ['obs', 'frc', 'frc'])
        calls = []

        def factory(**kwargs):
            calls.append(kwargs)
            return plan

        custom = DatasetPlanner.from_application(
            planner.application, logger=self.logger, time_factory=factory)
        pd.testing.assert_frame_equal(custom.time_table, expected)
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]['reference_mode_forecast'], '3d')

    def test_time_modes_are_normalized_in_tables_and_file_templates(self):
        config = deepcopy(self.config)
        config['data_src_frc']['rain']['file_name'] = 'cube.nc'
        for section in ('data_dst_obs', 'data_dst_frc'):
            config[section]['forcing']['file_name'] = '{time_mode}/forcing.{time:%H}.nc'
        planner = DatasetPlanner.from_config(**config, logger=self.logger)
        rows = planner.plan_files()
        self.assertEqual(list(rows.time_mode), ['observed', 'forecast', 'forecast'])
        self.assertEqual(list(rows.time_mode), list(rows.group))
        self.assertEqual([Path(path).parent.name for path in rows.file_destination],
                         ['observed', 'forecast', 'forecast'])
        from dataclasses import replace
        plan = create_time_period(**self.time)
        normalized = plan.table.copy(deep=True)
        normalized['time_mode'] = normalized['time_mode'].map(DatasetPlanner.GROUPS)
        planner.configure_time(time_plan=replace(plan, table=normalized))
        self.assertEqual(list(planner.time_table.time_mode), list(rows.time_mode))

    def test_cube_issue_outside_validity_window_does_not_require_a_time_row(self):
        self.config['data_src_frc']['rain'].update(dims='3d', operations={})
        tags = DatasetPlanner.create_time_tags(
            {name: '%Y%m%d%H' for name in ('file_time_observed', 'file_time_destination', 'file_time_forecast_nwp', 'file_time_nwp_valid')},
            {name: 'time' for name in ('file_time_observed', 'file_time_destination', 'file_time_forecast_nwp', 'file_time_nwp_valid')})
        planner = DatasetPlanner.from_config(**self.config, logger=self.logger, time_tags=tags)
        forecast = planner.plan_files().query("group == 'forecast'")
        self.assertNotIn(pd.Timestamp('2026-11-09 00:00'), planner.time_period)
        self.assertEqual([Path(path).name for path in forecast.file_source],
                         ['cube.2026110900.2026110910.nc', 'cube.2026110900.2026110911.nc'])
        self.assertTrue(forecast.time_reference.eq(pd.Timestamp('2026-11-09 00:00')).all())

    def test_destination_without_source_remains_in_plan(self):
        self.config['data_dst_obs']['forcing']['variable']['pressure'] = 'P'
        self.config['data_src_frc']['rain']['file_name'] = 'cube.nc'
        planner = DatasetPlanner.from_config(**self.config, logger=self.logger)
        rows = planner.plan_files().query("variable == 'pressure'")
        self.assertEqual(len(rows), 1)
        self.assertTrue(rows.file_source.isna().all())

    def test_ensemble_aliases_fill_sources_and_expand_deterministic_outputs(self):
        for alias in (*DEFAULT_ENSEMBLE_TAGS, 'realization'):
            with self.subTest(alias=alias):
                config = deepcopy(self.config)
                source = config['data_src_frc']['rain']
                source.update(mode='probabilistic', ensemble={'start': 1, 'end': 2, 'format': '03d'},
                              file_name='cube.{' + alias + '}.nc')
                for section in ('data_dst_obs', 'data_dst_frc'):
                    config[section]['forcing']['file_name'] = '{' + alias + '}/forcing.{time:%H}.nc'
                extra = {'ensemble_tags': ('realization',)} if alias == 'realization' else {}
                planner = DatasetPlanner.from_config(**config, logger=self.logger, **extra)
                rows = planner.plan_files()
                forecast = rows[rows.group == 'forecast']
                self.assertEqual([Path(path).name for path in forecast.file_source],
                                 ['cube.001.nc', 'cube.002.nc'] * 2)
                observed = rows[rows.group == 'observed']
                self.assertEqual(list(observed.output_member), ['001', '002'])
                self.assertEqual([Path(path).parent.name for path in observed.file_destination],
                                 ['001', '002'])
                with self.assertRaisesRegex(ValueError, 'requires a member'):
                    planner.fill_file_name(config['data_dst_obs']['forcing'],
                                           observed.time.iloc[0], source=False)

    def test_ensemble_detection_rejects_deterministic_alias_and_ignores_literals(self):
        config = deepcopy(self.config)
        config['data_src_obs']['rain']['file_name'] = 'obs.{member}.nc'
        with self.assertRaisesRegex(ValueError, 'must match probabilistic mode'):
            DatasetPlanner.from_config(**config, logger=self.logger)
        config['data_src_obs']['rain']['file_name'] = 'obs.{{member}}.nc'
        planner = DatasetPlanner.from_config(**config, logger=self.logger)
        self.assertFalse(planner._has_ensemble_tag('{ensemble_member_extra}'))
        self.assertTrue(planner._has_ensemble_tag('{member:>3}'))
        self.assertEqual(Path(planner.fill_file_name(config['data_src_obs']['rain'],
                                                   planner.time_table.index[0])).name,
                         'obs.{member}.nc')
        for tags in ('member', [], ['']):
            with self.subTest(tags=tags), self.assertRaises((TypeError, ValueError)):
                DatasetPlanner.from_config(**self.config, logger=self.logger, ensemble_tags=tags)

    def test_member_bounds_accept_integer_strings_and_reject_invalid_values(self):
        self.assertEqual(DatasetPlanner._members(
            {'ensemble': {'start': '1', 'end': '2', 'format': '03d'}}, 'forecast.rain'),
            ('001', '002'))
        for value in (None, True, False, 1.5, '1.5', '', [], {}):
            for field in ('start', 'end'):
                spec = {'start': 1, 'end': 2, 'format': '03d', field: value}
                with self.subTest(field=field, value=value), self.assertRaisesRegex(
                    ValueError, 'forecast.rain: ensemble range'):
                    DatasetPlanner._members({'ensemble': spec}, 'forecast.rain')
        with self.assertRaisesRegex(ValueError, 'ensemble range'):
            DatasetPlanner._members({'ensemble': {'end': 2, 'format': '03d'}}, 'forecast.rain')

    def test_invalid_config_and_time_tags_report_errors(self):
        for section in self.config:
            with self.subTest(section=section), self.assertRaises(TypeError):
                DatasetPlanner.from_config(**{**self.config, section: None}, logger=self.logger)
        for section, field in (('time', 'time_frequency'), ('data_src_obs', 'path'),
                               ('data_dst_frc', 'format')):
            config = deepcopy(self.config)
            target = config[section] if section == 'time' else next(iter(config[section].values()))
            del target[field]
            with self.subTest(section=section), self.assertRaisesRegex(ValueError, field):
                DatasetPlanner.from_config(**config, logger=self.logger)
        for template in ({}, {'path_time_run': None}, {'path_time_run': ''}):
            with self.subTest(template=template), self.assertRaises(ValueError):
                DatasetPlanner.create_time_tags(template, {'path_time_run': 'time'})
        with self.assertRaises(TypeError):
            DatasetPlanner.create_time_tags(None, {})
        with self.assertRaises(ValueError):
            DatasetPlanner.create_time_tags({'custom': '%H'}, {'custom': 'time'},
                                            references={'custom': 'unknown'})


class TestDatasetPlannerAvailability(unittest.TestCase):
    def setUp(self):
        fixture = TestDatasetPlanner()
        fixture.setUp()
        self.config = fixture.config
        self.config['time'].update(time_look_observed='5h', time_look_forecast='3h')
        self.config['data_src_obs']['rain']['file_name'] = 'obs.{time:%H}.nc'
        self.config['data_src_frc']['rain']['file_name'] = 'cube.{file_time_forecast_nwp}.{time:%H}.nc'
        for section in ('data_dst_obs', 'data_dst_frc'):
            self.config[section]['forcing']['file_name'] = '{time_mode}/forcing.{time:%H}.nc'
        self.logger = Mock()
        self.calls = []

    def planner(self):
        return DatasetPlanner.from_config(**self.config, logger=self.logger)

    def probe(self, path):
        self.calls.append(str(path))
        return path.name not in ('obs.08.nc', 'obs.09.nc') and path.name.startswith(('obs.', 'cube.'))

    def test_trailing_gap_moves_cutoff_and_rebuilds_all_metadata_with_bounded_checks(self):
        planner = self.planner()
        original = planner.time_plan
        updated = planner.update_time(n_step_to_check_obs=3, n_step_to_check_frc=1, exists=self.probe)
        table = planner.time_table
        moved = table.loc[table.time_step.isin([-1, 0])]
        self.assertTrue(moved.time_mode.eq('forecast').all())
        self.assertTrue(moved.time_adjusted.all())
        self.assertTrue(moved.time_reference.eq(updated.forecast_run).all())
        self.assertEqual(updated.observed[-1], pd.Timestamp('2026-11-09 07:00'))
        self.assertEqual(updated.forecast[0], pd.Timestamp('2026-11-09 08:00'))
        self.assertEqual(list(original.table.time_mode).count('obs'), 6)
        self.assertEqual(int(table.time_checked.sum()), 5)
        self.assertTrue(table.loc[table.time_checked, 'time_available'].all())
        files = planner.file_plan
        adjusted = files.loc[files.time_adjusted]
        self.assertTrue(adjusted.source_name.eq('precip').all())
        self.assertTrue(adjusted.group.eq('forecast').all())
        self.assertTrue(adjusted.file_source.str.contains('cube.202611090000', regex=False).all())
        self.assertTrue(adjusted.file_destination.str.contains('/forecast/', regex=False).all())
        pd.testing.assert_series_equal(files.file_id, files.time.map(table.file_id), check_names=False)
        self.assertTrue(files.loc[~files.time_checked, ['source_exists', 'destination_exists']].isna().all().all())
        self.assertEqual(len(self.calls), len(set(self.calls)))
        self.assertEqual(planner.check_count, len(self.calls))
        checked_hours = {pd.Timestamp('2026-11-09 ' + str(hour) + ':00').strftime('%H') for hour in range(6, 11)}
        self.assertTrue(all(Path(path).stem.split('.')[-1] in checked_hours for path in self.calls))
        self.assertEqual(set(planner.file_table().time_mode), {'observed', 'forecast'})
        self.assertTrue(planner.file_not_available().empty)
        self.assertFalse(planner.file_available().empty)
        self.assertFalse(planner.file_not_available('destination').empty)

    def test_repeated_calls_restore_observations_and_disabled_windows_probe_nothing(self):
        planner = self.planner()
        planner.update_time(3, 1, exists=self.probe)
        planner.update_time(n_steps_observed=3, n_steps_forecast=1, exists=lambda path: True)
        self.assertFalse(planner.time_table.time_adjusted.any())
        self.assertEqual(planner.time_plan.observed[-1], pd.Timestamp('2026-11-09 09:00'))
        never = Mock(side_effect=AssertionError('unexpected file check'))
        planner.update_time(exists=never)
        never.assert_not_called()
        self.assertEqual(planner.check_count, 0)
        self.assertFalse(planner.time_table.time_checked.any())
        self.assertTrue(planner.file_plan.source_exists.isna().all())
        self.assertTrue(planner.file_available().empty)
        self.assertTrue(planner.file_not_available().empty)

    def test_missing_forecast_member_prevents_replacement(self):
        self.config['data_src_frc']['rain'].update(
            mode='probabilistic', ensemble={'start': 1, 'end': 2, 'format': '03d'},
            file_name='cube.{ensemble_member}.{time:%H}.nc')
        planner = self.planner()
        planner.update_time(1, None, exists=lambda path: path.name.startswith('cube.001.'))
        self.assertFalse(planner.time_table.time_adjusted.any())
        checked = planner.time_table.loc[planner.time_table.time_checked]
        self.assertTrue(checked.time_mode.eq('observed').all())
        self.assertFalse(checked.time_available.any())
        planner.update_time(1, None, exists=lambda path: path.name.startswith('cube.'))
        self.assertEqual(int(planner.time_table.time_adjusted.sum()), 2)
        self.assertEqual(set(planner.file_plan.loc[planner.file_plan.time_adjusted, 'ensemble_member']), {'001', '002'})

    def test_optional_missing_data_and_internal_gaps_do_not_move_cutoff(self):
        source = deepcopy(self.config['data_src_obs']['rain'])
        source.update(variable={'pressure': 'pressure'}, required=False, file_name='optional.{time:%H}.nc')
        self.config['data_src_obs']['pressure'] = source
        self.config['data_dst_obs']['forcing']['variable']['pressure'] = 'P'
        planner = self.planner()
        planner.update_time(3, 1, exists=lambda path: path.name.startswith('obs.') and path.name != 'obs.07.nc')
        self.assertFalse(planner.time_table.time_adjusted.any())
        self.assertTrue(planner.time_table.loc[pd.Timestamp('2026-11-09 09:00'), 'time_available'])
        self.assertFalse(planner.time_table.loc[pd.Timestamp('2026-11-09 07:00'), 'time_available'])
        self.assertFalse(planner.file_not_available().empty)

    def test_adjustment_can_be_disabled_and_forecast_checks_are_independent(self):
        for extra in ({'flag_adjust_obs_frc': False}, {'rules_to_fill_obs': None}):
            with self.subTest(extra=extra):
                planner = self.planner()
                planner.update_time(3, 1, exists=self.probe, **extra)
                self.assertFalse(planner.time_table.time_adjusted.any())
        planner = self.planner()
        planner.update_time(None, 1, exists=self.probe)
        self.assertEqual(int(planner.time_table.time_checked.sum()), 2)
        self.assertEqual(int(planner.time_table.time_adjusted.sum()), 1)

    def test_no_forecast_configuration_keeps_missing_observations(self):
        self.config['data_src_frc'] = {}
        self.config['data_dst_frc'] = {}
        planner = self.planner()
        planner.update_time(3, 1, exists=self.probe)
        self.assertFalse(planner.time_table.time_adjusted.any())
        self.assertFalse(planner.time_table.loc[pd.Timestamp('2026-11-09 09:00'), 'time_available'])
        self.assertFalse(planner.time_table.loc[pd.Timestamp('2026-11-09 10:00'), 'time_available'])

    def test_replacement_requires_all_required_variables_in_forecast_configuration(self):
        source = deepcopy(self.config['data_src_obs']['rain'])
        source.update(variable={'temperature': 'air_t'}, file_name='temperature.{time:%H}.nc')
        self.config['data_src_obs']['air_t'] = source
        self.config['data_dst_obs']['forcing']['variable']['air_t'] = 'T'
        planner = self.planner()
        planner.update_time(1, 1, exists=lambda path: path.name.startswith('cube.'))
        self.assertFalse(planner.time_table.time_adjusted.any())
        self.assertFalse(planner.time_table.loc[pd.Timestamp('2026-11-09 09:00'), 'time_available'])

    def test_real_files_and_step_reference_forecasts(self):
        with tempfile.TemporaryDirectory() as folder:
            for section in ('data_src_obs', 'data_src_frc'):
                self.config[section]['rain']['path'] = folder
            self.config['data_src_frc']['rain'].update(operations={}, file_name='forecast.{source_time:%H}.nc')
            Path(folder, 'forecast.09.nc').touch()
            planner = self.planner()
            planner.update_time(0, None)
            stamp = pd.Timestamp('2026-11-09 09:00')
            self.assertTrue(planner.time_table.loc[stamp, 'time_adjusted'])
            self.assertEqual(planner.time_table.loc[stamp, 'time_reference'], stamp)
            self.assertEqual(len(planner.file_available()), 1)
            self.assertEqual(Path(planner.file_available().file_name.iloc[0]).name, 'forecast.09.nc')

    def test_window_scan_deduplicates_paths_and_keeps_unchecked_rows_unknown(self):
        self.config['data_src_frc']['rain']['file_name'] = 'cube.nc'
        planner = self.planner()
        files = planner.plan_files(check_exists=True, check_times=[pd.Timestamp('2026-11-09 11:00')])
        self.assertTrue(files.loc[files.time.ne(pd.Timestamp('2026-11-09 11:00')), 'source_exists'].isna().all())
        calls = []
        planner.check_availability(check_times=[pd.Timestamp('2026-11-09 11:00')],
                                   exists=lambda path: calls.append(str(path)) or True)
        self.assertEqual(len(calls), 2)
        cube = planner.file_available().query("time_mode == 'forecast'")
        self.assertEqual(len(cube), 1)
        self.assertTrue(cube.file_exists.iloc[0])

    def test_invalid_update_options_fail_before_scanning(self):
        planner = self.planner()
        for options in ({'n_step_to_check_obs': -1}, {'n_step_to_check_frc': True},
                        {'n_steps_forecast': '1'}, {'flag_adjust_obs_frc': 'yes'},
                        {'rules_to_fill_obs': 'unknown'}, {'rules_to_fill_frc': 'fill_using_obs'},
                        {'n_step_to_check_obs': 1, 'n_steps_observed': 2}):
            with self.subTest(options=options), self.assertRaises((ValueError, TypeError)):
                planner.update_time(**options)

    def test_scalar_check_times_report_clear_error_without_probing(self):
        planner = self.planner()
        probe = Mock(side_effect=AssertionError('unexpected file check'))
        for value in (True, False, 1, '2026-11-09', pd.Timestamp('2026-11-09')):
            with self.subTest(value=value), self.assertRaisesRegex(TypeError, 'collection of timestamps'):
                planner.check_availability(check_times=value, exists=probe)
        with self.assertRaisesRegex(TypeError, 'collection of timestamps'):
            planner.plan_files(check_exists=True, check_times=True)
        probe.assert_not_called()

    def test_files_by_ensemble_broadcasts_shared_variables_and_observations(self):
        self.config['data_src_frc']['rain'].update(
            mode='probabilistic', ensemble={'start': 1, 'end': 2, 'format': '03d'},
            file_name='rain.{ensemble_member}.{time:%H}.nc')
        for group in ('obs', 'frc'):
            source = deepcopy(self.config['data_src_obs']['rain'])
            source.update(variable={'temperature': 'air_t'}, file_name='temperature.{time:%H}.nc')
            self.config['data_src_' + group]['air_t'] = source
            self.config['data_dst_' + group]['forcing']['variable']['air_t'] = 'T'
        planner = self.planner()
        sources = planner.file_by_ensemble()
        self.assertEqual(set(sources), {'001', '002'})
        observed, forecast = '2026-11-09 09:00:00', '2026-11-09 10:00:00'
        self.assertEqual(sources['001'][observed], sources['002'][observed])
        self.assertEqual(set(sources['001'][forecast]), {'rain', 'air_t'})
        self.assertEqual(sources['001'][forecast]['air_t'], sources['002'][forecast]['air_t'])
        self.assertEqual(Path(sources['001'][forecast]['rain']).name, 'rain.001.10.nc')
        self.assertEqual(Path(sources['002'][forecast]['rain']).name, 'rain.002.10.nc')
        self.assertEqual(list(sources['001']), sorted(sources['001']))
        destinations = planner.file_by_ensemble('destination')
        self.assertEqual(destinations['001'], destinations['002'])
        self.assertEqual(destinations['001'][forecast]['rain'], destinations['001'][forecast]['air_t'])
        sources['001'][forecast]['rain'] = 'changed'
        self.assertNotEqual(planner.file_by_ensemble()['001'][forecast]['rain'], 'changed')
        self.assertEqual(planner.check_count, 0)

    def test_files_by_ensemble_uses_adjusted_plan_and_deterministic_key(self):
        planner = self.planner()
        planner.update_time(1, None, exists=self.probe)
        result = planner.file_by_ensemble()
        self.assertEqual(set(result), {'deterministic'})
        self.assertTrue(Path(result['deterministic']['2026-11-09 09:00:00']['rain']).name.startswith('cube.'))
        with self.assertRaisesRegex(ValueError, 'source or destination'):
            planner.file_by_ensemble('invalid')


if __name__ == '__main__':
    unittest.main()
