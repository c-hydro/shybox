"""Verify how lifecycle record paths are resolved."""
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import pandas as pd

from shybox.monitoring_toolkit.monitoring_execution_handler import MonitoringExecutionManager as MonitoringManager
from shybox.monitoring_toolkit.lib_monitoring_io import read_status


class TestMonitoringPaths(unittest.TestCase):
    def test_path_and_filename_use_string_and_timestamp_placeholders(self):
        for value in ('2026-11-20 15:30', pd.Timestamp('2026-11-20 15:30')):
            with self.subTest(value=value):
                monitor = MonitoringManager(
                    Path('records/{model}/{time_run:%Y%m%d}'), '{model}_{time_run:%H%M}_{execution_id}.json',
                    run_name='test', application='hmc', placeholders={'time_run': value, 'model': 'zzo'},
                )
                expected = Path('records/zzo/20261120') / f'zzo_1500_{monitor.execution_id}.json'
                self.assertEqual(monitor.file_path, expected)

    def test_rounds_record_and_filename_times_to_utc_hour(self):
        cases = (
            ({}, '2026-11-20T22:30:00-01:00', '2026-11-20T23:00:00.000000Z', '202611202300'),
            ({'time_rounding': 'ceil'}, '2026-11-20T22:30:00-01:00', '2026-11-21T00:00:00.000000Z', '202611210000'),
            ({'time_rounding': 'ceil'}, '2026-11-20 15:00', '2026-11-20T15:00:00.000000Z', '202611201500'),
            ({'time_rounding': None}, '2026-11-20 15:30', '2026-11-20T15:30:00.000000Z', '202611201530'),
        )
        for options, run_time, expected_time, label in cases:
            with self.subTest(options=options, run_time=run_time):
                monitor = MonitoringManager(
                    'records', '{run_time:%Y%m%d%H%M}.json', run_name='test', application='hmc',
                    run_time=run_time, **options,
                )
                self.assertEqual(monitor.record['run_time'], expected_time)
                self.assertEqual(monitor.file_path.name, f'{label}.json')

    def test_default_time_format_and_overrides(self):
        for options, label in (({}, '202611201500'), ({'time_format': '%d-%m-%Y_%H'}, '20-11-2026_15')):
            with self.subTest(options=options):
                monitor = MonitoringManager(
                    'records/{time_run}', '{run_time}_{run_time:%Y-%m-%d}_{run_name}_{app}.json',
                    run_name='test', application='hmc', run_time='2026-11-20 15:30', **options,
                )
                self.assertEqual(monitor.file_path, Path('records', label, f'{label}_2026-11-20_test_hmc.json'))
                self.assertEqual(monitor.record['run_time'], '2026-11-20T15:00:00.000000Z')

    def test_paths_use_monitoring_settings_and_placeholder_subkeys(self):
        monitor = MonitoringManager(
            'records/{run_domain}/{site.code}/{workload.variant}',
            '{run_name}_{run_mode}_{ensemble:03d}_{placeholders.site.code}_{model}.json',
            run_name='test', application='hmc', run_domain='marche', run_mode='probabilistic', ensemble=4,
            placeholders={'site': {'code': 'east'}, 'model': 'zzo'}, workload={'variant': 'demo'},
        )
        self.assertEqual(monitor.file_path, Path('records/marche/east/demo/test_probabilistic_004_east_zzo.json'))

    def test_unresolved_placeholders_are_reported_once(self):
        with patch('shybox.monitoring_toolkit.monitoring_execution_handler.LoggingManager.get_logger') as get_logger:
            monitor = MonitoringManager(
                'records/{missing}', '{model}_{missing}_{extra:03d}.json',
                run_name='test', application='hmc',
            )
        self.assertEqual(monitor.file_path, Path('records/{missing}/hmc_{missing}_{extra:03d}.json'))
        get_logger.return_value.warning.assert_called_once_with(
            'Unresolved monitoring path placeholders: %s', 'extra, missing',
        )

    def test_record_paths(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            directory = root / 'records'
            absolute = root / 'override.json'
            for file_name in (None, 'custom.json', absolute):
                with self.subTest(file_name=file_name):
                    monitor = MonitoringManager(directory, file_name, run_name='test', application='hmc')
                    expected = {
                        None: directory / f'{monitor.execution_id}.json',
                        'custom.json': directory / 'custom.json',
                        absolute: absolute,
                    }[file_name]
                    self.assertEqual(monitor.file_path, expected)
                    with monitor:
                        self.assertTrue(expected.is_file())
                        self.assertIsNone(read_status(expected)['exit_code'])
                    self.assertEqual(read_status(expected)['exit_code'], 0)

    def test_context_manager_records_failure_and_cancellation_codes(self):
        cases = (
            (RuntimeError('failure'), 1, 'failed'),
            (KeyboardInterrupt(), 130, 'cancelled'),
            (SystemExit(7), 7, 'failed'),
            (SystemExit(0), 0, 'succeeded'),
        )
        with tempfile.TemporaryDirectory() as folder:
            for error, code, status in cases:
                with self.subTest(error=error):
                    monitor = MonitoringManager(folder, None, run_name='test', application='hmc')
                    with self.assertRaises(type(error)), monitor:
                        raise error
                    record = read_status(monitor.file_path)
                    self.assertEqual(record['exit_code'], code)
                    self.assertEqual(record['status'], status)
